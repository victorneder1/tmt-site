from __future__ import annotations

import json
import threading
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .document_store import DocumentStore
from .sec_client import (
    Form4Filing,
    Form4Movement,
    download_form4_xml,
    fetch_recent_form4_filings,
    load_ticker_to_cik_map,
    parse_form4_xml,
)
from .sec_config import SecConfig
from .sec_repurchase_client import (
    MonthlyRepurchase,
    RepurchaseFiling,
    download_and_parse_repurchase,
    get_repurchase_filings,
)


class SECMonitor:
    def __init__(self, config: SecConfig) -> None:
        self.config = config
        self.document_store = DocumentStore(config.db_file)
        self.seen_accessions: set[str] = _load_seen(config.state_file)
        self.bootstrap_mode = self.document_store.is_empty()
        self.ticker_cik_map: dict[str, str] = {}
        self.lock = threading.Lock()
        self.thread: threading.Thread | None = None
        self.stop_event = threading.Event()
        self.last_check_at: str | None = None
        self.last_success_at: str | None = None
        self.last_error: str | None = None
        self.movements: list[dict[str, Any]] = self.document_store.get_movements()
        self.history_documents: list[dict[str, Any]] = self.document_store.get_history_documents()

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._run_loop, name="sec-monitor", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)

    def force_refresh(self) -> None:
        self._refresh_once()

    def get_snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "companies": [
                    {
                        "name": c.name,
                        "ticker": c.ticker,
                        "sector": c.sector,
                        "cik": c.cik,
                        "market": "US",
                    }
                    for c in self.config.companies
                ],
                "movements": list(self.movements),
                "history_documents": list(self.history_documents),
                "last_check_at": self.last_check_at,
                "last_success_at": self.last_success_at,
                "last_error": self.last_error,
            }

    def _run_loop(self) -> None:
        while not self.stop_event.is_set():
            self._refresh_once()
            self.stop_event.wait(self.config.poll_interval_seconds)

    def _refresh_once(self) -> None:
        timestamp = datetime.now(UTC).isoformat()
        with self.lock:
            self.last_check_at = timestamp

        try:
            # Fill in any missing CIKs via EDGAR ticker map (skip companies that
            # already have a hardcoded CIK from the config file).
            needs_lookup = [c for c in self.config.companies if not c.cik]
            if needs_lookup:
                self.ticker_cik_map = load_ticker_to_cik_map(
                    self.config.ticker_cik_cache,
                    self.config.user_agent,
                )
                for company in needs_lookup:
                    company.cik = self.ticker_cik_map.get(company.ticker.upper(), "")

            changed = False
            submissions_cache = self.config.filings_dir / "submissions"
            repurchase_cache  = self.config.filings_dir / "repurchase"

            for company in self.config.companies:
                if not company.cik:
                    continue

                # ── Form 4 (insider trades — ZETA and BRZE only) ───────────
                try:
                    form4_filings = fetch_recent_form4_filings(
                        cik=company.cik,
                        ticker=company.ticker,
                        company_alias=company.name,
                        since_date=self.config.history_start_date,
                        user_agent=self.config.user_agent,
                    )
                    for filing in form4_filings:
                        if filing.accession_number in self.seen_accessions:
                            continue
                        self._ingest_filing(filing, company.sector)
                        self.seen_accessions.add(filing.accession_number)
                        changed = True
                except Exception:  # noqa: BLE001
                    pass

                # ── Repurchase tables (10-Q / 10-K / 20-F — all companies) ─
                try:
                    repur_filings = get_repurchase_filings(
                        cik=company.cik,
                        ticker=company.ticker,
                        company_alias=company.name,
                        since_date=self.config.history_start_date,
                        user_agent=self.config.user_agent,
                        submissions_cache=submissions_cache,
                    )
                    for filing in repur_filings:
                        repur_key = f"REPUR:{filing.accession_number}"
                        if repur_key in self.seen_accessions:
                            continue
                        records = download_and_parse_repurchase(
                            filing=filing,
                            cache_dir=repurchase_cache,
                            user_agent=self.config.user_agent,
                            sector=company.sector,
                        )
                        # Aggregate by year_month: some filings report the same
                        # month twice (e.g. open-market + tax-withholding rows)
                        agg: dict[str, MonthlyRepurchase] = {}
                        for rec in records:
                            ym = rec.year_month
                            if ym not in agg:
                                agg[ym] = rec
                            else:
                                prev = agg[ym]
                                total_shares = prev.shares + rec.shares
                                total_vol    = prev.financial_volume + rec.financial_volume
                                wp = prev.price_avg * prev.shares + rec.price_avg * rec.shares
                                avg_price = wp / total_shares if total_shares else 0.0
                                agg[ym] = MonthlyRepurchase(
                                    year_month=ym,
                                    shares=total_shares,
                                    price_avg=round(avg_price, 4),
                                    financial_volume=round(total_vol, 2),
                                    accession_number=prev.accession_number,
                                    cik=prev.cik,
                                    ticker=prev.ticker,
                                    company_alias=prev.company_alias,
                                    sector=prev.sector,
                                    form_type=prev.form_type,
                                    filing_date=prev.filing_date,
                                )
                        for rec in agg.values():
                            self._ingest_repurchase_record(rec, filing)
                        self.seen_accessions.add(repur_key)
                        if agg:
                            changed = True
                except Exception:  # noqa: BLE001
                    pass

            if changed or self.bootstrap_mode:
                with self.lock:
                    self.movements = self.document_store.get_movements()
                    self.history_documents = self.document_store.get_history_documents()
                _save_seen(self.config.state_file, self.seen_accessions)

            if self.bootstrap_mode:
                self.bootstrap_mode = False

            with self.lock:
                self.last_success_at = timestamp
                self.last_error = None

        except Exception as exc:  # noqa: BLE001
            with self.lock:
                self.last_error = str(exc)

    def _ingest_filing(self, filing: Form4Filing, sector: str) -> None:
        """Download, parse, and store a single Form 4 filing."""
        try:
            xml_path = download_form4_xml(
                filing,
                self.config.filings_dir,
                self.config.user_agent,
            )
            movements = parse_form4_xml(xml_path, filing, sector=sector)
        except Exception as exc:  # noqa: BLE001
            movements = []

        # Build a synthetic document record (mirrors CVM document structure)
        document: dict[str, Any] = {
            "Protocolo_Entrega": filing.accession_number,
            "market": "US",
            "ticker": filing.ticker,
            "sector": sector,
            "company_alias": filing.company_alias,
            "Nome_Companhia": filing.company_alias,
            "Codigo_CVM": "",
            "CNPJ_Companhia": "",
            "Categoria": "SEC Form 4",
            "Tipo": "Form 4 / Statement of Changes in Beneficial Ownership",
            "Especie": "",
            "Assunto": "",
            "document_kind": "consolidada",
            "Data_Referencia": filing.filing_date,
            "Data_Entrega": filing.filing_date,
            "Versao": "1",
            "Link_Download": (
                f"https://www.sec.gov/Archives/edgar/data/{filing.cik}/"
                f"{filing.accession_number.replace('-', '')}/{filing.primary_doc}"
            ),
            "source_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={filing.cik}&type=4",
            "captured_at": datetime.now(UTC).isoformat(),
            "pdf_path": "",
            "pdf_downloaded_at": "",
            "parse_status": "success" if movements else "failed",
            "parse_error": "" if movements else "No non-derivative transactions found",
            "parse_updated_at": datetime.now(UTC).isoformat(),
            "summary": {
                "movement_count": len(movements),
                "buyback_count": 0,
            },
        }
        self.document_store.upsert_document(document)

        movement_dicts = [
            {
                "protocol": m.protocol,
                "market": m.market,
                "ticker": m.ticker,
                "sector": m.sector,
                "company_alias": m.company_alias,
                "document_kind": m.document_kind,
                "holder_name": m.holder_name,
                "holder_role": m.holder_role,
                "holder_group": m.holder_group,
                "asset": m.asset,
                "title_characteristics": m.title_characteristics,
                "intermediary": m.intermediary,
                "operation_type": m.operation_type,
                "operation_day": m.operation_day,
                "quantity": m.quantity,
                "price_avg": m.price_avg,
                "financial_volume": m.financial_volume,
                "initial_quantity": m.initial_quantity,
                "final_quantity": m.final_quantity,
                "details": m.details,
                "no_operations": m.no_operations,
                "is_buyback": m.is_buyback,
                "raw_text": m.raw_text,
            }
            for m in movements
        ]
        self.document_store.replace_movements(filing.accession_number, movement_dicts)


    def _ingest_repurchase_record(
        self, rec: MonthlyRepurchase, filing: RepurchaseFiling
    ) -> None:
        """Store one monthly repurchase record as its own document + movement.

        Each company+month gets a unique protocol so that get_movements() JOIN
        returns the correct reference_date for each month.  If the same month
        appears in both a 10-Q and a 10-K, the later call simply overwrites.
        """
        protocol = f"REPUR:{rec.ticker}:{rec.year_month}"
        accession_clean = rec.accession_number.replace("-", "")
        doc_filename = Path(filing.primary_doc).name

        document: dict[str, Any] = {
            "Protocolo_Entrega": protocol,
            "market": "US",
            "ticker": rec.ticker,
            "sector": rec.sector,
            "company_alias": rec.company_alias,
            "Nome_Companhia": rec.company_alias,
            "Codigo_CVM": "",
            "CNPJ_Companhia": "",
            "Categoria": f"SEC {rec.form_type}",
            "Tipo": f"{rec.form_type} / Issuer Purchases of Equity Securities",
            "Especie": "",
            "Assunto": "",
            "document_kind": "individual",
            # reference_date = the repurchase month → drives analytics grouping
            "Data_Referencia": f"{rec.year_month}-01",
            "Data_Entrega": rec.filing_date,
            "Versao": "1",
            "Link_Download": (
                f"https://www.sec.gov/Archives/edgar/data/{rec.cik}/"
                f"{accession_clean}/{doc_filename}"
            ),
            "source_url": (
                f"https://www.sec.gov/cgi-bin/browse-edgar"
                f"?action=getcompany&CIK={rec.cik}&type={rec.form_type}"
            ),
            "captured_at": datetime.now(UTC).isoformat(),
            "pdf_path": "",
            "pdf_downloaded_at": "",
            "parse_status": "success",
            "parse_error": "",
            "parse_updated_at": datetime.now(UTC).isoformat(),
            "summary": {"movement_count": 1, "buyback_count": 1},
        }
        self.document_store.upsert_document(document)

        movement: dict[str, Any] = {
            "protocol": protocol,
            "market": "US",
            "ticker": rec.ticker,
            "sector": rec.sector,
            "company_alias": rec.company_alias,
            "document_kind": "individual",
            "holder_name": "",
            "holder_role": "",
            "holder_group": "",
            "asset": "Common Stock",
            "title_characteristics": "",
            "intermediary": "",
            "operation_type": "compra",
            "operation_day": None,
            "quantity": rec.shares,
            "price_avg": rec.price_avg,
            "financial_volume": rec.financial_volume,
            "initial_quantity": None,
            "final_quantity": None,
            "details": f"Issuer repurchase from {rec.form_type} {rec.accession_number}",
            "no_operations": 0,
            "is_buyback": 1,
            "raw_text": "",
        }
        self.document_store.replace_movements(protocol, [movement])


def _load_seen(path: Path) -> set[str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return set(data.get("seen_accessions", []))
    except Exception:  # noqa: BLE001
        return set()


def _save_seen(path: Path, seen: set[str]) -> None:
    path.write_text(
        json.dumps({"seen_accessions": sorted(seen)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
