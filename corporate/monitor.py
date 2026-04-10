from __future__ import annotations

import threading
import unicodedata
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from typing import Any

from .config import AppConfig, CompanyFilter
from .cvm_client import download_document_pdf, fetch_documents, fetch_live_documents
from .document_store import DocumentStore
from .exporter import export_workbook
from .notifier import CompositeNotifier
from .pdf_parser import ParseError, parse_cvm_358_pdf
from .storage import SeenDocumentsStore



class CVMMonitor:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.state_store = SeenDocumentsStore(config.state_file)
        self.document_store = DocumentStore(config.documents_db_file)
        self.seen_protocols = self.state_store.load()
        self.alerted_protocols = self.state_store.load_alerted()
        self.bootstrap_mode = not config.state_file.exists() or self.document_store.is_empty()
        self.notifier = CompositeNotifier(config)
        self.lock = threading.Lock()
        self.thread: threading.Thread | None = None
        self.stop_event = threading.Event()
        self.last_check_at: str | None = None
        self.last_success_at: str | None = None
        self.last_error: str | None = None
        self.last_live_error: str | None = None
        self.last_export_at: str | None = None
        self.last_export_path: str | None = None
        self.new_documents: list[dict[str, Any]] = []
        self.recent_documents: list[dict[str, Any]] = self.document_store.get_recent_documents(
            self.config.recent_limit
        )
        self.history_documents: list[dict[str, Any]] = self.document_store.get_history_documents()
        # Load movements from DB on startup — data available immediately if DB is populated
        self.movements: list[dict[str, Any]] = self.document_store.get_movements()

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._run_loop, name="cvm-monitor", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)

    def force_refresh(self) -> None:
        self._refresh_once()

    def get_snapshot(self) -> dict[str, Any]:
        with self.lock:
            counts = self.document_store.get_counts()
            return {
                "companies": [asdict(company) for company in self.config.companies],
                "poll_interval_seconds": self.config.poll_interval_seconds,
                "history_start_date": self.config.history_start_date.isoformat(),
                "last_check_at": self.last_check_at,
                "last_success_at": self.last_success_at,
                "last_error": self.last_error,
                "last_live_error": self.last_live_error,
                "last_export_at": self.last_export_at,
                "new_documents": list(self.new_documents),
                "recent_documents": list(self.recent_documents),
                "history_documents": list(self.history_documents),
                "movements": list(self.movements),
                "tracked_protocols": len(self.seen_protocols),
                "teams_enabled": any(
                    type(notifier).__name__ == "TeamsNotifier" and notifier.is_enabled
                    for notifier in self.notifier.notifiers
                ),
                "email_enabled": any(
                    type(notifier).__name__ == "EmailNotifier" and notifier.is_enabled
                    for notifier in self.notifier.notifiers
                ),
                "export_xlsx_path": str(self.config.export_xlsx_path),
                "last_export_path": self.last_export_path,
                **counts,
            }

    def _run_loop(self) -> None:
        while not self.stop_event.is_set():
            self._refresh_once()
            self.stop_event.wait(self._next_interval())

    def _next_interval(self) -> int:
        """5-min polling on days 5–15 (CVM filing window), daily otherwise."""
        # Calendar days start at 1; the requested "day 0-12" window maps to days 1-12.
        day = datetime.now().day
        if day == 10:
            return 1800
        if day <= 12:
            return 3600
        return 86400

    def _refresh_once(self) -> None:
        timestamp = datetime.now(UTC).isoformat()
        with self.lock:
            self.last_check_at = timestamp

        try:
            # ENET only — fetches exactly the 9 configured companies, no full zip download.
            # Falls back to yearly zip if ENET is unavailable.
            try:
                raw_documents = fetch_live_documents(
                    self.config.companies,
                    self.config.history_start_date,
                    date.today(),
                )
                with self.lock:
                    self.last_live_error = None
            except Exception as live_exc:  # noqa: BLE001
                with self.lock:
                    self.last_live_error = str(live_exc)
                raw_documents = fetch_documents(self._years_to_query())
            filtered = filter_documents(
                raw_documents,
                self.config.companies,
                self.config.history_start_date.isoformat(),
            )
            filtered.sort(key=sort_key, reverse=True)

            processed_new_documents: list[dict[str, Any]] = []
            changed = False
            changed_count = 0
            CHECKPOINT_EVERY = 30  # update in-memory movements every N ingested docs
            for document in filtered:
                protocol = document["Protocolo_Entrega"]
                existing_protocol = self.document_store.find_matching_protocol(
                    protocol,
                    document.get("Link_Download", ""),
                    document.get("company_alias", ""),
                    document.get("document_kind", ""),
                    document.get("Data_Referencia", ""),
                    document.get("Data_Entrega", ""),
                    document.get("Versao", ""),
                )
                if existing_protocol == protocol:
                    self.seen_protocols.add(protocol)
                    continue
                if existing_protocol:
                    self._sync_existing_document(existing_protocol, document)
                    self.seen_protocols.add(existing_protocol)
                    self.seen_protocols.add(protocol)
                    if existing_protocol in self.alerted_protocols:
                        self.alerted_protocols.add(protocol)
                    changed = True
                    changed_count += 1
                    continue

                processed = self._ingest_document(document)
                processed_new_documents.append(processed)
                self.seen_protocols.add(protocol)
                changed = True
                changed_count += 1

                # Checkpoint: expose partial data to the API while the loop is still running
                if changed_count % CHECKPOINT_EVERY == 0:
                    with self.lock:
                        self.movements = self.document_store.get_movements()
                        self.last_success_at = timestamp

            if changed or self.bootstrap_mode:
                self.history_documents = self.document_store.get_history_documents()
                self.recent_documents = self.document_store.get_recent_documents(self.config.recent_limit)
                self.movements = self.document_store.get_movements()
                exported_path = export_workbook(
                    self.history_documents,
                    self.movements,
                    self.config.export_xlsx_path,
                    self.config.parsed_data_file,
                )
                self.last_export_at = datetime.now(UTC).isoformat()
                self.last_export_path = str(exported_path)

            if self.bootstrap_mode:
                self.alerted_protocols.update(self.seen_protocols)
                self.new_documents = []
                self.bootstrap_mode = False
            else:
                self.new_documents = processed_new_documents[: self.config.recent_limit]

            self.state_store.save_state(
                self.seen_protocols,
                self.alerted_protocols,
                self.recent_documents,
            )
            pending_alerts = [
                document
                for document in self.new_documents
                if document["Protocolo_Entrega"] not in self.alerted_protocols
            ]
            delivered_protocols = set(self.notifier.notify_documents(pending_alerts))
            if delivered_protocols:
                self.alerted_protocols.update(delivered_protocols)
                self.state_store.save_state(
                    self.seen_protocols,
                    self.alerted_protocols,
                    self.recent_documents,
                )

            with self.lock:
                self.last_success_at = timestamp
                self.last_error = None
        except Exception as exc:  # noqa: BLE001
            with self.lock:
                self.last_error = str(exc)

    def _ingest_document(self, document: dict[str, Any]) -> dict[str, Any]:
        pdf_path = ""
        pdf_downloaded_at = ""
        parse_status = "pending"
        parse_error = ""
        summary: dict[str, Any] = {}
        movements: list[dict[str, Any]] = []

        try:
            pdf_file, pdf_downloaded_at = download_document_pdf(
                document["Link_Download"],
                document["Protocolo_Entrega"],
                document.get("Versao", "1"),
                self.config.pdf_cache_dir,
            )
            pdf_path = str(pdf_file)
            parsed = parse_cvm_358_pdf(pdf_file, document)
            summary = parsed.summary
            movements = parsed.movements
            parse_status = "success"
        except ParseError as exc:
            parse_status = "failed"
            parse_error = str(exc)
        except Exception as exc:  # noqa: BLE001
            parse_status = "failed"
            parse_error = f"Falha ao processar PDF: {exc}"

        enriched = dict(document)
        enriched.update(
            {
                "pdf_path": pdf_path,
                "pdf_downloaded_at": pdf_downloaded_at,
                "parse_status": parse_status,
                "parse_error": parse_error,
                "parse_updated_at": datetime.now(UTC).isoformat(),
                "summary": summary,
            }
        )
        self.document_store.upsert_document(enriched)
        # Inject document-level fields (ticker, sector, market, company_alias)
        # into each movement, since the PDF parser doesn't set these.
        for m in movements:
            m.setdefault("ticker",        enriched.get("ticker", ""))
            m.setdefault("sector",        enriched.get("sector", ""))
            m.setdefault("market",        enriched.get("market", "BZ"))
            m.setdefault("company_alias", enriched.get("company_alias", ""))
        self.document_store.replace_movements(document["Protocolo_Entrega"], movements)
        return enriched

    def _sync_existing_document(self, existing_protocol: str, document: dict[str, Any]) -> None:
        existing = self.document_store.get_document(existing_protocol)
        if not existing:
            return

        incoming_protocol = document["Protocolo_Entrega"]
        target_protocol = existing_protocol
        if is_synthetic_protocol(existing_protocol) and not is_synthetic_protocol(incoming_protocol):
            self.document_store.reassign_protocol(existing_protocol, incoming_protocol)
            if existing_protocol in self.alerted_protocols:
                self.alerted_protocols.add(incoming_protocol)
            if existing_protocol in self.seen_protocols:
                self.seen_protocols.add(incoming_protocol)
            target_protocol = incoming_protocol

        merged = dict(document)
        merged["Protocolo_Entrega"] = target_protocol
        merged["pdf_path"] = existing.get("pdf_path", "")
        merged["pdf_downloaded_at"] = existing.get("pdf_downloaded_at", "")
        merged["parse_status"] = existing.get("parse_status", "pending")
        merged["parse_error"] = existing.get("parse_error", "")
        merged["parse_updated_at"] = existing.get("parse_updated_at", "")
        merged["summary"] = existing.get("summary", {})
        self.document_store.upsert_document(merged)

    def _years_to_query(self) -> list[int]:
        now = datetime.now()
        return list(range(self.config.history_start_date.year, now.year + 1))

    def _live_start_date(self) -> date:
        candidate = date.today() - timedelta(days=120)
        return max(self.config.history_start_date, candidate)


def filter_documents(
    documents: list[dict[str, str]],
    companies: list[CompanyFilter],
    history_start_date: str,
) -> list[dict[str, Any]]:
    company_index = build_company_index(companies)
    matched: list[dict[str, Any]] = []
    seen_documents: set[str] = set()

    for document in documents:
        protocol = document.get("Protocolo_Entrega", "")
        cvm_code = document.get("Codigo_CVM", "")
        cnpj = document.get("CNPJ_Companhia", "")
        category = document.get("Categoria", "")
        reference_date = document.get("Data_Referencia", "")
        link_download = document.get("Link_Download", "")
        document_kind = classify_document_kind(document)
        company = company_index.get(("cvm_code", cvm_code)) or company_index.get(("cnpj", cnpj))
        document_key = link_download or protocol
        if (
            not company
            or not protocol
            or not document_key
            or document_key in seen_documents
            or not is_art11_category(category)
            or not document_kind
            or (reference_date and reference_date < history_start_date)
        ):
            continue

        enriched = dict(document)
        enriched["company_alias"] = company.name
        enriched["document_kind"] = document_kind
        enriched["market"] = "BZ"
        enriched["sector"] = company.sector or ""
        enriched["ticker"] = company.ticker or ""
        matched.append(enriched)
        seen_documents.add(document_key)

    return matched


def classify_document_kind(document: dict[str, str]) -> str | None:
    type_name = normalize_text(document.get("Tipo", ""))
    if "consolidada" in type_name:
        return "consolidada"
    if "individual" in type_name and "controladas" in type_name:
        return "individual"
    return None


def build_company_index(companies: list[CompanyFilter]) -> dict[tuple[str, str], CompanyFilter]:
    index: dict[tuple[str, str], CompanyFilter] = {}
    for company in companies:
        if company.cvm_code:
            index[("cvm_code", company.cvm_code)] = company
        if company.cnpj:
            index[("cnpj", company.cnpj)] = company
    return index


def sort_key(document: dict[str, Any]) -> tuple[str, str, int, int]:
    priority = 0 if document.get("document_kind") == "consolidada" else 1
    return (
        document.get("Data_Entrega", ""),
        document.get("Data_Referencia", ""),
        -priority,
        int(document.get("Versao", "0") or 0),
    )


def is_art11_category(category: str) -> bool:
    return "valores mobiliarios negociados e detidos" in normalize_text(category)


def normalize_text(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    try:
        text = text.encode("latin-1").decode("utf-8")
    except UnicodeError:
        pass
    stripped = unicodedata.normalize("NFKD", text)
    return "".join(char for char in stripped if not unicodedata.combining(char)).casefold()


def is_synthetic_protocol(protocol: str) -> bool:
    return str(protocol or "").startswith("ENET:")
