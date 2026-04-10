from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from flask import Blueprint, current_app, jsonify, render_template

from .analytics import build_analytics_tables
from .config import load_corporate_config
from .cvm_client import download_document_pdf
from .document_store import DocumentStore
from .market_cap import market_cap_cache
from .monitor import CVMMonitor
from .pdf_parser import parse_cvm_358_pdf

_CONTAMINATED_RE = re.compile(
    r"\d{1,2}\s+[0-9.,]+\s+[0-9.,]+\s+[0-9.,]+\s+vista\b", re.I
)

BASE_DIR = Path(__file__).parent.parent  # tmt-site root

tracker_bp = Blueprint(
    "tracker",
    __name__,
    url_prefix="/tracker",
)


def create_tracker_bp() -> Blueprint:
    config = load_corporate_config(BASE_DIR)
    monitor = CVMMonitor(config)

    @tracker_bp.record_once
    def _startup(state: Any) -> None:
        state.app.config["cvm_monitor"] = monitor
        state.app.config["cvm_market_cap_cache"] = market_cap_cache
        monitor.start()
        # Kick off a background market cap refresh for all tickers at startup
        tickers = [c.ticker for c in config.companies if c.ticker]
        market_cap_cache.refresh_all_background(tickers)

    @tracker_bp.get("/")
    def index():
        return render_template("corporate/index.html")

    @tracker_bp.get("/api/status")
    def api_status():
        mon: CVMMonitor = current_app.config.get("cvm_monitor")
        snapshot = mon.get_snapshot() if mon else {}
        movements = snapshot.get("movements", [])
        companies = snapshot.get("companies", [])

        # Filter movements to only those belonging to currently configured companies
        # (guards against stale DB rows from removed companies sharing the same alias)
        allowed_cvm = {str(c["cvm_code"]) for c in companies if c.get("cvm_code")}
        movements = [m for m in movements if str(m.get("cvm_code", "")) in allowed_cvm]

        # Fetch cached market caps (BRL) for all tickers
        tickers = [c["ticker"] for c in companies if c.get("ticker")]
        mcaps = market_cap_cache.get_batch(tickers)

        # Build unique sorted sector list
        sectors = sorted({c["sector"] for c in companies if c.get("sector")})

        return jsonify({
            "companies": companies,
            "sectors": sectors,
            "analytics": build_analytics_tables(movements, mcaps),
            "last_success_at": snapshot.get("last_success_at"),
            "last_error": snapshot.get("last_error"),
            "documents_total": snapshot.get("documents_total", 0),
            "movements_total": snapshot.get("movements_total", 0),
        })

    @tracker_bp.post("/api/refresh")
    def api_refresh():
        mon: CVMMonitor = current_app.config.get("cvm_monitor")
        if mon:
            mon.force_refresh()
        # Also refresh market caps
        mon2: CVMMonitor = current_app.config.get("cvm_monitor")
        if mon2:
            tickers = [c.ticker for c in mon2.config.companies if c.ticker]
            market_cap_cache.refresh_all_background(tickers)
        return api_status()

    @tracker_bp.post("/api/admin/reparse-pagebreak")
    def api_reparse_pagebreak():
        """Re-parse documents whose movements were corrupted by the PDF page-break
        bug (orphaned 'vista' swallowed into adjacent row prefix).
        Safe to call multiple times — only acts on still-contaminated records."""
        mon: CVMMonitor = current_app.config.get("cvm_monitor")
        store: DocumentStore = mon.document_store if mon else DocumentStore(config.documents_db_file)
        pdf_cache_dir = config.pdf_cache_dir

        # Find contaminated protocols.
        all_movements = store.get_movements()
        protocols = {
            m["protocol"]
            for m in all_movements
            if _CONTAMINATED_RE.search(m.get("details", "") or "")
        }

        if not protocols:
            return jsonify({"results": ["Nothing to do — no contaminated movements found."], "count": 0})

        results = []
        for protocol in sorted(protocols):
            with store._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM documents WHERE protocol = ?", (protocol,)
                ).fetchone()
            if not row:
                results.append(f"SKIP {protocol}: not in DB")
                continue
            doc = dict(row)
            doc["Protocolo_Entrega"] = doc["protocol"]
            doc["Link_Download"] = doc["link_download"]
            doc["Versao"] = doc["version"]
            doc["Data_Referencia"] = doc["reference_date"]
            doc["Data_Entrega"] = doc["delivery_date"]

            pdf_file = Path(doc.get("pdf_path", "")) if doc.get("pdf_path") else None
            if not pdf_file or not pdf_file.exists():
                try:
                    pdf_file, _ = download_document_pdf(
                        doc["Link_Download"], protocol, doc.get("version", "1"), pdf_cache_dir
                    )
                except Exception as exc:
                    results.append(f"FAIL {protocol}: download error — {exc}")
                    continue

            try:
                parsed = parse_cvm_358_pdf(pdf_file, doc)
            except Exception as exc:
                results.append(f"FAIL {protocol}: parse error — {exc}")
                continue

            movements = parsed.movements
            for m in movements:
                m.setdefault("ticker", doc.get("ticker", ""))
                m.setdefault("sector", doc.get("sector", ""))
                m.setdefault("market", doc.get("market", "BZ"))
                m.setdefault("company_alias", doc.get("company_alias", ""))
            store.replace_movements(protocol, movements)
            buybacks = sum(1 for m in movements if m.get("is_buyback"))
            results.append(
                f"OK {protocol} ({doc.get('company_alias','?')} "
                f"{doc.get('reference_date','?')}): "
                f"{len(movements)} movements, {buybacks} buyback rows"
            )

        # Reload movements into the monitor so the API reflects the fix immediately.
        if mon:
            with mon.lock:
                mon.movements = store.get_movements()

        return jsonify({"results": results, "count": len(results)})

    return tracker_bp
