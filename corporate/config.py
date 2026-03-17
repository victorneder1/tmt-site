from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


DEFAULT_POLL_INTERVAL_SECONDS = 60
DEFAULT_RECENT_LIMIT = 100
DEFAULT_COMPANIES_FILE = "companies_bz.json"
DEFAULT_STATE_FILE = "data/corporate_state.json"
DEFAULT_LOCAL_SETTINGS_FILE = "settings.local.json"
DEFAULT_HISTORY_START_DATE = "2024-01-01"
DEFAULT_DOCUMENTS_DB_FILE = "data/corporate_bz.db"
DEFAULT_PARSED_DATA_FILE = "data/corporate_parsed.json"
DEFAULT_PDF_CACHE_DIR = "data/pdfs"
DEFAULT_EXPORT_XLSX_PATH = "data/exports/corporate_bz_monitor.xlsx"


@dataclass(slots=True)
class SmtpConfig:
    host: str | None
    port: int
    user: str | None
    password: str | None
    from_email: str | None
    to_emails: list[str]

    @property
    def is_enabled(self) -> bool:
        return bool(self.host and self.from_email and self.to_emails)


@dataclass(slots=True)
class CompanyFilter:
    name: str
    cvm_code: str | None = None
    cnpj: str | None = None
    sector: str | None = None
    ticker: str | None = None


@dataclass(slots=True)
class AppConfig:
    companies: list[CompanyFilter]
    poll_interval_seconds: int
    recent_limit: int
    history_start_date: date
    state_file: Path
    documents_db_file: Path
    parsed_data_file: Path
    pdf_cache_dir: Path
    export_xlsx_path: Path
    teams_webhook_url: str | None
    smtp: SmtpConfig


def load_corporate_config(base_dir: Path) -> AppConfig:
    local_settings = load_local_settings(base_dir / DEFAULT_LOCAL_SETTINGS_FILE)
    companies_path = Path(os.getenv("CORPORATE_COMPANIES_FILE", base_dir / DEFAULT_COMPANIES_FILE))
    state_file = Path(os.getenv("CORPORATE_STATE_FILE", base_dir / DEFAULT_STATE_FILE))
    documents_db_file = Path(os.getenv("CORPORATE_DOCUMENTS_DB_FILE", base_dir / DEFAULT_DOCUMENTS_DB_FILE))
    parsed_data_file = Path(os.getenv("CORPORATE_PARSED_DATA_FILE", base_dir / DEFAULT_PARSED_DATA_FILE))
    pdf_cache_dir = Path(os.getenv("CORPORATE_PDF_CACHE_DIR", base_dir / DEFAULT_PDF_CACHE_DIR))
    export_xlsx_path = Path(os.getenv("CORPORATE_EXPORT_XLSX_PATH", base_dir / DEFAULT_EXPORT_XLSX_PATH))
    poll_interval_seconds = int(os.getenv("CORPORATE_POLL_INTERVAL_SECONDS", DEFAULT_POLL_INTERVAL_SECONDS))
    recent_limit = int(os.getenv("CORPORATE_RECENT_LIMIT", DEFAULT_RECENT_LIMIT))
    history_start_date = date.fromisoformat(
        str(os.getenv("CORPORATE_HISTORY_START_DATE", local_settings.get("history_start_date", DEFAULT_HISTORY_START_DATE)))
    )
    teams_webhook_url = str(
        os.getenv("TEAMS_WEBHOOK_URL", local_settings.get("teams_webhook_url", ""))
    ).strip() or None
    smtp = load_smtp_config(local_settings)

    companies = load_companies(companies_path)
    return AppConfig(
        companies=companies,
        poll_interval_seconds=poll_interval_seconds,
        recent_limit=recent_limit,
        history_start_date=history_start_date,
        state_file=state_file,
        documents_db_file=documents_db_file,
        parsed_data_file=parsed_data_file,
        pdf_cache_dir=pdf_cache_dir,
        export_xlsx_path=export_xlsx_path,
        teams_webhook_url=teams_webhook_url,
        smtp=smtp,
    )


def load_local_settings(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("O arquivo settings.local.json precisa conter um objeto JSON.")
    return payload


def load_smtp_config(local_settings: dict[str, Any]) -> SmtpConfig:
    smtp_settings = local_settings.get("smtp", {})
    if not isinstance(smtp_settings, dict):
        smtp_settings = {}

    recipients = os.getenv("SMTP_TO", smtp_settings.get("to", ""))
    if isinstance(recipients, str):
        to_emails = [item.strip() for item in recipients.split(",") if item.strip()]
    elif isinstance(recipients, list):
        to_emails = [str(item).strip() for item in recipients if str(item).strip()]
    else:
        to_emails = []

    return SmtpConfig(
        host=str(os.getenv("SMTP_HOST", smtp_settings.get("host", ""))).strip() or None,
        port=int(os.getenv("SMTP_PORT", smtp_settings.get("port", 587)) or 587),
        user=str(os.getenv("SMTP_USER", smtp_settings.get("user", ""))).strip() or None,
        password=str(os.getenv("SMTP_PASSWORD", smtp_settings.get("password", ""))).strip() or None,
        from_email=str(os.getenv("SMTP_FROM", smtp_settings.get("from", ""))).strip() or None,
        to_emails=to_emails,
    )


def load_companies(path: Path) -> list[CompanyFilter]:
    if not path.exists():
        raise FileNotFoundError(
            f"Arquivo de empresas nao encontrado em {path}."
        )

    raw = json.loads(path.read_text(encoding="utf-8"))
    companies: list[CompanyFilter] = []
    for item in raw:
        normalized = normalize_company_item(item)
        companies.append(
            CompanyFilter(
                name=normalized["name"],
                cvm_code=normalized.get("cvm_code"),
                cnpj=normalized.get("cnpj"),
                sector=normalized.get("sector"),
                ticker=normalized.get("ticker"),
            )
        )

    if not companies:
        raise ValueError("A lista de empresas monitoradas esta vazia.")
    return companies


def normalize_company_item(item: Any) -> dict[str, str]:
    if not isinstance(item, dict):
        raise ValueError("Cada empresa precisa ser um objeto JSON.")

    name = str(item.get("name", "")).strip()
    cvm_code = str(item.get("cvm_code", "")).strip()
    cnpj = str(item.get("cnpj", "")).strip()
    sector = str(item.get("sector", "TMT")).strip()
    ticker = str(item.get("ticker", "")).strip()

    if not name:
        raise ValueError("Cada empresa precisa de um campo 'name'.")

    return {
        "name": name,
        "cvm_code": cvm_code or None,
        "cnpj": cnpj or None,
        "sector": sector or "TMT",
        "ticker": ticker or None,
    }
