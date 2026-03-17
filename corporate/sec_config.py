from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


DEFAULT_SEC_POLL_INTERVAL_SECONDS = 600  # 10 minutes
DEFAULT_SEC_COMPANIES_FILE = "companies_us.json"
DEFAULT_SEC_STATE_FILE = "data/corporate_us_state.json"
DEFAULT_SEC_DB_FILE = "data/corporate_us.db"
DEFAULT_SEC_FILINGS_DIR = "data/sec_filings"
DEFAULT_SEC_TICKER_CIK_CACHE = "data/sec_ticker_cik.json"
DEFAULT_SEC_HISTORY_START_DATE = "2024-01-01"
SEC_USER_AGENT = "BTG Pactual Research victor.neder@btgpactual.com"


@dataclass(slots=True)
class SecCompany:
    name: str
    ticker: str
    sector: str = "TMT"
    cik: str = ""  # may be pre-set from config or resolved at runtime from EDGAR


@dataclass(slots=True)
class SecConfig:
    companies: list[SecCompany]
    poll_interval_seconds: int
    history_start_date: date
    state_file: Path
    db_file: Path
    filings_dir: Path
    ticker_cik_cache: Path
    user_agent: str


def load_sec_config(base_dir: Path) -> SecConfig:
    local_settings = _load_local_settings(base_dir / "settings.local.json")
    sec_local = local_settings.get("sec", {}) if isinstance(local_settings.get("sec"), dict) else {}

    companies_path = Path(os.getenv("SEC_COMPANIES_FILE", base_dir / DEFAULT_SEC_COMPANIES_FILE))
    state_file = Path(os.getenv("SEC_STATE_FILE", base_dir / DEFAULT_SEC_STATE_FILE))
    db_file = Path(os.getenv("SEC_DB_FILE", base_dir / DEFAULT_SEC_DB_FILE))
    filings_dir = Path(os.getenv("SEC_FILINGS_DIR", base_dir / DEFAULT_SEC_FILINGS_DIR))
    ticker_cik_cache = Path(os.getenv("SEC_TICKER_CIK_CACHE", base_dir / DEFAULT_SEC_TICKER_CIK_CACHE))
    poll_interval_seconds = int(os.getenv(
        "SEC_POLL_INTERVAL_SECONDS",
        sec_local.get("poll_interval_seconds", DEFAULT_SEC_POLL_INTERVAL_SECONDS),
    ))
    history_start_date = date.fromisoformat(
        str(os.getenv(
            "SEC_HISTORY_START_DATE",
            sec_local.get("history_start_date", DEFAULT_SEC_HISTORY_START_DATE),
        ))
    )
    user_agent = str(os.getenv("SEC_USER_AGENT", sec_local.get("user_agent", SEC_USER_AGENT)))

    companies = _load_sec_companies(companies_path)
    return SecConfig(
        companies=companies,
        poll_interval_seconds=poll_interval_seconds,
        history_start_date=history_start_date,
        state_file=state_file,
        db_file=db_file,
        filings_dir=filings_dir,
        ticker_cik_cache=ticker_cik_cache,
        user_agent=user_agent,
    )


def _load_local_settings(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _load_sec_companies(path: Path) -> list[SecCompany]:
    if not path.exists():
        raise FileNotFoundError(f"US companies file not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    companies: list[SecCompany] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        ticker = str(item.get("ticker", "")).strip().upper()
        sector = str(item.get("sector", "TMT")).strip()
        # Accept hardcoded CIK from JSON — strip leading zeros for numeric form
        cik_raw = str(item.get("cik", "")).strip()
        cik = cik_raw.lstrip("0") if cik_raw else ""
        if name and ticker:
            companies.append(SecCompany(name=name, ticker=ticker, sector=sector, cik=cik))
    if not companies:
        raise ValueError("US companies list is empty.")
    return companies
