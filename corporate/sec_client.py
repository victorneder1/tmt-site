from __future__ import annotations

import json
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
EDGAR_ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{doc}"

_RATE_LIMIT_SLEEP = 0.11  # ~9 req/s, well under SEC's 10 req/s limit


@dataclass
class Form4Filing:
    accession_number: str   # e.g. "0001234567-24-000123"
    filing_date: str        # ISO date "YYYY-MM-DD"
    primary_doc: str        # filename of primary XML document
    cik: str
    ticker: str
    company_alias: str


@dataclass
class Form4Movement:
    protocol: str           # accession_number used as protocol key
    market: str = "US"
    ticker: str = ""
    sector: str = ""
    company_alias: str = ""
    document_kind: str = "consolidada"  # insider = consolidada
    holder_name: str = ""
    holder_role: str = ""
    holder_group: str = ""
    asset: str = ""
    title_characteristics: str = ""
    intermediary: str = ""
    operation_type: str = ""    # "compra" or "venda"
    operation_day: int | None = None
    quantity: float | None = None
    price_avg: float | None = None
    financial_volume: float | None = None
    initial_quantity: float | None = None
    final_quantity: float | None = None
    details: str = ""
    no_operations: int = 0
    is_buyback: int = 0
    raw_text: str = ""


def load_ticker_to_cik_map(cache_path: Path, user_agent: str) -> dict[str, str]:
    """Download (or load from cache) the SEC company_tickers.json and return ticker→padded-CIK."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    # Refresh cache if older than 7 days or absent
    if cache_path.exists():
        age_days = (datetime.now().timestamp() - cache_path.stat().st_mtime) / 86400
        if age_days < 7:
            raw = json.loads(cache_path.read_text(encoding="utf-8"))
            return raw

    req = Request(COMPANY_TICKERS_URL, headers={"User-Agent": user_agent})
    with urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    mapping = {
        v["ticker"].upper(): str(v["cik_str"]).zfill(10)
        for v in data.values()
        if v.get("ticker")
    }
    cache_path.write_text(json.dumps(mapping, ensure_ascii=False), encoding="utf-8")
    return mapping


def fetch_recent_form4_filings(
    cik: str,
    ticker: str,
    company_alias: str,
    since_date: date,
    user_agent: str,
) -> list[Form4Filing]:
    """Fetch recent Form 4 filings for a company from EDGAR submissions JSON."""
    # EDGAR requires 10-digit zero-padded CIK in the filename (e.g. CIK0001851003.json)
    cik_padded = str(cik).lstrip("0").zfill(10)
    url = SUBMISSIONS_URL.format(cik=cik_padded)
    req = Request(url, headers={"User-Agent": user_agent})
    time.sleep(_RATE_LIMIT_SLEEP)
    with urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])

    filings: list[Form4Filing] = []
    for form, filing_date, accession, primary_doc in zip(forms, dates, accessions, primary_docs):
        if form != "4":
            continue
        if filing_date < since_date.isoformat():
            continue
        filings.append(Form4Filing(
            accession_number=accession,
            filing_date=filing_date,
            primary_doc=primary_doc,
            cik=cik,
            ticker=ticker,
            company_alias=company_alias,
        ))

    return filings


def download_form4_xml(filing: Form4Filing, cache_dir: Path, user_agent: str) -> Path:
    """Download a Form 4 XML file to the local cache. Returns the cached file path."""
    accession_no_dashes = filing.accession_number.replace("-", "")
    filing_dir = cache_dir / accession_no_dashes
    filing_dir.mkdir(parents=True, exist_ok=True)

    # primary_doc may contain an XSL subdirectory (e.g. "xslF345X05/form4.xml").
    # Strip the prefix — the raw XML is always at the accession root as the filename.
    xml_filename = Path(filing.primary_doc).name  # e.g. "form4.xml"
    local_path = filing_dir / xml_filename

    if local_path.exists():
        return local_path

    # Fetch the raw XML from the accession root directory, NOT the XSL-styled path.
    url = EDGAR_ARCHIVES_URL.format(
        cik=filing.cik,
        accession=accession_no_dashes,
        doc=xml_filename,
    )
    req = Request(url, headers={"User-Agent": user_agent})
    time.sleep(_RATE_LIMIT_SLEEP)
    with urlopen(req, timeout=30) as resp:
        local_path.write_bytes(resp.read())

    return local_path


def parse_form4_xml(
    xml_path: Path,
    filing: Form4Filing,
    sector: str = "",
) -> list[Form4Movement]:
    """Parse a Form 4 XML file into a list of Form4Movement records."""
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError:
        return []

    root = tree.getroot()
    movements: list[Form4Movement] = []

    # Reporting owner info
    owner_name = _xml_text(root, ".//reportingOwnerName") or _xml_text(root, ".//rptOwnerName") or ""
    is_director = _xml_text(root, ".//isDirector") == "1"
    is_officer = _xml_text(root, ".//isOfficer") == "1"
    officer_title = _xml_text(root, ".//officerTitle") or ""
    is_ten_pct = _xml_text(root, ".//isTenPercentOwner") == "1"

    if is_officer and officer_title:
        holder_role = officer_title
    elif is_director:
        holder_role = "Director"
    elif is_ten_pct:
        holder_role = "10% Owner"
    else:
        holder_role = "Other"

    # Non-derivative transactions (open-market stock purchases/sales)
    for txn in root.findall(".//nonDerivativeTransaction"):
        movement = _parse_non_derivative_txn(
            txn,
            filing=filing,
            owner_name=owner_name,
            holder_role=holder_role,
            sector=sector,
        )
        if movement:
            movements.append(movement)

    return movements


def _parse_non_derivative_txn(
    txn: ET.Element,
    filing: Form4Filing,
    owner_name: str,
    holder_role: str,
    sector: str,
) -> Form4Movement | None:
    security_title = _xml_text(txn, ".//securityTitle/value") or ""
    txn_date = _xml_text(txn, ".//transactionDate/value") or filing.filing_date
    shares_raw = _xml_text(txn, ".//transactionShares/value")
    price_raw = _xml_text(txn, ".//transactionPricePerShare/value")
    acquired_disposed = _xml_text(txn, ".//transactionAcquiredDisposedCode/value") or ""
    txn_code = _xml_text(txn, ".//transactionCode") or ""
    post_shares_raw = _xml_text(txn, ".//sharesOwnedFollowingTransaction/value")

    # Only track open-market buys (P) and sales (S) and grants (A) and disposals (D/F)
    # Skip option exercises (M), withholding (F treated as sell), conversions, etc.
    # We map: A (acquired) → compra, D (disposed) → venda
    if acquired_disposed not in ("A", "D"):
        return None
    if not shares_raw:
        return None

    try:
        quantity = float(shares_raw)
    except (ValueError, TypeError):
        return None

    try:
        price_avg = float(price_raw) if price_raw else None
    except (ValueError, TypeError):
        price_avg = None

    try:
        final_quantity = float(post_shares_raw) if post_shares_raw else None
    except (ValueError, TypeError):
        final_quantity = None

    financial_volume = (quantity * price_avg) if (price_avg is not None) else None
    operation_type = "compra" if acquired_disposed == "A" else "venda"

    # Extract day from transaction date (YYYY-MM-DD → day as int)
    try:
        op_day = int(txn_date[8:10]) if len(txn_date) >= 10 else None
    except (ValueError, TypeError):
        op_day = None

    return Form4Movement(
        protocol=filing.accession_number,
        market="US",
        ticker=filing.ticker,
        sector=sector,
        company_alias=filing.company_alias,
        document_kind="consolidada",
        holder_name=owner_name,
        holder_role=holder_role,
        holder_group=holder_role,
        asset=security_title,
        title_characteristics=txn_code,
        operation_type=operation_type,
        operation_day=op_day,
        quantity=quantity,
        price_avg=price_avg,
        financial_volume=financial_volume,
        final_quantity=final_quantity,
        no_operations=0,
        is_buyback=0,
        raw_text=f"code={txn_code} date={txn_date}",
    )


def _xml_text(element: ET.Element, path: str) -> str | None:
    node = element.find(path)
    if node is not None and node.text:
        return node.text.strip()
    return None
