"""
SEC repurchase parser — extracts monthly share repurchase data from 10-Q, 10-K and 20-F filings.

Section targeted:
  • 10-Q / 10-K: "ISSUER PURCHASES OF EQUITY SECURITIES" (Part II, Item 2)
  • 20-F: "PURCHASES OF EQUITY SECURITIES BY THE ISSUER AND AFFILIATED PURCHASERS"

Each filing's repurchase table typically has one row per month with columns:
  Period | Total Shares Purchased | Average Price Paid | Shares Under Program | Max Value Remaining
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup, Tag

_RATE_SLEEP = 0.15

EDGAR_ARCHIVES = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{doc}"

REPURCHASE_FORM_TYPES = {"10-Q", "10-K", "20-F"}

# Section headings (normalised lowercase)
_SECTION_KEYWORDS = [
    "issuer purchases of equity securities",
    "purchases of equity securities by the issuer and affiliated purchasers",
    "purchases of equity securities by the issuer",
    "repurchases of equity securities",
    "purchase of equity securities",
]

_MONTH_TO_NUM: dict[str, int] = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


@dataclass(slots=True)
class RepurchaseFiling:
    accession_number: str
    primary_doc: str
    form_type: str
    filing_date: str
    cik: str
    ticker: str
    company_alias: str


@dataclass(slots=True)
class MonthlyRepurchase:
    year_month: str          # "2025-01"
    shares: float
    price_avg: float
    financial_volume: float
    accession_number: str
    cik: str
    ticker: str
    company_alias: str
    sector: str
    form_type: str
    filing_date: str


# ── Period parsing ─────────────────────────────────────────────────────────

# Phrases that appear as row labels in financial statements, not repurchase periods
_PERIOD_REJECT_PREFIXES = (
    "at ", "as of ", "for the ", "net ", "total", "share-", "exercise",
    "cancell", "transact", "other comp", "balance", "inception",
)

# A date range must contain one of these separators to be accepted as a period row
_RANGE_SEPARATORS = ("\u2013", "\u2014", " - ", " to ", " through ", "/")


def _parse_year_month(text: str) -> str | None:
    """Try to extract a YYYY-MM from a repurchase period description cell.

    Rejects accounting balance-sheet date labels like "At January 1, 2023" or
    "Net loss for the year" that appear in financial-statement tables mistakenly
    picked up after a repurchase heading.
    """
    t = text.strip()
    t_lower = t.lower()

    # Reject common non-period row labels
    if any(t_lower.startswith(p) for p in _PERIOD_REJECT_PREFIXES):
        return None

    # "01/01/2025" or "01/01/2025 – 01/31/2025" → use first date's month
    m = re.search(r'\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})', t)
    if m:
        # Accept if it looks like a range (has range separator or is date-only)
        is_range = any(sep in t for sep in _RANGE_SEPARATORS)
        is_date_only = bool(re.fullmatch(r'\s*\d{1,2}[/-]\d{1,2}[/-]\d{4}\s*', t))
        if is_range or is_date_only:
            return f"{m.group(3)}-{int(m.group(1)):02d}"

    # "January 1 – January 31, 2025" → use LAST month+year combo (end of period)
    matches = re.findall(
        r'(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
        r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
        r'[.,]?\s+\d{0,2}[,.]?\s*(\d{4})',
        t, re.I,
    )
    if matches:
        # Only accept if the cell contains a range separator
        if any(sep in t for sep in _RANGE_SEPARATORS):
            month_name, year = matches[-1]
            mn = _MONTH_TO_NUM.get(month_name[:3].lower())
            if mn:
                return f"{year}-{mn:02d}"

    # "Month YYYY" with no day — always valid as a period cell
    m = re.search(
        r'^\s*(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
        r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
        r'\s+(\d{4})\s*$',
        t, re.I,
    )
    if m:
        mn = _MONTH_TO_NUM.get(m.group(1)[:3].lower())
        if mn:
            return f"{m.group(2)}-{mn:02d}"

    return None


def _parse_number(text: str) -> float:
    """'1,234,567' → 1234567.0, '—'/'-'/'N/A' → 0."""
    t = re.sub(r'[,$\s\xa0\u2014\u2013]', '', text.strip())
    if not t or t in ('-', '—', 'N/A', 'nil', 'none', '—'):
        return 0.0
    try:
        return float(t)
    except ValueError:
        return 0.0


# ── Heading detection ──────────────────────────────────────────────────────

def _is_repurchase_heading(text: str) -> bool:
    lower = text.lower().strip()
    # Must be reasonably short (not a long paragraph)
    if len(lower) > 300:
        return False
    return any(kw in lower for kw in _SECTION_KEYWORDS)


def _is_toc_heading(tag: Tag) -> bool:
    """Return True if this tag is a table-of-contents entry (contains a hyperlink)."""
    return bool(tag.find("a", href=True))


def _parse_repurchase_table(table: Tag) -> list[dict[str, Any]]:
    """Parse the repurchase HTML table into a list of monthly records.

    SEC filings often insert empty 'spacer' columns between logical columns,
    so we skip empty cells and bare '$' symbols when extracting values.
    """
    records: list[dict[str, Any]] = []

    for row in table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        first_text = cells[0].get_text(separator=" ", strip=True)
        ym = _parse_year_month(first_text)
        if not ym:
            continue  # header or totals row

        # Collect non-empty, non-currency-symbol values from the remaining cells
        values: list[str] = []
        for cell in cells[1:]:
            txt = cell.get_text(separator=" ", strip=True)
            if txt and txt not in ("$", "USD", "Ps.", "MXN"):
                values.append(txt)

        if len(values) < 1:
            continue

        shares = _parse_number(values[0])
        price  = _parse_number(values[1]) if len(values) > 1 else 0.0

        if shares <= 0:
            continue

        records.append({
            "year_month":       ym,
            "shares":           shares,
            "price_avg":        price,
            "financial_volume": round(shares * price, 2),
        })

    return records


def _find_repurchase_table(soup: BeautifulSoup) -> Tag | None:
    """Walk the document tree looking for the repurchase section heading,
    then return the first <table> that follows it and contains date-like rows.

    Skips table-of-contents entries (headings whose text comes from a hyperlink).
    If the first table after a heading has no parseable records, tries subsequent
    tables until another major heading is encountered.
    """
    tags = soup.find_all(
        lambda t: t.name in ("h1", "h2", "h3", "h4", "h5", "p", "div", "td", "b", "strong", "span")
        and _is_repurchase_heading(t.get_text(separator=" ", strip=True))
        and not _is_toc_heading(t)
    )

    for heading_tag in tags:
        for sibling in heading_tag.find_all_next(["table", "h1", "h2", "h3", "h4"]):
            if sibling.name == "table":
                # Validate: must have at least one parseable date row
                if _parse_repurchase_table(sibling):
                    return sibling  # type: ignore[return-value]
                # Otherwise keep looking past this table
            elif sibling.name in ("h1", "h2", "h3", "h4"):
                break  # hit another major heading — give up on this candidate

    return None


# ── Filing list ────────────────────────────────────────────────────────────

def get_repurchase_filings(
    cik: str,
    ticker: str,
    company_alias: str,
    since_date: date,
    user_agent: str,
    submissions_cache: Path | None = None,
) -> list[RepurchaseFiling]:
    """Return all 10-Q / 10-K / 20-F filings since since_date."""
    from .sec_client import SUBMISSIONS_URL, load_ticker_to_cik_map

    cik_padded = str(cik).lstrip("0").zfill(10)
    url = SUBMISSIONS_URL.format(cik=cik_padded)

    # Try cache to avoid re-downloading submissions JSON every call
    cache_key = submissions_cache / f"submissions_{cik}.json" if submissions_cache else None
    raw: bytes | None = None
    if cache_key and cache_key.exists():
        try:
            import json as _json
            age = (date.today() - date.fromtimestamp(cache_key.stat().st_mtime)).days
            if age < 1:  # re-use same-day cache
                raw = cache_key.read_bytes()
        except Exception:
            pass

    if raw is None:
        req = Request(url, headers={"User-Agent": user_agent})
        time.sleep(_RATE_SLEEP)
        with urlopen(req, timeout=30) as resp:
            raw = resp.read()
        if cache_key:
            cache_key.parent.mkdir(parents=True, exist_ok=True)
            cache_key.write_bytes(raw)

    import json as _json
    data = _json.loads(raw.decode("utf-8"))
    recent = data.get("filings", {}).get("recent", {})
    forms   = recent.get("form", [])
    dates   = recent.get("filingDate", [])
    accs    = recent.get("accessionNumber", [])
    docs    = recent.get("primaryDocument", [])

    filings: list[RepurchaseFiling] = []
    for form, fdate, acc, doc in zip(forms, dates, accs, docs):
        if form not in REPURCHASE_FORM_TYPES:
            continue
        if fdate < since_date.isoformat():
            continue
        filings.append(RepurchaseFiling(
            accession_number=acc,
            primary_doc=doc,
            form_type=form,
            filing_date=fdate,
            cik=cik,
            ticker=ticker,
            company_alias=company_alias,
        ))

    # Older filings may be in extra pages of submissions
    for file_info in data.get("filings", {}).get("files", []):
        fname = file_info.get("name", "")
        if not fname.endswith(".json"):
            continue
        oldest = file_info.get("filingFrom", "9999-99")
        if oldest > since_date.isoformat():
            # Potentially overlapping range, fetch it
            extra_url = f"https://data.sec.gov/submissions/{fname}"
            try:
                req2 = Request(extra_url, headers={"User-Agent": user_agent})
                time.sleep(_RATE_SLEEP)
                with urlopen(req2, timeout=30) as r:
                    extra = _json.loads(r.read().decode("utf-8"))
                e_forms  = extra.get("form", [])
                e_dates  = extra.get("filingDate", [])
                e_accs   = extra.get("accessionNumber", [])
                e_docs   = extra.get("primaryDocument", [])
                for form, fdate, acc, doc in zip(e_forms, e_dates, e_accs, e_docs):
                    if form not in REPURCHASE_FORM_TYPES:
                        continue
                    if fdate < since_date.isoformat():
                        continue
                    filings.append(RepurchaseFiling(
                        accession_number=acc,
                        primary_doc=doc,
                        form_type=form,
                        filing_date=fdate,
                        cik=cik,
                        ticker=ticker,
                        company_alias=company_alias,
                    ))
            except Exception:
                pass

    return filings


# ── Download + parse ───────────────────────────────────────────────────────

def download_and_parse_repurchase(
    filing: RepurchaseFiling,
    cache_dir: Path,
    user_agent: str,
    sector: str = "",
) -> list[MonthlyRepurchase]:
    """Download the filing HTML and extract monthly repurchase records."""
    accession_clean = filing.accession_number.replace("-", "")
    filing_dir = cache_dir / accession_clean
    filing_dir.mkdir(parents=True, exist_ok=True)

    # Use just the filename (strip any XSL prefix as in Form 4 case)
    doc_filename = Path(filing.primary_doc).name
    local_path = filing_dir / doc_filename

    if not local_path.exists():
        url = EDGAR_ARCHIVES.format(
            cik=filing.cik,
            accession=accession_clean,
            doc=doc_filename,
        )
        req = Request(url, headers={"User-Agent": user_agent})
        time.sleep(_RATE_SLEEP)
        try:
            with urlopen(req, timeout=45) as resp:
                local_path.write_bytes(resp.read())
        except Exception as exc:
            raise RuntimeError(f"Could not download {url}: {exc}") from exc

    html = local_path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(html, "html.parser")

    table = _find_repurchase_table(soup)
    if table is None:
        return []

    raw_records = _parse_repurchase_table(table)
    results: list[MonthlyRepurchase] = []
    for rec in raw_records:
        results.append(MonthlyRepurchase(
            year_month=rec["year_month"],
            shares=rec["shares"],
            price_avg=rec["price_avg"],
            financial_volume=rec["financial_volume"],
            accession_number=filing.accession_number,
            cik=filing.cik,
            ticker=filing.ticker,
            company_alias=filing.company_alias,
            sector=sector,
            form_type=filing.form_type,
            filing_date=filing.filing_date,
        ))

    return results
