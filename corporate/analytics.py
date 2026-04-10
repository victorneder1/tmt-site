"""
analytics.py — Build buyback and insider trading summary tables.

Market cap enrichment:
  - market_cap (float | None): raw BRL value from Yahoo Finance
  - pct_market_cap (float | None): financial_volume / market_cap * 100
  Both financial_volume (from CVM PDF) and market_cap (from Yahoo Finance .SA)
  are denominated in BRL, so the ratio is dimensionally consistent.
"""
from __future__ import annotations

import unicodedata
from collections import defaultdict
from typing import Any


# Only spot cash transactions qualify — options, lending, swaps/TRS, subscriptions etc. excluded.
BUY_OPERATIONS  = {"compra"}
SELL_OPERATIONS = {"venda"}

# Only these groups represent meaningful insider activity.
# Fiscal Board and Advisory/Technical bodies are excluded.
RELEVANT_INSIDER_ORGANS = {"Controller", "Board of Directors", "Executive Directors"}


def build_analytics_tables(
    movements: list[dict[str, Any]],
    market_caps: dict[str, float] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    buybacks = build_buybacks_executed(movements)

    # Enrich buyback rows with market cap and buyback-as-%-of-market-cap.
    # Both values are in BRL, so the ratio is unit-safe.
    if market_caps:
        for row in buybacks:
            ticker = row.get("ticker", "")
            mcap = market_caps.get(ticker)  # BRL, raw from Yahoo Finance
            row["market_cap"] = mcap
            if mcap and mcap > 0:
                row["pct_market_cap"] = round(
                    row["financial_volume"] / mcap * 100, 4
                )
            else:
                row["pct_market_cap"] = None
    else:
        for row in buybacks:
            row["market_cap"] = None
            row["pct_market_cap"] = None

    buying  = build_insiders_side(movements, BUY_OPERATIONS)
    selling = build_insiders_side(movements, SELL_OPERATIONS)
    buying, selling = _filter_net_zero(buying, selling)

    return {
        "buybacks_executed": buybacks,
        "insiders_buying":   buying,
        "insiders_selling":  selling,
    }


def _latest_protocols(movements: list[dict[str, Any]], document_kind: str) -> set[str]:
    """Return the set of protocols that are the most recently delivered filing for each
    (company, reference_month) pair.  When a company re-files a corrected version with a
    later delivery_date, the earlier protocol is superseded and must be excluded from
    analytics to avoid double-counting."""
    latest: dict[tuple[str, str], tuple[str, str]] = {}  # key → (delivery_date, protocol)
    for m in movements:
        if m.get("document_kind") != document_kind:
            continue
        company = m.get("company_alias", "")
        ref_ym  = year_month(str(m.get("reference_date", "")))
        k       = (company, ref_ym)
        delivery = str(m.get("delivery_date", ""))
        protocol = str(m.get("protocol", ""))
        if k not in latest or delivery > latest[k][0]:
            latest[k] = (delivery, protocol)
    # Collect all protocols that ARE the latest delivery for their period
    latest_deliveries: dict[tuple[str, str], str] = {k: v[0] for k, v in latest.items()}
    result: set[str] = set()
    for m in movements:
        if m.get("document_kind") != document_kind:
            continue
        company  = m.get("company_alias", "")
        ref_ym   = year_month(str(m.get("reference_date", "")))
        delivery = str(m.get("delivery_date", ""))
        if delivery == latest_deliveries.get((company, ref_ym), ""):
            result.add(str(m.get("protocol", "")))
    return result


def build_buybacks_executed(movements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Only count movements from the latest-delivered filing for each (company, reference_month).
    # A company may re-file a corrected version with a later delivery_date; earlier protocols
    # are superseded and must be excluded to avoid double-counting.
    latest = _latest_protocols(movements, "individual")

    grouped: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "company_alias": "",
            "market": "BZ",
            "ticker": "",
            "sector": "",
            "delivery_year_month": "",
            "reference_year_month": "",
            "source_form": "CVM 44 / Movimentacao das Acoes em Tesouraria",
            "shares_reacquired": 0.0,
            "financial_volume": 0.0,
            "weighted_price_numerator": 0.0,
            "trade_count": 0,
        }
    )
    for movement in movements:
        if not is_buyback_trade(movement):
            continue
        if str(movement.get("protocol", "")) not in latest:
            continue
        delivery_ym = year_month(str(movement.get("delivery_date", "")))
        reference_ym = year_month(str(movement.get("reference_date", "")))
        key = (movement.get("company_alias", ""), delivery_ym, reference_ym)
        item = grouped[key]
        quantity = float(movement.get("quantity") or 0)
        financial_volume = float(movement.get("financial_volume") or 0)
        price_avg = float(movement.get("price_avg") or 0)
        item["company_alias"] = movement.get("company_alias", "")
        item["market"] = movement.get("market") or "BZ"
        item["ticker"] = movement.get("ticker") or ""
        item["sector"] = movement.get("sector") or ""
        item["delivery_year_month"] = delivery_ym
        item["reference_year_month"] = reference_ym
        item["shares_reacquired"] += quantity
        item["financial_volume"] += financial_volume
        item["weighted_price_numerator"] += price_avg * quantity
        item["trade_count"] += 1

    return finalize_grouped_rows(grouped.values())


def _trs_paired_keys(movements: list[dict[str, Any]], latest: set[str]) -> set[tuple]:
    """Return (protocol, quantity) pairs where a 'movimentacao' flagged as a
    derivative/TRS settlement co-exists with a spot 'à vista' operation in the
    same filing.  Any 'à vista' row sharing that (protocol, quantity) is synthetic
    and must be excluded from insider-trading analytics."""
    keys: set[tuple] = set()
    for m in movements:
        if m.get("document_kind") != "consolidada":
            continue
        if m.get("no_operations"):
            continue
        if str(m.get("protocol", "")) not in latest:
            continue
        if str(m.get("operation_type", "")).casefold() != "movimentacao":
            continue
        det = _strip_accents(str(m.get("details", "") or "")).casefold()
        if "trs" in det or ("derivativo" in det and "liquidac" in det):
            qty = float(m.get("quantity") or 0)
            if qty > 0:
                keys.add((str(m.get("protocol", "")), qty))
    return keys


def build_insiders_side(
    movements: list[dict[str, Any]],
    operation_types: set[str],
) -> list[dict[str, Any]]:
    latest = _latest_protocols(movements, "consolidada")
    trs_paired = _trs_paired_keys(movements, latest)

    grouped: dict[tuple[str, str, str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "company_alias": "",
            "market": "BZ",
            "ticker": "",
            "sector": "",
            "delivery_year_month": "",
            "reference_year_month": "",
            "source_form": "CVM 358/44 / Posicao Consolidada",
            "organ": "",
            "shares": 0.0,
            "financial_volume": 0.0,
            "weighted_price_numerator": 0.0,
            "trade_count": 0,
            "sum_initial_quantity": 0.0,
        }
    )
    initial_qty_seen: dict[tuple, set[tuple[str, str]]] = defaultdict(set)

    for movement in movements:
        if not is_insider_trade(movement, operation_types):
            continue
        proto = str(movement.get("protocol", ""))
        if proto not in latest:
            continue
        qty = float(movement.get("quantity") or 0)
        if (proto, qty) in trs_paired:
            continue  # synthetic TRS settlement paired with a derivative in same filing
        delivery_ym = year_month(str(movement.get("delivery_date", "")))
        reference_ym = year_month(str(movement.get("reference_date", "")))
        organ = normalize_organ(movement)
        if organ not in RELEVANT_INSIDER_ORGANS:
            continue
        key = (movement.get("company_alias", ""), delivery_ym, reference_ym, organ)
        item = grouped[key]
        quantity = float(movement.get("quantity") or 0)
        financial_volume = float(movement.get("financial_volume") or 0)
        price_avg = float(movement.get("price_avg") or 0)
        item["company_alias"] = movement.get("company_alias", "")
        item["market"] = movement.get("market") or "BZ"
        item["ticker"] = movement.get("ticker") or ""
        item["sector"] = movement.get("sector") or ""
        item["delivery_year_month"] = delivery_ym
        item["reference_year_month"] = reference_ym
        item["organ"] = organ
        item["shares"] += quantity
        item["financial_volume"] += financial_volume
        item["weighted_price_numerator"] += price_avg * quantity
        item["trade_count"] += 1
        # Count initial_quantity once per (protocol, holder) to avoid double-counting
        # when the same person has multiple operation rows in a single filing section,
        # while correctly summing across all members of the organ group.
        holder = str(movement.get("holder_name", "") or "")
        person_key = (proto, holder)
        if person_key not in initial_qty_seen[key]:
            initial_qty_seen[key].add(person_key)
            item["sum_initial_quantity"] += float(movement.get("initial_quantity") or 0)

    rows = finalize_grouped_rows(grouped.values(), shares_key="shares")
    for row in rows:
        siq = float(row.get("sum_initial_quantity") or 0)
        shares = float(row.get("shares") or 0)
        # Only show % when the denominator is plausible: initial holdings must be
        # at least as large as what was traded (selling more than you started with
        # is logically impossible and indicates a parsing mismatch between the
        # balance asset and the traded asset in the PDF).
        row["pct_shares_traded"] = round(shares / siq * 100, 4) if siq > 0 and siq >= shares else None
    return rows


def finalize_grouped_rows(
    rows: Any,
    shares_key: str = "shares_reacquired",
) -> list[dict[str, Any]]:
    finalized: list[dict[str, Any]] = []
    for item in rows:
        quantity = float(item.get(shares_key) or 0)
        weighted_numerator = float(item.pop("weighted_price_numerator", 0) or 0)
        item["avg_price"] = round(weighted_numerator / quantity, 5) if quantity else 0.0
        item[shares_key] = round(quantity, 5)
        item["financial_volume"] = round(float(item.get("financial_volume") or 0), 5)
        finalized.append(item)

    return sorted(
        finalized,
        key=lambda item: (
            item.get("delivery_year_month", ""),
            item.get("company_alias", ""),
            item.get("organ", ""),
        ),
        reverse=True,
    )


def is_buyback_trade(movement: dict[str, Any]) -> bool:
    """Only count genuine spot purchases ('Compra à vista').

    Excluded intentionally:
    - 'movimentacao': covers PLANO PARA OUTORGA DE AÇÕES (stock-grant plans that
      benefit employees, not shareholders), cancellations, derivative settlements, etc.
    - 'bonificação': stock splits / bonus shares (not a cash repurchase).
    - 'subscrição' / 'venda' / 'exercício': subscriptions, sales, option exercises.
    - 'compra' rows whose details contain 'termo' (forward purchase) or derivative
      instruments — these are not open-market spot repurchases.

    Note: we check 'termo' not in details (rather than 'vista' in details) because
    a PDF page break can split 'Compra à' and 'vista' onto different pages, causing
    the details field to contain only 'Compra à' without 'vista'. Excluding 'a termo'
    is the correct discriminator: any 'compra' that is not a forward is a spot purchase.
    """
    operation_type = str(movement.get("operation_type", "")).casefold()
    details = _strip_accents(str(movement.get("details", "") or "")).casefold()
    return (
        movement.get("document_kind") == "individual"
        and int(movement.get("is_buyback") or 0) == 1
        and int(movement.get("no_operations") or 0) == 0
        and movement.get("quantity") is not None
        and float(movement.get("quantity") or 0) > 0
        and operation_type == "compra"
        and "termo" not in details
    )


def is_insider_trade(movement: dict[str, Any], operation_types: set[str]) -> bool:
    """Only count spot equity transactions on Ações/Units of the filing company itself.

    Excluded intentionally:
    - Options, swaps (TRS), lending, subscriptions, bonus shares.
    - Any operation not labelled 'à vista' (e.g. term purchases 'a termo').
    - Non-equity assets: CRI, CRA, debentures, bônus, warrants, etc.
    - Movements from "Denominação da Controlada" sections, i.e. transactions in
      shares of a SUBSIDIARY company reported within the parent's filing.
      Detection: when the PDF section header is "Controlada" (not "Companhia"),
      pdf_parser cannot match "Denominação da Companhia" so holder_name falls
      back to company_alias.  The parent's own sections always yield holder_name
      equal to the formal legal name from the PDF (e.g. "COSAN S.A."), which
      never matches the short company_alias in companies.json (e.g. "Cosan").
    """
    operation_type = str(movement.get("operation_type", "")).casefold()
    details = _strip_accents(str(movement.get("details", "") or "")).casefold()
    asset   = _strip_accents(str(movement.get("asset",   "") or "")).casefold()

    # Exclude subsidiary sections: holder_name == company_alias is the fingerprint
    # of a "Denominação da Controlada" section (see docstring above).
    holder_name   = str(movement.get("holder_name",   "") or "")
    company_alias = str(movement.get("company_alias", "") or "")
    if holder_name and company_alias and holder_name == company_alias:
        return False

    # Asset must be equity: "Ação *" / "Ações *" / "Units *"
    first_token = asset.split()[0] if asset.split() else ""
    asset_ok = first_token in {"acao", "acoes", "units", "unit"}

    return (
        movement.get("document_kind") == "consolidada"
        and int(movement.get("no_operations") or 0) == 0
        and operation_type in operation_types
        and "vista" in details          # excludes options, forwards, lending, TRS
        and asset_ok                    # excludes debentures, CRI/CRA, warrants, etc.
        and movement.get("quantity") is not None
    )


def _filter_net_zero(
    buying:  list[dict[str, Any]],
    selling: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Remove company/month pairs where total buy volume == total sell volume
    AND total buy shares == total sell shares (operations that fully net out,
    e.g. a group simultaneously buying and selling the same position)."""
    buy_vol: dict[tuple, float]   = defaultdict(float)
    sell_vol: dict[tuple, float]  = defaultdict(float)
    buy_shr: dict[tuple, float]   = defaultdict(float)
    sell_shr: dict[tuple, float]  = defaultdict(float)

    for row in buying:
        k = (row.get("company_alias", ""), row.get("reference_year_month", ""))
        buy_vol[k] += float(row.get("financial_volume") or 0)
        buy_shr[k] += float(row.get("shares") or 0)

    for row in selling:
        k = (row.get("company_alias", ""), row.get("reference_year_month", ""))
        sell_vol[k] += float(row.get("financial_volume") or 0)
        sell_shr[k] += float(row.get("shares") or 0)

    net_zero: set[tuple] = {
        k for k in buy_vol
        if k in sell_vol
        and round(buy_vol[k], 2) == round(sell_vol[k], 2)
        and round(buy_shr[k], 2) == round(sell_shr[k], 2)
    }

    return (
        [r for r in buying  if (r.get("company_alias", ""), r.get("reference_year_month", "")) not in net_zero],
        [r for r in selling if (r.get("company_alias", ""), r.get("reference_year_month", "")) not in net_zero],
    )


def _strip_accents(text: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(ch)
    )


_ORGAN_TRANSLATION: dict[str, str] = {
    "controlador":                     "Controller",
    "diretoria":                       "Executive Directors",
    "conselho administracao":          "Board of Directors",
    "conselho de administracao":       "Board of Directors",
    "conselho fiscal":                 "Fiscal Board",
    "orgaos tecnicos ou consultivos":  "Advisory",
    "director":                        "Board of Directors",
    "officer":                         "Executive Directors",
    "10% owner":                       "Controller",
    "other":                           "Other",
}


def normalize_organ(movement: dict[str, Any]) -> str:
    raw = (
        str(movement.get("holder_group") or "").strip()
        or str(movement.get("holder_role") or "").strip()
        or str(movement.get("holder_name") or "").strip()
        or "Not Informed"
    )
    collapsed = " ".join(raw.split())
    lower = _strip_accents(collapsed).casefold()
    lower_nospace = lower.replace(" ", "")

    if lower in _ORGAN_TRANSLATION:
        return _ORGAN_TRANSLATION[lower]
    for pt, en in _ORGAN_TRANSLATION.items():
        if lower_nospace == pt.replace(" ", ""):
            return en
    for pt, en in _ORGAN_TRANSLATION.items():
        if lower.startswith(pt) or pt in lower:
            return en
    return collapsed


def year_month(value: str) -> str:
    return value[:7] if len(value) >= 7 else ""
