from __future__ import annotations

import unicodedata
from collections import defaultdict
from typing import Any


BUY_OPERATIONS = {"compra", "subscrição", "subscricao"}
SELL_OPERATIONS = {"venda"}


def build_analytics_tables(movements: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    return {
        "buybacks_executed": build_buybacks_executed(movements),
        "insiders_buying": build_insiders_side(movements, BUY_OPERATIONS),
        "insiders_selling": build_insiders_side(movements, SELL_OPERATIONS),
    }


def build_buybacks_executed(movements: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
        delivery_ym = year_month(str(movement.get("delivery_date", "")))
        reference_ym = year_month(str(movement.get("reference_date", "")))
        key = (movement.get("company_alias", ""), delivery_ym, reference_ym)
        item = grouped[key]
        quantity = float(movement.get("quantity") or 0)
        financial_volume = float(movement.get("financial_volume") or 0)
        price_avg = float(movement.get("price_avg") or 0)
        market = movement.get("market") or "BZ"
        item["company_alias"] = movement.get("company_alias", "")
        item["market"] = market
        item["ticker"] = movement.get("ticker") or ""
        item["sector"] = movement.get("sector") or ""
        item["delivery_year_month"] = delivery_ym
        item["reference_year_month"] = reference_ym
        item["source_form"] = (
            "SEC 10-Q / Share Repurchase" if market == "US"
            else "CVM 44 / Movimentacao das Acoes em Tesouraria"
        )
        item["shares_reacquired"] += quantity
        item["financial_volume"] += financial_volume
        item["weighted_price_numerator"] += price_avg * quantity
        item["trade_count"] += 1

    return finalize_grouped_rows(grouped.values())


def build_insiders_side(
    movements: list[dict[str, Any]],
    operation_types: set[str],
) -> list[dict[str, Any]]:
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
        }
    )
    for movement in movements:
        if not is_insider_trade(movement, operation_types):
            continue
        delivery_ym = year_month(str(movement.get("delivery_date", "")))
        reference_ym = year_month(str(movement.get("reference_date", "")))
        organ = normalize_organ(movement)
        key = (movement.get("company_alias", ""), delivery_ym, reference_ym, organ)
        item = grouped[key]
        quantity = float(movement.get("quantity") or 0)
        financial_volume = float(movement.get("financial_volume") or 0)
        price_avg = float(movement.get("price_avg") or 0)
        market = movement.get("market") or "BZ"
        item["company_alias"] = movement.get("company_alias", "")
        item["market"] = market
        item["ticker"] = movement.get("ticker") or ""
        item["sector"] = movement.get("sector") or ""
        item["delivery_year_month"] = delivery_ym
        item["reference_year_month"] = reference_ym
        item["source_form"] = (
            "SEC Form 4 / Statement of Changes" if market == "US"
            else "CVM 358/44 / Posicao Consolidada"
        )
        item["organ"] = organ
        item["shares"] += quantity
        item["financial_volume"] += financial_volume
        item["weighted_price_numerator"] += price_avg * quantity
        item["trade_count"] += 1

    return finalize_grouped_rows(grouped.values(), shares_key="shares")


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
    operation_type = str(movement.get("operation_type", "")).casefold()
    details = str(movement.get("details", "")).casefold()
    financial_volume = float(movement.get("financial_volume") or 0)
    price_avg = float(movement.get("price_avg") or 0)
    return (
        movement.get("document_kind") == "individual"
        and int(movement.get("is_buyback") or 0) == 1
        and int(movement.get("no_operations") or 0) == 0
        and movement.get("quantity") is not None
        and float(movement.get("quantity") or 0) > 0
        and (
            operation_type in BUY_OPERATIONS
            or "compra" in details
            or (financial_volume > 0 and price_avg > 0)
        )
    )


def is_insider_trade(movement: dict[str, Any], operation_types: set[str]) -> bool:
    return (
        movement.get("document_kind") == "consolidada"
        and int(movement.get("no_operations") or 0) == 0
        and str(movement.get("operation_type", "")).casefold() in operation_types
        and movement.get("quantity") is not None
    )


def _strip_accents(text: str) -> str:
    """Remove diacritical marks so 'ção' == 'cao', 'ã' == 'a', etc."""
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(ch)
    )


# Keys are lowercase + accent-stripped for robust matching
_ORGAN_TRANSLATION: dict[str, str] = {
    # Portuguese CVM groups → English display names
    "controlador":                     "Controller",
    "diretoria":                       "Executive Directors",
    "conselho administracao":          "Board of Directors",
    "conselho de administracao":       "Board of Directors",
    "conselho fiscal":                 "Fiscal Board",
    "orgaos tecnicos ou consultivos":  "Advisory",
    # SEC Form 4 issuer relationship values
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
    # Collapse whitespace (fixes PDF line-break artifacts like "Direto ria")
    collapsed = " ".join(raw.split())
    # Strip accents + casefold — handles encoding variations from PDF/EDGAR
    lower = _strip_accents(collapsed).casefold()
    lower_nospace = lower.replace(" ", "")  # "direto ria" → "diretoria"

    # Exact match
    if lower in _ORGAN_TRANSLATION:
        return _ORGAN_TRANSLATION[lower]
    # Exact match ignoring internal spaces
    for pt, en in _ORGAN_TRANSLATION.items():
        if lower_nospace == pt.replace(" ", ""):
            return en
    # Prefix / substring fallback
    for pt, en in _ORGAN_TRANSLATION.items():
        if lower.startswith(pt) or pt in lower:
            return en
    return collapsed


def year_month(value: str) -> str:
    return value[:7] if len(value) >= 7 else ""
