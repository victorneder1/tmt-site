from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pypdf import PdfReader


class ParseError(RuntimeError):
    pass


@dataclass(slots=True)
class ParsedDocument:
    summary: dict[str, Any]
    movements: list[dict[str, Any]]


def parse_cvm_358_pdf(pdf_path: Path, document: dict[str, Any]) -> ParsedDocument:
    text = extract_pdf_text(pdf_path)
    sections = split_sections(text)
    if not sections:
        raise ParseError("Nao foi possivel identificar secoes no formulario PDF.")

    movements: list[dict[str, Any]] = []
    buyback_count = 0
    for section in sections:
        for entry in split_company_entries(section, document):
            movements.extend(parse_section(entry, document))

    buyback_count = sum(1 for movement in movements if movement["is_buyback"])
    summary = {
        "section_count": len(sections),
        "movement_count": len(movements),
        "buyback_count": buyback_count,
        "has_operations": any(not movement["no_operations"] for movement in movements),
    }
    return ParsedDocument(summary=summary, movements=movements)


def extract_pdf_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    text = "\n".join(page.extract_text() or "" for page in reader.pages).strip()
    if not text:
        raise ParseError("PDF sem texto extraivel.")
    return text


def split_sections(text: str) -> list[str]:
    pattern = re.compile(r"(FORMULÁRIO (?:CONSOLIDADO|INDIVIDUAL).*?)(?=FORMULÁRIO (?:CONSOLIDADO|INDIVIDUAL)|\Z)", re.S)
    sections = [match.group(1).strip() for match in pattern.finditer(text)]
    return sections or [text.strip()]


def split_company_entries(section_text: str, document: dict[str, Any]) -> list[str]:
    if document.get("document_kind") != "individual":
        return [section_text]

    markers = list(re.finditer(r"Denominação da Companhia:", section_text))
    if len(markers) <= 1:
        return [section_text]

    header = section_text[: markers[0].start()].strip()
    entries: list[str] = []
    for index, marker in enumerate(markers):
        end = markers[index + 1].start() if index + 1 < len(markers) else len(section_text)
        entry_body = section_text[marker.start() : end].strip()
        entries.append(f"{header}\n{entry_body}".strip())
    return entries


def parse_section(section_text: str, document: dict[str, Any]) -> list[dict[str, Any]]:
    condensed = normalize_whitespace(section_text)
    company_alias = document.get("company_alias", "")
    document_kind = document.get("document_kind", "")
    company_name = extract_match(condensed, r"Denominação da Companhia:\s*(.+?)(?:Nome:|Grupo e Pessoas|Saldo Inicial)")
    holder_name = extract_match(condensed, r"Nome:\s*(.+?)\s+CPF/CNPJ:")
    holder_role = extract_match(condensed, r"Qualificação:\s*(.+?)\s+Saldo Inicial")
    holder_group = extract_selected_group(condensed) if document_kind == "consolidada" else ""
    balance = extract_balances(condensed)
    has_no_operations = bool(re.search(r"\(\s*X\s*\)\s*não foram realizadas operações", condensed, re.I))
    operation_rows = parse_operations(condensed)

    if not operation_rows:
        inferred_no_operations = has_no_operations or (
            balance.get("initial_quantity") is not None
            and balance.get("final_quantity") is not None
            and balance.get("initial_quantity") == balance.get("final_quantity")
        )
        operation_rows = [
            {
                "asset": balance.get("initial_asset") or balance.get("final_asset") or "",
                "title_characteristics": "",
                "intermediary": "",
                "operation_type": "sem_operacoes" if has_no_operations else "nao_identificado",
                "operation_day": None,
                "quantity": None,
                "price_avg": None,
                "financial_volume": None,
                "details": "Secao sem movimentacao detalhada no PDF.",
                "raw_text": condensed,
                "no_operations": 1 if inferred_no_operations else 0,
            }
        ]

    movements: list[dict[str, Any]] = []
    for row in operation_rows:
        holder_name_or_company = holder_name or company_name or company_alias
        holder_role_or_group = holder_role or holder_group
        is_buyback = int(is_buyback_holder(document_kind, company_name, holder_name, holder_role, row))
        movements.append(
            {
                "protocol": document["Protocolo_Entrega"],
                "company_alias": company_alias,
                "document_kind": document_kind,
                "holder_name": holder_name_or_company,
                "holder_role": holder_role_or_group,
                "holder_group": holder_group,
                "asset": row["asset"],
                "title_characteristics": row["title_characteristics"],
                "intermediary": row["intermediary"],
                "operation_type": row["operation_type"],
                "operation_day": row["operation_day"],
                "quantity": row["quantity"],
                "price_avg": row["price_avg"],
                "financial_volume": row["financial_volume"],
                "initial_quantity": balance.get("initial_quantity"),
                "final_quantity": balance.get("final_quantity"),
                "details": row["details"],
                "no_operations": row["no_operations"],
                "is_buyback": is_buyback,
                "raw_text": row["raw_text"],
            }
        )

    return movements


def is_buyback_holder(
    document_kind: str,
    company_name: str,
    holder_name: str,
    holder_role: str,
    row: dict[str, Any],
) -> bool:
    if document_kind != "individual":
        return False
    quantity = row.get("quantity")
    if quantity is None or float(quantity or 0) <= 0:
        return False

    normalized_company = normalize_for_compare(company_name)
    normalized_holder = normalize_for_compare(holder_name)
    normalized_role = normalize_for_compare(holder_role)

    company_match = normalized_company and normalized_holder and (
        normalized_company == normalized_holder
        or normalized_company in normalized_holder
        or normalized_holder in normalized_company
    )
    role_match = any(
        token in normalized_role
        for token in ("tesouraria", "companhia", "controlada", "coligada", "subsidiaria", "sociedade controlada")
    )
    return bool(company_match or role_match)


def extract_balances(text: str) -> dict[str, Any]:
    initial_match = re.search(
        r"Saldo Inicial.*?Valor Mobiliário Derivativo Características dos Títulos Quantidade\s+(.+?)\s+([0-9\.\,]+)\s+Movimentações no Mês",
        text,
        re.S,
    )
    final_match = re.search(
        r"Saldo Final.*?Valor Mobiliário Derivativo Características dos Títulos Quantidade\s+(.+?)\s+([0-9\.\,]+)\s*$",
        text,
        re.S,
    )
    return {
        "initial_asset": sanitize_asset(initial_match.group(1)) if initial_match else "",
        "initial_quantity": parse_brl_number(initial_match.group(2)) if initial_match else None,
        "final_asset": sanitize_asset(final_match.group(1)) if final_match else "",
        "final_quantity": parse_brl_number(final_match.group(2)) if final_match else None,
    }


def parse_operations(text: str) -> list[dict[str, Any]]:
    match = re.search(r"Movimentações no Mês.*?Volume \(R\$\)\s+(.+?)\s+Saldo Final", text, re.S)
    if not match:
        return []

    operations_text = normalize_whitespace(match.group(1))
    if not operations_text:
        return []

    rows = []
    pattern = re.compile(
        r"(?P<prefix>.+?)\s+(?P<day>\d{1,2})\s+(?P<quantity>-?[0-9\.\,]+)\s+(?P<price>-?[0-9\.\,]+)\s+(?P<volume>-?[0-9\.\,]+)(?=\s+[A-ZÁÉÍÓÚÀ-ÿ].+?\s+\d{1,2}\s+-?[0-9\.\,]+\s+-?[0-9\.\,]+\s+-?[0-9\.\,]+|$)"
    )
    for candidate in pattern.finditer(operations_text):
        prefix = candidate.group("prefix").strip()
        asset, title_characteristics, intermediary, operation_type = parse_operation_prefix(prefix)
        rows.append(
            {
                "asset": asset,
                "title_characteristics": title_characteristics,
                "intermediary": intermediary,
                "operation_type": operation_type,
                "operation_day": int(candidate.group("day")),
                "quantity": parse_brl_number(candidate.group("quantity")),
                "price_avg": parse_brl_number(candidate.group("price")),
                "financial_volume": parse_brl_number(candidate.group("volume")),
                "details": prefix,
                "raw_text": candidate.group(0),
                "no_operations": 0,
            }
        )
    return rows


def parse_operation_prefix(prefix: str) -> tuple[str, str, str, str]:
    cleaned = prefix.replace(" de Valores", " de Valores ")
    tokens = cleaned.split()
    asset = " ".join(tokens[:2]) if len(tokens) >= 2 else prefix
    remainder = " ".join(tokens[2:]).strip()
    operation_match = re.search(
        r"(compra|venda|exercício|exercicio|subscrição|subscricao|transferência|transferencia|opção|opcao|bonificação|bonificacao)",
        remainder,
        re.I,
    )
    operation_type = operation_match.group(1).lower() if operation_match else "movimentacao"
    intermediary = ""
    title_characteristics = remainder
    if " Corretora " in remainder or " Banco " in remainder:
        pieces = re.split(
            r"\b(compra|venda|exercício|exercicio|subscrição|subscricao|transferência|transferencia|opção|opcao|bonificação|bonificacao)\b",
            remainder,
            maxsplit=1,
            flags=re.I,
        )
        intermediary = pieces[0].strip()
        title_characteristics = "".join(pieces[1:]).strip() or remainder
    return sanitize_asset(asset), title_characteristics, intermediary, operation_type


def extract_selected_group(text: str) -> str:
    match = re.search(r"\(\s*X\s*\)\s*([A-Za-zÀ-ÿ ]{3,80}?)(?=\s+\(|\s+Saldo Inicial)", text)
    if not match:
        return ""
    return normalize_whitespace(match.group(1))


def extract_match(text: str, pattern: str) -> str:
    match = re.search(pattern, text, re.I | re.S)
    if not match:
        return ""
    return normalize_whitespace(match.group(1))


def sanitize_asset(value: str) -> str:
    return normalize_whitespace(value.replace("Valor Mobiliário Derivativo", ""))


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_for_compare(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").casefold())


def parse_brl_number(value: str | None) -> float | None:
    if not value:
        return None
    normalized = value.strip().replace(".", "").replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None
