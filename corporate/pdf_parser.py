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
    # Pass raw section_text (preserves newlines) so extract_balances can anchor on line boundaries.
    balance = extract_balances(section_text)
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

    initial_quantities: dict[str, float] = balance.get("initial_quantities") or {}

    movements: list[dict[str, Any]] = []
    for row in operation_rows:
        holder_name_or_company = holder_name or company_name or company_alias
        holder_role_or_group = holder_role or holder_group
        is_buyback = int(is_buyback_holder(document_kind, company_name, holder_name, holder_role, row))

        # Match operation to its asset-specific Saldo Inicial.
        # e.g. "Ações PN" → looks up "acoes pn" in initial_quantities.
        # Falls back to the first-listed asset's quantity if no specific match.
        op_asset_key = _balance_key(row["asset"])
        matched_initial = (
            initial_quantities.get(op_asset_key)
            if op_asset_key and initial_quantities
            else None
        )
        initial_qty = matched_initial if matched_initial is not None else balance.get("initial_quantity")

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
                "initial_quantity": initial_qty,
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


def _balance_key(asset: str) -> str:
    """Normalise an asset name to a lookup key for matching operations to balances.

    Uses the first two whitespace-separated tokens after accent-stripping and
    lower-casing, e.g. "Ações PN" → "acoes pn", "Units ON" → "units on".
    """
    import unicodedata as _ud
    stripped = "".join(
        ch for ch in _ud.normalize("NFKD", asset)
        if not _ud.combining(ch)
    ).casefold()
    tokens = stripped.split()
    return " ".join(tokens[:2]) if len(tokens) >= 2 else (tokens[0] if tokens else "")


def extract_balances(text: str) -> dict[str, Any]:
    """Extract asset quantities from both Saldo Inicial and Saldo Final sections.

    Returns:
        initial_asset      – name of the first listed asset in Saldo Inicial
        initial_quantity   – quantity of that first asset
        final_asset        – name of the first listed asset in Saldo Final
        final_quantity     – quantity of that first asset
        initial_quantities – dict mapping _balance_key(asset) → quantity for ALL
                             assets listed in Saldo Inicial (used to match each
                             operation row to its own asset's starting balance)

    ``text`` is the raw section text with newlines preserved, so each asset row
    in the balance table occupies its own physical line.  Asset descriptors may
    contain digits (e.g. "Units 1 ON E 2 PNA") or wrap across lines.  Algorithm:

    1. Locate the balance header and extract the block up to "Movimentações no Mês".
    2. Process lines one by one, accumulating descriptor tokens.
    3. A line ending with a ≤2-digit integer whose *next* line begins with a letter
       is a descriptor continuation — the integer is part of the asset name.
    4. A line ending with a genuine quantity terminates an asset row; reset and
       continue to the next row (ALL rows collected, not just the first).
    """
    _HEADER = r"Valor Mobili[aá]rio Derivativo Caracter[íi]sticas dos T[íi]tulos Quantidade"

    def _find_all(keyword: str) -> list[tuple[str, str]]:
        """Return list of (asset_text, quantity_str) for every row after keyword."""
        kw = re.search(keyword, text, re.S | re.I)
        if not kw:
            return []
        after_kw = text[kw.end():]
        hdr = re.search(_HEADER, after_kw, re.S | re.I)
        if not hdr:
            return []
        body = after_kw[hdr.end():]
        mov = re.search(r"Movimenta[çc][õo]es no M[êe]s", body, re.S | re.I)
        block = body[: mov.start()] if mov else body[:600]

        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        results: list[tuple[str, str]] = []
        row_parts: list[str] = []
        for i, line in enumerate(lines):
            num_m = re.search(r"([0-9][0-9.,]*)\s*$", line)
            if not num_m:
                row_parts.append(line)
                continue
            num_str = num_m.group(1)
            # A ≤2-digit bare integer followed by a letter-starting line is a
            # descriptor continuation — the integer is part of the asset name.
            is_descriptor = (
                re.fullmatch(r"\d{1,2}", num_str)
                and i + 1 < len(lines)
                and re.match(r"^[A-Za-z\u00C0-\u00FF]", lines[i + 1])
            )
            if is_descriptor:
                row_parts.append(line)
                continue
            pre = line[: num_m.start()].strip()
            if pre:
                row_parts.append(pre)
            asset_text = sanitize_asset(" ".join(row_parts).strip())
            if asset_text:
                results.append((asset_text, num_str))
            row_parts = []  # reset for next asset row

        if results:
            return results

        # Fallback for condensed (no-newline) text — capture only the first row.
        condensed = normalize_whitespace(block)
        m = re.search(
            r"([\u00C0-\u00FFa-zA-Z /]+?)\s+([0-9\.\,]+)\s+Movimenta[çc][õo]es",
            condensed,
            re.S,
        )
        return [(sanitize_asset(m.group(1)), m.group(2))] if m else []

    initial_rows = _find_all("Saldo Inicial")
    final_rows   = _find_all("Saldo Final")

    # Build per-asset lookup dict (first occurrence of each key wins).
    initial_quantities: dict[str, float] = {}
    for asset_text, qty_str in initial_rows:
        key = _balance_key(asset_text)
        qty = parse_brl_number(qty_str)
        if key and qty is not None and key not in initial_quantities:
            initial_quantities[key] = qty

    first_initial = initial_rows[0] if initial_rows else None
    first_final   = final_rows[0]   if final_rows   else None

    return {
        "initial_asset":      sanitize_asset(first_initial[0]) if first_initial else "",
        "initial_quantity":   parse_brl_number(first_initial[1]) if first_initial else None,
        "final_asset":        sanitize_asset(first_final[0]) if first_final else "",
        "final_quantity":     parse_brl_number(first_final[1]) if first_final else None,
        "initial_quantities": initial_quantities,  # all assets → qty
    }


def parse_operations(text: str) -> list[dict[str, Any]]:
    match = re.search(r"Movimentações no Mês.*?Volume \(R\$\)\s+(.+?)\s+Saldo Final", text, re.S)
    if not match:
        return []

    operations_text = normalize_whitespace(match.group(1))
    if not operations_text:
        return []

    # Handle PDF page breaks that split 'Compra/Venda à vista' across pages.
    # When 'vista' lands at the start of page N+1, pypdf places it AFTER the
    # numeric columns (day, qty, price, vol) of the split row:
    #   "...Compra à  27  19.300  2,04000  39.372,00  vista  Ações ON ..."
    # The row-regex lookahead rejects 'vista' (lowercase v not in [A-ZÁÉÍÓÚÀ-ÿ]),
    # causing the engine to backtrack and swallow the entire split row into the
    # prefix of the next row, losing those shares entirely.
    # Fix: remove the orphaned 'vista' between a number and an uppercase-starting
    # next-row prefix, then let the regex parse both rows normally.
    operations_text = re.sub(
        r"(-?[0-9][0-9.,]*)\s+vista\s+(?=[A-ZÁÉÍÓÚÀ-ÿ])",
        r"\1 ",
        operations_text,
        flags=re.I,
    )

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
