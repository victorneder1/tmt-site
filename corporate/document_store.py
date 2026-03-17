from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class DocumentStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    protocol TEXT PRIMARY KEY,
                    market TEXT NOT NULL DEFAULT 'BZ',
                    ticker TEXT,
                    sector TEXT,
                    company_alias TEXT NOT NULL,
                    company_name TEXT,
                    cvm_code TEXT,
                    cnpj TEXT,
                    category TEXT,
                    type_name TEXT,
                    species TEXT,
                    subject TEXT,
                    document_kind TEXT NOT NULL,
                    reference_date TEXT,
                    delivery_date TEXT,
                    version TEXT,
                    link_download TEXT,
                    source_url TEXT,
                    captured_at TEXT,
                    pdf_path TEXT,
                    pdf_downloaded_at TEXT,
                    parse_status TEXT NOT NULL,
                    parse_error TEXT,
                    parse_updated_at TEXT,
                    summary_json TEXT DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS movements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    protocol TEXT NOT NULL,
                    market TEXT NOT NULL DEFAULT 'BZ',
                    ticker TEXT,
                    sector TEXT,
                    company_alias TEXT NOT NULL,
                    document_kind TEXT NOT NULL,
                    holder_name TEXT,
                    holder_role TEXT,
                    holder_group TEXT,
                    asset TEXT,
                    title_characteristics TEXT,
                    intermediary TEXT,
                    operation_type TEXT,
                    operation_day INTEGER,
                    quantity REAL,
                    price_avg REAL,
                    financial_volume REAL,
                    initial_quantity REAL,
                    final_quantity REAL,
                    details TEXT,
                    no_operations INTEGER DEFAULT 0,
                    is_buyback INTEGER DEFAULT 0,
                    raw_text TEXT,
                    FOREIGN KEY(protocol) REFERENCES documents(protocol)
                );
                """
            )

    def is_empty(self) -> bool:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS total FROM documents").fetchone()
        return bool(row and row["total"] == 0)

    def has_protocol(self, protocol: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM documents WHERE protocol = ? LIMIT 1",
                (protocol,),
            ).fetchone()
        return row is not None

    def find_matching_protocol(
        self,
        protocol: str,
        link_download: str = "",
        company_alias: str = "",
        document_kind: str = "",
        reference_date: str = "",
        delivery_date: str = "",
        version: str = "",
    ) -> str | None:
        with self._connect() as connection:
            if protocol:
                row = connection.execute(
                    "SELECT protocol FROM documents WHERE protocol = ? LIMIT 1",
                    (protocol,),
                ).fetchone()
                if row:
                    return str(row["protocol"])

            if link_download:
                row = connection.execute(
                    "SELECT protocol FROM documents WHERE link_download = ? LIMIT 1",
                    (link_download,),
                ).fetchone()
                if row:
                    return str(row["protocol"])

            if company_alias and document_kind and reference_date and delivery_date and version:
                row = connection.execute(
                    """
                    SELECT protocol
                    FROM documents
                    WHERE company_alias = ?
                        AND document_kind = ?
                        AND reference_date = ?
                        AND delivery_date = ?
                        AND version = ?
                    LIMIT 1
                    """,
                    (company_alias, document_kind, reference_date, delivery_date, version),
                ).fetchone()
                if row:
                    return str(row["protocol"])
        return None

    def get_document(self, protocol: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM documents WHERE protocol = ? LIMIT 1",
                (protocol,),
            ).fetchone()
        return self._row_to_document(row) if row else None

    def reassign_protocol(self, current_protocol: str, new_protocol: str) -> None:
        if not current_protocol or current_protocol == new_protocol:
            return
        with self._connect() as connection:
            connection.execute(
                "UPDATE documents SET protocol = ? WHERE protocol = ?",
                (new_protocol, current_protocol),
            )
            connection.execute(
                "UPDATE movements SET protocol = ? WHERE protocol = ?",
                (new_protocol, current_protocol),
            )

    def upsert_document(self, document: dict[str, Any]) -> None:
        payload = {
            "protocol": document.get("Protocolo_Entrega", ""),
            "market": document.get("market", "BZ"),
            "ticker": document.get("ticker", ""),
            "sector": document.get("sector", ""),
            "company_alias": document.get("company_alias", ""),
            "company_name": document.get("Nome_Companhia", ""),
            "cvm_code": document.get("Codigo_CVM", ""),
            "cnpj": document.get("CNPJ_Companhia", ""),
            "category": document.get("Categoria", ""),
            "type_name": document.get("Tipo", ""),
            "species": document.get("Especie", ""),
            "subject": document.get("Assunto", ""),
            "document_kind": document.get("document_kind", ""),
            "reference_date": document.get("Data_Referencia", ""),
            "delivery_date": document.get("Data_Entrega", ""),
            "version": document.get("Versao", ""),
            "link_download": document.get("Link_Download", ""),
            "source_url": document.get("source_url", ""),
            "captured_at": document.get("captured_at", ""),
            "pdf_path": document.get("pdf_path", ""),
            "pdf_downloaded_at": document.get("pdf_downloaded_at", ""),
            "parse_status": document.get("parse_status", "pending"),
            "parse_error": document.get("parse_error", ""),
            "parse_updated_at": document.get("parse_updated_at", ""),
            "summary_json": json.dumps(document.get("summary", {}), ensure_ascii=False),
        }
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO documents (
                    protocol, market, ticker, sector, company_alias, company_name, cvm_code, cnpj,
                    category, type_name, species, subject, document_kind, reference_date,
                    delivery_date, version, link_download, source_url, captured_at, pdf_path,
                    pdf_downloaded_at, parse_status, parse_error, parse_updated_at, summary_json
                ) VALUES (
                    :protocol, :market, :ticker, :sector, :company_alias, :company_name, :cvm_code, :cnpj,
                    :category, :type_name, :species, :subject, :document_kind, :reference_date,
                    :delivery_date, :version, :link_download, :source_url, :captured_at, :pdf_path,
                    :pdf_downloaded_at, :parse_status, :parse_error, :parse_updated_at, :summary_json
                )
                ON CONFLICT(protocol) DO UPDATE SET
                    market = excluded.market,
                    ticker = excluded.ticker,
                    sector = excluded.sector,
                    company_alias = excluded.company_alias,
                    company_name = excluded.company_name,
                    cvm_code = excluded.cvm_code,
                    cnpj = excluded.cnpj,
                    category = excluded.category,
                    type_name = excluded.type_name,
                    species = excluded.species,
                    subject = excluded.subject,
                    document_kind = excluded.document_kind,
                    reference_date = excluded.reference_date,
                    delivery_date = excluded.delivery_date,
                    version = excluded.version,
                    link_download = excluded.link_download,
                    source_url = excluded.source_url,
                    captured_at = excluded.captured_at,
                    pdf_path = excluded.pdf_path,
                    pdf_downloaded_at = excluded.pdf_downloaded_at,
                    parse_status = excluded.parse_status,
                    parse_error = excluded.parse_error,
                    parse_updated_at = excluded.parse_updated_at,
                    summary_json = excluded.summary_json
                """,
                payload,
            )

    def replace_movements(self, protocol: str, movements: list[dict[str, Any]]) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM movements WHERE protocol = ?", (protocol,))
            connection.executemany(
                """
                INSERT INTO movements (
                    protocol, market, ticker, sector, company_alias, document_kind,
                    holder_name, holder_role, holder_group, asset, title_characteristics,
                    intermediary, operation_type, operation_day, quantity, price_avg,
                    financial_volume, initial_quantity, final_quantity, details,
                    no_operations, is_buyback, raw_text
                ) VALUES (
                    :protocol, :market, :ticker, :sector, :company_alias, :document_kind,
                    :holder_name, :holder_role, :holder_group, :asset, :title_characteristics,
                    :intermediary, :operation_type, :operation_day, :quantity, :price_avg,
                    :financial_volume, :initial_quantity, :final_quantity, :details,
                    :no_operations, :is_buyback, :raw_text
                )
                """,
                [_normalize_movement(m) for m in movements],
            )

    def get_recent_documents(self, limit: int) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM documents
                ORDER BY delivery_date DESC, reference_date DESC, document_kind ASC, CAST(version AS INTEGER) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_document(row) for row in rows]

    def get_history_documents(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM documents
                ORDER BY delivery_date DESC, reference_date DESC, document_kind ASC, CAST(version AS INTEGER) DESC
                """
            ).fetchall()
        return [self._row_to_document(row) for row in rows]

    def get_movements(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT m.*, d.reference_date, d.delivery_date, d.company_name
                FROM movements m
                INNER JOIN documents d ON d.protocol = m.protocol
                ORDER BY d.reference_date DESC, d.delivery_date DESC, m.document_kind ASC, m.id ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_counts(self) -> dict[str, int]:
        with self._connect() as connection:
            documents = connection.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN document_kind = 'consolidada' THEN 1 ELSE 0 END) AS consolidated,
                    SUM(CASE WHEN document_kind = 'individual' THEN 1 ELSE 0 END) AS individual,
                    SUM(CASE WHEN parse_status = 'failed' THEN 1 ELSE 0 END) AS failed
                FROM documents
                """
            ).fetchone()
            movements = connection.execute("SELECT COUNT(*) AS total FROM movements").fetchone()
        return {
            "documents_total": int(documents["total"] or 0),
            "documents_consolidated": int(documents["consolidated"] or 0),
            "documents_individual": int(documents["individual"] or 0),
            "documents_failed_parse": int(documents["failed"] or 0),
            "movements_total": int(movements["total"] or 0),
        }

    @staticmethod
    def _row_to_document(row: sqlite3.Row) -> dict[str, Any]:
        document = dict(row)
        document["summary"] = json.loads(document.pop("summary_json", "{}") or "{}")
        document["Protocolo_Entrega"] = document.pop("protocol")
        document["Nome_Companhia"] = document.pop("company_name")
        document["Codigo_CVM"] = document.pop("cvm_code")
        document["CNPJ_Companhia"] = document.pop("cnpj")
        document["Categoria"] = document.pop("category")
        document["Tipo"] = document.pop("type_name")
        document["Especie"] = document.pop("species")
        document["Assunto"] = document.pop("subject")
        document["Data_Referencia"] = document.pop("reference_date")
        document["Data_Entrega"] = document.pop("delivery_date")
        document["Versao"] = document.pop("version")
        document["Link_Download"] = document.pop("link_download")
        return document


def _normalize_movement(m: dict[str, Any]) -> dict[str, Any]:
    """Ensure movement dict has all required keys with defaults."""
    return {
        "protocol": m.get("protocol", ""),
        "market": m.get("market", "BZ"),
        "ticker": m.get("ticker", ""),
        "sector": m.get("sector", ""),
        "company_alias": m.get("company_alias", ""),
        "document_kind": m.get("document_kind", ""),
        "holder_name": m.get("holder_name", ""),
        "holder_role": m.get("holder_role", ""),
        "holder_group": m.get("holder_group", ""),
        "asset": m.get("asset", ""),
        "title_characteristics": m.get("title_characteristics", ""),
        "intermediary": m.get("intermediary", ""),
        "operation_type": m.get("operation_type", ""),
        "operation_day": m.get("operation_day"),
        "quantity": m.get("quantity"),
        "price_avg": m.get("price_avg"),
        "financial_volume": m.get("financial_volume"),
        "initial_quantity": m.get("initial_quantity"),
        "final_quantity": m.get("final_quantity"),
        "details": m.get("details", ""),
        "no_operations": int(m.get("no_operations") or 0),
        "is_buyback": int(m.get("is_buyback") or 0),
        "raw_text": m.get("raw_text", ""),
    }
