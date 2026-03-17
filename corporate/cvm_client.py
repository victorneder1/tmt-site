from __future__ import annotations

import csv
import html
import io
import json
import os
import re
import subprocess
import tempfile
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import quote
from urllib.error import URLError
from urllib.request import Request, urlopen

from .config import CompanyFilter


USER_AGENT = "local-cvm-monitor/1.0"
ENET_QUERY_PAGE_URL = "https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx"
ENET_LIST_DOCUMENTS_URL = f"{ENET_QUERY_PAGE_URL}/ListarDocumentos"
ENET_DOCUMENT_CODES = "IPE_8_7_-1,IPE_8_99_-1"
ENET_DOWNLOAD_PATTERN = re.compile(
    r"OpenDownloadDocumentos\('(?P<num_sequence>\d+)','(?P<num_version>\d+)','(?P<num_protocol>\d+)','(?P<doc_type>[^']+)'\)"
)
DATE_REFERENCE_PATTERN = re.compile(r"(?P<month>\d{2})/(?P<year>\d{4})$")
DATE_PATTERN = re.compile(r"(?P<day>\d{2})/(?P<month>\d{2})/(?P<year>\d{4})")


def build_year_urls(years: list[int]) -> list[str]:
    return [
        f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{year}.zip"
        for year in years
    ]


def fetch_documents(
    years: list[int],
    companies: list[CompanyFilter] | None = None,
    live_start_date: date | None = None,
    live_end_date: date | None = None,
) -> list[dict[str, str]]:
    documents: list[dict[str, str]] = []
    errors: list[str] = []
    for url in build_year_urls(years):
        try:
            documents.extend(fetch_zip_csv(url))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")

    if companies and live_start_date and live_end_date:
        try:
            documents.extend(fetch_live_documents(companies, live_start_date, live_end_date))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{ENET_LIST_DOCUMENTS_URL}: {exc}")

    if documents:
        return documents
    raise RuntimeError("; ".join(errors) or "Falha ao carregar documentos da CVM.")


def fetch_zip_csv(url: str) -> list[dict[str, str]]:
    payload = fetch_zip_payload(url)

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        csv_name = archive.namelist()[0]
        with archive.open(csv_name, "r") as file_handle:
            wrapper = io.TextIOWrapper(file_handle, encoding="cp1252")
            reader = csv.DictReader(wrapper, delimiter=";")
            return [normalize_document(row, source_url=url) for row in reader]


def fetch_zip_payload(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=60) as response:
            payload = response.read()
            persist_local_cache(url, payload)
            return payload
    except URLError as original_error:
        try:
            return load_cached_zip_payload(url, original_error)
        except Exception:
            pass
        if os.name != "nt":
            raise
        try:
            payload = fetch_zip_payload_powershell(url)
            persist_local_cache(url, payload)
            return payload
        except Exception:
            return load_cached_zip_payload(url, original_error)


def fetch_zip_payload_powershell(url: str) -> bytes:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as temp_file:
        temp_path = temp_file.name

    command = (
        "$ProgressPreference='SilentlyContinue'; "
        f"Invoke-WebRequest -Uri '{url}' -OutFile '{temp_path}'"
    )
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", command],
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip() or result.stdout.strip() or "Falha ao baixar arquivo da CVM."
            raise RuntimeError(stderr)
        return Path(temp_path).read_bytes()
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def persist_local_cache(url: str, payload: bytes) -> None:
    cache_path = get_cached_zip_path(url)
    if cache_path is None:
        return
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(payload)


def load_cached_zip_payload(url: str, original_error: Exception) -> bytes:
    cache_path = get_cached_zip_path(url)
    if cache_path and cache_path.exists():
        return cache_path.read_bytes()

    temp_cache = get_temp_cached_zip_path(url)
    if temp_cache and temp_cache.exists():
        return temp_cache.read_bytes()

    raise original_error


def get_cached_zip_path(url: str) -> Path | None:
    year = extract_year(url)
    if year is None:
        return None
    return Path("data") / "cache" / f"ipe_cia_aberta_{year}.zip"


def get_temp_cached_zip_path(url: str) -> Path | None:
    year = extract_year(url)
    if year is None:
        return None
    return Path(tempfile.gettempdir()) / f"ipe_cia_aberta_{year}.zip"


def extract_year(url: str) -> str | None:
    match = re.search(r"ipe_cia_aberta_(\d{4})\.zip", url)
    if not match:
        return None
    return match.group(1)


def normalize_document(row: dict[str, str], source_url: str) -> dict[str, str]:
    document = {
        key.strip(): repair_text((value or "").strip())
        for key, value in row.items()
    }
    document["source_url"] = source_url
    document["captured_at"] = datetime.now(UTC).isoformat()
    if "Link_Download" in document:
        document["Link_Download"] = normalize_download_url(document["Link_Download"])
    return document


def download_document_pdf(
    url: str,
    protocol: str,
    version: str,
    cache_dir: Path,
) -> tuple[Path, str]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    safe_protocol = re.sub(r"[^A-Za-z0-9_-]", "_", protocol)
    safe_version = re.sub(r"[^A-Za-z0-9_-]", "_", version or "1")
    pdf_path = cache_dir / f"{safe_protocol}_v{safe_version}.pdf"
    if pdf_path.exists():
        return pdf_path, datetime.now(UTC).isoformat()

    payload = fetch_binary_payload(url)
    pdf_path.write_bytes(payload)
    return pdf_path, datetime.now(UTC).isoformat()


def fetch_binary_payload(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=60) as response:
        return response.read()


def fetch_live_documents(
    companies: list[CompanyFilter],
    start_date: date,
    end_date: date,
) -> list[dict[str, str]]:
    opener = build_enet_opener()
    documents: list[dict[str, str]] = []
    for company in companies:
        if not company.cvm_code:
            continue
        documents.extend(fetch_live_documents_for_company(opener, company.cvm_code, start_date, end_date))
    return documents


def build_enet_opener():
    import http.cookiejar
    from urllib.request import HTTPCookieProcessor, build_opener

    jar = http.cookiejar.CookieJar()
    opener = build_opener(HTTPCookieProcessor(jar))
    request = Request(ENET_QUERY_PAGE_URL, headers={"User-Agent": USER_AGENT})
    with opener.open(request, timeout=60):
        pass
    return opener


def fetch_live_documents_for_company(
    opener,
    cvm_code: str,
    start_date: date,
    end_date: date,
) -> list[dict[str, str]]:
    payload = {
        "dataDe": start_date.strftime("%d/%m/%Y"),
        "dataAte": end_date.strftime("%d/%m/%Y"),
        "empresa": build_enet_company_filter(cvm_code),
        "setorAtividade": "-1",
        "categoriaEmissor": "-1",
        "situacaoEmissor": "-1",
        "tipoParticipante": "-1",
        "dataReferencia": "",
        "categoria": ENET_DOCUMENT_CODES,
        "periodo": "2",
        "horaIni": "",
        "horaFim": "",
        "palavraChave": "",
        "ultimaDtRef": "false",
        "tipoEmpresa": "0",
        "token": "",
        "versaoCaptcha": "V3",
    }
    request = Request(
        ENET_LIST_DOCUMENTS_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json; charset=utf-8",
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    with opener.open(request, timeout=60) as response:
        raw = response.read().decode("utf-8", errors="ignore")
    parsed = json.loads(raw).get("d", {})
    rows = str(parsed.get("dados", "") or "")
    if not rows:
        return []
    return parse_live_document_rows(rows, cvm_code)


def build_enet_company_filter(cvm_code: str) -> str:
    digits = re.sub(r"\D", "", cvm_code or "")
    return f",{digits.zfill(6)}" if digits else ""


def parse_live_document_rows(raw_rows: str, requested_cvm_code: str) -> list[dict[str, str]]:
    captured_at = datetime.now(UTC).isoformat()
    documents: list[dict[str, str]] = []
    for raw_row in raw_rows.split("&&*"):
        row = raw_row.strip()
        if not row:
            continue
        fields = row.split("$&")
        if len(fields) < 11:
            continue
        document = parse_live_document_row(fields, requested_cvm_code, captured_at)
        if document:
            documents.append(document)
    return documents


def parse_live_document_row(
    fields: list[str],
    requested_cvm_code: str,
    captured_at: str,
) -> dict[str, str] | None:
    download = extract_download_metadata(fields[10])
    if not download:
        return None

    cvm_code = normalize_cvm_code(fields[0]) or normalize_cvm_code(requested_cvm_code)
    version = clean_live_field(fields[8]) or download["num_version"]
    reference_date = parse_reference_date(clean_live_field(fields[5]))
    delivery_date = parse_delivery_date(clean_live_field(fields[6]))
    link_download = normalize_download_url(
        build_download_url(
            download["num_protocol"],
            download["num_sequence"],
            version,
            download["doc_type"],
        )
    )
    protocol = build_live_protocol(cvm_code, download["num_protocol"], download["num_sequence"], version)

    return {
        "Codigo_CVM": cvm_code,
        "Nome_Companhia": clean_live_field(fields[1]),
        "Categoria": clean_live_field(fields[2]),
        "Tipo": clean_live_field(fields[3]),
        "Especie": clean_live_field(fields[4]),
        "Assunto": "",
        "Data_Referencia": reference_date,
        "Data_Entrega": delivery_date,
        "Versao": version,
        "Protocolo_Entrega": protocol,
        "Link_Download": link_download,
        "source_url": ENET_LIST_DOCUMENTS_URL,
        "captured_at": captured_at,
    }


def extract_download_metadata(field: str) -> dict[str, str] | None:
    match = ENET_DOWNLOAD_PATTERN.search(field or "")
    if not match:
        return None
    return match.groupdict()


def build_download_url(num_protocol: str, num_sequence: str, num_version: str, doc_type: str) -> str:
    return (
        "https://www.rad.cvm.gov.br/ENET/frmDownloadDocumento.aspx"
        f"?Tela=ext&descTipo={quote(doc_type)}&CodigoInstituicao=1"
        f"&numProtocolo={quote(num_protocol)}&numSequencia={quote(num_sequence)}&numVersao={quote(num_version)}"
    )


def build_live_protocol(cvm_code: str, num_protocol: str, num_sequence: str, version: str) -> str:
    return f"ENET:{cvm_code}:{num_protocol}:{num_sequence}:{version}"


def normalize_cvm_code(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    return digits.lstrip("0") or digits


def clean_live_field(value: str) -> str:
    cleaned = re.sub(r"</?spanOrder>", "", value or "")
    cleaned = re.sub(r"<[^>]+>", "", cleaned)
    cleaned = html.unescape(cleaned).strip()
    if cleaned == "-":
        return ""
    return repair_text(cleaned)


def parse_reference_date(value: str) -> str:
    match = DATE_REFERENCE_PATTERN.search(value or "")
    if match:
        return f"{match.group('year')}-{match.group('month')}-01"

    match = DATE_PATTERN.search(value or "")
    if match:
        return f"{match.group('year')}-{match.group('month')}-{match.group('day')}"
    return ""


def parse_delivery_date(value: str) -> str:
    match = DATE_PATTERN.search(value or "")
    if not match:
        return ""
    return f"{match.group('year')}-{match.group('month')}-{match.group('day')}"


def normalize_download_url(url: str) -> str:
    return (url or "").strip()


def repair_text(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return text
    try:
        repaired = text.encode("latin-1").decode("utf-8")
    except UnicodeError:
        return text
    return repaired if repaired else text
