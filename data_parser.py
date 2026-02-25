import os
import re
import pandas as pd
import numpy as np
from datetime import datetime

# In-memory cache
_cache = {}


def _file_mtime(file_path):
    return os.path.getmtime(file_path)


def _is_cached(key, file_path):
    if key in _cache:
        if _cache[key]["mtime"] == _file_mtime(file_path):
            return True
    return False


def _safe_float(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, str) and val.strip() in ("", "-"):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _normalize_key(text):
    """Convert header text to a snake_case key."""
    text = text.replace("\n", " ")
    text = text.lower().strip()
    text = re.sub(r"[/\\().,:;$]", " ", text)
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = re.sub(r"\s+", "_", text)
    text = text.strip("_")
    text = re.sub(r"_+", "_", text)
    # Remove trailing y/y patterns
    text = re.sub(r"_y_y$", "", text)
    return text


def _infer_type_and_decimals(header_text):
    """Infer display type and decimal places from header text."""
    h = header_text.lower().replace("\n", " ")
    h_compact = re.sub(r"\s+", "", h)

    if "price" in h:
        return "number", 2
    if "mkt" in h or "market" in h:
        return "number", 0
    # EV standalone (not EV/Sales, EV/FCF)
    if re.search(r"\bev\b", h) and "ev/" not in h_compact:
        return "number", 0
    # Multiples (EV/Sales, EV/FCF, P/E)
    if any(k in h_compact for k in ["ev/sales", "ev/fcf", "p/e"]):
        return "multiple", 1
    # Percentages (growth, margin, yield)
    if any(k in h for k in ["growth", "margin", "yield"]):
        return "percent", 1
    return "number", 1


def _parse_sheet_dynamic(file_path, sheet_name):
    """Parse any sheet dynamically by reading headers from rows 2-3."""
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    # Row 2: main/group headers (merged cells show value only in first cell)
    # Row 3: sub-headers (CY-2026, CY-2027, units like US$ mn)
    # Data starts at row 4

    # Read row 2 headers (raw, no fill yet)
    raw_row2 = []
    for col in range(df.shape[1]):
        val = df.iloc[2, col] if df.shape[0] > 2 else None
        if not pd.isna(val) and str(val).strip():
            raw_row2.append(str(val).strip())
        else:
            raw_row2.append(None)

    # Read row 3 sub-headers
    sub_headers = []
    for col in range(df.shape[1]):
        val = df.iloc[3, col] if df.shape[0] > 3 else None
        if not pd.isna(val) and str(val).strip():
            sub_headers.append(str(val).strip())
        else:
            sub_headers.append(None)

    # Forward-fill row 2 for merged cells, but ONLY when row 3 has a sub-header
    # (prevents filling into truly empty columns)
    group_headers = list(raw_row2)
    last_val = None
    for i in range(len(group_headers)):
        if group_headers[i] is not None:
            last_val = group_headers[i]
        elif last_val is not None and i > 2 and sub_headers[i] is not None:
            group_headers[i] = last_val

    # Build column definitions
    columns = []
    for col_idx in range(1, df.shape[1]):
        group = group_headers[col_idx]
        sub = sub_headers[col_idx]

        # Column 1 = Company name (always present, even without header)
        if col_idx == 1:
            columns.append({
                "key": "name", "group": "Company", "label": "",
                "type": "text", "decimals": 0, "col_idx": col_idx,
            })
            continue

        # Column 2 = Ticker
        if col_idx == 2:
            columns.append({
                "key": "ticker", "group": "Ticker", "label": "",
                "type": "text", "decimals": 0, "col_idx": col_idx,
            })
            continue

        # Stop at empty column gap (separates display columns from auxiliary)
        if group is None and sub is None:
            break

        # Clean display group name
        display_group = (group or "").replace("\n", " ")

        # Check if sub-header is a year/period label
        is_year_label = bool(
            sub and re.match(r"(?:CY|FY|Q[1-4])[-\s]?\d{2,4}", sub, re.IGNORECASE)
        )

        # Generate key and display label
        group_key = _normalize_key(display_group) if display_group else f"col{col_idx}"

        if is_year_label:
            sub_key = _normalize_key(sub)
            key = f"{group_key}_{sub_key}"
            display_label = sub
        else:
            key = group_key
            if sub:
                display_group = f"{display_group} ({sub})"
            display_label = ""

        col_type, decimals = _infer_type_and_decimals(display_group)

        columns.append({
            "key": key, "group": display_group, "label": display_label,
            "type": col_type, "decimals": decimals, "col_idx": col_idx,
        })

    # Parse data rows
    data = []
    for i in range(4, df.shape[0]):
        name_val = df.iloc[i, 1]
        if pd.isna(name_val) or not str(name_val).strip():
            continue
        ticker_val = df.iloc[i, 2]
        if pd.isna(ticker_val) or not str(ticker_val).strip():
            continue

        row = {}
        for col_def in columns:
            val = df.iloc[i, col_def["col_idx"]]
            if col_def["type"] == "text":
                row[col_def["key"]] = str(val).strip() if not pd.isna(val) else None
            else:
                row[col_def["key"]] = _safe_float(val)
        data.append(row)

    clean_columns = [{k: v for k, v in c.items() if k != "col_idx"} for c in columns]
    return {"columns": clean_columns, "data": data}


def parse_software_comps(file_path):
    cache_key = f"software_{file_path}"
    if _is_cached(cache_key, file_path):
        return _cache[cache_key]["data"]

    data = {
        "gaap": _parse_sheet_dynamic(file_path, "Comps_GAAP"),
        "nongaap": _parse_sheet_dynamic(file_path, "Comps_NonGAAP"),
    }

    _cache[cache_key] = {"mtime": _file_mtime(file_path), "data": data}
    return data


def parse_itservices_comps(file_path):
    cache_key = f"itservices_{file_path}"
    if _is_cached(cache_key, file_path):
        return _cache[cache_key]["data"]

    result = _parse_sheet_dynamic(file_path, "Comps_ITServices")

    _cache[cache_key] = {"mtime": _file_mtime(file_path), "data": result}
    return result


def get_last_updated(software_path, itservices_path):
    times = []
    for p in [software_path, itservices_path]:
        if os.path.exists(p):
            times.append(_file_mtime(p))
    if times:
        latest = max(times)
        return datetime.fromtimestamp(latest).strftime("%Y-%m-%d")
    return None
