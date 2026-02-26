import sqlite3
import os
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
DB_PATH = os.path.join(DATA_DIR, "pairs.db")

# ---------------------------------------------------------------------------
# Price cache (in-memory, 30s TTL)
# ---------------------------------------------------------------------------
_price_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 30


def _cached_price(ticker):
    with _cache_lock:
        entry = _price_cache.get(ticker)
        if entry and time.time() - entry["ts"] < CACHE_TTL:
            return entry["price"]
    return None


def _set_cached_price(ticker, price):
    with _cache_lock:
        _price_cache[ticker] = {"price": price, "ts": time.time()}


# ---------------------------------------------------------------------------
# Yahoo Finance helpers
# ---------------------------------------------------------------------------
YF_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
YF_HEADERS = {"User-Agent": "Mozilla/5.0"}


def get_current_price(ticker):
    cached = _cached_price(ticker)
    if cached is not None:
        return cached
    try:
        r = requests.get(
            f"{YF_BASE}/{ticker}",
            params={"interval": "1d", "range": "1d"},
            headers=YF_HEADERS,
            timeout=5,
        )
        r.raise_for_status()
        result = r.json()["chart"]["result"][0]
        price = result["meta"]["regularMarketPrice"]
        _set_cached_price(ticker, price)
        return price
    except Exception:
        return None


def get_batch_prices(tickers):
    prices = {}
    to_fetch = []
    for t in tickers:
        cached = _cached_price(t)
        if cached is not None:
            prices[t] = cached
        else:
            to_fetch.append(t)

    if to_fetch:
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(get_current_price, to_fetch))
        for t, p in zip(to_fetch, results):
            if p is not None:
                prices[t] = p

    return prices


def get_historical_prices(ticker, from_iso, to_iso):
    try:
        from_ts = int(datetime.fromisoformat(from_iso.replace("Z", "+00:00")).timestamp())
        to_ts = int(datetime.fromisoformat(to_iso.replace("Z", "+00:00")).timestamp())
    except Exception:
        from_ts = int(datetime.fromisoformat(from_iso).timestamp())
        to_ts = int(datetime.fromisoformat(to_iso).timestamp())

    try:
        r = requests.get(
            f"{YF_BASE}/{ticker}",
            params={"period1": from_ts, "period2": to_ts, "interval": "1d"},
            headers=YF_HEADERS,
            timeout=10,
        )
        r.raise_for_status()
        result = r.json()["chart"]["result"][0]
        timestamps = result.get("timestamp", [])
        closes = result["indicators"]["quote"][0].get("close", [])
        data = []
        for ts, close in zip(timestamps, closes):
            if close is not None:
                data.append({"timestamp": datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT00:00:00.000Z"), "price": close})
        return data
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def _get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = _get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            long_ticker TEXT NOT NULL,
            short_ticker TEXT NOT NULL,
            entry_price_long TEXT NOT NULL,
            entry_price_short TEXT NOT NULL,
            entry_date TEXT NOT NULL,
            inception_date TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            closed_date TEXT,
            close_price_long TEXT,
            close_price_short TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Migration: add columns if missing
    existing = {col[1] for col in conn.execute("PRAGMA table_info(pairs)").fetchall()}
    migrations = {
        "inception_date": "ALTER TABLE pairs ADD COLUMN inception_date TEXT",
        "status": "ALTER TABLE pairs ADD COLUMN status TEXT NOT NULL DEFAULT 'open'",
        "closed_date": "ALTER TABLE pairs ADD COLUMN closed_date TEXT",
        "close_price_long": "ALTER TABLE pairs ADD COLUMN close_price_long TEXT",
        "close_price_short": "ALTER TABLE pairs ADD COLUMN close_price_short TEXT",
        "sort_order": "ALTER TABLE pairs ADD COLUMN sort_order INTEGER DEFAULT 0",
    }
    for col, sql in migrations.items():
        if col not in existing:
            try:
                conn.execute(sql)
            except Exception:
                pass
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Parse helpers (JSON arrays for baskets)
# ---------------------------------------------------------------------------
def _parse_tickers(raw):
    if isinstance(raw, str) and raw.startswith("["):
        return json.loads(raw)
    return raw


def _parse_prices(raw):
    if raw is None:
        return None
    if isinstance(raw, str) and raw.startswith("["):
        return json.loads(raw)
    return float(raw)


def _row_to_pair(row):
    d = dict(row)
    d["long_ticker"] = _parse_tickers(d["long_ticker"])
    d["short_ticker"] = _parse_tickers(d["short_ticker"])
    d["entry_price_long"] = _parse_prices(d["entry_price_long"])
    d["entry_price_short"] = _parse_prices(d["entry_price_short"])
    d["close_price_long"] = _parse_prices(d.get("close_price_long"))
    d["close_price_short"] = _parse_prices(d.get("close_price_short"))
    return d


# ---------------------------------------------------------------------------
# Performance calculation
# ---------------------------------------------------------------------------
def calculate_performance(entry_long, long_tickers, prices, entry_short, short_tickers):
    # Long return
    if isinstance(entry_long, list) and isinstance(long_tickers, list):
        returns = []
        for t, ep in zip(long_tickers, entry_long):
            cp = prices.get(t, ep)
            returns.append(cp / ep)
        long_return = sum(returns) / len(returns)
    else:
        cp = prices.get(long_tickers, entry_long)
        long_return = cp / entry_long

    # Short return
    if isinstance(entry_short, list) and isinstance(short_tickers, list):
        returns = []
        for t, ep in zip(short_tickers, entry_short):
            cp = prices.get(t, ep)
            returns.append(cp / ep)
        short_return = sum(returns) / len(returns)
    else:
        cp = prices.get(short_tickers, entry_short)
        short_return = cp / entry_short

    return long_return - short_return


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------
def get_all_pairs():
    conn = _get_db()
    rows = conn.execute("SELECT * FROM pairs ORDER BY sort_order ASC, COALESCE(inception_date, entry_date) DESC").fetchall()
    conn.close()

    pairs = [_row_to_pair(r) for r in rows]

    # Collect unique tickers for open pairs
    tickers = set()
    for p in pairs:
        if p["status"] == "open":
            lt = p["long_ticker"] if isinstance(p["long_ticker"], list) else [p["long_ticker"]]
            st = p["short_ticker"] if isinstance(p["short_ticker"], list) else [p["short_ticker"]]
            tickers.update(lt)
            tickers.update(st)

    prices = get_batch_prices(list(tickers)) if tickers else {}

    result = []
    for p in pairs:
        if p["status"] == "open":
            lt = p["long_ticker"] if isinstance(p["long_ticker"], list) else [p["long_ticker"]]
            st = p["short_ticker"] if isinstance(p["short_ticker"], list) else [p["short_ticker"]]

            if isinstance(p["long_ticker"], list):
                p["current_price_long"] = [prices.get(t, 0) for t in lt]
            else:
                p["current_price_long"] = prices.get(p["long_ticker"], p["entry_price_long"])

            if isinstance(p["short_ticker"], list):
                p["current_price_short"] = [prices.get(t, 0) for t in st]
            else:
                p["current_price_short"] = prices.get(p["short_ticker"], p["entry_price_short"])

            p["performance"] = calculate_performance(
                p["entry_price_long"], p["long_ticker"], prices,
                p["entry_price_short"], p["short_ticker"],
            )
        else:
            # Closed pair: use close prices for performance
            p["current_price_long"] = p.get("close_price_long") or p["entry_price_long"]
            p["current_price_short"] = p.get("close_price_short") or p["entry_price_short"]
            close_prices = {}
            lt = p["long_ticker"] if isinstance(p["long_ticker"], list) else [p["long_ticker"]]
            st = p["short_ticker"] if isinstance(p["short_ticker"], list) else [p["short_ticker"]]
            cpl = p["current_price_long"] if isinstance(p["current_price_long"], list) else [p["current_price_long"]]
            cps = p["current_price_short"] if isinstance(p["current_price_short"], list) else [p["current_price_short"]]
            for t, cp in zip(lt, cpl):
                close_prices[t] = cp
            for t, cp in zip(st, cps):
                close_prices[t] = cp
            p["performance"] = calculate_performance(
                p["entry_price_long"], p["long_ticker"], close_prices,
                p["entry_price_short"], p["short_ticker"],
            )

        result.append(p)

    return result


def create_pair(data):
    long_ticker = data.get("long_ticker", "")
    short_ticker = data.get("short_ticker", "")
    entry_price_long = data.get("entry_price_long", "")
    entry_price_short = data.get("entry_price_short", "")
    inception_date = data.get("inception_date") or None
    closed_date = data.get("closed_date") or None
    close_price_long = data.get("close_price_long") or None
    close_price_short = data.get("close_price_short") or None

    # Parse comma-separated tickers
    if isinstance(long_ticker, str):
        lt = [t.strip().upper() for t in long_ticker.split(",")]
    else:
        lt = long_ticker
    if isinstance(short_ticker, str):
        st = [t.strip().upper() for t in short_ticker.split(",")]
    else:
        st = short_ticker

    # Parse entry prices
    if isinstance(entry_price_long, str):
        lp = [float(p.strip()) for p in entry_price_long.split(",")]
    elif isinstance(entry_price_long, list):
        lp = [float(p) for p in entry_price_long]
    else:
        lp = [float(entry_price_long)]

    if isinstance(entry_price_short, str):
        sp = [float(p.strip()) for p in entry_price_short.split(",")]
    elif isinstance(entry_price_short, list):
        sp = [float(p) for p in entry_price_short]
    else:
        sp = [float(entry_price_short)]

    if len(lt) != len(lp):
        raise ValueError("Number of long tickers must match number of long prices")
    if len(st) != len(sp):
        raise ValueError("Number of short tickers must match number of short prices")

    if any(p <= 0 for p in lp + sp):
        raise ValueError("All prices must be positive")

    # Parse close prices if closed_date is set
    cpl_val = None
    cps_val = None
    if closed_date:
        if close_price_long:
            if isinstance(close_price_long, str):
                cpl = [float(p.strip()) for p in close_price_long.split(",")]
            elif isinstance(close_price_long, list):
                cpl = [float(p) for p in close_price_long]
            else:
                cpl = [float(close_price_long)]
            cpl_val = str(cpl[0]) if len(cpl) == 1 else json.dumps(cpl)

        if close_price_short:
            if isinstance(close_price_short, str):
                cps = [float(p.strip()) for p in close_price_short.split(",")]
            elif isinstance(close_price_short, list):
                cps = [float(p) for p in close_price_short]
            else:
                cps = [float(close_price_short)]
            cps_val = str(cps[0]) if len(cps) == 1 else json.dumps(cps)

    lt_val = lt[0] if len(lt) == 1 else json.dumps(lt)
    st_val = st[0] if len(st) == 1 else json.dumps(st)
    lp_val = str(lp[0]) if len(lp) == 1 else json.dumps(lp)
    sp_val = str(sp[0]) if len(sp) == 1 else json.dumps(sp)

    status = "closed" if closed_date else "open"

    conn = _get_db()
    # Assign next sort_order
    max_order = conn.execute("SELECT COALESCE(MAX(sort_order), 0) FROM pairs").fetchone()[0]
    cur = conn.execute(
        "INSERT INTO pairs (long_ticker, short_ticker, entry_price_long, entry_price_short, entry_date, inception_date, status, closed_date, close_price_long, close_price_short, sort_order) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [lt_val, st_val, lp_val, sp_val, datetime.utcnow().isoformat(), inception_date, status, closed_date, cpl_val, cps_val, max_order + 1],
    )
    pair_id = cur.lastrowid
    conn.commit()
    row = conn.execute("SELECT * FROM pairs WHERE id = ?", [pair_id]).fetchone()
    conn.close()
    return _row_to_pair(row)


def delete_pair(pair_id):
    conn = _get_db()
    cur = conn.execute("DELETE FROM pairs WHERE id = ?", [pair_id])
    conn.commit()
    deleted = cur.rowcount > 0
    conn.close()
    return deleted


def reorder_pairs(ordered_ids):
    """Update sort_order for all pairs based on the given list of IDs."""
    conn = _get_db()
    for idx, pid in enumerate(ordered_ids):
        conn.execute("UPDATE pairs SET sort_order = ? WHERE id = ?", [idx, pid])
    conn.commit()
    conn.close()


def update_pair_close(pair_id, closed_date, close_price_long=None, close_price_short=None):
    """Update the close date (and optionally close prices) of a pair.
    If closed_date is set, status becomes 'closed'. If None/empty, status becomes 'open'.
    """
    conn = _get_db()
    row = conn.execute("SELECT * FROM pairs WHERE id = ?", [pair_id]).fetchone()
    if not row:
        conn.close()
        return None

    if closed_date:
        pair = _row_to_pair(row)
        # Parse close prices if provided
        cpl_val = None
        cps_val = None

        if close_price_long:
            if isinstance(close_price_long, str) and close_price_long.strip():
                cpl = [float(p.strip()) for p in close_price_long.split(",")]
                cpl_val = str(cpl[0]) if len(cpl) == 1 else json.dumps(cpl)
        else:
            # Fetch current prices as close prices
            lt = pair["long_ticker"] if isinstance(pair["long_ticker"], list) else [pair["long_ticker"]]
            st = pair["short_ticker"] if isinstance(pair["short_ticker"], list) else [pair["short_ticker"]]
            prices = get_batch_prices(lt + st)
            if isinstance(pair["long_ticker"], list):
                cpl_val = json.dumps([prices.get(t, 0) for t in lt])
            else:
                cpl_val = str(prices.get(pair["long_ticker"], pair["entry_price_long"]))

        if close_price_short:
            if isinstance(close_price_short, str) and close_price_short.strip():
                cps = [float(p.strip()) for p in close_price_short.split(",")]
                cps_val = str(cps[0]) if len(cps) == 1 else json.dumps(cps)
        else:
            lt = pair["long_ticker"] if isinstance(pair["long_ticker"], list) else [pair["long_ticker"]]
            st = pair["short_ticker"] if isinstance(pair["short_ticker"], list) else [pair["short_ticker"]]
            prices = get_batch_prices(lt + st)
            if isinstance(pair["short_ticker"], list):
                cps_val = json.dumps([prices.get(t, 0) for t in st])
            else:
                cps_val = str(prices.get(pair["short_ticker"], pair["entry_price_short"]))

        conn.execute(
            "UPDATE pairs SET status = 'closed', closed_date = ?, close_price_long = ?, close_price_short = ? WHERE id = ?",
            [closed_date, cpl_val, cps_val, pair_id],
        )
    else:
        # Reopen: clear close fields
        conn.execute(
            "UPDATE pairs SET status = 'open', closed_date = NULL, close_price_long = NULL, close_price_short = NULL WHERE id = ?",
            [pair_id],
        )

    conn.commit()
    updated = conn.execute("SELECT * FROM pairs WHERE id = ?", [pair_id]).fetchone()
    conn.close()
    return _row_to_pair(updated)


def get_pair_history(pair_id, from_iso, to_iso):
    conn = _get_db()
    row = conn.execute("SELECT * FROM pairs WHERE id = ?", [pair_id]).fetchone()
    conn.close()
    if not row:
        return []

    pair = _row_to_pair(row)

    # Determine date boundaries
    from_d = from_iso.split("T")[0] if "T" in from_iso else from_iso[:10]
    inception_d = None
    if pair.get("inception_date"):
        inception_d = pair["inception_date"].split("T")[0] if "T" in pair["inception_date"] else pair["inception_date"][:10]
        # Never show data before inception
        if from_d < inception_d:
            from_d = inception_d

    # For closed pairs, cap the end date at closed_date
    to_d = to_iso.split("T")[0] if "T" in to_iso else to_iso[:10]
    closed_d = None
    if pair.get("status") == "closed" and pair.get("closed_date"):
        closed_d = pair["closed_date"].split("T")[0]
        if closed_d < to_d:
            to_d = closed_d

    # For Yahoo fetch, use inception_date as start if earlier (ensures enough data)
    fetch_from = from_d
    if inception_d and inception_d < from_d:
        fetch_from = inception_d
    fetch_to = to_d + "T23:59:59.000Z"
    fetch_from_iso = fetch_from + "T00:00:00.000Z"

    lt = pair["long_ticker"] if isinstance(pair["long_ticker"], list) else [pair["long_ticker"]]
    st = pair["short_ticker"] if isinstance(pair["short_ticker"], list) else [pair["short_ticker"]]

    # Fetch historical for all tickers
    long_histories = {}
    short_histories = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        long_futs = {t: pool.submit(get_historical_prices, t, fetch_from_iso, fetch_to) for t in lt}
        short_futs = {t: pool.submit(get_historical_prices, t, fetch_from_iso, fetch_to) for t in st}
        for t, f in long_futs.items():
            long_histories[t] = {d["timestamp"].split("T")[0]: d["price"] for d in f.result()}
        for t, f in short_futs.items():
            short_histories[t] = {d["timestamp"].split("T")[0]: d["price"] for d in f.result()}

    if not long_histories.get(lt[0]):
        return []

    all_dates = sorted(long_histories[lt[0]].keys())

    history = []
    for date in all_dates:
        if date < from_d:
            continue
        if date > to_d:
            break
        lp = [long_histories[t].get(date) for t in lt]
        sp = [short_histories[t].get(date) for t in st]
        if all(p is not None for p in lp) and all(p is not None for p in sp):
            price_map = {}
            for t, p in zip(lt, lp):
                price_map[t] = p
            for t, p in zip(st, sp):
                price_map[t] = p
            perf = calculate_performance(
                pair["entry_price_long"], pair["long_ticker"], price_map,
                pair["entry_price_short"], pair["short_ticker"],
            )
            history.append({
                "pair_id": pair_id,
                "performance": perf,
                "timestamp": f"{date}T00:00:00.000Z",
            })

    return history


# Initialize on import
init_db()
