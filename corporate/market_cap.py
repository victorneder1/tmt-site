"""
market_cap.py — Yahoo Finance market cap fetcher with in-memory cache.

Unit consistency guarantee:
  - Yahoo Finance returns marketCap.raw in the asset's native currency.
  - For Brazilian .SA tickers, the native currency is BRL (Real).
  - financial_volume in the movements DB is also stored in BRL.
  - Therefore: pct_market_cap = (financial_volume / marketCap_raw) * 100
    is dimensionally consistent and produces a percentage in BRL/BRL * 100.
  - We validate currency == "BRL" when fetching; if currency differs, the
    market cap is stored as None so the % column shows "—" in the UI.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests


# ---------------------------------------------------------------------------
# Yahoo Finance endpoint and headers
# ---------------------------------------------------------------------------
_YF_SUMMARY_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
_YF_CRUMB_URL   = "https://query1.finance.yahoo.com/v1/test/getcrumb"
_YF_COOKIE_URL  = "https://fc.yahoo.com"
_YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}
_YF_TIMEOUT = 10  # seconds per request
_YF_MAX_WORKERS = 8  # concurrent requests; keeps us well under YF rate limits

# ---------------------------------------------------------------------------
# Known ticker overrides: maps B3 ticker → Yahoo Finance symbol.
# Most Brazilian tickers work fine with the ".SA" suffix.
# Add entries here for any ticker that requires a different Yahoo Finance symbol.
# ---------------------------------------------------------------------------
TICKER_OVERRIDES: dict[str, str] = {
    # Examples (add as needed):
    # "XPTO3": "XPTO3.SA",
}

# Cache TTL: 6 hours (market cap changes slowly)
_CACHE_TTL_SECONDS = 6 * 3600


def _yf_symbol(ticker: str) -> str:
    """Convert a B3 ticker to its Yahoo Finance symbol."""
    if ticker in TICKER_OVERRIDES:
        return TICKER_OVERRIDES[ticker]
    return f"{ticker}.SA"


# ---------------------------------------------------------------------------
# MarketCapCache
# ---------------------------------------------------------------------------
class MarketCapCache:
    """
    Thread-safe in-memory cache for market caps fetched from Yahoo Finance.

    Values are in BRL (native currency for .SA tickers).
    Entries expire after _CACHE_TTL_SECONDS.
    Uses a persistent session with cookie + crumb auth (required by YF since 2024).
    """

    def __init__(self) -> None:
        self._cache: dict[str, dict[str, Any]] = {}  # {ticker: {mcap, ts}}
        self._lock = threading.Lock()
        self._session: requests.Session | None = None
        self._crumb: str | None = None
        self._session_lock = threading.Lock()

    def _ensure_session(self) -> tuple[requests.Session, str] | tuple[None, None]:
        """Return (session, crumb), initialising them if needed. Thread-safe."""
        with self._session_lock:
            if self._session and self._crumb:
                return self._session, self._crumb
            try:
                sess = requests.Session()
                sess.headers.update(_YF_HEADERS)
                sess.get(_YF_COOKIE_URL, timeout=_YF_TIMEOUT)  # sets cookies
                r = sess.get(_YF_CRUMB_URL, timeout=_YF_TIMEOUT)
                if r.status_code == 200 and r.text.strip():
                    self._session = sess
                    self._crumb = r.text.strip()
                    return self._session, self._crumb
            except Exception:
                pass
            return None, None

    def _invalidate_session(self) -> None:
        with self._session_lock:
            self._session = None
            self._crumb = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, ticker: str) -> float | None:
        """Return cached market cap for a single ticker, or None if stale/missing."""
        with self._lock:
            entry = self._cache.get(ticker)
            if entry and (time.time() - entry["ts"]) < _CACHE_TTL_SECONDS:
                return entry["mcap"]
        return None

    def get_batch(self, tickers: list[str]) -> dict[str, float]:
        """
        Return market caps (BRL) for a list of tickers.
        Stale or missing tickers are fetched in parallel from Yahoo Finance.
        Returns {ticker: market_cap_brl}; missing/failed tickers are omitted.
        """
        result: dict[str, float] = {}
        to_fetch: list[str] = []

        with self._lock:
            for t in tickers:
                entry = self._cache.get(t)
                if entry and (time.time() - entry["ts"]) < _CACHE_TTL_SECONDS:
                    if entry["mcap"] is not None:
                        result[t] = entry["mcap"]
                else:
                    to_fetch.append(t)

        if to_fetch:
            fetched = self._fetch_batch(to_fetch)
            with self._lock:
                for t, mcap in fetched.items():
                    self._cache[t] = {"mcap": mcap, "ts": time.time()}
                    if mcap is not None:
                        result[t] = mcap

        return result

    def refresh_all_background(self, tickers: list[str]) -> None:
        """Trigger a background refresh for all given tickers (non-blocking)."""
        def _bg():
            fetched = self._fetch_batch(tickers)
            with self._lock:
                for t, mcap in fetched.items():
                    self._cache[t] = {"mcap": mcap, "ts": time.time()}

        thread = threading.Thread(target=_bg, name="mcap-refresh", daemon=True)
        thread.start()

    # ------------------------------------------------------------------
    # Internal fetch helpers
    # ------------------------------------------------------------------

    def _fetch_batch(self, tickers: list[str]) -> dict[str, float | None]:
        """Fetch market caps for multiple tickers in parallel."""
        results: dict[str, float | None] = {}
        with ThreadPoolExecutor(max_workers=_YF_MAX_WORKERS) as pool:
            futures = {pool.submit(self._fetch_single, t): t for t in tickers}
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    results[ticker] = future.result()
                except Exception:
                    results[ticker] = None
        return results

    def _fetch_single(self, ticker: str) -> float | None:
        """
        Fetch market cap for one ticker from Yahoo Finance.

        Returns the raw market cap in BRL, or None on any failure.
        Uses session + crumb authentication required since 2024.

        Yahoo Finance quoteSummary response structure (modules=price):
          quoteSummary.result[0].price.marketCap.raw   → integer/float, native currency
          quoteSummary.result[0].price.currency        → "BRL" for .SA tickers
        """
        symbol = _yf_symbol(ticker)
        session, crumb = self._ensure_session()
        if not session or not crumb:
            return None
        try:
            resp = session.get(
                _YF_SUMMARY_URL.format(ticker=symbol),
                params={"modules": "price", "crumb": crumb},
                timeout=_YF_TIMEOUT,
            )
            if resp.status_code == 401:
                # Crumb expired — invalidate and retry once
                self._invalidate_session()
                session, crumb = self._ensure_session()
                if not session or not crumb:
                    return None
                resp = session.get(
                    _YF_SUMMARY_URL.format(ticker=symbol),
                    params={"modules": "price", "crumb": crumb},
                    timeout=_YF_TIMEOUT,
                )
            resp.raise_for_status()
            data = resp.json()

            result_list = (
                data.get("quoteSummary", {})
                    .get("result") or []
            )
            if not result_list:
                return None

            price_module = result_list[0].get("price", {})

            # Validate currency: we only use market caps denominated in BRL.
            currency = price_module.get("currency", "")
            if currency and currency.upper() != "BRL":
                return None

            mcap_obj = price_module.get("marketCap", {})
            raw = mcap_obj.get("raw")
            if raw is None or raw == 0:
                return None

            return float(raw)  # BRL, same unit as financial_volume in DB

        except Exception:
            return None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
market_cap_cache = MarketCapCache()
