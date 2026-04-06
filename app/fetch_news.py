"""
fetch_news.py — Fetch last-24h company news from Finnhub for each watchlist ticker.

Public API:
    fetch_all(settings) -> dict[str, list[dict]]
        Returns a mapping of ticker -> list of raw Finnhub article dicts.
        Saves each raw response to app/data/{TICKER}_{YYYY-MM-DD}.json.
        Continues on per-ticker errors (logs and skips).
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import finnhub

from app.logger import get_logger
from app.settings import Settings
from app.utils import datetime_to_unix

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_client(api_key: str) -> finnhub.Client:
    return finnhub.Client(api_key=api_key)


def _window() -> tuple[int, int]:
    """Return (from_ts, to_ts) covering the last 24 hours as Unix timestamps."""
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=24)
    return datetime_to_unix(since), datetime_to_unix(now)


def _save_raw(data_dir: str, ticker: str, articles: list[dict]) -> Path:
    """Persist raw API response to disk as JSON. Returns the file path."""
    out_dir = Path(data_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path = out_dir / f"{ticker}_{date_str}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    log.debug("Saved raw response for %s -> %s (%d articles)", ticker, out_path, len(articles))
    return out_path


def _fetch_ticker(
    client: finnhub.Client,
    ticker: str,
    from_ts: int,
    to_ts: int,
) -> list[dict]:
    """Fetch company news for a single ticker. Returns list of article dicts.

    Raises on API errors so the caller can decide how to handle them.
    """
    raw: list[dict] = client.company_news(ticker, _from=from_ts, to=to_ts)  # type: ignore[attr-defined]
    return raw if raw else []


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_all(settings: Settings) -> dict[str, list[dict[str, Any]]]:
    """Fetch news for every ticker in the watchlist.

    - Saves each raw response to ``{data_dir}/{TICKER}_{date}.json``.
    - Skips a ticker and logs the error if the API call fails.
    - Respects ``settings.request_delay_seconds`` between requests.

    Returns:
        dict mapping ticker -> list of raw Finnhub article dicts
        (empty list for tickers that failed or returned no news).
    """
    client = _build_client(settings.finnhub_api_key)
    from_ts, to_ts = _window()
    results: dict[str, list[dict]] = {}

    log.info(
        "Fetching news for %d tickers (window: last 24h)",
        len(settings.watchlist),
    )

    for i, ticker in enumerate(settings.watchlist):
        try:
            articles = _fetch_ticker(client, ticker, from_ts, to_ts)
            _save_raw(settings.data_dir, ticker, articles)
            results[ticker] = articles
            log.info("Fetched %d articles for %s", len(articles), ticker)
        except Exception as exc:  # noqa: BLE001
            log.error("Failed to fetch news for %s: %s", ticker, exc)
            results[ticker] = []

        # Rate-limit delay between requests (skip after last ticker)
        if i < len(settings.watchlist) - 1 and settings.request_delay_seconds > 0:
            time.sleep(settings.request_delay_seconds)

    total = sum(len(v) for v in results.values())
    log.info("Fetch complete. Total articles fetched: %d", total)
    return results


def fetch_ticker(settings: Settings, ticker: str) -> list[dict[str, Any]]:
    """Fetch news for a single ticker. Convenience wrapper used by tests and CLI.

    Returns empty list on error (logs the exception).
    """
    client = _build_client(settings.finnhub_api_key)
    from_ts, to_ts = _window()
    try:
        articles = _fetch_ticker(client, ticker, from_ts, to_ts)
        _save_raw(settings.data_dir, ticker, articles)
        log.info("Fetched %d articles for %s", len(articles), ticker)
        return articles
    except Exception as exc:  # noqa: BLE001
        log.error("Failed to fetch news for %s: %s", ticker, exc)
        return []
