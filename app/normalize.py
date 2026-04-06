"""
normalize.py — Transform raw Finnhub article dicts into Article dataclasses
and persist them to the SQLite articles table.

Public API:
    generate_article_id(ticker, url, published_at_ts) -> str
    normalize_article(raw, ticker) -> Article
    normalize_and_store(raw_articles, ticker, conn) -> list[Article]
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from app.logger import get_logger
from app.models import Article
from app.utils import clean_html, unix_to_datetime

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# ID generation
# ---------------------------------------------------------------------------

def generate_article_id(ticker: str, url: str, published_at_ts: int) -> str:
    """Generate a deterministic, collision-resistant article ID.

    Uses SHA-256 over the concatenation of ticker, url, and published_at
    timestamp so the same article always gets the same ID regardless of
    when it is fetched.

    Returns the first 16 hex characters (64-bit prefix) — sufficient for
    deduplication within a single watchlist.
    """
    raw = f"{ticker}|{url}|{published_at_ts}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Single-article normalisation
# ---------------------------------------------------------------------------

def normalize_article(raw: dict[str, Any], ticker: str) -> Article:
    """Transform a raw Finnhub article dict into an Article dataclass.

    Finnhub company-news fields used:
        id        (int)   — Finnhub internal ID (not used for our ID)
        headline  (str)
        summary   (str)
        source    (str)
        url       (str)
        datetime  (int)   — Unix timestamp of publication
        image     (str)   — ignored
        category  (str)   — ignored
        related   (str)   — ignored

    Raises:
        KeyError  if required fields (headline, datetime) are missing.
        ValueError if datetime is not a valid integer.
    """
    published_ts = int(raw["datetime"])
    url = raw.get("url") or ""
    headline = clean_html(raw.get("headline") or "").strip()
    summary = clean_html(raw.get("summary") or "").strip()
    source = (raw.get("source") or "").strip()

    article_id = generate_article_id(ticker, url, published_ts)
    published_at = unix_to_datetime(published_ts)
    fetched_at = datetime.now(timezone.utc)

    return Article(
        id=article_id,
        ticker=ticker,
        headline=headline,
        summary=summary,
        source=source,
        url=url,
        published_at=published_at,
        fetched_at=fetched_at,
        raw_json=json.dumps(raw, ensure_ascii=False),
    )


# ---------------------------------------------------------------------------
# Batch normalise + store
# ---------------------------------------------------------------------------

def normalize_and_store(
    raw_articles: list[dict[str, Any]],
    ticker: str,
    conn: sqlite3.Connection,
) -> list[Article]:
    """Normalise a list of raw Finnhub dicts and insert into the articles table.

    - Skips articles that already exist (same id) — no duplicates.
    - Skips malformed articles (logs a warning and continues).

    Returns the list of successfully normalised Article objects (including
    pre-existing ones that were skipped on insert).
    """
    articles: list[Article] = []

    for raw in raw_articles:
        try:
            article = normalize_article(raw, ticker)
        except (KeyError, ValueError, TypeError) as exc:
            log.warning("Skipping malformed article for %s: %s | raw=%s", ticker, exc, raw)
            continue

        articles.append(article)
        _insert_if_new(article, conn)

    log.debug("Normalised %d articles for %s", len(articles), ticker)
    return articles


def _insert_if_new(article: Article, conn: sqlite3.Connection) -> bool:
    """Insert article into DB. Returns True if inserted, False if already existed."""
    d = article.to_dict()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO articles
                (id, ticker, headline, summary, source, url,
                 published_at, fetched_at, raw_json)
            VALUES
                (:id, :ticker, :headline, :summary, :source, :url,
                 :published_at, :fetched_at, :raw_json)
            """,
            d,
        )
        conn.commit()
        inserted = conn.execute(
            "SELECT changes() AS c"
        ).fetchone()["c"]
        if inserted:
            log.debug("Inserted article %s (%s)", article.id, article.ticker)
            return True
        else:
            log.debug("Skipped duplicate article %s (%s)", article.id, article.ticker)
            return False
    except sqlite3.Error as exc:
        log.error("DB error inserting article %s: %s", article.id, exc)
        conn.rollback()
        return False
