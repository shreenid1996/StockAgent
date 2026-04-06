"""
db.py — SQLite connection management and schema initialisation for StockAgent.

Usage:
    from app.db import get_connection, init_db, transaction

    conn = get_connection("/path/to/stock_news.db")
    init_db(conn)

    with transaction(conn) as cur:
        cur.execute("INSERT INTO watchlist ...")
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from app.logger import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# DDL — all six tables
# ---------------------------------------------------------------------------

_DDL = """
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS watchlist (
    ticker    TEXT PRIMARY KEY,
    added_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS articles (
    id           TEXT PRIMARY KEY,
    ticker       TEXT NOT NULL,
    headline     TEXT NOT NULL,
    summary      TEXT,
    source       TEXT,
    url          TEXT,
    published_at TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    raw_json     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS article_scores (
    article_id        TEXT PRIMARY KEY REFERENCES articles(id),
    is_relevant       INTEGER NOT NULL,
    relevance_score   REAL    NOT NULL,
    importance_score  INTEGER NOT NULL,
    event_type        TEXT    NOT NULL,
    confidence        TEXT    NOT NULL,
    include_in_digest INTEGER NOT NULL,
    reason            TEXT    NOT NULL,
    scored_at         TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS event_clusters (
    id                     TEXT PRIMARY KEY,
    ticker                 TEXT NOT NULL,
    representative_headline TEXT NOT NULL,
    summary                TEXT,
    article_ids            TEXT NOT NULL,
    created_at             TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS digest_runs (
    id           TEXT PRIMARY KEY,
    run_date     TEXT NOT NULL,
    subject      TEXT NOT NULL,
    html_content TEXT,
    text_content TEXT,
    sent_at      TEXT,
    recipient    TEXT,
    status       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT    NOT NULL,
    step        TEXT    NOT NULL,
    status      TEXT    NOT NULL,
    duration_ms INTEGER,
    message     TEXT,
    logged_at   TEXT    NOT NULL
);
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_connection(db_path: str) -> sqlite3.Connection:
    """Open (or create) a SQLite database at *db_path* and return the connection.

    Enables WAL mode and foreign-key enforcement.
    Raises if the parent directory does not exist.
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row          # rows accessible by column name
    log.debug("Opened SQLite connection: %s", db_path)
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create all tables if they do not already exist.

    Safe to call multiple times (idempotent).
    """
    conn.executescript(_DDL)
    conn.commit()
    log.info("Database schema initialised (all tables ensured).")


@contextmanager
def transaction(conn: sqlite3.Connection) -> Generator[sqlite3.Cursor, None, None]:
    """Context manager that yields a cursor inside a transaction.

    Commits on clean exit; rolls back on any exception.

    Example::

        with transaction(conn) as cur:
            cur.execute("INSERT INTO watchlist VALUES (?, ?)", ("AAPL", "2026-01-01"))
    """
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
