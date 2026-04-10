"""
compose_digest.py — Render the daily investor digest using Jinja2 templates.

Public API:
    compose(clusters, scores_by_article_id, watchlist, settings, conn) -> DigestRun
    get_last_digest(conn) -> DigestRun | None
"""
from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from app.logger import get_logger
from app.models import ArticleScore, DigestRun, EventCluster
from app.utils import truncate

log = get_logger(__name__)

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
_MAX_TOP_STORIES = 5
_SUBJECT_PREFIX = "📈 Stock News Digest"


# ---------------------------------------------------------------------------
# Jinja2 environment
# ---------------------------------------------------------------------------

def _get_env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(_TEMPLATES_DIR)),
        autoescape=select_autoescape(["html"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )


# ---------------------------------------------------------------------------
# Template context builder
# ---------------------------------------------------------------------------

def _build_cluster_ctx(
    cluster: EventCluster,
    scores_by_article_id: dict[str, ArticleScore],
) -> dict[str, Any]:
    """Build a template-friendly dict for a single cluster."""
    # Pick the best score from member articles for event_type / source info
    best_score: ArticleScore | None = None
    for aid in cluster.article_ids:
        s = scores_by_article_id.get(aid)
        if s and (best_score is None or s.importance_score > best_score.importance_score):
            best_score = s

    return {
        "id": cluster.id,
        "ticker": cluster.ticker,
        "representative_headline": cluster.representative_headline,
        "summary": cluster.summary or "",
        "article_ids": cluster.article_ids,
        "article_count": len(cluster.article_ids),
        "event_type": best_score.event_type if best_score else "other",
        "importance_score": best_score.importance_score if best_score else 0,
        "url": "",   # Finnhub articles don't always have a canonical URL in clusters
        "source": "",
    }


def _build_context(
    clusters: list[EventCluster],
    scores_by_article_id: dict[str, ArticleScore],
    watchlist: list[str],
    max_top_stories: int = _MAX_TOP_STORIES,
) -> dict[str, Any]:
    """Build the full Jinja2 template context dict."""
    now = datetime.now(timezone.utc)
    run_date = now.strftime("%B %d, %Y")
    generated_at = now.strftime("%Y-%m-%d %H:%M UTC")

    # Build cluster context objects
    cluster_ctxs = [_build_cluster_ctx(c, scores_by_article_id) for c in clusters]

    # Group by ticker
    clusters_by_ticker: dict[str, list[dict]] = {t: [] for t in watchlist}
    for ctx in cluster_ctxs:
        ticker = ctx["ticker"]
        if ticker in clusters_by_ticker:
            clusters_by_ticker[ticker].append(ctx)
        # clusters for tickers not in watchlist are silently skipped

    # Sort each ticker's clusters by importance descending
    for ticker in clusters_by_ticker:
        clusters_by_ticker[ticker].sort(key=lambda c: c["importance_score"], reverse=True)

    # Top stories: highest importance across all tickers, capped
    all_sorted = sorted(cluster_ctxs, key=lambda c: c["importance_score"], reverse=True)
    top_stories = all_sorted[:max_top_stories]

    total_articles = sum(c["article_count"] for c in cluster_ctxs)
    relevant_articles = sum(
        1 for s in scores_by_article_id.values() if s.include_in_digest
    )

    return {
        "run_date": run_date,
        "generated_at": generated_at,
        "watchlist": watchlist,
        "clusters_by_ticker": clusters_by_ticker,
        "top_stories": top_stories,
        "total_articles": total_articles,
        "relevant_articles": relevant_articles,
        "total_clusters": len(clusters),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compose(
    clusters: list[EventCluster],
    scores_by_article_id: dict[str, ArticleScore],
    watchlist: list[str],
    conn: sqlite3.Connection,
    max_top_stories: int = _MAX_TOP_STORIES,
) -> DigestRun:
    """Render HTML + plain-text digest and persist to digest_runs table.

    Args:
        clusters:              All EventCluster objects for this run.
        scores_by_article_id:  Mapping of article_id -> ArticleScore.
        watchlist:             Ordered list of ticker symbols.
        conn:                  SQLite connection.
        max_top_stories:       Max clusters in the top-stories section.

    Returns:
        DigestRun with status='composed' and rendered content.
    """
    env = _get_env()
    ctx = _build_context(clusters, scores_by_article_id, watchlist, max_top_stories)

    run_date_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    subject = f"{_SUBJECT_PREFIX} — {ctx['run_date']}"
    ctx["subject"] = subject

    html_content = env.get_template("digest.html").render(**ctx)
    text_content = env.get_template("digest.txt").render(**ctx)

    digest = DigestRun(
        id=str(uuid.uuid4()),
        run_date=run_date_iso,
        subject=subject,
        html_content=html_content,
        text_content=text_content,
        sent_at=None,
        recipient=None,
        status="composed",
    )

    _upsert_digest_run(digest, conn)
    log.info("Digest composed: %s (%d clusters, %d top stories)", subject, len(clusters), len(ctx["top_stories"]))
    return digest


def get_last_digest(conn: sqlite3.Connection) -> DigestRun | None:
    """Return the most recently composed DigestRun, or None if none exist."""
    row = conn.execute(
        "SELECT * FROM digest_runs ORDER BY run_date DESC, rowid DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    return DigestRun.from_row(row)


def get_digest_for_date(date_str: str, conn: sqlite3.Connection) -> DigestRun | None:
    """Return the DigestRun for a specific date (YYYY-MM-DD), or None."""
    row = conn.execute(
        "SELECT * FROM digest_runs WHERE run_date = ? ORDER BY rowid DESC LIMIT 1",
        (date_str,),
    ).fetchone()
    if row is None:
        return None
    return DigestRun.from_row(row)


def _upsert_digest_run(digest: DigestRun, conn: sqlite3.Connection) -> None:
    d = digest.to_dict()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO digest_runs
                (id, run_date, subject, html_content, text_content,
                 sent_at, recipient, status)
            VALUES
                (:id, :run_date, :subject, :html_content, :text_content,
                 :sent_at, :recipient, :status)
            """,
            d,
        )
        conn.commit()
    except sqlite3.Error as exc:
        log.error("DB error storing digest run: %s", exc)
        conn.rollback()
        raise
