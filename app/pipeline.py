"""
pipeline.py — Orchestrate the full StockAgent pipeline.

Steps (in order):
    1. fetch      — Fetch last-24h news from Finnhub for each ticker
    2. normalize  — Normalise raw articles and store in DB
    3. score      — Score articles for relevance and importance
    4. cluster    — Deduplicate and cluster related articles
    5. summarize  — Generate extractive summaries for each cluster
    6. compose    — Render HTML + text digest
    7. send       — Send digest via Gmail (skipped on dry_run)

Public API:
    run_full(settings, conn, dry_run, force, fetch_only, compose_only) -> PipelineResult
    run_fetch_only(settings, conn) -> list[Article]
    run_compose_only(settings, conn) -> DigestRun
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.compose_digest import compose, get_last_digest
from app.db import transaction
from app.dedupe_cluster import cluster_and_store
from app.fetch_news import fetch_all
from app.filter_score import score_and_store
from app.logger import get_logger
from app.models import Article, ArticleScore, DigestRun, EventCluster, RunLog
from app.normalize import normalize_and_store
from app.send_email import send_digest
from app.settings import Settings
from app.summarize import summarize_and_store

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class PipelineResult:
    run_id: str
    status: str                          # "success" | "partial" | "failed"
    articles_fetched: int = 0
    articles_stored: int = 0
    articles_scored: int = 0
    clusters_formed: int = 0
    digest: DigestRun | None = None
    errors: list[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None

    def duration_ms(self) -> int | None:
        if self.finished_at is None:
            return None
        return int((self.finished_at - self.started_at).total_seconds() * 1000)


# ---------------------------------------------------------------------------
# Run-log helpers
# ---------------------------------------------------------------------------

def _log_step(
    conn: Any,
    run_id: str,
    step: str,
    status: str,
    duration_ms: int | None = None,
    message: str | None = None,
) -> None:
    """Insert a RunLog record for a pipeline step."""
    entry = RunLog(
        run_id=run_id,
        step=step,
        status=status,
        duration_ms=duration_ms,
        message=message,
        logged_at=datetime.now(timezone.utc),
    )
    d = entry.to_dict()
    try:
        conn.execute(
            """
            INSERT INTO run_logs
                (run_id, step, status, duration_ms, message, logged_at)
            VALUES
                (:run_id, :step, :status, :duration_ms, :message, :logged_at)
            """,
            d,
        )
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not write run_log for step %s: %s", step, exc)


def _timed(fn, *args, **kwargs):
    """Call fn(*args, **kwargs), return (result, elapsed_ms)."""
    t0 = time.monotonic()
    result = fn(*args, **kwargs)
    elapsed = int((time.monotonic() - t0) * 1000)
    return result, elapsed


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_full(
    settings: Settings,
    conn: Any,
    dry_run: bool = False,
    force: bool = False,
    fetch_only: bool = False,
    compose_only: bool = False,
) -> PipelineResult:
    """Run the complete pipeline.

    Args:
        settings:     Application settings.
        conn:         SQLite connection (must have schema initialised).
        dry_run:      Complete all steps but skip the email send.
        force:        Send even if a digest was already sent today.
        fetch_only:   Only fetch and store articles; skip scoring onward.
        compose_only: Skip fetch/score; compose digest from existing DB data.

    Returns:
        PipelineResult summarising what happened.
    """
    run_id = str(uuid.uuid4())[:8]
    result = PipelineResult(run_id=run_id, status="success")

    log.info(
        "Pipeline started [run_id=%s] dry_run=%s force=%s fetch_only=%s compose_only=%s",
        run_id, dry_run, force, fetch_only, compose_only,
    )

    # ------------------------------------------------------------------
    # STEP 1 — Fetch (skipped in compose_only mode)
    # ------------------------------------------------------------------
    all_articles: list[Article] = []

    if not compose_only:
        try:
            raw_by_ticker, elapsed = _timed(fetch_all, settings)
            total_raw = sum(len(v) for v in raw_by_ticker.values())
            result.articles_fetched = total_raw
            _log_step(conn, run_id, "fetch", "ok", elapsed,
                      f"Fetched {total_raw} raw articles across {len(settings.watchlist)} tickers")
            log.info("[%s] fetch: %d raw articles in %dms", run_id, total_raw, elapsed)
        except Exception as exc:  # noqa: BLE001
            msg = f"Fetch step failed: {exc}"
            log.error("[%s] %s", run_id, msg)
            result.errors.append(msg)
            result.status = "failed"
            result.finished_at = datetime.now(timezone.utc)
            _log_step(conn, run_id, "fetch", "error", message=msg)
            return result

        # ------------------------------------------------------------------
        # STEP 2 — Normalise
        # ------------------------------------------------------------------
        try:
            for ticker, raw_articles in raw_by_ticker.items():
                try:
                    articles = normalize_and_store(raw_articles, ticker, conn)
                    all_articles.extend(articles)
                except Exception as exc:  # noqa: BLE001
                    msg = f"Normalise failed for {ticker}: {exc}"
                    log.error("[%s] %s", run_id, msg)
                    result.errors.append(msg)

            result.articles_stored = len(all_articles)
            _log_step(conn, run_id, "normalize", "ok",
                      message=f"Stored {len(all_articles)} articles")
            log.info("[%s] normalize: %d articles stored", run_id, len(all_articles))
        except Exception as exc:  # noqa: BLE001
            msg = f"Normalise step failed: {exc}"
            log.error("[%s] %s", run_id, msg)
            result.errors.append(msg)
            result.status = "partial"

        if fetch_only:
            result.finished_at = datetime.now(timezone.utc)
            log.info("[%s] fetch-only mode — stopping after normalise.", run_id)
            _log_step(conn, run_id, "pipeline", "ok",
                      result.duration_ms(), "fetch-only complete")
            return result

        # ------------------------------------------------------------------
        # STEP 3 — Score
        # ------------------------------------------------------------------
        try:
            scores, elapsed = _timed(score_and_store, all_articles, conn)
            result.articles_scored = len(scores)
            _log_step(conn, run_id, "score", "ok", elapsed,
                      f"Scored {len(scores)} articles")
            log.info("[%s] score: %d articles scored in %dms", run_id, len(scores), elapsed)
        except Exception as exc:  # noqa: BLE001
            msg = f"Score step failed: {exc}"
            log.error("[%s] %s", run_id, msg)
            result.errors.append(msg)
            result.status = "partial"
            scores = []

        # ------------------------------------------------------------------
        # STEP 4 — Cluster
        # ------------------------------------------------------------------
        try:
            clusters, elapsed = _timed(cluster_and_store, all_articles, conn)
            result.clusters_formed = len(clusters)
            _log_step(conn, run_id, "cluster", "ok", elapsed,
                      f"Formed {len(clusters)} clusters")
            log.info("[%s] cluster: %d clusters in %dms", run_id, len(clusters), elapsed)
        except Exception as exc:  # noqa: BLE001
            msg = f"Cluster step failed: {exc}"
            log.error("[%s] %s", run_id, msg)
            result.errors.append(msg)
            result.status = "partial"
            clusters = []

        # ------------------------------------------------------------------
        # STEP 5 — Summarise
        # ------------------------------------------------------------------
        try:
            articles_by_id = {a.id: a for a in all_articles}
            clusters, elapsed = _timed(summarize_and_store, clusters, articles_by_id, conn)
            _log_step(conn, run_id, "summarize", "ok", elapsed,
                      f"Summarised {len(clusters)} clusters")
            log.info("[%s] summarize: %d clusters in %dms", run_id, len(clusters), elapsed)
        except Exception as exc:  # noqa: BLE001
            msg = f"Summarise step failed: {exc}"
            log.error("[%s] %s", run_id, msg)
            result.errors.append(msg)
            result.status = "partial"

    else:
        # compose_only: load existing clusters from DB
        clusters = _load_clusters_from_db(conn)
        scores = _load_scores_from_db(conn)
        log.info("[%s] compose-only: loaded %d clusters from DB", run_id, len(clusters))

    # ------------------------------------------------------------------
    # STEP 6 — Compose digest
    # ------------------------------------------------------------------
    scores_by_article_id: dict[str, ArticleScore] = {}
    if not compose_only:
        scores_by_article_id = {s.article_id: s for s in scores}
    else:
        scores_by_article_id = {s.article_id: s for s in scores}

    try:
        digest, elapsed = _timed(
            compose,
            clusters,
            scores_by_article_id,
            settings.watchlist,
            conn,
            settings.max_top_stories,
        )
        result.digest = digest
        _log_step(conn, run_id, "compose", "ok", elapsed,
                  f"Digest composed: {digest.subject}")
        log.info("[%s] compose: digest ready in %dms", run_id, elapsed)
    except Exception as exc:  # noqa: BLE001
        msg = f"Compose step failed: {exc}"
        log.error("[%s] %s", run_id, msg)
        result.errors.append(msg)
        result.status = "failed"
        result.finished_at = datetime.now(timezone.utc)
        _log_step(conn, run_id, "compose", "error", message=msg)
        return result

    # ------------------------------------------------------------------
    # STEP 7 — Send (skipped on dry_run)
    # ------------------------------------------------------------------
    if dry_run:
        log.info("[%s] dry-run mode — skipping email send.", run_id)
        _log_step(conn, run_id, "send", "skipped", message="dry-run")
    else:
        try:
            digest, elapsed = _timed(send_digest, result.digest, settings, conn, force)
            result.digest = digest
            _log_step(conn, run_id, "send", digest.status, elapsed,
                      f"Email status: {digest.status}")
            log.info("[%s] send: status=%s in %dms", run_id, digest.status, elapsed)
        except Exception as exc:  # noqa: BLE001
            msg = f"Send step failed: {exc}"
            log.error("[%s] %s", run_id, msg)
            result.errors.append(msg)
            result.status = "partial"
            _log_step(conn, run_id, "send", "error", message=msg)

    result.finished_at = datetime.now(timezone.utc)
    if result.errors and result.status == "success":
        result.status = "partial"

    _log_step(conn, run_id, "pipeline", result.status, result.duration_ms(),
              f"Pipeline complete. Errors: {len(result.errors)}")
    log.info(
        "[%s] Pipeline finished: status=%s duration=%sms errors=%d",
        run_id, result.status, result.duration_ms(), len(result.errors),
    )
    return result


# ---------------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------------

def run_fetch_only(settings: Settings, conn: Any) -> list[Article]:
    """Fetch and normalise articles only. Returns stored Article list."""
    result = run_full(settings, conn, fetch_only=True)
    return []  # articles are in DB; caller can query if needed


def run_compose_only(settings: Settings, conn: Any) -> DigestRun | None:
    """Compose digest from existing DB data. Returns DigestRun."""
    result = run_full(settings, conn, compose_only=True, dry_run=True)
    return result.digest


def run_send_last(settings: Settings, conn: Any, force: bool = False) -> DigestRun | None:
    """Send the most recently composed digest."""
    digest = get_last_digest(conn)
    if digest is None:
        log.warning("No digest found to send. Run 'compose-only' first.")
        return None
    return send_digest(digest, settings, conn, force=force)


# ---------------------------------------------------------------------------
# DB helpers for compose_only mode
# ---------------------------------------------------------------------------

def _load_clusters_from_db(conn: Any) -> list[EventCluster]:
    from app.models import EventCluster
    rows = conn.execute("SELECT * FROM event_clusters ORDER BY created_at DESC").fetchall()
    return [EventCluster.from_row(r) for r in rows]


def _load_scores_from_db(conn: Any) -> list[ArticleScore]:
    from app.models import ArticleScore
    rows = conn.execute("SELECT * FROM article_scores").fetchall()
    return [ArticleScore.from_row(r) for r in rows]
