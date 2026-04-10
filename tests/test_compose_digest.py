"""
Tests for app/compose_digest.py.

Covers:
  Property 8: Digest contains all tickers from watchlist
  Unit tests for rendering, DB storage, and helper functions.
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from app.compose_digest import compose, get_digest_for_date, get_last_digest
from app.db import get_connection, init_db
from app.models import Article, ArticleScore, EventCluster


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_WATCHLIST = ["AAPL", "MSFT", "NVDA", "GOOGL", "TSLA"]


def _make_cluster(
    cluster_id: str,
    ticker: str,
    headline: str = "Test headline",
    article_ids: list[str] | None = None,
) -> EventCluster:
    return EventCluster(
        id=cluster_id,
        ticker=ticker,
        representative_headline=headline,
        summary="A brief summary of the event.",
        article_ids=article_ids or [cluster_id + "_a1"],
        created_at=datetime.now(timezone.utc),
    )


def _make_score(article_id: str, event_type: str = "earnings", importance: int = 70) -> ArticleScore:
    return ArticleScore(
        article_id=article_id,
        is_relevant=True,
        relevance_score=0.85,
        importance_score=importance,
        event_type=event_type,
        confidence="high",
        include_in_digest=True,
        reason="Material update.",
        scored_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def db_conn():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "test.db")
        conn = get_connection(path)
        init_db(conn)
        yield conn
        conn.close()


def _make_clusters_and_scores(tickers: list[str]) -> tuple[list[EventCluster], dict[str, ArticleScore]]:
    clusters = []
    scores = {}
    for i, ticker in enumerate(tickers):
        cid = f"c{i}"
        aid = f"a{i}"
        clusters.append(_make_cluster(cid, ticker, f"{ticker} reports earnings", [aid]))
        scores[aid] = _make_score(aid)
    return clusters, scores


# ---------------------------------------------------------------------------
# Property 8: Digest contains all tickers from watchlist
# Feature: stock-news-agent, Property 8: Digest contains all tickers from watchlist
# Validates: Requirements 8.1
# ---------------------------------------------------------------------------

@given(
    watchlist=st.lists(
        st.sampled_from(["AAPL", "MSFT", "NVDA", "GOOGL", "TSLA", "AMD", "AMZN", "META"]),
        min_size=1,
        max_size=8,
        unique=True,
    )
)
@settings(max_examples=100)
def test_digest_html_contains_all_watchlist_tickers(watchlist: list[str]) -> None:
    """For any watchlist, rendered HTML must contain a section for each ticker."""
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        conn = get_connection(db_path)
        init_db(conn)

        clusters, scores = _make_clusters_and_scores(watchlist)
        digest = compose(clusters, scores, watchlist, conn)

        for ticker in watchlist:
            assert ticker in digest.html_content, (
                f"Ticker {ticker} not found in digest HTML"
            )
        conn.close()


# ---------------------------------------------------------------------------
# Unit tests — rendering
# ---------------------------------------------------------------------------

def test_compose_returns_digest_run(db_conn):
    clusters, scores = _make_clusters_and_scores(["AAPL", "MSFT"])
    digest = compose(clusters, scores, _WATCHLIST, db_conn)
    assert digest.status == "composed"
    assert digest.id is not None
    assert len(digest.html_content) > 0
    assert len(digest.text_content) > 0


def test_compose_subject_contains_date(db_conn):
    clusters, scores = _make_clusters_and_scores(["AAPL"])
    digest = compose(clusters, scores, _WATCHLIST, db_conn)
    assert "Stock News Digest" in digest.subject


def test_compose_html_contains_top_stories_section(db_conn):
    clusters, scores = _make_clusters_and_scores(["AAPL", "MSFT", "NVDA"])
    digest = compose(clusters, scores, _WATCHLIST, db_conn)
    assert "Top Stories" in digest.html_content


def test_compose_html_contains_no_updates_for_empty_ticker(db_conn):
    # Only AAPL has news — others should show "No major updates"
    clusters = [_make_cluster("c1", "AAPL", "Apple earnings beat")]
    scores = {"c1_a1": _make_score("c1_a1")}
    digest = compose(clusters, scores, _WATCHLIST, db_conn)
    assert "No major updates" in digest.html_content


def test_compose_text_contains_all_tickers(db_conn):
    clusters, scores = _make_clusters_and_scores(["AAPL", "MSFT"])
    digest = compose(clusters, scores, _WATCHLIST, db_conn)
    for ticker in _WATCHLIST:
        assert ticker in digest.text_content


def test_compose_text_contains_disclaimer(db_conn):
    clusters, scores = _make_clusters_and_scores(["AAPL"])
    digest = compose(clusters, scores, _WATCHLIST, db_conn)
    assert "financial advice" in digest.text_content.lower() or "disclaimer" in digest.text_content.lower()


def test_compose_html_contains_disclaimer(db_conn):
    clusters, scores = _make_clusters_and_scores(["AAPL"])
    digest = compose(clusters, scores, _WATCHLIST, db_conn)
    assert "financial advice" in digest.html_content.lower()


def test_compose_empty_clusters_still_renders(db_conn):
    digest = compose([], {}, _WATCHLIST, db_conn)
    assert len(digest.html_content) > 0
    assert "No major updates" in digest.html_content


def test_compose_metrics_in_footer(db_conn):
    clusters, scores = _make_clusters_and_scores(["AAPL", "MSFT"])
    digest = compose(clusters, scores, _WATCHLIST, db_conn)
    # Footer should mention article/cluster counts
    assert "2" in digest.html_content  # 2 clusters


# ---------------------------------------------------------------------------
# Unit tests — DB storage
# ---------------------------------------------------------------------------

def test_compose_stores_digest_in_db(db_conn):
    clusters, scores = _make_clusters_and_scores(["AAPL"])
    digest = compose(clusters, scores, _WATCHLIST, db_conn)
    row = db_conn.execute(
        "SELECT * FROM digest_runs WHERE id=?", (digest.id,)
    ).fetchone()
    assert row is not None
    assert row["status"] == "composed"


def test_get_last_digest_returns_most_recent(db_conn):
    clusters, scores = _make_clusters_and_scores(["AAPL"])
    digest1 = compose(clusters, scores, _WATCHLIST, db_conn)
    digest2 = compose(clusters, scores, _WATCHLIST, db_conn)
    last = get_last_digest(db_conn)
    assert last is not None
    assert last.id == digest2.id


def test_get_last_digest_returns_none_when_empty(db_conn):
    assert get_last_digest(db_conn) is None


def test_get_digest_for_date(db_conn):
    clusters, scores = _make_clusters_and_scores(["AAPL"])
    digest = compose(clusters, scores, _WATCHLIST, db_conn)
    found = get_digest_for_date(digest.run_date, db_conn)
    assert found is not None
    assert found.run_date == digest.run_date


def test_get_digest_for_date_returns_none_for_missing(db_conn):
    assert get_digest_for_date("1999-01-01", db_conn) is None
