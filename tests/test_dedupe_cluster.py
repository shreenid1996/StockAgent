"""
Tests for app/dedupe_cluster.py.

Covers:
  Property 4: Deduplication does not lose articles
  Unit tests for clustering logic and DB storage.
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from app.db import get_connection, init_db
from app.dedupe_cluster import DEFAULT_THRESHOLD, cluster_and_store, cluster_articles
from app.models import Article


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_article(
    article_id: str,
    headline: str,
    ticker: str = "AAPL",
) -> Article:
    now = datetime.now(timezone.utc)
    return Article(
        id=article_id,
        ticker=ticker,
        headline=headline,
        summary="",
        source="Reuters",
        url=f"https://reuters.com/{article_id}",
        published_at=now,
        fetched_at=now,
        raw_json="{}",
    )


# ---------------------------------------------------------------------------
# Property 4: Deduplication does not lose articles
# Feature: stock-news-agent, Property 4: Deduplication does not lose articles
# Validates: Requirements 6.1, 6.3
# ---------------------------------------------------------------------------

@st.composite
def _unique_article_list(draw):
    """Generate a list of articles with guaranteed unique IDs."""
    n = draw(st.integers(min_value=0, max_value=20))
    articles = []
    for i in range(n):
        headline = draw(st.text(max_size=150))
        ticker = draw(st.sampled_from(["AAPL", "MSFT", "NVDA"]))
        articles.append(_make_article(str(i), headline, ticker))
    return articles


@given(articles=_unique_article_list())
@settings(max_examples=100)
def test_clustering_does_not_lose_articles(articles: list[Article]) -> None:
    """Total article count across all clusters must equal input count."""
    clusters = cluster_articles(articles, threshold=DEFAULT_THRESHOLD)
    total_in_clusters = sum(len(c.article_ids) for c in clusters)
    assert total_in_clusters == len(articles), (
        f"Input had {len(articles)} articles but clusters contain {total_in_clusters}"
    )


@given(articles=_unique_article_list())
@settings(max_examples=100)
def test_clustering_no_article_duplicated(articles: list[Article]) -> None:
    """Each article ID appears in exactly one cluster."""
    clusters = cluster_articles(articles, threshold=DEFAULT_THRESHOLD)
    all_ids = [aid for c in clusters for aid in c.article_ids]
    assert len(all_ids) == len(set(all_ids)), "Duplicate article IDs found across clusters"


# ---------------------------------------------------------------------------
# Unit tests — clustering behaviour
# ---------------------------------------------------------------------------

def test_empty_input_returns_empty():
    assert cluster_articles([]) == []


def test_single_article_forms_one_cluster():
    articles = [_make_article("a1", "Apple reports record earnings")]
    clusters = cluster_articles(articles)
    assert len(clusters) == 1
    assert clusters[0].article_ids == ["a1"]


def test_identical_headlines_cluster_together():
    headline = "Apple reports record quarterly earnings beat"
    articles = [
        _make_article("a1", headline),
        _make_article("a2", headline),
    ]
    clusters = cluster_articles(articles, threshold=0.5)
    assert len(clusters) == 1
    assert set(clusters[0].article_ids) == {"a1", "a2"}


def test_dissimilar_headlines_form_separate_clusters():
    articles = [
        _make_article("a1", "Apple reports record earnings"),
        _make_article("a2", "Tesla recalls vehicles over safety concerns"),
        _make_article("a3", "NVIDIA launches new GPU architecture"),
    ]
    clusters = cluster_articles(articles, threshold=0.5)
    assert len(clusters) == 3


def test_similar_headlines_cluster_together():
    articles = [
        _make_article("a1", "Apple Q3 earnings beat analyst estimates"),
        _make_article("a2", "Apple Q3 earnings beat Wall Street estimates"),
        _make_article("a3", "Tesla recalls vehicles over brake failure"),
    ]
    clusters = cluster_articles(articles, threshold=0.3)
    # a1 and a2 should cluster together, a3 separate
    assert len(clusters) == 2
    cluster_sizes = sorted(len(c.article_ids) for c in clusters)
    assert cluster_sizes == [1, 2]


def test_threshold_zero_clusters_all_together():
    """Threshold of 0.0 means any overlap groups articles together."""
    articles = [
        _make_article("a1", "Apple earnings"),
        _make_article("a2", "Apple revenue"),
        _make_article("a3", "Apple stock"),
    ]
    # All share "apple" → all cluster together at threshold 0.0
    clusters = cluster_articles(articles, threshold=0.0)
    assert len(clusters) == 1
    assert len(clusters[0].article_ids) == 3


def test_threshold_one_never_clusters():
    """Threshold of 1.0 means only identical headlines cluster."""
    articles = [
        _make_article("a1", "Apple earnings beat"),
        _make_article("a2", "Apple earnings miss"),
    ]
    clusters = cluster_articles(articles, threshold=1.0)
    assert len(clusters) == 2


def test_cluster_representative_headline_is_first_article():
    articles = [
        _make_article("a1", "Apple Q3 earnings beat analyst estimates"),
        _make_article("a2", "Apple Q3 earnings beat Wall Street estimates"),
    ]
    clusters = cluster_articles(articles, threshold=0.3)
    assert clusters[0].representative_headline == articles[0].headline


def test_cluster_ticker_matches_representative():
    articles = [_make_article("a1", "Apple earnings", ticker="AAPL")]
    clusters = cluster_articles(articles)
    assert clusters[0].ticker == "AAPL"


def test_total_articles_preserved_mixed_similarity():
    articles = [
        _make_article("a1", "Apple reports record earnings beat"),
        _make_article("a2", "Apple reports record earnings miss"),
        _make_article("a3", "Tesla recalls vehicles over safety"),
        _make_article("a4", "Tesla recalls cars over brake failure"),
        _make_article("a5", "NVIDIA launches new AI chip"),
    ]
    clusters = cluster_articles(articles, threshold=0.3)
    total = sum(len(c.article_ids) for c in clusters)
    assert total == 5


# ---------------------------------------------------------------------------
# DB storage tests
# ---------------------------------------------------------------------------

@pytest.fixture
def db_conn():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "test.db")
        conn = get_connection(path)
        init_db(conn)
        yield conn
        conn.close()


def test_cluster_and_store_persists_clusters(db_conn):
    articles = [
        _make_article("a1", "Apple earnings beat estimates"),
        _make_article("a2", "Tesla recalls vehicles"),
    ]
    clusters = cluster_and_store(articles, db_conn)
    assert len(clusters) == 2

    row = db_conn.execute("SELECT COUNT(*) as c FROM event_clusters").fetchone()
    assert row["c"] == 2


def test_cluster_and_store_article_ids_round_trip(db_conn):
    articles = [
        _make_article("a1", "Apple Q3 earnings beat analyst estimates"),
        _make_article("a2", "Apple Q3 earnings beat Wall Street estimates"),
    ]
    clusters = cluster_and_store(articles, db_conn, threshold=0.3)

    row = db_conn.execute(
        "SELECT article_ids FROM event_clusters WHERE id=?", (clusters[0].id,)
    ).fetchone()
    import json
    stored_ids = json.loads(row["article_ids"])
    assert set(stored_ids) == set(clusters[0].article_ids)


def test_cluster_and_store_empty_input(db_conn):
    clusters = cluster_and_store([], db_conn)
    assert clusters == []
    row = db_conn.execute("SELECT COUNT(*) as c FROM event_clusters").fetchone()
    assert row["c"] == 0
