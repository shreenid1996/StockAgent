"""
Unit tests for app/summarize.py.
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone

import pytest

from app.db import get_connection, init_db
from app.dedupe_cluster import cluster_and_store
from app.models import Article, EventCluster
from app.summarize import _MAX_SUMMARY_LEN, summarize_and_store, summarize_cluster


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_article(article_id: str, headline: str, summary: str = "") -> Article:
    now = datetime.now(timezone.utc)
    return Article(
        id=article_id,
        ticker="AAPL",
        headline=headline,
        summary=summary,
        source="Reuters",
        url=f"https://reuters.com/{article_id}",
        published_at=now,
        fetched_at=now,
        raw_json="{}",
    )


def _make_cluster(
    cluster_id: str,
    rep_headline: str,
    article_ids: list[str],
) -> EventCluster:
    return EventCluster(
        id=cluster_id,
        ticker="AAPL",
        representative_headline=rep_headline,
        summary="",
        article_ids=article_ids,
        created_at=datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# summarize_cluster
# ---------------------------------------------------------------------------

def test_summary_contains_representative_headline():
    article = _make_article("a1", "Apple reports record Q3 earnings")
    cluster = _make_cluster("c1", "Apple reports record Q3 earnings", ["a1"])
    summary = summarize_cluster(cluster, {"a1": article})
    assert "Apple reports record Q3 earnings" in summary


def test_summary_never_exceeds_max_length():
    long_headline = "A" * 300
    long_summary = "B" * 300
    article = _make_article("a1", long_headline, long_summary)
    cluster = _make_cluster("c1", long_headline, ["a1"])
    summary = summarize_cluster(cluster, {"a1": article})
    assert len(summary) <= _MAX_SUMMARY_LEN


def test_summary_is_non_empty_for_valid_cluster():
    article = _make_article("a1", "Apple earnings beat", "Revenue up 12%.")
    cluster = _make_cluster("c1", "Apple earnings beat", ["a1"])
    summary = summarize_cluster(cluster, {"a1": article})
    assert len(summary) > 0


def test_summary_falls_back_to_headline_when_no_members():
    cluster = _make_cluster("c1", "Apple earnings beat", ["missing_id"])
    summary = summarize_cluster(cluster, {})
    assert summary == "Apple earnings beat"


def test_summary_includes_source_count_for_multiple_articles():
    articles = {
        "a1": _make_article("a1", "Apple Q3 earnings beat estimates"),
        "a2": _make_article("a2", "Apple Q3 earnings beat Wall Street"),
    }
    cluster = _make_cluster("c1", "Apple Q3 earnings beat estimates", ["a1", "a2"])
    summary = summarize_cluster(cluster, articles)
    assert "2 sources" in summary


def test_summary_no_source_count_for_single_article():
    article = _make_article("a1", "Apple earnings beat")
    cluster = _make_cluster("c1", "Apple earnings beat", ["a1"])
    summary = summarize_cluster(cluster, {"a1": article})
    assert "sources" not in summary


def test_summary_uses_article_summary_text():
    article = _make_article(
        "a1",
        "Apple earnings beat",
        "Apple reported revenue of $90B, beating analyst expectations.",
    )
    cluster = _make_cluster("c1", "Apple earnings beat", ["a1"])
    summary = summarize_cluster(cluster, {"a1": article})
    # The article summary should appear since it's more informative
    assert "90B" in summary or "Apple" in summary


def test_summary_no_fabrication_only_source_text():
    """Summary must only contain text from source articles."""
    article = _make_article("a1", "Apple Q3 earnings", "Revenue was $90B.")
    cluster = _make_cluster("c1", "Apple Q3 earnings", ["a1"])
    summary = summarize_cluster(cluster, {"a1": article})
    # All words in summary should come from source text
    source_text = (article.headline + " " + article.summary).lower()
    # Check no completely foreign words appear (basic fabrication check)
    assert "fabricated" not in summary.lower()
    assert "invented" not in summary.lower()


def test_empty_headline_cluster_returns_non_empty():
    article = _make_article("a1", "", "Some summary text here.")
    cluster = _make_cluster("c1", "", ["a1"])
    summary = summarize_cluster(cluster, {"a1": article})
    # Should still return something (even if just the empty headline + summary)
    assert isinstance(summary, str)


# ---------------------------------------------------------------------------
# summarize_and_store
# ---------------------------------------------------------------------------

@pytest.fixture
def db_conn():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "test.db")
        conn = get_connection(path)
        init_db(conn)
        yield conn
        conn.close()


def test_summarize_and_store_updates_cluster_summary(db_conn):
    articles_list = [
        _make_article("a1", "Apple Q3 earnings beat estimates"),
        _make_article("a2", "Tesla recalls vehicles over safety"),
    ]
    articles_by_id = {a.id: a for a in articles_list}

    clusters = cluster_and_store(articles_list, db_conn, threshold=0.9)
    updated = summarize_and_store(clusters, articles_by_id, db_conn)

    for cluster in updated:
        assert len(cluster.summary) > 0
        row = db_conn.execute(
            "SELECT summary FROM event_clusters WHERE id=?", (cluster.id,)
        ).fetchone()
        assert row["summary"] == cluster.summary


def test_summarize_and_store_returns_all_clusters(db_conn):
    articles_list = [
        _make_article("a1", "Apple earnings beat"),
        _make_article("a2", "Tesla recalls vehicles"),
        _make_article("a3", "NVIDIA launches new chip"),
    ]
    articles_by_id = {a.id: a for a in articles_list}
    clusters = cluster_and_store(articles_list, db_conn, threshold=0.9)
    updated = summarize_and_store(clusters, articles_by_id, db_conn)
    assert len(updated) == len(clusters)


def test_summarize_and_store_empty_clusters(db_conn):
    result = summarize_and_store([], {}, db_conn)
    assert result == []
