"""
Tests for app/filter_score.py.

Covers:
  Property 3: Scoring output conforms to schema
  Unit tests for event type detection, confidence levels, DB storage.
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from app.db import get_connection, init_db
from app.filter_score import (
    VALID_CONFIDENCE_LEVELS,
    VALID_EVENT_TYPES,
    RulesScoringBackend,
    score_and_store,
    score_article,
)
from app.models import Article


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_article(
    headline: str = "Apple reports record earnings",
    summary: str = "",
    ticker: str = "AAPL",
    article_id: str = "test001",
) -> Article:
    now = datetime.now(timezone.utc)
    return Article(
        id=article_id,
        ticker=ticker,
        headline=headline,
        summary=summary,
        source="Reuters",
        url="https://reuters.com/test",
        published_at=now,
        fetched_at=now,
        raw_json="{}",
    )


# ---------------------------------------------------------------------------
# Property 3: Scoring output conforms to schema
# Feature: stock-news-agent, Property 3: Scoring output conforms to schema
# Validates: Requirements 5.1, 5.2, 5.3
# ---------------------------------------------------------------------------

_article_strategy = st.builds(
    _make_article,
    headline=st.text(max_size=300),
    summary=st.text(max_size=500),
    ticker=st.sampled_from(["AAPL", "MSFT", "NVDA", "GOOGL", "TSLA"]),
    article_id=st.text(min_size=1, max_size=16),
)


@given(article=_article_strategy)
@settings(max_examples=100)
def test_scoring_output_conforms_to_schema(article: Article) -> None:
    """For any Article, score() returns ArticleScore with all fields in valid ranges."""
    score = score_article(article)

    # relevance_score in [0.0, 1.0]
    assert 0.0 <= score.relevance_score <= 1.0, (
        f"relevance_score out of range: {score.relevance_score}"
    )

    # importance_score in [0, 100]
    assert 0 <= score.importance_score <= 100, (
        f"importance_score out of range: {score.importance_score}"
    )

    # event_type in defined set
    assert score.event_type in VALID_EVENT_TYPES, (
        f"Unknown event_type: {score.event_type}"
    )

    # confidence in defined set
    assert score.confidence in VALID_CONFIDENCE_LEVELS, (
        f"Unknown confidence: {score.confidence}"
    )

    # boolean fields are actual bools
    assert isinstance(score.is_relevant, bool)
    assert isinstance(score.include_in_digest, bool)

    # reason is a non-empty string
    assert isinstance(score.reason, str) and len(score.reason) > 0

    # article_id matches input
    assert score.article_id == article.id

    # scored_at is a UTC-aware datetime
    assert isinstance(score.scored_at, datetime)
    assert score.scored_at.tzinfo is not None


# ---------------------------------------------------------------------------
# Unit tests — event type detection
# ---------------------------------------------------------------------------

def test_earnings_event_detected():
    article = _make_article(
        headline="Apple reports record quarterly earnings, beats EPS estimates"
    )
    score = score_article(article)
    assert score.event_type == "earnings"
    assert score.is_relevant is True


def test_merger_event_detected():
    article = _make_article(
        headline="Microsoft announces acquisition of gaming company for $10B"
    )
    score = score_article(article)
    assert score.event_type == "merger"
    assert score.is_relevant is True


def test_regulation_event_detected():
    article = _make_article(
        headline="FTC launches antitrust investigation into Google"
    )
    score = score_article(article)
    assert score.event_type == "regulation"
    assert score.is_relevant is True


def test_executive_event_detected():
    article = _make_article(
        headline="Tesla CEO Elon Musk to step down as board chair"
    )
    score = score_article(article)
    assert score.event_type == "executive"
    assert score.is_relevant is True


def test_irrelevant_article_low_score():
    article = _make_article(
        headline="Weather forecast for the weekend",
        summary="Sunny skies expected across the region.",
    )
    score = score_article(article)
    assert score.is_relevant is False
    assert score.include_in_digest is False
    assert score.relevance_score < 0.4


def test_empty_headline_scores_as_irrelevant():
    article = _make_article(headline="", summary="")
    score = score_article(article)
    assert score.is_relevant is False
    assert score.relevance_score == 0.0


# ---------------------------------------------------------------------------
# Unit tests — confidence levels
# ---------------------------------------------------------------------------

def test_high_confidence_for_strong_keywords():
    article = _make_article(
        headline="Apple Q3 earnings beat estimates, revenue up 12%, guidance raised"
    )
    score = score_article(article)
    assert score.confidence == "high"


def test_low_confidence_for_weak_signal():
    article = _make_article(headline="Tech stocks move higher today")
    score = score_article(article)
    assert score.confidence in {"low", "medium"}


# ---------------------------------------------------------------------------
# Unit tests — include_in_digest logic
# ---------------------------------------------------------------------------

def test_include_in_digest_requires_both_thresholds():
    # High relevance + high importance → include
    article = _make_article(
        headline="NVIDIA reports record earnings, beats EPS estimates by 20%"
    )
    score = score_article(article)
    assert score.include_in_digest is True


def test_is_relevant_consistent_with_include_in_digest():
    article = _make_article(headline="Random unrelated news story about weather")
    score = score_article(article)
    # include_in_digest can only be True if is_relevant is True
    if score.include_in_digest:
        assert score.is_relevant is True


# ---------------------------------------------------------------------------
# Unit tests — custom backend interface
# ---------------------------------------------------------------------------

def test_custom_backend_is_used():
    from app.filter_score import ScoringBackend
    from app.models import ArticleScore

    class AlwaysRelevantBackend(ScoringBackend):
        def score(self, article: Article) -> ArticleScore:
            return ArticleScore(
                article_id=article.id,
                is_relevant=True,
                relevance_score=1.0,
                importance_score=100,
                event_type="other",
                confidence="high",
                include_in_digest=True,
                reason="Always relevant.",
                scored_at=datetime.now(timezone.utc),
            )

    article = _make_article(headline="Boring news")
    score = score_article(article, backend=AlwaysRelevantBackend())
    assert score.is_relevant is True
    assert score.relevance_score == 1.0


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


def test_score_and_store_inserts_scores(db_conn):
    articles = [
        _make_article(headline="Apple earnings beat estimates", article_id="a1"),
        _make_article(headline="Microsoft acquires startup", article_id="a2"),
    ]
    # Insert articles first (FK constraint)
    for a in articles:
        db_conn.execute(
            "INSERT OR IGNORE INTO articles VALUES (?,?,?,?,?,?,?,?,?)",
            (a.id, a.ticker, a.headline, a.summary, a.source, a.url,
             a.published_at.isoformat(), a.fetched_at.isoformat(), a.raw_json),
        )
    db_conn.commit()

    scores = score_and_store(articles, db_conn)
    assert len(scores) == 2

    row = db_conn.execute("SELECT COUNT(*) as c FROM article_scores").fetchone()
    assert row["c"] == 2


def test_score_and_store_skips_duplicate(db_conn):
    article = _make_article(headline="Apple earnings beat estimates", article_id="a1")
    db_conn.execute(
        "INSERT OR IGNORE INTO articles VALUES (?,?,?,?,?,?,?,?,?)",
        (article.id, article.ticker, article.headline, article.summary,
         article.source, article.url,
         article.published_at.isoformat(), article.fetched_at.isoformat(),
         article.raw_json),
    )
    db_conn.commit()

    score_and_store([article], db_conn)
    score_and_store([article], db_conn)  # second call — should not duplicate

    row = db_conn.execute("SELECT COUNT(*) as c FROM article_scores").fetchone()
    assert row["c"] == 1
