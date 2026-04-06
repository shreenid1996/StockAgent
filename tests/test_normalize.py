"""
Tests for app/normalize.py.

Covers:
  Property 1: Article normalization produces a valid schema
  Property 2: Normalization is idempotent on ID generation
  Unit tests for edge cases and DB deduplication.
"""
from __future__ import annotations

import sqlite3
import tempfile
import os
from datetime import datetime, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from app.db import get_connection, init_db
from app.normalize import generate_article_id, normalize_article, normalize_and_store


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# A well-formed Finnhub article dict
_finnhub_article = st.fixed_dictionaries({
    "headline": st.text(min_size=1, max_size=200),
    "summary": st.text(max_size=500),
    "source": st.text(max_size=100),
    "url": st.text(min_size=1, max_size=300),
    "datetime": st.integers(min_value=1_000_000_000, max_value=4_000_000_000),
    "id": st.integers(min_value=1),
    "image": st.just(""),
    "category": st.just("company"),
    "related": st.text(max_size=10),
})

_ticker = st.sampled_from(["AAPL", "MSFT", "NVDA", "GOOGL", "TSLA"])


# ---------------------------------------------------------------------------
# Property 1: Article normalization produces a valid schema
# Feature: stock-news-agent, Property 1: Article normalization produces a valid schema
# Validates: Requirements 4.1
# ---------------------------------------------------------------------------

@given(raw=_finnhub_article, ticker=_ticker)
@settings(max_examples=100)
def test_normalize_produces_valid_schema(raw: dict, ticker: str) -> None:
    """For any well-formed Finnhub dict, normalize_article returns an Article
    with all required fields non-empty and published_at as a valid datetime."""
    article = normalize_article(raw, ticker)

    # Required string fields must be non-empty
    assert isinstance(article.id, str) and len(article.id) > 0
    assert isinstance(article.ticker, str) and len(article.ticker) > 0
    assert isinstance(article.headline, str)   # may be empty after clean_html on whitespace
    assert isinstance(article.summary, str)
    assert isinstance(article.source, str)
    assert isinstance(article.url, str)
    assert isinstance(article.raw_json, str) and len(article.raw_json) > 0

    # published_at must be a UTC-aware datetime
    assert isinstance(article.published_at, datetime)
    assert article.published_at.tzinfo is not None

    # fetched_at must be a UTC-aware datetime
    assert isinstance(article.fetched_at, datetime)
    assert article.fetched_at.tzinfo is not None

    # ticker must match input
    assert article.ticker == ticker


# ---------------------------------------------------------------------------
# Property 2: Normalization is idempotent on ID generation
# Feature: stock-news-agent, Property 2: Normalization is idempotent on ID generation
# Validates: Requirements 4.1
# ---------------------------------------------------------------------------

@given(
    ticker=_ticker,
    url=st.text(max_size=200),
    ts=st.integers(min_value=1_000_000_000, max_value=4_000_000_000),
)
@settings(max_examples=100)
def test_id_generation_is_deterministic(ticker: str, url: str, ts: int) -> None:
    """Same inputs always produce the same article ID."""
    id1 = generate_article_id(ticker, url, ts)
    id2 = generate_article_id(ticker, url, ts)
    assert id1 == id2


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def _sample_raw(
    headline="Apple hits record high",
    url="https://reuters.com/aapl",
    ts=1700000000,
) -> dict:
    return {
        "headline": headline,
        "summary": "AAPL stock surged today.",
        "source": "Reuters",
        "url": url,
        "datetime": ts,
        "id": 1001,
        "image": "",
        "category": "company",
        "related": "AAPL",
    }


def test_normalize_article_basic():
    raw = _sample_raw()
    article = normalize_article(raw, "AAPL")
    assert article.ticker == "AAPL"
    assert article.headline == "Apple hits record high"
    assert article.source == "Reuters"
    assert article.url == "https://reuters.com/aapl"
    assert article.published_at == datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)


def test_normalize_article_id_is_16_hex_chars():
    article = normalize_article(_sample_raw(), "AAPL")
    assert len(article.id) == 16
    assert all(c in "0123456789abcdef" for c in article.id)


def test_normalize_article_strips_html_from_headline():
    raw = _sample_raw(headline="<b>Apple</b> hits <i>record</i> high")
    article = normalize_article(raw, "AAPL")
    assert "<b>" not in article.headline
    assert "Apple" in article.headline


def test_normalize_article_missing_headline_defaults_empty():
    raw = _sample_raw()
    raw["headline"] = ""
    article = normalize_article(raw, "AAPL")
    assert article.headline == ""


def test_normalize_article_missing_datetime_raises():
    raw = _sample_raw()
    del raw["datetime"]
    with pytest.raises(KeyError):
        normalize_article(raw, "AAPL")


def test_normalize_article_different_tickers_different_ids():
    raw = _sample_raw()
    id_aapl = normalize_article(raw, "AAPL").id
    id_msft = normalize_article(raw, "MSFT").id
    assert id_aapl != id_msft


def test_normalize_article_different_urls_different_ids():
    raw1 = _sample_raw(url="https://a.com/1")
    raw2 = _sample_raw(url="https://a.com/2")
    assert normalize_article(raw1, "AAPL").id != normalize_article(raw2, "AAPL").id


# ---------------------------------------------------------------------------
# DB deduplication tests
# ---------------------------------------------------------------------------

@pytest.fixture
def db_conn():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "test.db")
        conn = get_connection(path)
        init_db(conn)
        yield conn
        conn.close()


def test_normalize_and_store_inserts_articles(db_conn):
    articles = normalize_and_store([_sample_raw()], "AAPL", db_conn)
    assert len(articles) == 1
    row = db_conn.execute("SELECT COUNT(*) as c FROM articles").fetchone()
    assert row["c"] == 1


def test_normalize_and_store_skips_duplicate(db_conn):
    raw = _sample_raw()
    normalize_and_store([raw], "AAPL", db_conn)
    normalize_and_store([raw], "AAPL", db_conn)  # second insert of same article
    row = db_conn.execute("SELECT COUNT(*) as c FROM articles").fetchone()
    assert row["c"] == 1  # still only one record


def test_normalize_and_store_skips_malformed(db_conn):
    bad = {"headline": "No datetime field"}
    articles = normalize_and_store([bad], "AAPL", db_conn)
    assert articles == []
    row = db_conn.execute("SELECT COUNT(*) as c FROM articles").fetchone()
    assert row["c"] == 0


def test_normalize_and_store_multiple_articles(db_conn):
    raws = [
        _sample_raw(url="https://a.com/1", ts=1700000001),
        _sample_raw(url="https://a.com/2", ts=1700000002),
        _sample_raw(url="https://a.com/3", ts=1700000003),
    ]
    articles = normalize_and_store(raws, "AAPL", db_conn)
    assert len(articles) == 3
    row = db_conn.execute("SELECT COUNT(*) as c FROM articles").fetchone()
    assert row["c"] == 3
