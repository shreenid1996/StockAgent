"""
Unit tests for app/fetch_news.py.

All tests use stubs — no live Finnhub API calls are made.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.fetch_news import _save_raw, _window, fetch_all, fetch_ticker
from app.settings import Settings


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_settings(tmp_path: Path) -> Settings:
    """Return a minimal Settings object pointing at a temp data dir."""
    return Settings(
        finnhub_api_key="test_key",
        gmail_sender="a@b.com",
        gmail_recipient="b@c.com",
        gmail_credentials_path="creds.json",
        gmail_token_path="token.json",
        watchlist=["AAPL", "MSFT"],
        db_path=str(tmp_path / "test.db"),
        data_dir=str(tmp_path / "data"),
        log_dir=str(tmp_path / "logs"),
        log_retention_days=7,
        request_delay_seconds=0.0,
        relevance_threshold=0.4,
        importance_threshold=30,
        scoring_backend="rules",
        max_top_stories=5,
        max_articles_per_ticker=3,
        run_time="07:00",
    )


_SAMPLE_ARTICLES = [
    {
        "id": 1001,
        "headline": "Apple hits record high",
        "summary": "AAPL stock surged today.",
        "source": "Reuters",
        "url": "https://reuters.com/aapl",
        "datetime": 1700000000,
        "related": "AAPL",
        "image": "",
        "category": "company",
    }
]


# ---------------------------------------------------------------------------
# _window
# ---------------------------------------------------------------------------

def test_window_returns_two_ints():
    from_ts, to_ts = _window()
    assert isinstance(from_ts, int)
    assert isinstance(to_ts, int)


def test_window_24h_gap():
    from_ts, to_ts = _window()
    assert 86390 <= (to_ts - from_ts) <= 86410  # ~24 hours ± 10s


# ---------------------------------------------------------------------------
# _save_raw
# ---------------------------------------------------------------------------

def test_save_raw_creates_file(tmp_path):
    path = _save_raw(str(tmp_path), "AAPL", _SAMPLE_ARTICLES)
    assert path.exists()
    loaded = json.loads(path.read_text())
    assert loaded == _SAMPLE_ARTICLES


def test_save_raw_filename_contains_ticker_and_date(tmp_path):
    path = _save_raw(str(tmp_path), "NVDA", _SAMPLE_ARTICLES)
    assert "NVDA" in path.name
    # date portion is YYYY-MM-DD
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert date_str in path.name


def test_save_raw_empty_list(tmp_path):
    path = _save_raw(str(tmp_path), "TSLA", [])
    assert path.exists()
    assert json.loads(path.read_text()) == []


# ---------------------------------------------------------------------------
# fetch_all — stubbed client
# ---------------------------------------------------------------------------

def test_fetch_all_returns_results_for_all_tickers(tmp_path):
    settings = _make_settings(tmp_path)

    with patch("app.fetch_news._build_client") as mock_build:
        mock_client = MagicMock()
        mock_client.company_news.return_value = _SAMPLE_ARTICLES
        mock_build.return_value = mock_client

        results = fetch_all(settings)

    assert set(results.keys()) == {"AAPL", "MSFT"}
    assert results["AAPL"] == _SAMPLE_ARTICLES
    assert results["MSFT"] == _SAMPLE_ARTICLES


def test_fetch_all_continues_on_ticker_error(tmp_path):
    settings = _make_settings(tmp_path)

    def side_effect(ticker, _from, to):
        if ticker == "AAPL":
            raise RuntimeError("API error")
        return _SAMPLE_ARTICLES

    with patch("app.fetch_news._build_client") as mock_build:
        mock_client = MagicMock()
        mock_client.company_news.side_effect = side_effect
        mock_build.return_value = mock_client

        results = fetch_all(settings)

    # AAPL failed → empty list, MSFT succeeded
    assert results["AAPL"] == []
    assert results["MSFT"] == _SAMPLE_ARTICLES


def test_fetch_all_saves_raw_files(tmp_path):
    settings = _make_settings(tmp_path)

    with patch("app.fetch_news._build_client") as mock_build:
        mock_client = MagicMock()
        mock_client.company_news.return_value = _SAMPLE_ARTICLES
        mock_build.return_value = mock_client

        fetch_all(settings)

    data_dir = Path(settings.data_dir)
    files = list(data_dir.glob("*.json"))
    assert len(files) == 2  # one per ticker


def test_fetch_all_empty_response_stored_as_empty_list(tmp_path):
    settings = _make_settings(tmp_path)

    with patch("app.fetch_news._build_client") as mock_build:
        mock_client = MagicMock()
        mock_client.company_news.return_value = []
        mock_build.return_value = mock_client

        results = fetch_all(settings)

    assert results["AAPL"] == []


# ---------------------------------------------------------------------------
# fetch_ticker — single ticker convenience wrapper
# ---------------------------------------------------------------------------

def test_fetch_ticker_returns_articles(tmp_path):
    settings = _make_settings(tmp_path)

    with patch("app.fetch_news._build_client") as mock_build:
        mock_client = MagicMock()
        mock_client.company_news.return_value = _SAMPLE_ARTICLES
        mock_build.return_value = mock_client

        result = fetch_ticker(settings, "AAPL")

    assert result == _SAMPLE_ARTICLES


def test_fetch_ticker_returns_empty_on_error(tmp_path):
    settings = _make_settings(tmp_path)

    with patch("app.fetch_news._build_client") as mock_build:
        mock_client = MagicMock()
        mock_client.company_news.side_effect = Exception("network error")
        mock_build.return_value = mock_client

        result = fetch_ticker(settings, "AAPL")

    assert result == []
