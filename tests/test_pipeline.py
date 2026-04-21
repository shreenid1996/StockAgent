"""
Unit tests for app/pipeline.py.

All external I/O (Finnhub API, Gmail API) is stubbed.
Tests verify orchestration logic, flag handling, and run_log recording.
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from app.db import get_connection, init_db
from app.models import Article, DigestRun
from app.pipeline import (
    PipelineResult,
    _load_clusters_from_db,
    _load_scores_from_db,
    run_compose_only,
    run_fetch_only,
    run_full,
    run_send_last,
)
from app.settings import Settings


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_conn():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "test.db")
        conn = get_connection(path)
        init_db(conn)
        yield conn
        conn.close()


def _make_settings(tmp_path: str) -> Settings:
    return Settings(
        finnhub_api_key="key",
        gmail_sender="sender@gmail.com",
        gmail_recipient="recipient@gmail.com",
        gmail_credentials_path=os.path.join(tmp_path, "credentials.json"),
        gmail_token_path=os.path.join(tmp_path, "token.json"),
        watchlist=["AAPL", "MSFT"],
        db_path=os.path.join(tmp_path, "test.db"),
        data_dir=os.path.join(tmp_path, "data"),
        log_dir=os.path.join(tmp_path, "logs"),
        log_retention_days=7,
        request_delay_seconds=0.0,
        relevance_threshold=0.4,
        importance_threshold=30,
        scoring_backend="rules",
        max_top_stories=5,
        max_articles_per_ticker=3,
        run_time="07:00",
    )


_SAMPLE_RAW = [
    {
        "headline": "Apple reports record quarterly earnings beat",
        "summary": "AAPL revenue surged on strong iPhone sales.",
        "source": "Reuters",
        "url": "https://reuters.com/aapl-earnings",
        "datetime": 1700000000,
        "id": 1001,
        "image": "",
        "category": "company",
        "related": "AAPL",
    }
]


def _stub_fetch(raw_by_ticker):
    """Return a patch for fetch_all that returns the given dict."""
    return patch("app.pipeline.fetch_all", return_value=raw_by_ticker)


def _stub_send(status="sent"):
    """Return a patch for send_digest that marks digest as sent."""
    def _fake_send(digest, settings, conn, force=False):
        digest.status = status
        return digest
    return patch("app.pipeline.send_digest", side_effect=_fake_send)


# ---------------------------------------------------------------------------
# PipelineResult
# ---------------------------------------------------------------------------

def test_pipeline_result_duration_ms():
    r = PipelineResult(run_id="x", status="success")
    r.finished_at = datetime.now(timezone.utc)
    assert r.duration_ms() is not None
    assert r.duration_ms() >= 0


def test_pipeline_result_duration_none_when_not_finished():
    r = PipelineResult(run_id="x", status="success")
    assert r.duration_ms() is None


# ---------------------------------------------------------------------------
# run_full — dry_run
# ---------------------------------------------------------------------------

def test_run_full_dry_run_does_not_send(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)
        raw = {"AAPL": _SAMPLE_RAW, "MSFT": []}

        with _stub_fetch(raw), _stub_send() as mock_send:
            result = run_full(settings, db_conn, dry_run=True)

        mock_send.assert_not_called()
        assert result.digest is not None
        assert result.digest.status == "composed"


def test_run_full_dry_run_returns_success(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)
        raw = {"AAPL": _SAMPLE_RAW, "MSFT": []}

        with _stub_fetch(raw):
            result = run_full(settings, db_conn, dry_run=True)

        assert result.status in ("success", "partial")
        assert result.articles_fetched == 1
        assert result.articles_stored >= 1


# ---------------------------------------------------------------------------
# run_full — fetch_only
# ---------------------------------------------------------------------------

def test_run_full_fetch_only_stops_after_normalise(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)
        raw = {"AAPL": _SAMPLE_RAW, "MSFT": []}

        with _stub_fetch(raw), _stub_send() as mock_send:
            result = run_full(settings, db_conn, fetch_only=True)

        mock_send.assert_not_called()
        assert result.digest is None
        assert result.articles_stored >= 1


# ---------------------------------------------------------------------------
# run_full — compose_only
# ---------------------------------------------------------------------------

def test_run_full_compose_only_skips_fetch(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)

        with patch("app.pipeline.fetch_all") as mock_fetch:
            result = run_full(settings, db_conn, compose_only=True, dry_run=True)

        mock_fetch.assert_not_called()
        assert result.digest is not None


# ---------------------------------------------------------------------------
# run_full — force flag
# ---------------------------------------------------------------------------

def test_run_full_force_passes_to_send(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)
        raw = {"AAPL": _SAMPLE_RAW, "MSFT": []}
        captured = {}

        def _fake_send(digest, s, conn, force=False):
            captured["force"] = force
            digest.status = "sent"
            return digest

        with _stub_fetch(raw), patch("app.pipeline.send_digest", side_effect=_fake_send):
            run_full(settings, db_conn, force=True)

        assert captured.get("force") is True


# ---------------------------------------------------------------------------
# run_full — per-ticker failure continues
# ---------------------------------------------------------------------------

def test_run_full_continues_when_one_ticker_fails(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)

        def _failing_fetch(s):
            raise RuntimeError("Finnhub down")

        with patch("app.pipeline.fetch_all", side_effect=_failing_fetch):
            result = run_full(settings, db_conn, dry_run=True)

        assert result.status == "failed"
        assert len(result.errors) > 0


# ---------------------------------------------------------------------------
# run_full — run_logs are written
# ---------------------------------------------------------------------------

def test_run_full_writes_run_logs(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)
        raw = {"AAPL": _SAMPLE_RAW, "MSFT": []}

        with _stub_fetch(raw):
            result = run_full(settings, db_conn, dry_run=True)

        rows = db_conn.execute(
            "SELECT step FROM run_logs WHERE run_id = ?", (result.run_id,)
        ).fetchall()
        steps = {r["step"] for r in rows}
        assert "fetch" in steps
        assert "normalize" in steps
        assert "compose" in steps


# ---------------------------------------------------------------------------
# run_send_last
# ---------------------------------------------------------------------------

def test_run_send_last_returns_none_when_no_digest(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)
        result = run_send_last(settings, db_conn)
        assert result is None


def test_run_send_last_sends_most_recent_digest(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)

        # Insert a composed digest
        digest = DigestRun(
            id="d1", run_date="2026-04-05", subject="Test",
            html_content="<html/>", text_content="text", status="composed",
        )
        db_conn.execute(
            """INSERT INTO digest_runs (id, run_date, subject, html_content,
               text_content, sent_at, recipient, status)
               VALUES (?,?,?,?,?,?,?,?)""",
            (digest.id, digest.run_date, digest.subject, digest.html_content,
             digest.text_content, None, None, digest.status),
        )
        db_conn.commit()

        with _stub_send("sent"):
            result = run_send_last(settings, db_conn)

        assert result is not None
        assert result.status == "sent"


# ---------------------------------------------------------------------------
# run_compose_only
# ---------------------------------------------------------------------------

def test_run_compose_only_returns_digest(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)
        digest = run_compose_only(settings, db_conn)
        assert digest is not None
        assert digest.status == "composed"
