"""
Unit tests for app/send_email.py.

All tests stub out the Gmail API — no real network calls are made.
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from unittest.mock import MagicMock, patch

import pytest

from app.db import get_connection, init_db
from app.models import DigestRun
from app.send_email import (
    _already_sent_today,
    _encode_message,
    _update_digest_status,
    build_mime_message,
    send_digest,
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
        watchlist=["AAPL"],
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


def _make_digest(run_date: str = "2026-04-05", status: str = "composed") -> DigestRun:
    return DigestRun(
        id="test-digest-id",
        run_date=run_date,
        subject=f"Stock News Digest — {run_date}",
        html_content="<html><body>Test</body></html>",
        text_content="Test digest plain text",
        sent_at=None,
        recipient=None,
        status=status,
    )


def _insert_digest(conn, digest: DigestRun) -> None:
    d = digest.to_dict()
    conn.execute(
        """INSERT INTO digest_runs
           (id, run_date, subject, html_content, text_content, sent_at, recipient, status)
           VALUES (:id, :run_date, :subject, :html_content, :text_content,
                   :sent_at, :recipient, :status)""",
        d,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# build_mime_message
# ---------------------------------------------------------------------------

def test_build_mime_message_returns_multipart():
    msg = build_mime_message("a@b.com", "c@d.com", "Subject", "<b>html</b>", "plain")
    assert isinstance(msg, MIMEMultipart)
    assert msg["Subject"] == "Subject"
    assert msg["From"] == "a@b.com"
    assert msg["To"] == "c@d.com"


def test_build_mime_message_has_two_parts():
    msg = build_mime_message("a@b.com", "c@d.com", "Subject", "<b>html</b>", "plain")
    parts = msg.get_payload()
    assert len(parts) == 2
    content_types = [p.get_content_type() for p in parts]
    assert "text/plain" in content_types
    assert "text/html" in content_types


# ---------------------------------------------------------------------------
# _encode_message
# ---------------------------------------------------------------------------

def test_encode_message_returns_dict_with_raw_key():
    msg = build_mime_message("a@b.com", "c@d.com", "Subject", "<b>html</b>", "plain")
    encoded = _encode_message(msg)
    assert "raw" in encoded
    assert isinstance(encoded["raw"], str)
    assert len(encoded["raw"]) > 0


# ---------------------------------------------------------------------------
# _already_sent_today
# ---------------------------------------------------------------------------

def test_already_sent_today_false_when_no_records(db_conn):
    assert _already_sent_today(db_conn, "2026-04-05") is False


def test_already_sent_today_false_when_only_composed(db_conn):
    digest = _make_digest(run_date="2026-04-05", status="composed")
    _insert_digest(db_conn, digest)
    assert _already_sent_today(db_conn, "2026-04-05") is False


def test_already_sent_today_true_when_sent(db_conn):
    digest = _make_digest(run_date="2026-04-05", status="sent")
    _insert_digest(db_conn, digest)
    assert _already_sent_today(db_conn, "2026-04-05") is True


def test_already_sent_today_false_for_different_date(db_conn):
    digest = _make_digest(run_date="2026-04-04", status="sent")
    _insert_digest(db_conn, digest)
    assert _already_sent_today(db_conn, "2026-04-05") is False


# ---------------------------------------------------------------------------
# _update_digest_status
# ---------------------------------------------------------------------------

def test_update_digest_status_updates_record(db_conn):
    digest = _make_digest()
    _insert_digest(db_conn, digest)
    now = datetime.now(timezone.utc)
    _update_digest_status(db_conn, digest.id, "sent", now, "r@example.com")
    row = db_conn.execute(
        "SELECT status, recipient FROM digest_runs WHERE id = ?", (digest.id,)
    ).fetchone()
    assert row["status"] == "sent"
    assert row["recipient"] == "r@example.com"


# ---------------------------------------------------------------------------
# send_digest — stubbed Gmail API
# ---------------------------------------------------------------------------

def test_send_digest_sends_successfully(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)
        digest = _make_digest()
        _insert_digest(db_conn, digest)

        mock_service = MagicMock()
        mock_service.users().messages().send().execute.return_value = {"id": "msg123"}

        with patch("app.send_email.build_gmail_service", return_value=mock_service):
            result = send_digest(digest, settings, db_conn, force=False)

        assert result.status == "sent"
        assert result.sent_at is not None
        assert result.recipient == "recipient@gmail.com"


def test_send_digest_updates_db_on_success(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)
        digest = _make_digest()
        _insert_digest(db_conn, digest)

        mock_service = MagicMock()
        mock_service.users().messages().send().execute.return_value = {"id": "msg123"}

        with patch("app.send_email.build_gmail_service", return_value=mock_service):
            send_digest(digest, settings, db_conn)

        row = db_conn.execute(
            "SELECT status FROM digest_runs WHERE id = ?", (digest.id,)
        ).fetchone()
        assert row["status"] == "sent"


def test_send_digest_skips_if_already_sent_today(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)
        # Insert a digest that was already sent today
        sent_digest = _make_digest(status="sent")
        _insert_digest(db_conn, sent_digest)

        # Try to send a new digest for the same date
        new_digest = DigestRun(
            id="new-digest-id",
            run_date="2026-04-05",
            subject="New digest",
            html_content="<html/>",
            text_content="text",
            status="composed",
        )
        _insert_digest(db_conn, new_digest)

        with patch("app.send_email.build_gmail_service") as mock_build:
            result = send_digest(new_digest, settings, db_conn, force=False)

        # Gmail service should NOT have been called
        mock_build.assert_not_called()
        assert result.status == "skipped"


def test_send_digest_force_overrides_skip(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)
        sent_digest = _make_digest(status="sent")
        _insert_digest(db_conn, sent_digest)

        new_digest = DigestRun(
            id="forced-digest-id",
            run_date="2026-04-05",
            subject="Forced digest",
            html_content="<html/>",
            text_content="text",
            status="composed",
        )
        _insert_digest(db_conn, new_digest)

        mock_service = MagicMock()
        mock_service.users().messages().send().execute.return_value = {"id": "msg456"}

        with patch("app.send_email.build_gmail_service", return_value=mock_service):
            result = send_digest(new_digest, settings, db_conn, force=True)

        assert result.status == "sent"


def test_send_digest_handles_api_failure_gracefully(db_conn):
    with tempfile.TemporaryDirectory() as tmp:
        settings = _make_settings(tmp)
        digest = _make_digest()
        _insert_digest(db_conn, digest)

        mock_service = MagicMock()
        mock_service.users().messages().send().execute.side_effect = Exception("API error")

        with patch("app.send_email.build_gmail_service", return_value=mock_service):
            result = send_digest(digest, settings, db_conn)

        assert result.status == "failed"
        # DB should also reflect failure
        row = db_conn.execute(
            "SELECT status FROM digest_runs WHERE id = ?", (digest.id,)
        ).fetchone()
        assert row["status"] == "failed"
