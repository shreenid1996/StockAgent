"""
send_email.py — Send the daily digest via Gmail API using OAuth2.

Public API:
    send_digest(digest, settings, conn, force=False) -> DigestRun
    build_gmail_service(credentials_path, token_path) -> Resource

Flow:
    1. Check if a digest was already sent today (unless force=True).
    2. Build a multipart MIME message (HTML + plain text).
    3. Send via Gmail API.
    4. Update digest_runs record with sent_at, recipient, status.
"""
from __future__ import annotations

import base64
import sqlite3
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

from app.logger import get_logger
from app.models import DigestRun
from app.settings import Settings

log = get_logger(__name__)

# Gmail API scopes required
_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


# ---------------------------------------------------------------------------
# Gmail service builder
# ---------------------------------------------------------------------------

def build_gmail_service(credentials_path: str, token_path: str) -> Any:
    """Build and return an authenticated Gmail API service resource.

    Uses OAuth2 with a local credentials.json (downloaded from Google Cloud
    Console) and persists the token to token_path for subsequent runs.

    Raises:
        FileNotFoundError: if credentials_path does not exist.
        google.auth.exceptions.TransportError: on network issues during auth.
    """
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    creds_file = Path(credentials_path)
    if not creds_file.exists():
        raise FileNotFoundError(
            f"Gmail credentials file not found: {credentials_path}\n"
            "Download it from Google Cloud Console → APIs & Services → Credentials."
        )

    token_file = Path(token_path)
    creds = None

    if token_file.exists():
        creds = Credentials.from_authorized_user_file(str(token_file), _SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_file), _SCOPES)
            creds = flow.run_local_server(port=0)
        token_file.write_text(creds.to_json())
        log.info("Gmail token saved to %s", token_path)

    service = build("gmail", "v1", credentials=creds)
    log.debug("Gmail API service built successfully.")
    return service


# ---------------------------------------------------------------------------
# MIME message builder
# ---------------------------------------------------------------------------

def build_mime_message(
    sender: str,
    recipient: str,
    subject: str,
    html_content: str,
    text_content: str,
) -> MIMEMultipart:
    """Build a multipart/alternative MIME message (plain text + HTML)."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    # Plain text first (fallback), HTML second (preferred)
    msg.attach(MIMEText(text_content, "plain", "utf-8"))
    msg.attach(MIMEText(html_content, "html", "utf-8"))
    return msg


def _encode_message(msg: MIMEMultipart) -> dict:
    """Encode a MIME message to the Gmail API raw format."""
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    return {"raw": raw}


# ---------------------------------------------------------------------------
# Duplicate-send guard
# ---------------------------------------------------------------------------

def _already_sent_today(conn: sqlite3.Connection, run_date: str) -> bool:
    """Return True if a digest with status='sent' exists for run_date."""
    row = conn.execute(
        "SELECT id FROM digest_runs WHERE run_date = ? AND status = 'sent' LIMIT 1",
        (run_date,),
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# DB update helpers
# ---------------------------------------------------------------------------

def _update_digest_status(
    conn: sqlite3.Connection,
    digest_id: str,
    status: str,
    sent_at: datetime | None,
    recipient: str | None,
) -> None:
    conn.execute(
        """
        UPDATE digest_runs
        SET status = ?, sent_at = ?, recipient = ?
        WHERE id = ?
        """,
        (
            status,
            sent_at.isoformat() if sent_at else None,
            recipient,
            digest_id,
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def send_digest(
    digest: DigestRun,
    settings: Settings,
    conn: sqlite3.Connection,
    force: bool = False,
) -> DigestRun:
    """Send the digest email via Gmail API.

    Args:
        digest:   The DigestRun to send (must have html_content and text_content).
        settings: Application settings (sender, recipient, credentials paths).
        conn:     SQLite connection for recording send status.
        force:    If True, send even if a digest was already sent today.

    Returns:
        Updated DigestRun with status='sent', 'skipped', or 'failed'.

    Raises:
        Does NOT raise on send failure — logs the error and returns status='failed'.
    """
    run_date = digest.run_date

    # --- Duplicate-send guard ---
    if not force and _already_sent_today(conn, run_date):
        log.info(
            "Digest for %s already sent today. Skipping. Use --force to override.",
            run_date,
        )
        digest.status = "skipped"
        _update_digest_status(conn, digest.id, "skipped", None, None)
        return digest

    # --- Build and send ---
    try:
        service = build_gmail_service(
            settings.gmail_credentials_path,
            settings.gmail_token_path,
        )
        mime_msg = build_mime_message(
            sender=settings.gmail_sender,
            recipient=settings.gmail_recipient,
            subject=digest.subject,
            html_content=digest.html_content,
            text_content=digest.text_content,
        )
        encoded = _encode_message(mime_msg)
        service.users().messages().send(userId="me", body=encoded).execute()

        sent_at = datetime.now(timezone.utc)
        digest.status = "sent"
        digest.sent_at = sent_at
        digest.recipient = settings.gmail_recipient
        _update_digest_status(conn, digest.id, "sent", sent_at, settings.gmail_recipient)
        log.info(
            "Digest sent to %s at %s",
            settings.gmail_recipient,
            sent_at.isoformat(),
        )

    except Exception as exc:  # noqa: BLE001
        log.error("Failed to send digest: %s", exc)
        digest.status = "failed"
        _update_digest_status(conn, digest.id, "failed", None, settings.gmail_recipient)

    return digest
