"""
models.py — Pure Python dataclasses for all StockAgent domain objects.

Each model provides:
  - to_dict()        → dict suitable for JSON serialisation or DB insertion
  - from_row(row)    → construct from a sqlite3.Row or plain dict
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_dt(value: str | datetime | None) -> datetime | None:
    """Parse an ISO-8601 string to a UTC-aware datetime, or return None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _dt_str(dt: datetime | None) -> str | None:
    """Serialise a datetime to an ISO-8601 string, or return None."""
    if dt is None:
        return None
    return dt.isoformat()


def _row_to_dict(row: Any) -> dict:
    """Convert a sqlite3.Row or plain dict to a regular dict."""
    if hasattr(row, "keys"):
        return dict(row)
    return row


# ---------------------------------------------------------------------------
# Article
# ---------------------------------------------------------------------------

@dataclass
class Article:
    id: str
    ticker: str
    headline: str
    summary: str
    source: str
    url: str
    published_at: datetime
    fetched_at: datetime
    raw_json: str

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "ticker": self.ticker,
            "headline": self.headline,
            "summary": self.summary,
            "source": self.source,
            "url": self.url,
            "published_at": _dt_str(self.published_at),
            "fetched_at": _dt_str(self.fetched_at),
            "raw_json": self.raw_json,
        }

    @classmethod
    def from_row(cls, row: Any) -> "Article":
        d = _row_to_dict(row)
        return cls(
            id=d["id"],
            ticker=d["ticker"],
            headline=d["headline"],
            summary=d.get("summary") or "",
            source=d.get("source") or "",
            url=d.get("url") or "",
            published_at=_parse_dt(d["published_at"]),
            fetched_at=_parse_dt(d["fetched_at"]),
            raw_json=d["raw_json"],
        )


# ---------------------------------------------------------------------------
# ArticleScore
# ---------------------------------------------------------------------------

@dataclass
class ArticleScore:
    article_id: str
    is_relevant: bool
    relevance_score: float
    importance_score: int
    event_type: str
    confidence: str
    include_in_digest: bool
    reason: str
    scored_at: datetime

    def to_dict(self) -> dict:
        return {
            "article_id": self.article_id,
            "is_relevant": int(self.is_relevant),
            "relevance_score": self.relevance_score,
            "importance_score": self.importance_score,
            "event_type": self.event_type,
            "confidence": self.confidence,
            "include_in_digest": int(self.include_in_digest),
            "reason": self.reason,
            "scored_at": _dt_str(self.scored_at),
        }

    def to_json_dict(self) -> dict:
        """Return a JSON-friendly dict with bool values (not ints)."""
        d = self.to_dict()
        d["is_relevant"] = self.is_relevant
        d["include_in_digest"] = self.include_in_digest
        return d

    @classmethod
    def from_row(cls, row: Any) -> "ArticleScore":
        d = _row_to_dict(row)
        return cls(
            article_id=d["article_id"],
            is_relevant=bool(d["is_relevant"]),
            relevance_score=float(d["relevance_score"]),
            importance_score=int(d["importance_score"]),
            event_type=d["event_type"],
            confidence=d["confidence"],
            include_in_digest=bool(d["include_in_digest"]),
            reason=d["reason"],
            scored_at=_parse_dt(d["scored_at"]),
        )


# ---------------------------------------------------------------------------
# EventCluster
# ---------------------------------------------------------------------------

@dataclass
class EventCluster:
    id: str
    ticker: str
    representative_headline: str
    summary: str
    article_ids: list[str]
    created_at: datetime

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "ticker": self.ticker,
            "representative_headline": self.representative_headline,
            "summary": self.summary,
            "article_ids": json.dumps(self.article_ids),
            "created_at": _dt_str(self.created_at),
        }

    @classmethod
    def from_row(cls, row: Any) -> "EventCluster":
        d = _row_to_dict(row)
        raw_ids = d.get("article_ids", "[]")
        article_ids = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
        return cls(
            id=d["id"],
            ticker=d["ticker"],
            representative_headline=d["representative_headline"],
            summary=d.get("summary") or "",
            article_ids=article_ids,
            created_at=_parse_dt(d["created_at"]),
        )


# ---------------------------------------------------------------------------
# DigestRun
# ---------------------------------------------------------------------------

@dataclass
class DigestRun:
    id: str
    run_date: str
    subject: str
    html_content: str = ""
    text_content: str = ""
    sent_at: datetime | None = None
    recipient: str | None = None
    status: str = "composed"   # composed | sent | skipped | failed

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "run_date": self.run_date,
            "subject": self.subject,
            "html_content": self.html_content,
            "text_content": self.text_content,
            "sent_at": _dt_str(self.sent_at),
            "recipient": self.recipient,
            "status": self.status,
        }

    @classmethod
    def from_row(cls, row: Any) -> "DigestRun":
        d = _row_to_dict(row)
        return cls(
            id=d["id"],
            run_date=d["run_date"],
            subject=d["subject"],
            html_content=d.get("html_content") or "",
            text_content=d.get("text_content") or "",
            sent_at=_parse_dt(d.get("sent_at")),
            recipient=d.get("recipient"),
            status=d.get("status", "composed"),
        )


# ---------------------------------------------------------------------------
# RunLog
# ---------------------------------------------------------------------------

@dataclass
class RunLog:
    run_id: str
    step: str
    status: str
    duration_ms: int | None = None
    message: str | None = None
    logged_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    id: int | None = None   # set by DB after insert

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "step": self.step,
            "status": self.status,
            "duration_ms": self.duration_ms,
            "message": self.message,
            "logged_at": _dt_str(self.logged_at),
        }

    @classmethod
    def from_row(cls, row: Any) -> "RunLog":
        d = _row_to_dict(row)
        return cls(
            id=d.get("id"),
            run_id=d["run_id"],
            step=d["step"],
            status=d["status"],
            duration_ms=d.get("duration_ms"),
            message=d.get("message"),
            logged_at=_parse_dt(d["logged_at"]),
        )
