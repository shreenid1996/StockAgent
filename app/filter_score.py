"""
filter_score.py — Rule-based article relevance and importance scoring.

Public API:
    score_article(article) -> ArticleScore
    score_and_store(articles, conn) -> list[ArticleScore]

Design:
    - RulesScoringBackend implements the ScoringBackend ABC using keyword lists.
    - The interface is open for an optional AI backend (e.g. OpenAI) via subclassing.
    - Scores are stored in the article_scores SQLite table.
"""
from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from app.logger import get_logger
from app.models import Article, ArticleScore

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_EVENT_TYPES = frozenset({
    "earnings", "merger", "regulation", "product",
    "executive", "legal", "macro", "other",
})

VALID_CONFIDENCE_LEVELS = frozenset({"low", "medium", "high"})

# ---------------------------------------------------------------------------
# Keyword maps — each event_type has high-signal and medium-signal keywords
# ---------------------------------------------------------------------------

_KEYWORDS: dict[str, dict[str, list[str]]] = {
    "earnings": {
        "high": [
            "earnings", "revenue", "profit", "eps", "quarterly results",
            "beat estimates", "missed estimates", "guidance", "outlook",
            "fiscal year", "annual results", "net income", "operating income",
        ],
        "medium": [
            "financial results", "q1", "q2", "q3", "q4", "full year",
            "sales", "margin", "forecast", "analyst",
        ],
    },
    "merger": {
        "high": [
            "acquisition", "merger", "takeover", "buyout", "deal",
            "acquire", "acquired", "purchase agreement", "bid",
        ],
        "medium": [
            "partnership", "joint venture", "strategic alliance",
            "consolidation", "combine",
        ],
    },
    "regulation": {
        "high": [
            "sec", "ftc", "doj", "antitrust", "fine", "penalty",
            "regulatory", "compliance", "investigation", "subpoena",
            "lawsuit", "settlement", "consent decree",
        ],
        "medium": [
            "regulation", "regulator", "government", "congress",
            "legislation", "policy", "rule", "ban",
        ],
    },
    "product": {
        "high": [
            "launch", "release", "unveil", "announce", "new product",
            "iphone", "chip", "gpu", "ai model", "software update",
            "version", "upgrade",
        ],
        "medium": [
            "product", "feature", "update", "platform", "service",
            "technology", "innovation",
        ],
    },
    "executive": {
        "high": [
            "ceo", "cfo", "cto", "coo", "president", "resign",
            "appoint", "hire", "fired", "step down", "departure",
            "succession",
        ],
        "medium": [
            "executive", "leadership", "management", "board",
            "director", "officer",
        ],
    },
    "legal": {
        "high": [
            "lawsuit", "sued", "litigation", "court", "judge",
            "verdict", "damages", "patent", "infringement", "class action",
        ],
        "medium": [
            "legal", "attorney", "counsel", "dispute", "claim",
            "allegation", "complaint",
        ],
    },
    "macro": {
        "high": [
            "federal reserve", "fed", "interest rate", "inflation",
            "recession", "gdp", "unemployment", "tariff", "trade war",
            "geopolitical",
        ],
        "medium": [
            "economy", "economic", "market", "sector", "industry",
            "global", "macro",
        ],
    },
}

# Minimum relevance score to include in digest
_RELEVANCE_THRESHOLD = 0.4
_IMPORTANCE_THRESHOLD = 30


# ---------------------------------------------------------------------------
# Scoring backend interface
# ---------------------------------------------------------------------------

class ScoringBackend(ABC):
    """Abstract interface for article scoring backends.

    Subclass this to add an AI-powered backend (e.g. OpenAI).
    The rules-only backend is always available as a fallback.
    """

    @abstractmethod
    def score(self, article: Article) -> ArticleScore:
        """Score a single article and return an ArticleScore."""
        ...


# ---------------------------------------------------------------------------
# Rules-only backend
# ---------------------------------------------------------------------------

class RulesScoringBackend(ScoringBackend):
    """Pure rule-based scoring using keyword frequency matching."""

    def score(self, article: Article) -> ArticleScore:
        text = f"{article.headline} {article.summary}".lower()

        best_event_type = "other"
        best_high_hits = 0
        best_medium_hits = 0

        for event_type, kw_map in _KEYWORDS.items():
            high_hits = sum(1 for kw in kw_map["high"] if kw in text)
            medium_hits = sum(1 for kw in kw_map["medium"] if kw in text)
            # Prefer event type with most high-signal hits, break ties with medium
            if (high_hits, medium_hits) > (best_high_hits, best_medium_hits):
                best_high_hits = high_hits
                best_medium_hits = medium_hits
                best_event_type = event_type

        # Relevance score: weighted keyword density
        total_high = sum(
            sum(1 for kw in kw_map["high"] if kw in text)
            for kw_map in _KEYWORDS.values()
        )
        total_medium = sum(
            sum(1 for kw in kw_map["medium"] if kw in text)
            for kw_map in _KEYWORDS.values()
        )

        raw_score = (total_high * 2.0 + total_medium * 1.0)
        # Normalise to [0, 1] — cap at 10 weighted hits = 1.0
        relevance_score = min(raw_score / 10.0, 1.0)

        # Importance score: 0–100
        importance_score = min(int(best_high_hits * 20 + best_medium_hits * 5), 100)

        # Confidence
        if best_high_hits >= 2:
            confidence = "high"
        elif best_high_hits == 1 or best_medium_hits >= 2:
            confidence = "medium"
        else:
            confidence = "low"

        is_relevant = relevance_score >= _RELEVANCE_THRESHOLD
        include_in_digest = is_relevant and importance_score >= _IMPORTANCE_THRESHOLD

        reason = _build_reason(
            best_event_type, best_high_hits, best_medium_hits,
            is_relevant, include_in_digest,
        )

        return ArticleScore(
            article_id=article.id,
            is_relevant=is_relevant,
            relevance_score=round(relevance_score, 4),
            importance_score=importance_score,
            event_type=best_event_type,
            confidence=confidence,
            include_in_digest=include_in_digest,
            reason=reason,
            scored_at=datetime.now(timezone.utc),
        )


def _build_reason(
    event_type: str,
    high_hits: int,
    medium_hits: int,
    is_relevant: bool,
    include_in_digest: bool,
) -> str:
    if not is_relevant:
        return "No significant stock-relevant keywords detected."
    if include_in_digest:
        return (
            f"Material company-specific update. "
            f"Event type: {event_type}. "
            f"High-signal keywords: {high_hits}, medium: {medium_hits}."
        )
    return (
        f"Relevant but below importance threshold. "
        f"Event type: {event_type}. "
        f"High-signal keywords: {high_hits}, medium: {medium_hits}."
    )


# ---------------------------------------------------------------------------
# Module-level default backend (rules only)
# ---------------------------------------------------------------------------

_default_backend = RulesScoringBackend()


def score_article(article: Article, backend: ScoringBackend | None = None) -> ArticleScore:
    """Score a single article using the given backend (defaults to rules)."""
    b = backend or _default_backend
    return b.score(article)


# ---------------------------------------------------------------------------
# Batch score + store
# ---------------------------------------------------------------------------

def score_and_store(
    articles: list[Article],
    conn: sqlite3.Connection,
    backend: ScoringBackend | None = None,
) -> list[ArticleScore]:
    """Score all articles and persist results to article_scores table.

    Skips articles that already have a score (idempotent).
    Returns list of ArticleScore objects.
    """
    scores: list[ArticleScore] = []
    b = backend or _default_backend

    for article in articles:
        try:
            score = b.score(article)
            _insert_score(score, conn)
            scores.append(score)
        except Exception as exc:  # noqa: BLE001
            log.error("Failed to score article %s: %s", article.id, exc)

    relevant = sum(1 for s in scores if s.include_in_digest)
    log.info(
        "Scored %d articles — %d flagged for digest",
        len(scores), relevant,
    )
    return scores


def _insert_score(score: ArticleScore, conn: sqlite3.Connection) -> None:
    """Insert score into DB, ignoring if already exists."""
    d = score.to_dict()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO article_scores
                (article_id, is_relevant, relevance_score, importance_score,
                 event_type, confidence, include_in_digest, reason, scored_at)
            VALUES
                (:article_id, :is_relevant, :relevance_score, :importance_score,
                 :event_type, :confidence, :include_in_digest, :reason, :scored_at)
            """,
            d,
        )
        conn.commit()
    except sqlite3.Error as exc:
        log.error("DB error inserting score for %s: %s", score.article_id, exc)
        conn.rollback()
