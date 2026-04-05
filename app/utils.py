"""
utils.py — Pure utility functions with no external I/O or side effects.
All functions are deterministic and safe to use in property-based tests.
"""
from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone
from html.parser import HTMLParser


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    """Convert text to a lowercase URL-safe slug.

    Example: "Hello World!" -> "hello-world"
    """
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = text.strip("-")
    return text


def truncate(text: str, max_len: int) -> str:
    """Return text truncated to at most max_len characters.

    If truncation occurs, the last 3 characters are replaced with '...'.
    Guarantees len(result) <= max_len for any max_len >= 0.
    """
    if max_len < 0:
        raise ValueError("max_len must be >= 0")
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3] + "..."


class _HTMLStripper(HTMLParser):
    """Minimal HTML parser that collects visible text."""

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return " ".join(self._parts)


def clean_html(text: str) -> str:
    """Strip HTML tags and collapse whitespace from a string."""
    stripper = _HTMLStripper()
    stripper.feed(text)
    cleaned = stripper.get_text()
    return re.sub(r"\s+", " ", cleaned).strip()


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def unix_to_datetime(ts: int) -> datetime:
    """Convert a Unix timestamp (seconds) to a UTC-aware datetime."""
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def datetime_to_unix(dt: datetime) -> int:
    """Convert a datetime to a Unix timestamp (seconds, integer).

    If dt is naive, it is assumed to be UTC.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


# ---------------------------------------------------------------------------
# Similarity
# ---------------------------------------------------------------------------

def compute_text_similarity(a: str, b: str) -> float:
    """Compute Jaccard similarity between the word sets of two strings.

    Returns a float in [0.0, 1.0].
    Returns 0.0 if both strings are empty.
    """
    tokens_a = set(re.findall(r"\w+", a.lower()))
    tokens_b = set(re.findall(r"\w+", b.lower()))

    if not tokens_a and not tokens_b:
        return 0.0

    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)
