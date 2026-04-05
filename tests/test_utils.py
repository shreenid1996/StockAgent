"""
Property-based tests for app/utils.py.

Feature: stock-news-agent
Properties covered:
  Property 5: Text similarity is symmetric
  Property 6: Truncation never exceeds max length
  Property 7: Unix timestamp round-trip
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from app.utils import (
    clean_html,
    compute_text_similarity,
    datetime_to_unix,
    slugify,
    truncate,
    unix_to_datetime,
)

# ---------------------------------------------------------------------------
# Property 5: Text similarity is symmetric
# Validates: Requirements 6.1
# ---------------------------------------------------------------------------

# Feature: stock-news-agent, Property 5: Text similarity is symmetric
@given(a=st.text(), b=st.text())
@settings(max_examples=100)
def test_similarity_is_symmetric(a: str, b: str) -> None:
    """compute_text_similarity(a, b) == compute_text_similarity(b, a) for any strings."""
    assert compute_text_similarity(a, b) == compute_text_similarity(b, a)


# ---------------------------------------------------------------------------
# Property 6: Truncation never exceeds max length
# Validates: Requirements 7.1
# ---------------------------------------------------------------------------

# Feature: stock-news-agent, Property 6: Truncation never exceeds max length
@given(text=st.text(), max_len=st.integers(min_value=0, max_value=500))
@settings(max_examples=100)
def test_truncate_never_exceeds_max_len(text: str, max_len: int) -> None:
    """len(truncate(text, n)) <= n for any text and non-negative n."""
    result = truncate(text, max_len)
    assert len(result) <= max_len


# ---------------------------------------------------------------------------
# Property 7: Unix timestamp round-trip
# Validates: Requirements 3.1
# ---------------------------------------------------------------------------

# Feature: stock-news-agent, Property 7: Unix timestamp round-trip
@given(
    ts=st.integers(
        min_value=0,           # 1970-01-01
        max_value=4_102_444_800,  # 2100-01-01
    )
)
@settings(max_examples=100)
def test_unix_timestamp_round_trip(ts: int) -> None:
    """unix_to_datetime(datetime_to_unix(dt)) == dt within 1-second tolerance."""
    original_dt = unix_to_datetime(ts)
    round_tripped_ts = datetime_to_unix(original_dt)
    round_tripped_dt = unix_to_datetime(round_tripped_ts)
    delta = abs((round_tripped_dt - original_dt).total_seconds())
    assert delta < 1.0


# ---------------------------------------------------------------------------
# Unit tests for edge cases
# ---------------------------------------------------------------------------

def test_truncate_short_string_unchanged() -> None:
    assert truncate("hello", 10) == "hello"


def test_truncate_exact_length_unchanged() -> None:
    assert truncate("hello", 5) == "hello"


def test_truncate_adds_ellipsis() -> None:
    result = truncate("hello world", 8)
    assert result == "hello..."
    assert len(result) == 8


def test_truncate_max_len_zero() -> None:
    assert truncate("hello", 0) == ""


def test_truncate_max_len_three() -> None:
    result = truncate("hello", 3)
    assert len(result) <= 3


def test_similarity_identical_strings() -> None:
    assert compute_text_similarity("apple", "apple") == 1.0


def test_similarity_disjoint_strings() -> None:
    assert compute_text_similarity("apple", "orange") == 0.0


def test_similarity_both_empty() -> None:
    assert compute_text_similarity("", "") == 0.0


def test_similarity_range() -> None:
    score = compute_text_similarity("apple pie", "apple tart")
    assert 0.0 <= score <= 1.0


def test_clean_html_strips_tags() -> None:
    assert clean_html("<b>Hello</b> <i>World</i>") == "Hello World"


def test_clean_html_plain_text_unchanged() -> None:
    assert clean_html("plain text") == "plain text"


def test_slugify_basic() -> None:
    assert slugify("Hello World!") == "hello-world"


def test_slugify_special_chars() -> None:
    result = slugify("AAPL Q3 Earnings: +12%")
    assert " " not in result
    assert result == result.lower()


def test_unix_to_datetime_returns_utc() -> None:
    dt = unix_to_datetime(0)
    assert dt == datetime(1970, 1, 1, tzinfo=timezone.utc)


def test_datetime_to_unix_epoch() -> None:
    dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert datetime_to_unix(dt) == 0


def test_datetime_to_unix_naive_assumed_utc() -> None:
    naive = datetime(1970, 1, 1)
    assert datetime_to_unix(naive) == 0
