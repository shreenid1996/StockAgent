"""
summarize.py — Extractive summarization for EventClusters.

Generates a plain-language summary for each cluster by selecting the most
informative sentence from member article headlines and summaries.
No LLM or external API is used — all text comes directly from source articles.

Public API:
    summarize_cluster(cluster, articles_by_id) -> str
    summarize_and_store(clusters, articles_by_id, conn) -> list[EventCluster]
"""
from __future__ import annotations

import sqlite3

from app.logger import get_logger
from app.models import Article, EventCluster
from app.utils import truncate

log = get_logger(__name__)

_MAX_SUMMARY_LEN = 400   # characters
_MAX_HEADLINE_LEN = 150


def summarize_cluster(
    cluster: EventCluster,
    articles_by_id: dict[str, Article],
) -> str:
    """Generate an extractive summary for a single EventCluster.

    Strategy:
    1. Collect all headlines and non-empty summaries from member articles.
    2. Use the representative headline as the opening sentence.
    3. Append the most informative unique summary sentence (longest non-duplicate).
    4. Truncate the result to _MAX_SUMMARY_LEN characters.

    Returns a non-empty string. Falls back to the representative headline
    if no member articles are found.
    """
    members = [
        articles_by_id[aid]
        for aid in cluster.article_ids
        if aid in articles_by_id
    ]

    if not members:
        return truncate(cluster.representative_headline, _MAX_SUMMARY_LEN)

    # Start with the representative headline
    rep_headline = truncate(cluster.representative_headline, _MAX_HEADLINE_LEN)
    parts = [rep_headline]

    # Collect unique, non-empty summaries from member articles
    seen: set[str] = {rep_headline.lower()}
    candidate_summaries: list[str] = []

    for article in members:
        for text in (article.summary, article.headline):
            cleaned = text.strip()
            if cleaned and cleaned.lower() not in seen:
                seen.add(cleaned.lower())
                candidate_summaries.append(cleaned)

    # Pick the longest candidate as the most informative additional sentence
    if candidate_summaries:
        best = max(candidate_summaries, key=len)
        parts.append(truncate(best, _MAX_HEADLINE_LEN))

    # Add source count context if multiple articles
    if len(members) > 1:
        parts.append(f"({len(members)} sources)")

    summary = " ".join(parts)
    return truncate(summary, _MAX_SUMMARY_LEN)


def summarize_and_store(
    clusters: list[EventCluster],
    articles_by_id: dict[str, Article],
    conn: sqlite3.Connection,
) -> list[EventCluster]:
    """Generate summaries for all clusters and update their DB records.

    Mutates each cluster's `summary` field in-place and persists to DB.
    Returns the updated list of EventCluster objects.
    """
    updated: list[EventCluster] = []

    for cluster in clusters:
        summary = summarize_cluster(cluster, articles_by_id)
        cluster.summary = summary
        _update_cluster_summary(cluster, conn)
        updated.append(cluster)
        log.debug("Summarised cluster %s: %s", cluster.id, summary[:80])

    log.info("Summarised %d clusters", len(updated))
    return updated


def _update_cluster_summary(cluster: EventCluster, conn: sqlite3.Connection) -> None:
    """Persist the summary field back to the event_clusters table."""
    try:
        conn.execute(
            "UPDATE event_clusters SET summary = ? WHERE id = ?",
            (cluster.summary, cluster.id),
        )
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        log.error("DB error updating summary for cluster %s: %s", cluster.id, exc)
        conn.rollback()
