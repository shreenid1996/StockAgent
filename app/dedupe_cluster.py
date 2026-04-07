"""
dedupe_cluster.py — Deduplicate and cluster related articles by headline similarity.

Uses Jaccard similarity on headline word tokens. Articles whose headlines are
similar above a configurable threshold are grouped into the same EventCluster.

Public API:
    cluster_articles(articles, threshold) -> list[EventCluster]
    cluster_and_store(articles, conn, threshold) -> list[EventCluster]
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone

from app.logger import get_logger
from app.models import Article, EventCluster
from app.utils import compute_text_similarity

log = get_logger(__name__)

# Default Jaccard similarity threshold for grouping articles into a cluster
DEFAULT_THRESHOLD = 0.4


# ---------------------------------------------------------------------------
# Clustering logic
# ---------------------------------------------------------------------------

def cluster_articles(
    articles: list[Article],
    threshold: float = DEFAULT_THRESHOLD,
) -> list[EventCluster]:
    """Group articles into EventClusters using greedy Jaccard similarity.

    Algorithm:
        - For each unassigned article, start a new cluster.
        - Assign subsequent unassigned articles to the first cluster whose
          representative headline has Jaccard similarity >= threshold.
        - Single-article clusters are valid.

    Args:
        articles:  List of Article objects to cluster.
        threshold: Minimum Jaccard similarity to merge into an existing cluster.

    Returns:
        List of EventCluster objects. Total article count across all clusters
        equals len(articles) — no article is dropped or duplicated.
    """
    if not articles:
        return []

    # Each cluster is represented as (representative_article, [member_articles])
    raw_clusters: list[tuple[Article, list[Article]]] = []

    for article in articles:
        placed = False
        for rep, members in raw_clusters:
            sim = compute_text_similarity(rep.headline, article.headline)
            if sim >= threshold:
                members.append(article)
                placed = True
                break
        if not placed:
            raw_clusters.append((article, [article]))

    now = datetime.now(timezone.utc)
    clusters: list[EventCluster] = []

    for rep, members in raw_clusters:
        cluster_id = _make_cluster_id(rep, members)
        cluster = EventCluster(
            id=cluster_id,
            ticker=rep.ticker,
            representative_headline=rep.headline,
            summary="",  # filled in by summarize.py
            article_ids=[a.id for a in members],
            created_at=now,
        )
        clusters.append(cluster)

    log.debug(
        "Clustered %d articles into %d clusters (threshold=%.2f)",
        len(articles), len(clusters), threshold,
    )
    return clusters


def _make_cluster_id(rep: Article, members: list[Article]) -> str:
    """Deterministic cluster ID based on representative article ID + member count."""
    raw = f"{rep.id}|{len(members)}|{rep.ticker}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Batch cluster + store
# ---------------------------------------------------------------------------

def cluster_and_store(
    articles: list[Article],
    conn: sqlite3.Connection,
    threshold: float = DEFAULT_THRESHOLD,
) -> list[EventCluster]:
    """Cluster articles and persist each EventCluster to the event_clusters table.

    Uses INSERT OR REPLACE so re-running is safe (clusters are regenerated
    fresh each pipeline run).

    Returns the list of EventCluster objects.
    """
    clusters = cluster_articles(articles, threshold=threshold)

    for cluster in clusters:
        _upsert_cluster(cluster, conn)

    log.info(
        "Stored %d clusters for %d articles",
        len(clusters), len(articles),
    )
    return clusters


def _upsert_cluster(cluster: EventCluster, conn: sqlite3.Connection) -> None:
    d = cluster.to_dict()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO event_clusters
                (id, ticker, representative_headline, summary, article_ids, created_at)
            VALUES
                (:id, :ticker, :representative_headline, :summary, :article_ids, :created_at)
            """,
            d,
        )
        conn.commit()
    except sqlite3.Error as exc:
        log.error("DB error upserting cluster %s: %s", cluster.id, exc)
        conn.rollback()
