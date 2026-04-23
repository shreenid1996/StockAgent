"""
scheduler.py — Run the StockAgent pipeline on a daily schedule.

Uses the `schedule` library to trigger the full pipeline at a configured
time each day (set via config.yaml → scheduler.run_time).

Usage (run manually in a terminal or as a background service):
    python -m app.scheduler

The process runs indefinitely, checking every 60 seconds whether a
scheduled job is due.

To run as a Windows Task Scheduler or cron job instead, use:
    python main.py run
"""
from __future__ import annotations

import time

import schedule

from app.db import get_connection, init_db
from app.logger import get_logger
from app.pipeline import run_full
from app.settings import load_settings

log = get_logger(__name__)


def _run_pipeline() -> None:
    """Execute the full pipeline. Called by the scheduler."""
    log.info("Scheduler triggered pipeline run.")
    try:
        settings = load_settings()
        conn = get_connection(settings.db_path)
        init_db(conn)
        result = run_full(settings, conn)
        conn.close()
        log.info(
            "Scheduled run complete: status=%s articles=%d clusters=%d",
            result.status,
            result.articles_stored,
            result.clusters_formed,
        )
    except Exception as exc:  # noqa: BLE001
        log.error("Scheduled pipeline run failed: %s", exc)


def start(run_time: str | None = None) -> None:
    """Start the daily scheduler loop.

    Args:
        run_time: Time string in HH:MM format (24h). If None, loads from
                  config.yaml (scheduler.run_time).
    """
    if run_time is None:
        settings = load_settings()
        run_time = settings.run_time

    log.info("Scheduler starting — pipeline will run daily at %s", run_time)
    schedule.every().day.at(run_time).do(_run_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    start()
