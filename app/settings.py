"""
settings.py — Load configuration from config.yaml and secrets from .env.
Raises ValueError at startup if any required secret is missing.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Resolve project root (two levels up from this file: app/ -> stock_news_agent/)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise ValueError(
            f"Missing required environment variable: {key}. "
            f"Copy .env.example to .env and fill in the value."
        )
    return value


@dataclass
class Settings:
    # Secrets (from .env)
    finnhub_api_key: str
    gmail_sender: str
    gmail_recipient: str
    gmail_credentials_path: str
    gmail_token_path: str

    # Watchlist
    watchlist: list[str]

    # Paths
    db_path: str
    data_dir: str
    log_dir: str

    # Logging
    log_retention_days: int

    # Finnhub
    request_delay_seconds: float

    # Scoring
    relevance_threshold: float
    importance_threshold: int
    scoring_backend: str

    # Digest
    max_top_stories: int
    max_articles_per_ticker: int

    # Scheduler
    run_time: str


def load_settings(
    env_path: str | Path | None = None,
    config_path: str | Path | None = None,
) -> Settings:
    """Load and validate settings. Raises ValueError for missing secrets."""
    env_file = Path(env_path) if env_path else _PROJECT_ROOT / ".env"
    cfg_file = Path(config_path) if config_path else _PROJECT_ROOT / "config.yaml"

    load_dotenv(dotenv_path=env_file, override=False)

    with open(cfg_file, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    return Settings(
        # Secrets
        finnhub_api_key=_require_env("FINNHUB_API_KEY"),
        gmail_sender=_require_env("GMAIL_SENDER"),
        gmail_recipient=_require_env("GMAIL_RECIPIENT"),
        gmail_credentials_path=_require_env("GMAIL_CREDENTIALS_PATH"),
        gmail_token_path=_require_env("GMAIL_TOKEN_PATH"),
        # Watchlist
        watchlist=cfg["watchlist"],
        # Paths
        db_path=str(_PROJECT_ROOT / cfg["paths"]["db"]),
        data_dir=str(_PROJECT_ROOT / cfg["paths"]["data_dir"]),
        log_dir=str(_PROJECT_ROOT / cfg["paths"]["log_dir"]),
        # Logging
        log_retention_days=int(cfg["logging"]["retention_days"]),
        # Finnhub
        request_delay_seconds=float(cfg["finnhub"]["request_delay_seconds"]),
        # Scoring
        relevance_threshold=float(cfg["scoring"]["relevance_threshold"]),
        importance_threshold=int(cfg["scoring"]["importance_threshold"]),
        scoring_backend=cfg["scoring"]["backend"],
        # Digest
        max_top_stories=int(cfg["digest"]["max_top_stories"]),
        max_articles_per_ticker=int(cfg["digest"]["max_articles_per_ticker"]),
        # Scheduler
        run_time=cfg["scheduler"]["run_time"],
    )
