"""
cli.py — Command-line interface for StockAgent.

Commands:
    run              Run the full pipeline (fetch → score → compose → send)
    run --dry-run    Full pipeline but skip email send
    run --force      Full pipeline, send even if already sent today
    fetch-only       Fetch and store articles only
    compose-only     Compose digest from existing DB data (no send)
    send-last        Send the most recently composed digest
    init-db          Initialise the SQLite database schema

Usage:
    python main.py run
    python main.py run --dry-run
    python main.py run --force
    python main.py fetch-only
    python main.py compose-only
    python main.py send-last
    python main.py send-last --force
    python main.py init-db
"""
from __future__ import annotations

import argparse
import sys

from app.db import get_connection, init_db
from app.logger import get_logger
from app.pipeline import run_compose_only, run_fetch_only, run_full, run_send_last
from app.settings import load_settings

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def cmd_run(args: argparse.Namespace) -> int:
    """Run the full pipeline."""
    try:
        settings = load_settings()
        conn = get_connection(settings.db_path)
        init_db(conn)
        result = run_full(
            settings,
            conn,
            dry_run=args.dry_run,
            force=args.force,
        )
        conn.close()
        if result.status == "failed":
            log.error("Pipeline failed. Errors: %s", result.errors)
            return 1
        if result.errors:
            log.warning("Pipeline completed with %d error(s).", len(result.errors))
        print(f"Pipeline complete: status={result.status}, "
              f"articles={result.articles_stored}, "
              f"clusters={result.clusters_formed}, "
              f"digest={'sent' if result.digest and result.digest.status == 'sent' else result.digest.status if result.digest else 'none'}")
        return 0
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        log.error("Unexpected error: %s", exc)
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def cmd_fetch_only(args: argparse.Namespace) -> int:
    """Fetch and store articles only."""
    try:
        settings = load_settings()
        conn = get_connection(settings.db_path)
        init_db(conn)
        result = run_full(settings, conn, fetch_only=True)
        conn.close()
        print(f"Fetch complete: {result.articles_stored} articles stored.")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def cmd_compose_only(args: argparse.Namespace) -> int:
    """Compose digest from existing DB data."""
    try:
        settings = load_settings()
        conn = get_connection(settings.db_path)
        init_db(conn)
        digest = run_compose_only(settings, conn)
        conn.close()
        if digest:
            print(f"Digest composed: {digest.subject}")
        else:
            print("No digest composed.")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def cmd_send_last(args: argparse.Namespace) -> int:
    """Send the most recently composed digest."""
    try:
        settings = load_settings()
        conn = get_connection(settings.db_path)
        init_db(conn)
        digest = run_send_last(settings, conn, force=args.force)
        conn.close()
        if digest is None:
            print("No digest found to send. Run 'compose-only' first.")
            return 1
        print(f"Send status: {digest.status}")
        return 0 if digest.status == "sent" else 1
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def cmd_init_db(args: argparse.Namespace) -> int:
    """Initialise the SQLite database schema."""
    try:
        settings = load_settings()
        conn = get_connection(settings.db_path)
        init_db(conn)
        conn.close()
        print(f"Database initialised at: {settings.db_path}")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="stockagent",
        description="StockAgent — Daily tech stock news digest",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers.required = True

    # run
    run_parser = subparsers.add_parser("run", help="Run the full pipeline")
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Complete all steps but skip email send",
    )
    run_parser.add_argument(
        "--force",
        action="store_true",
        help="Send even if a digest was already sent today",
    )
    run_parser.set_defaults(func=cmd_run)

    # fetch-only
    fetch_parser = subparsers.add_parser(
        "fetch-only", help="Fetch and store articles only"
    )
    fetch_parser.set_defaults(func=cmd_fetch_only)

    # compose-only
    compose_parser = subparsers.add_parser(
        "compose-only", help="Compose digest from existing DB data"
    )
    compose_parser.set_defaults(func=cmd_compose_only)

    # send-last
    send_parser = subparsers.add_parser(
        "send-last", help="Send the most recently composed digest"
    )
    send_parser.add_argument(
        "--force",
        action="store_true",
        help="Send even if already sent today",
    )
    send_parser.set_defaults(func=cmd_send_last)

    # init-db
    init_parser = subparsers.add_parser(
        "init-db", help="Initialise the SQLite database schema"
    )
    init_parser.set_defaults(func=cmd_init_db)

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)
