"""
main.py — Entry point for StockAgent CLI.

Usage:
    python main.py <command> [options]

Commands:
    run              Run the full pipeline
    run --dry-run    Full pipeline, skip email send
    run --force      Full pipeline, force send even if sent today
    fetch-only       Fetch and store articles only
    compose-only     Compose digest from existing DB data
    send-last        Send the most recently composed digest
    send-last --force
    init-db          Initialise the SQLite database schema
"""
import sys
from cli import main

if __name__ == "__main__":
    sys.exit(main())
