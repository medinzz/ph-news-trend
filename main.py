"""
Main script to fetch news articles and store in SQLite or BigQuery.
Edit config.py to change storage backend and other settings.
"""

import argparse
import signal
import sys
from datetime import datetime, timedelta

from news.apis import get_all_articles
from news.crawler import refresh_news_articles
from config import get_storage_config, print_config, DEFAULT_DAYS_BACK
from util.storage_backend import get_storage_backend
from util.tools import setup_logger

logger = setup_logger()

# Global reference to storage for signal handler
storage_instance = None


def signal_handler(sig, frame):
    """Handle shutdown signals (Ctrl+C, SIGTERM) gracefully."""
    signal_name = 'SIGINT' if sig == signal.SIGINT else 'SIGTERM'
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Received {signal_name} - Shutting down gracefully...")
    logger.info(f"{'=' * 60}")

    if storage_instance:
        try:
            logger.info("Flushing remaining data to storage...")
            storage_instance.close()
            logger.info("Storage closed successfully")
        except Exception as e:
            logger.error(f"Error closing storage: {e}")

    logger.info("Shutdown complete")
    sys.exit(0)


def run_query(query: str, config: dict) -> None:
    """
    Open a storage connection, run a SQL query, print results, and close.

    Works with SQLite and DuckDB backends. For BigQuery, results are printed
    as a DataFrame (same as DuckDB). Meant for quick ad-hoc data inspection
    from the CLI without needing a separate DB client.
    """
    backend_type = config.get('backend_type', 'sqlite')
    kwargs = {k: v for k, v in config.items() if k != 'backend_type'}

    logger.info(f"Running query on {backend_type} backend...")
    storage = get_storage_backend(backend_type, **kwargs)

    try:
        results = storage.run_query(query)

        if results is None:
            print("Query returned no results.")
            return

        # DuckDB and BigQuery return a DataFrame; SQLite returns a list of tuples
        import pandas as pd
        if isinstance(results, pd.DataFrame):
            if results.empty:
                print("Query returned 0 rows.")
            else:
                print(f"\n{results.to_string(index=False)}")
                print(f"\n{len(results)} row(s) returned.")
        else:
            if not results:
                print("Query returned 0 rows.")
            else:
                for row in results:
                    print(row)
                print(f"\n{len(results)} row(s) returned.")
    finally:
        storage.close()


def main():
    global storage_instance

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    logger.info("Signal handlers registered (Ctrl+C to gracefully stop)")

    parser = argparse.ArgumentParser(
        description='Fetch news articles and store in SQLite, DuckDB, or BigQuery'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date in YYYY-MM-DD format (default: 7 days ago)'
    )
    parser.add_argument(
        '--days-back',
        type=int,
        help=f'Number of days to look back (default: {DEFAULT_DAYS_BACK})'
    )
    parser.add_argument(
        '--backend',
        type=str,
        choices=['sqlite', 'duckdb', 'bigquery'],
        help='Override storage backend from config.py'
    )
    parser.add_argument(
        '--show-config',
        action='store_true',
        help='Show current configuration and exit'
    )
    parser.add_argument(
        '--use-crawler',
        action='store_true',
        help='Run the Inquirer Scrapy crawler instead of the API scrapers'
    )
    parser.add_argument(
        '--query',
        type=str,
        metavar='SQL',
        help=(
            'Run a SQL query against the storage backend and print results. '
            'Example: --query "SELECT source, COUNT(*) FROM articles_raw GROUP BY source"'
        )
    )

    args = parser.parse_args()

    # ── Get storage config ──────────────────────────────────────────────────
    config = get_storage_config(args.backend) if args.backend else get_storage_config()

    if args.backend:
        print(f"Backend overridden to: {args.backend}")

    # ── --show-config ───────────────────────────────────────────────────────
    if args.show_config:
        print_config()
        return

    # ── --query ─────────────────────────────────────────────────────────────
    if args.query:
        run_query(args.query, config)
        return

    # ── --use-crawler ───────────────────────────────────────────────────────
    if args.use_crawler:
        refresh_news_articles(
            start_date=datetime.now().strftime('%Y-%m-%d'),
            end_date=datetime.today().strftime('%Y-%m-%d'),
            categories=[
                'NEWS',
                'WORLD',
                'ENTERTAINMENT',
                'BUSINESS',
                'OPINION',
                'GLOBALNATION',
                'POP',
                'TECHNOLOGY',
                'SPORTS',
            ]
        )
        return

    # ── Default: run API scrapers ───────────────────────────────────────────
    if args.start_date:
        start_date = args.start_date
    elif args.days_back:
        start_date = (datetime.now() - timedelta(days=args.days_back)).strftime('%Y-%m-%d')
    else:
        start_date = (datetime.now() - timedelta(days=DEFAULT_DAYS_BACK)).strftime('%Y-%m-%d')

    print("=" * 60)
    print(f"Fetching articles from {start_date}")
    print(f"Using {config['backend_type'].upper()} storage")
    print("=" * 60)

    try:
        get_all_articles(
            start_date=start_date,
            backend=config['backend_type'],
            **{k: v for k, v in config.items() if k != 'backend_type'}
        )

        print("\n" + "=" * 60)
        print("SCRAPING COMPLETED SUCCESSFULLY")
        print("=" * 60)

    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"\nError during scraping: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()