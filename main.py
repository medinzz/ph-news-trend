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
from util.tools import setup_logger

logger = setup_logger()

# Global reference to storage for signal handler
storage_instance = None


def signal_handler(sig, frame):
    """
    Handle shutdown signals (Ctrl+C, SIGTERM) gracefully.
    
    This ensures that:
    1. The queue processor stops cleanly
    2. Remaining items in buffer are flushed to BigQuery
    3. No data is lost on unexpected shutdown
    """
    signal_name = 'SIGINT' if sig == signal.SIGINT else 'SIGTERM'
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Received {signal_name} - Shutting down gracefully...")
    logger.info(f"{'=' * 60}")
    
    # Close storage to flush remaining data
    if storage_instance:
        try:
            logger.info("Flushing remaining data to storage...")
            storage_instance.close()
            logger.info("Storage closed successfully")
        except Exception as e:
            logger.error(f"Error closing storage: {e}")
    
    logger.info("Shutdown complete")
    sys.exit(0)


def main():
    """
    Main function to run the news scraper.
    """
    global storage_instance
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Docker/systemd shutdown
    logger.info("Signal handlers registered (Ctrl+C to gracefully stop)")
    
    parser = argparse.ArgumentParser(
        description='Fetch news articles and store in SQLite or BigQuery'
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
        choices=['sqlite', 'bigquery'],
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
        help='Include crawler process'
    )
    
    
    args = parser.parse_args()
    
    if args.use_crawler:
        refresh_news_articles(
            start_date=datetime.now().strftime('%Y-%m-%d'),
            end_date=datetime.today().strftime('%Y-%m-%d')
        )
        return
    
    
    # Get storage configuration
    config = get_storage_config()
    
    # Override backend if specified
    if args.backend:
        config = get_storage_config(args.backend)
        print(f"Backend overridden to: {args.backend}")
    
    # Show config and exit if requested
    if args.show_config:
        print_config()
        return
    
    # Calculate start date
    if args.start_date:
        start_date = args.start_date
    elif args.days_back:
        start_date = (datetime.now() - timedelta(days=args.days_back)).strftime('%Y-%m-%d')
    else:
        start_date = (datetime.now() - timedelta(days=DEFAULT_DAYS_BACK)).strftime('%Y-%m-%d')
    
    # Print configuration
    print("=" * 60)
    print(f"Fetching articles from {start_date}")
    print(f"Using {config['backend_type'].upper()} storage")
    print("=" * 60)
    
    try:
        # Run the scraper
        # Note: get_all_articles will set the global storage variable
        get_all_articles(
            start_date=start_date,
            backend=config['backend_type'],
            **{k: v for k, v in config.items() if k != 'backend_type'}
        )
        
        print("\n" + "=" * 60)
        print("SCRAPING COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
    except KeyboardInterrupt:
        # This should be caught by signal handler, but just in case
        logger.info("\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"\nError during scraping: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()