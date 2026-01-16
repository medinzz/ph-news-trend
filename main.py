"""
Main script to fetch news articles and store them in SQLite or BigQuery.
Edit config.py to change storage backend and other settings.
"""

import argparse
from datetime import datetime, timedelta

from news.apis import get_all_articles
from config import get_storage_config, print_config, DEFAULT_DAYS_BACK


def main():
    """
    Main function to run the news scraper.
    """
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
    
    args = parser.parse_args()
    
    # Get storage configuration
    config = get_storage_config()
    
    # Override backend if specified
    if args.backend:
        config['backend'] = args.backend
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
    print(f"Using {config['backend'].upper()} storage")
    print("=" * 60)
    
    # Run the scraper
    get_all_articles(
        start_date=start_date,
        backend=config['backend'],
        **{k: v for k, v in config.items() if k != 'backend'}
    )
    
    print("\n" + "=" * 60)
    print("SCRAPING COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    main()