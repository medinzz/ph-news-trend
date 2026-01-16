"""
Configuration file for the news scraper.
Edit these settings to control storage backend and other options.
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ============================================================================
# STORAGE CONFIGURATION
# ============================================================================

# Choose storage backend: 'sqlite' or 'bigquery'
STORAGE_BACKEND = os.getenv('STORAGE_BACKEND', 'sqlite')

# SQLite Configuration
SQLITE_CONFIG = {
    'db_path': os.getenv('SQLITE_DB_PATH', 'articles.db'),
    'table_name': os.getenv('SQLITE_TABLE_NAME', 'articles')
}

# BigQuery Configuration
BIGQUERY_CONFIG = {
    'dataset_id': os.getenv('BQ_DATASET_ID', 'news_data'),
    'table_name': os.getenv('BQ_TABLE_NAME', 'articles'),
    'buffer_size': int(os.getenv('BQ_BUFFER_SIZE', 100))
}

# ============================================================================
# SCRAPING CONFIGURATION
# ============================================================================

# Default number of days to look back
DEFAULT_DAYS_BACK = int(os.getenv('DAYS_BACK', 7))

# Manila Bulletin section IDs to scrape
# 25=Philippines, 26=Business, 27=World, 28=Lifestyle, 
# 29=Entertainment, 30=Sports, 31=Opinion
MANILA_BULLETIN_SECTIONS = [25, 26, 27, 28, 29, 30, 31]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_storage_config() -> Dict[str, Any]:
    """
    Get the storage configuration based on the selected backend.
    
    Returns:
        Dict containing the backend type and its configuration
    """
    if STORAGE_BACKEND.lower() == 'sqlite':
        return {
            'backend': 'sqlite',
            **SQLITE_CONFIG
        }
    elif STORAGE_BACKEND.lower() == 'bigquery':
        return {
            'backend': 'bigquery',
            **BIGQUERY_CONFIG
        }
    else:
        raise ValueError(f"Unknown storage backend: {STORAGE_BACKEND}")


def print_config():
    """Print current configuration."""
    config = get_storage_config()
    print("=" * 60)
    print("CURRENT CONFIGURATION")
    print("=" * 60)
    print(f"Storage Backend: {config['backend'].upper()}")
    
    if config['backend'] == 'sqlite':
        print(f"Database Path: {config['db_path']}")
        print(f"Table Name: {config['table_name']}")
    else:
        print(f"Dataset ID: {config['dataset_id']}")
        print(f"Table Name: {config['table_name']}")
    
    print(f"Days to look back: {DEFAULT_DAYS_BACK}")
    print(f"Manila Bulletin sections: {MANILA_BULLETIN_SECTIONS}")
    print("=" * 60)