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

# Choose storage backend: 'sqlite', 'duckdb', or 'bigquery'
STORAGE_BACKEND = os.getenv('STORAGE_BACKEND', 'duckdb')

# SQLite Configuration
SQLITE_CONFIG = {
    'db_path': os.getenv('SQLITE_DB_PATH', 'articles_raw.db'),
    'table_name': os.getenv('TABLE_NAME', 'articles_raw')
}

# DuckDB Configuration (NEW!)
DUCKDB_CONFIG = {
    'db_path': os.getenv('DUCKDB_DB_PATH', 'articles_raw.duckdb'),
    'table_name': os.getenv('TABLE_NAME', 'articles_raw')
}

# BigQuery Configuration
BIGQUERY_CONFIG = {
    'dataset_id': os.getenv('BQ_DATASET_ID', 'ph_news_raw'),
    'table_name': os.getenv('BQ_TABLE_NAME', 'articles_raw'),
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
    backend = STORAGE_BACKEND.lower()
    
    if backend == 'sqlite':
        return {
            'backend_type': 'sqlite',
            **SQLITE_CONFIG
        }
    elif backend == 'duckdb':
        return {
            'backend_type': 'duckdb',
            **DUCKDB_CONFIG
        }
    elif backend == 'bigquery':
        return {
            'backend_type': 'bigquery',
            **BIGQUERY_CONFIG
        }
    else:
        raise ValueError(f"Unknown storage backend: {STORAGE_BACKEND}")


def get_storage_backend_instance():
    """
    Get a configured storage backend instance.
    
    Returns:
        StorageBackend: Configured storage backend instance
    """
    from util.storage_backend import get_storage_backend
    
    backend = STORAGE_BACKEND.lower()
    
    if backend == 'sqlite':
        return get_storage_backend(
            backend_type='sqlite',
            **SQLITE_CONFIG
        )
    elif backend == 'duckdb':
        return get_storage_backend(
            backend_type='duckdb',
            **DUCKDB_CONFIG
        )
    elif backend == 'bigquery':
        return get_storage_backend(
            backend_type='bigquery',
            **BIGQUERY_CONFIG
        )
    else:
        raise ValueError(f"Unknown storage backend: {STORAGE_BACKEND}")


def print_config():
    """Print current configuration."""
    config = get_storage_config()
    backend = config['backend_type']
    
    print("=" * 60)
    print("CURRENT CONFIGURATION")
    print("=" * 60)
    print(f"Storage Backend: {backend.upper()}")
    
    if backend == 'sqlite':
        print(f"Database Path: {config['db_path']}")
        print(f"Table Name: {config['table_name']}")
        print("Note: SQLite is best for OLTP (transactions)")
    elif backend == 'duckdb':
        print(f"Database Path: {config['db_path']}")
        print(f"Table Name: {config['table_name']}")
        print("Note: DuckDB is optimized for OLAP (analytics)")
    elif backend == 'bigquery':
        print(f"Dataset ID: {config['dataset_id']}")
        print(f"Table Name: {config['table_name']}")
        print(f"Buffer Size: {config['buffer_size']}")
        print("Note: BigQuery is cloud-scale analytics")
    
    print(f"Days to look back: {DEFAULT_DAYS_BACK}")
    print(f"Manila Bulletin sections: {MANILA_BULLETIN_SECTIONS}")
    print("=" * 60)