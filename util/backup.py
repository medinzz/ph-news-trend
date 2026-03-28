"""
Incremental backup utility — syncs data from MotherDuck to a local DuckDB file.

Only copies records not already present locally (ON CONFLICT DO NOTHING),
so re-running is always safe. Uses DUCKDB_DB_PATH from your .env as the
local backup target.

Usage:
    # From CLI:
    python main.py --backup

    # From a Python shell or notebook:
    from util.backup import backup_to_local
    backup_to_local()
"""

import os
import duckdb
from dotenv import load_dotenv
from util.tools import setup_logger

load_dotenv()
logger = setup_logger()


def backup_to_local(
    table_name: str = None,
    local_db_path: str = None,
) -> None:
    """
    Incrementally sync articles_raw from MotherDuck to a local DuckDB file.

    Args:
        table_name:    Table to back up. Defaults to TABLE_NAME env var.
        local_db_path: Path to local DuckDB file. Defaults to DUCKDB_DB_PATH env var.
    """
    table_name    = table_name    or os.getenv('TABLE_NAME', 'articles_raw')
    local_db_path = local_db_path or os.getenv('DUCKDB_DB_PATH', 'articles_raw.duckdb')
    motherduck_db = os.getenv('MOTHERDUCK_DB', 'ph_news')
    token         = os.getenv('MOTHERDUCK_TOKEN')

    if not token:
        raise EnvironmentError(
            'MOTHERDUCK_TOKEN is not set. '
            'Get your token from https://app.motherduck.com -> Settings -> Access Tokens.'
        )

    logger.info(f'Starting incremental backup: md:{motherduck_db}.{table_name} -> {local_db_path}')

    # ── Step 1: Connect to MotherDuck and fetch all records ───────────────
    logger.info('Connecting to MotherDuck...')
    md_conn = duckdb.connect(f'md:{motherduck_db}', read_only=True)

    try:
        total_remote = md_conn.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()[0]
        logger.info(f'MotherDuck: {total_remote:,} total records in {table_name}')

        remote_df = md_conn.execute(f'SELECT * FROM {table_name}').fetchdf()
    finally:
        md_conn.close()
        logger.info('MotherDuck connection closed.')

    # ── Step 2: Connect to local DuckDB ──────────────────────────────────
    logger.info(f'Connecting to local DuckDB at {local_db_path}...')
    local_conn = duckdb.connect(local_db_path, read_only=False)

    try:
        # Create table if it doesn't exist locally
        local_conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id           VARCHAR PRIMARY KEY,
                source       VARCHAR,
                url          VARCHAR,
                category     VARCHAR,
                title        VARCHAR,
                author       VARCHAR,
                date         DATE,
                publish_time TIMESTAMP,
                content      VARCHAR,
                tags         VARCHAR
            )
        ''')

        total_local_before = local_conn.execute(
            f'SELECT COUNT(*) FROM {table_name}'
        ).fetchone()[0]
        logger.info(f'Local DuckDB: {total_local_before:,} existing records before backup.')

        # ── Step 3: Incremental insert — skip records already in local DB ─
        # Register the remote DataFrame as a temp view so we can INSERT from it
        local_conn.register('remote_data', remote_df)

        local_conn.execute(f'''
            INSERT INTO {table_name}
            SELECT * FROM remote_data
            ON CONFLICT (id) DO NOTHING
        ''')

        local_conn.unregister('remote_data')

        total_local_after = local_conn.execute(
            f'SELECT COUNT(*) FROM {table_name}'
        ).fetchone()[0]

        inserted = total_local_after - total_local_before

        # ── Step 4: Summary ───────────────────────────────────────────────
        print(f'\n{"=" * 50}')
        print(f'  BACKUP COMPLETE')
        print(f'{"=" * 50}')
        print(f'  Source:    md:{motherduck_db}.{table_name}')
        print(f'  Target:    {local_db_path}')
        print(f'  Remote:    {total_remote:,} records')
        print(f'  Inserted:  {inserted:,} new records')
        print(f'  Skipped:   {total_remote - inserted:,} already existed locally')
        print(f'  Local now: {total_local_after:,} total records')
        print(f'{"=" * 50}\n')

        logger.info(f'Backup complete — {inserted:,} new records added to {local_db_path}.')

    finally:
        local_conn.close()
        logger.info('Local DuckDB connection closed.')