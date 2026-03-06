"""
Abstract storage backend interface for news articles.
Supports SQLite, DuckDB, MotherDuck, and BigQuery storage backends.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List
import sqlite3
import duckdb
import pandas as pd
from google.cloud import bigquery
import os
from dotenv import load_dotenv
import asyncio
import threading
import traceback

from util.tools import setup_logger

# Load environment variables from .env file
load_dotenv()

gcp_project_id = os.getenv('GCP_PROJECT_ID')
table_name = os.getenv('TABLE_NAME')
logger = setup_logger()


class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    def insert_record(self, item: Dict[str, Any]) -> None:
        """Insert a single record into storage. Skips silently if id already exists."""
        pass
    
    @abstractmethod
    def fetch_all(self, query: str) -> List[Any]:
        """Fetch all records matching a query."""
        pass
    
    @abstractmethod
    def run_query(self, query: str, **kwargs):
        """Execute a SQL query."""
        pass

    @abstractmethod
    def record_exists(self, record_id: str) -> bool:
        """Check if a record with the given id already exists."""
        pass

    @abstractmethod
    def upsert_record(self, item: Dict[str, Any]) -> None:
        """
        Insert stub in phase 1, update content fields in phase 2.
        Only updates title/author/publish_time/content/tags — never
        overwrites id/source/url/category/date.
        """
        pass

    @abstractmethod
    def get_pending_articles(self) -> List[Dict[str, Any]]:
        """Return articles where title IS NULL (phase 1 stubs not yet populated)."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the storage connection."""
        pass


class SQLiteBackend(StorageBackend):
    """SQLite storage backend implementation."""
    
    def __init__(self, db_path: str, table_name: str):
        self.db_path = db_path
        self.table_name = table_name
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        logger.info(f"Connected to SQLite database at {db_path}.")
        self._create_table()
    
    def _create_table(self):
        """Create the raw articles table if it doesn't exist."""
        try:
            if self.table_name == table_name:
                self.cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id TEXT PRIMARY KEY,
                        source TEXT,
                        url TEXT,
                        category TEXT,
                        title TEXT,
                        author TEXT,
                        date TEXT,
                        publish_time TEXT,
                        content TEXT, 
                        tags TEXT
                    )
                ''')
                logger.info(f"Created '{table_name}' table if it didn't exist.")
            else:
                raise ValueError(f"Unknown table name: {self.table_name}")
            
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error creating table: {e}")
    
    def insert_record(self, item: Dict[str, Any]) -> None:
        """
        Insert a record into the SQLite database.
        Uses INSERT OR IGNORE so existing records are never overwritten.
        """
        try:
            if self.table_name == table_name:
                self.cursor.execute(f'''
                    INSERT OR IGNORE INTO {table_name}
                        (id, source, url, category, title, author, 
                         date, publish_time, content, tags)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                ''', (
                    item.get('id'),
                    item.get('source'),
                    item.get('url'),
                    item.get('category'),
                    item.get('title'),
                    item.get('author'),
                    item.get('date'),
                    item.get('publish_time'),
                    item.get('cleaned_content'),
                    item.get('tags'),
                ))
            else:
                raise ValueError(f"Unknown table name: {self.table_name}")
        except Exception as e:
            logger.error(f"Error inserting record into SQLite: {e}")
        finally:    
            self.conn.commit()

    def upsert_record(self, item: Dict[str, Any]) -> None:
        """
        Phase 1: INSERT stub only — never overwrites existing content.
        Phase 2: UPDATE content fields on id conflict.
        """
        try:
            if item.get('title') is None:
                # Phase 1 stub — insert only, never overwrite existing content
                self.cursor.execute(f'''
                    INSERT OR IGNORE INTO {self.table_name}
                        (id, source, url, category, date)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    item.get('id'),
                    item.get('source'),
                    item.get('url'),
                    item.get('category'),
                    item.get('date'),
                ))
            else:
                # Phase 2 — update content fields only
                self.cursor.execute(f'''
                    INSERT INTO {self.table_name}
                        (id, source, url, category, title, author,
                        date, publish_time, content, tags)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE SET
                        title        = excluded.title,
                        author       = excluded.author,
                        publish_time = excluded.publish_time,
                        content      = excluded.content,
                        tags         = excluded.tags
                ''', (
                    item.get('id'),
                    item.get('source'),
                    item.get('url'),
                    item.get('category'),
                    item.get('title'),
                    item.get('author'),
                    item.get('date'),
                    item.get('publish_time'),
                    item.get('cleaned_content'),
                    item.get('tags'),
                ))
        except Exception as e:
            logger.error(f"Error upserting record into SQLite: {e}")
        finally:
            self.conn.commit()

    def get_pending_articles(self) -> List[Dict[str, Any]]:
        """Return stub records not yet populated (title IS NULL)."""
        try:
            self.cursor.execute(f'''
                SELECT url, category, date
                FROM {self.table_name}
                WHERE title IS NULL
            ''')
            result = self.cursor.fetchall()
            logger.info(f"Found {len(result)} pending articles.")
            return [{'url': row[0], 'category': row[1], 'date': row[2]} for row in result]
        except Exception as e:
            logger.error(f"Error fetching pending articles from SQLite: {e}")
            return []
    
    def fetch_all(self, query: str) -> List[Any]:
        """Fetch all records from the SQLite database."""
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching records from SQLite: {e}")
            return []
    
    def run_query(self, query: str, params: tuple = None) -> List[Any]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query string
            params: Optional tuple of parameters for parameterized queries
            
        Returns:
            List of results for SELECT queries, empty list for other queries
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            else:
                self.conn.commit()
                logger.info(f"Query executed: {query[:50]}...")
                return []
                
        except Exception as e:
            logger.error(f"Error executing query in SQLite: {e}")
            return []
        
    def record_exists(self, record_id: str) -> bool:
        try:
            self.cursor.execute(
                f'SELECT 1 FROM {self.table_name} WHERE id = ? LIMIT 1',
                (record_id,)
            )
            return self.cursor.fetchone() is not None
        except Exception as e:
            logger.error(f'Error checking record existence in SQLite: {e}')
            return False
    
    def close(self) -> None:
        """Close the SQLite database connection."""
        self.conn.close()
        logger.info("SQLite connection closed.")


class DuckDBBackend(StorageBackend):
    """DuckDB storage backend implementation - optimized for analytics."""
    
    def __init__(self, db_path: str, table_name: str):
        self.db_path = db_path
        self.table_name = table_name
        
        self.conn = duckdb.connect(database=db_path, read_only=False)
        
        logger.info(f"Connected to DuckDB database at {db_path}.")
        self._create_table()
    
    def _create_table(self):
        """Create the raw articles table if it doesn't exist."""
        try:
            if self.table_name == table_name:
                self.conn.execute(f'''
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id VARCHAR PRIMARY KEY,
                        source VARCHAR,
                        url VARCHAR,
                        category VARCHAR,
                        title VARCHAR,
                        author VARCHAR,
                        date DATE,
                        publish_time TIMESTAMP,
                        content VARCHAR,
                        tags VARCHAR
                    )
                ''')
                logger.info(f"Created '{table_name}' table if it didn't exist (DuckDB).")
            else:
                raise ValueError(f"Unknown table name: {self.table_name}")
                
        except Exception as e:
            logger.error(f"Error creating DuckDB table: {e}")
    
    def insert_record(self, item: Dict[str, Any]) -> None:
        """
        Insert a record into the DuckDB database.
        Uses ON CONFLICT DO NOTHING so existing records are never overwritten.
        """
        try:
            if self.table_name == table_name:
                self.conn.execute(f'''
                    INSERT INTO {table_name}
                        (id, source, url, category, title, author, 
                         date, publish_time, content, tags)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT (id) DO NOTHING
                ''', [
                    item.get('id'),
                    item.get('source'),
                    item.get('url'),
                    item.get('category'),
                    item.get('title'),
                    item.get('author'),
                    item.get('date'),
                    item.get('publish_time'),
                    item.get('cleaned_content'),
                    item.get('tags'),
                ])
                logger.debug(f"Inserted record (skipped if exists): {item.get('id')}")
            else:
                raise ValueError(f"Unknown table name: {self.table_name}")
                
        except Exception as e:
            logger.error(f"Error inserting record into DuckDB: {e}")
            logger.error(f"Item: {item}")

    def upsert_record(self, item: Dict[str, Any]) -> None:
        """
        Phase 1: INSERT stub only — never overwrites existing content.
        Phase 2: UPDATE content fields on id conflict.
        """
        try:
            if item.get('title') is None:
                # Phase 1 stub — insert only, never overwrite existing content
                self.conn.execute(f'''
                    INSERT INTO {self.table_name}
                        (id, source, url, category, date)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                ''', [
                    item.get('id'),
                    item.get('source'),
                    item.get('url'),
                    item.get('category'),
                    item.get('date'),
                ])
            else:
                # Phase 2 — update content fields only
                self.conn.execute(f'''
                    INSERT INTO {self.table_name}
                        (id, source, url, category, title, author,
                        date, publish_time, content, tags)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE SET
                        title        = EXCLUDED.title,
                        author       = EXCLUDED.author,
                        publish_time = EXCLUDED.publish_time,
                        content      = EXCLUDED.content,
                        tags         = EXCLUDED.tags
                ''', [
                    item.get('id'),
                    item.get('source'),
                    item.get('url'),
                    item.get('category'),
                    item.get('title'),
                    item.get('author'),
                    item.get('date'),
                    item.get('publish_time'),
                    item.get('cleaned_content'),
                    item.get('tags'),
                ])
            logger.debug(f"Upserted record: {item.get('id')}")
        except Exception as e:
            logger.error(f"Error upserting record into DuckDB: {e}")
            logger.error(f"Item: {item}")

    def get_pending_articles(self) -> List[Dict[str, Any]]:
        """Return stub records not yet populated (title IS NULL)."""
        try:
            result = self.conn.execute(f'''
                SELECT url, category, CAST(date AS VARCHAR) AS date
                FROM {self.table_name}
                WHERE title IS NULL
            ''').fetchall()
            logger.info(f"Found {len(result)} pending articles.")
            return [{'url': row[0], 'category': row[1], 'date': row[2]} for row in result]
        except Exception as e:
            logger.error(f"Error fetching pending articles from DuckDB: {e}")
            return []

    def fetch_all(self, query: str) -> List[Any]:
        """Fetch all records from DuckDB."""
        try:
            result = self.conn.execute(query).fetchall()
            return result
        except Exception as e:
            logger.error(f"Error fetching records from DuckDB: {e}")
            return []
    
    def run_query(self, query: str, return_df: bool = True):
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query string
            return_df: If True, return pandas DataFrame, else return list
            
        Returns:
            pandas DataFrame or list of results
        """
        try:
            if return_df:
                result = self.conn.execute(query).fetchdf()
                logger.info(f"Query executed successfully, returned {len(result)} rows")
                return result
            else:
                result = self.conn.execute(query).fetchall()
                logger.info(f"Query executed successfully")
                return result
                
        except Exception as e:
            logger.error(f"Error executing query in DuckDB: {e}")
            return pd.DataFrame() if return_df else []
    
    def export_to_parquet(self, output_path: str, query: str = None):
        """
        Export data to Parquet format (efficient for analytics).
        
        Args:
            output_path: Path to save Parquet file
            query: Optional SQL query to filter data (default: export all)
        """
        try:
            if query is None:
                query = f"SELECT * FROM {self.table_name}"
            
            self.conn.execute(f"""
                COPY ({query}) TO '{output_path}' (FORMAT PARQUET)
            """)
            logger.info(f"Data exported to {output_path}")
        except Exception as e:
            logger.error(f"Error exporting to Parquet: {e}")
    
    def query_csv_directly(self, csv_path: str, query: str) -> pd.DataFrame:
        """
        Query CSV file directly without loading into DuckDB.
        
        Args:
            csv_path: Path to CSV file
            query: SQL query (use 'read_csv_auto' as table name)
            
        Returns:
            pandas DataFrame with results
        """
        try:
            result = self.conn.execute(f"""
                {query.replace('read_csv_auto', f"read_csv_auto('{csv_path}')")}
            """).fetchdf()
            return result
        except Exception as e:
            logger.error(f"Error querying CSV: {e}")
            return pd.DataFrame()
    
    def record_exists(self, record_id: str) -> bool:
        try:
            result = self.conn.execute(
                f'SELECT 1 FROM {self.table_name} WHERE id = ? LIMIT 1',
                [record_id]
            ).fetchone()
            return result is not None
        except Exception as e:
            logger.error(f'Error checking record existence in DuckDB: {e}')
            return False
    
    def close(self) -> None:
        """Close the DuckDB database connection."""
        self.conn.close()
        logger.info("DuckDB connection closed.")


class MotherDuckBackend(DuckDBBackend):
    """
    MotherDuck (cloud-hosted DuckDB) storage backend.

    Inherits everything from DuckDBBackend — all queries, upserts, and
    schema creation work identically. The only difference is the connection
    string: 'md:<database>' instead of a local file path.

    The duckdb library picks up MOTHERDUCK_TOKEN from the environment
    automatically — no need to pass it explicitly in code.

    Setup:
        1. Sign up at https://app.motherduck.com (free tier available)
        2. Create a database (e.g. 'ph_news')
        3. Go to Settings → Access Tokens → Create Token
        4. Add to your .env or GitHub Actions secrets:
               MOTHERDUCK_TOKEN=your_token_here
               MOTHERDUCK_DB=ph_news
               TABLE_NAME=articles_raw
               STORAGE_BACKEND=motherduck
    """

    def __init__(self, database: str, table_name: str):
        """
        Args:
            database:   MotherDuck database name, e.g. 'ph_news'.
            table_name: Table to read/write, e.g. 'articles_raw'.
        """
        token = os.getenv('MOTHERDUCK_TOKEN')
        if not token:
            raise EnvironmentError(
                'MOTHERDUCK_TOKEN environment variable is not set. '
                'Get your token from https://app.motherduck.com → Settings → Access Tokens.'
            )

        # Set db_path and table_name before calling _create_table via super()
        self.db_path = f'md:{database}'
        self.table_name = table_name

        # duckdb picks up MOTHERDUCK_TOKEN from the environment automatically
        self.conn = duckdb.connect(database=self.db_path, read_only=False)

        logger.info(f"Connected to MotherDuck database '{database}'.")
        self._create_table()


class BigQueryBackend(StorageBackend):
    """BigQuery storage backend implementation with queue-based batch insert."""
    
    def __init__(self, dataset_id: str, table_name: str, buffer_size: int):
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.client = bigquery.Client(project=gcp_project_id)
        self.table_id = f'{gcp_project_id}.{dataset_id}.{table_name}'
        
        # Buffer for batch inserts
        self.buffer = []
        self.buffer_size = buffer_size
        
        # Queue-based processing
        self._queue = asyncio.Queue(maxsize=1000)
        self._processor_task = None
        self._is_processing = False
        
        logger.info(f"Connected to BigQuery dataset {dataset_id}.")
        self._create_dataset_and_table()
        
        # Start the background processor
        self._start_processor()
        
        self._existing_ids: set = self._load_existing_ids()
        
    def _create_dataset_and_table(self):
        """Create the dataset and table if they don't exist."""
        dataset_ref = f'{gcp_project_id}.{self.dataset_id}'
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'
        
        try:
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f'Dataset `{self.dataset_id}` ready.')
        except Exception as e:
            logger.error(f'Dataset creation failed: {e}')
        
        schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("author", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("publish_time", "TIMESTAMP"),
            bigquery.SchemaField("content", "STRING"),
            bigquery.SchemaField("tags", "STRING"),
        ]
        
        table = bigquery.Table(self.table_id, schema=schema)
        
        try:
            self.client.create_table(table, exists_ok=True)
            logger.info(f'Table `{self.table_name}` ready.')
        except Exception as e:
            logger.error(f'Table creation failed: {e}')
    
    def fetch_all(self, query: str) -> List[Any]:
        """Fetch all records from BigQuery."""
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            return list(results)
        except Exception as e:
            logger.error(f"Error fetching records from BigQuery: {e}")
            return []
        
    def run_query(self, query: str) -> pd.DataFrame:
        """
        Execute a BigQuery SQL query and return results as DataFrame.
        
        Args:
            query: SQL query string
            
        Returns:
            pandas DataFrame with query results, empty DataFrame on error
        """
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            df = results.to_dataframe()
            logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error executing query in BigQuery: {e}")
            return pd.DataFrame()

    def _load_existing_ids(self) -> set:
        """Load all existing IDs into memory once to avoid per-record BQ queries."""
        try:
            results = self.client.query(
                f'SELECT id FROM `{self.table_id}`'
            ).result()
            ids = {row.id for row in results}
            logger.info(f'Loaded {len(ids)} existing IDs from BigQuery.')
            return ids
        except Exception as e:
            logger.warning(f'Could not load existing IDs from BigQuery: {e}')
            return set()

    def record_exists(self, record_id: str) -> bool:
        return str(record_id) in self._existing_ids

    def insert_record(self, item: Dict[str, Any]) -> None:
        """
        Add record to queue for processing (non-blocking, synchronous).
        Skips silently if the id is already known — existing records are
        never overwritten.
        """
        record_id = str(item.get('id'))
        if record_id in self._existing_ids:
            logger.debug(f"Skipping existing record: {record_id}")
            return

        self._existing_ids.add(record_id)
        try:
            asyncio.get_event_loop().call_soon_threadsafe(
                self._queue.put_nowait, item
            )
        except Exception as e:
            logger.error(f"Error adding item to queue: {e}")

    def upsert_record(self, item: Dict[str, Any]) -> None:
        """
        Phase 1: skip if record already exists.
        Phase 2: always queue to update content fields.
        """
        record_id = str(item.get('id'))
        if item.get('title') is None:
            # Phase 1 stub — skip entirely if record already exists
            if record_id in self._existing_ids:
                logger.debug(f"Skipping existing record: {record_id}")
                return
            self._existing_ids.add(record_id)
        # Phase 2 items always go through to update content
        try:
            asyncio.get_event_loop().call_soon_threadsafe(
                self._queue.put_nowait, item
            )
        except Exception as e:
            logger.error(f"Error adding item to queue for upsert: {e}")

    def get_pending_articles(self) -> List[Dict[str, Any]]:
        """Return stub records not yet populated (title IS NULL)."""
        try:
            query_job = self.client.query(f'''
                SELECT url, category, CAST(date AS STRING) AS date
                FROM `{self.table_id}`
                WHERE title IS NULL
            ''')
            result = query_job.result()
            rows = [{'url': row.url, 'category': row.category, 'date': row.date} for row in result]
            logger.info(f"Found {len(rows)} pending articles.")
            return rows
        except Exception as e:
            logger.error(f"Error fetching pending articles from BigQuery: {e}")
            return []

    async def _flush_buffer_async(self) -> None:
        """Async version of flush buffer."""
        if not self.buffer:
            return
        
        try:
            df = pd.DataFrame(self.buffer)
            
            if 'id' in df.columns:
                df['id'] = df['id'].astype(str)
            
            # Rename cleaned_content to content
            if 'cleaned_content' in df.columns:
                df = df.rename(columns={'cleaned_content': 'content'})
            
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.normalize()
            if 'publish_time' in df.columns:
                df['publish_time'] = pd.to_datetime(df['publish_time'])
            
            for col in df.columns:
                if df[col].dtype == 'object' and col not in ['date', 'publish_time']:
                    df[col] = df[col].astype(str).replace('None', None)
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self._sync_load_to_bigquery,
                df,
                job_config
            )
            
            logger.info(f'Inserted {len(self.buffer)} records into BigQuery')
            self.buffer = []
            
        except Exception as e:
            logger.error(f"Error batch inserting into BigQuery: {e}")
            logger.error(traceback.format_exc())
            self.buffer = []
    
    def _start_processor(self):
        """Start the background queue processor."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        self._processor_task = asyncio.create_task(self._process_queue())
        self._is_processing = True
        logger.info("Background queue processor started")
    
    async def _process_queue(self):
        """Background task that processes items from the queue."""
        logger.info("Queue processor running...")
        
        try:
            while self._is_processing:
                try:
                    item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                    
                    if item is None:
                        logger.info("Received stop sentinel, shutting down processor")
                        break
                    
                    self.buffer.append(item)
                    
                    if len(self.buffer) >= self.buffer_size:
                        logger.info(f"Buffer full ({len(self.buffer)} items), flushing...")
                        await self._flush_buffer_async()
                    
                    self._queue.task_done()
                    
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Error processing queue item: {e}")
                    logger.error(traceback.format_exc())
                    
            logger.info(f"Queue size: {self._queue.qsize()}, Buffer size: {len(self.buffer)}")
        
        finally:
            if self.buffer:
                logger.info(f"Final flush on shutdown ({len(self.buffer)} items)")
                await self._flush_buffer_async()
            logger.info("Queue processor stopped")
    
    def _sync_load_to_bigquery(self, df, job_config):
        """Synchronous BigQuery load (runs in thread pool)."""
        job = self.client.load_table_from_dataframe(
            df, self.table_id, job_config=job_config
        )
        job.result()
    
    async def _stop_processor(self):
        """Stop the background processor gracefully."""
        logger.info("Stopping queue processor...")
        
        self._is_processing = False
        await self._queue.put(None)
        await self._queue.join()
        
        if self._processor_task:
            await self._processor_task
        
        logger.info("Queue processor stopped successfully")
    
    def get_queue_status(self):
        return {
            'queue_size': self._queue.qsize(),
            'buffer_size': len(self.buffer),
            'is_processing': self._is_processing
        }
    
    def close(self) -> None:
        """Flush remaining buffer and close connection."""
        logger.info("Closing BigQuery backend...")
        
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self._stop_processor())
            else:
                loop.run_until_complete(self._stop_processor())
        except Exception as e:
            logger.error(f"Error stopping processor: {e}")
            if self.buffer:
                logger.warning(f"{len(self.buffer)} items may not have been flushed")
        
        # Dedup: for each id, keep the row with content populated (phase 2 wins
        # over stubs); fall back to most recent publish_time if both have content.
        try:
            self.run_query(f'''
                CREATE OR REPLACE TABLE `{self.table_id}` AS
                SELECT * EXCEPT(row_num)
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY id
                            ORDER BY
                                (title IS NOT NULL) DESC,
                                publish_time DESC
                        ) AS row_num
                    FROM `{self.table_id}`
                )
                WHERE row_num = 1
            ''')
        except Exception as e:
            logger.error(f"Error cleaning duplicates: {e}")
        
        self.client.close()
        logger.info("BigQuery connection closed.")
        

def get_storage_backend(backend_type: str = 'duckdb', **kwargs) -> StorageBackend:
    """
    Factory function to get the appropriate storage backend.
    
    Args:
        backend_type: 'sqlite', 'duckdb', 'motherduck', or 'bigquery'
        **kwargs: Additional arguments to pass to the backend constructor
        
    Returns:
        StorageBackend: An instance of the requested storage backend
    """
    backend_type = backend_type.lower()
    
    if backend_type == 'sqlite':
        sqlite_kwargs = {
            'db_path': kwargs.get('db_path', 'articles_raw.db'),
            'table_name': kwargs.get('table_name', table_name)
        }
        return SQLiteBackend(**sqlite_kwargs)
    
    elif backend_type == 'duckdb':
        duckdb_kwargs = {
            'db_path': kwargs.get('db_path', 'articles_raw.duckdb'),
            'table_name': kwargs.get('table_name', table_name)
        }
        return DuckDBBackend(**duckdb_kwargs)

    elif backend_type == 'motherduck':
        motherduck_kwargs = {
            'database':   kwargs.get('database',   os.getenv('MOTHERDUCK_DB', 'articles_raw')),
            'table_name': kwargs.get('table_name', table_name),
        }
        return MotherDuckBackend(**motherduck_kwargs)
    
    elif backend_type == 'bigquery':
        bigquery_kwargs = {
            'dataset_id': kwargs.get('dataset_id', 'ph_news_raw'),
            'table_name': kwargs.get('table_name', table_name),
            'buffer_size': kwargs.get('buffer_size', 100)
        }
        return BigQueryBackend(**bigquery_kwargs)
    
    else:
        raise ValueError(f"Unknown backend type: {backend_type}. Choose 'sqlite', 'duckdb', 'motherduck', or 'bigquery'.")