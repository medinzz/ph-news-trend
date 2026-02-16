"""
Abstract storage backend interface for news articles.
Supports SQLite, DuckDB, and BigQuery storage backends.
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
        """Insert a single record into storage."""
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
        """Insert a record into the SQLite database."""
        try:
            if self.table_name == table_name:
                self.cursor.execute(f'''
                    INSERT OR REPLACE INTO {table_name}
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
                    item.get('tags')
                ))
            else:
                raise ValueError(f"Unknown table name: {self.table_name}")
        except Exception as e:
            logger.error(f"Error inserting record into SQLite: {e}")
        finally:    
            self.conn.commit()
    
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
            
            # If it's a SELECT query, return results
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            else:
                # For INSERT, UPDATE, DELETE, commit and return empty list
                self.conn.commit()
                logger.info(f"Query executed: {query[:50]}...")
                return []
                
        except Exception as e:
            logger.error(f"Error executing query in SQLite: {e}")
            return []
    
    def close(self) -> None:
        """Close the SQLite database connection."""
        self.conn.close()
        logger.info("SQLite connection closed.")


class DuckDBBackend(StorageBackend):
    """DuckDB storage backend implementation - optimized for analytics."""
    
    def __init__(self, db_path: str, table_name: str):
        self.db_path = db_path
        self.table_name = table_name
        
        # Connect to DuckDB (creates file if doesn't exist)
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
        """Insert a record into the DuckDB database."""
        try:
            if self.table_name == table_name:
                # DuckDB doesn't have INSERT OR REPLACE, use INSERT OR IGNORE then UPDATE
                self.conn.execute(f'''
                    INSERT INTO {table_name}
                        (id, source, url, category, title, author, 
                         date, publish_time, content, tags)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT (id) DO UPDATE SET
                        source = EXCLUDED.source,
                        url = EXCLUDED.url,
                        category = EXCLUDED.category,
                        title = EXCLUDED.title,
                        author = EXCLUDED.author,
                        date = EXCLUDED.date,
                        publish_time = EXCLUDED.publish_time,
                        content = EXCLUDED.content,
                        tags = EXCLUDED.tags
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
                    item.get('tags')
                ])
                logger.debug(f"Inserted/Updated record: {item.get('id')}")
            else:
                raise ValueError(f"Unknown table name: {self.table_name}")
                
        except Exception as e:
            logger.error(f"Error inserting record into DuckDB: {e}")
            logger.error(f"Item: {item}")
    
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
                # Return as DataFrame (great for analytics)
                result = self.conn.execute(query).fetchdf()
                logger.info(f"Query executed successfully, returned {len(result)} rows")
                return result
            else:
                # Return as list
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
            # DuckDB can query CSV files directly!
            result = self.conn.execute(f"""
                {query.replace('read_csv_auto', f"read_csv_auto('{csv_path}')")}
            """).fetchdf()
            return result
        except Exception as e:
            logger.error(f"Error querying CSV: {e}")
            return pd.DataFrame()
    
    def close(self) -> None:
        """Close the DuckDB database connection."""
        self.conn.close()
        logger.info("DuckDB connection closed.")


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
        
    
    def _create_dataset_and_table(self):
        """Create the dataset and table if they don't exist."""
        # Create dataset
        dataset_ref = f'{gcp_project_id}.{self.dataset_id}'
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'
        
        try:
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f'Dataset `{self.dataset_id}` ready.')
        except Exception as e:
            logger.error(f'Dataset creation failed: {e}')
        
        # Define table schema
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
    
    def insert_record(self, item: Dict[str, Any]) -> None:
        """Add record to queue for processing (non-blocking, synchronous)."""
        try:
            # Put item in queue without blocking
            # This is safe to call from sync code
            asyncio.get_event_loop().call_soon_threadsafe(
                self._queue.put_nowait, item
            )
        except Exception as e:
            logger.error(f"Error adding item to queue: {e}")
    
    async def _flush_buffer_async(self) -> None:
        """Async version of flush buffer."""
        if not self.buffer:
            return
        
        try:
            # Convert buffer to DataFrame
            df = pd.DataFrame(self.buffer)
            
            # Convert id to string
            if 'id' in df.columns:
                df['id'] = df['id'].astype(str)
            
            # Rename cleaned_content to content
            if 'cleaned_content' in df.columns:
                df = df.rename(columns={'cleaned_content': 'content'})
            
            # Ensure proper data types
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.normalize()
            if 'publish_time' in df.columns:
                df['publish_time'] = pd.to_datetime(df['publish_time'])
            
            # Convert object columns to string (but exclude date/publish_time)
            for col in df.columns:
                if df[col].dtype == 'object' and col not in ['date', 'publish_time']:
                    df[col] = df[col].astype(str).replace('None', None)
            
            # Load to BigQuery (run in thread pool to not block event loop)
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            
            # Run blocking BigQuery operation in thread
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,  # Use default executor
                self._sync_load_to_bigquery,
                df,
                job_config
            )
            
            logger.info(f'Inserted {len(self.buffer)} records into BigQuery')
            self.buffer = []  # Clear buffer
            
        except Exception as e:
            logger.error(f"Error batch inserting into BigQuery: {e}")
            logger.error(traceback.format_exc())
            self.buffer = []  # Clear buffer even on error
    
    
    def _start_processor(self):
        """Start the background queue processor."""
        try:
            # Get or create event loop
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Start the processor task
        self._processor_task = asyncio.create_task(self._process_queue())
        self._is_processing = True
        logger.info("Background queue processor started")
    
    async def _process_queue(self):
        """Background task that processes items from the queue."""
        logger.info("Queue processor running...")
        
        try:
            while self._is_processing:
                try:
                    # Wait for item with timeout to check _is_processing flag
                    item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                    
                    if item is None:  # Sentinel value to stop
                        logger.info("Received stop sentinel, shutting down processor")
                        break
                    
                    # Add to buffer
                    self.buffer.append(item)
                    
                    # Flush if buffer is full
                    if len(self.buffer) >= self.buffer_size:
                        logger.info(f"Buffer full ({len(self.buffer)} items), flushing...")
                        await self._flush_buffer_async()
                    
                    # Mark task as done
                    self._queue.task_done()
                    
                except asyncio.TimeoutError:
                    # No item received, continue loop to check _is_processing flag
                    continue
                except Exception as e:
                    logger.error(f"Error processing queue item: {e}")
                    logger.error(traceback.format_exc())
                    
            logger.info(f"Queue size: {self._queue.qsize()}, Buffer size: {len(self.buffer)}")
        
        finally:
            # Final flush when stopping
            if self.buffer:
                logger.info(f"Final flush on shutdown ({len(self.buffer)} items)")
                await self._flush_buffer_async()
            logger.info("Queue processor stopped")
    
    def _sync_load_to_bigquery(self, df, job_config):
        """Synchronous BigQuery load (runs in thread pool)."""
        job = self.client.load_table_from_dataframe(
            df, self.table_id, job_config=job_config
        )
        job.result()  # Wait for completion
    
    async def _stop_processor(self):
        """Stop the background processor gracefully."""
        logger.info("Stopping queue processor...")
        
        # Signal processor to stop
        self._is_processing = False
        
        # Send sentinel value
        await self._queue.put(None)
        
        # Wait for all queued items to be processed
        await self._queue.join()
        
        # Wait for processor task to complete
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
        
        # Stop the processor (this will flush remaining items)
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If loop is running, schedule the stop
                asyncio.ensure_future(self._stop_processor())
            else:
                # If loop is not running, run it
                loop.run_until_complete(self._stop_processor())
        except Exception as e:
            logger.error(f"Error stopping processor: {e}")
            # Manual flush as fallback
            if self.buffer:
                logger.info("Attempting manual flush...")
                logger.warning(f"{len(self.buffer)} items may not have been flushed")
        
        # Clean up duplicates
        try:
            self.run_query(
                f'CREATE OR REPLACE TABLE {self.table_id} AS '
                f'SELECT DISTINCT * FROM {self.table_id};'
            )
        except Exception as e:
            logger.error(f"Error cleaning duplicates: {e}")
        
        self.client.close()
        logger.info("BigQuery connection closed.")
        

def get_storage_backend(backend_type: str = 'duckdb', **kwargs) -> StorageBackend:
    """
    Factory function to get the appropriate storage backend.
    
    Args:
        backend_type: 'sqlite', 'duckdb', or 'bigquery'
        **kwargs: Additional arguments to pass to the backend constructor
        
    Returns:
        StorageBackend: An instance of the requested storage backend
    """
    backend_type = backend_type.lower()
    
    if backend_type == 'sqlite':
        # Extract only SQLite-specific parameters
        sqlite_kwargs = {
            'db_path': kwargs.get('db_path', 'articles_raw.db'),
            'table_name': kwargs.get('table_name', table_name)
        }
        return SQLiteBackend(**sqlite_kwargs)
    
    elif backend_type == 'duckdb':
        # Extract only DuckDB-specific parameters
        duckdb_kwargs = {
            'db_path': kwargs.get('db_path', 'articles_raw.duckdb'),
            'table_name': kwargs.get('table_name', table_name)
        }
        return DuckDBBackend(**duckdb_kwargs)
    
    elif backend_type == 'bigquery':
        # Extract only BigQuery-specific parameters
        bigquery_kwargs = {
            'dataset_id': kwargs.get('dataset_id', 'ph_news_raw'),
            'table_name': kwargs.get('table_name', table_name),
            'buffer_size': kwargs.get('buffer_size', 100)
        }
        return BigQueryBackend(**bigquery_kwargs)
    
    else:
        raise ValueError(f"Unknown backend type: {backend_type}. Choose 'sqlite', 'duckdb', or 'bigquery'.")