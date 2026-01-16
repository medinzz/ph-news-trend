"""
Abstract storage backend interface for news articles.
Supports both SQLite and BigQuery storage backends.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List
import sqlite3
import pandas as pd
from google.cloud import bigquery
import os
from dotenv import load_dotenv

from util.tools import setup_logger

# Load environment variables from .env file
load_dotenv()

gcp_project_id = os.getenv('GCP_PROJECT_ID')
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
        """Create the articles table if it doesn't exist."""
        try:
            if self.table_name == 'articles':
                self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS articles (
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
                logger.info("Created 'articles' table if it didn't exist.")
            else:
                raise ValueError(f"Unknown table name: {self.table_name}")
            
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error creating table: {e}")
    
    def insert_record(self, item: Dict[str, Any]) -> None:
        """Insert a record into the SQLite database."""
        try:
            if self.table_name == 'articles':
                self.cursor.execute('''
                    INSERT OR REPLACE INTO articles
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


class BigQueryBackend(StorageBackend):
    """BigQuery storage backend implementation with batch insert support."""
    
    def __init__(self, dataset_id: str, table_name: str, buffer_size: int):
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.client = bigquery.Client(project=gcp_project_id)
        self.table_id = f'{gcp_project_id}.{dataset_id}.{table_name}'
        
        # Buffer for batch inserts
        self.buffer = []
        self.buffer_size = 100  # Insert every 100 records
        
        logger.info(f"Connected to BigQuery dataset {dataset_id}.")
        self._create_dataset_and_table()
    
    
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
    

    def insert_record(self, item: Dict[str, Any]) -> None:
        """Add record to buffer for batch insert."""
        self.buffer.append(item)
        
        # Flush buffer when it reaches the threshold
        if len(self.buffer) >= self.buffer_size:
            self._flush_buffer()
            
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
    
    def close(self) -> None:
        """Flush remaining buffer and close connection."""
        # Insert any remaining records
        self._flush_buffer()  
        # update the table's expiration time by refreshing the table
        self.run_query(f'CREATE OR REPLACE TABLE {self.table_id} AS SELECT * FROM {self.table_id};')
        self.client.close()
        logger.info("BigQuery connection closed.")
    
    def _flush_buffer(self) -> None:
        """Insert all buffered records using load_table_from_dataframe."""
        if not self.buffer:
            return
        
        try:
            import pandas as pd
            
            # Convert buffer to DataFrame
            df = pd.DataFrame(self.buffer)
            
            # Rename cleaned_content to content to match schema
            if 'cleaned_content' in df.columns:
                df = df.rename(columns={'cleaned_content': 'content'})
            
            # Ensure proper data types
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.date
            if 'publish_time' in df.columns:
                df['publish_time'] = pd.to_datetime(df['publish_time'])
            
            # Load to BigQuery
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            
            job = self.client.load_table_from_dataframe(
                df, self.table_id, job_config=job_config
            )
            job.result()  # Wait for completion
            
            logger.info(f'Inserted {len(self.buffer)} records into BigQuery')
            self.buffer = []  # Clear buffer
            
        except Exception as e:
            logger.error(f"Error batch inserting into BigQuery: {e}")
            self.buffer = []  # Clear buffer even on error to avoid re-trying bad data


def get_storage_backend(backend_type: str = 'sqlite', **kwargs) -> StorageBackend:
    """
    Factory function to get the appropriate storage backend.
    
    Args:
        backend_type: Either 'sqlite' or 'bigquery'
        **kwargs: Additional arguments to pass to the backend constructor
        
    Returns:
        StorageBackend: An instance of the requested storage backend
    """
    backends = {
        'sqlite': SQLiteBackend,
        'bigquery': BigQueryBackend,
    }
    
    backend_class = backends.get(backend_type.lower())
    if not backend_class:
        raise ValueError(f"Unknown backend type: {backend_type}. Choose 'sqlite' or 'bigquery'.")
    
    return backend_class(**kwargs)