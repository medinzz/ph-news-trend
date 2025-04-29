import sqlite3
from typing import Dict, Any
from util.tools import setup_logger

logger = setup_logger()

class SQLiteConnection:
    """SQLite connection manager."""

    def __init__(self, db_path: str, table_name: str):
        self.db_path = db_path
        self.table_name = table_name
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

        logger.info(f"Connected to SQLite database at {db_path}.")
        
        try:
            match table_name:
                case 'articles':
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
                case _:
                    raise ValueError(f"Unknown table name: {table_name}")
            
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            
        self.conn.commit()
    
    def insert_record(self, item: Dict[str, Any]):
        """Insert a record into the database."""
        try:
            match self.table_name:
                case 'articles':
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO articles
                            (
                                id,
                                source,
                                url,
                                category,
                                title,
                                author,
                                date,
                                publish_time,
                                content,
                                tags
                            )
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    ''', (
                        item['id'],
                        item['source'],
                        item['url'],
                        item['category'],
                        item['title'],
                        item['author'],
                        item['date'],
                        item['publish_time'],
                        item['cleaned_content'],
                        item['tags']
                    ))
                case _:
                    raise ValueError(f"Unknown table name: {self.table_name}")
        except Exception as e:
            logger.error(f"Error inserting record: {e}")
        finally:    
            self.conn.commit()
    
    def fetch_all(self, query: str) -> list:
        """Fetch all records from the database."""
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching records: {e}")
            return []
        
    def close(self):
        """Close the database connection."""
        self.conn.close()
            
    