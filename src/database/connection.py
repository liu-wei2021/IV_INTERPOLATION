# src/database/connection.py - Enhanced version
import psycopg2
from contextlib import contextmanager
from typing import Generator, Optional
import logging
from config import DatabaseConfig

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self._connection_string = None
        self._test_connection()
    
    def _test_connection(self):
        """Test database connection on initialization"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            logger.info("Database connection successful")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    @property
    def connection_string(self) -> str:
        """Get SQLAlchemy-compatible connection string"""
        if self._connection_string is None:
            self._connection_string = (
                f"postgresql://{self.db_config.user}:{self.db_config.password}"
                f"@{self.db_config.host}:{self.db_config.port}/{self.db_config.database}"
            )
        return self._connection_string
    
    @contextmanager
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config.to_dict())
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def get_pandas_connection(self):
        """Get connection optimized for pandas operations"""
        try:
            from sqlalchemy import create_engine
            return create_engine(self.connection_string, pool_pre_ping=True)
        except ImportError:
            logger.warning("SQLAlchemy not available, using psycopg2 connection")
            return self.get_connection()
    
    def execute_query(self, query: str, params=None, fetch: bool = False):
        """Execute a query with optional parameters"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
                conn.commit()