# src/database/optimized_connection.py
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from typing import Generator, Optional
import logging
import threading
import time
from config import DatabaseConfig

logger = logging.getLogger(__name__)

class OptimizedDatabaseManager:
    """
    Production-optimized database manager with connection pooling for 32M+ rows
    
    Key Optimizations:
    - Connection pooling to eliminate connection overhead
    - Thread-safe operations for multiprocessing
    - Batch optimization with configurable sizes
    - Connection health monitoring
    - Memory-efficient cursor management
    """
    
    def __init__(self, db_config: DatabaseConfig, pool_size: int = 20, max_overflow: int = 40):
        self.db_config = db_config
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self._connection_pool = None
        self._lock = threading.Lock()
        self._init_connection_pool()
    
    def _init_connection_pool(self):
        """Initialize connection pool with optimized settings"""
        try:
            self._connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.pool_size // 2,  # Minimum connections
                maxconn=self.pool_size + self.max_overflow,  # Maximum connections
                host=self.db_config.host,
                database=self.db_config.database,
                user=self.db_config.user,
                password=self.db_config.password,
                port=self.db_config.port
                # Note: Server-level optimizations should be set in postgresql.conf
            )
            logger.info(f"Connection pool initialized: {self.pool_size}+{self.max_overflow} connections")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Get connection from pool with automatic cleanup"""
        conn = None
        try:
            with self._lock:
                conn = self._connection_pool.getconn()
            
            # Optimize connection for bulk operations
            conn.autocommit = False
            
            yield conn
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if conn:
                with self._lock:
                    self._connection_pool.putconn(conn)
    
    def execute_batch_insert(self, query: str, data_batch: list, page_size: int = 5000) -> int:
        """
        Optimized batch insert with transaction management
        
        For 32M rows, this is critical for performance
        """
        total_inserted = 0
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Start transaction
                    cur.execute("BEGIN")
                    
                    # Process in chunks to manage memory
                    for i in range(0, len(data_batch), page_size):
                        chunk = data_batch[i:i + page_size]
                        
                        import psycopg2.extras
                        psycopg2.extras.execute_values(
                            cur, query, chunk,
                            template=None, page_size=min(page_size, 1000)
                        )
                        total_inserted += len(chunk)
                        
                        # Log progress for large batches
                        if total_inserted % 10000 == 0:
                            logger.debug(f"Inserted {total_inserted}/{len(data_batch)} rows")
                    
                    # Commit transaction
                    conn.commit()
                    logger.debug(f"Batch insert complete: {total_inserted} rows")
                    
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            raise
        
        return total_inserted
    
    def execute_streaming_query(self, query: str, params=None, fetch_size: int = 10000):
        """
        Memory-efficient streaming query for large datasets
        
        Critical for processing 32M rows without OOM
        """
        with self.get_connection() as conn:
            with conn.cursor(name='streaming_cursor') as cur:
                # Set cursor to stream results
                cur.itersize = fetch_size
                cur.execute(query, params)
                
                while True:
                    batch = cur.fetchmany(fetch_size)
                    if not batch:
                        break
                    yield batch
    
    def execute_optimized_query(self, query: str, params=None, fetch: bool = False):
        """Execute query with connection pooling"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
                conn.commit()
    
    def get_table_stats(self, table_name: str) -> dict:
        """Get table statistics for optimization planning"""
        stats_query = f"""
        SELECT 
            schemaname,
            tablename,
            n_tup_ins as inserts,
            n_tup_upd as updates,
            n_tup_del as deletes,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables 
        WHERE tablename = %s
        """
        
        result = self.execute_optimized_query(stats_query, (table_name,), fetch=True)
        
        if result:
            row = result[0]
            return {
                'table_name': row[1],
                'live_tuples': row[5],
                'dead_tuples': row[6],
                'last_vacuum': row[7],
                'last_analyze': row[9],
                'needs_vacuum': row[6] > row[5] * 0.2 if row[5] > 0 else False
            }
        return {}
    
    def optimize_table(self, table_name: str):
        """Run VACUUM and ANALYZE for better performance"""
        try:
            with self.get_connection() as conn:
                # VACUUM and ANALYZE cannot run in transaction
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(f"VACUUM ANALYZE {table_name}")
                logger.info(f"Table {table_name} optimized")
        except Exception as e:
            logger.error(f"Failed to optimize table {table_name}: {e}")
    
    def get_connection_pool_stats(self) -> dict:
        """Monitor connection pool health"""
        if not self._connection_pool:
            return {}
        
        with self._lock:
            return {
                'pool_size': self.pool_size,
                'max_overflow': self.max_overflow,
                'checked_in': len(self._connection_pool._pool),
                'checked_out': self._connection_pool._maxconn - len(self._connection_pool._pool),
                'overflow': max(0, self._connection_pool._maxconn - self.pool_size)
            }
    
    def close_pool(self):
        """Cleanup connection pool"""
        if self._connection_pool:
            with self._lock:
                self._connection_pool.closeall()
            logger.info("Connection pool closed")


class MemoryOptimizedProcessor:
    """
    Memory-efficient processor for large datasets
    
    Key features:
    - Streaming data processing
    - Memory monitoring
    - Garbage collection optimization
    - Batch size auto-tuning
    """
    
    def __init__(self, db_manager: OptimizedDatabaseManager, config):
        self.db_manager = db_manager
        self.config = config
        self.memory_threshold_gb = config.processing.memory_limit_gb
        self.current_batch_size = config.processing.chunk_size
        self.min_batch_size = 1000
        self.max_batch_size = 100000
    
    def get_memory_usage_gb(self) -> float:
        """Get current memory usage in GB"""
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / (1024 ** 3)
    
    def auto_tune_batch_size(self, processing_time: float, memory_used: float) -> int:
        """
        Automatically tune batch size based on performance and memory
        
        Critical for 32M row processing
        """
        memory_ratio = memory_used / self.memory_threshold_gb
        
        if memory_ratio > 0.8:
            # Reduce batch size if near memory limit
            self.current_batch_size = max(
                self.min_batch_size,
                int(self.current_batch_size * 0.7)
            )
        elif memory_ratio < 0.4 and processing_time < 30:
            # Increase batch size if memory is available and processing is fast
            self.current_batch_size = min(
                self.max_batch_size,
                int(self.current_batch_size * 1.3)
            )
        
        return self.current_batch_size
    
    def process_large_symbol_streaming(self, symbol: str, query: str):
        """
        Process large symbols using streaming to avoid memory issues
        
        Essential for symbols with 100K+ rows
        """
        total_processed = 0
        batch_buffer = []
        
        # Stream data in chunks
        for batch in self.db_manager.execute_streaming_query(query, (symbol,), fetch_size=5000):
            batch_buffer.extend(batch)
            
            # Process when batch is full or memory threshold reached
            if len(batch_buffer) >= self.current_batch_size or self.get_memory_usage_gb() > self.memory_threshold_gb * 0.7:
                processed_count = self._process_batch(batch_buffer)
                total_processed += processed_count
                
                # Clear buffer and force garbage collection
                batch_buffer.clear()
                import gc
                gc.collect()
                
                # Log progress
                if total_processed % 50000 == 0:
                    memory_gb = self.get_memory_usage_gb()
                    logger.info(f"Processed {total_processed} rows, Memory: {memory_gb:.2f}GB")
        
        # Process remaining data
        if batch_buffer:
            total_processed += self._process_batch(batch_buffer)
        
        return total_processed
    
    def _process_batch(self, batch_data: list) -> int:
        """Process a batch of data with error handling"""
        try:
            # Your interpolation logic here
            # Return number of processed rows
            return len(batch_data)
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            return 0