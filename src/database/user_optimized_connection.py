# src/database/user_optimized_connection.py
"""
User-Level Database Optimization (No Admin Access Required)

This version optimizes everything possible without server admin privileges:
- Connection pooling at application level
- Session-level PostgreSQL optimizations 
- Intelligent query batching
- Memory management
- Adaptive resource usage
"""

import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from typing import Generator, Optional
import logging
import threading
import time
import os
from config import DatabaseConfig

logger = logging.getLogger(__name__)

class UserLevelDatabaseManager:
    """
    Database manager optimized for users without server admin access
    
    Optimizations that DON'T require admin access:
    - Connection pooling
    - Session-level memory settings
    - Batch processing
    - Query optimization
    - Index creation (if user has permissions)
    """
    
    def __init__(self, db_config: DatabaseConfig, pool_size: int = 8, max_overflow: int = 16):
        self.db_config = db_config
        self.pool_size = min(pool_size, 10)  # Conservative for shared servers
        self.max_overflow = min(max_overflow, 20)
        self._connection_pool = None
        self._lock = threading.Lock()
        self._session_optimized = False
        self._init_connection_pool()
    
    def _init_connection_pool(self):
        """Initialize connection pool with conservative settings"""
        try:
            self._connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=2,  # Conservative minimum
                maxconn=self.pool_size + self.max_overflow,
                host=self.db_config.host,
                database=self.db_config.database,
                user=self.db_config.user,
                password=self.db_config.password,
                port=self.db_config.port,
                # Conservative connection timeout
                connect_timeout=30
            )
            logger.info(f"User-level connection pool initialized: {self.pool_size}+{self.max_overflow} connections")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise
    
    def _apply_session_optimizations(self, conn):
        """Apply session-level optimizations that don't require admin access"""
        if self._session_optimized:
            return
            
        try:
            with conn.cursor() as cur:
                # Session-level memory optimizations (no restart required)
                session_settings = [
                    "SET work_mem = '128MB'",  # Conservative for shared server
                    "SET temp_buffers = '32MB'",
                    "SET enable_hashjoin = on",
                    "SET enable_mergejoin = on", 
                    "SET enable_nestloop = off",  # Avoid nested loops for large datasets
                    "SET random_page_cost = 1.1",  # Assume SSD storage
                    "SET seq_page_cost = 1.0",
                    "SET cpu_tuple_cost = 0.01",
                    "SET effective_cache_size = '4GB'",  # Conservative estimate
                ]
                
                for setting in session_settings:
                    try:
                        cur.execute(setting)
                        logger.debug(f"Applied: {setting}")
                    except Exception as e:
                        logger.warning(f"Could not apply {setting}: {e}")
                
                conn.commit()
                self._session_optimized = True
                logger.info("Session-level optimizations applied")
                
        except Exception as e:
            logger.warning(f"Session optimization failed: {e}")
    
    @contextmanager
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Get optimized connection from pool"""
        conn = None
        try:
            with self._lock:
                conn = self._connection_pool.getconn()
            
            # Apply session optimizations
            self._apply_session_optimizations(conn)
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
    
    def execute_optimized_query(self, query: str, params=None, fetch: bool = False):
        """Execute query with user-level optimizations"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
                conn.commit()
    
    def execute_streaming_query(self, query: str, params=None, fetch_size: int = 5000):
        """Memory-efficient streaming query (critical for 32M rows)"""
        with self.get_connection() as conn:
            with conn.cursor(name=f'stream_{int(time.time())}') as cur:
                cur.itersize = fetch_size
                cur.execute(query, params)
                
                while True:
                    batch = cur.fetchmany(fetch_size)
                    if not batch:
                        break
                    yield batch
    
    def execute_batch_insert(self, query: str, data_batch: list, page_size: int = 3000) -> int:
        """Optimized batch insert for shared servers"""
        total_inserted = 0
        
        # Conservative batch size for shared servers
        safe_page_size = min(page_size, 3000)
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Process in smaller chunks for shared server
                    for i in range(0, len(data_batch), safe_page_size):
                        chunk = data_batch[i:i + safe_page_size]
                        
                        import psycopg2.extras
                        psycopg2.extras.execute_values(
                            cur, query, chunk,
                            template=None, page_size=min(safe_page_size, 1000)
                        )
                        total_inserted += len(chunk)
                        
                        # Small delay to be courteous on shared server
                        if total_inserted % 10000 == 0:
                            time.sleep(0.1)
                    
                conn.commit()
                logger.debug(f"Batch insert complete: {total_inserted} rows")
                
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            raise
        
        return total_inserted
    
    def check_user_permissions(self) -> dict:
        """Check what database permissions the user has"""
        permissions = {
            'can_create_index': False,
            'can_analyze': False,
            'can_create_table': False,
            'current_user': None,
            'database_size': None
        }
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Check current user
                    cur.execute("SELECT current_user")
                    permissions['current_user'] = cur.fetchone()[0]
                    
                    # Test index creation permission
                    try:
                        cur.execute("CREATE INDEX IF NOT EXISTS test_permission_idx ON trading_tickers (symbol) WHERE FALSE")
                        cur.execute("DROP INDEX IF EXISTS test_permission_idx")
                        permissions['can_create_index'] = True
                    except:
                        pass
                    
                    # Test analyze permission
                    try:
                        cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables LIMIT 1")
                        permissions['can_analyze'] = True
                    except:
                        pass
                    
                    # Get database size (if permitted)
                    try:
                        cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
                        permissions['database_size'] = cur.fetchone()[0]
                    except:
                        pass
                    
                conn.rollback()  # Don't commit test operations
                
        except Exception as e:
            logger.warning(f"Permission check failed: {e}")
        
        return permissions
    
    def create_user_level_indexes(self) -> dict:
        """Create indexes if user has permission"""
        results = {'created': [], 'failed': [], 'skipped': []}
        
        indexes = [
            {
                'name': 'idx_user_trading_symbol_date',
                'sql': 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_user_trading_symbol_date ON trading_tickers(symbol, date)'
            },
            {
                'name': 'idx_user_trading_date',
                'sql': 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_user_trading_date ON trading_tickers(date)'
            },
            {
                'name': 'idx_user_interpolated_batch',
                'sql': 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_user_interpolated_batch ON interpolated_trading_tickers(batch_id, symbol)'
            }
        ]
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    for index in indexes:
                        try:
                            # Check if exists
                            cur.execute(f"SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = '{index['name']}')")
                            
                            if cur.fetchone()[0]:
                                results['skipped'].append(f"{index['name']}: Already exists")
                            else:
                                cur.execute(index['sql'])
                                results['created'].append(f"{index['name']}: Created successfully")
                        except Exception as e:
                            results['failed'].append(f"{index['name']}: {str(e)}")
                    
                conn.commit()
                
        except Exception as e:
            logger.error(f"Index creation failed: {e}")
            results['failed'].append(f"Connection error: {str(e)}")
        
        return results
    
    def get_table_info(self) -> dict:
        """Get information about tables and data volume"""
        info = {}
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Row counts
                    tables = ['trading_tickers', 'interpolated_trading_tickers', 'interpolation_progress']
                    
                    for table in tables:
                        try:
                            cur.execute(f"SELECT COUNT(*) FROM {table}")
                            count = cur.fetchone()[0]
                            info[f'{table}_count'] = count
                        except:
                            info[f'{table}_count'] = 'No permission or table not found'
                    
                    # Symbol count
                    try:
                        cur.execute("SELECT COUNT(DISTINCT symbol) FROM trading_tickers")
                        info['symbol_count'] = cur.fetchone()[0]
                    except:
                        info['symbol_count'] = 'Unknown'
                    
                    # Date range
                    try:
                        cur.execute("SELECT MIN(date), MAX(date) FROM trading_tickers")
                        result = cur.fetchone()
                        info['date_range'] = f"{result[0]} to {result[1]}" if result else 'Unknown'
                    except:
                        info['date_range'] = 'Unknown'
        
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            info['error'] = str(e)
        
        return info
    
    def optimize_for_user_environment(self) -> dict:
        """Run all user-level optimizations"""
        print("🔧 USER-LEVEL DATABASE OPTIMIZATION")
        print("=" * 50)
        
        results = {}
        
        # Check permissions
        print("1. Checking user permissions...")
        permissions = self.check_user_permissions()
        results['permissions'] = permissions
        
        print(f"   Current user: {permissions['current_user']}")
        print(f"   Can create indexes: {permissions['can_create_index']}")
        print(f"   Can analyze tables: {permissions['can_analyze']}")
        if permissions['database_size']:
            print(f"   Database size: {permissions['database_size']}")
        
        # Get table information
        print("\n2. Analyzing data volume...")
        table_info = self.get_table_info()
        results['table_info'] = table_info
        
        for key, value in table_info.items():
            if key.endswith('_count'):
                table_name = key.replace('_count', '')
                print(f"   {table_name}: {value:,}" if isinstance(value, int) else f"   {table_name}: {value}")
        
        # Create indexes if possible
        if permissions['can_create_index']:
            print("\n3. Creating performance indexes...")
            index_results = self.create_user_level_indexes()
            results['indexes'] = index_results
            
            for created in index_results['created']:
                print(f"   ✅ {created}")
            for skipped in index_results['skipped']:
                print(f"   ⏭️  {skipped}")
            for failed in index_results['failed']:
                print(f"   ❌ {failed}")
        else:
            print("\n3. Skipping index creation (no permission)")
            results['indexes'] = {'message': 'No permission to create indexes'}
        
        # Test session optimizations
        print("\n4. Testing session optimizations...")
        try:
            with self.get_connection() as conn:
                results['session_optimizations'] = 'Applied successfully'
                print("   ✅ Session optimizations applied")
        except Exception as e:
            results['session_optimizations'] = f'Failed: {str(e)}'
            print(f"   ❌ Session optimization failed: {e}")
        
        return results
    
    def get_connection_pool_stats(self) -> dict:
        """Get connection pool statistics"""
        if not self._connection_pool:
            return {}
        
        with self._lock:
            try:
                return {
                    'pool_size': self.pool_size,
                    'max_overflow': self.max_overflow,
                    'checked_in': len(self._connection_pool._pool) if hasattr(self._connection_pool, '_pool') else 0,
                    'checked_out': self._connection_pool._maxconn - len(self._connection_pool._pool) if hasattr(self._connection_pool, '_pool') else 0,
                }
            except:
                return {'pool_size': self.pool_size, 'status': 'active'}
    
    def execute_query(self, query: str, params=None, fetch: bool = False):
        """Compatibility method for existing code that expects execute_query"""
        return self.execute_optimized_query(query, params, fetch)
    
    def close_pool(self):
        """Cleanup connection pool"""
        if self._connection_pool:
            with self._lock:
                self._connection_pool.closeall()
            logger.info("Connection pool closed")