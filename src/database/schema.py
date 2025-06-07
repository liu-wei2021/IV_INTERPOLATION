from database.connection import DatabaseManager
import logging

logger = logging.getLogger(__name__)

class SchemaManager:
    """Manages database schema for interpolated data"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def create_interpolated_table(self) -> bool:
        """Create the interpolated trading tickers table"""
        
        create_table_sql = """
        -- Drop existing table for clean start
        DROP TABLE IF EXISTS interpolated_trading_tickers CASCADE;
        
        -- Create main interpolated data table
        CREATE TABLE interpolated_trading_tickers (
            id BIGSERIAL PRIMARY KEY,
            symbol VARCHAR(100) NOT NULL,
            date TIMESTAMP NOT NULL,
            record_time VARCHAR(50),
            
            -- Core option data
            iv DECIMAL(12, 8) NOT NULL,
            underlying_price DECIMAL(15, 4) NOT NULL,
            time_to_maturity DECIMAL(12, 8) NOT NULL,
            strike DECIMAL(15, 4),
            callput CHAR(1),
            interest_rate DECIMAL(8, 6) DEFAULT 0.0,
            
            -- Calculated Greeks
            delta DECIMAL(12, 8),
            gamma DECIMAL(12, 8),
            theta DECIMAL(12, 8),
            vega DECIMAL(12, 8),
            rho DECIMAL(12, 8),
            
            -- Price data
            mark_price DECIMAL(15, 4),
            index_price DECIMAL(15, 4),
            volume DECIMAL(20, 8),
            quote_volume DECIMAL(20, 8),
            
            -- Metadata
            is_interpolated BOOLEAN NOT NULL DEFAULT FALSE,
            batch_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        create_indexes_sql = """
        -- Performance indexes
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_interp_symbol_date 
        ON interpolated_trading_tickers(symbol, date);
        
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_interp_date 
        ON interpolated_trading_tickers(date);
        
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_interp_symbol 
        ON interpolated_trading_tickers(symbol);
        
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_interp_batch 
        ON interpolated_trading_tickers(batch_id);
        """
        
        try:
            logger.info("Creating interpolated data table...")
            self.db_manager.execute_query(create_table_sql)
            
            logger.info("Creating indexes...")
            self.db_manager.execute_query(create_indexes_sql)
            
            logger.info("✅ Database schema created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Schema creation failed: {e}")
            return False
    
    def create_progress_table(self) -> bool:
        """Create progress tracking table"""
        
        progress_table_sql = """
        DROP TABLE IF EXISTS interpolation_progress CASCADE;
        
        CREATE TABLE interpolation_progress (
            symbol VARCHAR(100) PRIMARY KEY,
            status VARCHAR(20) DEFAULT 'pending',
            input_rows INTEGER DEFAULT 0,
            output_rows INTEGER DEFAULT 0,
            processing_time_seconds DECIMAL(10, 2) DEFAULT 0,
            error_message TEXT,
            batch_id INTEGER,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_progress_status 
        ON interpolation_progress(status);
        
        CREATE INDEX IF NOT EXISTS idx_progress_batch 
        ON interpolation_progress(batch_id);
        """
        
        try:
            logger.info("Creating progress tracking table...")
            self.db_manager.execute_query(progress_table_sql)
            logger.info("✅ Progress table created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Progress table creation failed: {e}")
            return False
