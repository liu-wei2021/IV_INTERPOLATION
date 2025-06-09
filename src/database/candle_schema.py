# src/database/candle_schema.py
from database.connection import DatabaseManager
import logging

logger = logging.getLogger(__name__)

class CandleSchemaManager:
    """Manages database schema for candle reconstruction"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def create_candle_tables(self) -> bool:
        """Create tables for candle reconstruction"""
        
        # Table for storing reconstructed candles (5-minute from 1-minute)
        create_reconstructed_table = """
        -- Drop existing table for clean start
        DROP TABLE IF EXISTS reconstructed_candles CASCADE;
        
        -- Create reconstructed candles table
        CREATE TABLE reconstructed_candles (
            id BIGSERIAL PRIMARY KEY,
            symbol VARCHAR(100) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            
            -- OHLCV data
            open DECIMAL(15, 4) NOT NULL,
            high DECIMAL(15, 4) NOT NULL,
            low DECIMAL(15, 4) NOT NULL,
            close DECIMAL(15, 4) NOT NULL,
            volume DECIMAL(20, 8) NOT NULL DEFAULT 0,
            
            -- Metadata
            frequency VARCHAR(10) NOT NULL DEFAULT '5min',
            source_candles INTEGER NOT NULL DEFAULT 5,
            batch_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Ensure OHLC integrity
            CONSTRAINT valid_ohlc CHECK (
                high >= low AND 
                high >= open AND 
                high >= close AND 
                low <= open AND 
                low <= close
            ),
            CONSTRAINT positive_volume CHECK (volume >= 0)
        );
        """
        
        # Indexes for performance
        create_candle_indexes = """
        -- Performance indexes for reconstructed candles
        CREATE INDEX IF NOT EXISTS idx_recon_symbol_timestamp 
        ON reconstructed_candles(symbol, timestamp);
        
        CREATE INDEX IF NOT EXISTS idx_recon_timestamp 
        ON reconstructed_candles(timestamp);
        
        CREATE INDEX IF NOT EXISTS idx_recon_symbol 
        ON reconstructed_candles(symbol);
        
        CREATE INDEX IF NOT EXISTS idx_recon_batch 
        ON reconstructed_candles(batch_id);
        
        -- Unique constraint to prevent duplicates
        CREATE UNIQUE INDEX IF NOT EXISTS idx_recon_unique 
        ON reconstructed_candles(symbol, timestamp, frequency);
        """
        
        try:
            logger.info("Creating reconstructed candles table...")
            self.db_manager.execute_query(create_reconstructed_table)
            
            logger.info("Creating candle indexes...")
            self.db_manager.execute_query(create_candle_indexes)
            
            logger.info("SUCCESS: Candle reconstruction schema created")
            return True
            
        except Exception as e:
            logger.error(f"FAILED: Candle schema creation failed: {e}")
            return False
    
    def create_candle_progress_table(self) -> bool:
        """Create progress tracking for candle reconstruction"""
        
        progress_table_sql = """
        DROP TABLE IF EXISTS candle_reconstruction_progress CASCADE;
        
        CREATE TABLE candle_reconstruction_progress (
            symbol VARCHAR(100) PRIMARY KEY,
            status VARCHAR(20) DEFAULT 'pending',
            input_candles INTEGER DEFAULT 0,
            output_candles INTEGER DEFAULT 0,
            compression_ratio DECIMAL(8, 2) DEFAULT 0,
            processing_time_seconds DECIMAL(10, 2) DEFAULT 0,
            error_message TEXT,
            batch_id INTEGER,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_candle_progress_status 
        ON candle_reconstruction_progress(status);
        
        CREATE INDEX IF NOT EXISTS idx_candle_progress_batch 
        ON candle_reconstruction_progress(batch_id);
        """
        
        try:
            logger.info("Creating candle reconstruction progress table...")
            self.db_manager.execute_query(progress_table_sql)
            logger.info("SUCCESS: Candle progress table created")
            return True
            
        except Exception as e:
            logger.error(f"FAILED: Candle progress table creation failed: {e}")
            return False
    
    def create_sample_minute_candles_table(self) -> bool:
        """Create a sample table structure for 1-minute candles (if needed)"""
        
        sample_table_sql = """
        -- Create sample minute_candles table structure (modify as needed)
        -- This assumes you have 1-minute candle data in this format
        CREATE TABLE IF NOT EXISTS minute_candles (
            id BIGSERIAL PRIMARY KEY,
            symbol VARCHAR(100) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open DECIMAL(15, 4) NOT NULL,
            high DECIMAL(15, 4) NOT NULL,
            low DECIMAL(15, 4) NOT NULL,
            close DECIMAL(15, 4) NOT NULL,
            volume DECIMAL(20, 8) NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- OHLC integrity constraints
            CONSTRAINT minute_valid_ohlc CHECK (
                high >= low AND 
                high >= open AND 
                high >= close AND 
                low <= open AND 
                low <= close
            ),
            CONSTRAINT minute_positive_volume CHECK (volume >= 0)
        );
        
        -- Indexes for minute candles
        CREATE INDEX IF NOT EXISTS idx_minute_symbol_timestamp 
        ON minute_candles(symbol, timestamp);
        
        CREATE INDEX IF NOT EXISTS idx_minute_timestamp 
        ON minute_candles(timestamp);
        
        -- Unique constraint
        CREATE UNIQUE INDEX IF NOT EXISTS idx_minute_unique 
        ON minute_candles(symbol, timestamp);
        """
        
        try:
            logger.info("Creating sample minute_candles table structure...")
            self.db_manager.execute_query(sample_table_sql)
            logger.info("SUCCESS: Sample minute candles table created")
            return True
            
        except Exception as e:
            logger.error(f"FAILED: Sample table creation failed: {e}")
            return False