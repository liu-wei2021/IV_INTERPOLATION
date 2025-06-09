# src/candle_reconstruction/core.py
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)

class CandleReconstructor:
    """
    Reconstruct 5-minute candles from 1-minute OHLCV data
    
    Standard aggregation rules:
    - Open: First 1-min candle's Open
    - High: Maximum High from all 5 candles
    - Low: Minimum Low from all 5 candles  
    - Close: Last 1-min candle's Close
    - Volume: Sum of all 5 candles' Volume
    """
    
    def __init__(self, target_frequency: str = '5min'):
        self.target_frequency = target_frequency
        self.frequency_minutes = self._parse_frequency(target_frequency)
    
    def _parse_frequency(self, freq: str) -> int:
        """Parse frequency string to minutes"""
        if freq.endswith('min'):
            return int(freq[:-3])
        elif freq.endswith('m'):
            return int(freq[:-1])
        else:
            raise ValueError(f"Unsupported frequency: {freq}")
    
    def reconstruct_symbol_candles(self, minute_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Reconstruct candles for a single symbol from 1-minute data
        
        Args:
            minute_data: DataFrame with 1-minute OHLCV data
            Expected columns: symbol, timestamp, open, high, low, close, volume
            
        Returns:
            DataFrame with 5-minute reconstructed candles or None if insufficient data
        """
        if minute_data.empty:
            logger.warning("No minute data provided")
            return None
        
        try:
            # Ensure we have required columns
            required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in minute_data.columns]
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                return None
            
            # Sort by timestamp
            data = minute_data.sort_values('timestamp').copy()
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            
            # Check for minimum data points
            if len(data) < self.frequency_minutes:
                logger.warning(f"Insufficient data: {len(data)} rows, need at least {self.frequency_minutes}")
                return None
            
            # Create time groups for aggregation
            # Round down to nearest target frequency interval
            data['time_group'] = data['timestamp'].dt.floor(f'{self.frequency_minutes}min')
            
            # Group and aggregate using OHLCV rules
            aggregated = data.groupby('time_group').agg({
                'open': 'first',      # First candle's open
                'high': 'max',        # Maximum high
                'low': 'min',         # Minimum low  
                'close': 'last',      # Last candle's close
                'volume': 'sum',      # Sum of volumes
                'symbol': 'first'     # Preserve symbol
            }).reset_index()
            
            # Rename time_group back to timestamp
            aggregated = aggregated.rename(columns={'time_group': 'timestamp'})
            
            # Filter out incomplete groups (less than target frequency minutes)
            # Count how many 1-min candles went into each group
            group_counts = data.groupby('time_group').size()
            complete_groups = group_counts[group_counts >= self.frequency_minutes].index
            aggregated = aggregated[aggregated['timestamp'].isin(complete_groups)]
            
            # Add metadata
            aggregated['frequency'] = self.target_frequency
            aggregated['source_candles'] = self.frequency_minutes
            aggregated['created_at'] = datetime.now()
            
            # Reorder columns
            column_order = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 
                          'frequency', 'source_candles', 'created_at']
            aggregated = aggregated[column_order]
            
            logger.debug(f"Reconstructed {len(data)} 1-min candles → {len(aggregated)} {self.target_frequency} candles")
            return aggregated
            
        except Exception as e:
            logger.error(f"Candle reconstruction failed: {e}")
            return None
    
    def validate_candle_data(self, df: pd.DataFrame) -> bool:
        """Validate OHLCV candle data integrity"""
        if df.empty:
            return False
        
        try:
            # Check OHLC relationships
            invalid_ohlc = (
                (df['high'] < df['low']) |  # High must be >= Low
                (df['high'] < df['open']) | (df['high'] < df['close']) |  # High must be >= Open, Close
                (df['low'] > df['open']) | (df['low'] > df['close'])      # Low must be <= Open, Close
            )
            
            if invalid_ohlc.any():
                invalid_count = invalid_ohlc.sum()
                logger.warning(f"Found {invalid_count} candles with invalid OHLC relationships")
                return False
            
            # Check for negative volumes
            negative_volumes = df['volume'] < 0
            if negative_volumes.any():
                logger.warning(f"Found {negative_volumes.sum()} candles with negative volume")
                return False
            
            # Check for missing values in critical columns
            critical_cols = ['open', 'high', 'low', 'close']
            for col in critical_cols:
                if df[col].isnull().any():
                    logger.warning(f"Found null values in {col}")
                    return False
            
            logger.debug("Candle data validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return False
    
    def get_reconstruction_stats(self, original_data: pd.DataFrame, reconstructed_data: pd.DataFrame) -> dict:
        """Get statistics about the reconstruction process"""
        if original_data.empty or reconstructed_data.empty:
            return {}
        
        try:
            original_timespan = original_data['timestamp'].max() - original_data['timestamp'].min()
            reconstructed_timespan = reconstructed_data['timestamp'].max() - reconstructed_data['timestamp'].min()
            
            stats = {
                'original_candles': len(original_data),
                'reconstructed_candles': len(reconstructed_data),
                'compression_ratio': len(original_data) / len(reconstructed_data) if len(reconstructed_data) > 0 else 0,
                'original_timespan': original_timespan,
                'reconstructed_timespan': reconstructed_timespan,
                'coverage_ratio': reconstructed_timespan / original_timespan if original_timespan.total_seconds() > 0 else 0,
                'total_volume_original': original_data['volume'].sum(),
                'total_volume_reconstructed': reconstructed_data['volume'].sum(),
                'volume_preservation': abs(1 - reconstructed_data['volume'].sum() / original_data['volume'].sum()) if original_data['volume'].sum() > 0 else 1
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to calculate stats: {e}")
            return {}


class MultiSymbolCandleReconstructor:
    """Handle candle reconstruction for multiple symbols"""
    
    def __init__(self, db_manager, config):
        self.db_manager = db_manager
        self.config = config
        self.reconstructor = CandleReconstructor(config.candle_reconstruction.target_frequency)
    
    def get_symbols_with_minute_data(self, start_date: str = None, end_date: str = None) -> List[str]:
        """Get list of symbols that have 1-minute candle data"""
        date_filter = ""
        params = []
        
        if start_date and end_date:
            date_filter = "WHERE timestamp >= %s AND timestamp <= %s"
            params = [start_date, end_date]
        
        query = f"""
        SELECT DISTINCT symbol 
        FROM minute_candles 
        {date_filter}
        ORDER BY symbol
        """
        
        try:
            results = self.db_manager.execute_query(query, params, fetch=True)
            symbols = [row[0] for row in results]
            logger.info(f"Found {len(symbols)} symbols with minute candle data")
            return symbols
        except Exception as e:
            logger.error(f"Failed to retrieve symbols: {e}")
            return []
    
    def process_symbol(self, symbol: str) -> dict:
        """Process candle reconstruction for a single symbol"""
        start_time = time.time()
        
        try:
            logger.debug(f"Processing candle reconstruction for: {symbol}")
            
            # Fetch 1-minute data
            query = """
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM minute_candles 
            WHERE symbol = %s 
            ORDER BY timestamp
            """
            
            with self.db_manager.get_connection() as conn:
                minute_data = pd.read_sql(query, conn, params=(symbol,))
            
            if minute_data.empty:
                return {'symbol': symbol, 'status': 'skipped', 'reason': 'No minute data found'}
            
            # Validate input data
            if not self.reconstructor.validate_candle_data(minute_data):
                return {'symbol': symbol, 'status': 'error', 'error': 'Invalid input candle data'}
            
            # Reconstruct candles
            reconstructed = self.reconstructor.reconstruct_symbol_candles(minute_data)
            
            if reconstructed is None:
                return {'symbol': symbol, 'status': 'skipped', 'reason': 'Reconstruction failed'}
            
            # Validate output data  
            if not self.reconstructor.validate_candle_data(reconstructed):
                return {'symbol': symbol, 'status': 'error', 'error': 'Invalid reconstructed candle data'}
            
            # Save results
            save_success = self._save_reconstructed_candles(reconstructed)
            
            processing_time = time.time() - start_time
            
            if save_success:
                stats = self.reconstructor.get_reconstruction_stats(minute_data, reconstructed)
                
                return {
                    'symbol': symbol,
                    'status': 'success',
                    'input_candles': len(minute_data),
                    'output_candles': len(reconstructed),
                    'processing_time': processing_time,
                    'stats': stats
                }
            else:
                return {'symbol': symbol, 'status': 'error', 'error': 'Failed to save reconstructed candles'}
                
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = str(e)
            logger.error(f"Error processing {symbol}: {error_msg}")
            return {
                'symbol': symbol,
                'status': 'error', 
                'error': error_msg,
                'processing_time': processing_time
            }
    
    def _save_reconstructed_candles(self, candle_data: pd.DataFrame) -> bool:
        """Save reconstructed candles to database with conflict resolution"""
        try:
            if candle_data.empty:
                return True
            
            # Prepare data for insertion
            columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 
                      'frequency', 'source_candles', 'created_at']
            
            # Convert to list of tuples
            data_tuples = []
            for _, row in candle_data.iterrows():
                data_tuple = tuple(
                    None if pd.isna(row[col]) else row[col] 
                    for col in columns
                )
                data_tuples.append(data_tuple)
            
            # Batch insert with conflict resolution (UPSERT)
            insert_query = f"""
            INSERT INTO reconstructed_candles 
            ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (symbol, timestamp, frequency) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                source_candles = EXCLUDED.source_candles,
                created_at = EXCLUDED.created_at
            """
            
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cur:
                    import psycopg2.extras
                    psycopg2.extras.execute_values(
                        cur, insert_query, data_tuples,
                        template=None, page_size=1000
                    )
                conn.commit()
            
            logger.debug(f"Saved {len(data_tuples)} reconstructed candles (with conflict resolution)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save reconstructed candles: {e}")
            return False