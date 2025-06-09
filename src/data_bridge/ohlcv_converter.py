# src/data_bridge/ohlcv_converter.py
"""
Production Data Bridge: Convert Task 1 interpolated data to Task 2 OHLCV format
Integrates seamlessly into the main pipeline workflow.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import logging
import time

logger = logging.getLogger(__name__)

class InterpolatedToOHLCVConverter:
    """
    Production-grade converter from Task 1 interpolated data to Task 2 OHLCV candles
    
    This bridges the gap between IV interpolation output and candle reconstruction input,
    enabling a seamless Task 1 → Task 2 pipeline.
    """
    
    def __init__(self, db_manager, config):
        self.db_manager = db_manager
        self.config = config
        self.conversion_strategy = config.data_bridge.conversion_strategy
        self.spread_method = config.data_bridge.spread_method
        self.quality_checks = config.data_bridge.enable_quality_checks
    
    def get_interpolated_symbols(self, batch_id: Optional[int] = None) -> List[str]:
        """Get symbols that have interpolated data ready for conversion"""
        
        batch_filter = ""
        params = []
        
        if batch_id:
            batch_filter = "WHERE batch_id = %s"
            params = [batch_id]
        
        query = f"""
        SELECT DISTINCT symbol 
        FROM interpolated_trading_tickers 
        {batch_filter}
        ORDER BY symbol
        """
        
        try:
            results = self.db_manager.execute_query(query, params, fetch=True)
            symbols = [row[0] for row in results]
            logger.info(f"Found {len(symbols)} symbols with interpolated data")
            return symbols
        except Exception as e:
            logger.error(f"Failed to retrieve interpolated symbols: {e}")
            return []
    
    def convert_symbol_to_ohlcv(self, symbol: str, batch_id: Optional[int] = None) -> Dict:
        """Convert one symbol's interpolated data to OHLCV format"""
        
        start_time = time.time()
        
        try:
            logger.debug(f"Converting {symbol} to OHLCV format")
            
            # Fetch interpolated data
            batch_filter = ""
            params = [symbol]
            
            if batch_id:
                batch_filter = "AND batch_id = %s"
                params.append(batch_id)
            
            query = f"""
            SELECT 
                symbol,
                date as timestamp,
                underlying_price,
                mark_price,
                index_price,
                volume,
                is_interpolated,
                batch_id
            FROM interpolated_trading_tickers 
            WHERE symbol = %s {batch_filter}
            ORDER BY date
            """
            
            with self.db_manager.get_connection() as conn:
                df = pd.read_sql(query, conn, params=params)
            
            if df.empty:
                return {'symbol': symbol, 'status': 'skipped', 'reason': 'No interpolated data found'}
            
            # Convert to OHLCV
            ohlcv_data = self._generate_ohlcv_from_interpolated(df)
            
            if ohlcv_data is None or ohlcv_data.empty:
                return {'symbol': symbol, 'status': 'error', 'error': 'OHLCV conversion failed'}
            
            # Quality validation
            if self.quality_checks:
                validation_result = self._validate_ohlcv_quality(ohlcv_data)
                if not validation_result['valid']:
                    return {
                        'symbol': symbol, 
                        'status': 'error', 
                        'error': f"Quality check failed: {validation_result['reason']}"
                    }
            
            # Save to minute_candles table
            save_success = self._save_ohlcv_candles(ohlcv_data)
            
            processing_time = time.time() - start_time
            
            if save_success:
                return {
                    'symbol': symbol,
                    'status': 'success',
                    'input_points': len(df),
                    'output_candles': len(ohlcv_data),
                    'processing_time': processing_time,
                    'conversion_method': self.conversion_strategy
                }
            else:
                return {'symbol': symbol, 'status': 'error', 'error': 'Failed to save OHLCV data'}
                
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = str(e)
            logger.error(f"Error converting {symbol}: {error_msg}")
            return {
                'symbol': symbol,
                'status': 'error',
                'error': error_msg,
                'processing_time': processing_time
            }
    
    def _generate_ohlcv_from_interpolated(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Generate realistic OHLCV candles from interpolated price data"""
        
        if df.empty:
            return None
        
        try:
            # Choose primary price source
            price_column = self._select_price_column(df)
            if price_column not in df.columns:
                logger.error(f"Price column {price_column} not found")
                return None
            
            ohlcv_candles = []
            
            for i, row in df.iterrows():
                timestamp = pd.to_datetime(row['timestamp'])
                base_price = row[price_column]
                
                if pd.isna(base_price) or base_price <= 0:
                    continue
                
                # Generate realistic OHLCV using selected strategy
                if self.conversion_strategy == 'spread_simulation':
                    candle = self._create_candle_with_spread(row, base_price, i, ohlcv_candles)
                elif self.conversion_strategy == 'price_as_midpoint':
                    candle = self._create_candle_price_midpoint(row, base_price)
                elif self.conversion_strategy == 'trend_following':
                    candle = self._create_candle_trend_following(row, base_price, i, ohlcv_candles)
                else:
                    # Default: simple spread method
                    candle = self._create_candle_simple_spread(row, base_price)
                
                if candle:
                    ohlcv_candles.append(candle)
            
            if not ohlcv_candles:
                return None
            
            ohlcv_df = pd.DataFrame(ohlcv_candles)
            
            # Final validation and cleanup
            ohlcv_df = self._cleanup_ohlcv_data(ohlcv_df)
            
            logger.debug(f"Generated {len(ohlcv_df)} OHLCV candles from {len(df)} interpolated points")
            return ohlcv_df
            
        except Exception as e:
            logger.error(f"OHLCV generation failed: {e}")
            return None
    
    def _select_price_column(self, df: pd.DataFrame) -> str:
        """Select the best price column based on data availability"""
        
        price_priority = ['underlying_price', 'mark_price', 'index_price']
        
        for col in price_priority:
            if col in df.columns:
                non_null_count = df[col].notna().sum()
                if non_null_count > len(df) * 0.8:  # At least 80% data
                    logger.debug(f"Using {col} as primary price source ({non_null_count}/{len(df)} valid)")
                    return col
        
        # Fallback to first available
        for col in price_priority:
            if col in df.columns:
                logger.warning(f"Using {col} as fallback price source (limited data)")
                return col
        
        raise ValueError("No suitable price column found")
    
    def _create_candle_with_spread(self, row, base_price: float, index: int, previous_candles: List) -> Dict:
        """Create OHLCV candle using spread simulation method"""
        
        # Calculate realistic spread based on price level and market conditions
        spread_config = self.config.data_bridge.spread_parameters
        
        # Base spread as percentage of price
        base_spread_pct = spread_config.get('base_spread_percent', 0.002)  # 0.2%
        volatility_factor = spread_config.get('volatility_factor', 1.5)
        
        # Add randomness for realism
        volatility_multiplier = np.random.uniform(0.5, volatility_factor)
        spread = base_price * base_spread_pct * volatility_multiplier
        
        # Determine trend from previous candle
        trend_bias = 0
        if previous_candles and len(previous_candles) > 0:
            prev_close = previous_candles[-1]['close']
            trend_bias = (base_price - prev_close) * 0.3  # 30% of trend continuation
        
        # Generate OHLC with realistic relationships
        open_offset = np.random.uniform(-spread/3, spread/3) + trend_bias * 0.2
        open_price = base_price + open_offset
        
        close_offset = np.random.uniform(-spread/3, spread/3) + trend_bias * 0.5
        close_price = base_price + close_offset
        
        # High and Low with proper relationships
        mid_oc = (open_price + close_price) / 2
        high_extra = np.random.uniform(0, spread/2)
        low_reduction = np.random.uniform(0, spread/2)
        
        high_price = max(open_price, close_price) + high_extra
        low_price = min(open_price, close_price) - low_reduction
        
        # Ensure minimum spread
        if high_price - low_price < base_price * 0.0005:  # Minimum 0.05% spread
            high_price = mid_oc + base_price * 0.00025
            low_price = mid_oc - base_price * 0.00025
        
        # Volume handling
        volume = self._process_volume(row)
        
        return {
            'symbol': row['symbol'],
            'timestamp': pd.to_datetime(row['timestamp']),
            'open': round(open_price, 4),
            'high': round(high_price, 4),
            'low': round(low_price, 4),
            'close': round(close_price, 4),
            'volume': round(volume, 6),
            'source_price': base_price,
            'conversion_method': 'spread_simulation',
            'is_synthetic': True
        }
    
    def _create_candle_price_midpoint(self, row, base_price: float) -> Dict:
        """Create candle treating interpolated price as midpoint"""
        
        spread_pct = 0.001  # 0.1% spread
        spread = base_price * spread_pct
        
        # Simple symmetric spread
        open_price = base_price + np.random.uniform(-spread/4, spread/4)
        close_price = base_price + np.random.uniform(-spread/4, spread/4)
        high_price = base_price + spread/2
        low_price = base_price - spread/2
        
        volume = self._process_volume(row)
        
        return {
            'symbol': row['symbol'],
            'timestamp': pd.to_datetime(row['timestamp']),
            'open': round(open_price, 4),
            'high': round(high_price, 4),
            'low': round(low_price, 4),
            'close': round(close_price, 4),
            'volume': round(volume, 6),
            'source_price': base_price,
            'conversion_method': 'price_midpoint',
            'is_synthetic': True
        }
    
    def _create_candle_trend_following(self, row, base_price: float, index: int, previous_candles: List) -> Dict:
        """Create candle with trend-following behavior"""
        
        # Calculate trend from recent candles
        lookback = min(5, len(previous_candles))
        trend = 0
        
        if lookback > 0:
            recent_closes = [c['close'] for c in previous_candles[-lookback:]]
            if len(recent_closes) > 1:
                trend = (recent_closes[-1] - recent_closes[0]) / len(recent_closes)
        
        # Apply trend with some noise
        trend_strength = 0.6
        noise_factor = np.random.normal(0, base_price * 0.001)
        
        open_price = base_price + trend * trend_strength + noise_factor
        close_price = base_price + trend * trend_strength * 1.2 + noise_factor
        
        # High/Low with trend consideration
        if trend > 0:  # Uptrend
            high_price = max(open_price, close_price) + abs(trend) * 0.5
            low_price = min(open_price, close_price) - abs(trend) * 0.2
        else:  # Downtrend
            high_price = max(open_price, close_price) + abs(trend) * 0.2
            low_price = min(open_price, close_price) - abs(trend) * 0.5
        
        volume = self._process_volume(row)
        
        return {
            'symbol': row['symbol'],
            'timestamp': pd.to_datetime(row['timestamp']),
            'open': round(open_price, 4),
            'high': round(high_price, 4),
            'low': round(low_price, 4),
            'close': round(close_price, 4),
            'volume': round(volume, 6),
            'source_price': base_price,
            'conversion_method': 'trend_following',
            'is_synthetic': True
        }
    
    def _create_candle_simple_spread(self, row, base_price: float) -> Dict:
        """Create simple candle with basic spread (fallback method)"""
        
        spread = base_price * 0.001  # 0.1% spread
        
        open_price = base_price
        close_price = base_price + np.random.uniform(-spread/2, spread/2)
        high_price = base_price + spread/2
        low_price = base_price - spread/2
        
        volume = self._process_volume(row)
        
        return {
            'symbol': row['symbol'],
            'timestamp': pd.to_datetime(row['timestamp']),
            'open': round(open_price, 4),
            'high': round(high_price, 4),
            'low': round(low_price, 4),
            'close': round(close_price, 4),
            'volume': round(volume, 6),
            'source_price': base_price,
            'conversion_method': 'simple_spread',
            'is_synthetic': True
        }
    
    def _process_volume(self, row) -> float:
        """Process volume data from interpolated row"""
        
        volume = row.get('volume', 0)
        
        if pd.isna(volume) or volume <= 0:
            # Generate realistic volume based on market conditions
            base_volume = 50  # Base volume
            volume = np.random.exponential(base_volume)
        
        return max(0, volume)
    
    def _validate_ohlcv_quality(self, ohlcv_df: pd.DataFrame) -> Dict:
        """Validate OHLCV data quality"""
        
        try:
            # Check OHLC relationships
            invalid_high_low = (ohlcv_df['high'] < ohlcv_df['low']).any()
            if invalid_high_low:
                return {'valid': False, 'reason': 'High < Low found'}
            
            invalid_high_oc = ((ohlcv_df['high'] < ohlcv_df['open']) | 
                              (ohlcv_df['high'] < ohlcv_df['close'])).any()
            if invalid_high_oc:
                return {'valid': False, 'reason': 'High < Open/Close found'}
            
            invalid_low_oc = ((ohlcv_df['low'] > ohlcv_df['open']) | 
                             (ohlcv_df['low'] > ohlcv_df['close'])).any()
            if invalid_low_oc:
                return {'valid': False, 'reason': 'Low > Open/Close found'}
            
            # Check for reasonable spreads
            spreads = (ohlcv_df['high'] - ohlcv_df['low']) / ohlcv_df['source_price']
            if (spreads > 0.1).any():  # 10% spread seems unrealistic
                return {'valid': False, 'reason': 'Unrealistic spreads detected'}
            
            # Check for negative prices
            if (ohlcv_df[['open', 'high', 'low', 'close']] <= 0).any().any():
                return {'valid': False, 'reason': 'Negative or zero prices found'}
            
            return {'valid': True, 'reason': 'All quality checks passed'}
            
        except Exception as e:
            return {'valid': False, 'reason': f'Validation error: {str(e)}'}
    
    def _cleanup_ohlcv_data(self, ohlcv_df: pd.DataFrame) -> pd.DataFrame:
        """Clean up and finalize OHLCV data"""
        
        # Remove any invalid rows
        ohlcv_df = ohlcv_df.dropna(subset=['open', 'high', 'low', 'close'])
        
        # Ensure proper data types
        price_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in price_columns:
            ohlcv_df[col] = pd.to_numeric(ohlcv_df[col], errors='coerce')
        
        # Sort by timestamp
        ohlcv_df = ohlcv_df.sort_values('timestamp')
        
        return ohlcv_df
    
    def _save_ohlcv_candles(self, ohlcv_df: pd.DataFrame) -> bool:
        """Save OHLCV candles to minute_candles table"""
        
        try:
            if ohlcv_df.empty:
                return True
            
            # Prepare for database insertion
            insert_query = """
            INSERT INTO minute_candles (symbol, timestamp, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
            """
            
            # Convert to tuples
            data_tuples = [
                (row['symbol'], row['timestamp'], row['open'], 
                 row['high'], row['low'], row['close'], row['volume'])
                for _, row in ohlcv_df.iterrows()
            ]
            
            # Batch insert
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cur:
                    import psycopg2.extras
                    psycopg2.extras.execute_values(
                        cur, insert_query, data_tuples,
                        template=None, page_size=1000
                    )
                conn.commit()
            
            logger.debug(f"Saved {len(data_tuples)} OHLCV candles to database")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save OHLCV candles: {e}")
            return False
    
    def convert_batch(self, symbols: List[str] = None, batch_id: Optional[int] = None) -> Dict:
        """Convert a batch of symbols from interpolated data to OHLCV"""
        
        if symbols is None:
            symbols = self.get_interpolated_symbols(batch_id)
        
        if not symbols:
            logger.warning("No symbols found for OHLCV conversion")
            return {'total': 0, 'success': 0, 'errors': 0, 'skipped': 0}
        
        logger.info(f"Converting {len(symbols)} symbols to OHLCV format")
        
        results = {'total': len(symbols), 'success': 0, 'errors': 0, 'skipped': 0}
        
        for symbol in symbols:
            result = self.convert_symbol_to_ohlcv(symbol, batch_id)
            
            if result['status'] == 'success':
                results['success'] += 1
                logger.info(f"✅ {symbol}: {result['input_points']} → {result['output_candles']} candles")
            elif result['status'] == 'skipped':
                results['skipped'] += 1
                logger.warning(f"⏭️  {symbol}: {result['reason']}")
            else:  # error
                results['errors'] += 1
                logger.error(f"❌ {symbol}: {result['error']}")
        
        logger.info(f"OHLCV conversion complete: {results['success']} success, {results['errors']} errors, {results['skipped']} skipped")
        return results