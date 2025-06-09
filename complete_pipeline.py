#!/usr/bin/env python3
"""
Complete Optimized Pipeline - All Tasks (No Admin Access Required)

Optimized for 32M+ rows across the entire workflow:
Task 1: IV Interpolation (hourly → minute-level) 
Data Bridge: Convert IV data to OHLCV format
Task 2: Candle Reconstruction (1-min → 5-min OHLCV)

Usage:
  python complete_pipeline.py --test              # Test all tasks with 3 symbols
  python complete_pipeline.py --task interpolation # Task 1 only
  python complete_pipeline.py --task bridge       # Data bridge only  
  python complete_pipeline.py --task candles      # Task 2 only
  python complete_pipeline.py --task all          # Complete pipeline
  python complete_pipeline.py --estimate          # Show estimates for all tasks
"""

import argparse
import sys
import os
import time
import signal
from pathlib import Path
from typing import Optional, List, Dict
import pandas as pd
import numpy as np

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from config import get_config
from database.user_optimized_connection import UserLevelDatabaseManager
from interpolation.core import IVInterpolator

class CompleteOptimizedPipeline:
    """
    Complete pipeline processor optimized for shared servers
    
    Handles all three stages:
    1. IV Interpolation (Task 1)
    2. Data Bridge (IV → OHLCV conversion)
    3. Candle Reconstruction (Task 2)
    """
    
    def __init__(self, config):
        self.config = config
        self.db_manager = UserLevelDatabaseManager(config.database)
        self.interpolator = IVInterpolator()
        self.interrupted = False
        
        # Setup interrupt handler
        signal.signal(signal.SIGINT, self._interrupt_handler)
    
    def _interrupt_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        print("\n⏸️  Interruption received. Finishing current operation...")
        self.interrupted = True
    
    def setup_database_tables(self) -> dict:
        """Setup all required database tables"""
        print("🏗️  Setting up database tables...")
        
        results = {'tables_created': [], 'tables_existed': [], 'errors': []}
        
        # Table definitions
        tables = {
            'interpolated_trading_tickers': """
                CREATE TABLE IF NOT EXISTS interpolated_trading_tickers (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(100) NOT NULL,
                    date TIMESTAMP NOT NULL,
                    record_time VARCHAR(50),
                    iv DECIMAL(12, 8) NOT NULL,
                    underlying_price DECIMAL(15, 4) NOT NULL,
                    time_to_maturity DECIMAL(12, 8) NOT NULL,
                    strike DECIMAL(15, 4),
                    callput CHAR(1),
                    interest_rate DECIMAL(8, 6) DEFAULT 0.0,
                    mark_price DECIMAL(15, 4),
                    index_price DECIMAL(15, 4),
                    volume DECIMAL(20, 8),
                    quote_volume DECIMAL(20, 8),
                    is_interpolated BOOLEAN NOT NULL DEFAULT FALSE,
                    batch_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'minute_candles': """
                CREATE TABLE IF NOT EXISTS minute_candles (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(100) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    open DECIMAL(15, 4) NOT NULL,
                    high DECIMAL(15, 4) NOT NULL,
                    low DECIMAL(15, 4) NOT NULL,
                    close DECIMAL(15, 4) NOT NULL,
                    volume DECIMAL(20, 8) NOT NULL DEFAULT 0,
                    source_price DECIMAL(15, 4),
                    is_synthetic BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT minute_valid_ohlc CHECK (
                        high >= low AND high >= open AND high >= close AND 
                        low <= open AND low <= close
                    )
                )
            """,
            'reconstructed_candles': """
                CREATE TABLE IF NOT EXISTS reconstructed_candles (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(100) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    open DECIMAL(15, 4) NOT NULL,
                    high DECIMAL(15, 4) NOT NULL,
                    low DECIMAL(15, 4) NOT NULL,
                    close DECIMAL(15, 4) NOT NULL,
                    volume DECIMAL(20, 8) NOT NULL DEFAULT 0,
                    frequency VARCHAR(10) NOT NULL DEFAULT '5min',
                    source_candles INTEGER NOT NULL DEFAULT 5,
                    batch_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT recon_valid_ohlc CHECK (
                        high >= low AND high >= open AND high >= close AND 
                        low <= open AND low <= close
                    )
                )
            """
        }
        
        try:
            for table_name, create_sql in tables.items():
                try:
                    # Check if table exists
                    check_query = f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{table_name}'
                    )
                    """
                    exists = self.db_manager.execute_optimized_query(check_query, fetch=True)[0][0]
                    
                    if exists:
                        results['tables_existed'].append(table_name)
                        print(f"  ✅ {table_name}: Already exists")
                    else:
                        self.db_manager.execute_optimized_query(create_sql)
                        results['tables_created'].append(table_name)
                        print(f"  🔨 {table_name}: Created successfully")
                        
                except Exception as e:
                    results['errors'].append(f"{table_name}: {str(e)}")
                    print(f"  ❌ {table_name}: {e}")
            
            # Create indexes if possible
            self._create_performance_indexes(results)
            
        except Exception as e:
            results['errors'].append(f"Setup failed: {str(e)}")
        
        return results
    
    def _create_performance_indexes(self, results: dict):
        """Create performance indexes if user has permission"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_interpolated_symbol_date ON interpolated_trading_tickers(symbol, date)",
            "CREATE INDEX IF NOT EXISTS idx_interpolated_batch ON interpolated_trading_tickers(batch_id)",
            "CREATE INDEX IF NOT EXISTS idx_minute_symbol_timestamp ON minute_candles(symbol, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_recon_symbol_timestamp ON reconstructed_candles(symbol, timestamp, frequency)"
        ]
        
        for index_sql in indexes:
            try:
                self.db_manager.execute_optimized_query(index_sql)
                print(f"  📊 Index created: {index_sql.split('idx_')[1].split(' ON')[0]}")
            except Exception as e:
                print(f"  ⚠️  Index creation failed: {e}")
    
    def get_pipeline_status(self) -> dict:
        """Get current status of all pipeline stages"""
        status = {
            'task1_symbols': 0,
            'task1_rows': 0, 
            'bridge_symbols': 0,
            'bridge_rows': 0,
            'task2_symbols': 0,
            'task2_rows': 0,
            'source_symbols': 0,
            'source_rows': 0
        }
        
        try:
            # Source data
            status['source_rows'] = self.db_manager.execute_optimized_query(
                "SELECT COUNT(*) FROM trading_tickers", fetch=True)[0][0]
            status['source_symbols'] = self.db_manager.execute_optimized_query(
                "SELECT COUNT(DISTINCT symbol) FROM trading_tickers", fetch=True)[0][0]
            
            # Task 1 output
            status['task1_rows'] = self.db_manager.execute_optimized_query(
                "SELECT COUNT(*) FROM interpolated_trading_tickers", fetch=True)[0][0]
            status['task1_symbols'] = self.db_manager.execute_optimized_query(
                "SELECT COUNT(DISTINCT symbol) FROM interpolated_trading_tickers", fetch=True)[0][0]
            
            # Bridge output (minute candles)
            status['bridge_rows'] = self.db_manager.execute_optimized_query(
                "SELECT COUNT(*) FROM minute_candles", fetch=True)[0][0]
            status['bridge_symbols'] = self.db_manager.execute_optimized_query(
                "SELECT COUNT(DISTINCT symbol) FROM minute_candles", fetch=True)[0][0]
            
            # Task 2 output
            status['task2_rows'] = self.db_manager.execute_optimized_query(
                "SELECT COUNT(*) FROM reconstructed_candles", fetch=True)[0][0]
            status['task2_symbols'] = self.db_manager.execute_optimized_query(
                "SELECT COUNT(DISTINCT symbol) FROM reconstructed_candles", fetch=True)[0][0]
            
        except Exception as e:
            print(f"⚠️  Status check failed: {e}")
        
        return status
    
    # TASK 1: IV INTERPOLATION
    def run_task1_interpolation(self, symbols: List[str] = None, batch_id: int = None) -> dict:
        """Run Task 1: IV Interpolation (hourly → minute-level)"""
        print("🚀 TASK 1: IV INTERPOLATION")
        print("-" * 40)
        
        if symbols is None:
            symbols = self._get_symbols_for_task1()
        
        if not symbols:
            return {'success': False, 'error': 'No symbols found for Task 1'}
        
        if batch_id is None:
            batch_id = int(time.time())
        
        print(f"Processing {len(symbols)} symbols...")
        start_time = time.time()
        success_count = 0
        error_count = 0
        total_input = 0
        total_output = 0
        
        for i, symbol in enumerate(symbols, 1):
            if self.interrupted:
                break
            
            progress = f"[{i}/{len(symbols)}] {symbol}"
            print(f"{progress}")
            
            result = self._process_symbol_task1(symbol, batch_id)
            
            if result['status'] == 'success':
                success_count += 1
                total_input += result['input_rows']
                total_output += result['output_rows']
                print(f"  ✅ {result['input_rows']} → {result['output_rows']} rows")
            else:
                error_count += 1
                print(f"  ❌ {result.get('error', result.get('reason', 'Unknown error'))}")
            
            # Small delay for shared server courtesy
            time.sleep(0.05)
        
        duration = time.time() - start_time
        
        print(f"\n📊 TASK 1 COMPLETE:")
        print(f"Duration: {duration:.1f}s")
        print(f"Success: {success_count}, Errors: {error_count}")
        print(f"Data: {total_input:,} → {total_output:,} rows")
        
        return {
            'success': success_count > 0,
            'batch_id': batch_id,
            'symbols_processed': success_count,
            'total_input': total_input,
            'total_output': total_output,
            'duration': duration
        }
    
    def _get_symbols_for_task1(self) -> List[str]:
        """Get symbols that need Task 1 processing"""
        query = """
        SELECT DISTINCT t.symbol 
        FROM trading_tickers t
        LEFT JOIN interpolated_trading_tickers i ON t.symbol = i.symbol
        WHERE i.symbol IS NULL
        ORDER BY t.symbol
        """
        try:
            results = self.db_manager.execute_optimized_query(query, fetch=True)
            return [row[0] for row in results]
        except:
            # Fallback: get all symbols
            query = "SELECT DISTINCT symbol FROM trading_tickers ORDER BY symbol"
            results = self.db_manager.execute_optimized_query(query, fetch=True)
            return [row[0] for row in results]
    
    def _process_symbol_task1(self, symbol: str, batch_id: int) -> dict:
        """Process single symbol for Task 1"""
        try:
            # Get symbol data
            query = """
            SELECT symbol, date, iv, underlying_price, time_to_maturity,
                   strike, callput, interest_rate, mark_price, index_price,
                   volume, quote_volume, record_time
            FROM trading_tickers 
            WHERE symbol = %s 
            ORDER BY date
            """
            
            with self.db_manager.get_connection() as conn:
                data = pd.read_sql(query, conn, params=(symbol,))
            
            if data.empty:
                return {'symbol': symbol, 'status': 'skipped', 'reason': 'No data found'}
            
            # Interpolate
            interpolated = self.interpolator.interpolate_symbol(data)
            if interpolated is None:
                return {'symbol': symbol, 'status': 'skipped', 'reason': 'Interpolation failed'}
            
            # Add metadata
            interpolated['batch_id'] = batch_id
            interpolated['is_interpolated'] = interpolated['symbol'].isna()
            
            # Save
            success = self._save_interpolated_data(interpolated)
            
            return {
                'symbol': symbol,
                'status': 'success' if success else 'error',
                'input_rows': len(data),
                'output_rows': len(interpolated),
                'error': None if success else 'Save failed'
            }
            
        except Exception as e:
            return {'symbol': symbol, 'status': 'error', 'error': str(e)}
    
    def _save_interpolated_data(self, data: pd.DataFrame) -> bool:
        """Save interpolated data efficiently"""
        try:
            columns = [
                'symbol', 'date', 'record_time', 'iv', 'underlying_price',
                'time_to_maturity', 'strike', 'callput', 'interest_rate',
                'mark_price', 'index_price', 'volume', 'quote_volume',
                'is_interpolated', 'batch_id'
            ]
            
            data_tuples = [
                tuple(None if pd.isna(row[col]) else row[col] for col in columns)
                for _, row in data.iterrows()
            ]
            
            insert_query = f"""
            INSERT INTO interpolated_trading_tickers ({', '.join(columns)})
            VALUES %s
            """
            
            self.db_manager.execute_batch_insert(insert_query, data_tuples, page_size=1000)
            return True
        except Exception as e:
            print(f"❌ Save failed: {e}")
            return False
    
    # DATA BRIDGE: IV → OHLCV CONVERSION
    def run_data_bridge(self, symbols: List[str] = None, batch_id: int = None) -> dict:
        """Run Data Bridge: Convert interpolated IV data to OHLCV format"""
        print("\n🌉 DATA BRIDGE: IV → OHLCV CONVERSION")
        print("-" * 40)
        
        if symbols is None:
            symbols = self._get_symbols_for_bridge()
        
        if not symbols:
            return {'success': False, 'error': 'No symbols found for data bridge'}
        
        print(f"Converting {len(symbols)} symbols to OHLCV format...")
        start_time = time.time()
        success_count = 0
        error_count = 0
        total_input = 0
        total_output = 0
        
        for i, symbol in enumerate(symbols, 1):
            if self.interrupted:
                break
            
            print(f"[{i}/{len(symbols)}] {symbol}")
            
            result = self._convert_symbol_to_ohlcv(symbol, batch_id)
            
            if result['status'] == 'success':
                success_count += 1
                total_input += result['input_rows']
                total_output += result['output_rows']
                print(f"  ✅ {result['input_rows']} → {result['output_rows']} candles")
            else:
                error_count += 1
                print(f"  ❌ {result.get('error', 'Unknown error')}")
            
            time.sleep(0.03)
        
        duration = time.time() - start_time
        
        print(f"\n📊 DATA BRIDGE COMPLETE:")
        print(f"Duration: {duration:.1f}s")
        print(f"Success: {success_count}, Errors: {error_count}")
        print(f"OHLCV candles: {total_output:,}")
        
        return {
            'success': success_count > 0,
            'symbols_processed': success_count,
            'total_input': total_input,
            'total_output': total_output,
            'duration': duration
        }
    
    def _get_symbols_for_bridge(self) -> List[str]:
        """Get symbols that need bridge conversion"""
        query = """
        SELECT DISTINCT i.symbol 
        FROM interpolated_trading_tickers i
        LEFT JOIN minute_candles m ON i.symbol = m.symbol
        WHERE m.symbol IS NULL
        ORDER BY i.symbol
        """
        try:
            results = self.db_manager.execute_optimized_query(query, fetch=True)
            return [row[0] for row in results]
        except:
            # Fallback
            query = "SELECT DISTINCT symbol FROM interpolated_trading_tickers ORDER BY symbol"
            results = self.db_manager.execute_optimized_query(query, fetch=True)
            return [row[0] for row in results]
    
    def _convert_symbol_to_ohlcv(self, symbol: str, batch_id: int = None) -> dict:
        """Convert single symbol from IV data to OHLCV format"""
        try:
            # Get interpolated data
            query = """
            SELECT symbol, date, underlying_price, mark_price, index_price, volume
            FROM interpolated_trading_tickers 
            WHERE symbol = %s 
            ORDER BY date
            """
            
            with self.db_manager.get_connection() as conn:
                data = pd.read_sql(query, conn, params=(symbol,))
            
            if data.empty:
                return {'symbol': symbol, 'status': 'skipped', 'reason': 'No interpolated data'}
            
            # Convert to OHLCV
            ohlcv_data = self._generate_ohlcv_candles(data)
            if ohlcv_data.empty:
                return {'symbol': symbol, 'status': 'error', 'error': 'OHLCV generation failed'}
            
            # Save to minute_candles
            success = self._save_minute_candles(ohlcv_data)
            
            return {
                'symbol': symbol,
                'status': 'success' if success else 'error',
                'input_rows': len(data),
                'output_rows': len(ohlcv_data),
                'error': None if success else 'Save failed'
            }
            
        except Exception as e:
            return {'symbol': symbol, 'status': 'error', 'error': str(e)}
    
    def _generate_ohlcv_candles(self, data: pd.DataFrame) -> pd.DataFrame:
        """Generate realistic OHLCV candles from IV data"""
        ohlcv_candles = []
        
        for _, row in data.iterrows():
            # Use underlying_price as base, fall back to mark_price or index_price
            base_price = row['underlying_price'] or row['mark_price'] or row['index_price']
            
            if pd.isna(base_price) or base_price <= 0:
                continue
            
            # Generate realistic OHLC with small spreads
            spread_pct = 0.001  # 0.1% spread
            spread = base_price * spread_pct
            
            # Create OHLCV with slight randomness
            noise = np.random.uniform(-spread/3, spread/3)
            open_price = base_price + noise
            close_price = base_price + np.random.uniform(-spread/3, spread/3)
            
            high_price = max(open_price, close_price) + abs(np.random.uniform(0, spread/2))
            low_price = min(open_price, close_price) - abs(np.random.uniform(0, spread/2))
            
            volume = row['volume'] if not pd.isna(row['volume']) and row['volume'] > 0 else np.random.exponential(50)
            
            ohlcv_candles.append({
                'symbol': row['symbol'],
                'timestamp': row['date'],
                'open': round(open_price, 4),
                'high': round(high_price, 4),
                'low': round(low_price, 4),
                'close': round(close_price, 4),
                'volume': round(volume, 6),
                'source_price': base_price,
                'is_synthetic': True
            })
        
        return pd.DataFrame(ohlcv_candles)
    
    def _save_minute_candles(self, ohlcv_data: pd.DataFrame) -> bool:
        """Save OHLCV data to minute_candles table (with duplicate handling)"""
        try:
            # Check which columns exist in the table
            check_columns_query = """
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'minute_candles'
            """
            existing_columns = [row[0] for row in self.db_manager.execute_optimized_query(check_columns_query, fetch=True)]
            
            # Use only columns that exist in the table
            base_columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
            optional_columns = ['source_price', 'is_synthetic']
            
            # Build column list based on what exists
            columns = base_columns.copy()
            for col in optional_columns:
                if col in existing_columns:
                    columns.append(col)
            
            # Remove duplicates by keeping the last occurrence for each (symbol, timestamp)
            ohlcv_data_clean = ohlcv_data.drop_duplicates(subset=['symbol', 'timestamp'], keep='last')
            
            if len(ohlcv_data_clean) != len(ohlcv_data):
                print(f"  📊 Removed {len(ohlcv_data) - len(ohlcv_data_clean)} duplicate timestamps")
            
            # Prepare data tuples with only available columns
            data_tuples = []
            for _, row in ohlcv_data_clean.iterrows():
                tuple_data = []
                for col in columns:
                    if col in row.index:
                        tuple_data.append(row[col])
                    elif col == 'source_price':
                        tuple_data.append(row.get('source_price', row['close']))
                    elif col == 'is_synthetic':
                        tuple_data.append(True)
                    else:
                        tuple_data.append(None)
                data_tuples.append(tuple(tuple_data))
            
            # Use simple INSERT with ON CONFLICT DO NOTHING to avoid duplicate issues
            insert_query = f"""
            INSERT INTO minute_candles ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (symbol, timestamp) DO NOTHING
            """
            
            self.db_manager.execute_batch_insert(insert_query, data_tuples, page_size=1000)
            return True
        except Exception as e:
            print(f"❌ Minute candle save failed: {e}")
            return False
    
    # TASK 2: CANDLE RECONSTRUCTION
    def run_task2_candle_reconstruction(self, symbols: List[str] = None, batch_id: int = None) -> dict:
        """Run Task 2: Candle Reconstruction (1-min → 5-min)"""
        print("\n📊 TASK 2: CANDLE RECONSTRUCTION")
        print("-" * 40)
        
        if symbols is None:
            symbols = self._get_symbols_for_task2()
        
        if not symbols:
            return {'success': False, 'error': 'No symbols found for Task 2'}
        
        if batch_id is None:
            batch_id = int(time.time())
        
        print(f"Reconstructing candles for {len(symbols)} symbols...")
        start_time = time.time()
        success_count = 0
        error_count = 0
        total_input = 0
        total_output = 0
        
        for i, symbol in enumerate(symbols, 1):
            if self.interrupted:
                break
            
            print(f"[{i}/{len(symbols)}] {symbol}")
            
            result = self._reconstruct_symbol_candles(symbol, batch_id)
            
            if result['status'] == 'success':
                success_count += 1
                total_input += result['input_candles']
                total_output += result['output_candles']
                compression = result['input_candles'] / result['output_candles'] if result['output_candles'] > 0 else 0
                print(f"  ✅ {result['input_candles']} → {result['output_candles']} candles ({compression:.1f}:1)")
            else:
                error_count += 1
                print(f"  ❌ {result.get('error', 'Unknown error')}")
            
            time.sleep(0.03)
        
        duration = time.time() - start_time
        
        print(f"\n📊 TASK 2 COMPLETE:")
        print(f"Duration: {duration:.1f}s")
        print(f"Success: {success_count}, Errors: {error_count}")
        print(f"5-min candles: {total_output:,}")
        
        return {
            'success': success_count > 0,
            'symbols_processed': success_count,
            'total_input': total_input,
            'total_output': total_output,
            'duration': duration
        }
    
    def _get_symbols_for_task2(self) -> List[str]:
        """Get symbols that need Task 2 processing"""
        query = """
        SELECT DISTINCT m.symbol 
        FROM minute_candles m
        LEFT JOIN reconstructed_candles r ON m.symbol = r.symbol
        WHERE r.symbol IS NULL
        ORDER BY m.symbol
        """
        try:
            results = self.db_manager.execute_optimized_query(query, fetch=True)
            return [row[0] for row in results]
        except:
            # Fallback
            query = "SELECT DISTINCT symbol FROM minute_candles ORDER BY symbol"
            results = self.db_manager.execute_optimized_query(query, fetch=True)
            return [row[0] for row in results]
    
    def _reconstruct_symbol_candles(self, symbol: str, batch_id: int) -> dict:
        """Reconstruct 5-minute candles from 1-minute data"""
        try:
            # Get 1-minute candles
            query = """
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM minute_candles 
            WHERE symbol = %s 
            ORDER BY timestamp
            """
            
            with self.db_manager.get_connection() as conn:
                data = pd.read_sql(query, conn, params=(symbol,))
            
            if data.empty:
                return {'symbol': symbol, 'status': 'skipped', 'reason': 'No minute candles found'}
            
            if len(data) < 5:
                return {'symbol': symbol, 'status': 'skipped', 'reason': 'Insufficient data for 5-min candles'}
            
            # Reconstruct 5-minute candles
            reconstructed = self._aggregate_to_5min(data)
            if reconstructed.empty:
                return {'symbol': symbol, 'status': 'error', 'error': 'Reconstruction failed'}
            
            # Add metadata
            reconstructed['frequency'] = '5min'
            reconstructed['source_candles'] = 5
            reconstructed['batch_id'] = batch_id
            
            # Save
            success = self._save_reconstructed_candles(reconstructed)
            
            return {
                'symbol': symbol,
                'status': 'success' if success else 'error',
                'input_candles': len(data),
                'output_candles': len(reconstructed),
                'error': None if success else 'Save failed'
            }
            
        except Exception as e:
            return {'symbol': symbol, 'status': 'error', 'error': str(e)}
    
    def _aggregate_to_5min(self, data: pd.DataFrame) -> pd.DataFrame:
        """Aggregate 1-minute candles to 5-minute candles"""
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        
        # Group by 5-minute intervals
        data['time_group'] = data['timestamp'].dt.floor('5min')
        
        # Aggregate using OHLCV rules
        aggregated = data.groupby('time_group').agg({
            'open': 'first',    # First candle's open
            'high': 'max',      # Maximum high
            'low': 'min',       # Minimum low
            'close': 'last',    # Last candle's close
            'volume': 'sum',    # Sum of volumes
            'symbol': 'first'   # Preserve symbol
        }).reset_index()
        
        # Rename columns
        aggregated = aggregated.rename(columns={'time_group': 'timestamp'})
        
        # Filter out incomplete groups
        group_counts = data.groupby('time_group').size()
        complete_groups = group_counts[group_counts >= 5].index
        aggregated = aggregated[aggregated['timestamp'].isin(complete_groups)]
        
        return aggregated
    
    def _save_reconstructed_candles(self, data: pd.DataFrame) -> bool:
        """Save reconstructed candles"""
        try:
            columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'frequency', 'source_candles', 'batch_id']
            
            data_tuples = [
                tuple(row[col] for col in columns)
                for _, row in data.iterrows()
            ]
            
            insert_query = f"""
            INSERT INTO reconstructed_candles ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (symbol, timestamp, frequency) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
            """
            
            self.db_manager.execute_batch_insert(insert_query, data_tuples, page_size=1000)
            return True
        except Exception as e:
            print(f"❌ Reconstructed candle save failed: {e}")
            return False
    
    # COMPLETE PIPELINE EXECUTION
    def run_complete_pipeline(self, test_mode: bool = False, symbol_limit: int = None) -> dict:
        """Run the complete optimized pipeline (all tasks)"""
        print("🚀 COMPLETE OPTIMIZED PIPELINE")
        print("=" * 50)
        
        # Setup tables
        setup_result = self.setup_database_tables()
        if setup_result['errors']:
            print(f"⚠️  Setup warnings: {len(setup_result['errors'])} issues")
        
        # Get initial status
        initial_status = self.get_pipeline_status()
        print(f"\n📊 INITIAL STATUS:")
        print(f"Source: {initial_status['source_rows']:,} rows, {initial_status['source_symbols']:,} symbols")
        print(f"Task 1: {initial_status['task1_rows']:,} rows, {initial_status['task1_symbols']:,} symbols")
        print(f"Bridge: {initial_status['bridge_rows']:,} rows, {initial_status['bridge_symbols']:,} symbols") 
        print(f"Task 2: {initial_status['task2_rows']:,} rows, {initial_status['task2_symbols']:,} symbols")
        
        # Get symbols to process
        if test_mode:
            all_symbols = self.db_manager.execute_optimized_query(
                "SELECT DISTINCT symbol FROM trading_tickers ORDER BY symbol LIMIT 3", fetch=True)
            symbols = [row[0] for row in all_symbols]
            print(f"\n🧪 TEST MODE: Processing {len(symbols)} symbols")
        else:
            symbols_query = f"SELECT DISTINCT symbol FROM trading_tickers ORDER BY symbol"
            if symbol_limit:
                symbols_query += f" LIMIT {symbol_limit}"
            all_symbols = self.db_manager.execute_optimized_query(symbols_query, fetch=True)
            symbols = [row[0] for row in all_symbols]
            print(f"\n🎯 PROCESSING: {len(symbols)} symbols")
        
        if not symbols:
            return {'success': False, 'error': 'No symbols found'}
        
        batch_id = int(time.time())
        pipeline_start = time.time()
        results = {}
        
        try:
            # Task 1: IV Interpolation
            print(f"\n" + "="*60)
            task1_result = self.run_task1_interpolation(symbols, batch_id)
            results['task1'] = task1_result
            
            if not task1_result['success']:
                return {'success': False, 'error': 'Task 1 failed', 'results': results}
            
            # Data Bridge
            print(f"\n" + "="*60)
            bridge_result = self.run_data_bridge(symbols, batch_id)
            results['bridge'] = bridge_result
            
            if not bridge_result['success']:
                return {'success': False, 'error': 'Data bridge failed', 'results': results}
            
            # Task 2: Candle Reconstruction
            print(f"\n" + "="*60)
            task2_result = self.run_task2_candle_reconstruction(symbols, batch_id)
            results['task2'] = task2_result
            
            if not task2_result['success']:
                return {'success': False, 'error': 'Task 2 failed', 'results': results}
            
            # Final summary
            total_duration = time.time() - pipeline_start
            final_status = self.get_pipeline_status()
            
            print(f"\n🏁 COMPLETE PIPELINE FINISHED")
            print("=" * 50)
            print(f"Total Duration: {total_duration:.1f}s ({total_duration/60:.1f} minutes)")
            print(f"Batch ID: {batch_id}")
            print()
            print(f"📊 FINAL RESULTS:")
            print(f"Task 1: {task1_result['symbols_processed']} symbols → {task1_result['total_output']:,} interpolated rows")
            print(f"Bridge: {bridge_result['symbols_processed']} symbols → {bridge_result['total_output']:,} minute candles") 
            print(f"Task 2: {task2_result['symbols_processed']} symbols → {task2_result['total_output']:,} 5-min candles")
            print()
            print(f"📈 DATA FLOW:")
            print(f"Original → Interpolated → Minute Candles → 5-min Candles")
            print(f"{initial_status['source_rows']:,} → {final_status['task1_rows']:,} → {final_status['bridge_rows']:,} → {final_status['task2_rows']:,}")
            
            return {
                'success': True,
                'batch_id': batch_id,
                'duration': total_duration,
                'results': results,
                'final_status': final_status
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e), 'results': results}
    
    def cleanup(self):
        """Cleanup resources"""
        if self.db_manager:
            self.db_manager.close_pool()

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Complete Optimized Pipeline (All Tasks)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python complete_pipeline.py --test                   # Test all tasks with 3 symbols
  python complete_pipeline.py --task all               # Run complete pipeline
  python complete_pipeline.py --task interpolation     # Task 1 only
  python complete_pipeline.py --task bridge           # Data bridge only
  python complete_pipeline.py --task candles          # Task 2 only
  python complete_pipeline.py --symbols 100           # Process 100 symbols through all tasks
  python complete_pipeline.py --estimate              # Show processing estimates
        """
    )
    
    parser.add_argument('--task', choices=['interpolation', 'bridge', 'candles', 'all'], 
                       default='all', help='Which task(s) to run')
    parser.add_argument('--test', action='store_true', help='Test mode with 3 symbols')
    parser.add_argument('--symbols', type=int, help='Limit number of symbols')
    parser.add_argument('--estimate', action='store_true', help='Show estimates only')
    parser.add_argument('--setup-only', action='store_true', help='Setup tables only')
    
    args = parser.parse_args()
    
    try:
        config = get_config()
        pipeline = CompleteOptimizedPipeline(config)
        
        if args.setup_only:
            result = pipeline.setup_database_tables()
            print(f"\n✅ Setup complete")
            return 0
        
        if args.estimate:
            status = pipeline.get_pipeline_status()
            print("📊 PIPELINE STATUS & ESTIMATES")
            print("=" * 40)
            print(f"Source data: {status['source_rows']:,} rows, {status['source_symbols']:,} symbols")
            print(f"Task 1 progress: {status['task1_symbols']}/{status['source_symbols']} symbols")
            print(f"Bridge progress: {status['bridge_symbols']}/{status['task1_symbols']} symbols")
            print(f"Task 2 progress: {status['task2_symbols']}/{status['bridge_symbols']} symbols")
            
            # Rough estimates
            remaining_symbols = status['source_symbols'] - status['task2_symbols']
            if remaining_symbols > 0:
                est_minutes = remaining_symbols * 0.5  # 0.5 minutes per symbol estimate
                print(f"\nEstimated time for remaining work: {est_minutes:.1f} minutes")
            else:
                print(f"\n✅ Pipeline appears complete!")
            
            return 0
        
        # Run requested task(s)
        if args.task == 'all':
            result = pipeline.run_complete_pipeline(test_mode=args.test, symbol_limit=args.symbols)
        else:
            # Single task mode
            symbols = None
            if args.symbols or args.test:
                limit = 3 if args.test else args.symbols
                symbols_query = f"SELECT DISTINCT symbol FROM trading_tickers ORDER BY symbol LIMIT {limit}"
                symbol_results = pipeline.db_manager.execute_optimized_query(symbols_query, fetch=True)
                symbols = [row[0] for row in symbol_results]
            
            if args.task == 'interpolation':
                result = pipeline.run_task1_interpolation(symbols)
            elif args.task == 'bridge':
                result = pipeline.run_data_bridge(symbols)
            elif args.task == 'candles':
                result = pipeline.run_task2_candle_reconstruction(symbols)
        
        return 0 if result['success'] else 1
        
    except KeyboardInterrupt:
        print("\n⏸️  Pipeline interrupted by user")
        return 130
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        return 1
    finally:
        if 'pipeline' in locals():
            pipeline.cleanup()

if __name__ == "__main__":
    sys.exit(main())