#!/usr/bin/env python3
"""
Complete Dual-Task Pipeline: IV Interpolation + Candle Reconstruction
Usage: 
  python main.py                              # Run Task 1 (IV interpolation)
  python main.py --task candles               # Run Task 2 (candle reconstruction)  
  python main.py --task both                  # Run both tasks sequentially
  python main.py --test --task interpolation  # Test Task 1
  python main.py --test --task candles        # Test Task 2
  python main.py --generate-sample-candles    # Generate sample 1-min candle data
"""

import argparse
import sys
import os
from pathlib import Path
from typing import Optional
import psycopg2.extras

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from config import get_config
from database.connection import DatabaseManager
from database.schema import SchemaManager
from database.candle_schema import CandleSchemaManager
from interpolation.batch_processor import BatchProcessor
from candle_reconstruction.core import MultiSymbolCandleReconstructor
from data_bridge.ohlcv_converter import InterpolatedToOHLCVConverter
from monitoring.logging import setup_logging
from monitoring.progress import ProgressTracker, RealtimeMonitor

def setup_environment():
    """Setup logging and directories"""
    config = get_config()
    
    # Create directories
    Path(config.output_dir).mkdir(exist_ok=True)
    Path(config.log_dir).mkdir(exist_ok=True)
    
    # Setup logging
    setup_logging(config.log_dir, config.processing.log_level)
    
    return config

def validate_database_setup(config, task: str = 'interpolation') -> bool:
    """Validate database connection and setup tables based on task"""
    try:
        db_manager = DatabaseManager(config.database)
        
        if task in ['interpolation', 'both']:
            schema_manager = SchemaManager(db_manager)
            print("CHECKING IV INTERPOLATION SCHEMA...")
            
            # Check interpolated table
            check_table_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'interpolated_trading_tickers'
            )
            """
            table_exists = db_manager.execute_query(check_table_query, fetch=True)[0][0]
            
            if not table_exists:
                print("CREATING: interpolated data table...")
                if not schema_manager.create_interpolated_table():
                    print("FAILED: Failed to create interpolated data table")
                    return False
            else:
                print("SUCCESS: Interpolated data table exists")
            
            # Check progress table
            check_progress_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'interpolation_progress'
            )
            """
            progress_exists = db_manager.execute_query(check_progress_query, fetch=True)[0][0]
            
            if not progress_exists:
                print("CREATING: interpolation progress table...")
                if not schema_manager.create_progress_table():
                    print("FAILED: Failed to create progress table")
                    return False
            else:
                print("SUCCESS: Interpolation progress table exists")
        
        if task in ['candles', 'both']:
            candle_schema_manager = CandleSchemaManager(db_manager)
            print("CHECKING CANDLE RECONSTRUCTION SCHEMA...")
            
            # Check reconstructed candles table
            check_candle_table = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'reconstructed_candles'
            )
            """
            candle_table_exists = db_manager.execute_query(check_candle_table, fetch=True)[0][0]
            
            if not candle_table_exists:
                print("CREATING: reconstructed candles table...")
                if not candle_schema_manager.create_candle_tables():
                    print("FAILED: Failed to create candle tables")
                    return False
            else:
                print("SUCCESS: Reconstructed candles table exists")
            
            # Check candle progress table
            check_candle_progress = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'candle_reconstruction_progress'
            )
            """
            candle_progress_exists = db_manager.execute_query(check_candle_progress, fetch=True)[0][0]
            
            if not candle_progress_exists:
                print("CREATING: candle reconstruction progress table...")
                if not candle_schema_manager.create_candle_progress_table():
                    print("FAILED: Failed to create candle progress table")
                    return False
            else:
                print("SUCCESS: Candle reconstruction progress table exists")
            
            # Check if minute_candles table exists (input data)
            check_minute_candles = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'minute_candles'
            )
            """
            minute_exists = db_manager.execute_query(check_minute_candles, fetch=True)[0][0]
            
            if not minute_exists:
                print("CREATING: sample minute_candles table structure...")
                candle_schema_manager.create_sample_minute_candles_table()
                print("INFO: You'll need to populate minute_candles with your 1-minute OHLCV data")
            else:
                # Check if there's actual data
                count_query = "SELECT COUNT(*) FROM minute_candles"
                minute_count = db_manager.execute_query(count_query, fetch=True)[0][0]
                print(f"SOURCE DATA: {minute_count:,} rows in minute_candles")
                
                if minute_count == 0:
                    print("INFO: minute_candles table is empty - use --generate-sample-candles for testing")
        
        # Check source data for interpolation
        if task in ['interpolation', 'both']:
            count_query = "SELECT COUNT(*) FROM trading_tickers"
            source_count = db_manager.execute_query(count_query, fetch=True)[0][0]
            print(f"IV SOURCE DATA: {source_count:,} rows in trading_tickers")
            
            if source_count == 0:
                print("WARNING: No source data found in trading_tickers table")
                return False
        
        return True
        
    except Exception as e:
        print(f"FAILED: Database validation failed: {e}")
        return False

def generate_sample_candle_data(config, num_symbols: int = 5, hours: int = 24):
    """Generate sample 1-minute candle data for testing"""
    print(f"GENERATING: Sample 1-minute candle data ({num_symbols} symbols, {hours} hours)")
    
    try:
        import numpy as np
        import pandas as pd
        from datetime import datetime, timedelta
        
        db_manager = DatabaseManager(config.database)
        
        # Sample symbols (Bitcoin options)
        symbols = [
            'btc-20mar23-24500-c',
            'btc-20mar23-25000-c', 
            'btc-20mar23-25500-c',
            'btc-20mar23-24500-p',
            'btc-20mar23-25000-p'
        ][:num_symbols]
        
        start_time = datetime(2023, 3, 20, 9, 0)  # 9 AM
        end_time = start_time + timedelta(hours=hours)
        
        all_candles = []
        
        for symbol in symbols:
            print(f"   Generating candles for {symbol}...")
            
            # Generate minute-by-minute timeline
            current_time = start_time
            base_price = 25000 + np.random.normal(0, 500)  # Random base price around $25k
            
            while current_time < end_time:
                # Simulate price movement (random walk)
                price_change = np.random.normal(0, 10)  # $10 std dev per minute
                
                # Calculate OHLC for this minute
                open_price = base_price
                close_price = base_price + price_change
                
                # High and low around open/close
                high_price = max(open_price, close_price) + abs(np.random.normal(0, 3))
                low_price = min(open_price, close_price) - abs(np.random.normal(0, 3))
                
                # Volume (random but realistic)
                volume = max(0, np.random.exponential(50))
                
                candle = {
                    'symbol': symbol,
                    'timestamp': current_time,
                    'open': round(open_price, 2),
                    'high': round(high_price, 2), 
                    'low': round(low_price, 2),
                    'close': round(close_price, 2),
                    'volume': round(volume, 4)
                }
                
                all_candles.append(candle)
                
                # Update for next minute
                base_price = close_price
                current_time += timedelta(minutes=1)
        
        # Create DataFrame and save to database
        df_candles = pd.DataFrame(all_candles)
        
        print(f"   Generated {len(df_candles):,} 1-minute candles")
        
        # Save to database using proper PostgreSQL insertion
        insert_query = """
        INSERT INTO minute_candles (symbol, timestamp, open, high, low, close, volume)
        VALUES %s
        """
        
        # Convert DataFrame to list of tuples
        data_tuples = [
            (row['symbol'], row['timestamp'], row['open'], row['high'], 
             row['low'], row['close'], row['volume'])
            for _, row in df_candles.iterrows()
        ]
        
        # Batch insert using psycopg2.extras
        with db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                import psycopg2.extras
                psycopg2.extras.execute_values(
                    cur, insert_query, data_tuples,
                    template=None, page_size=1000
                )
            conn.commit()
        
        print(f"SUCCESS: Sample candle data generated and saved")
        print(f"   Total candles: {len(df_candles):,}")
        print(f"   Symbols: {num_symbols}")
        print(f"   Time range: {start_time} to {end_time}")
        
        return True
        
    except Exception as e:
        print(f"FAILED: Sample data generation failed: {e}")
        return False

def test_iv_interpolation(config):
    """Test IV interpolation (Task 1)"""
    print("TEST MODE: IV Interpolation (Task 1)")
    
    if not validate_database_setup(config, 'interpolation'):
        return False
    
    try:
        db_manager = DatabaseManager(config.database)
        processor = BatchProcessor(db_manager, config)
        
        # Get test symbols
        test_symbols = processor.get_symbols(
            start_date='2023-03-20 00:00:00',
            end_date='2023-03-20 23:59:59'
        )[:3]  # Limit to 3 symbols for testing
        
        if not test_symbols:
            print("FAILED: No symbols found for test")
            return False
        
        print(f"   Testing with {len(test_symbols)} symbols: {', '.join(test_symbols)}")
        
        processor.run_sequential(symbols=test_symbols, limit=3)
        processor.progress_tracker.print_progress_report()
        
        print("SUCCESS: IV interpolation test completed")
        return True
        
    except Exception as e:
        print(f"FAILED: IV interpolation test failed: {e}")
        return False

def test_candle_reconstruction(config):
    """Test candle reconstruction (Task 2)"""
    print("TEST MODE: Candle Reconstruction (Task 2)")
    
    if not validate_database_setup(config, 'candles'):
        return False
    
    try:
        db_manager = DatabaseManager(config.database)
        
        # Check if we have minute candle data
        count_query = "SELECT COUNT(*) FROM minute_candles"
        minute_count = db_manager.execute_query(count_query, fetch=True)[0][0]
        
        if minute_count == 0:
            print("INFO: No minute candle data found. Generating sample data...")
            if not generate_sample_candle_data(config, num_symbols=3, hours=2):
                return False
        
        reconstructor = MultiSymbolCandleReconstructor(db_manager, config)
        
        # Get symbols with minute data
        symbols = reconstructor.get_symbols_with_minute_data()[:3]  # Test with 3 symbols
        
        if not symbols:
            print("FAILED: No symbols with minute candle data found")
            return False
        
        print(f"   Testing with {len(symbols)} symbols: {', '.join(symbols)}")
        
        # Process each symbol
        for symbol in symbols:
            result = reconstructor.process_symbol(symbol)
            print(f"   {symbol}: {result['status']}")
            if result['status'] == 'success':
                print(f"      {result['input_candles']} → {result['output_candles']} candles")
                print(f"      Compression: {result['stats']['compression_ratio']:.1f}:1")
        
        print("SUCCESS: Candle reconstruction test completed")
        return True
        
    except Exception as e:
        print(f"FAILED: Candle reconstruction test failed: {e}")
        return False

def run_task_1(config):
    """Run Task 1: IV Interpolation"""
    print("TASK 1: IV Interpolation (Hourly → 1-minute)")
    
    if not validate_database_setup(config, 'interpolation'):
        return False
    
    try:
        db_manager = DatabaseManager(config.database)
        processor = BatchProcessor(db_manager, config)
        
        symbols = processor.get_symbols()
        if not symbols:
            print("FAILED: No symbols found for processing")
            return False
        
        print(f"PROCESSING: Found {len(symbols)} symbols")
        
        if len(symbols) > 100:
            response = input(f"WARNING: About to process {len(symbols)} symbols. Continue? (y/N): ")
            if response.lower() != 'y':
                print("CANCELLED: Processing cancelled by user")
                return False
        
        result = processor.run_parallel(symbols)
        processor.progress_tracker.print_progress_report()
        
        print(f"SUCCESS: Task 1 completed - Batch ID: {result['batch_id']}")
        return True
        
    except Exception as e:
        print(f"FAILED: Task 1 failed: {e}")
        return False

def run_data_bridge(config, batch_id: Optional[int] = None):
    """Run Data Bridge: Convert Task 1 interpolated data to Task 2 OHLCV format"""
    print("BRIDGE: Converting interpolated data to OHLCV format")
    
    if not validate_database_setup(config, 'both'):
        return False
    
    try:
        db_manager = DatabaseManager(config.database)
        converter = InterpolatedToOHLCVConverter(db_manager, config)
        
        # Get symbols from Task 1 output
        symbols = converter.get_interpolated_symbols(batch_id)
        if not symbols:
            print("FAILED: No interpolated data found for conversion")
            print("HINT: Run Task 1 first: python main.py --task interpolation")
            return False
        
        print(f"CONVERTING: Found {len(symbols)} symbols with interpolated data")
        
        # Convert to OHLCV
        result = converter.convert_batch(symbols, batch_id)
        
        print(f"\nBRIDGE COMPLETE:")
        print(f"   Success: {result['success']}")
        print(f"   Errors: {result['errors']}")
        print(f"   Skipped: {result['skipped']}")
        
        if result['success'] > 0:
            print(f"✅ Data bridge successful - ready for Task 2")
            return True
        else:
            print(f"❌ No symbols converted successfully")
            return False
        
    except Exception as e:
        print(f"FAILED: Data bridge failed: {e}")
        return False
    """Run Task 2: Candle Reconstruction"""
    print("TASK 2: Candle Reconstruction (1-minute → 5-minute)")
    
    if not validate_database_setup(config, 'candles'):
        return False
    
    try:
        db_manager = DatabaseManager(config.database)
        reconstructor = MultiSymbolCandleReconstructor(db_manager, config)
        
        symbols = reconstructor.get_symbols_with_minute_data()
        if not symbols:
            print("FAILED: No symbols with minute candle data found")
            print("HINT: Use --generate-sample-candles to create test data")
            return False
        
        print(f"PROCESSING: Found {len(symbols)} symbols with minute data")
        
        success_count = 0
        error_count = 0
        
        for i, symbol in enumerate(symbols, 1):
            print(f"Processing {i}/{len(symbols)}: {symbol}")
            result = reconstructor.process_symbol(symbol)
            
            if result['status'] == 'success':
                success_count += 1
                print(f"   SUCCESS: {result['input_candles']} → {result['output_candles']} candles")
            else:
                error_count += 1
                print(f"   FAILED: {result.get('error', result.get('reason', 'Unknown error'))}")
        
        print(f"\nTASK 2 COMPLETE:")
        print(f"   Success: {success_count}")
        print(f"   Errors: {error_count}")
        
        return success_count > 0
        
    except Exception as e:
        print(f"FAILED: Task 2 failed: {e}")
        return False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Dual-Task Pipeline: IV Interpolation + Candle Reconstruction',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Tasks:
  interpolation    IV interpolation (hourly → 1-minute)
  bridge          Convert Task 1 output to OHLCV for Task 2
  candles         Candle reconstruction (1-minute → 5-minute)  
  both            Run Task 1, then Task 2 (using sample data)
  pipeline        Run complete pipeline: Task 1 → Bridge → Task 2

Examples:
  python main.py                              # Run Task 1 (default)
  python main.py --task bridge               # Convert Task 1 to OHLCV
  python main.py --task candles               # Run Task 2
  python main.py --task pipeline             # Full pipeline
  python main.py --test --task pipeline      # Test complete pipeline
  python main.py --auto-bridge --task both   # Auto-bridge between tasks
        """
    )
    
    parser.add_argument('--task', choices=['interpolation', 'bridge', 'candles', 'both', 'pipeline'], 
                       default='interpolation', help='Which task to run')
    parser.add_argument('--test', action='store_true', help='Run in test mode')
    parser.add_argument('--auto-bridge', action='store_true', 
                       help='Automatically run data bridge between Task 1 and Task 2')
    parser.add_argument('--bridge-batch-id', type=int,
                       help='Convert specific batch ID from Task 1 to OHLCV')
    parser.add_argument('--generate-sample-candles', action='store_true',
                       help='Generate sample 1-minute candle data for testing')
    parser.add_argument('--validate-only', action='store_true',
                       help='Only validate database setup')
    parser.add_argument('--env', choices=['development', 'testing', 'production'], 
                       default='production', help='Environment')
    
    args = parser.parse_args()
    
    # Set environment
    os.environ['ENVIRONMENT'] = args.env
    
    try:
        config = setup_environment()
        print(f"ENVIRONMENT: {config.environment}")
        print(f"LOG DIRECTORY: {config.log_dir}")
        
    except Exception as e:
        print(f"FAILED: Setup failed: {e}")
        sys.exit(1)
    
    try:
        if args.generate_sample_candles:
            success = generate_sample_candle_data(config)
            sys.exit(0 if success else 1)
            
        elif args.validate_only:
            print("VALIDATE MODE: Checking database setup")
            success = validate_database_setup(config, args.task)
            sys.exit(0 if success else 1)
            
        elif args.test:
            if args.task == 'interpolation':
                success = test_iv_interpolation(config)
            elif args.task == 'bridge':
                # Test bridge requires Task 1 data
                print("BRIDGE TEST: Ensuring Task 1 data exists...")
                test_iv_interpolation(config)  # Generate some test data
                success = run_data_bridge(config)
            elif args.task == 'candles':
                success = test_candle_reconstruction(config)
            elif args.task == 'both':
                success1 = test_iv_interpolation(config)
                success2 = test_candle_reconstruction(config)
                success = success1 and success2
            elif args.task == 'pipeline':
                print("PIPELINE TEST: Testing complete pipeline flow")
                success1 = test_iv_interpolation(config)
                if success1 and args.auto_bridge:
                    success2 = run_data_bridge(config)
                    success3 = test_candle_reconstruction(config) if success2 else False
                    success = success1 and success2 and success3
                else:
                    success2 = test_candle_reconstruction(config)
                    success = success1 and success2
            sys.exit(0 if success else 1)
            
        else:
            # Production runs
            if args.task == 'interpolation':
                success = run_task_1(config)
            elif args.task == 'bridge':
                success = run_data_bridge(config, args.bridge_batch_id)
            elif args.task == 'candles':
                success = run_task_2(config)
            elif args.task == 'both':
                print("RUNNING BOTH TASKS")
                if args.auto_bridge:
                    # Task 1 → Bridge → Task 2
                    success1 = run_task_1(config)
                    if success1:
                        print("\n" + "="*50)
                        success2 = run_data_bridge(config)
                        if success2:
                            print("\n" + "="*50)
                            success3 = run_task_2(config)
                            success = success1 and success2 and success3
                        else:
                            success = False
                    else:
                        success = False
                else:
                    # Traditional: Task 1, then Task 2 (with sample data)
                    success1 = run_task_1(config)
                    if success1:
                        print("\n" + "="*50)
                        success2 = run_task_2(config)
                        success = success1 and success2
                    else:
                        success = False
            elif args.task == 'pipeline':
                print("RUNNING COMPLETE PIPELINE: Task 1 → Bridge → Task 2")
                success1 = run_task_1(config)
                if success1:
                    print("\n" + "="*50)
                    success2 = run_data_bridge(config)
                    if success2:
                        print("\n" + "="*50)
                        success3 = run_task_2(config)
                        success = success1 and success2 and success3
                    else:
                        success = False
                else:
                    success = False
            
            sys.exit(0 if success else 1)
    
    except KeyboardInterrupt:
        print("\nWARNING: Processing interrupted by user")
        sys.exit(130)
        
    except Exception as e:
        print(f"FAILED: Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()