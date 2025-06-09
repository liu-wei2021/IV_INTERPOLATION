# src/interpolation/batch_processor.py
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Optional
import pandas as pd
import logging
import time
import psycopg2.extras
from interpolation.core import IVInterpolator
from monitoring.progress import ProgressTracker
from monitoring.logging import PerformanceLogger

logger = logging.getLogger(__name__)

class BatchProcessor:
    """Enhanced batch processor with progress tracking and error recovery"""
    
    def __init__(self, db_manager, config):
        self.db_manager = db_manager
        self.config = config
        self.interpolator = IVInterpolator(
            method=config.interpolation.method,
            min_points=config.interpolation.min_data_points
        )
        self.progress_tracker = ProgressTracker(db_manager)
        self.perf_logger = PerformanceLogger()
    
    def get_symbols(self, start_date: str = None, end_date: str = None) -> List[str]:
        """Get list of unique symbols to process with optional date filtering"""
        if start_date is None:
            start_date = '2023-03-15'
        if end_date is None:
            end_date = '2023-03-26'
            
        query = """
        SELECT DISTINCT symbol 
        FROM trading_tickers 
        WHERE date >= %s AND date <= %s
        ORDER BY symbol
        """
        
        try:
            results = self.db_manager.execute_query(
                query, (start_date, end_date), fetch=True
            )
            symbols = [row[0] for row in results]
            logger.info(f"Found {len(symbols)} unique symbols between {start_date} and {end_date}")
            return symbols
        except Exception as e:
            logger.error(f"Failed to retrieve symbols: {e}")
            return []
    
    def get_pending_symbols(self, batch_id: int) -> List[str]:
        """Get symbols that still need processing from a previous batch"""
        query = """
        SELECT symbol FROM interpolation_progress 
        WHERE batch_id = %s AND status IN ('pending', 'error')
        ORDER BY symbol
        """
        try:
            results = self.db_manager.execute_query(query, (batch_id,), fetch=True)
            return [row[0] for row in results]
        except Exception as e:
            logger.error(f"Failed to get pending symbols: {e}")
            return []
    
    def process_symbol(self, symbol: str) -> Dict:
        """Process a single symbol with comprehensive error handling"""
        start_time = time.time()
        
        try:
            logger.debug(f"Starting processing for symbol: {symbol}")
            self.progress_tracker.start_symbol(symbol)
            
            # Fetch data for symbol with optimized query
            query = """
            SELECT symbol, date, iv, underlying_price, time_to_maturity,
                   strike, callput, interest_rate, mark_price, index_price,
                   volume, quote_volume, record_time
            FROM trading_tickers 
            WHERE symbol = %s 
            ORDER BY date
            """
            
            # Use pandas-optimized connection to avoid warnings
            engine = self.db_manager.get_pandas_connection()
            data = pd.read_sql(query, engine, params=(symbol,))
            
            if data.empty:
                reason = "No data found"
                self.progress_tracker.skip_symbol(symbol, reason)
                return {'symbol': symbol, 'status': 'skipped', 'reason': reason}
            
            logger.debug(f"Retrieved {len(data)} rows for {symbol}")
            
            # Interpolate using core engine
            interpolated = self.interpolator.interpolate_symbol(data)
            
            if interpolated is None:
                reason = "Interpolation failed or insufficient data"
                self.progress_tracker.skip_symbol(symbol, reason)
                return {'symbol': symbol, 'status': 'skipped', 'reason': reason}
            
            # Add batch metadata
            interpolated['batch_id'] = self.progress_tracker.batch_id
            
            # Save results to database
            save_success = self._save_results(interpolated)
            
            processing_time = time.time() - start_time
            
            if save_success:
                self.progress_tracker.complete_symbol(
                    symbol, len(data), len(interpolated), processing_time
                )
                self.perf_logger.log_symbol_processing(
                    symbol, len(data), len(interpolated), processing_time
                )
                
                return {
                    'symbol': symbol, 
                    'status': 'success', 
                    'input_rows': len(data),
                    'output_rows': len(interpolated),
                    'processing_time': processing_time
                }
            else:
                error_msg = "Failed to save to database"
                self.progress_tracker.error_symbol(symbol, error_msg)
                return {'symbol': symbol, 'status': 'error', 'error': error_msg}
                
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = str(e)
            logger.error(f"Error processing {symbol}: {error_msg}")
            self.progress_tracker.error_symbol(symbol, error_msg)
            return {
                'symbol': symbol, 
                'status': 'error', 
                'error': error_msg,
                'processing_time': processing_time
            }
    
    def _save_results(self, interpolated_data: pd.DataFrame) -> bool:
        """Save interpolated results to database efficiently using batch insert"""
        try:
            if interpolated_data.empty:
                logger.warning("No data to save")
                return True
            
            start_time = time.time()
            
            # Prepare data for insertion
            columns = [
                'symbol', 'date', 'record_time', 'iv', 'underlying_price',
                'time_to_maturity', 'strike', 'callput', 'interest_rate',
                'mark_price', 'index_price', 'volume', 'quote_volume',
                'is_interpolated', 'batch_id'
            ]
            
            # Ensure all required columns exist
            for col in columns:
                if col not in interpolated_data.columns:
                    interpolated_data[col] = None
            
            # Convert DataFrame to list of tuples for batch insert
            data_tuples = []
            for _, row in interpolated_data.iterrows():
                data_tuple = tuple(
                    None if pd.isna(row[col]) else row[col] 
                    for col in columns
                )
                data_tuples.append(data_tuple)
            
            # Batch insert using psycopg2.extras.execute_values for efficiency
            insert_query = f"""
            INSERT INTO interpolated_trading_tickers 
            ({', '.join(columns)})
            VALUES %s
            """
            
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur, insert_query, data_tuples,
                        template=None, page_size=1000
                    )
                conn.commit()
            
            save_time = time.time() - start_time
            self.perf_logger.log_database_operation(
                'INSERT', len(data_tuples), save_time
            )
            
            logger.debug(f"Saved {len(data_tuples)} rows in {save_time:.3f}s")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")
            return False
    
    def run_parallel(self, symbols: List[str] = None, resume_batch_id: int = None):
        """Run batch processing in parallel with progress tracking"""
        
        if resume_batch_id:
            # Resume mode - get pending symbols from previous batch
            symbols = self.get_pending_symbols(resume_batch_id)
            self.progress_tracker.batch_id = resume_batch_id
            logger.info(f"Resuming batch {resume_batch_id} with {len(symbols)} pending symbols")
        else:
            # New batch mode
            if symbols is None:
                symbols = self.get_symbols()
            
            if not symbols:
                logger.error("No symbols to process")
                return
            
            # Initialize progress tracking
            if not self.progress_tracker.initialize_symbols(symbols):
                logger.error("Failed to initialize progress tracking")
                return
        
        logger.info(f"Starting parallel processing of {len(symbols)} symbols")
        self.perf_logger.log_batch_start(self.progress_tracker.batch_id, len(symbols))
        
        batch_start_time = time.time()
        success_count = 0
        error_count = 0
        skip_count = 0
        
        try:
            # Process symbols in parallel
            with ProcessPoolExecutor(max_workers=self.config.processing.max_workers) as executor:
                # Submit all jobs
                future_to_symbol = {
                    executor.submit(self.process_symbol, symbol): symbol 
                    for symbol in symbols
                }
                
                # Process completed jobs
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        result = future.result()
                        
                        # Log result
                        if result['status'] == 'success':
                            success_count += 1
                            logger.info(f"SUCCESS {symbol}: {result['input_rows']} -> {result['output_rows']} rows")
                        elif result['status'] == 'skipped':
                            skip_count += 1
                            logger.warning(f"SKIPPED {symbol}: {result['reason']}")
                        else:  # error
                            error_count += 1
                            logger.error(f"FAILED {symbol}: {result.get('error', 'Unknown error')}")
                        
                        # Print periodic progress
                        total_processed = success_count + error_count + skip_count
                        if total_processed % 10 == 0:
                            progress_pct = (total_processed / len(symbols)) * 100
                            logger.info(f"Progress: {total_processed}/{len(symbols)} ({progress_pct:.1f}%)")
                            
                    except Exception as e:
                        error_count += 1
                        logger.error(f"FAILED {symbol}: Future execution failed: {e}")
        
        except KeyboardInterrupt:
            logger.warning("Processing interrupted by user")
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
        
        # Log batch completion
        batch_duration = time.time() - batch_start_time
        self.perf_logger.log_batch_complete(
            self.progress_tracker.batch_id, batch_duration, success_count, error_count
        )
        
        # Print final summary
        logger.info(f"\nBATCH PROCESSING COMPLETE")
        logger.info(f"Total Duration: {batch_duration:.1f}s")
        logger.info(f"Success: {success_count} | Errors: {error_count} | Skipped: {skip_count}")
        
        if error_count > 0:
            logger.warning(f"WARNING: {error_count} symbols failed - check logs or use --resume")
        
        return {
            'batch_id': self.progress_tracker.batch_id,
            'success_count': success_count,
            'error_count': error_count,
            'skip_count': skip_count,
            'duration': batch_duration
        }
    
    def run_sequential(self, symbols: List[str] = None, limit: int = None):
        """Run processing sequentially (for debugging or testing)"""
        if symbols is None:
            symbols = self.get_symbols()
        
        if limit:
            symbols = symbols[:limit]
        
        logger.info(f"Starting sequential processing of {len(symbols)} symbols")
        
        # Initialize progress tracking for sequential mode
        if not self.progress_tracker.initialize_symbols(symbols):
            logger.error("Failed to initialize progress tracking")
            return
        
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"Processing {i}/{len(symbols)}: {symbol}")
            result = self.process_symbol(symbol)
            logger.info(f"Result: {result['status']}")
            
            if result['status'] == 'error':
                logger.error(f"Error details: {result.get('error', 'Unknown')}")
        
        # Print final progress report
        self.progress_tracker.print_progress_report()


def process_symbol_standalone(symbol: str, db_config_dict: dict, interpolation_config: dict) -> Dict:
    """Standalone function for multiprocessing (needed for pickle serialization)"""
    from database.connection import DatabaseManager
    from config import DatabaseConfig, InterpolationConfig
    
    # Recreate objects in the worker process
    db_config = DatabaseConfig(**db_config_dict)
    db_manager = DatabaseManager(db_config)
    
    # Create a simple processor for this symbol
    processor = BatchProcessor.__new__(BatchProcessor)
    processor.db_manager = db_manager
    processor.interpolator = IVInterpolator(
        method=interpolation_config['method'],
        min_points=interpolation_config['min_data_points']
    )
    processor.progress_tracker = ProgressTracker(db_manager)
    processor.perf_logger = PerformanceLogger()
    
    return processor.process_symbol(symbol)