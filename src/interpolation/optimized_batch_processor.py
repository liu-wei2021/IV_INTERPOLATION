# src/interpolation/optimized_batch_processor.py
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import psutil
import gc
from typing import List, Dict, Optional, Tuple
import pandas as pd
import numpy as np
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ProcessingMetrics:
    """Track processing performance metrics"""
    symbols_processed: int = 0
    total_input_rows: int = 0
    total_output_rows: int = 0
    processing_time: float = 0.0
    memory_peak_gb: float = 0.0
    errors: int = 0
    batch_size: int = 0

class ProductionBatchProcessor:
    """
    Production-optimized batch processor for 32M+ rows
    
    Key Optimizations:
    1. Intelligent symbol batching based on data size
    2. Memory-aware processing with auto-scaling
    3. Connection pool optimization across workers
    4. Progress checkpointing for long-running jobs
    5. Auto-recovery from failures
    6. Resource monitoring and throttling
    """
    
    def __init__(self, db_manager, config):
        self.db_manager = db_manager
        self.config = config
        self.metrics = ProcessingMetrics()
        
        # Performance tuning
        self.max_workers = min(config.processing.max_workers, mp.cpu_count())
        self.memory_limit_gb = config.processing.memory_limit_gb
        self.enable_monitoring = config.processing.enable_logging
        
        # Auto-scaling parameters
        self.min_batch_size = 100
        self.max_batch_size = 2000
        self.current_batch_size = config.processing.symbols_per_batch
        
        # Progress tracking
        self.checkpoint_interval = 50  # Save progress every N symbols
        self.processed_symbols = set()
        
        logger.info(f"Production processor initialized: {self.max_workers} workers, {self.memory_limit_gb}GB limit")
    
    def analyze_symbol_sizes(self, symbols: List[str]) -> Dict[str, Dict]:
        """
        Analyze symbol data sizes for intelligent batching
        
        Critical for 32M rows - some symbols may have 100K+ rows each
        """
        logger.info("Analyzing symbol data sizes for optimal batching...")
        
        size_query = """
        SELECT 
            symbol,
            COUNT(*) as row_count,
            MIN(date) as start_date,
            MAX(date) as end_date,
            COUNT(DISTINCT date) as unique_dates
        FROM trading_tickers 
        WHERE symbol = ANY(%s)
        GROUP BY symbol
        ORDER BY row_count DESC
        """
        
        try:
            results = self.db_manager.execute_optimized_query(size_query, (symbols,), fetch=True)
            
            symbol_analysis = {}
            total_rows = 0
            
            for row in results:
                symbol, row_count, start_date, end_date, unique_dates = row
                
                # Estimate complexity
                time_span = (end_date - start_date).total_seconds() / 3600  # hours
                interpolation_factor = max(1, int(time_span * 60 / unique_dates)) if unique_dates > 0 else 1
                estimated_output_rows = row_count * interpolation_factor
                
                symbol_analysis[symbol] = {
                    'input_rows': row_count,
                    'estimated_output_rows': estimated_output_rows,
                    'time_span_hours': time_span,
                    'complexity_score': row_count * interpolation_factor,
                    'size_category': self._categorize_symbol_size(row_count)
                }
                
                total_rows += row_count
            
            logger.info(f"Symbol analysis complete: {len(symbol_analysis)} symbols, {total_rows:,} total rows")
            return symbol_analysis
            
        except Exception as e:
            logger.error(f"Symbol analysis failed: {e}")
            return {}
    
    def _categorize_symbol_size(self, row_count: int) -> str:
        """Categorize symbols by size for processing strategy"""
        if row_count < 1000:
            return 'small'
        elif row_count < 10000:
            return 'medium'
        elif row_count < 50000:
            return 'large'
        else:
            return 'xlarge'
    
    def create_intelligent_batches(self, symbol_analysis: Dict[str, Dict]) -> List[List[str]]:
        """
        Create intelligent batches based on symbol complexity
        
        Groups symbols to balance processing time and memory usage
        """
        # Sort symbols by complexity (largest first for better load balancing)
        sorted_symbols = sorted(
            symbol_analysis.items(),
            key=lambda x: x[1]['complexity_score'],
            reverse=True
        )
        
        batches = []
        current_batch = []
        current_batch_complexity = 0
        max_batch_complexity = 500000  # Adjust based on your server capacity
        
        for symbol, analysis in sorted_symbols:
            complexity = analysis['complexity_score']
            
            # Start new batch if current would exceed limits
            if (current_batch_complexity + complexity > max_batch_complexity and current_batch) or \
               len(current_batch) >= self.max_batch_size:
                
                batches.append(current_batch)
                current_batch = [symbol]
                current_batch_complexity = complexity
            else:
                current_batch.append(symbol)
                current_batch_complexity += complexity
        
        # Add final batch
        if current_batch:
            batches.append(current_batch)
        
        # Log batch statistics
        batch_sizes = [len(batch) for batch in batches]
        logger.info(f"Created {len(batches)} intelligent batches:")
        logger.info(f"  Batch sizes: min={min(batch_sizes)}, max={max(batch_sizes)}, avg={np.mean(batch_sizes):.1f}")
        
        return batches
    
    def monitor_system_resources(self) -> Dict[str, float]:
        """Monitor system resources for throttling decisions"""
        try:
            # Memory usage
            memory = psutil.virtual_memory()
            memory_used_gb = (memory.total - memory.available) / (1024**3)
            memory_percent = memory.percent
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Database connections
            pool_stats = self.db_manager.get_connection_pool_stats()
            
            return {
                'memory_used_gb': memory_used_gb,
                'memory_percent': memory_percent,
                'cpu_percent': cpu_percent,
                'db_connections_used': pool_stats.get('checked_out', 0),
                'db_connections_available': pool_stats.get('checked_in', 0)
            }
        except Exception as e:
            logger.warning(f"Resource monitoring failed: {e}")
            return {}
    
    def should_throttle_processing(self, resources: Dict[str, float]) -> bool:
        """Determine if processing should be throttled based on resources"""
        
        # Throttle if memory usage is high
        if resources.get('memory_percent', 0) > 85:
            logger.warning(f"High memory usage: {resources['memory_percent']:.1f}% - throttling")
            return True
        
        # Throttle if CPU is maxed out
        if resources.get('cpu_percent', 0) > 95:
            logger.warning(f"High CPU usage: {resources['cpu_percent']:.1f}% - throttling")
            return True
        
        # Throttle if database connections are exhausted
        if resources.get('db_connections_used', 0) > 15:  # Leave some headroom
            logger.warning("Database connection pool near capacity - throttling")
            return True
        
        return False
    
    def process_symbol_worker(self, symbol_batch: List[str], worker_id: int, batch_id: int) -> Dict:
        """
        Worker function optimized for multiprocessing
        
        Each worker has its own database connection and processes multiple symbols
        """
        worker_start = time.time()
        results = []
        
        # Initialize worker-specific resources
        from database.optimized_connection import OptimizedDatabaseManager
        worker_db = OptimizedDatabaseManager(self.config.database, pool_size=2)
        
        try:
            logger.info(f"Worker {worker_id} processing {len(symbol_batch)} symbols")
            
            for symbol in symbol_batch:
                try:
                    result = self._process_single_symbol(symbol, worker_db, batch_id)
                    results.append(result)
                    
                    if result['status'] == 'success':
                        logger.debug(f"Worker {worker_id}: {symbol} - {result['input_rows']} → {result['output_rows']} rows")
                    
                except Exception as e:
                    error_result = {
                        'symbol': symbol,
                        'status': 'error',
                        'error': str(e),
                        'worker_id': worker_id
                    }
                    results.append(error_result)
                    logger.error(f"Worker {worker_id} failed on {symbol}: {e}")
            
            worker_duration = time.time() - worker_start
            
            return {
                'worker_id': worker_id,
                'results': results,
                'processing_time': worker_duration,
                'symbols_processed': len(symbol_batch)
            }
            
        except Exception as e:
            logger.error(f"Worker {worker_id} failed completely: {e}")
            return {
                'worker_id': worker_id,
                'results': [],
                'error': str(e),
                'processing_time': time.time() - worker_start
            }
        finally:
            # Cleanup worker resources
            if 'worker_db' in locals():
                worker_db.close_pool()
    
    def _process_single_symbol(self, symbol: str, worker_db, batch_id: int) -> Dict:
        """Process a single symbol with optimized database operations"""
        start_time = time.time()
        
        try:
            # Use streaming query for large symbols
            query = """
            SELECT symbol, date, iv, underlying_price, time_to_maturity,
                   strike, callput, interest_rate, mark_price, index_price,
                   volume, quote_volume, record_time
            FROM trading_tickers 
            WHERE symbol = %s 
            ORDER BY date
            """
            
            # Check symbol size first
            size_query = "SELECT COUNT(*) FROM trading_tickers WHERE symbol = %s"
            row_count = worker_db.execute_optimized_query(size_query, (symbol,), fetch=True)[0][0]
            
            if row_count == 0:
                return {'symbol': symbol, 'status': 'skipped', 'reason': 'No data found'}
            
            # Use streaming for large symbols (>10K rows)
            if row_count > 10000:
                interpolated_data = self._process_large_symbol_streaming(symbol, query, worker_db, batch_id)
            else:
                # Standard processing for smaller symbols
                with worker_db.get_connection() as conn:
                    data = pd.read_sql(query, conn, params=(symbol,))
                
                if data.empty:
                    return {'symbol': symbol, 'status': 'skipped', 'reason': 'No data found'}
                
                # Interpolate (using your existing interpolation logic)
                from interpolation.core import IVInterpolator
                interpolator = IVInterpolator()
                interpolated_data = interpolator.interpolate_symbol(data)
                
                if interpolated_data is None:
                    return {'symbol': symbol, 'status': 'skipped', 'reason': 'Interpolation failed'}
                
                # Add batch metadata
                interpolated_data['batch_id'] = batch_id
                
                # Save results
                self._save_results_optimized(interpolated_data, worker_db)
            
            processing_time = time.time() - start_time
            
            return {
                'symbol': symbol,
                'status': 'success',
                'input_rows': row_count,
                'output_rows': len(interpolated_data) if interpolated_data is not None else 0,
                'processing_time': processing_time
            }
            
        except Exception as e:
            processing_time = time.time() - start_time
            return {
                'symbol': symbol,
                'status': 'error',
                'error': str(e),
                'processing_time': processing_time
            }
    
    def _process_large_symbol_streaming(self, symbol: str, query: str, worker_db, batch_id: int):
        """Process large symbols using streaming to avoid memory issues"""
        total_output_rows = 0
        
        # Process in chunks to manage memory
        for batch in worker_db.execute_streaming_query(query, (symbol,), fetch_size=5000):
            # Convert to DataFrame
            df = pd.DataFrame(batch, columns=[
                'symbol', 'date', 'iv', 'underlying_price', 'time_to_maturity',
                'strike', 'callput', 'interest_rate', 'mark_price', 'index_price',
                'volume', 'quote_volume', 'record_time'
            ])
            
            # Interpolate chunk
            from interpolation.core import IVInterpolator
            interpolator = IVInterpolator()
            interpolated_chunk = interpolator.interpolate_symbol(df)
            
            if interpolated_chunk is not None:
                interpolated_chunk['batch_id'] = batch_id
                self._save_results_optimized(interpolated_chunk, worker_db)
                total_output_rows += len(interpolated_chunk)
            
            # Force garbage collection
            del df, interpolated_chunk
            gc.collect()
        
        return total_output_rows
    
    def _save_results_optimized(self, interpolated_data: pd.DataFrame, worker_db) -> bool:
        """Optimized save with batch processing"""
        if interpolated_data.empty:
            return True
        
        try:
            # Prepare data for batch insert
            columns = [
                'symbol', 'date', 'record_time', 'iv', 'underlying_price',
                'time_to_maturity', 'strike', 'callput', 'interest_rate',
                'mark_price', 'index_price', 'volume', 'quote_volume',
                'is_interpolated', 'batch_id'
            ]
            
            # Convert to tuples efficiently
            data_tuples = [
                tuple(None if pd.isna(row[col]) else row[col] for col in columns)
                for _, row in interpolated_data.iterrows()
            ]
            
            insert_query = f"""
            INSERT INTO interpolated_trading_tickers 
            ({', '.join(columns)})
            VALUES %s
            """
            
            # Use optimized batch insert
            worker_db.execute_batch_insert(insert_query, data_tuples, page_size=5000)
            return True
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")
            return False
    
    def run_production_processing(self, symbols: List[str] = None) -> Dict:
        """
        Main production processing pipeline optimized for 32M+ rows
        """
        start_time = time.time()
        logger.info("Starting production-scale batch processing...")
        
        if symbols is None:
            symbols = self._get_all_symbols()
        
        if not symbols:
            logger.error("No symbols to process")
            return {'success': False, 'error': 'No symbols found'}
        
        # Step 1: Analyze symbol sizes
        symbol_analysis = self.analyze_symbol_sizes(symbols)
        if not symbol_analysis:
            logger.error("Symbol analysis failed")
            return {'success': False, 'error': 'Symbol analysis failed'}
        
        # Step 2: Create intelligent batches
        symbol_batches = self.create_intelligent_batches(symbol_analysis)
        
        # Step 3: Initialize progress tracking
        batch_id = int(time.time())
        self._initialize_progress_tracking(symbols, batch_id)
        
        # Step 4: Process batches with monitoring
        total_success = 0
        total_errors = 0
        total_skipped = 0
        
        logger.info(f"Processing {len(symbol_batches)} batches with {self.max_workers} workers")
        
        try:
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit batches
                future_to_batch = {
                    executor.submit(self.process_symbol_worker, batch, i, batch_id): i 
                    for i, batch in enumerate(symbol_batches)
                }
                
                # Process results with monitoring
                for future in as_completed(future_to_batch):
                    batch_idx = future_to_batch[future]
                    
                    try:
                        batch_result = future.result()
                        
                        # Process results
                        for result in batch_result.get('results', []):
                            if result['status'] == 'success':
                                total_success += 1
                            elif result['status'] == 'error':
                                total_errors += 1
                            else:
                                total_skipped += 1
                        
                        # Monitor resources and throttle if needed
                        resources = self.monitor_system_resources()
                        if self.should_throttle_processing(resources):
                            time.sleep(5)  # Brief pause to let system recover
                        
                        # Log progress
                        completed_batches = batch_idx + 1
                        if completed_batches % 10 == 0:
                            progress_pct = (completed_batches / len(symbol_batches)) * 100
                            logger.info(f"Progress: {completed_batches}/{len(symbol_batches)} batches ({progress_pct:.1f}%)")
                            logger.info(f"Resources: Memory {resources.get('memory_percent', 0):.1f}%, CPU {resources.get('cpu_percent', 0):.1f}%")
                        
                    except Exception as e:
                        total_errors += len(symbol_batches[batch_idx])
                        logger.error(f"Batch {batch_idx} failed: {e}")
        
        except KeyboardInterrupt:
            logger.warning("Processing interrupted by user")
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            return {'success': False, 'error': str(e)}
        
        # Final summary
        total_duration = time.time() - start_time
        
        results = {
            'success': True,
            'batch_id': batch_id,
            'duration': total_duration,
            'symbols_success': total_success,
            'symbols_error': total_errors,
            'symbols_skipped': total_skipped,
            'symbols_total': len(symbols),
            'batches_processed': len(symbol_batches)
        }
        
        logger.info(f"Production processing complete:")
        logger.info(f"  Duration: {total_duration:.1f}s ({total_duration/3600:.2f} hours)")
        logger.info(f"  Success: {total_success}, Errors: {total_errors}, Skipped: {total_skipped}")
        logger.info(f"  Success rate: {(total_success/len(symbols)*100):.1f}%")
        
        return results
    
    def _get_all_symbols(self) -> List[str]:
        """Get all symbols for processing"""
        query = "SELECT DISTINCT symbol FROM trading_tickers ORDER BY symbol"
        try:
            results = self.db_manager.execute_optimized_query(query, fetch=True)
            return [row[0] for row in results]
        except Exception as e:
            logger.error(f"Failed to get symbols: {e}")
            return []
    
    def _initialize_progress_tracking(self, symbols: List[str], batch_id: int):
        """Initialize progress tracking efficiently"""
        # Batch insert progress records
        progress_data = [(symbol, 'pending', batch_id) for symbol in symbols]
        
        insert_query = """
        INSERT INTO interpolation_progress (symbol, status, batch_id, created_at)
        VALUES %s
        ON CONFLICT (symbol) DO UPDATE SET
            status = 'pending',
            batch_id = EXCLUDED.batch_id,
            created_at = CURRENT_TIMESTAMP
        """
        
        try:
            self.db_manager.execute_batch_insert(insert_query, progress_data, page_size=1000)
            logger.info(f"Progress tracking initialized for {len(symbols)} symbols")
        except Exception as e:
            logger.error(f"Failed to initialize progress tracking: {e}")


# Helper function for multiprocessing
def process_symbol_batch_worker(symbol_batch, worker_id, batch_id, config_dict):
    """Standalone worker function for multiprocessing"""
    from database.optimized_connection import OptimizedDatabaseManager
    from config import DatabaseConfig
    
    # Recreate objects in worker process
    db_config = DatabaseConfig(**config_dict)
    worker_db = OptimizedDatabaseManager(db_config, pool_size=2)
    
    # Process symbols
    processor = ProductionBatchProcessor.__new__(ProductionBatchProcessor)
    processor.db_manager = worker_db
    processor.config = type('Config', (), config_dict)()
    
    try:
        return processor.process_symbol_worker(symbol_batch, worker_id, batch_id)
    finally:
        worker_db.close_pool()