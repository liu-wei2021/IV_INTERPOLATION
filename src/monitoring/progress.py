# src/monitoring/progress.py
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from database.connection import DatabaseManager

logger = logging.getLogger(__name__)

class ProgressTracker:
    """Track and monitor interpolation progress"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.batch_id = self._generate_batch_id()
        self.start_time = time.time()
    
    def _generate_batch_id(self) -> int:
        """Generate unique batch ID based on timestamp"""
        return int(time.time())
    
    def initialize_symbols(self, symbols: List[str]) -> bool:
        """Initialize progress tracking for a batch of symbols"""
        try:
            # Insert all symbols with pending status
            insert_query = """
            INSERT INTO interpolation_progress 
            (symbol, status, batch_id, created_at)
            VALUES (%s, 'pending', %s, CURRENT_TIMESTAMP)
            ON CONFLICT (symbol) DO UPDATE SET
                status = 'pending',
                batch_id = EXCLUDED.batch_id,
                created_at = CURRENT_TIMESTAMP,
                started_at = NULL,
                completed_at = NULL,
                error_message = NULL
            """
            
            for symbol in symbols:
                self.db_manager.execute_query(insert_query, (symbol, self.batch_id))
            
            logger.info(f"Initialized progress tracking for {len(symbols)} symbols (Batch ID: {self.batch_id})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize progress tracking: {e}")
            return False
    
    def start_symbol(self, symbol: str):
        """Mark symbol processing as started"""
        update_query = """
        UPDATE interpolation_progress 
        SET status = 'processing', started_at = CURRENT_TIMESTAMP
        WHERE symbol = %s AND batch_id = %s
        """
        try:
            self.db_manager.execute_query(update_query, (symbol, self.batch_id))
        except Exception as e:
            logger.error(f"Failed to update start status for {symbol}: {e}")
    
    def complete_symbol(self, symbol: str, input_rows: int, output_rows: int, processing_time: float):
        """Mark symbol processing as completed successfully"""
        update_query = """
        UPDATE interpolation_progress 
        SET status = 'completed',
            input_rows = %s,
            output_rows = %s,
            processing_time_seconds = %s,
            completed_at = CURRENT_TIMESTAMP
        WHERE symbol = %s AND batch_id = %s
        """
        try:
            self.db_manager.execute_query(
                update_query, 
                (input_rows, output_rows, processing_time, symbol, self.batch_id)
            )
        except Exception as e:
            logger.error(f"Failed to update completion status for {symbol}: {e}")
    
    def error_symbol(self, symbol: str, error_message: str):
        """Mark symbol processing as failed"""
        update_query = """
        UPDATE interpolation_progress 
        SET status = 'error',
            error_message = %s,
            completed_at = CURRENT_TIMESTAMP
        WHERE symbol = %s AND batch_id = %s
        """
        try:
            self.db_manager.execute_query(update_query, (error_message, symbol, self.batch_id))
        except Exception as e:
            logger.error(f"Failed to update error status for {symbol}: {e}")
    
    def skip_symbol(self, symbol: str, reason: str):
        """Mark symbol as skipped"""
        update_query = """
        UPDATE interpolation_progress 
        SET status = 'skipped',
            error_message = %s,
            completed_at = CURRENT_TIMESTAMP
        WHERE symbol = %s AND batch_id = %s
        """
        try:
            self.db_manager.execute_query(update_query, (reason, symbol, self.batch_id))
        except Exception as e:
            logger.error(f"Failed to update skip status for {symbol}: {e}")
    
    def get_progress_summary(self, batch_id: Optional[int] = None) -> Dict:
        """Get progress summary for current or specified batch"""
        if batch_id is None:
            batch_id = self.batch_id
            
        query = """
        SELECT 
            status,
            COUNT(*) as count,
            COALESCE(SUM(input_rows), 0) as total_input_rows,
            COALESCE(SUM(output_rows), 0) as total_output_rows,
            COALESCE(SUM(processing_time_seconds), 0) as total_processing_time
        FROM interpolation_progress 
        WHERE batch_id = %s
        GROUP BY status
        ORDER BY status
        """
        
        try:
            results = self.db_manager.execute_query(query, (batch_id,), fetch=True)
            
            summary = {
                'batch_id': batch_id,
                'statuses': {},
                'totals': {
                    'symbols': 0,
                    'input_rows': 0,
                    'output_rows': 0,
                    'processing_time': 0.0
                }
            }
            
            for row in results:  # type: ignore
                status, count, input_rows, output_rows, proc_time = row
                summary['statuses'][status] = {
                    'count': count,
                    'input_rows': int(input_rows),
                    'output_rows': int(output_rows),
                    'processing_time': float(proc_time)
                }
                summary['totals']['symbols'] += count
                summary['totals']['input_rows'] += int(input_rows)
                summary['totals']['output_rows'] += int(output_rows)
                summary['totals']['processing_time'] += float(proc_time)
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get progress summary: {e}")
            return {}
    
    def get_failed_symbols(self, batch_id: Optional[int] = None) -> List[str]:
        """Get list of failed symbols for retry"""
        if batch_id is None:
            batch_id = self.batch_id
            
        query = """
        SELECT symbol FROM interpolation_progress 
        WHERE batch_id = %s AND status = 'error'
        ORDER BY symbol
        """
        
        try:
            results = self.db_manager.execute_query(query, (batch_id,), fetch=True)
            return [row[0] for row in results] # type: ignore
        except Exception as e:
            logger.error(f"Failed to get failed symbols: {e}")
            return []
    
    def print_progress_report(self):
        """Print detailed progress report"""
        summary = self.get_progress_summary()
        if not summary:
            print("❌ Unable to retrieve progress information")
            return
        
        elapsed_time = time.time() - self.start_time
        
        print(f"\n📊 INTERPOLATION PROGRESS REPORT")
        print(f"{'='*50}")
        print(f"Batch ID: {summary['batch_id']}")
        print(f"Elapsed Time: {timedelta(seconds=int(elapsed_time))}")
        print(f"Total Symbols: {summary['totals']['symbols']}")
        print()
        
        # Status breakdown
        print("Status Breakdown:")
        for status, data in summary['statuses'].items():
            percentage = (data['count'] / summary['totals']['symbols']) * 100 if summary['totals']['symbols'] > 0 else 0
            print(f"  {status.upper():>12}: {data['count']:>6} symbols ({percentage:5.1f}%)")
        
        print()
        
        # Data summary
        if summary['totals']['input_rows'] > 0:
            compression_ratio = summary['totals']['output_rows'] / summary['totals']['input_rows']
            print(f"Data Summary:")
            print(f"  Input Rows:  {summary['totals']['input_rows']:>12,}")
            print(f"  Output Rows: {summary['totals']['output_rows']:>12,}")
            print(f"  Expansion:   {compression_ratio:>12.2f}x")
        
        # Performance
        if summary['totals']['processing_time'] > 0:
            print(f"\nPerformance:")
            print(f"  Total Processing Time: {summary['totals']['processing_time']:>8.1f}s")
            avg_time = summary['totals']['processing_time'] / max(summary['statuses'].get('completed', {}).get('count', 1), 1)
            print(f"  Avg Time per Symbol:   {avg_time:>8.2f}s")
        
        print(f"{'='*50}\n")


class RealtimeMonitor:
    """Real-time monitoring of interpolation progress"""
    
    def __init__(self, db_manager: DatabaseManager, refresh_interval: int = 30):
        self.db_manager = db_manager
        self.refresh_interval = refresh_interval
        self.tracker = ProgressTracker(db_manager)
    
    def monitor_latest_batch(self):
        """Monitor the most recent batch"""
        # Get latest batch ID
        query = "SELECT MAX(batch_id) FROM interpolation_progress"
        result = self.db_manager.execute_query(query, fetch=True)
        
        if not result or not result[0][0]:
            print("No active batches found")
            return
        
        latest_batch_id = result[0][0]
        print(f"Monitoring batch {latest_batch_id} (Refresh every {self.refresh_interval}s)")
        print("Press Ctrl+C to stop monitoring\n")
        
        try:
            while True:
                summary = self.tracker.get_progress_summary(latest_batch_id)
                if summary:
                    # Clear screen (works on most terminals)
                    print("\033[2J\033[H")
                    
                    # Print timestamp
                    print(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    # Print summary using tracker method
                    temp_tracker = ProgressTracker(self.db_manager)
                    temp_tracker.batch_id = latest_batch_id
                    temp_tracker.print_progress_report()
                    
                    # Check if processing is complete
                    pending_count = summary['statuses'].get('pending', {}).get('count', 0)
                    processing_count = summary['statuses'].get('processing', {}).get('count', 0)
                    
                    if pending_count == 0 and processing_count == 0:
                        print("✅ Batch processing completed!")
                        break
                
                time.sleep(self.refresh_interval)
                
        except KeyboardInterrupt:
            print("\n⏹️  Monitoring stopped by user")