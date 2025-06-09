# src/monitoring/logging.py
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from datetime import datetime

def setup_logging(log_dir: str, log_level: str = 'INFO'):
    """Setup comprehensive logging system"""
    
    # Create log directory
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    simple_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    root_logger.addHandler(console_handler)
    
    # Main log file (rotating)
    main_log_file = log_path / f"iv_interpolation_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = RotatingFileHandler(
        main_log_file,
        maxBytes=100*1024*1024,  # 100MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(file_handler)
    
    # Error log file
    error_log_file = log_path / f"errors_{datetime.now().strftime('%Y%m%d')}.log"
    error_handler = RotatingFileHandler(
        error_log_file,
        maxBytes=50*1024*1024,  # 50MB
        backupCount=3
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(error_handler)
    
    # Performance log for batch processing
    perf_logger = logging.getLogger('performance')
    perf_log_file = log_path / f"performance_{datetime.now().strftime('%Y%m%d')}.log"
    perf_handler = RotatingFileHandler(
        perf_log_file,
        maxBytes=50*1024*1024,
        backupCount=3
    )
    perf_handler.setFormatter(detailed_formatter)
    perf_logger.addHandler(perf_handler)
    perf_logger.setLevel(logging.INFO)
    
    logging.info(f"Logging initialized - Level: {log_level}")
    logging.info(f"Log directory: {log_path.absolute()}")
    
    return root_logger


class PerformanceLogger:
    """Helper class for logging performance metrics"""
    
    def __init__(self):
        self.logger = logging.getLogger('performance')
    
    def log_batch_start(self, batch_id: int, symbol_count: int):
        """Log batch processing start"""
        self.logger.info(f"BATCH_START | ID: {batch_id} | Symbols: {symbol_count}")
    
    def log_batch_complete(self, batch_id: int, duration: float, success_count: int, error_count: int):
        """Log batch processing completion"""
        self.logger.info(
            f"BATCH_COMPLETE | ID: {batch_id} | Duration: {duration:.2f}s | "
            f"Success: {success_count} | Errors: {error_count}"
        )
    
    def log_symbol_processing(self, symbol: str, input_rows: int, output_rows: int, duration: float):
        """Log individual symbol processing"""
        self.logger.info(
            f"SYMBOL_PROCESSED | {symbol} | Input: {input_rows} | "
            f"Output: {output_rows} | Duration: {duration:.3f}s"
        )
    
    def log_database_operation(self, operation: str, rows_affected: int, duration: float):
        """Log database operations"""
        self.logger.info(
            f"DB_OPERATION | {operation} | Rows: {rows_affected} | Duration: {duration:.3f}s"
        )