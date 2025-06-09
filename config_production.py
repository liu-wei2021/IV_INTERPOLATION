# config_production.py - Optimized for 32M+ rows
import os
import psutil
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class ProductionDatabaseConfig:
    """Database configuration optimized for high-volume processing"""
    host: str = os.getenv('DB_HOST', 'localhost')
    database: str = os.getenv('DB_DATABASE', 'trading_data')
    user: str = os.getenv('DB_USER', 'postgres')
    password: str = os.getenv('DB_PASSWORD', '')
    port: int = int(os.getenv('DB_PORT', '5432'))
    
    # Production connection pool settings
    pool_size: int = int(os.getenv('DB_POOL_SIZE', '20'))
    max_overflow: int = int(os.getenv('DB_MAX_OVERFLOW', '40'))
    pool_timeout: int = int(os.getenv('DB_POOL_TIMEOUT', '30'))
    
    def to_dict(self):
        """Convert to dictionary for psycopg2"""
        return {
            'host': self.host,
            'database': self.database,
            'user': self.user,
            'password': self.password,
            'port': self.port
        }

@dataclass
class ProductionProcessingConfig:
    """Processing configuration optimized for 32M+ rows"""
    
    # Auto-calculate optimal settings based on system resources
    def __post_init__(self):
        # Get system resources
        cpu_count = psutil.cpu_count()
        memory_gb = psutil.virtual_memory().total / (1024**3)
        
        # Auto-calculate workers (leave 2 cores for system)
        self.max_workers = max(2, min(cpu_count - 2, int(os.getenv('MAX_WORKERS', str(cpu_count // 2)))))
        
        # Auto-calculate memory limits (leave 4GB for system)
        self.memory_limit_gb = max(8, min(memory_gb - 4, int(os.getenv('MEMORY_LIMIT_GB', str(int(memory_gb * 0.8))))))
        
        # Dynamic batch sizing based on available memory
        if memory_gb >= 32:
            self.symbols_per_batch = 500
            self.chunk_size = 100000
        elif memory_gb >= 16:
            self.symbols_per_batch = 200
            self.chunk_size = 50000
        else:
            self.symbols_per_batch = 100
            self.chunk_size = 25000
    
    # Processing performance settings
    max_workers: int = 0  # Will be set in __post_init__
    symbols_per_batch: int = 0  # Will be set in __post_init__
    chunk_size: int = 0  # Will be set in __post_init__
    memory_limit_gb: int = 0  # Will be set in __post_init__
    
    # Database operation settings
    db_batch_size: int = int(os.getenv('DB_BATCH_SIZE', '10000'))
    db_page_size: int = int(os.getenv('DB_PAGE_SIZE', '5000'))
    
    # Monitoring and logging
    enable_logging: bool = True
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')
    enable_performance_monitoring: bool = True
    enable_resource_monitoring: bool = True
    
    # Checkpointing and recovery
    checkpoint_interval: int = int(os.getenv('CHECKPOINT_INTERVAL', '100'))
    enable_auto_recovery: bool = True
    max_retries: int = int(os.getenv('MAX_RETRIES', '3'))
    
    # Memory management
    enable_gc_optimization: bool = True
    gc_threshold: float = 0.8  # Trigger GC at 80% memory usage
    streaming_threshold: int = int(os.getenv('STREAMING_THRESHOLD', '10000'))  # Use streaming for symbols with >10K rows

@dataclass
class ProductionInterpolationConfig:
    """IV interpolation settings optimized for production"""
    frequency: str = '1min'
    method: str = 'linear'  # Linear is fastest and most stable
    max_gap_hours: int = 72  # Increased for production data
    min_data_points: int = 5  # Reduced for efficiency
    extrapolate: bool = False
    preserve_greeks: bool = False  # Disabled for performance
    
    # Production optimizations
    enable_data_validation: bool = True
    skip_invalid_symbols: bool = True
    max_interpolation_points: int = int(os.getenv('MAX_INTERP_POINTS', '100000'))

@dataclass
class ProductionMonitoringConfig:
    """Monitoring and alerting configuration"""
    enable_real_time_monitoring: bool = True
    monitoring_interval: int = 30  # seconds
    
    # Resource thresholds for alerting
    memory_alert_threshold: float = 0.85  # 85%
    cpu_alert_threshold: float = 0.90  # 90%
    disk_alert_threshold: float = 0.90  # 90%
    
    # Performance thresholds
    symbol_processing_timeout: int = 300  # 5 minutes per symbol
    batch_processing_timeout: int = 3600  # 1 hour per batch
    
    # Metrics collection
    collect_detailed_metrics: bool = True
    metrics_retention_days: int = 30

@dataclass
class ProductionConfig:
    """Main production configuration optimized for 32M+ rows"""
    database: ProductionDatabaseConfig
    processing: ProductionProcessingConfig
    interpolation: ProductionInterpolationConfig
    monitoring: ProductionMonitoringConfig
    
    # Environment settings
    environment: str = os.getenv('ENVIRONMENT', 'production')
    debug: bool = os.getenv('DEBUG', 'false').lower() == 'true'
    
    # Paths
    output_dir: str = os.getenv('OUTPUT_DIR', './production_output')
    log_dir: str = os.getenv('LOG_DIR', './production_logs')
    checkpoint_dir: str = os.getenv('CHECKPOINT_DIR', './checkpoints')
    
    # Production features
    enable_parallel_processing: bool = True
    enable_connection_pooling: bool = True
    enable_intelligent_batching: bool = True
    enable_memory_optimization: bool = True
    enable_progress_checkpointing: bool = True
    
    def print_configuration(self):
        """Print configuration summary for verification"""
        print(f"\n🚀 PRODUCTION CONFIGURATION SUMMARY")
        print(f"{'='*50}")
        print(f"Environment: {self.environment}")
        print(f"Max Workers: {self.processing.max_workers}")
        print(f"Memory Limit: {self.processing.memory_limit_gb}GB")
        print(f"Symbols per Batch: {self.processing.symbols_per_batch}")
        print(f"Chunk Size: {self.processing.chunk_size:,}")
        print(f"DB Pool Size: {self.database.pool_size}")
        print(f"Streaming Threshold: {self.processing.streaming_threshold:,} rows")
        print(f"{'='*50}\n")

# Production configuration factory
def get_production_config() -> ProductionConfig:
    """Get production configuration with auto-optimization"""
    
    # Detect environment and adjust settings
    env = os.getenv('ENVIRONMENT', 'production')
    
    if env == 'development':
        config = ProductionConfig(
            database=ProductionDatabaseConfig(pool_size=5, max_overflow=10),
            processing=ProductionProcessingConfig(),
            interpolation=ProductionInterpolationConfig(),
            monitoring=ProductionMonitoringConfig()
        )
        # Override for development
        config.processing.max_workers = 2
        config.processing.symbols_per_batch = 10
        config.processing.chunk_size = 1000
        
    elif env == 'testing':
        config = ProductionConfig(
            database=ProductionDatabaseConfig(pool_size=10, max_overflow=20),
            processing=ProductionProcessingConfig(),
            interpolation=ProductionInterpolationConfig(),
            monitoring=ProductionMonitoringConfig()
        )
        # Override for testing
        config.processing.max_workers = 4
        config.processing.symbols_per_batch = 50
        config.processing.chunk_size = 10000
        
    else:  # production
        config = ProductionConfig(
            database=ProductionDatabaseConfig(),
            processing=ProductionProcessingConfig(),
            interpolation=ProductionInterpolationConfig(),
            monitoring=ProductionMonitoringConfig()
        )
    
    # Ensure directories exist
    import pathlib
    pathlib.Path(config.output_dir).mkdir(exist_ok=True)
    pathlib.Path(config.log_dir).mkdir(exist_ok=True)
    pathlib.Path(config.checkpoint_dir).mkdir(exist_ok=True)
    
    return config

# Performance estimation functions
def estimate_processing_time(total_rows: int, config: ProductionConfig) -> dict:
    """Estimate processing time for given number of rows"""
    
    # Base processing rates (rows per second) - adjust based on your testing
    base_rate_per_worker = 1000  # rows/second per worker (conservative estimate)
    
    # Calculate effective processing rate
    effective_workers = min(config.processing.max_workers, 
                          max(1, total_rows // config.processing.symbols_per_batch))
    
    total_rate = base_rate_per_worker * effective_workers
    
    # Account for overhead (database I/O, memory management, etc.)
    overhead_factor = 1.5
    adjusted_rate = total_rate / overhead_factor
    
    # Estimate times
    processing_seconds = total_rows / adjusted_rate
    processing_hours = processing_seconds / 3600
    
    # Memory estimation
    avg_row_size_bytes = 500  # Estimate based on your data
    peak_memory_gb = (total_rows * avg_row_size_bytes * 2) / (1024**3)  # 2x for processing overhead
    
    return {
        'total_rows': total_rows,
        'estimated_processing_hours': round(processing_hours, 2),
        'estimated_processing_days': round(processing_hours / 24, 2),
        'effective_workers': effective_workers,
        'processing_rate_per_second': int(adjusted_rate),
        'peak_memory_gb_estimate': round(peak_memory_gb, 2),
        'recommended_memory_gb': round(peak_memory_gb * 1.5, 2)  # 50% buffer
    }

def print_performance_estimates(total_rows: int = 32_000_000):
    """Print performance estimates for 32M rows"""
    config = get_production_config()
    estimates = estimate_processing_time(total_rows, config)
    
    print(f"\n📊 PERFORMANCE ESTIMATES FOR {total_rows:,} ROWS")
    print(f"{'='*50}")
    print(f"Estimated Processing Time: {estimates['estimated_processing_hours']:.1f} hours ({estimates['estimated_processing_days']:.1f} days)")
    print(f"Effective Workers: {estimates['effective_workers']}")
    print(f"Processing Rate: {estimates['processing_rate_per_second']:,} rows/second")
    print(f"Peak Memory Usage: {estimates['peak_memory_gb_estimate']:.1f}GB")
    print(f"Recommended RAM: {estimates['recommended_memory_gb']:.1f}GB")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    # Test configuration
    config = get_production_config()
    config.print_configuration()
    print_performance_estimates()