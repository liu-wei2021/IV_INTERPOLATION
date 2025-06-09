# config.py
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str = os.getenv('DB_HOST', 'localhost')
    database: str = os.getenv('DB_DATABASE', 'trading_data')
    user: str = os.getenv('DB_USER', 'postgres')
    password: str = os.getenv('DB_PASSWORD', '')
    port: int = int(os.getenv('DB_PORT', '5432'))
    
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
class ProcessingConfig:
    """Processing and performance configuration"""
    max_workers: int = 32               # Parallel processing workers
    symbols_per_batch: int = 100        # Symbols processed per batch
    chunk_size: int = 50000            # Database operation chunk size
    memory_limit_gb: int = 16          # Memory usage limit
    enable_logging: bool = True        # Enable detailed logging
    log_level: str = 'INFO'           # DEBUG, INFO, WARNING, ERROR

@dataclass
class InterpolationConfig:
    """IV interpolation specific settings"""
    frequency: str = '1min'            # Target frequency for interpolation
    method: str = 'linear'             # Interpolation method
    max_gap_hours: int = 48           # Skip symbols with gaps larger than this
    min_data_points: int = 10         # Minimum data points required
    extrapolate: bool = False         # Allow extrapolation beyond data range
    preserve_greeks: bool = True      # Recalculate Greeks after interpolation

@dataclass
class CandleReconstructionConfig:
    """Candle reconstruction specific settings"""
    target_frequency: str = '5min'      # Target frequency (5min, 15min, 1h, etc.)
    source_frequency: str = '1min'      # Source frequency 
    min_candles_required: int = 5       # Minimum candles needed for reconstruction
    validate_ohlc: bool = True          # Validate OHLC relationships
    batch_size: int = 1000             # Candles per batch for DB operations

@dataclass
class DataBridgeConfig:
    """Data bridge configuration for connecting Task 1 and Task 2"""
    conversion_strategy: str = 'spread_simulation'    # 'spread_simulation', 'price_midpoint', 'trend_following'
    spread_method: str = 'adaptive'                   # 'fixed', 'adaptive', 'volatility_based'
    enable_quality_checks: bool = True                # Enable OHLCV quality validation
    
    # Spread parameters for synthetic OHLCV generation
    spread_parameters: dict = None
    
    def __post_init__(self):
        if self.spread_parameters is None:
            self.spread_parameters = {
                'base_spread_percent': 0.002,    # 0.2% base spread
                'volatility_factor': 1.5,        # Volatility multiplier
                'min_spread_percent': 0.0005,    # Minimum 0.05% spread
                'max_spread_percent': 0.02,      # Maximum 2% spread
                'trend_strength': 0.6            # Trend following strength
            }

@dataclass
class Config:
    """Main configuration object"""
    database: DatabaseConfig
    processing: ProcessingConfig
    interpolation: InterpolationConfig
    candle_reconstruction: CandleReconstructionConfig
    data_bridge: DataBridgeConfig
    
    # Paths
    output_dir: str = "./interpolated_data"
    log_dir: str = "./logs"
    
    # Environment
    environment: str = "production"  # development, testing, production
    debug: bool = False

# Default configuration
def get_config() -> Config:
    """Get configuration based on environment"""
    env = os.getenv('ENVIRONMENT', 'production')
    
    # Adjust settings based on environment
    if env == 'development':
        processing_config = ProcessingConfig(
            max_workers=4,
            symbols_per_batch=10,
            chunk_size=1000,
            enable_logging=True,
            log_level='DEBUG'
        )
        debug = True
    elif env == 'testing':
        processing_config = ProcessingConfig(
            max_workers=8,
            symbols_per_batch=25,
            chunk_size=10000,
            enable_logging=True,
            log_level='INFO'
        )
        debug = False
    else:  # production
        processing_config = ProcessingConfig(
            max_workers=32,
            symbols_per_batch=100,
            chunk_size=50000,
            enable_logging=True,
            log_level='INFO'
        )
        debug = False
    
    return Config(
        database=DatabaseConfig(),
        processing=processing_config,
        interpolation=InterpolationConfig(),
        candle_reconstruction=CandleReconstructionConfig(),
        data_bridge=DataBridgeConfig(),
        debug=debug,
        environment=env
    )