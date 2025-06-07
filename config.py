import os
from dataclasses import dataclass
from typing import Optional
import logging
from dotenv import load_dotenv
load_dotenv



@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str = os.getenv('DB_HOST')
    database: str = os.getenv('DB_DATABASE')
    user: str = os.getenv('DB_USER')
    password: str = os.getenv('DB_PASSWORD')
    port: int = int(os.getenv('DB_PORT', 5432))
    
    def to_dict(self):
        return {
            'host': self.host,
            'database': self.database,
            'user': self.user,
            'password': self.password,
            'port': self.port
        }

@dataclass
class ProcessingConfig:
    """Processing configuration"""
    max_workers: int = 32
    symbols_per_batch: int = 100
    chunk_size: int = 50000
    memory_limit_gb: int = 16
    enable_logging: bool = True
    log_level: str = 'INFO'
    
@dataclass
class InterpolationConfig:
    """Interpolation specific settings"""
    frequency: str = '1min'  # Target frequency
    method: str = 'linear'   # Interpolation method
    max_gap_hours: int = 48  # Skip symbols with gaps > this
    min_data_points: int = 10  # Minimum points needed
    
@dataclass
class Config:
    """Main configuration object"""
    database: DatabaseConfig
    processing: ProcessingConfig
    interpolation: InterpolationConfig
    
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
        debug=debug
    )
