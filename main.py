#!/usr/bin/env python3
"""
Main execution script for IV Interpolation Pipeline
Usage: python main.py [--test] [--resume] [--monitor]
"""

import argparse
import sys
import os
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from config import get_config
from database.connection import DatabaseManager
from database.schema import SchemaManager
from interpolation.core import IVInterpolator
from monitoring.logging import setup_logging
from monitoring.progress import ProgressTracker

def setup_environment():
    """Setup logging and directories"""
    config = get_config()
    
    # Create directories
    Path(config.output_dir).mkdir(exist_ok=True)
    Path(config.log_dir).mkdir(exist_ok=True)
    
    # Setup logging
    setup_logging(config.log_dir, config.processing.log_level)
    
    return config

def test_run():
    """Run test with small dataset"""
    print("🧪 RUNNING TEST MODE")
    config = get_config()
    
    # Test with 1 day of data
    start_date = '2023-03-20 00:00:00'
    end_date = '2023-03-20 23:59:59'
    
    # TODO: Implement test run logic
    print(f"   Testing with date range: {start_date} to {end_date}")

def production_run():
    """Run full production pipeline"""
    print("🚀 RUNNING PRODUCTION MODE")
    config = get_config()
    
    # Setup database
    db_manager = DatabaseManager(config.database)
    schema_manager = SchemaManager(db_manager)
    
    # Create tables
    if not schema_manager.create_interpolated_table():
        print("❌ Failed to create tables")
        return False
    
    if not schema_manager.create_progress_table():
        print("❌ Failed to create progress table")
        return False
    
    # TODO: Implement full production pipeline
    print("   Full production pipeline will run here...")
    return True

def monitor_progress():
    """Monitor current progress"""
    print("📊 MONITORING PROGRESS")
    # TODO: Implement progress monitoring
    
def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='IV Interpolation Pipeline')
    parser.add_argument('--test', action='store_true', help='Run in test mode')
    parser.add_argument('--resume', action='store_true', help='Resume failed processing')
    parser.add_argument('--monitor', action='store_true', help='Monitor progress')
    parser.add_argument('--env', choices=['development', 'testing', 'production'], 
                       default='production', help='Environment')
    
    args = parser.parse_args()
    
    # Set environment
    os.environ['ENVIRONMENT'] = args.env
    
    # Setup
    config = setup_environment()
    
    try:
        if args.monitor:
            monitor_progress()
        elif args.test:
            test_run()
        elif args.resume:
            print("🔄 RESUMING PROCESSING")
            # TODO: Implement resume logic
        else:
            production_run()
    
    except KeyboardInterrupt:
        print("\n⚠️ Processing interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()