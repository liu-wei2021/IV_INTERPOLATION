# production_main.py - Optimized for 32M+ rows
#!/usr/bin/env python3
"""
Production-Optimized Main Pipeline for 32M+ Row Processing

Key Optimizations Implemented:
1. Connection pooling with 20-40 database connections
2. Intelligent symbol batching based on data size analysis
3. Memory-aware processing with auto-scaling
4. Real-time resource monitoring and throttling
5. Progress checkpointing for recovery
6. Streaming processing for large symbols (>10K rows)
7. Garbage collection optimization
8. Performance metrics and alerting

Usage:
  python production_main.py                     # Run production pipeline
  python production_main.py --monitor           # Run with real-time monitoring
  python production_main.py --estimate          # Show performance estimates
  python production_main.py --resume BATCH_ID   # Resume failed batch
"""

import argparse
import sys
import os
import time
from pathlib import Path
from typing import Optional
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from config_production import get_production_config, print_performance_estimates
from database.optimized_connection import OptimizedDatabaseManager
from interpolation.optimized_batch_processor import ProductionBatchProcessor
from performance_monitor import ProductionPerformanceMonitor


class ProductionPipelineManager:
    """
    Production pipeline manager optimized for 32M+ rows
    
    Handles:
    - Resource pre-allocation and validation
    - Intelligent processing planning
    - Real-time monitoring integration  
    - Error recovery and resumption
    - Performance optimization
    """
    
    def __init__(self, config):
        self.config = config
        self.db_manager = OptimizedDatabaseManager(
            config.database, 
            pool_size=config.database.pool_size,
            max_overflow=config.database.max_overflow
        )
        self.processor = ProductionBatchProcessor(self.db_manager, config)
        self.monitor = ProductionPerformanceMonitor(config, self.db_manager) if config.monitoring.enable_real_time_monitoring else None
        
    def validate_production_readiness(self) -> dict:
        """Validate system readiness for production-scale processing"""
        
        validation_results = {
            'ready': True,
            'warnings': [],
            'errors': [],
            'system_info': {}
        }
        
        try:
            import psutil
            
            # System resource validation
            memory_gb = psutil.virtual_memory().total / (1024**3)
            cpu_count = psutil.cpu_count()
            
            validation_results['system_info'] = {
                'memory_gb': memory_gb,
                'cpu_count': cpu_count,
                'recommended_workers': self.config.processing.max_workers,
                'recommended_memory_gb': self.config.processing.memory_limit_gb
            }
            
            # Memory validation
            if memory_gb < 16:
                validation_results['errors'].append(f"Insufficient RAM: {memory_gb:.1f}GB (minimum 16GB recommended)")
                validation_results['ready'] = False
            elif memory_gb < 32:
                validation_results['warnings'].append(f"Limited RAM: {memory_gb:.1f}GB (32GB+ recommended for 32M rows)")
            
            # CPU validation
            if cpu_count < 4:
                validation_results['errors'].append(f"Insufficient CPU cores: {cpu_count} (minimum 4 recommended)")
                validation_results['ready'] = False
            
            # Database validation
            try:
                with self.db_manager.get_connection() as conn:
                    with conn.cursor() as cur:
                        # Check database configuration
                        cur.execute("SHOW shared_buffers")
                        shared_buffers = cur.fetchone()[0]
                        
                        cur.execute("SHOW work_mem")
                        work_mem = cur.fetchone()[0]
                        
                        # Check if tables exist
                        cur.execute("""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_name IN ('trading_tickers', 'interpolated_trading_tickers')
                        """)
                        tables_exist = cur.fetchone()[0] == 2
                        
                        if not tables_exist:
                            validation_results['errors'].append("Required database tables not found")
                            validation_results['ready'] = False
                        
                        validation_results['system_info']['database'] = {
                            'shared_buffers': shared_buffers,
                            'work_mem': work_mem,
                            'tables_exist': tables_exist
                        }
                        
            except Exception as e:
                validation_results['errors'].append(f"Database validation failed: {e}")
                validation_results['ready'] = False
            
            # Data volume estimation
            try:
                count_query = "SELECT COUNT(*) FROM trading_tickers"
                total_rows = self.db_manager.execute_optimized_query(count_query, fetch=True)[0][0]
                
                symbol_count_query = "SELECT COUNT(DISTINCT symbol) FROM trading_tickers"
                symbol_count = self.db_manager.execute_optimized_query(symbol_count_query, fetch=True)[0][0]
                
                validation_results['system_info']['data'] = {
                    'total_rows': total_rows,
                    'symbol_count': symbol_count,
                    'avg_rows_per_symbol': total_rows // symbol_count if symbol_count > 0 else 0
                }
                
                # Performance estimation
                from config_production import estimate_processing_time
                estimates = estimate_processing_time(total_rows, self.config)
                validation_results['system_info']['estimates'] = estimates
                
                # Warn if processing time is very long
                if estimates['estimated_processing_hours'] > 48:
                    validation_results['warnings'].append(
                        f"Estimated processing time: {estimates['estimated_processing_hours']:.1f} hours"
                    )
                
            except Exception as e:
                validation_results['warnings'].append(f"Could not estimate data volume: {e}")
            
        except Exception as e:
            validation_results['errors'].append(f"System validation failed: {e}")
            validation_results['ready'] = False
        
        return validation_results
    
    def print_validation_report(self, validation: dict):
        """Print system validation report"""
        
        print(f"\n🔍 PRODUCTION READINESS VALIDATION")
        print(f"{'='*60}")
        
        # System info
        sys_info = validation['system_info']
        print(f"💻 System Resources:")
        print(f"   RAM: {sys_info.get('memory_gb', 0):.1f}GB")
        print(f"   CPU: {sys_info.get('cpu_count', 0)} cores")
        print(f"   Workers: {sys_info.get('recommended_workers', 0)}")
        
        # Data info
        if 'data' in sys_info:
            data_info = sys_info['data']
            print(f"\n📊 Data Volume:")
            print(f"   Total Rows: {data_info.get('total_rows', 0):,}")
            print(f"   Symbols: {data_info.get('symbol_count', 0):,}")
            print(f"   Avg Rows/Symbol: {data_info.get('avg_rows_per_symbol', 0):,}")
        
        # Performance estimates
        if 'estimates' in sys_info:
            estimates = sys_info['estimates']
            print(f"\n⏱️  Performance Estimates:")
            print(f"   Processing Time: {estimates.get('estimated_processing_hours', 0):.1f} hours")
            print(f"   Throughput: {estimates.get('processing_rate_per_second', 0):,} rows/second")
            print(f"   Peak Memory: {estimates.get('peak_memory_gb_estimate', 0):.1f}GB")
        
        # Warnings
        if validation['warnings']:
            print(f"\n⚠️  Warnings:")
            for warning in validation['warnings']:
                print(f"   - {warning}")
        
        # Errors
        if validation['errors']:
            print(f"\n❌ Errors:")
            for error in validation['errors']:
                print(f"   - {error}")
        
        # Status
        status = "✅ READY" if validation['ready'] else "❌ NOT READY"
        print(f"\n🎯 Status: {status}")
        print(f"{'='*60}\n")
    
    def run_production_pipeline(self, monitor: bool = True, resume_batch_id: Optional[int] = None) -> dict:
        """Run the production pipeline with all optimizations"""
        
        start_time = time.time()
        
        print(f"🚀 STARTING PRODUCTION PIPELINE")
        print(f"{'='*50}")
        
        # Validation
        validation = self.validate_production_readiness()
        self.print_validation_report(validation)
        
        if not validation['ready']:
            print("❌ System not ready for production processing")
            return {'success': False, 'error': 'System validation failed'}
        
        # User confirmation for large datasets
        data_info = validation['system_info'].get('data', {})
        total_rows = data_info.get('total_rows', 0)
        
        if total_rows > 1_000_000 and not resume_batch_id:
            print(f"⚠️  About to process {total_rows:,} rows")
            estimates = validation['system_info'].get('estimates', {})
            if estimates:
                print(f"   Estimated time: {estimates.get('estimated_processing_hours', 0):.1f} hours")
                print(f"   Peak memory: {estimates.get('peak_memory_gb_estimate', 0):.1f}GB")
            
            response = input(f"\nContinue with production processing? (y/N): ")
            if response.lower() != 'y':
                print("❌ Processing cancelled by user")
                return {'success': False, 'error': 'Cancelled by user'}
        
        try:
            # Start monitoring if enabled
            monitor_process = None
            if monitor and self.monitor:
                print("📊 Starting real-time monitoring...")
                monitor_process = mp.Process(
                    target=self.monitor.monitor_batch,
                    args=(resume_batch_id, 30, True)
                )
                monitor_process.start()
            
            # Run processing
            print("🔄 Starting batch processing...")
            
            if resume_batch_id:
                print(f"📂 Resuming batch {resume_batch_id}")
                # Get pending symbols from failed batch
                pending_symbols = self.processor.get_pending_symbols(resume_batch_id)
                result = self.processor.run_production_processing(pending_symbols)
            else:
                result = self.processor.run_production_processing()
            
            # Stop monitoring
            if monitor_process and monitor_process.is_alive():
                monitor_process.terminate()
                monitor_process.join()
            
            total_duration = time.time() - start_time
            
            # Final report
            print(f"\n🏁 PRODUCTION PIPELINE COMPLETE")
            print(f"{'='*50}")
            print(f"Total Duration: {total_duration/3600:.2f} hours")
            print(f"Success Rate: {(result.get('symbols_success', 0)/result.get('symbols_total', 1)*100):.1f}%")
            print(f"Batch ID: {result.get('batch_id', 'N/A')}")
            
            if result.get('symbols_error', 0) > 0:
                print(f"⚠️  {result['symbols_error']} symbols failed - can resume with --resume {result.get('batch_id')}")
            
            return result
            
        except KeyboardInterrupt:
            print("\n⏹️  Processing interrupted by user")
            if monitor_process and monitor_process.is_alive():
                monitor_process.terminate()
            return {'success': False, 'error': 'Interrupted by user'}
        
        except Exception as e:
            print(f"\n❌ Pipeline failed: {e}")
            if monitor_process and monitor_process.is_alive():
                monitor_process.terminate()
            return {'success': False, 'error': str(e)}
    
    def cleanup(self):
        """Cleanup resources"""
        if self.db_manager:
            self.db_manager.close_pool()


def main():
    """Main entry point for production pipeline"""
    parser = argparse.ArgumentParser(
        description='Production IV Interpolation Pipeline (32M+ rows)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python production_main.py                     # Run production pipeline
  python production_main.py --monitor           # Run with real-time monitoring  
  python production_main.py --estimate          # Show performance estimates only
  python production_main.py --resume 1634567890 # Resume failed batch
  python production_main.py --validate-only     # Validate system readiness only
  python production_main.py --env development   # Use development settings
        """
    )
    
    parser.add_argument('--monitor', action='store_true', 
                       help='Enable real-time performance monitoring')
    parser.add_argument('--estimate', action='store_true',
                       help='Show performance estimates and exit')
    parser.add_argument('--resume', type=int, metavar='BATCH_ID',
                       help='Resume processing from failed batch')
    parser.add_argument('--validate-only', action='store_true',
                       help='Only validate system readiness')
    parser.add_argument('--env', choices=['development', 'testing', 'production'],
                       default='production', help='Environment configuration')
    parser.add_argument('--no-confirmation', action='store_true',
                       help='Skip confirmation prompts')
    
    args = parser.parse_args()
    
    # Set environment
    os.environ['ENVIRONMENT'] = args.env
    
    try:
        # Load configuration
        config = get_production_config()
        
        if args.estimate:
            print_performance_estimates()
            return
        
        # Create pipeline manager
        pipeline = ProductionPipelineManager(config)
        
        try:
            if args.validate_only:
                validation = pipeline.validate_production_readiness()
                pipeline.print_validation_report(validation)
                sys.exit(0 if validation['ready'] else 1)
            
            # Run production pipeline
            result = pipeline.run_production_pipeline(
                monitor=args.monitor,
                resume_batch_id=args.resume
            )
            
            sys.exit(0 if result.get('success', False) else 1)
            
        finally:
            pipeline.cleanup()
    
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()


# Additional optimization scripts for production deployment

def create_database_optimizations():
    """
    PostgreSQL optimization script for 32M+ rows
    
    Run these commands in your PostgreSQL database:
    """
    
    optimizations = """
-- PostgreSQL Optimizations for 32M+ Row Processing

-- 1. Memory and Performance Settings
ALTER SYSTEM SET shared_buffers = '4GB';  -- Adjust based on your RAM
ALTER SYSTEM SET work_mem = '256MB';       -- For sorting and hashing
ALTER SYSTEM SET maintenance_work_mem = '1GB';  -- For VACUUM/ANALYZE
ALTER SYSTEM SET effective_cache_size = '12GB';  -- Estimate of OS cache

-- 2. Parallel Processing Settings  
ALTER SYSTEM SET max_parallel_workers = '8';
ALTER SYSTEM SET max_parallel_workers_per_gather = '4';
ALTER SYSTEM SET parallel_tuple_cost = '0.1';

-- 3. Checkpoint and WAL Settings
ALTER SYSTEM SET checkpoint_completion_target = '0.8';
ALTER SYSTEM SET wal_buffers = '64MB';
ALTER SYSTEM SET checkpoint_segments = '64';

-- 4. Connection Settings
ALTER SYSTEM SET max_connections = '100';  -- Adjust based on your pool size

-- Apply settings (requires restart)
SELECT pg_reload_conf();

-- 5. Create optimized indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trading_tickers_symbol_date 
ON trading_tickers(symbol, date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trading_tickers_date 
ON trading_tickers(date) WHERE date >= '2023-01-01';

-- 6. Analyze tables for optimal query plans
ANALYZE trading_tickers;
ANALYZE interpolated_trading_tickers;

-- 7. Partitioning for very large tables (optional)
-- If trading_tickers becomes too large, consider partitioning by date:
-- CREATE TABLE trading_tickers_2023 PARTITION OF trading_tickers 
-- FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
"""
    
    print("📄 PostgreSQL Optimization Script:")
    print("="*50)
    print(optimizations)
    print("="*50)
    print("💡 Run these commands in your PostgreSQL database as superuser")
    print("⚠️  Some settings require a database restart to take effect")


def create_monitoring_script():
    """Create a monitoring startup script"""
    
    script = """#!/bin/bash
# production_monitor.sh - Start production monitoring

echo "🔍 Starting Production Performance Monitor"

# Start monitoring in background
python performance_monitor.py --interval 30 > monitor.log 2>&1 &
MONITOR_PID=$!

echo "📊 Monitoring started (PID: $MONITOR_PID)"
echo "📄 Log file: monitor.log"
echo "⏹️  Stop with: kill $MONITOR_PID"

# Save PID for later cleanup
echo $MONITOR_PID > monitor.pid
"""
    
    with open('production_monitor.sh', 'w') as f:
        f.write(script)
    
    os.chmod('production_monitor.sh', 0o755)
    print("✅ Created production_monitor.sh")


if __name__ == "__main__":
    # Additional setup when run directly
    print("🔧 Production Setup Utilities")
    print("="*40)
    
    response = input("Create database optimizations script? (y/N): ")
    if response.lower() == 'y':
        create_database_optimizations()
    
    response = input("Create monitoring script? (y/N): ")
    if response.lower() == 'y':
        create_monitoring_script()
    
    print("\n✅ Production setup utilities created!")
    print("🚀 Ready for 32M+ row processing!")