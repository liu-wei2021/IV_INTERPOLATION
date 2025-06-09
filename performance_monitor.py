#!/usr/bin/env python3
"""
Production Performance Monitor for 32M+ Row Processing
Usage: python performance_monitor.py [--batch-id BATCH_ID] [--interval SECONDS]
"""

import time
import psutil
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import sys
from pathlib import Path
import json

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from config_production import get_production_config
from database.optimized_connection import OptimizedDatabaseManager

class ProductionPerformanceMonitor:
    """
    Real-time performance monitor for production-scale processing
    
    Tracks:
    - Processing throughput (rows/second)
    - System resource usage (CPU, Memory, Disk)
    - Database performance (connections, query times)
    - ETAs and completion estimates
    - Error rates and bottlenecks
    """
    
    def __init__(self, config, db_manager: OptimizedDatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.start_time = time.time()
        self.previous_stats = {}
        self.performance_history = []
        
        # Performance thresholds
        self.memory_warning_threshold = 0.80
        self.memory_critical_threshold = 0.90
        self.cpu_warning_threshold = 0.85
        self.cpu_critical_threshold = 0.95
        
        # Monitoring state
        self.alerts_sent = set()
        self.last_checkpoint = time.time()
        
        logger = logging.getLogger(__name__)
    
    def get_current_batch_stats(self, batch_id: Optional[int] = None) -> Dict:
        """Get current batch processing statistics"""
        
        # Get latest batch ID if not specified
        if batch_id is None:
            query = "SELECT MAX(batch_id) FROM interpolation_progress"
            result = self.db_manager.execute_optimized_query(query, fetch=True)
            if not result or not result[0][0]:
                return {}
            batch_id = result[0][0]
        
        # Get batch progress
        progress_query = """
        SELECT 
            status,
            COUNT(*) as count,
            COALESCE(SUM(input_rows), 0) as total_input_rows,
            COALESCE(SUM(output_rows), 0) as total_output_rows,
            COALESCE(SUM(processing_time_seconds), 0) as total_processing_time,
            COALESCE(AVG(processing_time_seconds), 0) as avg_processing_time
        FROM interpolation_progress 
        WHERE batch_id = %s
        GROUP BY status
        ORDER BY status
        """
        
        try:
            results = self.db_manager.execute_optimized_query(progress_query, (batch_id,), fetch=True)
            
            stats = {
                'batch_id': batch_id,
                'timestamp': datetime.now(),
                'statuses': {},
                'totals': {
                    'symbols': 0,
                    'input_rows': 0,
                    'output_rows': 0,
                    'processing_time': 0.0
                }
            }
            
            for row in results:
                status, count, input_rows, output_rows, proc_time, avg_time = row
                stats['statuses'][status] = {
                    'count': count,
                    'input_rows': int(input_rows),
                    'output_rows': int(output_rows),
                    'processing_time': float(proc_time),
                    'avg_processing_time': float(avg_time)
                }
                stats['totals']['symbols'] += count
                stats['totals']['input_rows'] += int(input_rows)
                stats['totals']['output_rows'] += int(output_rows)
                stats['totals']['processing_time'] += float(proc_time)
            
            return stats
            
        except Exception as e:
            logging.error(f"Failed to get batch stats: {e}")
            return {}
    
    def get_system_resources(self) -> Dict:
        """Get current system resource usage"""
        try:
            # Memory usage
            memory = psutil.virtual_memory()
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_per_core = psutil.cpu_percent(interval=1, percpu=True)
            
            # Disk usage
            disk = psutil.disk_usage('/')
            
            # Network I/O
            network = psutil.net_io_counters()
            
            # Process information
            process = psutil.Process()
            process_memory = process.memory_info()
            
            return {
                'memory': {
                    'total_gb': memory.total / (1024**3),
                    'used_gb': (memory.total - memory.available) / (1024**3),
                    'available_gb': memory.available / (1024**3),
                    'percent': memory.percent
                },
                'cpu': {
                    'percent': cpu_percent,
                    'per_core': cpu_per_core,
                    'count': psutil.cpu_count()
                },
                'disk': {
                    'total_gb': disk.total / (1024**3),
                    'used_gb': disk.used / (1024**3),
                    'free_gb': disk.free / (1024**3),
                    'percent': (disk.used / disk.total) * 100
                },
                'network': {
                    'bytes_sent': network.bytes_sent,
                    'bytes_recv': network.bytes_recv
                },
                'process': {
                    'memory_rss_gb': process_memory.rss / (1024**3),
                    'memory_vms_gb': process_memory.vms / (1024**3)
                }
            }
            
        except Exception as e:
            logging.error(f"Failed to get system resources: {e}")
            return {}
    
    def get_database_performance(self) -> Dict:
        """Get database performance metrics"""
        try:
            # Connection pool stats
            pool_stats = self.db_manager.get_connection_pool_stats()
            
            # Database activity
            db_stats_query = """
            SELECT 
                numbackends,
                xact_commit,
                xact_rollback,
                blks_read,
                blks_hit,
                tup_returned,
                tup_fetched,
                tup_inserted,
                tup_updated,
                tup_deleted
            FROM pg_stat_database 
            WHERE datname = CURRENT_DATABASE()
            """
            
            db_result = self.db_manager.execute_optimized_query(db_stats_query, fetch=True)
            
            if db_result:
                db_row = db_result[0]
                cache_hit_ratio = (db_row[4] / (db_row[3] + db_row[4])) * 100 if (db_row[3] + db_row[4]) > 0 else 0
                
                return {
                    'connections': {
                        'active': db_row[0],
                        'pool_checked_out': pool_stats.get('checked_out', 0),
                        'pool_checked_in': pool_stats.get('checked_in', 0),
                        'pool_overflow': pool_stats.get('overflow', 0)
                    },
                    'transactions': {
                        'commits': db_row[1],
                        'rollbacks': db_row[2]
                    },
                    'performance': {
                        'cache_hit_ratio': cache_hit_ratio,
                        'blocks_read': db_row[3],
                        'blocks_hit': db_row[4]
                    },
                    'activity': {
                        'tuples_returned': db_row[5],
                        'tuples_fetched': db_row[6],
                        'tuples_inserted': db_row[7],
                        'tuples_updated': db_row[8],
                        'tuples_deleted': db_row[9]
                    }
                }
            
        except Exception as e:
            logging.error(f"Failed to get database performance: {e}")
        
        return {}
    
    def calculate_throughput_metrics(self, current_stats: Dict, previous_stats: Dict) -> Dict:
        """Calculate throughput and performance metrics"""
        if not previous_stats:
            return {}
        
        try:
            current_time = time.time()
            time_diff = current_time - previous_stats.get('timestamp', current_time)
            
            if time_diff <= 0:
                return {}
            
            # Calculate rows processed per second
            current_completed = current_stats['statuses'].get('completed', {})
            previous_completed = previous_stats['statuses'].get('completed', {})
            
            rows_processed = (current_completed.get('output_rows', 0) - 
                            previous_completed.get('output_rows', 0))
            
            symbols_processed = (current_completed.get('count', 0) - 
                               previous_completed.get('count', 0))
            
            throughput = {
                'rows_per_second': rows_processed / time_diff,
                'symbols_per_minute': (symbols_processed / time_diff) * 60,
                'time_interval': time_diff
            }
            
            # Calculate ETA
            remaining_symbols = current_stats['statuses'].get('pending', {}).get('count', 0)
            if throughput['symbols_per_minute'] > 0:
                eta_minutes = remaining_symbols / throughput['symbols_per_minute']
                throughput['eta_hours'] = eta_minutes / 60
                throughput['eta_completion'] = datetime.now() + timedelta(minutes=eta_minutes)
            
            return throughput
            
        except Exception as e:
            logging.error(f"Failed to calculate throughput: {e}")
            return {}
    
    def check_performance_alerts(self, system_resources: Dict, throughput: Dict) -> List[str]:
        """Check for performance alerts and warnings"""
        alerts = []
        
        # Memory alerts
        memory_percent = system_resources.get('memory', {}).get('percent', 0)
        if memory_percent > self.memory_critical_threshold * 100:
            alert = f"CRITICAL: Memory usage {memory_percent:.1f}% (>{self.memory_critical_threshold*100:.0f}%)"
            if alert not in self.alerts_sent:
                alerts.append(alert)
                self.alerts_sent.add(alert)
        elif memory_percent > self.memory_warning_threshold * 100:
            alert = f"WARNING: Memory usage {memory_percent:.1f}% (>{self.memory_warning_threshold*100:.0f}%)"
            if alert not in self.alerts_sent:
                alerts.append(alert)
                self.alerts_sent.add(alert)
        
        # CPU alerts
        cpu_percent = system_resources.get('cpu', {}).get('percent', 0)
        if cpu_percent > self.cpu_critical_threshold * 100:
            alert = f"CRITICAL: CPU usage {cpu_percent:.1f}% (>{self.cpu_critical_threshold*100:.0f}%)"
            if alert not in self.alerts_sent:
                alerts.append(alert)
                self.alerts_sent.add(alert)
        elif cpu_percent > self.cpu_warning_threshold * 100:
            alert = f"WARNING: CPU usage {cpu_percent:.1f}% (>{self.cpu_warning_threshold*100:.0f}%)"
            if alert not in self.alerts_sent:
                alerts.append(alert)
                self.alerts_sent.add(alert)
        
        # Throughput alerts
        if throughput.get('rows_per_second', 0) < 100:  # Less than 100 rows/second is concerning
            alert = f"WARNING: Low throughput {throughput.get('rows_per_second', 0):.0f} rows/second"
            if alert not in self.alerts_sent:
                alerts.append(alert)
                self.alerts_sent.add(alert)
        
        return alerts
    
    def print_dashboard(self, stats: Dict, resources: Dict, db_perf: Dict, throughput: Dict):
        """Print real-time performance dashboard"""
        
        # Clear screen
        print("\033[2J\033[H")
        
        # Header
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        elapsed = time.time() - self.start_time
        elapsed_str = str(timedelta(seconds=int(elapsed)))
        
        print(f"🚀 PRODUCTION PERFORMANCE MONITOR - {current_time}")
        print(f"{'='*80}")
        print(f"Batch ID: {stats.get('batch_id', 'N/A')} | Elapsed: {elapsed_str}")
        print()
        
        # Processing Progress
        print(f"📊 PROCESSING PROGRESS")
        print(f"{'-'*40}")
        
        totals = stats.get('totals', {})
        statuses = stats.get('statuses', {})
        
        completed = statuses.get('completed', {}).get('count', 0)
        pending = statuses.get('pending', {}).get('count', 0)
        error = statuses.get('error', {}).get('count', 0)
        total_symbols = totals.get('symbols', 0)
        
        if total_symbols > 0:
            progress_pct = (completed / total_symbols) * 100
            print(f"Progress: {completed:,}/{total_symbols:,} symbols ({progress_pct:.1f}%)")
            
            # Progress bar
            bar_width = 50
            filled = int(bar_width * progress_pct / 100)
            bar = '█' * filled + '░' * (bar_width - filled)
            print(f"[{bar}] {progress_pct:.1f}%")
        
        print(f"✅ Completed: {completed:,}")
        print(f"⏳ Pending: {pending:,}")
        print(f"❌ Errors: {error:,}")
        print(f"📈 Input Rows: {totals.get('input_rows', 0):,}")
        print(f"📉 Output Rows: {totals.get('output_rows', 0):,}")
        print()
        
        # Throughput Metrics
        if throughput:
            print(f"⚡ THROUGHPUT METRICS")
            print(f"{'-'*40}")
            print(f"Rows/Second: {throughput.get('rows_per_second', 0):,.0f}")
            print(f"Symbols/Minute: {throughput.get('symbols_per_minute', 0):.1f}")
            
            if 'eta_hours' in throughput:
                print(f"ETA: {throughput['eta_hours']:.1f} hours ({throughput.get('eta_completion', 'N/A')})")
            print()
        
        # System Resources
        memory = resources.get('memory', {})
        cpu = resources.get('cpu', {})
        disk = resources.get('disk', {})
        
        print(f"💻 SYSTEM RESOURCES")
        print(f"{'-'*40}")
        print(f"Memory: {memory.get('used_gb', 0):.1f}GB/{memory.get('total_gb', 0):.1f}GB ({memory.get('percent', 0):.1f}%)")
        print(f"CPU: {cpu.get('percent', 0):.1f}% ({cpu.get('count', 0)} cores)")
        print(f"Disk: {disk.get('used_gb', 0):.1f}GB/{disk.get('total_gb', 0):.1f}GB ({disk.get('percent', 0):.1f}%)")
        print()
        
        # Database Performance
        db_conn = db_perf.get('connections', {})
        db_perf_metrics = db_perf.get('performance', {})
        
        print(f"🗄️  DATABASE PERFORMANCE")
        print(f"{'-'*40}")
        print(f"Active Connections: {db_conn.get('active', 0)}")
        print(f"Pool Usage: {db_conn.get('pool_checked_out', 0)}/{db_conn.get('pool_checked_in', 0)} (overflow: {db_conn.get('pool_overflow', 0)})")
        print(f"Cache Hit Ratio: {db_perf_metrics.get('cache_hit_ratio', 0):.1f}%")
        print()
        
        # Alerts
        alerts = self.check_performance_alerts(resources, throughput)
        if alerts:
            print(f"🚨 ALERTS")
            print(f"{'-'*40}")
            for alert in alerts:
                print(f"  {alert}")
            print()
        
        print(f"Last Update: {current_time} | Press Ctrl+C to stop monitoring")
        print(f"{'='*80}")
    
    def save_performance_snapshot(self, stats: Dict, resources: Dict, db_perf: Dict, throughput: Dict):
        """Save performance snapshot to file"""
        snapshot = {
            'timestamp': datetime.now().isoformat(),
            'batch_stats': stats,
            'system_resources': resources,
            'database_performance': db_perf,
            'throughput_metrics': throughput
        }
        
        # Save to file
        snapshot_file = Path(self.config.log_dir) / f"performance_snapshot_{stats.get('batch_id', 'unknown')}.json"
        
        try:
            with open(snapshot_file, 'w') as f:
                json.dump(snapshot, f, indent=2, default=str)
        except Exception as e:
            logging.error(f"Failed to save performance snapshot: {e}")
    
    def monitor_batch(self, batch_id: Optional[int] = None, interval: int = 30, save_snapshots: bool = True):
        """Monitor batch processing with real-time updates"""
        
        print(f"🔍 Starting performance monitoring (refresh every {interval}s)")
        print("Press Ctrl+C to stop monitoring\n")
        
        try:
            while True:
                # Collect metrics
                current_stats = self.get_current_batch_stats(batch_id)
                if not current_stats:
                    print("❌ No active batch found")
                    time.sleep(interval)
                    continue
                
                system_resources = self.get_system_resources()
                db_performance = self.get_database_performance()
                throughput = self.calculate_throughput_metrics(current_stats, self.previous_stats)
                
                # Display dashboard
                self.print_dashboard(current_stats, system_resources, db_performance, throughput)
                
                # Save snapshot
                if save_snapshots:
                    self.save_performance_snapshot(current_stats, system_resources, db_performance, throughput)
                
                # Store current stats for next iteration
                self.previous_stats = current_stats.copy()
                self.previous_stats['timestamp'] = time.time()
                
                # Check completion
                pending_count = current_stats['statuses'].get('pending', {}).get('count', 0)
                processing_count = current_stats['statuses'].get('processing', {}).get('count', 0)
                
                if pending_count == 0 and processing_count == 0:
                    print("\n✅ Batch processing completed!")
                    break
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n⏹️  Monitoring stopped by user")
        except Exception as e:
            print(f"\n❌ Monitoring failed: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Production Performance Monitor')
    parser.add_argument('--batch-id', type=int, help='Specific batch ID to monitor')
    parser.add_argument('--interval', type=int, default=30, help='Refresh interval in seconds')
    parser.add_argument('--no-snapshots', action='store_true', help='Disable snapshot saving')
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = get_production_config()
        
        # Initialize database connection
        db_manager = OptimizedDatabaseManager(config.database)
        
        # Create monitor
        monitor = ProductionPerformanceMonitor(config, db_manager)
        
        # Start monitoring
        monitor.monitor_batch(
            batch_id=args.batch_id,
            interval=args.interval,
            save_snapshots=not args.no_snapshots
        )
        
    except Exception as e:
        print(f"❌ Failed to start monitoring: {e}")
        sys.exit(1)
    finally:
        if 'db_manager' in locals():
            db_manager.close_pool()

if __name__ == "__main__":
    main()