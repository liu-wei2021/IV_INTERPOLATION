#!/usr/bin/env python3
"""
Complete Pipeline Data Visualizer

Shows what data was created in each stage and provides visual analysis
of the interpolation results and candle reconstruction.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import seaborn as sns

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from config import get_config
from database.user_optimized_connection import UserLevelDatabaseManager

class PipelineDataVisualizer:
    """Comprehensive visualization of pipeline data and results"""
    
    def __init__(self):
        self.config = get_config()
        self.db_manager = UserLevelDatabaseManager(self.config.database)
        
        # Set up plotting style
        plt.style.use('default')
        sns.set_palette("husl")
        
    def analyze_database_tables(self):
        """Analyze what tables and data were created"""
        print("📊 PIPELINE DATABASE ANALYSIS")
        print("=" * 50)
        
        tables_info = {}
        
        # Tables to analyze
        tables = {
            'trading_tickers': 'Original source data (hourly IV)',
            'interpolated_trading_tickers': 'Task 1 output (minute-level IV)', 
            'minute_candles': 'Bridge output (1-min OHLCV)',
            'reconstructed_candles': 'Task 2 output (5-min OHLCV)'
        }
        
        for table_name, description in tables.items():
            try:
                # Check if table exists
                exists_query = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                )
                """
                exists = self.db_manager.execute_optimized_query(exists_query, fetch=True)[0][0]
                
                if exists:
                    # Get row count
                    count_query = f"SELECT COUNT(*) FROM {table_name}"
                    row_count = self.db_manager.execute_optimized_query(count_query, fetch=True)[0][0]
                    
                    # Get symbol count
                    symbol_query = f"SELECT COUNT(DISTINCT symbol) FROM {table_name}"
                    symbol_count = self.db_manager.execute_optimized_query(symbol_query, fetch=True)[0][0]
                    
                    # Get date range
                    date_col = 'date' if table_name in ['trading_tickers', 'interpolated_trading_tickers'] else 'timestamp'
                    try:
                        date_query = f"SELECT MIN({date_col}), MAX({date_col}) FROM {table_name}"
                        date_result = self.db_manager.execute_optimized_query(date_query, fetch=True)[0]
                        date_range = f"{date_result[0]} to {date_result[1]}" if date_result[0] else "No data"
                    except:
                        date_range = "Unknown"
                    
                    tables_info[table_name] = {
                        'exists': True,
                        'description': description,
                        'rows': row_count,
                        'symbols': symbol_count,
                        'date_range': date_range
                    }
                    
                    print(f"\n📋 {table_name.upper()}")
                    print(f"   Description: {description}")
                    print(f"   Rows: {row_count:,}")
                    print(f"   Symbols: {symbol_count:,}")
                    print(f"   Date range: {date_range}")
                    
                else:
                    tables_info[table_name] = {'exists': False, 'description': description}
                    print(f"\n❌ {table_name.upper()}: Table not found")
                    
            except Exception as e:
                print(f"\n❌ {table_name.upper()}: Error analyzing - {e}")
        
        return tables_info
    
    def plot_data_flow_diagram(self, tables_info):
        """Create a visual data flow diagram"""
        fig, ax = plt.subplots(1, 1, figsize=(14, 8))
        
        # Define positions for each stage
        stages = [
            ('Original Data\n(trading_tickers)', 0, tables_info.get('trading_tickers', {}).get('rows', 0)),
            ('Task 1: IV Interpolation\n(interpolated_trading_tickers)', 1, tables_info.get('interpolated_trading_tickers', {}).get('rows', 0)),
            ('Data Bridge\n(minute_candles)', 2, tables_info.get('minute_candles', {}).get('rows', 0)),
            ('Task 2: Candle Reconstruction\n(reconstructed_candles)', 3, tables_info.get('reconstructed_candles', {}).get('rows', 0))
        ]
        
        # Colors for each stage
        colors = ['#3498db', '#2ecc71', '#f39c12', '#e74c3c']
        
        # Plot boxes for each stage
        for i, (stage_name, x_pos, row_count) in enumerate(stages):
            # Box size based on data volume
            box_height = max(0.3, min(1.0, row_count / 50000000)) if row_count > 0 else 0.1
            
            # Draw box
            box = plt.Rectangle((x_pos - 0.4, 0.5 - box_height/2), 0.8, box_height, 
                              facecolor=colors[i], alpha=0.7, edgecolor='black')
            ax.add_patch(box)
            
            # Add text
            ax.text(x_pos, 0.5, stage_name, ha='center', va='center', 
                   fontsize=10, fontweight='bold', wrap=True)
            
            # Add row count
            if row_count > 0:
                ax.text(x_pos, 0.5 - box_height/2 - 0.15, f"{row_count:,} rows", 
                       ha='center', va='center', fontsize=9, style='italic')
            else:
                ax.text(x_pos, 0.5 - box_height/2 - 0.15, "No data", 
                       ha='center', va='center', fontsize=9, style='italic', color='red')
            
            # Draw arrows
            if i < len(stages) - 1:
                arrow = plt.Arrow(x_pos + 0.4, 0.5, 0.2, 0, width=0.1, color='gray')
                ax.add_patch(arrow)
        
        ax.set_xlim(-0.6, 3.6)
        ax.set_ylim(-0.5, 1.5)
        ax.set_aspect('equal')
        ax.axis('off')
        ax.set_title('Pipeline Data Flow & Volume', fontsize=16, fontweight='bold', pad=20)
        
        plt.tight_layout()
        plt.savefig('pipeline_data_flow.png', dpi=300, bbox_inches='tight')
        print(f"\n📊 Data flow diagram saved as: pipeline_data_flow.png")
        
        return fig
    
    def plot_interpolation_analysis(self, symbols=None):
        """Analyze and plot IV interpolation results"""
        if symbols is None:
            # Get symbols that have both original and interpolated data
            symbols_query = """
            SELECT DISTINCT t.symbol 
            FROM trading_tickers t
            INNER JOIN interpolated_trading_tickers i ON t.symbol = i.symbol
            ORDER BY t.symbol
            LIMIT 5
            """
            try:
                results = self.db_manager.execute_optimized_query(symbols_query, fetch=True)
                symbols = [row[0] for row in results]
            except:
                print("❌ No symbols found with both original and interpolated data")
                return
        
        if not symbols:
            print("❌ No symbols available for interpolation analysis")
            return
        
        print(f"\n📈 INTERPOLATION ANALYSIS for {len(symbols)} symbols")
        
        fig, axes = plt.subplots(len(symbols), 3, figsize=(18, 4 * len(symbols)))
        if len(symbols) == 1:
            axes = axes.reshape(1, -1)
        
        for i, symbol in enumerate(symbols):
            print(f"   Analyzing {symbol}...")
            
            try:
                # Get original data
                original_query = """
                SELECT date, iv, underlying_price, volume
                FROM trading_tickers 
                WHERE symbol = %s 
                ORDER BY date
                """
                
                with self.db_manager.get_connection() as conn:
                    original_data = pd.read_sql(original_query, conn, params=(symbol,))
                
                # Get interpolated data
                interp_query = """
                SELECT date, iv, underlying_price, volume, is_interpolated
                FROM interpolated_trading_tickers 
                WHERE symbol = %s 
                ORDER BY date
                """
                
                with self.db_manager.get_connection() as conn:
                    interp_data = pd.read_sql(interp_query, conn, params=(symbol,))
                
                if original_data.empty or interp_data.empty:
                    continue
                
                # Convert dates
                original_data['date'] = pd.to_datetime(original_data['date'])
                interp_data['date'] = pd.to_datetime(interp_data['date'])
                
                # Plot 1: IV comparison
                ax1 = axes[i, 0]
                ax1.plot(original_data['date'], original_data['iv'], 'o-', 
                        label='Original (hourly)', markersize=8, linewidth=2)
                
                # Separate interpolated vs original points
                original_points = interp_data[~interp_data['is_interpolated']]
                interpolated_points = interp_data[interp_data['is_interpolated']]
                
                ax1.plot(original_points['date'], original_points['iv'], 'o', 
                        color='blue', markersize=6, label='Original points')
                ax1.plot(interpolated_points['date'], interpolated_points['iv'], '.', 
                        color='red', markersize=2, alpha=0.6, label='Interpolated')
                
                ax1.set_title(f'{symbol}\nImplied Volatility')
                ax1.set_ylabel('IV')
                ax1.legend()
                ax1.grid(True, alpha=0.3)
                
                # Plot 2: Underlying price
                ax2 = axes[i, 1]
                ax2.plot(original_data['date'], original_data['underlying_price'], 'o-', 
                        label='Original', markersize=8, linewidth=2)
                ax2.plot(interp_data['date'], interp_data['underlying_price'], '.', 
                        color='orange', markersize=1, alpha=0.7, label='Interpolated')
                
                ax2.set_title('Underlying Price')
                ax2.set_ylabel('Price')
                ax2.legend()
                ax2.grid(True, alpha=0.3)
                
                # Plot 3: Data density
                ax3 = axes[i, 2]
                
                # Count data points per hour
                original_data['hour'] = original_data['date'].dt.floor('H')
                interp_data['hour'] = interp_data['date'].dt.floor('H')
                
                orig_counts = original_data.groupby('hour').size()
                interp_counts = interp_data.groupby('hour').size()
                
                ax3.bar(orig_counts.index, orig_counts.values, alpha=0.7, 
                       label=f'Original ({len(original_data)} points)', width=0.02)
                ax3.bar(interp_counts.index, interp_counts.values, alpha=0.7,
                       label=f'Interpolated ({len(interp_data)} points)', width=0.02)
                
                ax3.set_title('Data Density (points per hour)')
                ax3.set_ylabel('Count')
                ax3.legend()
                ax3.grid(True, alpha=0.3)
                
                # Format x-axis
                for ax in [ax1, ax2, ax3]:
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
                    ax.tick_params(axis='x', rotation=45)
                
            except Exception as e:
                print(f"   ❌ Error analyzing {symbol}: {e}")
                continue
        
        plt.tight_layout()
        plt.savefig('interpolation_analysis.png', dpi=300, bbox_inches='tight')
        print(f"\n📊 Interpolation analysis saved as: interpolation_analysis.png")
        
        return fig
    
    def plot_candle_analysis(self, symbols=None):
        """Analyze candle reconstruction if data exists"""
        if symbols is None:
            # Get symbols with minute candles
            symbols_query = """
            SELECT DISTINCT symbol FROM minute_candles 
            ORDER BY symbol LIMIT 3
            """
            try:
                results = self.db_manager.execute_optimized_query(symbols_query, fetch=True)
                symbols = [row[0] for row in results]
            except:
                print("❌ No minute candle data found")
                return None
        
        if not symbols:
            print("❌ No symbols available for candle analysis")
            return None
        
        print(f"\n📊 CANDLE ANALYSIS for {len(symbols)} symbols")
        
        fig, axes = plt.subplots(len(symbols), 2, figsize=(15, 4 * len(symbols)))
        if len(symbols) == 1:
            axes = axes.reshape(1, -1)
        
        for i, symbol in enumerate(symbols):
            print(f"   Analyzing candles for {symbol}...")
            
            try:
                # Get minute candles
                minute_query = """
                SELECT timestamp, open, high, low, close, volume
                FROM minute_candles 
                WHERE symbol = %s 
                ORDER BY timestamp
                LIMIT 200
                """
                
                with self.db_manager.get_connection() as conn:
                    minute_data = pd.read_sql(minute_query, conn, params=(symbol,))
                
                # Get 5-minute candles if they exist
                five_min_query = """
                SELECT timestamp, open, high, low, close, volume
                FROM reconstructed_candles 
                WHERE symbol = %s 
                ORDER BY timestamp
                LIMIT 40
                """
                
                try:
                    with self.db_manager.get_connection() as conn:
                        five_min_data = pd.read_sql(five_min_query, conn, params=(symbol,))
                except:
                    five_min_data = pd.DataFrame()
                
                if minute_data.empty:
                    continue
                
                minute_data['timestamp'] = pd.to_datetime(minute_data['timestamp'])
                
                # Plot 1: Price action
                ax1 = axes[i, 0]
                ax1.plot(minute_data['timestamp'], minute_data['close'], 
                        linewidth=1, alpha=0.7, label='1-min Close')
                
                if not five_min_data.empty:
                    five_min_data['timestamp'] = pd.to_datetime(five_min_data['timestamp'])
                    ax1.plot(five_min_data['timestamp'], five_min_data['close'], 
                            'ro-', markersize=6, linewidth=2, label='5-min Close')
                
                ax1.set_title(f'{symbol}\nPrice Comparison')
                ax1.set_ylabel('Price')
                ax1.legend()
                ax1.grid(True, alpha=0.3)
                
                # Plot 2: Volume comparison
                ax2 = axes[i, 1]
                
                # Create volume bars
                width = 0.0003  # Narrow bars for minute data
                ax2.bar(minute_data['timestamp'], minute_data['volume'], 
                       width=width, alpha=0.6, label='1-min Volume')
                
                if not five_min_data.empty:
                    width_5min = 0.002  # Wider bars for 5-min data
                    ax2.bar(five_min_data['timestamp'], five_min_data['volume'], 
                           width=width_5min, alpha=0.8, color='red', label='5-min Volume')
                
                ax2.set_title('Volume Comparison')
                ax2.set_ylabel('Volume')
                ax2.legend()
                ax2.grid(True, alpha=0.3)
                
                # Format x-axis
                for ax in [ax1, ax2]:
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                    ax.tick_params(axis='x', rotation=45)
                
            except Exception as e:
                print(f"   ❌ Error analyzing candles for {symbol}: {e}")
                continue
        
        plt.tight_layout()
        plt.savefig('candle_analysis.png', dpi=300, bbox_inches='tight')
        print(f"\n📊 Candle analysis saved as: candle_analysis.png")
        
        return fig
    
    def generate_summary_report(self):
        """Generate comprehensive summary report"""
        print(f"\n📋 PIPELINE SUMMARY REPORT")
        print("=" * 60)
        
        # Get table info
        tables_info = self.analyze_database_tables()
        
        # Create data flow diagram
        self.plot_data_flow_diagram(tables_info)
        
        # Create interpolation analysis
        self.plot_interpolation_analysis()
        
        # Create candle analysis
        self.plot_candle_analysis()
        
        # Print summary
        print(f"\n📊 PROCESSING SUMMARY:")
        
        if tables_info.get('trading_tickers', {}).get('exists'):
            orig_rows = tables_info['trading_tickers']['rows']
            orig_symbols = tables_info['trading_tickers']['symbols']
            print(f"   📥 Source: {orig_rows:,} rows, {orig_symbols:,} symbols")
        
        if tables_info.get('interpolated_trading_tickers', {}).get('exists'):
            interp_rows = tables_info['interpolated_trading_tickers']['rows']
            interp_symbols = tables_info['interpolated_trading_tickers']['symbols']
            expansion = interp_rows / orig_rows if orig_rows > 0 else 0
            print(f"   📈 Task 1: {interp_rows:,} rows, {interp_symbols} symbols ({expansion:.1f}x expansion)")
        
        if tables_info.get('minute_candles', {}).get('exists'):
            minute_rows = tables_info['minute_candles']['rows']
            minute_symbols = tables_info['minute_candles']['symbols']
            print(f"   🌉 Bridge: {minute_rows:,} minute candles, {minute_symbols} symbols")
        
        if tables_info.get('reconstructed_candles', {}).get('exists'):
            recon_rows = tables_info['reconstructed_candles']['rows']
            recon_symbols = tables_info['reconstructed_candles']['symbols']
            compression = minute_rows / recon_rows if 'minute_rows' in locals() and recon_rows > 0 else 0
            print(f"   📊 Task 2: {recon_rows:,} 5-min candles, {recon_symbols} symbols ({compression:.1f}:1 compression)")
        
        print(f"\n📁 VISUALIZATIONS CREATED:")
        print("   📊 pipeline_data_flow.png - Data flow diagram")
        print("   📈 interpolation_analysis.png - IV interpolation analysis")
        print("   📊 candle_analysis.png - Candle reconstruction analysis")
        
        plt.show()
    
    def cleanup(self):
        """Cleanup resources"""
        if self.db_manager:
            self.db_manager.close_pool()

def main():
    """Main entry point"""
    try:
        visualizer = PipelineDataVisualizer()
        visualizer.generate_summary_report()
        
    except Exception as e:
        print(f"❌ Visualization failed: {e}")
        return 1
    finally:
        if 'visualizer' in locals():
            visualizer.cleanup()
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())