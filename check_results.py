#!/usr/bin/env python3
"""
Dual Task Results Checker - Check results for both IV interpolation and candle reconstruction
Usage: 
  python check_results.py                          # Check both tasks
  python check_results.py --task interpolation     # Check Task 1 only
  python check_results.py --task candles           # Check Task 2 only
  python check_results.py --plot --task candles    # Plot candle reconstruction
"""

import sys
from pathlib import Path
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import argparse

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from config import get_config

def check_iv_interpolation_results():
    """Check Task 1: IV Interpolation results"""
    print("📊 TASK 1: IV INTERPOLATION RESULTS")
    print("=" * 50)
    
    config = get_config()
    
    try:
        conn = psycopg2.connect(**config.database.to_dict())
        
        # 1. Overall summary
        print("\n1. OVERALL SUMMARY")
        print("-" * 30)
        
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM trading_tickers")
            original_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM interpolated_trading_tickers")
            interpolated_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(DISTINCT symbol) FROM interpolated_trading_tickers")
            symbol_count = cur.fetchone()[0]
            
            print(f"Original rows: {original_count:,}")
            print(f"Interpolated rows: {interpolated_count:,}")
            print(f"Symbols processed: {symbol_count}")
            
            if original_count > 0:
                expansion_ratio = interpolated_count / original_count
                print(f"Expansion ratio: {expansion_ratio:.1f}x")
        
        # 2. Symbol breakdown
        print("\n2. SYMBOL BREAKDOWN")
        print("-" * 30)
        
        symbols_query = """
        SELECT 
            symbol,
            COUNT(*) as interpolated_rows,
            MIN(date) as start_time,
            MAX(date) as end_time
        FROM interpolated_trading_tickers 
        GROUP BY symbol
        ORDER BY interpolated_rows DESC
        LIMIT 10
        """
        
        df_symbols = pd.read_sql(symbols_query, conn)
        
        if not df_symbols.empty:
            print("Top 10 symbols by data volume:")
            for _, row in df_symbols.iterrows():
                duration = row['end_time'] - row['start_time']
                print(f"  {row['symbol']}: {row['interpolated_rows']:,} rows ({duration})")
        
    except Exception as e:
        print(f"❌ Error checking IV interpolation: {e}")
    finally:
        conn.close()

def check_candle_reconstruction_results():
    """Check Task 2: Candle Reconstruction results"""
    print("📊 TASK 2: CANDLE RECONSTRUCTION RESULTS")
    print("=" * 50)
    
    config = get_config()
    
    try:
        conn = psycopg2.connect(**config.database.to_dict())
        
        # 1. Check if tables exist
        with conn.cursor() as cur:
            cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'minute_candles'
            )
            """)
            minute_exists = cur.fetchone()[0]
            
            cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'reconstructed_candles'
            )
            """)
            reconstructed_exists = cur.fetchone()[0]
        
        if not minute_exists:
            print("❌ minute_candles table not found")
            return
        
        if not reconstructed_exists:
            print("❌ reconstructed_candles table not found")
            return
        
        # 2. Overall summary
        print("\n1. OVERALL SUMMARY")
        print("-" * 30)
        
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM minute_candles")
            minute_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM reconstructed_candles")
            reconstructed_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(DISTINCT symbol) FROM reconstructed_candles")
            symbol_count = cur.fetchone()[0]
            
            print(f"Input (1-min candles): {minute_count:,}")
            print(f"Output (5-min candles): {reconstructed_count:,}")
            print(f"Symbols processed: {symbol_count}")
            
            if minute_count > 0:
                compression_ratio = minute_count / reconstructed_count if reconstructed_count > 0 else 0
                print(f"Compression ratio: {compression_ratio:.1f}:1")
        
        # 3. Symbol breakdown
        print("\n2. SYMBOL BREAKDOWN")
        print("-" * 30)
        
        symbols_query = """
        SELECT 
            r.symbol,
            COUNT(r.*) as reconstructed_candles,
            MIN(r.timestamp) as start_time,
            MAX(r.timestamp) as end_time,
            SUM(r.volume) as total_volume
        FROM reconstructed_candles r
        GROUP BY r.symbol
        ORDER BY reconstructed_candles DESC
        """
        
        df_symbols = pd.read_sql(symbols_query, conn)
        
        if not df_symbols.empty:
            print("Reconstructed candles by symbol:")
            for _, row in df_symbols.iterrows():
                duration = row['end_time'] - row['start_time']
                print(f"  {row['symbol']}: {row['reconstructed_candles']:,} candles")
                print(f"    Time: {row['start_time']} to {row['end_time']} ({duration})")
                print(f"    Volume: {row['total_volume']:,.2f}")
                print()
        
        # 4. Data quality check
        print("3. DATA QUALITY CHECK")
        print("-" * 30)
        
        quality_query = """
        SELECT 
            COUNT(*) as total_candles,
            COUNT(*) FILTER (WHERE high >= low) as valid_high_low,
            COUNT(*) FILTER (WHERE high >= open AND high >= close) as valid_high_oc,
            COUNT(*) FILTER (WHERE low <= open AND low <= close) as valid_low_oc,
            COUNT(*) FILTER (WHERE volume >= 0) as valid_volume,
            ROUND(AVG(high - low), 4) as avg_spread,
            ROUND(AVG(volume), 2) as avg_volume
        FROM reconstructed_candles
        """
        
        df_quality = pd.read_sql(quality_query, conn)
        
        for _, row in df_quality.iterrows():
            total = row['total_candles']
            print(f"Total candles: {total:,}")
            print(f"Valid OHLC relationships: {row['valid_high_low']:,}/{total:,}")
            print(f"Valid high vs open/close: {row['valid_high_oc']:,}/{total:,}")
            print(f"Valid low vs open/close: {row['valid_low_oc']:,}/{total:,}")
            print(f"Valid volume: {row['valid_volume']:,}/{total:,}")
            print(f"Average spread: ${row['avg_spread']}")
            print(f"Average volume: {row['avg_volume']}")
        
        # 5. Sample comparison
        print("\n4. SAMPLE COMPARISON")
        print("-" * 30)
        
        # Get a sample symbol for comparison
        sample_query = """
        SELECT symbol FROM reconstructed_candles 
        GROUP BY symbol 
        ORDER BY COUNT(*) DESC 
        LIMIT 1
        """
        
        sample_symbol = pd.read_sql(sample_query, conn).iloc[0]['symbol']
        print(f"Sample analysis for: {sample_symbol}")
        
        # Compare 1-min vs 5-min for the same time period
        comparison_query = f"""
        WITH minute_sample AS (
            SELECT 
                timestamp,
                open, high, low, close, volume,
                '1min' as frequency
            FROM minute_candles 
            WHERE symbol = '{sample_symbol}'
            ORDER BY timestamp 
            LIMIT 10
        ),
        reconstructed_sample AS (
            SELECT 
                timestamp,
                open, high, low, close, volume,
                frequency
            FROM reconstructed_candles 
            WHERE symbol = '{sample_symbol}'
            ORDER BY timestamp 
            LIMIT 2
        )
        SELECT * FROM minute_sample
        UNION ALL
        SELECT * FROM reconstructed_sample
        ORDER BY timestamp, frequency
        """
        
        df_comparison = pd.read_sql(comparison_query, conn)
        print("\nSample data (first 10 1-min + first 2 5-min candles):")
        print(df_comparison.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error checking candle reconstruction: {e}")
    finally:
        conn.close()

def plot_candle_reconstruction(symbol=None):
    """Plot candle reconstruction comparison"""
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        print("❌ matplotlib not installed. Install with: pip install matplotlib")
        return
    
    print("📈 PLOTTING CANDLE RECONSTRUCTION")
    print("=" * 40)
    
    config = get_config()
    conn = psycopg2.connect(**config.database.to_dict())
    
    try:
        # Get available symbol if none specified
        if symbol is None:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT symbol FROM reconstructed_candles LIMIT 1")
                result = cur.fetchone()
                if not result:
                    print("❌ No reconstructed candle data found!")
                    return
                symbol = result[0]
        
        print(f"Plotting candles for symbol: {symbol}")
        
        # Fetch 1-minute data
        minute_query = """
        SELECT timestamp, open, high, low, close, volume
        FROM minute_candles 
        WHERE symbol = %s
        ORDER BY timestamp
        LIMIT 100
        """
        
        df_minute = pd.read_sql(minute_query, conn, params=(symbol,))
        
        # Fetch 5-minute data
        reconstructed_query = """
        SELECT timestamp, open, high, low, close, volume
        FROM reconstructed_candles 
        WHERE symbol = %s
        ORDER BY timestamp
        LIMIT 20
        """
        
        df_5min = pd.read_sql(reconstructed_query, conn, params=(symbol,))
        
        if df_minute.empty or df_5min.empty:
            print(f"❌ No data found for symbol: {symbol}")
            return
        
        df_minute['timestamp'] = pd.to_datetime(df_minute['timestamp'])
        df_5min['timestamp'] = pd.to_datetime(df_5min['timestamp'])
        
        # Create candlestick-style plot
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle(f'Candle Reconstruction Analysis: {symbol}', fontsize=16, fontweight='bold')
        
        # 1. Price comparison
        ax1 = axes[0, 0]
        ax1.plot(df_minute['timestamp'], df_minute['close'], 'b-', alpha=0.7, linewidth=1, label='1-min Close')
        ax1.plot(df_5min['timestamp'], df_5min['close'], 'ro-', markersize=8, linewidth=2, label='5-min Close')
        ax1.set_title('Close Price Comparison')
        ax1.set_ylabel('Price')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 2. Volume comparison
        ax2 = axes[0, 1]
        ax2.bar(df_minute['timestamp'], df_minute['volume'], alpha=0.6, width=0.0003, label='1-min Volume')
        ax2.bar(df_5min['timestamp'], df_5min['volume'], alpha=0.8, width=0.002, color='red', label='5-min Volume')
        ax2.set_title('Volume Comparison')
        ax2.set_ylabel('Volume')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # 3. High-Low spread
        ax3 = axes[1, 0]
        minute_spread = df_minute['high'] - df_minute['low']
        five_min_spread = df_5min['high'] - df_5min['low']
        
        ax3.plot(df_minute['timestamp'], minute_spread, 'b-', alpha=0.7, label='1-min Spread')
        ax3.plot(df_5min['timestamp'], five_min_spread, 'ro-', markersize=8, label='5-min Spread')
        ax3.set_title('High-Low Spread Comparison')
        ax3.set_ylabel('Price Spread')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # 4. OHLC visualization for 5-min candles
        ax4 = axes[1, 1]
        
        for _, row in df_5min.head(10).iterrows():  # Show first 10 candles
            x = row['timestamp']
            open_price = row['open']
            high_price = row['high']
            low_price = row['low']
            close_price = row['close']
            
            # Candlestick colors
            color = 'green' if close_price >= open_price else 'red'
            
            # High-low line
            ax4.plot([x, x], [low_price, high_price], color='black', linewidth=1)
            
            # Body rectangle
            body_height = abs(close_price - open_price)
            body_bottom = min(open_price, close_price)
            
            rect = plt.Rectangle((x, body_bottom), timedelta(minutes=1), body_height, 
                               facecolor=color, alpha=0.7, edgecolor='black')
            ax4.add_patch(rect)
        
        ax4.set_title('5-min Candlestick Chart (First 10 Candles)')
        ax4.set_ylabel('Price')
        ax4.grid(True, alpha=0.3)
        
        # Format time axes
        for ax in axes.flat:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        
        # Save plot
        plot_filename = f"candle_reconstruction_{symbol.replace('-', '_')}.png"
        plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
        print(f"📊 Plot saved as: {plot_filename}")
        
        # Statistics
        print(f"\n📊 RECONSTRUCTION STATISTICS:")
        print(f"   1-min candles: {len(df_minute)}")
        print(f"   5-min candles: {len(df_5min)}")
        print(f"   Compression ratio: {len(df_minute)/len(df_5min):.1f}:1")
        print(f"   Volume preservation: {df_5min['volume'].sum()/df_minute['volume'].sum():.3f}")
        
        plt.show()
        
    except Exception as e:
        print(f"❌ Error creating candle plots: {e}")
    finally:
        conn.close()

def quick_summary():
    """Quick summary of both tasks"""
    config = get_config()
    
    try:
        conn = psycopg2.connect(**config.database.to_dict())
        
        print("📊 QUICK SUMMARY - BOTH TASKS")
        print("=" * 40)
        
        with conn.cursor() as cur:
            # Task 1 summary
            cur.execute("SELECT COUNT(*) FROM interpolated_trading_tickers")
            interp_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(DISTINCT symbol) FROM interpolated_trading_tickers")
            interp_symbols = cur.fetchone()[0] if cur.fetchone() else 0
            
            print(f"TASK 1 (IV Interpolation):")
            print(f"   Interpolated rows: {interp_count:,}")
            print(f"   Symbols: {interp_symbols}")
            
            # Task 2 summary
            cur.execute("""
            SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'reconstructed_candles')
            """)
            table_exists = cur.fetchone()[0]
            
            if table_exists:
                cur.execute("SELECT COUNT(*) FROM reconstructed_candles")
                candle_count = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(DISTINCT symbol) FROM reconstructed_candles")
                candle_symbols = cur.fetchone()[0] if cur.fetchone() else 0
                
                print(f"\nTASK 2 (Candle Reconstruction):")
                print(f"   Reconstructed candles: {candle_count:,}")
                print(f"   Symbols: {candle_symbols}")
            else:
                print(f"\nTASK 2 (Candle Reconstruction): Not yet run")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check results for both tasks')
    parser.add_argument('--task', choices=['interpolation', 'candles', 'both'], 
                       default='both', help='Which task results to check')
    parser.add_argument('--quick', action='store_true', help='Quick summary only')
    parser.add_argument('--plot', action='store_true', help='Generate plots')
    parser.add_argument('--symbol', help='Specific symbol to analyze/plot')
    
    args = parser.parse_args()
    
    if args.quick:
        quick_summary()
    elif args.plot:
        if args.task == 'candles':
            plot_candle_reconstruction(args.symbol)
        elif args.task == 'interpolation':
            print("Use previous plot functionality for IV interpolation")
        else:
            print("Specify --task candles or --task interpolation for plotting")
    else:
        if args.task in ['interpolation', 'both']:
            check_iv_interpolation_results()
        
        if args.task in ['candles', 'both']:
            if args.task == 'both':
                print("\n" + "="*60)
            check_candle_reconstruction_results()