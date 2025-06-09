#!/usr/bin/env python3
"""
Quick Sample Data Generator - Fixed for PostgreSQL
Usage: python generate_sample_candles.py
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2.extras

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from config import get_config
from database.connection import DatabaseManager

def generate_sample_candle_data(num_symbols: int = 5, hours: int = 24):
    """Generate sample 1-minute candle data for testing (PostgreSQL compatible)"""
    print(f"🔧 GENERATING: Sample 1-minute candle data ({num_symbols} symbols, {hours} hours)")
    
    try:
        config = get_config()
        db_manager = DatabaseManager(config.database)
        
        # Sample symbols (Bitcoin options)
        symbols = [
            'btc-20mar23-24500-c',
            'btc-20mar23-25000-c', 
            'btc-20mar23-25500-c',
            'btc-20mar23-24500-p',
            'btc-20mar23-25000-p'
        ][:num_symbols]
        
        start_time = datetime(2023, 3, 20, 9, 0)  # 9 AM
        end_time = start_time + timedelta(hours=hours)
        
        all_candles = []
        
        for symbol in symbols:
            print(f"   📊 Generating candles for {symbol}...")
            
            # Generate minute-by-minute timeline
            current_time = start_time
            base_price = 25000 + np.random.normal(0, 500)  # Random base price around $25k
            
            symbol_candles = []
            
            while current_time < end_time:
                # Simulate price movement (random walk)
                price_change = np.random.normal(0, 10)  # $10 std dev per minute
                
                # Calculate OHLC for this minute
                open_price = base_price
                close_price = base_price + price_change
                
                # High and low around open/close
                high_price = max(open_price, close_price) + abs(np.random.normal(0, 3))
                low_price = min(open_price, close_price) - abs(np.random.normal(0, 3))
                
                # Volume (random but realistic)
                volume = max(0, np.random.exponential(50))
                
                symbol_candles.append((
                    symbol,
                    current_time,
                    round(open_price, 2),
                    round(high_price, 2),
                    round(low_price, 2),
                    round(close_price, 2),
                    round(volume, 4)
                ))
                
                # Update for next minute
                base_price = close_price
                current_time += timedelta(minutes=1)
            
            all_candles.extend(symbol_candles)
            print(f"      ✅ Generated {len(symbol_candles):,} candles")
        
        print(f"   📈 Total generated: {len(all_candles):,} 1-minute candles")
        
        # Insert into database using PostgreSQL-compatible method
        insert_query = """
        INSERT INTO minute_candles (symbol, timestamp, open, high, low, close, volume)
        VALUES %s
        """
        
        print(f"   💾 Saving to database...")
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur, insert_query, all_candles,
                    template=None, page_size=1000
                )
            conn.commit()
        
        print(f"✅ SUCCESS: Sample candle data generated and saved")
        print(f"   📊 Total candles: {len(all_candles):,}")
        print(f"   🎯 Symbols: {num_symbols}")
        print(f"   ⏰ Time range: {start_time} to {end_time}")
        
        # Quick verification
        count_query = "SELECT COUNT(*) FROM minute_candles"
        total_count = db_manager.execute_query(count_query, fetch=True)[0][0]
        print(f"   ✅ Verification: {total_count:,} rows in minute_candles table")
        
        return True
        
    except Exception as e:
        print(f"❌ FAILED: Sample data generation failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Quick Sample Candle Data Generator")
    print("=" * 50)
    
    success = generate_sample_candle_data(num_symbols=5, hours=24)
    
    if success:
        print("\n🎯 NEXT STEPS:")
        print("1. Test candle reconstruction: python main.py --test --task candles")
        print("2. Check results: python check_results.py --task candles")
    else:
        print("\n❌ Please check the error above and try again")