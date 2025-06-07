# src/interpolation/batch_processor.py
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict
import pandas as pd
import logging
from interpolation.core import IVInterpolator


logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self, db_manager, config):
        self.db_manager = db_manager
        self.config = config
        self.interpolator = IVInterpolator(
            method=config.interpolation.method,
            min_points=config.interpolation.min_data_points
        )
    
    def get_symbols(self) -> List[str]:
        """Get list of unique symbols to process"""
        query = """
        SELECT DISTINCT symbol 
        FROM trading_tickers 
        WHERE date >= %s AND date <= %s
        ORDER BY symbol
        """
        # Define your date range
        start_date = '2023-03-15'
        end_date = '2023-03-26'
        
        results = self.db_manager.execute_query(
            query, (start_date, end_date), fetch=True
        )
        return [row[0] for row in results]
    
    def process_symbol(self, symbol: str) -> Dict:
        """Process a single symbol"""
        try:
            # Fetch data for symbol
            query = """
            SELECT * FROM trading_tickers 
            WHERE symbol = %s 
            ORDER BY date
            """
            data = pd.read_sql(query, self.db_manager.get_connection(), 
                               params=(symbol,))
            
            # Interpolate
            interpolated = self.interpolator.interpolate_symbol(data)
            
            if interpolated is not None:
                # Save to database
                self._save_results(interpolated)
                return {'symbol': symbol, 'status': 'success', 
                        'rows': len(interpolated)}
            else:
                return {'symbol': symbol, 'status': 'skipped', 
                        'reason': 'insufficient data'}
                
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            return {'symbol': symbol, 'status': 'error', 'error': str(e)}
    
    def run_parallel(self):
        """Run batch processing in parallel"""
        symbols = self.get_symbols()
        logger.info(f"Processing {len(symbols)} symbols")
        
        with ProcessPoolExecutor(max_workers=self.config.processing.max_workers) as executor:
            futures = {executor.submit(self.process_symbol, symbol): symbol 
                      for symbol in symbols}
            
            for future in as_completed(futures):
                result = future.result()
                logger.info(f"Processed {result['symbol']}: {result['status']}")