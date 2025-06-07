import pandas as pd
import numpy as np
from datetime import timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class IVInterpolator:
    """Core interpolation engine for IV data"""
    
    def __init__(self, method: str = 'linear', min_points: int = 10):
        self.method = method
        self.min_points = min_points
    
    def interpolate_symbol(self, symbol_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Interpolate IV data for a single symbol
        
        Args:
            symbol_data: DataFrame with hourly data for one symbol
            
        Returns:
            DataFrame with minute-level interpolated data or None if insufficient data
        """
        if len(symbol_data) < self.min_points:
            logger.warning(f"Insufficient data points: {len(symbol_data)} < {self.min_points}")
            return None
        
        try:
            # Prepare data
            symbol_data = symbol_data.sort_values('date').reset_index(drop=True)
            symbol_data['date'] = pd.to_datetime(symbol_data['date'])
            
            # Check for reasonable time range
            time_range = symbol_data['date'].max() - symbol_data['date'].min()
            if time_range > timedelta(days=30):  # Skip symbols with huge time gaps
                logger.warning(f"Time range too large: {time_range}")
                return None
            
            # Create minute-level timeline
            timeline = pd.date_range(
                start=symbol_data['date'].min(),
                end=symbol_data['date'].max(),
                freq='1min'
            )
            
            # Safety check for memory
            if len(timeline) > 100000:  # ~69 days at 1-minute frequency
                logger.warning(f"Timeline too long: {len(timeline)} minutes")
                return None
            
            # Merge with timeline
            interp_df = pd.DataFrame({'date': timeline})
            merged = interp_df.merge(symbol_data, on='date', how='left')
            
            # Interpolate critical numerical columns
            numeric_cols = ['iv', 'underlying_price', 'time_to_maturity']
            for col in numeric_cols:
                if col in merged.columns:
                    merged[col] = merged[col].interpolate(method=self.method)
            
            # Forward fill categorical and other columns
            fill_cols = ['symbol', 'strike', 'callput', 'interest_rate', 
                        'mark_price', 'index_price', 'volume', 'quote_volume', 'record_time']
            for col in fill_cols:
                if col in merged.columns:
                    merged[col] = merged[col].fillna(method='ffill')
            
            # Mark interpolated vs original data
            merged['is_interpolated'] = merged['symbol'].isna()
            
            # Remove rows that couldn't be filled
            merged = merged.dropna(subset=['symbol', 'iv', 'underlying_price', 'time_to_maturity'])
            
            if merged.empty:
                logger.warning("No valid data after interpolation")
                return None
            
            logger.debug(f"Interpolated {len(symbol_data)} → {len(merged)} rows")
            return merged
            
        except Exception as e:
            logger.error(f"Interpolation failed: {e}")
            return None