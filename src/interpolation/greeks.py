'''
Not required before 10 June
'''


# src/interpolation/greeks.py
import numpy as np
from scipy.stats import norm

class BlackScholesGreeks:
    @staticmethod
    def calculate_greeks(S, K, T, r, sigma, option_type='call'):
        """
        Calculate Black-Scholes Greeks
        S: Underlying price
        K: Strike price
        T: Time to maturity
        r: Risk-free rate
        sigma: Implied volatility
        """
        d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
        d2 = d1 - sigma*np.sqrt(T)
        
        if option_type == 'call':
            delta = norm.cdf(d1)
            theta = (-S*norm.pdf(d1)*sigma/(2*np.sqrt(T)) - 
                     r*K*np.exp(-r*T)*norm.cdf(d2)) / 365
        else:  # put
            delta = norm.cdf(d1) - 1
            theta = (-S*norm.pdf(d1)*sigma/(2*np.sqrt(T)) + 
                     r*K*np.exp(-r*T)*norm.cdf(-d2)) / 365
        
        gamma = norm.pdf(d1) / (S*sigma*np.sqrt(T))
        vega = S*norm.pdf(d1)*np.sqrt(T) / 100
        rho = K*T*np.exp(-r*T)*norm.cdf(d2 if option_type == 'call' else -d2) / 100
        
        return {
            'delta': delta,
            'gamma': gamma,
            'theta': theta,
            'vega': vega,
            'rho': rho
        }