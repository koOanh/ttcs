# utils/__init__.py
# Import common classes to make them available directly from the package
from .coin_market import CoinMarketCapAPI
from .posgres_pool import PostgresManager, DatabaseError

# Define what gets imported with "from utils import *"
__all__ = ['CoinMarketCapAPI', 'PostgresManager', 'DatabaseError']

# Package-level variables
__version__ = '0.1.0'
