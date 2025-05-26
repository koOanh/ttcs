# crypto_api.py
import requests
from typing import Dict, Any

class CoinMarketCapAPI:
    """Client for interacting with the CoinMarketCap API."""

    BASE_URL = "https://pro-api.coinmarketcap.com/v1"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            'X-CMC_PRO_API_KEY': self.api_key,
            'Accept': 'application/json'
        }

    def get_latest_listings(self, start: int = 1, limit: int = 200,
                           convert: str = 'USD') -> Dict[str, Any]:
        endpoint = f"{self.BASE_URL}/cryptocurrency/listings/latest"
        params = {
            'start': start,
            'limit': limit,
            'convert': convert
        }

        return self._make_request(endpoint, params)

    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}
