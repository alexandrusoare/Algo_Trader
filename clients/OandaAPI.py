import requests
from utils.config import OANDA_URL, SECURE_HEADER
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class OandaAPI:
    def __init__(self, secure_header=SECURE_HEADER, oanda_url=OANDA_URL):
        """
        Initializes the OandaAPI class with a secure header and base URL.

        :param secure_header: The OANDA API secure header containing the API_KEY
        :param oanda_url: The base URL for the OANDA API
        """
        self.oanda_url = oanda_url
        self.secure_header = secure_header
        self.session = requests.Session()

    def fetch_candles(self, pair_name, granularity="M1", count=10):
        """
        Fetches the last 'n' candles for a given currency pair.

        :param pair_name: The name of the currency pair (e.g., "EUR_USD")
        :param granularity: The time frame for each candle (default "M1" for 1-minute candles)
        :param count: The number of candles to retrieve (default 10)
        :return: JSON response containing the candlestick data or None if an error occurred
        """
        url = f"{self.oanda_url}/instruments/{pair_name}/candles"
        params = {"granularity": granularity, "price": "MBA", "count": count}

        try:
            response = self.session.get(
                url, params=params, headers=self.secure_header, timeout=10
            )
            response.raise_for_status()
            logging.info(
                f"Successfully fetched {count} candles for {pair_name} with granularity {granularity}"
            )
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            logging.error(f"Connection error occurred: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            logging.error(f"Timeout error occurred: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            logging.error(f"An error occurred: {req_err}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")

        return None
