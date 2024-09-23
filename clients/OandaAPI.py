from concurrent.futures import ThreadPoolExecutor
from typing import Optional
import requests
from utils.config import OANDA_URL, SECURE_HEADER
import logging

from utils.helpers import (
    compute_candle_count,
    compute_date_chunks,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

MAX_CANDLES_PER_REQUEST = 5000


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

    def get_candles(
        self, pair_name, granularity="M1", count=None, date_from=None, date_to=None
    ):
        """
        Makes the API call to OANDA to fetch candles.

        :param pair_name: The name of the currency pair (e.g., "EUR_USD").
        :param granularity: The time frame for each candle (e.g., "M1", "H1").
        :param count: The number of candles to retrieve. If None, fetch based on date range.
        :param date_from: Start date for fetching candles (datetime object). If None, use `count`.
        :param date_to: End date for fetching candles (datetime object). If None, use `count`.
        :return: The JSON response from the API or None in case of an error.
        """
        url = f"{self.oanda_url}/instruments/{pair_name}/candles"
        params = {"granularity": granularity, "price": "MBA"}

        if count:
            params["count"] = count
        if date_from:
            params["from"] = int(date_from.timestamp())
        if date_to:
            params["to"] = int(date_to.timestamp())

        if not count and not date_from and not date_to:
            raise ValueError(
                "You must provide either `count` or a date range (`date_from` or `date_to`)."
            )

        try:
            response = self.session.get(url, params=params, headers=self.secure_header)
            response.raise_for_status()
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

    def fetch_candles_in_parallel(
        self,
        pair_name: str,
        granularity: str,
        date_from: str,
        date_to: Optional[str] = None,
        max_workers: int = 10,
    ):
        """
        Fetches candles in parallel using threading for the specified date range.

        :param pair_name: The currency pair to fetch candles for (e.g., "EUR_USD").
        :param granularity: The granularity of the candles (e.g., "M1", "H1").
        :param date_from: The start date for fetching candles (string format: 'YYYY-MM-DD HH:MM:SS').
        :param date_to: The end date for fetching candles (string format: 'YYYY-MM-DD HH:MM:SS'). If None, fetch until now.
        :param max_workers: The maximum number of worker threads.
        :return: A list of fetched candles.
        """
        logging.info(f"Fetching candles for {pair_name} and granularity {granularity}")
        total_candles = compute_candle_count(granularity, date_from, date_to)

        date_chunks = compute_date_chunks(
            granularity,
            date_from,
            date_to,
            total_candles,
            MAX_CANDLES_PER_REQUEST,
        )
        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    self.get_candles,
                    pair_name,
                    granularity,
                    date_from=chunk[0],
                    date_to=chunk[1],
                )
                for chunk in date_chunks
            ]

            for future in futures:
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    logging.error(f"Error occurred while fetching candles: {e}")
        logging.info(
            f"Finished fetching candles for {pair_name} with granularity {granularity}"
        )
        return results
