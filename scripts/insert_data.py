from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import sqlite3
from clients.OandaAPI import OandaAPI
from database.db import CandleDatabase
from utils.config import DB_PATH
import threading

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

instruments = ["NAS100_USD", "XAU_USD", "EUR_USD", "GBP_USD", "US30_USD"]
granularities = ["M1", "M2", "M5", "M15", "H1", "H4", "D"]

MAX_WORKERS = 10
DATE_FROM = "2014-08-31 00:00:00"
DATE_TO = "2024-08-31 00:00:00"
SEMAPHORE_LIMIT = 5

oanda_api = OandaAPI()
db = CandleDatabase(DB_PATH)
db_semaphore = threading.Semaphore(SEMAPHORE_LIMIT)


def fetch_and_store(pair_name, granularity, date_from, date_to, db_name):
    """
    Fetch candles for a specific pair and granularity, convert to DataFrame, and store in the database.

    :param pair_name: The instrument pair (e.g., "EUR_USD").
    :param granularity: The granularity (e.g., "M1", "H1").
    :param date_from: The start date in the format 'YYYY-MM-DD HH:MM:SS'.
    :param date_to: The end date in the format 'YYYY-MM-DD HH:MM:SS'.
    :param db_name: The SQLite database file name.
    """
    try:
        with db_semaphore:
            with sqlite3.connect(db_name) as conn:
                db = CandleDatabase(db_name)
                db.conn = conn
                db.cursor = conn.cursor()

                db.create_table(pair_name, granularity)

                candles = oanda_api.fetch_candles_in_parallel(
                    pair_name, granularity, date_from, date_to
                )

                df = db.convert_to_dataframe(candles)

                if not df.empty:
                    logging.info(
                        f"Fetched {len(df)} rows for {pair_name} at {granularity}"
                    )
                    db.insert_candles_from_dataframe(pair_name, granularity, df)
                else:
                    logging.warning(f"No data for {pair_name} at {granularity}")

    except Exception as e:
        logging.error(f"Error fetching {pair_name} at {granularity}: {e}")


def main():
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []

        for instrument in instruments:
            for granularity in granularities:
                logging.info(f"Submitting task for {instrument} at {granularity}")
                futures.append(
                    executor.submit(
                        fetch_and_store,
                        instrument,
                        granularity,
                        DATE_FROM,
                        DATE_TO,
                        DB_PATH,
                    )
                )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
