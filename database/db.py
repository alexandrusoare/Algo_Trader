import logging
import sqlite3
from typing import Optional
from utils.config import DB_PATH
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class CandleDatabase:
    def __init__(self, db_name: Optional[str]):
        if db_name is None and DB_PATH is None:
            raise ValueError("Missing database path")
        else:
            self.db_name = db_name or DB_PATH
            self.conn = sqlite3.connect(self.db_name)  # type: ignore
            self.cursor = self.conn.cursor()
            self.conn.execute("PRAGMA journal_mode=WAL;")

    def create_table(self, instrument: str, granularity: str):
        """
        Creates a table for the specified instrument and granularity if it doesn't already exist.

        :param instrument: The currency pair (e.g., "EUR_USD").
        :param granularity: The granularity of the candles (e.g., "M1", "H1").
        """
        table_name = f"{instrument}_{granularity}".replace("/", "_")
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                UNIQUE (timestamp)
            )
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        logging.info(f"Created table for {instrument} with granularity {granularity}")

    def insert_candles_from_dataframe(
        self,
        instrument: str,
        granularity: str,
        candle_data: pd.DataFrame,
        batch_size: int = 200000,
    ) -> None:
        """
        Insert multiple candles from a Pandas DataFrame into the corresponding table, wrapped in a transaction.

        :param instrument: The currency pair (e.g., "EUR_USD").
        :param granularity: The granularity of the candles (e.g., "M1", "H1").
        :param candle_data: A Pandas DataFrame containing the candle data.
        :param batch_size: The number of rows to insert per batch (default: 200,000 rows).
        """
        table_name = f"{instrument}_{granularity}".replace("/", "_")

        try:
            self.cursor.execute("BEGIN TRANSACTION")

            for i in range(0, len(candle_data), batch_size):
                batch_df = candle_data.iloc[i : i + batch_size]
                batch_df.to_sql(table_name, self.conn, if_exists="append", index=False)

            self.conn.commit()
            logging.info(f"Inserted {len(candle_data)} rows into {table_name}")

        except sqlite3.DatabaseError as e:
            self.conn.rollback()
            logging.error(f"Error inserting into {table_name}: {e}")
            raise
        logging.info(
            f"Finished adding data for {instrument} with granularity{granularity} in db!"
        )

    def convert_to_dataframe(self, candles: list) -> pd.DataFrame:
        """
        Converts a list of fetched candle data into a pandas DataFrame, sorted by Timestamp.

        :param candles: A list of fetched candle data (from Oanda API).
        :return: A pandas DataFrame containing the candles, ordered by Timestamp.
        """
        if not candles:
            raise ValueError("No candle data provided")

        candle_data = []
        for batch in candles:
            for candle in batch["candles"]:
                if candle["complete"]:
                    candle_data.append(
                        {
                            "Timestamp": candle["time"],
                            "Open": candle["mid"]["o"],
                            "High": candle["mid"]["h"],
                            "Low": candle["mid"]["l"],
                            "Close": candle["mid"]["c"],
                            "Volume": candle["volume"],
                        }
                    )

        df = pd.DataFrame(candle_data)

        df["Timestamp"] = pd.to_datetime(df["Timestamp"])
        df.sort_values(by="Timestamp", inplace=True)
        return df

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()
