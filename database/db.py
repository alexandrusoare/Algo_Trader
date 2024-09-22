import sqlite3
from typing import Optional
from utils.config import DB_PATH
import pandas as pd


class CandleDatabase:
    def __init__(self, db_name: Optional[str]):
        if db_name is None and DB_PATH is None:
            raise ValueError("Missing database path")
        else:
            self.db_name = db_name or DB_PATH
            self.conn = sqlite3.connect(self.db_name)  # type: ignore
            self.cursor = self.conn.cursor()

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
                in_session BOOLEAN NOT NULL,
                UNIQUE (timestamp)
            )
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

    def insert_candles_from_dataframe(
        self, instrument: str, granularity: str, candle_data: pd.DataFrame
    ) -> None:
        """
        Insert multiple candles from a Pandas DataFrame into the corresponding table.

        :param instrument: The currency pair (e.g., "EUR_USD").
        :param granularity: The granularity of the candles (e.g., "M1", "H1").
        :param candle_data: A Pandas DataFrame containing the candle data.
        """
        table_name = f"{instrument}_{granularity}".replace("/", "_")

        candle_data.to_sql(table_name, self.conn, if_exists="append", index=False)

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
