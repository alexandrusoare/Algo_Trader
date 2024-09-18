from clients.OandaAPI import OandaAPI
from database.db import CandleDatabase


class Storage:
    def __init__(self, db_name: str, oanda_api: OandaAPI):
        """
        Initialize the Storage class with a database connection and an instance of the OandaAPI class.

        :param db_name: The name of the SQLite database file.
        :param oanda_api: An instance of the OandaAPI class for fetching candles.
        """
        self.db = CandleDatabase(db_name)
        self.oanda_api = oanda_api
