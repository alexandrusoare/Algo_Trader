from utils.config import DB_PATH
from clients.OandaAPI import OandaAPI
from database.db import CandleDatabase


oanda = OandaAPI()
db = CandleDatabase(DB_PATH)
