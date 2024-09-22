from utils.config import DB_PATH
from clients.OandaAPI import OandaAPI
from database.db import CandleDatabase


oanda = OandaAPI()
db = CandleDatabase(DB_PATH)

# candles = oanda.fetch_candles_in_parallel("EUR_USD", "M1", "21/12/2023 00:00:00", "31/12/2023 00:00:00")
candles = oanda.get_candles("EUR_USD", "M1", count=10)
# df = db.convert_to_dataframe(candles)

print(candles)
