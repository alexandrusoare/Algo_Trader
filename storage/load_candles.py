from clients.OandaAPI import OandaAPI

OANDA = OandaAPI()

print(OANDA.fetch_candles("EUR_USD"))
