import os

from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
OANDA_URL = os.getenv("OANDA_URL")

SECURE_HEADER = {"Authorization": f"Bearer {API_KEY}"}
