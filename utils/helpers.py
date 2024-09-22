from typing import Optional
import pandas as pd
from dateutil.parser import parse
import datetime as dt

INCREMENTS = {"M1": 1, "M5": 5, "M15": 15, "M30": 30, "H1": 60, "H4": 240, "D": 1440}


def get_utc_dt_from_string(date_str: str) -> dt.datetime:
    """
    Converts a date string in day/month/year format into a UTC datetime object.

    :param date_str: The date string in the format 'DD/MM/YYYY HH:MM:SS'.
    :return: A datetime object in UTC.
    """
    d = parse(date_str, dayfirst=True)

    return d.replace(tzinfo=dt.timezone.utc)


def compute_candle_count(
    granularity: str, date_from: str, date_to: Optional[str] = None
) -> int:
    """
    Computes the number of candles to fetch based on granularity and the date range (using get_utc_dt_from_string for parsing).

    :param granularity: The granularity of the candles (e.g., "M1", "H1", "D").
    :param date_from: The start date (string) in the format 'DD/MM/YYYY HH:MM:SS'.
    :param date_to: The end date (string) in the format 'DD/MM/YYYY HH:MM:SS'. If None, uses the current date and time.
    :return: The number of candles to fetch.
    """
    utc_date_from = get_utc_dt_from_string(date_from)

    if date_to is None:
        utc_date_to = dt.datetime.now(dt.timezone.utc)
    else:
        utc_date_to = get_utc_dt_from_string(date_to)

    time_diff_minutes = (utc_date_to - utc_date_from).total_seconds() / 60

    if granularity not in INCREMENTS:
        raise ValueError(f"Unsupported granularity: {granularity}")

    candle_count = time_diff_minutes // INCREMENTS[granularity]

    return int(candle_count)


def compute_date_chunks(
    granularity: str,
    date_from: str,
    date_to: str,
    total_candles: int,
    max_candles_per_request: int,
):
    """
    Computes the date ranges for each chunk based on the total candles to fetch and max candles per request.

    :param granularity: The granularity of the candles (e.g., "M1", "H1", "D").
    :param date_from: The start date (string) in the format 'DD/MM/YYYY HH:MM:SS'.
    :param date_to: The end date (string) in the format 'DD/MM/YYYY HH:MM:SS'.
    :param total_candles: Total number of candles to fetch.
    :param max_candles_per_request: Maximum number of candles that can be fetched per request.
    :return: List of tuples containing date_from and date_to for each chunk.
    """
    utc_date_from = get_utc_dt_from_string(date_from)

    granularity_minutes = INCREMENTS[granularity]

    chunks = []
    remaining_candles = total_candles
    current_date_from = utc_date_from

    while remaining_candles > 0:
        candles_in_chunk = min(max_candles_per_request, remaining_candles)
        time_in_chunk = candles_in_chunk * granularity_minutes
        current_date_to = current_date_from + dt.timedelta(minutes=time_in_chunk)

        chunks.append(
            (
                current_date_from.strftime("%d-%m-%Y %H:%M:%S"),
                current_date_to.strftime("%d-%m-%Y %H:%M:%S"),
            )
        )

        current_date_from = current_date_to
        remaining_candles -= candles_in_chunk

    return chunks


def get_trading_session_status(index: pd.DatetimeIndex) -> pd.Series:
    """
    Returns a Pandas Series indicating whether the market is open (True) or closed (False).

    :param index: Pandas DatetimeIndex of timestamps.
    :return: Pandas Series with True for open market hours (Monday to Friday) and False for weekends.
    """
    index = pd.DatetimeIndex(index)
    return ~index.weekday.isin([5, 6])
