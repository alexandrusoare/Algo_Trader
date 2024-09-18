import unittest
from utils.helpers import get_utc_dt_from_string, compute_candle_count
from datetime import datetime, timezone


class TestUtils(unittest.TestCase):
    def test_get_utc_dt_from_string(self):
        date_str = "01/01/2023 12:00:00"
        result = get_utc_dt_from_string(date_str)
        expected = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        self.assertEqual(result, expected, "The UTC datetime conversion failed.")

    def test_compute_candle_count(self):
        date_from = "01/01/2023 00:00:00"
        date_to = "01/01/2023 01:23:00"
        result = compute_candle_count("M1", date_from, date_to)

        expected = 83
        self.assertEqual(
            result, expected, "The number of M1 candles calculated is incorrect."
        )


if __name__ == "__main__":
    unittest.main()
