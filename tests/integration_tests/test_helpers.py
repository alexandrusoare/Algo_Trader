import unittest
from utils.helpers import compute_date_chunks, get_utc_dt_from_string


class TestComputeDateChunks(unittest.TestCase):
    def test_single_chunk(self):
        # Case where total candles are less than max_candles_per_request (no splitting)
        granularity = "M1"
        date_from = "01/01/2023 00:00:00"
        date_to = "01/01/2023 02:00:00"  # 120 candles (M1 granularity)
        total_candles = 120
        max_candles_per_request = 5000

        chunks = compute_date_chunks(
            granularity, date_from, date_to, total_candles, max_candles_per_request
        )

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0][0], get_utc_dt_from_string(date_from))
        self.assertEqual(chunks[0][1], get_utc_dt_from_string(date_to))

    def test_multiple_chunks(self):
        granularity = "M1"
        date_from = "01/01/2023 00:00:00"
        date_to = "03/01/2023 00:00:00"
        total_candles = 2880
        max_candles_per_request = 1000

        chunks = compute_date_chunks(
            granularity, date_from, date_to, total_candles, max_candles_per_request
        )

        self.assertEqual(len(chunks), 3)

        first_chunk_duration = (chunks[0][1] - chunks[0][0]).total_seconds() / 60
        self.assertEqual(first_chunk_duration, 1000)

        last_chunk_duration = (chunks[-1][1] - chunks[-1][0]).total_seconds() / 60
        self.assertEqual(last_chunk_duration, 880)
        print(chunks)

    def test_edge_case(self):
        granularity = "M1"
        date_from = "01/01/2023 00:00:00"
        date_to = "01/01/2023 10:00:00"
        total_candles = 600
        max_candles_per_request = 600

        chunks = compute_date_chunks(
            granularity, date_from, date_to, total_candles, max_candles_per_request
        )

        self.assertEqual(len(chunks), 1)

        self.assertEqual(chunks[0][0], get_utc_dt_from_string(date_from))
        self.assertEqual(chunks[0][1], get_utc_dt_from_string(date_to))


if __name__ == "__main__":
    unittest.main()
