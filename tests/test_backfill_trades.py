"""Seam 1 unit tests for the pure cell of nb_backfill_trades (run: python -m unittest discover tests)."""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK = ROOT / "fabric/Crypto-DEV/nb_backfill_trades.Notebook/notebook-content.py"


def load_pure_cell():
    # The REST-to-Bronze conversion and the paging live in the notebook's first code cell, which has no Spark dependency.
    cells = NOTEBOOK.read_text(encoding="utf-8").split("# CELL ********************")
    scope = {}
    exec(cells[1], scope)
    return scope


class ToBronzeRowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_pure_cell()

    def test_rest_row_becomes_the_bronze_shape_the_collector_sends(self):
        # A real /0/public/Trades row of BTC/USD (2025-10-29); the time is epoch seconds, side and order type are one letter.
        rest = ["112500.00000", "0.00029263", 1761700001.888478, "s", "l", "", 89067725]
        self.assertEqual(
            self.m["to_bronze_row"]("BTC/USD", rest, "2026-10-29T10:00:00+00:00"),
            {"symbol": "BTC/USD", "side": "sell", "price": 112500.0, "qty": 0.00029263, "ord_type": "limit",
             "trade_id": 89067725, "timestamp": "2025-10-29T01:06:41.888478Z",
             "_ingest_path": "backfill", "_load_ts": "2026-10-29T10:00:00+00:00"})

    def test_buy_and_market_letters(self):
        rest = ["112500.10000", "0.00031000", 1761700003.6602204, "b", "m", "", 89067727]
        row = self.m["to_bronze_row"]("BTC/USD", rest, "x")
        self.assertEqual((row["side"], row["ord_type"], row["timestamp"]), ("buy", "market", "2025-10-29T01:06:43.660220Z"))


class ParseTimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_pure_cell()

    def test_iso_time_with_z_becomes_the_since_cursor_in_nanoseconds(self):
        # 1761696000 is 2025-10-29T00:00:00Z (checked with `date -u -d @1761696000`)
        self.assertEqual(self.m["to_ns"](self.m["parse_utc"]("2025-10-29T00:00:00Z")), 1761696000 * 10**9)

    def test_offset_is_honoured_and_no_offset_means_utc(self):
        ns = 1761696000 * 10**9
        self.assertEqual(self.m["to_ns"](self.m["parse_utc"]("2025-10-29T07:00:00+07:00")), ns)
        self.assertEqual(self.m["to_ns"](self.m["parse_utc"]("2025-10-29 00:00:00")), ns)

    def test_microseconds_survive(self):
        self.assertEqual(self.m["to_ns"](self.m["parse_utc"]("2025-10-29T00:00:00.123456Z")), 1761696000123456000)


BASE = 1761700000  # 2025-10-29T00:00:00Z + 16000 s, whole seconds so the ns cursor is exact


class FakeKraken:
    """/0/public/Trades as observed on 2026-10-29: trades with a time at or after `since` (inclusive, so the
    last trade of a page comes again on the next call), at most `page` rows, `last` = the ns time of the
    last row returned, and `since` echoed back when nothing is returned."""

    def __init__(self, trade_count, page):
        # trade_id i happens at BASE + i seconds; REST time is epoch seconds
        self.trades = [["100.0", "1.0", float(BASE + i), "b", "l", "", i] for i in range(1, trade_count + 1)]
        self.page = page
        self.calls = 0

    def fetch(self, since_ns):
        self.calls += 1
        rows = [r for r in self.trades if int(r[2]) * 10**9 >= since_ns][: self.page]
        return rows, (int(rows[-1][2]) * 10**9 if rows else since_ns)


def paged_ids(m, exchange, start_second, end_second=None):
    end = None if end_second is None else float(end_second)
    return [[r[6] for r in page] for page in m["iter_pages"](exchange.fetch, start_second * 10**9, end)]


class IterPagesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_pure_cell()

    def test_empty_range_yields_nothing_and_stops_after_one_call(self):
        exchange = FakeKraken(trade_count=0, page=3)
        self.assertEqual(paged_ids(self.m, exchange, BASE), [])
        self.assertEqual(exchange.calls, 1)

    def test_start_after_the_newest_trade_is_an_empty_range(self):
        exchange = FakeKraken(trade_count=5, page=10)
        self.assertEqual(paged_ids(self.m, exchange, BASE + 100), [])

    def test_range_under_one_page_is_one_page(self):
        exchange = FakeKraken(trade_count=2, page=3)
        self.assertEqual(paged_ids(self.m, exchange, BASE), [[1, 2]])

    def test_range_of_exactly_one_full_page_stops_on_the_next_empty_page(self):
        exchange = FakeKraken(trade_count=3, page=3)
        self.assertEqual(paged_ids(self.m, exchange, BASE), [[1, 2, 3]])

    def test_range_over_several_pages_has_every_trade_once(self):
        # `since` is inclusive, so the last trade of a page comes again at the top of the next one (taking a
        # place in it): it is dropped, and the pages after the first hold one new trade fewer.
        exchange = FakeKraken(trade_count=7, page=3)
        self.assertEqual(paged_ids(self.m, exchange, BASE), [[1, 2, 3], [4, 5], [6, 7]])

    def test_start_in_the_middle_begins_at_that_trade(self):
        exchange = FakeKraken(trade_count=7, page=3)
        self.assertEqual(paged_ids(self.m, exchange, BASE + 4), [[4, 5, 6], [7]])

    def test_stops_at_the_end_time_inside_a_page_and_does_not_fetch_more(self):
        exchange = FakeKraken(trade_count=9, page=4)
        # end is exclusive: trades at BASE+1 .. BASE+5 are before BASE+6
        self.assertEqual(paged_ids(self.m, exchange, BASE, end_second=BASE + 6), [[1, 2, 3, 4], [5]])
        self.assertEqual(exchange.calls, 2)

    def test_end_time_on_a_page_boundary_stops_without_another_call(self):
        exchange = FakeKraken(trade_count=9, page=4)
        self.assertEqual(paged_ids(self.m, exchange, BASE, end_second=BASE + 5), [[1, 2, 3, 4]])
        self.assertEqual(exchange.calls, 2)  # the second page starts with trade 4 again and 5 is not before the end

    def test_end_before_the_first_trade_is_an_empty_range(self):
        exchange = FakeKraken(trade_count=5, page=3)
        self.assertEqual(paged_ids(self.m, exchange, BASE + 3, end_second=BASE + 3), [])

    def test_end_after_the_newest_trade_stops_at_the_newest_trade(self):
        exchange = FakeKraken(trade_count=5, page=3)
        self.assertEqual(paged_ids(self.m, exchange, BASE, end_second=BASE + 1000), [[1, 2, 3], [4, 5]])

    def test_a_cursor_that_does_not_advance_ends_the_range(self):
        # An exchange answering the same page whatever `since` is must not loop forever.
        page = [["1.0", "1.0", float(BASE + 1), "b", "l", "", 1]]
        calls = []

        def stuck(since_ns):
            calls.append(since_ns)
            return page, since_ns

        self.assertEqual(len(list(self.m["iter_pages"](stuck, BASE * 10**9, None))), 1)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
