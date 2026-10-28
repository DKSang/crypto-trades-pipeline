"""Unit tests for the pure mapping cell of nb_build_dim_pair (run: python -m unittest discover tests)."""
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK = ROOT / "fabric/Crypto-DEV/nb_build_dim_pair.Notebook/notebook-content.py"
SAMPLE = ROOT / "data/asset_pairs_sample.json"
VARIABLES = ROOT / "fabric/Crypto-DEV/vl_crypto.VariableLibrary/variables.json"


def load_mapping_cell():
    # The mapping lives in the notebook's first code cell, which has no Spark dependency.
    cells = NOTEBOOK.read_text(encoding="utf-8").split("# CELL ********************")
    scope = {}
    exec(cells[1], scope)
    return scope


class DimPairMappingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_mapping_cell()
        cls.result = json.loads(SAMPLE.read_text(encoding="utf-8"))["result"]
        variables = {v["name"]: v["value"] for v in json.loads(VARIABLES.read_text())["variables"]}
        cls.tracked = variables["tracked_pairs"].split(",")

    def test_legacy_names_become_ws_v2_names(self):
        self.assertEqual(self.m["ws_v2_name"]("XBT/USD"), "BTC/USD")
        self.assertEqual(self.m["ws_v2_name"]("XDG/USD"), "DOGE/USD")
        self.assertEqual(self.m["ws_v2_name"]("ETH/USD"), "ETH/USD")

    def test_all_twenty_tracked_pairs_are_built_under_ws_v2_names(self):
        rows = self.m["tracked_rows"](self.result, self.tracked)
        self.assertEqual(sorted(r["symbol"] for r in rows), sorted(self.tracked))
        self.assertEqual(len(rows), 20)
        self.assertTrue(all(r["is_tracked"] for r in rows))

    def test_row_carries_base_quote_status(self):
        rows = {r["symbol"]: r for r in self.m["tracked_rows"](self.result, self.tracked)}
        self.assertEqual(rows["BTC/USD"]["base"], "BTC")
        self.assertEqual(rows["BTC/USD"]["quote"], "USD")
        self.assertEqual(rows["DOGE/USD"]["base"], "DOGE")
        self.assertEqual(rows["BTC/USD"]["status"], "online")

    def test_untracked_pairs_are_left_out(self):
        rows = self.m["tracked_rows"](self.result, self.tracked)
        self.assertNotIn("0G/EUR", [r["symbol"] for r in rows])

    def test_tracked_pair_missing_from_snapshot_fails_loudly(self):
        with self.assertRaisesRegex(ValueError, "FAKE/USD"):
            self.m["tracked_rows"](self.result, self.tracked + ["FAKE/USD"])

    def test_tracked_list_tolerates_spaces_and_trailing_comma(self):
        self.assertEqual(self.m["parse_tracked"](" BTC/USD, ETH/USD,"), ["BTC/USD", "ETH/USD"])


if __name__ == "__main__":
    unittest.main()
