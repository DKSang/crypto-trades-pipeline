# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "039e4fc7-5892-4238-9c2f-a260cb73fc9f",
# META       "default_lakehouse_name": "lh_crypto",
# META       "default_lakehouse_workspace_id": "c8d6538e-e60c-467b-8440-dec5dae4d897",
# META       "known_lakehouses": [
# META         { "id": "039e4fc7-5892-4238-9c2f-a260cb73fc9f" }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

library_only = True  # nb_reconcile_hot_gold then defines its functions and skips its run

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_reconcile_hot_gold

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Tests of nb_reconcile_hot_gold (issue #14), run in Fabric on scratch tables in schema `test` (prefix rc_);
# the real tables are never touched. The hot-path totals are passed in as a DataFrame, so Eventhouse is not
# read here (nb_reconcile_hot_gold's own run does that). %run also runs the parameters cell of the notebook
# under test, so the names bronze_table, fact_table, reconcile_table, from_date and kql_database are taken here and not used.
#   1 clean_day           hot, Bronze and Gold agree  -> a row for every tracked pair (a pair without trades too), gap and causes 0
#   2 rerun_replaces      the same day again, then with a changed hot total  -> still one row per pair and day, the new numbers
#   3 day_without_hot     Bronze and Gold hold a day the hot path has no total for  -> no row for that day
#   4 duplicates          the live stream sent a trade twice  -> the gap is the duplicate, nothing unexplained
#   5 late                a live trade is in Bronze and not in Gold yet  -> hot is above Gold by that trade, cause late
#   6 backfill            Gold holds trades the live stream never delivered  -> Gold above hot, cause backfill
#   7 unexplained         hot counts an event Bronze never got  -> the gap is not explained by any cause
#   8 causes_add_up       several causes on one pair and day  -> gap = duplicates + late - backfill + unexplained, count and notional
#   9 test_event          a sender other than the collector (nb_test_whale_alert, path 'test') reached the hot path and Bronze, Silver
#                         rejected it  -> cause late, not unexplained
# Prices and quantities are binary fractions, so every notional is exact and compared with ==. Expected values are
# written out by hand, not computed by the code under test. Results go to Files/checks/nb_test_reconcile_hot_gold.json;
# the run fails if a case fails.

import json
import traceback
from datetime import date

RESULT_PATH = "Files/checks/nb_test_reconcile_hot_gold.json"
BRONZE = "test.rc_bronze_trades"
FACT = "test.rc_fact_trade"
OUT = "test.rc_reconcile_daily"
TRACKED = ["BTC/USD", "ETH/USD", "SOL/USD"]
DAY_1, DAY_2 = date(2026, 10, 28), date(2026, 10, 29)

BRONZE_DDL = ("symbol STRING, side STRING, price DOUBLE, qty DOUBLE, ord_type STRING, "
              "trade_id BIGINT, timestamp STRING, _ingest_path STRING, _load_ts STRING")
FACT_DDL = ("symbol STRING, trade_id BIGINT, trade_ts TIMESTAMP, trade_date DATE, side STRING, ord_type STRING, "
            "price DOUBLE, qty DOUBLE, notional DOUBLE, _silver_version BIGINT")
FACT_STAGE_DDL = FACT_DDL.replace("trade_ts TIMESTAMP", "trade_ts STRING").replace("trade_date DATE", "trade_date STRING")
HOT_DDL = "symbol STRING, trade_date DATE, hot_trades BIGINT, hot_notional DOUBLE"

# (symbol, trade_id) -> (price, qty, timestamp). Notional: 80000 * 0.5 = 40000, 80100 * 0.25 = 20025, 2300 * 2 = 4600, 2310 * 1 = 2310.
TRADE = {
    ("BTC/USD", 1): (80000.0, 0.5, "2026-10-28T10:00:00.000000Z"),
    ("BTC/USD", 2): (80100.0, 0.25, "2026-10-28T10:00:01.000000Z"),
    ("BTC/USD", 3): (80000.0, 0.5, "2026-10-28T10:00:02.000000Z"),
    ("ETH/USD", 1): (2300.0, 2.0, "2026-10-28T10:00:00.500000Z"),
    ("ETH/USD", 2): (2310.0, 1.0, "2026-10-28T10:00:03.000000Z"),
    ("BTC/USD", 4): (80100.0, 0.25, "2026-10-28T10:00:04.000000Z"),
}
DAY_2_TRADE = ("BTC/USD", 99)  # a trade on DAY_2, added to TRADE by the case that needs it
NOTIONAL = {key: price * qty for key, (price, qty, _) in TRADE.items()}


def bronze_row(key, path="ws"):
    price, qty, ts = TRADE[key]
    return {"symbol": key[0], "side": "buy", "price": price, "qty": qty, "ord_type": "limit", "trade_id": key[1],
            "timestamp": ts, "_ingest_path": path, "_load_ts": "2026-10-28T18:00:00.000000Z"}


def fact_row(key):
    price, qty, ts = TRADE[key]
    return (key[0], key[1], ts, ts[:10], "buy", "limit", price, qty, price * qty, 1)


def load_bronze(rows):
    spark.createDataFrame(rows, BRONZE_DDL).write.format("delta").mode("append").saveAsTable(BRONZE)


def load_fact(keys):
    rows = [fact_row(k) for k in keys]
    spark.createDataFrame(rows, FACT_STAGE_DDL) \
        .selectExpr("symbol", "trade_id", "to_timestamp(trade_ts) AS trade_ts", "to_date(trade_date) AS trade_date",
                    "side", "ord_type", "price", "qty", "notional", "_silver_version") \
        .write.format("delta").mode("append").saveAsTable(FACT)


def hot_df(rows):
    # rows: (symbol, day, hot_trades, hot_notional)
    return spark.createDataFrame(rows, HOT_DDL)


def reset():
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql("CREATE SCHEMA IF NOT EXISTS test")
    for table in (BRONZE, FACT, OUT):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {BRONZE} ({BRONZE_DDL}) USING DELTA")
    spark.sql(f"CREATE TABLE {FACT} ({FACT_DDL}) USING DELTA")


def run_reconcile(hot):
    return reconcile(BRONZE, FACT, hot, OUT, TRACKED)


def rows_by_key():
    return {(r["trade_date"], r["symbol"]): r.asDict() for r in spark.table(OUT).collect()}


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def check_equal(actual, expected, what):
    if actual != expected:
        raise AssertionError(f"{what}: expected {expected!r}, got {actual!r}")


COLUMNS = ["hot_trades", "hot_notional", "gold_trades", "gold_notional", "gap_trades", "gap_notional",
           "duplicates_trades", "duplicates_notional", "late_trades", "late_notional",
           "backfill_trades", "backfill_notional", "unexplained_trades", "unexplained_notional"]


def check_row(row, expected, what):
    # expected: only the columns that are not 0
    got = {c: row[c] for c in COLUMNS}
    want = {c: expected.get(c, 0) for c in COLUMNS}
    check_equal(got, want, what)


def seed_clean_day():
    # BTC/USD trades 1 and 2, ETH/USD trade 1: in the live stream, in Bronze and in Gold. SOL/USD has none.
    keys = [("BTC/USD", 1), ("BTC/USD", 2), ("ETH/USD", 1)]
    load_bronze([bronze_row(k) for k in keys])
    load_fact(keys)
    return hot_df([("BTC/USD", DAY_1, 2, NOTIONAL[keys[0]] + NOTIONAL[keys[1]]), ("ETH/USD", DAY_1, 1, NOTIONAL[keys[2]])])


evidence = {}


def case_1_clean_day():
    reset()
    result = run_reconcile(seed_clean_day())
    check_equal(result["days"], [str(DAY_1)], "days reconciled")
    rows = rows_by_key()
    check_equal(sorted(rows), [(DAY_1, s) for s in sorted(TRACKED)], "one row per tracked pair")
    check_row(rows[(DAY_1, "BTC/USD")], {"hot_trades": 2, "hot_notional": 60025.0, "gold_trades": 2, "gold_notional": 60025.0}, "BTC/USD")
    check_row(rows[(DAY_1, "ETH/USD")], {"hot_trades": 1, "hot_notional": 4600.0, "gold_trades": 1, "gold_notional": 4600.0}, "ETH/USD")
    check_row(rows[(DAY_1, "SOL/USD")], {}, "SOL/USD (no trades)")


def case_2_rerun_replaces():
    reset()
    hot = seed_clean_day()
    run_reconcile(hot)
    run_reconcile(hot)
    check_equal(spark.table(OUT).count(), 3, "rows after the same day twice")
    # The live stream sent BTC/USD trade 3 as well, Bronze and Gold did not get it: the BTC/USD row is rewritten.
    changed = hot_df([("BTC/USD", DAY_1, 3, 100025.0), ("ETH/USD", DAY_1, 1, 4600.0)])
    run_reconcile(changed)
    rows = rows_by_key()
    check_equal(len(rows), 3, "rows after a changed rerun")
    check_equal((rows[(DAY_1, "BTC/USD")]["hot_trades"], rows[(DAY_1, "BTC/USD")]["gap_trades"]), (3, 1), "BTC/USD hot_trades and gap after the rerun")


def case_3_day_without_hot():
    reset()
    hot = seed_clean_day()  # hot totals for DAY_1 only
    TRADE[DAY_2_TRADE] = (80000.0, 0.5, "2026-10-29T10:00:00.000000Z")
    load_bronze([bronze_row(DAY_2_TRADE)])
    load_fact([DAY_2_TRADE])
    result = run_reconcile(hot)
    check_equal(result["days"], [str(DAY_1)], "days reconciled")
    check_equal({d for d, _ in rows_by_key()}, {DAY_1}, "days in the table")


def seed(ws_keys, fact_keys, backfill_keys=()):
    # ws_keys may repeat a key (the same event sent twice); backfill_keys are in Bronze with path 'backfill'.
    load_bronze([bronze_row(k) for k in ws_keys] + [bronze_row(k, "backfill") for k in backfill_keys])
    load_fact(fact_keys)


BTC = lambda n: ("BTC/USD", n)


def case_4_duplicates():
    reset()
    seed([BTC(1), BTC(2), BTC(2)], [BTC(1), BTC(2)])  # trade 2 sent twice
    run_reconcile(hot_df([("BTC/USD", DAY_1, 3, 80050.0)]))
    check_row(rows_by_key()[(DAY_1, "BTC/USD")],
              {"hot_trades": 3, "hot_notional": 80050.0, "gold_trades": 2, "gold_notional": 60025.0,
               "gap_trades": 1, "gap_notional": 20025.0, "duplicates_trades": 1, "duplicates_notional": 20025.0}, "BTC/USD")


def case_5_late():
    reset()
    seed([BTC(1), BTC(2), BTC(3)], [BTC(1), BTC(2)])  # trade 3 reached Bronze, not Gold
    run_reconcile(hot_df([("BTC/USD", DAY_1, 3, 100025.0)]))
    check_row(rows_by_key()[(DAY_1, "BTC/USD")],
              {"hot_trades": 3, "hot_notional": 100025.0, "gold_trades": 2, "gold_notional": 60025.0,
               "gap_trades": 1, "gap_notional": 40000.0, "late_trades": 1, "late_notional": 40000.0}, "BTC/USD")


def case_6_backfill():
    reset()
    seed([BTC(1)], [BTC(1), BTC(2), BTC(3)], backfill_keys=[BTC(2), BTC(3)])  # the live stream missed trades 2 and 3
    run_reconcile(hot_df([("BTC/USD", DAY_1, 1, 40000.0)]))
    check_row(rows_by_key()[(DAY_1, "BTC/USD")],
              {"hot_trades": 1, "hot_notional": 40000.0, "gold_trades": 3, "gold_notional": 100025.0,
               "gap_trades": -2, "gap_notional": -60025.0, "backfill_trades": 2, "backfill_notional": 60025.0}, "BTC/USD")


def case_7_unexplained():
    reset()
    seed([BTC(1)], [BTC(1)])
    run_reconcile(hot_df([("BTC/USD", DAY_1, 2, 60025.0)]))  # the hot path counted one event more than Bronze got
    check_row(rows_by_key()[(DAY_1, "BTC/USD")],
              {"hot_trades": 2, "hot_notional": 60025.0, "gold_trades": 1, "gold_notional": 40000.0,
               "gap_trades": 1, "gap_notional": 20025.0, "unexplained_trades": 1, "unexplained_notional": 20025.0}, "BTC/USD")


def case_8_causes_add_up():
    reset()
    # Live stream: trades 1 (twice), 2 and 3. Gold: 1, 2 and 4 (4 came from a backfill; 3 is not loaded yet).
    # The hot path counted one event Bronze never got, worth 25000.
    seed([BTC(1), BTC(1), BTC(2), BTC(3)], [BTC(1), BTC(2), BTC(4)], backfill_keys=[BTC(4)])
    run_reconcile(hot_df([("BTC/USD", DAY_1, 5, 165025.0)]))
    row = rows_by_key()[(DAY_1, "BTC/USD")]
    check_row(row,
              {"hot_trades": 5, "hot_notional": 165025.0, "gold_trades": 3, "gold_notional": 80050.0,
               "gap_trades": 2, "gap_notional": 84975.0,
               "duplicates_trades": 1, "duplicates_notional": 40000.0, "late_trades": 1, "late_notional": 40000.0,
               "backfill_trades": 1, "backfill_notional": 20025.0, "unexplained_trades": 1, "unexplained_notional": 25000.0}, "BTC/USD")
    check_equal(row["gap_trades"], row["duplicates_trades"] + row["late_trades"] - row["backfill_trades"] + row["unexplained_trades"], "count identity")
    check_equal(row["gap_notional"], row["duplicates_notional"] + row["late_notional"] - row["backfill_notional"] + row["unexplained_notional"], "notional identity")


def case_9_test_event():
    reset()
    seed([BTC(1), BTC(2)], [BTC(1), BTC(2)])
    load_bronze([bronze_row(BTC(3), "test")])  # in Bronze, rejected by Silver: not in Gold
    run_reconcile(hot_df([("BTC/USD", DAY_1, 3, 100025.0)]))
    check_row(rows_by_key()[(DAY_1, "BTC/USD")],
              {"hot_trades": 3, "hot_notional": 100025.0, "gold_trades": 2, "gold_notional": 60025.0,
               "gap_trades": 1, "gap_notional": 40000.0, "late_trades": 1, "late_notional": 40000.0}, "BTC/USD")


def step(name, fn):
    try:
        fn()
        evidence[name] = "passed"
    except Exception:
        evidence[name] = "FAILED: " + traceback.format_exc()


for name, fn in [("1_clean_day", case_1_clean_day), ("2_rerun_replaces", case_2_rerun_replaces),
                 ("3_day_without_hot", case_3_day_without_hot), ("4_duplicates", case_4_duplicates), ("5_late", case_5_late),
                 ("6_backfill", case_6_backfill), ("7_unexplained", case_7_unexplained), ("8_causes_add_up", case_8_causes_add_up),
                 ("9_test_event", case_9_test_event)]:
    step(name, fn)

failed = {n: r for n, r in evidence.items() if r != "passed"}
summary = {"passed": len(evidence) - len(failed), "failed": len(failed), "cases": evidence}
notebookutils.fs.put(RESULT_PATH, json.dumps(summary, indent=2), True)
print(json.dumps(summary, indent=2))
if failed:
    raise RuntimeError(f"reconcile tests failed: {sorted(failed)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
