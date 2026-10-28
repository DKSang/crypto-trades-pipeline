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

library_only = True  # nb_load_gold then defines its functions and skips its load

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_load_gold

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Seam 1 tests of nb_load_gold and of the SELECT of the agg_pair_daily MLV (issue #10), run in
# Fabric: load_increment takes a Silver table (its Change Data Feed) plus the current Gold and
# watermark tables; the MLV SELECT runs with spark.sql over the scratch fact table. Every case runs
# on scratch tables in the `test` schema, never on the real ones.
# Fixtures are real Kraken trades (REST /0/public/Trades, 2026-10-28, two of them from each side of
# midnight UTC) in the Silver shape. Expected aggregates come from a plain-Python oracle that sorts
# by (timestamp, trade_id), not from the code under test.
# Results go to Files/checks/nb_test_load_gold.json; the run fails if a case fails.

import json
import traceback
from pyspark.sql import functions as F

RESULT_PATH = "Files/checks/nb_test_load_gold.json"
SILVER, FACT, DIM_PAIR, DIM_DATE, WATERMARK = (f"test.{t}" for t in
                                               ["g_silver_trades", "g_fact_trade", "g_dim_pair", "g_dim_date", "g_etl_watermark"])
SILVER_SCHEMA = ("symbol STRING, trade_id BIGINT, side STRING, price DOUBLE, qty DOUBLE, ord_type STRING, "
                 "trade_ts STRING, _ingest_path STRING, _load_ts STRING, _bronze_version BIGINT")
TRACKED = ["BTC/USD", "ETH/USD"]

# (symbol, trade_id) -> (side, price, qty, ord_type, timestamp)
REAL = {
    ("BTC/USD", 113191288): ("buy", 80800.9, 0.00040314, "limit", "2026-10-27T23:59:59.047657Z"),
    ("BTC/USD", 113191289): ("buy", 80800.9, 0.00542817, "market", "2026-10-27T23:59:59.528693Z"),
    ("BTC/USD", 113191290): ("buy", 80800.9, 0.00012284, "limit", "2026-10-28T00:00:00.209477Z"),
    ("BTC/USD", 113191291): ("buy", 80800.9, 0.00094992, "limit", "2026-10-28T00:00:00.229830Z"),
    ("BTC/USD", 113261941): ("sell", 80691.6, 0.01228705, "limit", "2026-10-28T17:49:39.713645Z"),
    ("BTC/USD", 113261942): ("buy", 80691.7, 0.00261820, "market", "2026-10-28T17:49:40.718137Z"),
    ("BTC/USD", 113261943): ("buy", 80691.7, 0.05947431, "market", "2026-10-28T17:49:40.718137Z"),
    ("BTC/USD", 113261944): ("buy", 80691.7, 0.00018589, "market", "2026-10-28T17:49:43.292547Z"),
    ("ETH/USD", 68421366): ("sell", 2341.65, 2.26825323, "limit", "2026-10-28T17:49:38.309843Z"),
    ("ETH/USD", 68421367): ("sell", 2341.68, 0.20498103, "limit", "2026-10-28T17:49:38.321315Z"),
}
DAY27 = [k for k in REAL if k[1] in (113191288, 113191289)]
BTC28 = [k for k in REAL if k[0] == "BTC/USD" and k not in DAY27]
ETH28 = [k for k in REAL if k[0] == "ETH/USD"]
ALL = list(REAL)


def trade(key, **override):
    symbol, trade_id = key
    side, price, qty, ord_type, ts = REAL[key]
    row = {"symbol": symbol, "trade_id": trade_id, "side": side, "price": price, "qty": qty, "ord_type": ord_type, "ts": ts}
    row.update(override)
    return row


def reset():
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for t in [SILVER, FACT, DIM_PAIR, DIM_DATE, WATERMARK]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql("CREATE SCHEMA IF NOT EXISTS test")
    spark.sql(f"""
    CREATE TABLE {SILVER} (
        symbol STRING, trade_id BIGINT, side STRING, price DOUBLE, qty DOUBLE, ord_type STRING,
        trade_ts TIMESTAMP, _ingest_path STRING, _load_ts TIMESTAMP, _bronze_version BIGINT
    ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    ensure_tables(FACT, DIM_PAIR, DIM_DATE, WATERMARK)


def land(rows):
    # One call is one Silver commit, as one nb_load_silver write is.
    data = [(r["symbol"], r["trade_id"], r["side"], r["price"], r["qty"], r["ord_type"], r["ts"], "ws",
             "2026-10-28T18:00:00.000000Z", 1) for r in rows]
    (spark.createDataFrame(data, SILVER_SCHEMA)
     .withColumn("trade_ts", F.try_to_timestamp("trade_ts")).withColumn("_load_ts", F.try_to_timestamp("_load_ts"))
     .write.format("delta").mode("append").saveAsTable(SILVER))


def silver_version():
    return spark.sql(f"DESCRIBE HISTORY {SILVER} LIMIT 1").first()["version"]


def load(**kw):
    return load_increment(SILVER, FACT, DIM_PAIR, DIM_DATE, WATERMARK, TRACKED, **kw)


def fact_keys():
    return sorted((r["symbol"], r["trade_id"]) for r in spark.table(FACT).collect())


def agg_rows():
    rows = spark.sql(agg_pair_daily_select(FACT)).collect()  # the MLV SELECT, run as it is
    return {(r["symbol"], str(r["trade_date"])): r.asDict() for r in rows}


def expected_agg(trades):
    by_day = {}
    for t in trades:
        by_day.setdefault((t["symbol"], t["ts"][:10]), []).append(t)
    out = {}
    for day, day_trades in by_day.items():
        day_trades.sort(key=lambda t: (t["ts"], t["trade_id"]))
        notional = sum(t["price"] * t["qty"] for t in day_trades)
        volume = sum(t["qty"] for t in day_trades)
        out[day] = {"open": day_trades[0]["price"], "high": max(t["price"] for t in day_trades),
                    "low": min(t["price"] for t in day_trades), "close": day_trades[-1]["price"],
                    "volume": volume, "notional": notional, "vwap": notional / volume,
                    "trade_count": len(day_trades),
                    "taker_buy_notional": sum(t["price"] * t["qty"] for t in day_trades if t["side"] == "buy")}
    return out


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def check_equal(actual, expected, what):
    if actual != expected:
        raise AssertionError(f"{what}: expected {expected!r}, got {actual!r}")


def check_aggregates(actual, expected, what):
    check_equal(sorted(actual), sorted(expected), f"{what} days")
    for day, want in expected.items():
        for column, value in want.items():
            if not isinstance(value, (int, float)):
                continue  # symbol and trade_date of a row taken whole
            got = actual[day][column]
            check(abs(got - value) <= 1e-9 * max(1.0, abs(value)), f"{what} {day} {column}: expected {value!r}, got {got!r}")


def test_new_trades_land_in_fact_with_notional_utc_day_and_dimensions():
    reset()
    land([trade(k) for k in ALL])
    result = load()
    check_equal(fact_keys(), sorted(ALL), "fact keys")
    check_equal(read_watermark(WATERMARK, SILVER), silver_version(), "watermark")
    check_equal(result["fact_inserted"], len(ALL), "fact_inserted")
    row = spark.table(FACT).where("trade_id = 113261941").first()
    check(abs(row["notional"] - 80691.6 * 0.01228705) < 1e-9, f"notional {row['notional']!r}")
    # The two trades around midnight UTC fall on different days.
    days = {r["trade_id"]: str(r["trade_date"]) for r in spark.table(FACT).where("symbol = 'BTC/USD'").collect()}
    check_equal((days[113191289], days[113191290]), ("2026-10-27", "2026-10-28"), "trade_date around midnight UTC")
    pairs = {r["symbol"]: (r["base"], r["quote"], r["is_tracked"]) for r in spark.table(DIM_PAIR).collect()}
    check_equal(pairs, {"BTC/USD": ("BTC", "USD", True), "ETH/USD": ("ETH", "USD", True)}, "dim_pair")
    dates = {str(r["date"]): (r["day_name"], r["day_of_week"], r["is_weekend"]) for r in spark.table(DIM_DATE).collect()}
    check_equal(dates, {"2026-10-27": ("Tuesday", 2, False), "2026-10-28": ("Wednesday", 3, False)}, "dim_date")


def test_duplicates_from_overlapping_runs_and_backfills_leave_each_trade_once():
    reset()
    land([trade(k) for k in BTC28[:4]])
    load()
    # Next commits: an overlapping run resends two trades, a backfill resends one of them again and
    # adds an older trade, and one commit holds the same trade twice.
    land([trade(k) for k in BTC28[2:4]])
    land([trade(BTC28[3]), trade(DAY27[0]), trade(DAY27[0])])
    result = load()
    check_equal(fact_keys(), sorted(BTC28[:4] + DAY27[:1]), "fact keys")
    check_equal(result["fact_inserted"], 1, "fact_inserted")
    check_equal(result["changes_read"], 5, "changes_read")
    # 5 rows for 3 trades: the 2 trades already in fact are not new, the 2 repeats in the batch are dropped.
    check_equal(result["duplicates_dropped"], 2, "duplicates_dropped")
    check_aggregates(agg_rows(), expected_agg([trade(k) for k in BTC28[:4] + DAY27[:1]]), "aggregates after overlaps")


def test_running_the_same_increment_twice_changes_nothing():
    reset()
    land([trade(k) for k in ALL])
    load()
    fact_before, version_before = fact_keys(), read_watermark(WATERMARK, SILVER)
    again = load()  # nothing new in Silver
    check_equal((again["changes_read"], again["fact_inserted"]), (0, 0), "second run")
    # An overlapping run that rereads the same range: same Silver rows, watermark rewound.
    spark.sql(f"UPDATE {WATERMARK} SET last_version = 0")
    reread = load()
    check(reread["changes_read"] > 0, "the rewound run should reread the range")
    check_equal((reread["fact_inserted"], reread["dim_pair_inserted"], reread["dim_date_inserted"]), (0, 0, 0), "reread inserts")
    check_equal(fact_keys(), fact_before, "fact keys")
    check_equal(read_watermark(WATERMARK, SILVER), version_before, "watermark")
    check_aggregates(agg_rows(), expected_agg([trade(k) for k in ALL]), "aggregates after reread")


def test_a_failure_in_the_middle_leaves_the_watermark_and_the_rerun_reads_the_same_range():
    reset()
    land([trade(k) for k in BTC28[:2]])
    load()
    start = read_watermark(WATERMARK, SILVER)
    land([trade(k) for k in BTC28[2:] + ETH28])
    try:
        load(fail_after_write=True)
        raise AssertionError("the forced failure did not raise")
    except RuntimeError as e:
        check("forced failure" in str(e), f"unexpected error {e}")
    check_equal(read_watermark(WATERMARK, SILVER), start, "watermark after failure")
    rerun = load()
    check_equal(rerun["from_version"], start + 1, "rerun reads from")
    check_equal(rerun["to_version"], silver_version(), "rerun reads to")
    check_equal(rerun["fact_inserted"], 0, "the writes before the failure are not repeated")
    check_equal(fact_keys(), sorted(BTC28 + ETH28), "fact keys")
    check_equal(read_watermark(WATERMARK, SILVER), silver_version(), "watermark after rerun")


def test_a_pair_with_a_dimension_row_keeps_it():
    reset()
    spark.sql(f"INSERT INTO {DIM_PAIR} VALUES ('BTC/USD', 'BTC', 'USD', 'online', true)")
    land([trade(k) for k in BTC28[:2] + ETH28])
    result = load()
    check_equal(result["dim_pair_inserted"], 1, "dim_pair_inserted")
    pairs = {r["symbol"]: r["status"] for r in spark.table(DIM_PAIR).collect()}
    check_equal(pairs, {"BTC/USD": "online", "ETH/USD": None}, "dim_pair status")


def test_agg_pair_daily_holds_ohlc_volume_notional_vwap_count_and_taker_buy_per_pair_and_day():
    reset()
    land([trade(k) for k in ALL])
    load()
    actual = agg_rows()
    check_aggregates(actual, expected_agg([trade(k) for k in ALL]), "aggregates")
    # One hand-checked row: BTC/USD on 2026-10-28 opens at 00:00:00 (113191290) and closes at 17:49:43 (113261944).
    row = actual[("BTC/USD", "2026-10-28")]
    check_equal((row["open"], row["close"], row["high"], row["low"], row["trade_count"]),
                (80800.9, 80691.7, 80800.9, 80691.6, 6), "BTC/USD 2026-10-28")


def test_open_and_close_come_from_the_first_and_last_trade_by_timestamp_then_trade_id():
    reset()
    # 942 and 943 share a timestamp (real data); give them different prices and land the higher
    # trade_id first, so only (timestamp, trade_id) can say which is the open and which the close.
    low_id, high_id = trade(BTC28[3], price=100.0), trade(BTC28[4], price=200.0)
    check_equal(low_id["ts"], high_id["ts"], "fixture ties")
    land([high_id, low_id])
    load()
    row = agg_rows()[("BTC/USD", "2026-10-28")]
    check_equal((row["open"], row["close"]), (100.0, 200.0), "open, close of a tied timestamp")
    # Trades landed later but earlier and later in time still move the open and the close.
    land([trade(BTC28[5]), trade(BTC28[0])])
    load()
    row = agg_rows()[("BTC/USD", "2026-10-28")]
    check_equal((row["open"], row["close"]), (80800.9, 80691.7), "open, close with trades landed out of order")


def test_a_late_trade_updates_only_its_own_day():
    reset()
    land([trade(k) for k in ALL if k not in DAY27[:1]])
    load()
    before = agg_rows()
    fact_28_before = sorted(tuple(r) for r in spark.table(FACT).where("trade_date = '2026-10-28'").collect())
    land([trade(DAY27[0])])  # a trade of 2026-10-27 arriving after 2026-10-28 was loaded
    result = load()
    after = agg_rows()
    check_equal(result["fact_inserted"], 1, "only the late trade is written")
    check_equal(sorted(tuple(r) for r in spark.table(FACT).where("trade_date = '2026-10-28'").collect()), fact_28_before,
                "fact rows of 2026-10-28")
    for day in before:
        if day[1] == "2026-10-27":
            check(after[day]["trade_count"] != before[day]["trade_count"], f"{day} should change")
        else:
            # Sums may differ in the last bit with the file layout, so compare with a tolerance.
            check_aggregates({day: after[day]}, {day: before[day]}, f"{day} must not change")
    check_aggregates(after, expected_agg([trade(k) for k in ALL]), "aggregates after the late trade")


def run_tests():
    results = {}
    for name, fn in sorted((n, f) for n, f in globals().items() if n.startswith("test_") and callable(f)):
        try:
            fn()
            results[name] = "passed"
        except Exception:
            results[name] = "FAILED: " + traceback.format_exc()
    return results


results = run_tests()
failed = {n: r for n, r in results.items() if r != "passed"}
summary = {"passed": len(results) - len(failed), "failed": len(failed), "results": results}
notebookutils.fs.put(RESULT_PATH, json.dumps(summary, indent=2), True)
print(json.dumps(summary, indent=2))
if failed:
    raise RuntimeError(f"{len(failed)} of {len(results)} cases failed: {sorted(failed)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
