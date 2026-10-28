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

library_only = True  # nb_load_silver then defines its functions and skips its load

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_load_silver

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Seam 1 tests of nb_load_silver (issue #9), run in Fabric: load_increment takes a Bronze table
# (its Change Data Feed) plus the current Silver, quarantine and watermark tables and returns the
# DQ results. Every case runs on scratch tables in the `test` schema, never on the real ones.
# Fixtures are real Kraken trades (REST /0/public/Trades, 2026-10-28) converted to the Bronze
# shape the collector sends (WS v2 names, ISO 8601 strings).
# Results go to Files/checks/nb_test_load_silver.json; the run fails if a case fails.

import json
import traceback

RESULT_PATH = "Files/checks/nb_test_load_silver.json"
BRONZE, SILVER, QUARANTINE, WATERMARK = (f"test.{t}" for t in
                                         ["bronze_trades", "silver_trades", "quarantine_trades", "etl_watermark"])
BRONZE_SCHEMA = ("symbol STRING, side STRING, price DOUBLE, qty DOUBLE, ord_type STRING, "
                 "trade_id BIGINT, timestamp STRING, _ingest_path STRING, _load_ts STRING")
TRACKED = ["BTC/USD", "ETH/USD"]

# symbol, trade_id -> (side, price, qty, ord_type, timestamp)
REAL = {
    ("BTC/USD", 113261941): ("sell", 80691.6, 0.01228705, "limit", "2026-10-28T17:49:39.713645Z"),
    ("BTC/USD", 113261942): ("buy", 80691.7, 0.00261820, "market", "2026-10-28T17:49:40.718137Z"),
    ("BTC/USD", 113261943): ("buy", 80691.7, 0.05947431, "market", "2026-10-28T17:49:40.718137Z"),
    ("BTC/USD", 113261944): ("buy", 80691.7, 0.00018589, "market", "2026-10-28T17:49:43.292547Z"),
    ("ETH/USD", 68421366): ("sell", 2341.65, 2.26825323, "limit", "2026-10-28T17:49:38.309843Z"),
    ("ETH/USD", 68421367): ("sell", 2341.68, 0.20498103, "limit", "2026-10-28T17:49:38.321315Z"),
}
BTC = [k for k in REAL if k[0] == "BTC/USD"]
ETH = [k for k in REAL if k[0] == "ETH/USD"]


def trade(key, path="ws", **override):
    symbol, trade_id = key
    side, price, qty, ord_type, ts = REAL[key]
    row = {"symbol": symbol, "side": side, "price": price, "qty": qty, "ord_type": ord_type,
           "trade_id": trade_id, "timestamp": ts, "_ingest_path": path, "_load_ts": "2026-10-28T17:50:00.000000Z"}
    row.update(override)
    return row


def reset():
    for t in [BRONZE, SILVER, QUARANTINE, WATERMARK]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql("CREATE SCHEMA IF NOT EXISTS test")
    spark.sql(f"CREATE TABLE {BRONZE} ({BRONZE_SCHEMA}) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    ensure_tables(SILVER, QUARANTINE, WATERMARK)


def land(rows):
    # One call is one Bronze commit, as one es_trades write is.
    spark.createDataFrame(rows, BRONZE_SCHEMA).write.format("delta").mode("append").saveAsTable(BRONZE)


def bronze_version():
    return spark.sql(f"DESCRIBE HISTORY {BRONZE} LIMIT 1").first()["version"]


def load(**kw):
    return load_increment(BRONZE, SILVER, QUARANTINE, WATERMARK, TRACKED, **kw)


def silver_keys():
    return sorted((r["symbol"], r["trade_id"]) for r in spark.table(SILVER).collect())


def quarantined():
    return sorted((r["rule_id"], r["symbol"], r["trade_id"]) for r in spark.table(QUARANTINE).collect())


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def check_equal(actual, expected, what):
    if actual != expected:
        raise AssertionError(f"{what}: expected {expected!r}, got {actual!r}")


def test_new_trades_land_in_silver_and_the_watermark_moves_to_the_bronze_version():
    reset()
    land([trade(k) for k in BTC + ETH])
    result = load()
    check_equal(silver_keys(), sorted(REAL), "silver keys")
    check_equal(read_watermark(WATERMARK, BRONZE), bronze_version(), "watermark")
    check_equal(result["silver_inserted"], len(REAL), "silver_inserted")
    row = spark.table(SILVER).where("trade_id = 113261941").first()
    check_equal((row["side"], row["price"], row["qty"], row["ord_type"]), ("sell", 80691.6, 0.01228705, "limit"), "silver row")
    check_equal(str(row["trade_ts"]), "2026-10-28 17:49:39.713645", "trade_ts")
    check_equal(dict(spark.table(SILVER).dtypes)["trade_ts"], "timestamp", "trade_ts type")


def test_snapshot_reconnect_and_overlapping_runs_leave_each_trade_once():
    reset()
    land([trade(k) for k in BTC[:3]])  # first run's trades
    load()
    # Next commits: the subscribe snapshot of a new run resends 942 and 943, a reconnect in the
    # same run resends 943 again, and 944 is new.
    land([trade(k) for k in BTC[1:3]])
    land([trade(BTC[2]), trade(BTC[3]), trade(BTC[3], path="backfill")])
    result = load()
    check_equal(silver_keys(), sorted(BTC), "silver keys")
    check_equal(result["silver_inserted"], 1, "silver_inserted")
    # The batch holds 5 rows for 3 trades (942, 943, 944); 942 and 943 are already in Silver.
    check_equal(result["duplicates_dropped"], 2, "duplicates_dropped")
    check_equal(quarantined(), [], "quarantine")


def test_each_row_rule_sends_its_rows_to_quarantine_with_a_rule_id():
    cases = [
        ("null_key", BTC[0], {"symbol": None}),
        ("null_key", BTC[1], {"trade_id": None}),
        ("non_positive_qty", BTC[2], {"qty": 0.0}),
        ("non_positive_qty", BTC[3], {"qty": None}),
        ("non_positive_price", ETH[0], {"price": -1.0}),
        ("invalid_side", ETH[1], {"side": "short"}),
        ("invalid_ord_type", BTC[0], {"ord_type": "stop"}),
        ("non_positive_qty", BTC[1], {"qty": float("nan")}),  # NaN > 0 is true in Spark
        ("non_positive_price", BTC[2], {"price": float("inf")}),
        ("invalid_timestamp", BTC[3], {"timestamp": "not a time"}),
        ("invalid_timestamp", ETH[0], {"timestamp": None}),
    ]
    for rule_id, key, bad in cases:
        reset()
        land([trade(k) for k in BTC + ETH if k != key] + [trade(key, **bad)])
        result = load()
        check_equal([q[0] for q in quarantined()], [rule_id], f"quarantine for {bad}")
        check_equal(result["quarantined"], {rule_id: 1}, f"DQ quarantined for {bad}")
        check_equal(len(silver_keys()), len(REAL) - 1, f"silver rows for {bad}")
        check(key not in silver_keys(), f"bad row reached silver for {bad}")


def test_a_trade_on_an_untracked_pair_is_dropped_not_quarantined():
    reset()
    land([trade(BTC[0]), trade(BTC[1], symbol="DOGE/USD", trade_id=900), trade(BTC[2], symbol="DOGE/USD", qty=0.0)])
    result = load()
    check_equal(silver_keys(), [BTC[0]], "silver keys")
    check_equal(quarantined(), [], "quarantine")
    check_equal(result["untracked_dropped"], 2, "untracked_dropped")


def test_running_the_same_increment_twice_changes_nothing():
    reset()
    land([trade(k) for k in BTC + ETH] + [trade(BTC[0], qty=0.0)])
    load()
    silver_before, quarantine_before, version_before = silver_keys(), quarantined(), read_watermark(WATERMARK, BRONZE)
    again = load()  # nothing new in Bronze
    check_equal((again["changes_read"], again["silver_inserted"]), (0, 0), "second run")
    # An overlapping run that rereads the same range: same Bronze rows, watermark rewound.
    spark.sql(f"UPDATE {WATERMARK} SET last_version = 0")
    reread = load()
    check(reread["changes_read"] > 0, "the rewound run should reread the range")
    check_equal((reread["silver_inserted"], reread["quarantined_inserted"]), (0, 0), "reread inserts")
    check_equal(silver_keys(), silver_before, "silver keys")
    check_equal(quarantined(), quarantine_before, "quarantine")
    check_equal(read_watermark(WATERMARK, BRONZE), version_before, "watermark")


def test_a_failure_in_the_middle_leaves_the_watermark_and_the_rerun_reads_the_same_range():
    reset()
    land([trade(k) for k in BTC[:2]])
    load()
    start = read_watermark(WATERMARK, BRONZE)
    land([trade(k) for k in BTC[2:]] + [trade(ETH[0])])
    try:
        load(fail_after_write=True)
        raise AssertionError("the forced failure did not raise")
    except RuntimeError as e:
        check("forced failure" in str(e), f"unexpected error {e}")
    check_equal(read_watermark(WATERMARK, BRONZE), start, "watermark after failure")
    rerun = load()
    check_equal(rerun["from_version"], start + 1, "rerun reads from")
    check_equal(rerun["to_version"], bronze_version(), "rerun reads to")
    check_equal(silver_keys(), sorted(BTC + ETH[:1]), "silver keys")
    check_equal(read_watermark(WATERMARK, BRONZE), bronze_version(), "watermark after rerun")


def test_missing_trade_ids_are_reported_per_pair_and_clear_once_filled():
    reset()
    land([trade(BTC[0]), trade(BTC[2]), trade(BTC[3]), trade(ETH[0]), trade(ETH[1])])  # BTC 942 is missing
    check_equal(load()["gaps"], {"BTC/USD": 1}, "gaps")
    land([trade(BTC[1], path="backfill")])
    check_equal(load()["gaps"], {}, "gaps after backfill")
    check_equal(len(silver_keys()), 6, "gaps are reported, never block the load")


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
