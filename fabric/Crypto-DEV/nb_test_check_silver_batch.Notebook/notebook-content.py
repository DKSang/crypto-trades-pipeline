# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "01a138e9-6b56-461d-8df6-a037f9c2afdc",
# META       "workspaceId": "c8d6538e-e60c-467b-8440-dec5dae4d897"
# META     },
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

library_only = True  # nb_check_silver_batch then defines its functions and skips its run

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_check_silver_batch

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Seam 1 tests of nb_check_silver_batch (issue #12), run in Fabric: check_batch takes the Silver,
# dim_pair, watermark and result tables and returns the checks it ran. Every case runs on scratch
# tables in the `test` schema, never on the real ones. A "batch" is what Silver committed after
# Gold's watermark; add_silver is one Silver commit and set_watermark is "Gold has loaded up to here".
# Results go to Files/checks/nb_test_check_silver_batch.json; the run fails if a case fails.

import json
import traceback
from datetime import datetime, timedelta
from pyspark.sql import functions as F

RESULT_PATH = "Files/checks/nb_test_check_silver_batch.json"
SILVER, DIM_PAIR, WATERMARK, DQ = (f"test.dq_{t}" for t in ["silver_trades", "dim_pair", "etl_watermark", "dq_result"])
SILVER_SCHEMA = ("symbol STRING, trade_id BIGINT, side STRING, price DOUBLE, qty DOUBLE, ord_type STRING, "
                 "trade_ts TIMESTAMP, _ingest_path STRING, _load_ts TIMESTAMP, _bronze_version BIGINT")
NOW = datetime(2026, 10, 29, 12, 0, 0)  # the "current time" of every case, passed to check_batch
CHECK_IDS = ["freshness", "key_unique", "ref_integrity_dim_pair", "row_count", "schema", "trade_id_contiguity"]


def silver_row(symbol, trade_id, day=NOW - timedelta(days=0), load_ts=None, **override):
    ts = day.replace(hour=11, minute=0, second=0) + timedelta(seconds=trade_id)
    row = {"symbol": symbol, "trade_id": trade_id, "side": "buy", "price": 100.0, "qty": 1.0, "ord_type": "limit",
           "trade_ts": ts, "_ingest_path": "ws", "_load_ts": load_ts or NOW - timedelta(minutes=10), "_bronze_version": 1}
    row.update(override)
    return tuple(row[c.split()[0]] for c in SILVER_SCHEMA.split(", "))


def reset():
    for t in [SILVER, DIM_PAIR, WATERMARK, DQ]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql("CREATE SCHEMA IF NOT EXISTS test")
    spark.sql(f"CREATE TABLE {SILVER} ({SILVER_SCHEMA}) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    spark.sql("CREATE TABLE " + DIM_PAIR + " (symbol STRING, base STRING, quote STRING, status STRING, is_tracked BOOLEAN) USING DELTA")
    spark.sql(f"INSERT INTO {DIM_PAIR} VALUES ('BTC/USD', 'BTC', 'USD', 'online', true), ('ETH/USD', 'ETH', 'USD', 'online', true)")
    spark.sql(f"CREATE TABLE {WATERMARK} (source_table STRING, last_version BIGINT, _updated_ts TIMESTAMP) USING DELTA")


def add_silver(rows):
    # One call is one Silver commit.
    spark.createDataFrame(rows, SILVER_SCHEMA).write.format("delta").mode("append").saveAsTable(SILVER)


def silver_version():
    return spark.sql(f"DESCRIBE HISTORY {SILVER} LIMIT 1").first()["version"]


def set_watermark():
    # Gold has loaded everything Silver holds so far.
    spark.sql(f"INSERT INTO {WATERMARK} VALUES ('{SILVER}', {silver_version()}, current_timestamp())")


def run(now=NOW):
    return check_batch(SILVER, DIM_PAIR, WATERMARK, DQ, now=now)


def dq_rows(run_id=None):
    rows = spark.table(DQ).collect()
    return [r for r in rows if run_id is None or r["run_id"] == run_id]


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def check_equal(actual, expected, what):
    if actual != expected:
        raise AssertionError(f"{what}: expected {expected!r}, got {actual!r}")


BTC = [silver_row("BTC/USD", i) for i in range(1, 6)]
ETH = [silver_row("ETH/USD", i) for i in range(1, 4)]


def test_a_clean_batch_passes_every_check_and_every_result_is_recorded():
    reset()
    add_silver(BTC + ETH)
    summary = run()
    check_equal((summary["errors"], summary["warns"]), ([], []), "errors and warns")
    check_equal(sorted(r["check_id"] for r in dq_rows(summary["run_id"])), CHECK_IDS, "recorded checks")
    check(all(r["passed"] for r in dq_rows()), "every check should pass")
    severities = {r["check_id"]: r["severity"] for r in dq_rows()}
    check_equal(severities, {"schema": "error", "key_unique": "error", "ref_integrity_dim_pair": "error",
                             "row_count": "warn", "freshness": "warn", "trade_id_contiguity": "warn"}, "severities")


def test_a_duplicate_key_in_the_batch_is_an_error_and_nothing_else_moves():
    reset()
    add_silver(BTC + [silver_row("BTC/USD", 3)] + ETH)  # BTC trade 3 twice
    summary = run()
    check_equal(summary["errors"], ["key_unique"], "errors")
    row = [r for r in dq_rows(summary["run_id"]) if r["check_id"] == "key_unique"][0]
    check_equal((row["passed"], row["severity"]), (False, "error"), "key_unique row")
    check(row["unexpected_count"] > 0, "unexpected_count should count the repeated rows")
    check_equal(spark.table(WATERMARK).count(), 0, "the gate must not write a watermark")
    again = run()  # a rerun adds its own results, history is kept
    check(again["run_id"] != summary["run_id"], "each run has its own run_id")
    check_equal(len(dq_rows()), 12, "results of two runs")


def test_a_key_that_is_already_in_an_older_commit_is_an_error_too():
    reset()
    add_silver(BTC[:3])
    set_watermark()  # trades 1 to 3 are loaded
    add_silver([silver_row("BTC/USD", 3), silver_row("BTC/USD", 4)])  # 3 again, 4 is new
    summary = run()
    check_equal(summary["from_version"], silver_version(), "the batch is the last commit only")
    check_equal(summary["errors"], ["key_unique"], "errors")


def test_a_pair_missing_from_dim_pair_is_an_error():
    reset()
    add_silver(BTC + [silver_row("XYZ/USD", 1)])
    summary = run()
    check_equal(summary["errors"], ["ref_integrity_dim_pair"], "errors")
    row = [r for r in dq_rows() if r["check_id"] == "ref_integrity_dim_pair"][0]
    check("XYZ/USD" in row["details"], f"details should name the pair: {row['details']}")


def test_a_schema_that_is_not_silvers_is_an_error():
    good = spark.createDataFrame([], SILVER_SCHEMA)
    check_equal(check_schema(good)["passed"], True, "Silver's own schema")
    extra = good.withColumn("surprise", F.lit(1))
    check_equal(check_schema(extra)["passed"], False, "an extra column")
    retyped = good.withColumn("price", F.col("price").cast("string"))
    result = check_schema(retyped)
    check_equal(result["passed"], False, "price as a string")
    check_equal([f["column"] for f in result["details"]["failures"]], ["price"], "the column that broke")
    check_equal(check_schema(good.drop("qty"))["passed"], False, "a missing column")


def test_a_trade_id_gap_is_a_warn_that_does_not_block():
    reset()
    add_silver([silver_row("BTC/USD", i) for i in (1, 2, 5)] + ETH)  # BTC 3 and 4 are missing
    summary = run()
    check_equal((summary["errors"], summary["warns"]), ([], ["trade_id_contiguity"]), "errors and warns")
    row = [r for r in dq_rows() if r["check_id"] == "trade_id_contiguity"][0]
    check_equal((row["passed"], row["severity"]), (False, "warn"), "contiguity row")
    check_equal(json.loads(row["details"])["missing_per_pair"], {"BTC/USD": 2}, "gaps")


def test_a_stale_batch_is_a_freshness_warn():
    reset()
    add_silver(BTC + ETH)  # loaded 10 minutes before NOW
    check_equal(run(now=NOW + timedelta(hours=5))["warns"], ["freshness"], "warns 5 hours later")


def test_a_batch_far_bigger_than_the_days_before_is_a_row_count_warn():
    reset()
    day1, day2 = NOW - timedelta(days=2), NOW - timedelta(days=1)
    add_silver([silver_row("BTC/USD", i, day=day1) for i in range(1, 11)] + [silver_row("BTC/USD", i, day=day2) for i in range(11, 21)])
    set_watermark()  # two days of 10 trades each are loaded: the limit is 3 x 10 = 30
    add_silver([silver_row("BTC/USD", i) for i in range(21, 41)])  # 20 trades: inside
    check_equal(run()["warns"], [], "20 trades")
    add_silver([silver_row("BTC/USD", i) for i in range(41, 121)])  # 100 trades in the batch now: outside
    summary = run()
    check_equal((summary["errors"], summary["warns"]), ([], ["row_count"]), "errors and warns for 100 trades")


def test_no_new_silver_commit_checks_nothing_and_records_nothing():
    reset()
    add_silver(BTC)
    set_watermark()
    summary = run()
    check("skipped" in summary, f"expected skipped, got {summary}")
    check_equal(spark.table(DQ).count() if spark.catalog.tableExists(DQ) else 0, 0, "recorded results")


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
