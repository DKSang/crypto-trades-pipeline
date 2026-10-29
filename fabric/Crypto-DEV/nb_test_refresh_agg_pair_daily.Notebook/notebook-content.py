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

# Experiment (issue #10 follow-up): can the agg_pair_daily MLV refresh incrementally, and does it
# depend on how fact_trade is partitioned? The SELECT is the real one (agg_pair_daily_select) over
# two scratch copies of fact_trade in schema `test`: no partition (control) and PARTITIONED BY
# trade_date. Volumes follow the real feed: 20 pairs, 17,500 trades a pair a day.
# Per variant: an "hour" of new trades lands in today (insert-only MERGE, as nb_load_gold does), then
# 5 late trades land in an old day, then a refresh with nothing new. After each landing the MLV is
# refreshed in "optimal" mode, timed, its Delta history is recorded (what Fabric wrote) and its rows are
# compared with the SELECT run directly on the fact.
# Results go to Files/checks/nb_test_refresh_agg_pair_daily.json. Nothing here touches the real tables.
import json
import math
import time
import traceback
from pyspark.sql import functions as F

RESULT_PATH = "Files/checks/nb_test_refresh_agg_pair_daily.json"
SYMBOLS = ["BTC/USD", "ETH/USD", "XRP/USD", "ZEC/USD", "NEAR/USD", "SOL/USD", "HYPE/USD", "SUI/USD", "XMR/USD", "QNT/USD",
           "PEPE/USD", "TAO/USD", "ENA/USD", "XLM/USD", "LTC/USD", "LINK/USD", "ADA/USD", "DASH/USD", "DOT/USD", "DOGE/USD"]
VARIANTS = {"none": None, "date": "trade_date"}
HISTORY_DAYS = [f"2026-10-{d:02d}" for d in range(1, 11)]
TODAY = "2026-10-11"
PER_DAY, TODAY_INITIAL, HOURLY = 17500, 5000, 1000
# trade_id ranges: day i of HISTORY_DAYS starts at i * 100000 (up to 1,000,000), so today starts above that;
# a range inside an earlier one would make the insert-only MERGE drop the "new" trades as duplicates.
TODAY_SEQ = 2_000_000
LATE_DAY, LATE_SYMBOL, LATE_COUNT = "2026-10-04", "BTC/USD", 5
MEASURES = ["open", "high", "low", "close", "volume", "notional", "vwap", "trade_count", "taker_buy_notional"]


def fact_name(v):
    return f"test.pt_fact_{v}"


def mlv_name(v):
    return f"test.pt_mlv_{v}"


def gen(day, start_seq, count, symbols=SYMBOLS):
    # Synthetic trades of one UTC day: trade_id start_seq .. start_seq+count-1 on every symbol.
    h = F.hash("symbol", "trade_id")
    df = (spark.createDataFrame([(s,) for s in symbols], "symbol STRING")
          .crossJoin(spark.range(start_seq, start_seq + count).withColumnRenamed("id", "trade_id")))
    return (df
            .withColumn("trade_ts", F.expr(f"timestamp_seconds(unix_timestamp(to_timestamp('{day}')) + (trade_id % 86400))"))
            .withColumn("trade_date", F.to_date("trade_ts"))
            .withColumn("side", F.when(F.pmod(h, F.lit(2)) == 0, "buy").otherwise("sell"))
            .withColumn("ord_type", F.when(F.pmod(h, F.lit(3)) == 0, "market").otherwise("limit"))
            .withColumn("price", F.lit(100.0) + F.pmod(h, F.lit(10000)) / 100.0)
            .withColumn("qty", F.lit(0.01) + F.pmod(F.hash("trade_id", "symbol"), F.lit(1000)) / 1000.0)
            .withColumn("notional", F.col("qty") * F.col("price"))
            .withColumn("_silver_version", F.lit(1).cast("bigint"))
            .select("symbol", "trade_id", "trade_ts", "trade_date", "side", "ord_type", "price", "qty", "notional", "_silver_version"))


def merge_in(fact, df):
    # Insert-only MERGE on the trade key, as nb_load_gold writes fact_trade.
    df.createOrReplaceTempView("incoming_trades")
    spark.sql(f"""
    MERGE INTO {fact} t USING incoming_trades s
    ON t.symbol = s.symbol AND t.trade_id = s.trade_id
    WHEN NOT MATCHED THEN INSERT *
    """)


def build_fact(fact, partition_col):
    spark.sql(f"DROP TABLE IF EXISTS {fact}")
    part = f"PARTITIONED BY ({partition_col})" if partition_col else ""
    spark.sql(f"""
    CREATE TABLE {fact} (
        symbol STRING, trade_id BIGINT, trade_ts TIMESTAMP, trade_date DATE, side STRING, ord_type STRING,
        price DOUBLE, qty DOUBLE, notional DOUBLE, _silver_version BIGINT
    ) USING DELTA {part} TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    for i, day in enumerate(HISTORY_DAYS, start=1):
        gen(day, i * 100000, PER_DAY).write.format("delta").mode("append").saveAsTable(fact)
    gen(TODAY, TODAY_SEQ, TODAY_INITIAL).write.format("delta").mode("append").saveAsTable(fact)


def history_after(table, version):
    rows = (spark.sql(f"DESCRIBE HISTORY {table}").where(F.col("version") > version)
            .orderBy("version").collect())
    return [{"version": r["version"], "operation": r["operation"],
             "operationParameters": {k: str(v)[:200] for k, v in (r["operationParameters"] or {}).items()},
             "operationMetrics": dict(r["operationMetrics"] or {})} for r in rows]


def table_version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def wait_for(op, timeout_s=1200):
    # Learn doesn't say what status() returns: keep the first and last values in the result.
    seen, deadline = [], time.time() + timeout_s
    while time.time() < deadline:
        status = str(op.status())
        if not seen or seen[-1] != status:
            seen.append(status)
        low = status.lower()
        if any(w in low for w in ("complete", "succe", "fail", "cancel", "skip")):
            return seen
        time.sleep(10)
    seen.append("TIMEOUT")
    return seen


def refresh_optimal(mlv):
    # notebookutils.lakehouse.refreshMlv is the documented way to ask for the "optimal" mode (preview);
    # fall back to plain REFRESH (no FULL) when the runtime has no such utility.
    started = time.time()
    try:
        op = notebookutils.lakehouse.refreshMlv(mlv, "optimal")
        how, statuses = "notebookutils.lakehouse.refreshMlv(optimal)", wait_for(op)
    except AttributeError:
        spark.sql(f"REFRESH MATERIALIZED LAKE VIEW {mlv}")
        how, statuses = "REFRESH MATERIALIZED LAKE VIEW (no FULL)", []
    return {"how": how, "statuses": statuses, "seconds": round(time.time() - started, 1)}


def rows_by_key(df):
    return {(r["symbol"], str(r["trade_date"])): r.asDict() for r in df.collect()}


def matches_select(fact, mlv):
    expected = rows_by_key(spark.sql(agg_pair_daily_select(fact)))
    actual = rows_by_key(spark.table(mlv))
    if set(expected) != set(actual):
        return {"ok": False, "why": f"keys differ: {len(expected)} expected, {len(actual)} actual"}
    worst = 0.0
    for key, exp in expected.items():
        for m in MEASURES:
            a, e = actual[key][m], exp[m]
            if not math.isclose(a, e, rel_tol=1e-9, abs_tol=1e-9):
                return {"ok": False, "why": f"{key} {m}: {a} != {e}"}
            worst = max(worst, abs(a - e) / max(1.0, abs(e)))
    return {"ok": True, "rows": len(expected), "worst_relative_diff": worst}


def step(fact, mlv, name, land=None):
    if land:
        land()
    version = table_version(mlv)
    info = refresh_optimal(mlv)
    info["mlv_history_added"] = history_after(mlv, version)
    info["fact_rows"] = spark.table(fact).count()
    info["matches_select"] = matches_select(fact, mlv)
    print(name, json.dumps({k: info[k] for k in ("how", "statuses", "seconds", "matches_select")}))
    return info


def run_variant(name, partition_col):
    fact, mlv = fact_name(name), mlv_name(name)
    out = {"partition_by": partition_col, "steps": {}}
    spark.sql(f"DROP MATERIALIZED LAKE VIEW IF EXISTS {mlv}")
    build_fact(fact, partition_col)
    detail = spark.sql(f"DESCRIBE DETAIL {fact}").first()
    out["fact_after_build"] = {"numFiles": detail["numFiles"], "sizeInBytes": detail["sizeInBytes"],
                               "partitionColumns": list(detail["partitionColumns"])}
    spark.sql(f"CREATE MATERIALIZED LAKE VIEW {mlv} TBLPROPERTIES (delta.enableChangeDataFeed = true) AS {agg_pair_daily_select(fact)}")
    out["rows_after_create"] = spark.table(mlv).count()
    out["steps"]["initial"] = step(fact, mlv, f"{name}/initial")
    out["steps"]["hour_of_new_trades_today"] = step(
        fact, mlv, f"{name}/hour", lambda: merge_in(fact, gen(TODAY, TODAY_SEQ + TODAY_INITIAL, HOURLY)))
    out["steps"]["late_trades_old_day"] = step(
        fact, mlv, f"{name}/late", lambda: merge_in(fact, gen(LATE_DAY, 450_000, LATE_COUNT, [LATE_SYMBOL])))
    out["steps"]["nothing_new"] = step(fact, mlv, f"{name}/nothing")
    return out


results, errors = {}, {}
for variant, column in VARIANTS.items():
    try:
        results[variant] = run_variant(variant, column)
    except Exception:
        errors[variant] = traceback.format_exc()
        print(variant, "FAILED", errors[variant])
    notebookutils.fs.put(RESULT_PATH, json.dumps({"results": results, "errors": errors}, indent=2, default=str), True)

if errors:
    raise RuntimeError(f"variants failed: {sorted(errors)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
