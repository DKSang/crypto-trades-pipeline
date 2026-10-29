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

# Read-only numbers for the README (issue #16): data volume, ingest latency, data quality and the
# reconcile, read from the real tables for one collector session. Nothing is written except
# Files/checks/nb_check_pipeline_metrics.json. The session is the window of `nb_collect_trades`
# (started_utc / finished_utc in Files/checks/nb_collect_trades.json); change it to measure another one.

import json
import traceback
from pyspark.sql import functions as F

RESULT_PATH = "Files/checks/nb_check_pipeline_metrics.json"
SESSION_START = "2026-10-29T11:12:59"
SESSION_END = "2026-10-29T12:13:00"
SESSION_SECONDS = 3600.0
RECONCILE_DAY = "2026-10-29"
TABLES = ["bronze.trades", "silver.trades", "gold.fact_trade", "gold.dim_pair", "gold.dim_date", "gold.agg_pair_daily",
          "ops.quarantine_trades", "ops.dq_result", "ops.reconcile_daily"]
IN_WINDOW = f"_ingest_path = 'ws' AND _load_ts >= '{SESSION_START}' AND _load_ts < '{SESSION_END}'"


def volume():
    out = {}
    for table in TABLES:
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
            rows = spark.table(table).count()
            out[table] = {"rows": rows, "files": detail["numFiles"], "bytes": detail["sizeInBytes"],
                          "bytes_per_row": round(detail["sizeInBytes"] / rows, 1) if rows else None,
                          "rows_per_file": round(rows / detail["numFiles"], 1) if detail["numFiles"] else None}
        except Exception as e:
            out[table] = {"error": str(e)[:200]}
    return out


def session_window():
    bronze = spark.read.table("bronze.trades")
    window = bronze.where(IN_WINDOW)
    live = window.where(F.col("timestamp") >= SESSION_START)  # the subscribe snapshot holds older trades
    counts = window.agg(F.count("*").alias("rows"), F.countDistinct("symbol", "trade_id").alias("distinct_keys"),
                        F.min("_load_ts").alias("first_load"), F.max("_load_ts").alias("last_load")).first().asDict()
    counts["live_rows"] = live.count()
    counts["snapshot_rows"] = counts["rows"] - counts["live_rows"]
    counts["live_rows_per_second"] = round(counts["live_rows"] / SESSION_SECONDS, 2)
    # keys the window repeats from before it (an earlier session's tail, tests): events Silver drops
    before = bronze.where(f"_load_ts < '{SESSION_START}'").select("symbol", "trade_id").distinct()
    counts["keys_already_in_bronze"] = window.select("symbol", "trade_id").distinct().join(before, ["symbol", "trade_id"]).count()

    # latency, live trades only: collector = Kraken time to our send; landing = our send to the Bronze file;
    # end to end = Kraken time to the Bronze file (the file's write time stands for the commit)
    lat = (spark.read.format("delta").table("bronze.trades").where(IN_WINDOW).where(F.col("timestamp") >= SESSION_START)
           .select(F.to_timestamp("timestamp").alias("ts"), F.to_timestamp("_load_ts").alias("sent"),
                   F.col("_metadata.file_modification_time").alias("file"))
           .select((F.col("sent").cast("double") - F.col("ts").cast("double")).alias("collector_s"),
                   (F.col("file").cast("double") - F.col("sent").cast("double")).alias("landing_s"),
                   (F.col("file").cast("double") - F.col("ts").cast("double")).alias("end_to_end_s")))
    latency = {}
    for column in ["collector_s", "landing_s", "end_to_end_s"]:
        p = lat.select(F.expr(f"percentile_approx({column}, array(0.5, 0.95, 0.99))").alias("p"), F.max(column).alias("max")).first()
        latency[column] = {"p50": p["p"][0], "p95": p["p"][1], "p99": p["p"][2], "max": p["max"]}
    counts["latency_s"] = latency
    return counts


def quality():
    silver = spark.table("silver.trades").where(IN_WINDOW)
    quarantine = spark.table("ops.quarantine_trades").where(IN_WINDOW)
    silver_keys = silver.select("symbol", "trade_id").distinct().count()
    quarantined = quarantine.count()
    out = {"silver_rows_in_window": silver.count(), "silver_keys_in_window": silver_keys, "quarantined_in_window": quarantined,
           "quarantined_by_rule": {r["rule_id"]: r["count"] for r in quarantine.groupBy("rule_id").count().collect()},
           "pass_rate": round(silver_keys / (silver_keys + quarantined), 6) if silver_keys + quarantined else None}
    dq = spark.table("ops.dq_result")
    out["dq_checks_all_time"] = [r.asDict() for r in dq.groupBy("check_id", "severity")
                                 .agg(F.count("*").alias("runs"), F.sum(F.col("passed").cast("int")).alias("passed"))
                                 .orderBy("check_id").collect()]
    out["dq_runs_since_session_start"] = [r.asDict() for r in
                                          dq.where(f"checked_ts >= '{SESSION_START}'").groupBy("check_id", "severity", "passed")
                                          .agg(F.count("*").alias("runs"), F.sum("unexpected_count").alias("unexpected"))
                                          .orderBy("check_id").collect()]
    bronze = spark.table("bronze.trades")
    out["bronze_duplicates_all_time"] = bronze.count() - bronze.select("symbol", "trade_id").distinct().count()
    return out


def reconcile():
    rows = spark.table("ops.reconcile_daily").where(f"trade_date = '{RECONCILE_DAY}'")
    sums = rows.agg(*[F.sum(c).alias(c) for c in rows.columns if c.endswith("_trades") or c.endswith("_notional")]).first().asDict()
    sums["pairs"] = rows.count()
    sums["pairs_with_gap"] = rows.where("gap_trades != 0").count()
    sums["reconciled_ts"] = str(rows.agg(F.max("_reconciled_ts")).first()[0])
    sums["test_events_in_bronze_that_day"] = spark.table("bronze.trades").where(
        f"_ingest_path = 'test' AND substr(timestamp, 1, 10) = '{RECONCILE_DAY}'").count()
    return sums


result = {"session": {"start": SESSION_START, "end": SESSION_END}, "failures": []}
for name, fn in [("volume", volume), ("session", session_window), ("quality", quality), ("reconcile", reconcile)]:
    try:
        result[name] = fn()
    except Exception:
        result[name] = {"error": traceback.format_exc()}
        result["failures"].append(name)

notebookutils.fs.put(RESULT_PATH, json.dumps(result, indent=2, default=str), True)
print(json.dumps(result, indent=2, default=str))
if result["failures"]:
    raise RuntimeError("failed: " + ", ".join(result["failures"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
