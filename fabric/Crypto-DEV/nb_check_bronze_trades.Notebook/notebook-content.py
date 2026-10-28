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

# Read-only checks of bronze.trades: one row per trade, load columns filled, trade_id
# contiguous per pair (holds within one collector run without a reconnect), and
# Change Data Feed read by commit version returns the rows each commit landed.
# Results go to Files/checks/nb_check_bronze_trades.json (the run's failure reason
# doesn't carry the message); the run fails if a check fails.

import json
import traceback
from pyspark.sql import functions as F

RESULT_PATH = "Files/checks/nb_check_bronze_trades.json"


def run_checks():
    trades = spark.table("bronze.trades")
    history = spark.sql("DESCRIBE HISTORY bronze.trades")
    detail = spark.sql("DESCRIBE DETAIL bronze.trades").first()
    latest_version = history.agg(F.max("version")).first()[0]

    stats = trades.agg(
        F.count("*").alias("rows"),
        F.sum((F.col("symbol").isNull() | F.col("trade_id").isNull()).cast("int")).alias("null_key"),
        F.sum((F.col("_ingest_path") != "ws").cast("int")).alias("other_path"),
        F.sum(F.col("_load_ts").isNull().cast("int")).alias("null_load_ts"),
        F.countDistinct("symbol", "trade_id").alias("distinct_trades"),
        F.min("_load_ts").alias("first_load_ts"),
        F.max("_load_ts").alias("last_load_ts"),
    ).first()

    # Every commit after the last CREATE (OR REPLACE) TABLE is an es_trades write. CDF can't
    # be read across that commit when it changed the schema, so start right after it.
    created_version = history.filter(F.col("operation").contains("CREATE")).agg(F.max("version")).first()[0]
    cdf_by_version = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", created_version + 1)
        .option("endingVersion", latest_version)
        .table("bronze.trades")
        .groupBy("_commit_version", "_change_type")
        # Lag: the longest any trade in a commit waited between the collector and the commit.
        .agg(F.count("*").alias("count"),
             F.max(F.col("_commit_timestamp").cast("double") - F.to_timestamp("_load_ts").cast("double")).alias("max_lag_s"))
        .orderBy("_commit_version")
        .collect()
    )
    cdf_rows = sum(r["count"] for r in cdf_by_version)

    # Missing trade_ids per pair between its first and last trade in Bronze.
    pairs = (
        trades.groupBy("symbol")
        .agg(F.countDistinct("trade_id").alias("trades"),
             (F.max("trade_id") - F.min("trade_id") + 1 - F.countDistinct("trade_id")).alias("missing_ids"))
        .orderBy("symbol")
        .collect()
    )

    result = {
        "protocol": [detail["minReaderVersion"], detail["minWriterVersion"]],
        "history": [[r["version"], str(r["timestamp"]), r["operation"]] for r in history.orderBy("version").collect()],
        "rows": stats["rows"],
        "distinct_trades": stats["distinct_trades"],
        "null_key": stats["null_key"],
        "other_path": stats["other_path"],
        "null_load_ts": stats["null_load_ts"],
        "first_load_ts": str(stats["first_load_ts"]),
        "last_load_ts": str(stats["last_load_ts"]),
        "cdf_rows": cdf_rows,
        "cdf_by_version": [[r["_commit_version"], r["_change_type"], r["count"], round(r["max_lag_s"], 1)] for r in cdf_by_version],
        "pairs": {r["symbol"]: [r["trades"], r["missing_ids"]] for r in pairs},
    }
    failures = []
    if stats["rows"] == 0:
        failures.append("bronze.trades is empty")
    if stats["null_key"]:
        failures.append("rows without symbol or trade_id (array or empty payload?)")
    # Duplicates (snapshot after each run start) and gaps (between runs) are expected
    # across runs and handled in Silver, so they are reported, not failed.
    result["warnings"] = [w for w, bad in [
        ("duplicate (symbol, trade_id) rows", stats["distinct_trades"] != stats["rows"]),
        ("trade_id gaps", any(r["missing_ids"] for r in pairs)),
    ] if bad]
    if stats["other_path"] or stats["null_load_ts"]:
        failures.append("load columns not filled")
    if cdf_rows != stats["rows"]:
        failures.append("Change Data Feed does not return every landed row")
    result["failures"] = failures
    return result


try:
    result = run_checks()
except Exception:
    result = {"failures": ["check crashed"], "error": traceback.format_exc()}

notebookutils.fs.put(RESULT_PATH, json.dumps(result, indent=2), True)
print(json.dumps(result, indent=2))
if result["failures"]:
    raise RuntimeError("; ".join(result["failures"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
