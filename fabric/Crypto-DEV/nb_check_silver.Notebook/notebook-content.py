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

# Read-only checks of the Bronze to Silver load (nb_load_silver): one Silver row per
# (symbol, trade_id), only tracked pairs, the watermark position against Bronze, and the quarantine
# by rule. Results go to Files/checks/nb_check_silver.json; the run fails if a check fails.

import json
import traceback
from pyspark.sql import functions as F

RESULT_PATH = "Files/checks/nb_check_silver.json"


def run_checks():
    lib = notebookutils.variableLibrary.getLibrary("vl_crypto")
    tracked = [p.strip() for p in lib.tracked_pairs.split(",") if p.strip()]

    silver = spark.table("silver.trades")
    stats = silver.agg(F.count("*").alias("rows"),
                       F.countDistinct("symbol", "trade_id").alias("distinct_trades"),
                       F.sum((~F.col("symbol").isin(tracked)).cast("int")).alias("untracked"),
                       F.sum(F.col("trade_ts").isNull().cast("int")).alias("null_trade_ts"),
                       F.max("_bronze_version").alias("max_bronze_version")).first()
    bronze_version = spark.sql("DESCRIBE HISTORY bronze.trades LIMIT 1").first()["version"]
    watermark = {r["source_table"]: r["last_version"] for r in spark.table("ops.etl_watermark").collect()}
    quarantine = {r["rule_id"]: r["count"] for r in spark.table("ops.quarantine_trades").groupBy("rule_id").count().collect()}

    result = {"silver_rows": stats["rows"], "distinct_trades": stats["distinct_trades"],
              "untracked": stats["untracked"], "null_trade_ts": stats["null_trade_ts"],
              "max_bronze_version": stats["max_bronze_version"], "bronze_version": bronze_version,
              "watermark": watermark, "quarantine": quarantine,
              "silver_history": [[r["version"], r["operation"]] for r in
                                 spark.sql("DESCRIBE HISTORY silver.trades").orderBy("version").collect()]}
    failures = []
    if stats["rows"] != stats["distinct_trades"]:
        failures.append("duplicate (symbol, trade_id) rows in silver.trades")
    if stats["untracked"]:
        failures.append("untracked pairs in silver.trades")
    if stats["null_trade_ts"]:
        failures.append("trade_ts could not be parsed")
    if watermark.get("bronze.trades") is None:
        failures.append("no watermark for bronze.trades")
    elif watermark["bronze.trades"] > bronze_version:
        failures.append("watermark is ahead of Bronze")
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
