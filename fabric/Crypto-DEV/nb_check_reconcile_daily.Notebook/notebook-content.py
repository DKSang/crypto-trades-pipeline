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

# Read-only check of ops.reconcile_daily (issue #14): one row per pair and UTC day with the reconcile gap and
# its causes, biggest gap first. Result in Files/checks/nb_check_reconcile_daily.json.
import json

from pyspark.sql import functions as F

table = spark.table("ops.reconcile_daily")
rows = [r.asDict() for r in table.orderBy(F.col("trade_date").desc(), F.abs(F.col("gap_trades")).desc()).collect()]
per_pair_day = table.groupBy("trade_date", "symbol").count().where("count > 1").count()
summary = {"rows": len(rows), "pair_days_with_more_than_one_row": per_pair_day, "days": sorted({str(r["trade_date"]) for r in rows}),
           "sample": rows[:25]}
notebookutils.fs.put("Files/checks/nb_check_reconcile_daily.json", json.dumps(summary, indent=2, default=str), True)
print(json.dumps(summary, indent=2, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
