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

# PARAMETERS CELL ********************

# The names differ from nb_load_gold's parameters on purpose: %run shares variables both ways, so a
# parameter named fact_table would be reset to nb_load_gold's default when it is %run.
mlv_source_table = "gold.fact_trade"
mlv_table = "gold.agg_pair_daily"
refresh_full = False  # True forces a FULL refresh (a manual rebuild)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

# Creates the Materialized Lake View agg_pair_daily when it is missing and refreshes it, as the last
# step of pl_process_gold. The pipeline's own Refresh Materialized Lake View activity is not used:
# it doesn't support a service principal and refreshes every MLV of the lakehouse
# (https://learn.microsoft.com/fabric/data-factory/refresh-materialized-lake-view-activity).
# A plain REFRESH lets Fabric choose between skipping, an incremental MERGE of the changed rows and a
# full overwrite. With Change Data Feed on and fact_trade partitioned by trade_date it merged only the
# affected rows (OBSERVED 2026-10-29, reference/lessons.md). A FULL refresh runs on the first run of
# each Sunday (UTC hour 3) as a sweep for what incremental refresh can miss, and when refresh_full is
# set. To change the SELECT, run CREATE OR REPLACE by hand.
import json
from datetime import datetime, timezone

if not spark.catalog.tableExists(mlv_source_table):
    raise RuntimeError(f"{mlv_source_table} does not exist: run nb_load_gold first")
# Change Data Feed on the source is what lets Fabric refresh incrementally (ADR 0003).
spark.sql(f"ALTER TABLE {mlv_source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
spark.sql(f"""
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS {mlv_table}
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
{agg_pair_daily_select(mlv_source_table)}
""")
now = datetime.now(timezone.utc)
mode = "FULL" if refresh_full or (now.weekday() == 6 and now.hour == 3) else "optimal"
spark.sql(f"REFRESH MATERIALIZED LAKE VIEW {mlv_table}" + (" FULL" if mode == "FULL" else ""))

summary = {"mlv": mlv_table, "refresh": mode, "rows": spark.table(mlv_table).count()}
print(summary)
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
