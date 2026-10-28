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
# The view is recomputed in full (REFRESH ... FULL, the one documented SQL form): open and close
# (min_by/max_by) are not among the constructs incremental refresh supports, and a day of Kraken
# trades for 20 pairs is ~350,000 rows. To change the SELECT, run CREATE OR REPLACE by hand.
import json

if not spark.catalog.tableExists(mlv_source_table):
    raise RuntimeError(f"{mlv_source_table} does not exist: run nb_load_gold first")
# Change Data Feed on the source lets Fabric refresh incrementally if the SELECT ever allows it (ADR 0003).
spark.sql(f"ALTER TABLE {mlv_source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
spark.sql(f"""
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS {mlv_table}
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
{agg_pair_daily_select(mlv_source_table)}
""")
spark.sql(f"REFRESH MATERIALIZED LAKE VIEW {mlv_table} FULL")

summary = {"mlv": mlv_table, "rows": spark.table(mlv_table).count()}
print(summary)
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
