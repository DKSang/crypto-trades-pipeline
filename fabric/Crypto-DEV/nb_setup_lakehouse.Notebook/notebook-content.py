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

# Idempotent setup of lh_crypto: medallion schemas and the Bronze trades table.
# bronze.trades is created here, not by es_trades, so its shape is fixed up front
# (the Eventstream destination would otherwise take the schema from the first event)
# and Change Data Feed is on from the first commit (ADR 0002).
# The Eventstream Delta writer rejects table features (writer v7), and Fabric Spark turns
# on deletion vectors by default, which forces v7. So pin the legacy protocol: writer v4
# is the lowest version that supports Change Data Feed.

for schema in ["bronze", "silver", "gold", "ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

notebookutils.fs.mkdirs("Files/checks")  # stats and check results of the other notebooks

# A bronze.trades of an older shape (time columns as TIMESTAMP) is replaced in place:
# DROP TABLE would deactivate the es_trades destination (reference/lessons.md).
old_shape = (spark.catalog.tableExists("bronze.trades")
             and dict(spark.table("bronze.trades").dtypes).get("_load_ts") != "string")

# Bronze is raw: Kraken WS v2 names and types as sent (naming-conventions.md). Time fields
# stay ISO 8601 strings, which es_trades can't write to a TIMESTAMP column; Silver casts them.
spark.sql(f"""
{"CREATE OR REPLACE TABLE" if old_shape else "CREATE TABLE IF NOT EXISTS"} bronze.trades (
    symbol STRING,
    side STRING,
    price DOUBLE,
    qty DOUBLE,
    ord_type STRING,
    trade_id BIGINT,
    timestamp STRING,
    _ingest_path STRING,
    _load_ts STRING
)
USING DELTA
TBLPROPERTIES (
    delta.enableChangeDataFeed = true,
    delta.enableDeletionVectors = false,
    delta.minReaderVersion = 1,
    delta.minWriterVersion = 4
)
""")

display(spark.sql("SHOW TBLPROPERTIES bronze.trades"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
