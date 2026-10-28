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

# fail_after_write raises after the writes and before the watermark moves, to prove a rerun is safe.
# The table names are parameters so pl_process_gold can run on scratch tables (seam 2).
fail_after_write = False
silver_table = "silver.trades"
fact_table = "gold.fact_trade"
dim_pair_table = "gold.dim_pair"
dim_date_table = "gold.dim_date"
watermark_table = "ops.etl_watermark"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Silver to Gold by Change Data Feed commit version (ADR 0002). load_increment is seam 1 for Gold:
# it takes the Silver table plus the Gold and watermark tables, loads what Silver committed since
# the watermark and returns the counts. Every table name is an argument so nb_test_load_gold can
# run it on scratch tables. The doc of every Gold table: reference/architecture/gold-model.md.
#
# Order is the safety: dim_pair, dim_date and fact_trade MERGEs, then the watermark. All three
# MERGEs are insert-only, so a run that fails before the watermark moves is rerun over the same
# range and adds nothing twice (also what an overlapping run does).
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def agg_pair_daily_select(fact):
    # The SELECT of the Materialized Lake View gold.agg_pair_daily, at the grain pair x UTC day.
    # Always recomputed from the deduplicated fact, so a backfill cannot double it. Open and close
    # are the price of the first and last trade of the day by (trade_ts, trade_id): min_by/max_by
    # over a struct, since a window function would be a full refresh anyway and is not needed.
    return f"""
    SELECT
        symbol,
        trade_date,
        min_by(price, struct(trade_ts, trade_id)) AS `open`,
        max(price) AS high,
        min(price) AS low,
        max_by(price, struct(trade_ts, trade_id)) AS `close`,
        sum(qty) AS volume,
        sum(notional) AS notional,
        sum(notional) / sum(qty) AS vwap,
        count(*) AS trade_count,
        sum(CASE WHEN side = 'buy' THEN notional ELSE 0 END) AS taker_buy_notional
    FROM {fact}
    GROUP BY symbol, trade_date
    """


def ensure_tables(fact, dim_pair, dim_date, watermark):
    # fact_trade has Change Data Feed on: the MLV reads it (ADR 0003).
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {fact} (
        symbol STRING, trade_id BIGINT, trade_ts TIMESTAMP, trade_date DATE, side STRING, ord_type STRING,
        price DOUBLE, qty DOUBLE, notional DOUBLE, _silver_version BIGINT
    ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    # Same columns as nb_build_dim_pair, which owns base, quote and status; this notebook only adds
    # a row for a pair that has trades but no row yet.
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {dim_pair} (
        symbol STRING, base STRING, quote STRING, status STRING, is_tracked BOOLEAN
    ) USING DELTA
    """)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {dim_date} (
        date DATE, year INT, quarter INT, month INT, day INT, day_of_week INT, day_name STRING, is_weekend BOOLEAN
    ) USING DELTA
    """)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {watermark} (
        source_table STRING, last_version BIGINT, _updated_ts TIMESTAMP
    ) USING DELTA
    """)


def read_watermark(watermark, source_table):
    rows = spark.table(watermark).where(F.col("source_table") == source_table).collect()
    return rows[0]["last_version"] if rows else None


def silver_start_version(silver):
    # Before the first run, start after the last CREATE (OR REPLACE) of Silver: Change Data Feed
    # can't be read across a schema change (reference/lessons.md).
    history = spark.sql(f"DESCRIBE HISTORY {silver}")
    created = history.filter(F.col("operation").contains("CREATE")).agg(F.max("version")).first()[0]
    if created is None:
        raise RuntimeError(f"No CREATE in the history of {silver}: insert its first watermark by hand")
    return created


def date_rows(batch):
    # One row per UTC day from the first to the last day of the batch, so dim_date has no hole
    # even for a day without trades.
    bounds = batch.agg(F.min("trade_date").alias("first"), F.max("trade_date").alias("last"))
    days = bounds.select(F.explode(F.sequence("first", "last")).alias("date"))
    return days.select(
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        (((F.dayofweek("date") + 5) % 7) + 1).alias("day_of_week"),  # ISO: Monday = 1
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"))


def load_increment(silver, fact, dim_pair, dim_date, watermark, tracked, fail_after_write=False):
    # trade_date is the UTC day: the session must not shift it.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    ensure_tables(fact, dim_pair, dim_date, watermark)
    last = read_watermark(watermark, silver)
    if last is None:
        last = silver_start_version(silver)
    to_version = spark.sql(f"DESCRIBE HISTORY {silver} LIMIT 1").first()["version"]  # captured once, before reading
    result = {"from_version": last + 1, "to_version": to_version, "changes_read": 0, "duplicates_dropped": 0,
              "fact_inserted": 0, "dim_pair_inserted": 0, "dim_date_inserted": 0}
    if to_version <= last:
        return result

    changes = (spark.read.format("delta")
               .option("readChangeFeed", "true")
               .option("startingVersion", last + 1)
               .option("endingVersion", to_version)
               .table(silver)
               .where("_change_type = 'insert'"))  # Silver is insert-only
    typed = changes.select("symbol", "trade_id", "trade_ts", F.to_date("trade_ts").alias("trade_date"), "side",
                           "ord_type", "price", "qty", (F.col("qty") * F.col("price")).alias("notional"),
                           F.col("_commit_version").alias("_silver_version")).persist()
    try:
        result["changes_read"] = typed.count()
        # One row per (symbol, trade_id) in the batch (ADR 0006), earliest commit first, then insert-only.
        first_seen = Window.partitionBy("symbol", "trade_id").orderBy("_silver_version")
        batch = typed.withColumn("_rn", F.row_number().over(first_seen)).where("_rn = 1").drop("_rn").persist()
        try:
            batch_rows = batch.count()
            result["duplicates_dropped"] = result["changes_read"] - batch_rows
            if batch_rows:
                batch.createOrReplaceTempView("fact_batch")
                pairs = (batch.select("symbol").distinct()
                         .select("symbol",
                                 F.split("symbol", "/")[0].alias("base"),
                                 F.split("symbol", "/")[1].alias("quote"),
                                 F.lit(None).cast("string").alias("status"),
                                 F.col("symbol").isin(tracked).alias("is_tracked")))
                pairs.createOrReplaceTempView("pair_batch")
                date_rows(batch).createOrReplaceTempView("date_batch")

                merged = spark.sql(f"""
                    MERGE INTO {dim_pair} AS t USING pair_batch AS s ON t.symbol = s.symbol
                    WHEN NOT MATCHED THEN INSERT *
                """).first()
                result["dim_pair_inserted"] = merged["num_inserted_rows"]
                merged = spark.sql(f"""
                    MERGE INTO {dim_date} AS t USING date_batch AS s ON t.date = s.date
                    WHEN NOT MATCHED THEN INSERT *
                """).first()
                result["dim_date_inserted"] = merged["num_inserted_rows"]
                merged = spark.sql(f"""
                    MERGE INTO {fact} AS t USING fact_batch AS s
                    ON t.symbol = s.symbol AND t.trade_id = s.trade_id
                    WHEN NOT MATCHED THEN INSERT *
                """).first()
                result["fact_inserted"] = merged["num_inserted_rows"]
        finally:
            batch.unpersist()

        if fail_after_write:
            raise RuntimeError("forced failure after the writes, before the watermark moves")

        spark.sql(f"""
            MERGE INTO {watermark} AS t
            USING (SELECT '{silver}' AS source_table, CAST({to_version} AS BIGINT) AS last_version,
                          current_timestamp() AS _updated_ts) AS s
            ON t.source_table = s.source_table
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        typed.unpersist()
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# nb_test_load_gold and nb_refresh_agg_pair_daily set library_only before they %run this notebook,
# to get the functions without a load.
RESULT_PATH = "Files/checks/nb_load_gold.json"

if not globals().get("library_only", False):
    import json

    lib = notebookutils.variableLibrary.getLibrary("vl_crypto")
    tracked_pairs = list(dict.fromkeys(p.strip() for p in lib.tracked_pairs.split(",") if p.strip()))
    if not tracked_pairs:
        # An empty list would flag every pair as untracked.
        raise ValueError("No tracked pairs in vl_crypto")

    summary = load_increment(silver_table, fact_table, dim_pair_table, dim_date_table, watermark_table,
                             tracked_pairs, fail_after_write)
    notebookutils.fs.put(RESULT_PATH, json.dumps(summary, indent=2), True)
    print(summary)
    notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
