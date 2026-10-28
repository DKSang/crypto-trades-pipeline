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
bronze_table = "bronze.trades"
silver_table = "silver.trades"
quarantine_table = "ops.quarantine_trades"
watermark_table = "ops.etl_watermark"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Bronze to Silver by Change Data Feed commit version (ADR 0002). load_increment is seam 1: it takes
# a Bronze table plus the Silver, quarantine and watermark tables, loads what Bronze committed since
# the watermark and returns the DQ results. Every table name is an argument so nb_test_load_silver
# can run it on scratch tables.
#
# Order is the safety: Silver MERGE, quarantine MERGE, then the watermark. Both MERGEs are
# insert-only, so a run that fails before the watermark moves is rerun over the same range and
# adds nothing twice (also what an overlapping run does).
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAW_COLUMNS = ["symbol", "side", "price", "qty", "ord_type", "trade_id", "timestamp", "_ingest_path", "_load_ts"]

def is_positive_number(column):
    # Spark says NaN > 0 is true, so NaN and Infinity are ruled out by hand; null is not positive.
    name = F.col(column)
    return F.coalesce((name > 0) & ~F.isnan(name) & (name < float("inf")), F.lit(False))


# First failing rule wins, so a row is quarantined once. try_to_timestamp gives null, not an error,
# for a time Silver can't cast.
ROW_RULES = [
    ("null_key", F.col("symbol").isNull() | F.col("trade_id").isNull()),
    ("non_positive_qty", ~is_positive_number("qty")),
    ("non_positive_price", ~is_positive_number("price")),
    ("invalid_side", ~F.coalesce(F.col("side").isin("buy", "sell"), F.lit(False))),
    ("invalid_ord_type", ~F.coalesce(F.col("ord_type").isin("limit", "market"), F.lit(False))),
    ("invalid_timestamp", F.try_to_timestamp(F.col("timestamp")).isNull()),
]


def ensure_tables(silver, quarantine, watermark):
    # Silver has Change Data Feed on: Gold reads it by commit version too.
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver} (
        symbol STRING, trade_id BIGINT, side STRING, price DOUBLE, qty DOUBLE, ord_type STRING,
        trade_ts TIMESTAMP, _ingest_path STRING, _load_ts TIMESTAMP, _bronze_version BIGINT
    ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    # Quarantine keeps the Bronze row as it came (raw types), the rule it broke and the commit it came in.
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {quarantine} (
        rule_id STRING, symbol STRING, side STRING, price DOUBLE, qty DOUBLE, ord_type STRING,
        trade_id BIGINT, timestamp STRING, _ingest_path STRING, _load_ts STRING,
        _commit_version BIGINT, _row_id STRING, _quarantined_ts TIMESTAMP
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


def bronze_start_version(bronze):
    # Before the first run, start after the last CREATE (OR REPLACE) of Bronze: Change Data Feed
    # can't be read across a schema change (reference/lessons.md).
    history = spark.sql(f"DESCRIBE HISTORY {bronze}")
    created = history.filter(F.col("operation").contains("CREATE")).agg(F.max("version")).first()[0]
    if created is None:
        raise RuntimeError(f"No CREATE in the history of {bronze}: insert its first watermark by hand")
    return created


def load_increment(bronze, silver, quarantine, watermark, tracked, fail_after_write=False):
    ensure_tables(silver, quarantine, watermark)
    last = read_watermark(watermark, bronze)
    if last is None:
        last = bronze_start_version(bronze)
    to_version = spark.sql(f"DESCRIBE HISTORY {bronze} LIMIT 1").first()["version"]  # captured once, before reading
    # gaps stays empty on a run that loaded nothing: it is measured on Silver after a load.
    result = {"from_version": last + 1, "to_version": to_version, "changes_read": 0, "untracked_dropped": 0,
              "quarantined": {}, "quarantined_inserted": 0, "duplicates_dropped": 0, "silver_inserted": 0,
              "gaps": {}}
    if to_version <= last:
        return result

    changes = (spark.read.format("delta")
               .option("readChangeFeed", "true")
               .option("startingVersion", last + 1)
               .option("endingVersion", to_version)
               .table(bronze)
               .where("_change_type = 'insert'"))  # Bronze is append-only
    rule_id = reduce(lambda chain, rule: chain.when(rule[1], rule[0]), ROW_RULES[1:],
                     F.when(ROW_RULES[0][1], ROW_RULES[0][0]))
    # Untracked pairs are not our trades, so they are dropped before the rules: only a row that
    # could be a tracked pair (or has no symbol at all) is checked.
    tagged = (changes.withColumn("_rule_id", rule_id)
              .withColumn("_bucket",
                          F.when(F.col("symbol").isNotNull() & ~F.col("symbol").isin(tracked), "untracked")
                           .when(F.col("_rule_id").isNotNull(), "quarantine")
                           .otherwise("valid"))
              .persist())
    try:
        counts = tagged.groupBy("_bucket", "_rule_id").count().collect()
        result["changes_read"] = sum(r["count"] for r in counts)
        result["untracked_dropped"] = sum(r["count"] for r in counts if r["_bucket"] == "untracked")
        result["quarantined"] = {r["_rule_id"]: r["count"] for r in counts if r["_bucket"] == "quarantine"}
        valid_rows = sum(r["count"] for r in counts if r["_bucket"] == "valid")

        # Silver: one row per (symbol, trade_id) in the batch (ADR 0006), earliest commit first, then insert-only.
        first_seen = Window.partitionBy("symbol", "trade_id").orderBy("_commit_version", "_load_ts")
        silver_batch = (tagged.where("_bucket = 'valid'")
                        .withColumn("_rn", F.row_number().over(first_seen)).where("_rn = 1")
                        .select("symbol", "trade_id", "side", "price", "qty", "ord_type",
                                F.try_to_timestamp(F.col("timestamp")).alias("trade_ts"), "_ingest_path",
                                F.try_to_timestamp(F.col("_load_ts")).alias("_load_ts"),
                                F.col("_commit_version").alias("_bronze_version")))
        silver_batch.createOrReplaceTempView("silver_batch")
        merged = spark.sql(f"""
            MERGE INTO {silver} AS t USING silver_batch AS s
            ON t.symbol = s.symbol AND t.trade_id = s.trade_id
            WHEN NOT MATCHED THEN INSERT *
        """).first()
        result["silver_inserted"] = merged["num_inserted_rows"]
        result["duplicates_dropped"] = valid_rows - spark.table("silver_batch").count()

        # Quarantine: the same row in the same commit is one quarantine row, so a reread adds nothing.
        quarantine_batch = (tagged.where("_bucket = 'quarantine'")
                            .select(F.col("_rule_id").alias("rule_id"), *RAW_COLUMNS, "_commit_version",
                                    F.sha2(F.to_json(F.struct(*RAW_COLUMNS)), 256).alias("_row_id"),
                                    F.current_timestamp().alias("_quarantined_ts"))
                            .dropDuplicates(["_commit_version", "_row_id"]))
        quarantine_batch.createOrReplaceTempView("quarantine_batch")
        merged = spark.sql(f"""
            MERGE INTO {quarantine} AS t USING quarantine_batch AS s
            ON t._commit_version = s._commit_version AND t._row_id = s._row_id
            WHEN NOT MATCHED THEN INSERT *
        """).first()
        result["quarantined_inserted"] = merged["num_inserted_rows"]

        # Missing trade_ids per pair over all of Silver (ADR 0006). A gap between collector runs is
        # accepted (ADR 0007): it is reported here and filled by backfill, never a reason to stop.
        gaps = (spark.table(silver).groupBy("symbol")
                .agg((F.max("trade_id") - F.min("trade_id") + 1 - F.count("*")).alias("missing"))
                .where("missing > 0").collect())
        result["gaps"] = {r["symbol"]: r["missing"] for r in gaps}

        if fail_after_write:
            raise RuntimeError("forced failure after the writes, before the watermark moves")

        spark.sql(f"""
            MERGE INTO {watermark} AS t
            USING (SELECT '{bronze}' AS source_table, CAST({to_version} AS BIGINT) AS last_version,
                          current_timestamp() AS _updated_ts) AS s
            ON t.source_table = s.source_table
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        tagged.unpersist()
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# nb_test_load_silver sets library_only before it %runs this notebook, to get the functions without a load.
RESULT_PATH = "Files/checks/nb_load_silver.json"

if not globals().get("library_only", False):
    import json

    lib = notebookutils.variableLibrary.getLibrary("vl_crypto")
    tracked_pairs = list(dict.fromkeys(p.strip() for p in lib.tracked_pairs.split(",") if p.strip()))
    if not tracked_pairs:
        # An empty list would drop every trade.
        raise ValueError("No tracked pairs in vl_crypto")

    summary = load_increment(bronze_table, silver_table, quarantine_table, watermark_table,
                             tracked_pairs, fail_after_write)
    notebookutils.fs.put(RESULT_PATH, json.dumps(summary, indent=2), True)
    print(summary)
    notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
