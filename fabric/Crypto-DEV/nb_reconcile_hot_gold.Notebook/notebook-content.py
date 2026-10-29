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

# The table names are parameters so pl_process_gold can run on scratch tables (seam 2).
# from_date (UTC, YYYY-MM-DD) is the first day to reconcile; empty = yesterday. kql_database is the
# display name of the KQL database holding hot_daily.
bronze_table = "bronze.trades"
fact_table = "gold.fact_trade"
reconcile_table = "ops.reconcile_daily"
from_date = ""
kql_database = "eh_trades"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Daily reconcile of the hot path against Gold (issue #14, ADR 0004). It compares, per pair and UTC day,
# the trades and notional the hot path counted (eh_trades.hot_daily) with fact_trade, and attributes the
# gap to causes. It only writes ops.reconcile_daily: no Gold number changes.
#
# reconcile is the seam: the hot totals come in as a DataFrame (symbol, trade_date, hot_trades,
# hot_notional), so nb_test_reconcile_hot_gold can run it on scratch tables without Eventhouse.
#
# Bronze rows that are not from a backfill were sent live (the collector, or a test sender such as
# nb_test_whale_alert with path 'test'): the same events as the hot path got.
# R = those rows, U = their distinct (symbol, trade_id) keys, G = the keys in fact_trade. For a pair and day:
#   gap         = hot - G                        (count and notional; positive: the hot path counted more)
#   duplicates  = R - U                          events sent more than once (snapshot after a reconnect, overlapping runs)
#   late        = U not in G                     live trades Gold does not have yet: arrived after the last Gold load,
#                                                or held by the gate, or rejected by Silver (quarantine, test events)
#   backfill    = G not in U                     trades the live stream never delivered, added to Bronze by a backfill
#   unexplained = hot - R                        the hot path and Bronze got a different number of events
# so gap = duplicates + late - backfill + unexplained, exactly. A trade the collector missed and nothing
# backfilled is in neither the hot path nor Gold, so it is not in the gap: Silver's contiguity check shows it.
from datetime import date, timedelta

import requests
from pyspark.sql import functions as F

HOT_COLUMNS = "symbol STRING, trade_date DATE, hot_trades BIGINT, hot_notional DOUBLE"
API = "https://api.fabric.microsoft.com/v1"


def ensure_table(reconcile):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {reconcile} (
        trade_date DATE, symbol STRING,
        hot_trades BIGINT, hot_notional DOUBLE, gold_trades BIGINT, gold_notional DOUBLE,
        gap_trades BIGINT, gap_notional DOUBLE,
        duplicates_trades BIGINT, duplicates_notional DOUBLE, late_trades BIGINT, late_notional DOUBLE,
        backfill_trades BIGINT, backfill_notional DOUBLE, unexplained_trades BIGINT, unexplained_notional DOUBLE,
        _reconciled_ts TIMESTAMP
    ) USING DELTA
    """)


def finite_notional(price, qty):
    # A NaN or Infinity (Silver quarantines those rows) would poison every sum of its pair and day.
    notional = F.col(price) * F.col(qty)
    return F.when(F.isnan(notional) | (F.abs(notional) == float("inf")), F.lit(0.0)).otherwise(F.coalesce(notional, F.lit(0.0)))


def per_pair_day(frame, **aggregates):
    return frame.groupBy("trade_date", "symbol").agg(*[expr.alias(name) for name, expr in aggregates.items()])


def reconcile(bronze, fact, hot, reconcile_table, tracked):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    ensure_table(reconcile_table)
    hot = hot.where(F.col("symbol").isin(tracked))
    days = hot.select("trade_date").distinct()
    day_list = sorted(r["trade_date"] for r in days.collect())
    if not day_list:
        return {"days": [], "rows": 0}
    grid = days.crossJoin(spark.createDataFrame([(s,) for s in tracked], "symbol STRING"))

    hot_totals = per_pair_day(hot, hot_trades=F.sum("hot_trades"), hot_notional=F.sum("hot_notional"))
    gold_keys = (spark.table(fact).where(F.col("symbol").isin(tracked)).join(days, "trade_date")
                 .select("trade_date", "symbol", "trade_id", "notional"))
    gold_totals = per_pair_day(gold_keys, gold_trades=F.count("*"), gold_notional=F.sum("notional"))

    live_rows = (spark.table(bronze).where("coalesce(_ingest_path, '') <> 'backfill' AND trade_id IS NOT NULL")
               .where(F.col("symbol").isin(tracked))
               .withColumn("trade_date", F.to_date(F.try_to_timestamp("timestamp")))
               .join(days, "trade_date")
               .select("trade_date", "symbol", "trade_id", finite_notional("price", "qty").alias("notional")))
    live_row_totals = per_pair_day(live_rows, live_trades=F.count("*"), live_notional=F.sum("notional"))
    live_keys = live_rows.groupBy("trade_date", "symbol", "trade_id").agg(F.first("notional").alias("notional"))
    live_key_totals = per_pair_day(live_keys, live_key_trades=F.count("*"), live_key_notional=F.sum("notional"))

    late_totals = per_pair_day(live_keys.join(gold_keys.select("symbol", "trade_id"), ["symbol", "trade_id"], "left_anti"),
                               late_trades=F.count("*"), late_notional=F.sum("notional"))
    backfill_totals = per_pair_day(gold_keys.join(live_keys.select("symbol", "trade_id"), ["symbol", "trade_id"], "left_anti"),
                                   backfill_trades=F.count("*"), backfill_notional=F.sum("notional"))

    joined = grid
    for part in (hot_totals, gold_totals, live_row_totals, live_key_totals, late_totals, backfill_totals):
        joined = joined.join(part, ["trade_date", "symbol"], "left")
    zeroed = joined.na.fill(0)
    batch = zeroed.select(
        "trade_date", "symbol",
        F.col("hot_trades").cast("bigint"), "hot_notional",
        F.col("gold_trades").cast("bigint"), "gold_notional",
        (F.col("hot_trades") - F.col("gold_trades")).cast("bigint").alias("gap_trades"),
        F.round(F.col("hot_notional") - F.col("gold_notional"), 6).alias("gap_notional"),
        (F.col("live_trades") - F.col("live_key_trades")).cast("bigint").alias("duplicates_trades"),
        F.round(F.col("live_notional") - F.col("live_key_notional"), 6).alias("duplicates_notional"),
        F.col("late_trades").cast("bigint"), "late_notional",
        F.col("backfill_trades").cast("bigint"), "backfill_notional",
        (F.col("hot_trades") - F.col("live_trades")).cast("bigint").alias("unexplained_trades"),
        F.round(F.col("hot_notional") - F.col("live_notional"), 6).alias("unexplained_notional"),
        F.current_timestamp().alias("_reconciled_ts"))
    batch.createOrReplaceTempView("reconcile_batch")
    merged = spark.sql(f"""
        MERGE INTO {reconcile_table} AS t USING reconcile_batch AS s
        ON t.trade_date = s.trade_date AND t.symbol = s.symbol
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).first()
    return {"days": [str(d) for d in day_list], "rows": merged["num_affected_rows"]}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# nb_test_reconcile_hot_gold sets library_only before it %runs this notebook, to get reconcile without a run.
RESULT_PATH = "Files/checks/nb_reconcile_hot_gold.json"


def api_headers():
    return {"Authorization": f"Bearer {notebookutils.credentials.getToken('pbi')}", "x-ms-fabric-skill": "eventhouse-cli"}


def find_query_uri(name):
    workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
    response = requests.get(f"{API}/workspaces/{workspace_id}/kqlDatabases", headers=api_headers())
    response.raise_for_status()
    matches = [i["properties"] for i in response.json()["value"] if i["displayName"] == name]
    if len(matches) != 1:
        raise RuntimeError(f"Expected one KQL database named {name}, found {len(matches)}")
    return matches[0]["queryServiceUri"]


def read_hot(query_uri, database, from_day):
    # hot_daily is the materialized view over trades_raw that outlives trades_raw's one day (eh_trades DatabaseSchema.kql).
    csl = (f"hot_daily | where trade_date >= datetime({from_day}) "
           "| project symbol, trade_date, hot_trades = events, hot_notional = notional")
    response = requests.post(f"{query_uri}/v1/rest/query", json={"db": database, "csl": csl},
                             headers={"Authorization": f"Bearer {notebookutils.credentials.getToken('kusto')}"})
    response.raise_for_status()
    rows = [(r[0], date.fromisoformat(r[1][:10]), int(r[2]), float(r[3])) for r in response.json()["Tables"][0]["Rows"]]
    return spark.createDataFrame(rows, HOT_COLUMNS)


if not globals().get("library_only", False):
    import json
    import traceback

    try:
        lib = notebookutils.variableLibrary.getLibrary("vl_crypto")
        tracked_pairs = list(dict.fromkeys(p.strip() for p in lib.tracked_pairs.split(",") if p.strip()))
        if not tracked_pairs:
            # An empty list would reconcile nothing.
            raise ValueError("No tracked pairs in vl_crypto")

        # Default: yesterday and today (UTC). A backfill of an older day is reconciled by passing its date.
        start = from_date or (date.today() - timedelta(days=1)).isoformat()
        # A query names its database by its display name.
        hot = read_hot(find_query_uri(kql_database), kql_database, start)
        summary = reconcile(bronze_table, fact_table, hot, reconcile_table, tracked_pairs)
    except Exception:
        # A failed run's failureReason carries no traceback (reference/lessons.md).
        notebookutils.fs.put(RESULT_PATH, json.dumps({"failed": traceback.format_exc()}, indent=2), True)
        raise
    notebookutils.fs.put(RESULT_PATH, json.dumps(summary, indent=2), True)
    print(summary)
    notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
