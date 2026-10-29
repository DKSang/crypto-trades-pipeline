# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "01a138e9-6b56-461d-8df6-a037f9c2afdc",
# META       "workspaceId": "c8d6538e-e60c-467b-8440-dec5dae4d897"
# META     },
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
# alerts_enabled = False stops the event to es_dq_alerts, for tests that expect an error.
silver_table = "silver.trades"
dim_pair_table = "gold.dim_pair"
watermark_table = "ops.etl_watermark"
dq_result_table = "ops.dq_result"
alerts_enabled = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quality gate between Silver and Gold: table-level Great Expectations checks on the Silver batch that
# nb_load_gold is about to load (the Change Data Feed range after Gold's watermark, ADR 0002).
# check_batch is seam 1: it takes the Silver, dim_pair, watermark and result tables, runs the checks
# and writes every result to ops.dq_result. It never writes Gold and never moves a watermark: the
# notebook fails when an error-level check fails, which skips the Gold activities of pl_process_gold
# and so holds Gold's watermark. A warn is only recorded.
#
#   check_id               severity  passes when
#   schema                 error     the batch has exactly Silver's columns, with Silver's types
#   key_unique             error     (symbol, trade_id) is unique in Silver among the batch's keys
#   ref_integrity_dim_pair error     every symbol of the batch has a row in dim_pair
#   row_count              warn      the batch is no bigger than ROW_COUNT_FACTOR x the average day so far
#   freshness              warn      the newest _load_ts in the batch is under FRESHNESS_MAX_AGE_HOURS old
#   trade_id_contiguity    warn      no missing trade_id between a batch pair's first and last in Silver
#
# Why the last three are warns: a backfill or a collector pause legitimately breaks the volume and
# age baselines, and a trade_id gap between collector runs is accepted (ADR 0007); none of them makes
# a row wrong. Every error check finds a table that is wrong whatever the cause.
import json
import uuid
from datetime import datetime, timedelta

from pyspark.sql import functions as F

import great_expectations as gx
import great_expectations.expectations as gxe

ROW_COUNT_FACTOR = 3
ROW_COUNT_HISTORY_DAYS = 7
ROW_COUNT_MIN_DAYS = 2
FRESHNESS_MAX_AGE_HOURS = 3
ALERT_STREAM = "es_dq_alerts"
ALERT_SOURCE = "DqResults"

# Silver's columns and their Spark types (nb_load_silver.ensure_tables).
SILVER_COLUMNS = [("symbol", "StringType"), ("trade_id", "LongType"), ("side", "StringType"),
                  ("price", "DoubleType"), ("qty", "DoubleType"), ("ord_type", "StringType"),
                  ("trade_ts", "TimestampType"), ("_ingest_path", "StringType"),
                  ("_load_ts", "TimestampType"), ("_bronze_version", "LongType")]
CDF_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]

_gx = {}


def gx_validate(df, expectation):
    # One ephemeral context for the notebook; a new DataFrame asset per call.
    if "datasource" not in _gx:
        _gx["datasource"] = gx.get_context(mode="ephemeral").data_sources.add_spark("silver_batch")
    asset = _gx["datasource"].add_dataframe_asset(f"asset_{uuid.uuid4().hex}")
    batch = asset.add_batch_definition_whole_dataframe("whole").get_batch(batch_parameters={"dataframe": df})
    return batch.validate(expectation)


def outcome(check_id, severity, passed, unexpected_count=0, **details):
    return {"check_id": check_id, "severity": severity, "passed": bool(passed),
            "unexpected_count": int(unexpected_count or 0), "details": details}


def check_schema(df):
    expectations = [gxe.ExpectTableColumnsToMatchOrderedList(column_list=[c for c, _ in SILVER_COLUMNS])]
    expectations += [gxe.ExpectColumnValuesToBeOfType(column=c, type_=t) for c, t in SILVER_COLUMNS if c in df.columns]
    failures = []
    for expectation in expectations:
        result = gx_validate(df, expectation)
        if not result.success:
            failures.append({"expectation": type(expectation).__name__, "column": getattr(expectation, "column", None),
                             "observed": str(result.result.get("observed_value"))})
    return outcome("schema", "error", not failures, len(failures), failures=failures)


def check_key_unique(silver_at_batch, batch):
    # The Silver rows of the batch's keys, old and new: a repeat inside the batch or of an older row both show.
    rows = silver_at_batch.join(batch.select("symbol", "trade_id").distinct(), ["symbol", "trade_id"], "left_semi")
    result = gx_validate(rows, gxe.ExpectCompoundColumnsToBeUnique(column_list=["symbol", "trade_id"]))
    return outcome("key_unique", "error", result.success, result.result.get("unexpected_count"),
                   examples=result.result.get("partial_unexpected_list", [])[:10])


def check_ref_integrity(batch, dim_pair):
    known = ([r["symbol"] for r in spark.table(dim_pair).select("symbol").distinct().collect()]
             if spark.catalog.tableExists(dim_pair) else [])
    result = gx_validate(batch, gxe.ExpectColumnValuesToBeInSet(column="symbol", value_set=known or ["<dim_pair is empty>"]))
    missing = sorted(r["symbol"] for r in batch.select("symbol").distinct().collect() if r["symbol"] not in known)
    return outcome("ref_integrity_dim_pair", "error", result.success, result.result.get("unexpected_count"),
                   symbols_not_in_dim_pair=missing)


def check_row_count(batch, batch_rows, silver_before):
    # The baseline is the days Silver already holds before this batch; with too few days there is none yet.
    days = (silver_before.groupBy(F.to_date("trade_ts").alias("day")).count()
            .orderBy(F.col("day").desc()).limit(ROW_COUNT_HISTORY_DAYS).collect())
    if len(days) < ROW_COUNT_MIN_DAYS:
        return outcome("row_count", "warn", True, 0, skipped=f"fewer than {ROW_COUNT_MIN_DAYS} days in Silver before the batch",
                       batch_rows=batch_rows)
    average = sum(r["count"] for r in days) / len(days)
    limit = max(1, int(average * ROW_COUNT_FACTOR))
    result = gx_validate(batch, gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=limit))
    return outcome("row_count", "warn", result.success, 0 if result.success else batch_rows - limit,
                   batch_rows=batch_rows, average_rows_per_day=round(average), limit=limit, baseline_days=len(days))


def check_freshness(batch, now):
    oldest_allowed = now - timedelta(hours=FRESHNESS_MAX_AGE_HOURS)
    result = gx_validate(batch, gxe.ExpectColumnMaxToBeBetween(column="_load_ts", min_value=oldest_allowed))
    return outcome("freshness", "warn", result.success, 0 if result.success else 1,
                   newest_load_ts=str(result.result.get("observed_value")), oldest_allowed=str(oldest_allowed))


def check_contiguity(silver_at_batch, batch):
    # Missing trade_ids per pair over all of Silver, for the pairs in the batch (as nb_load_silver reports them).
    pairs = batch.select("symbol").distinct()
    missing = (silver_at_batch.join(pairs, "symbol", "left_semi").groupBy("symbol")
               .agg((F.max("trade_id") - F.min("trade_id") + 1 - F.count("*")).alias("missing")))
    result = gx_validate(missing, gxe.ExpectColumnValuesToBeBetween(column="missing", min_value=0, max_value=0))
    gaps = {r["symbol"]: r["missing"] for r in missing.where("missing > 0").collect()}
    return outcome("trade_id_contiguity", "warn", result.success, len(gaps), missing_per_pair=gaps)


def ensure_dq_table(dq_result):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {dq_result} (
        run_id STRING, checked_ts TIMESTAMP, table_name STRING, from_version BIGINT, to_version BIGINT,
        check_id STRING, severity STRING, passed BOOLEAN, unexpected_count BIGINT, details STRING
    ) USING DELTA
    """)


def read_watermark(watermark, source_table):
    rows = spark.table(watermark).where(F.col("source_table") == source_table).collect()
    return rows[0]["last_version"] if rows else None


def silver_start_version(silver):
    # The range nb_load_gold reads before its first run: after the last CREATE (OR REPLACE) of Silver.
    history = spark.sql(f"DESCRIBE HISTORY {silver}")
    created = history.filter(F.col("operation").contains("CREATE")).agg(F.max("version")).first()[0]
    if created is None:
        raise RuntimeError(f"No CREATE in the history of {silver}: insert its first watermark by hand")
    return created


def check_batch(silver, dim_pair, watermark, dq_result, now=None):
    # trade_ts and _load_ts are compared in UTC.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    now = now or datetime.utcnow()
    ensure_dq_table(dq_result)
    last = read_watermark(watermark, silver)  # Gold's position in Silver: the batch is what comes after it
    if last is None:
        last = silver_start_version(silver)
    to_version = spark.sql(f"DESCRIBE HISTORY {silver} LIMIT 1").first()["version"]  # captured once, before reading
    run_id = uuid.uuid4().hex
    summary = {"run_id": run_id, "from_version": last + 1, "to_version": to_version, "batch_rows": 0,
               "results": [], "errors": [], "warns": []}
    if to_version <= last:
        summary["skipped"] = "no new Silver commit"
        return summary

    changes = (spark.read.format("delta").option("readChangeFeed", "true")
               .option("startingVersion", last + 1).option("endingVersion", to_version)
               .table(silver).where("_change_type = 'insert'"))  # Silver is insert-only
    batch = changes.drop(*CDF_COLUMNS).persist()
    try:
        summary["batch_rows"] = batch_rows = batch.count()
        if not batch_rows:
            summary["skipped"] = "the new Silver commits added no rows"
            return summary
        silver_at_batch = spark.sql(f"SELECT * FROM {silver} VERSION AS OF {to_version}")
        silver_before = spark.sql(f"SELECT * FROM {silver} VERSION AS OF {last}")
        results = [check_schema(batch),
                   check_key_unique(silver_at_batch, batch),
                   check_ref_integrity(batch, dim_pair),
                   check_row_count(batch, batch_rows, silver_before),
                   check_freshness(batch, now),
                   check_contiguity(silver_at_batch, batch)]
    finally:
        batch.unpersist()

    rows = [(run_id, now, silver, last + 1, to_version, r["check_id"], r["severity"], r["passed"], r["unexpected_count"],
             json.dumps(r["details"], default=str)) for r in results]
    (spark.createDataFrame(rows, "run_id STRING, checked_ts TIMESTAMP, table_name STRING, from_version BIGINT, "
                                 "to_version BIGINT, check_id STRING, severity STRING, passed BOOLEAN, "
                                 "unexpected_count BIGINT, details STRING")
     .write.format("delta").mode("append").saveAsTable(dq_result))
    summary["results"] = [{k: r[k] for k in ("check_id", "severity", "passed", "unexpected_count")} for r in results]
    summary["errors"] = [r["check_id"] for r in results if r["severity"] == "error" and not r["passed"]]
    summary["warns"] = [r["check_id"] for r in results if r["severity"] == "warn" and not r["passed"]]
    return summary


def send_alerts(summary, silver):
    # One event per failed error-level check to the es_dq_alerts custom endpoint; Activator fires on each.
    # The endpoint key is read at run time, so it is never stored or printed (as nb_collect_trades does).
    import requests
    from azure.eventhub import EventData, EventHubProducerClient

    def fabric_get(path):
        token = notebookutils.credentials.getToken("pbi")
        r = requests.get(f"https://api.fabric.microsoft.com/v1/{path}", headers={"Authorization": f"Bearer {token}"}, timeout=30)
        r.raise_for_status()
        return r.json()

    ws = notebookutils.runtime.context["currentWorkspaceId"]
    es = next(i["id"] for i in fabric_get(f"workspaces/{ws}/eventstreams")["value"] if i["displayName"] == ALERT_STREAM)
    src = next(s["id"] for s in fabric_get(f"workspaces/{ws}/eventstreams/{es}/topology")["sources"] if s["name"] == ALERT_SOURCE)
    connection = fabric_get(f"workspaces/{ws}/eventstreams/{es}/sources/{src}/connection")["accessKeys"]["primaryConnectionString"]
    producer = EventHubProducerClient.from_connection_string(connection)
    try:
        batch = producer.create_batch()
        for r in summary["results"]:
            if r["severity"] == "error" and not r["passed"]:
                batch.add(EventData(json.dumps({
                    "run_id": summary["run_id"], "table_name": silver, "check_id": r["check_id"], "severity": "error",
                    "unexpected_count": r["unexpected_count"],
                    "message": f"Gold held: {r['check_id']} failed on {silver} (versions {summary['from_version']} to {summary['to_version']})"})))
        producer.send_batch(batch)
    finally:
        producer.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# nb_test_check_silver_batch sets library_only before it %runs this notebook, to get the functions without a run.
RESULT_PATH = "Files/checks/nb_check_silver_batch.json"

if not globals().get("library_only", False):
    summary = check_batch(silver_table, dim_pair_table, watermark_table, dq_result_table)
    summary["alert"] = "not needed"
    if summary["errors"]:
        summary["alert"] = "off"
        if alerts_enabled:
            try:
                send_alerts(summary, silver_table)
                summary["alert"] = "sent"
            except Exception as e:  # the block matters more than the alert: still fail the gate
                summary["alert"] = f"failed: {type(e).__name__}"  # not the message: it could echo the connection string
    notebookutils.fs.put(RESULT_PATH, json.dumps(summary, indent=2), True)
    print(summary)
    if summary["errors"]:
        raise RuntimeError(f"Quality gate blocked Gold: {', '.join(summary['errors'])} failed (run {summary['run_id']}, alert {summary['alert']})")
    notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
