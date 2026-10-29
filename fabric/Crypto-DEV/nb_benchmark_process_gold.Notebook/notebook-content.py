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

# Scale run of pl_process_gold (issue #16), in Crypto-DEV: the real pipeline runs, through the Fabric job API with the
# table names as parameters (as nb_test_process_gold does), on scratch tables `test.sc_*` fed with rows of
# `test.bm_src`, the Bronze-shaped copy that nb_benchmark_bronze leaves. The real tables are never touched.
#   one fresh run per size in SIZES: seconds per activity and for the pipeline, rows per second, and the
#   rows per second between two sizes (which leaves out the fixed cost of starting each Spark session).
#   After each run Silver, Gold and the aggregate must match Bronze (row count and notional).
#   Not measured here: reruns, duplicate batches and recovery; nb_test_process_gold covers them without timing.
# The rows are synthetic volume on real shape; every row is valid and tracked, so Silver quarantines nothing.
# Activity times include the start of each notebook's Spark session. Other runs of the capacity (the hourly
# real pl_process_gold) can overlap and slow a step: every run's start and end are kept.
# Results go to Files/checks/nb_benchmark_process_gold.json; the run fails if a step fails.

import json
import math
import time
import traceback
from datetime import datetime, timedelta, timezone

import requests
from pyspark.sql import functions as F

RESULT_PATH = "Files/checks/nb_benchmark_process_gold.json"
PIPELINE = "pl_process_gold"
SRC = "test.bm_src"
SIZES = [17_000, 350_000, 3_500_000]   # about an hour of the live session, a day, ten days
COMMIT_ROWS = 20_000                   # rows per Bronze commit when landing (a batch is at most 25 commits)
BRONZE_COLUMNS = ["symbol", "side", "price", "qty", "ord_type", "trade_id", "timestamp", "_ingest_path", "_load_ts"]
T = {name: f"test.sc_{name}" for name in
     ["bronze_trades", "silver_trades", "quarantine_trades", "etl_watermark", "fact_trade", "dim_pair", "dim_date", "agg_pair_daily",
      "dq_result", "reconcile_daily"]}
BRONZE_SCHEMA = ("symbol STRING, side STRING, price DOUBLE, qty DOUBLE, ord_type STRING, "
                 "trade_id BIGINT, timestamp STRING, _ingest_path STRING, _load_ts STRING")

API = "https://api.fabric.microsoft.com/v1"
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
result = {"settings": {"sizes": SIZES}, "scale": {}}


def save():
    notebookutils.fs.put(RESULT_PATH, json.dumps(result, indent=2, default=str), True)


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def api_headers():
    return {"Authorization": f"Bearer {notebookutils.credentials.getToken('pbi')}", "x-ms-fabric-skill": "spark-cli"}


def moment(text):
    return datetime.fromisoformat(text[:26].rstrip("Z"))


def pipeline_id():
    items = requests.get(f"{API}/workspaces/{WORKSPACE_ID}/items?type=DataPipeline", headers=api_headers()).json()["value"]
    matches = [i["id"] for i in items if i["displayName"] == PIPELINE]
    check(len(matches) == 1, f"items named {PIPELINE}: {len(matches)}")
    return matches[0]


def activity_seconds(run_id):
    now = datetime.now(timezone.utc)
    body = {"filters": [], "orderBy": [{"orderBy": "ActivityRunStart", "order": "ASC"}],
            "lastUpdatedAfter": (now - timedelta(days=1)).isoformat(), "lastUpdatedBefore": (now + timedelta(days=1)).isoformat()}
    response = requests.post(f"{API}/workspaces/{WORKSPACE_ID}/datapipelines/pipelineruns/{run_id}/queryactivityruns",
                             headers=api_headers(), json=body)
    check(response.status_code == 200, f"queryactivityruns: {response.status_code}")
    return {a["activityName"]: round(a["durationInMs"] / 1000, 1) for a in response.json()["value"]}


def run_pipeline(fail_gold_after_write=False, timeout_s=3000):
    parameters = {"bronze_table": T["bronze_trades"], "silver_table": T["silver_trades"],
                  "quarantine_table": T["quarantine_trades"], "fact_table": T["fact_trade"],
                  "dim_pair_table": T["dim_pair"], "dim_date_table": T["dim_date"],
                  "watermark_table": T["etl_watermark"], "mlv_table": T["agg_pair_daily"],
                  "dq_result_table": T["dq_result"], "reconcile_table": T["reconcile_daily"],
                  "send_dq_alerts": False, "fail_gold_after_write": fail_gold_after_write}
    response = requests.post(f"{API}/workspaces/{WORKSPACE_ID}/items/{pipeline_id()}/jobs/instances?jobType=Pipeline",
                             headers=api_headers(), json={"executionData": {"parameters": parameters}})
    check(response.status_code == 202, f"start pipeline: {response.status_code}")
    location = response.headers["Location"]
    started = time.time()
    while time.time() - started < timeout_s:
        time.sleep(10)
        job = requests.get(location, headers=api_headers()).json()
        if job["status"] in ("Completed", "Failed", "Cancelled", "Deduped"):
            run = {"id": job["id"], "status": job["status"], "start_utc": job["startTimeUtc"], "end_utc": job["endTimeUtc"],
                   "seconds": round((moment(job["endTimeUtc"]) - moment(job["startTimeUtc"])).total_seconds(), 1)}
            run["activities_s"] = activity_seconds(job["id"])
            return run
    raise TimeoutError(f"pipeline run {location} did not finish in {timeout_s} s")


def reset():
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql("CREATE SCHEMA IF NOT EXISTS test")
    drop_all()
    spark.sql(f"CREATE TABLE {T['bronze_trades']} ({BRONZE_SCHEMA}) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    # The quality gate needs every pair of the batch in dim_pair: a copy of the real one.
    spark.sql(f"CREATE TABLE {T['dim_pair']} USING DELTA AS SELECT * FROM gold.dim_pair")


def drop_all():
    spark.sql(f"DROP MATERIALIZED LAKE VIEW IF EXISTS {T['agg_pair_daily']}")
    for name, table in T.items():
        if name != "agg_pair_daily":
            spark.sql(f"DROP TABLE IF EXISTS {table}")


def land(low, high):
    """Append the source rows with low < trade_id <= high to Bronze, as several commits."""
    step = max(COMMIT_ROWS, math.ceil((high - low) / 25))
    for a in range(low, high, step):
        b = min(a + step, high)
        (spark.table(SRC).where((F.col("trade_id") > a) & (F.col("trade_id") <= b)).select(*BRONZE_COLUMNS)
         .write.format("delta").mode("append").saveAsTable(T["bronze_trades"]))


def watermarks():
    rows = {r["source_table"]: r["last_version"] for r in spark.table(T["etl_watermark"]).collect()}
    return {"bronze": rows.get(T["bronze_trades"]), "silver": rows.get(T["silver_trades"])}


def state():
    fact = spark.table(T["fact_trade"]).agg(F.count("*").alias("rows"), F.sum("notional").alias("notional")).first()
    mlv = spark.table(T["agg_pair_daily"]).agg(F.sum("trade_count").alias("trades"), F.sum("notional").alias("notional")).first()
    return {"bronze_rows": spark.table(T["bronze_trades"]).count(), "silver_rows": spark.table(T["silver_trades"]).count(),
            "fact_rows": fact["rows"], "fact_notional": fact["notional"],
            "mlv_trades": mlv["trades"], "mlv_notional": mlv["notional"], "watermarks": watermarks()}


def verify(label):
    """Gold must hold exactly the distinct trades of Bronze, and the aggregate must add up to the fact."""
    oracle = (spark.table(T["bronze_trades"]).dropDuplicates(["symbol", "trade_id"])
              .agg(F.count("*").alias("rows"), F.sum(F.col("qty") * F.col("price")).alias("notional")).first())
    s = state()
    check(s["silver_rows"] == oracle["rows"], f"{label}: silver rows {s['silver_rows']} != distinct bronze {oracle['rows']}")
    check(s["fact_rows"] == oracle["rows"], f"{label}: fact rows {s['fact_rows']} != distinct bronze {oracle['rows']}")
    check(abs(s["fact_notional"] - oracle["notional"]) <= 1e-8 * oracle["notional"], f"{label}: fact notional differs from Bronze")
    check(s["mlv_trades"] == oracle["rows"], f"{label}: aggregate trade_count {s['mlv_trades']} != {oracle['rows']}")
    check(abs(s["mlv_notional"] - oracle["notional"]) <= 1e-8 * oracle["notional"], f"{label}: aggregate notional differs from Bronze")
    return s


def scale_run(size):
    reset()
    land(0, size)
    run = run_pipeline()
    check(run["status"] == "Completed", f"size {size}: {run}")
    state_after = verify(f"size {size}")
    run["rows"] = size
    run["rows_per_s_pipeline"] = round(size / run["seconds"], 1)
    for activity, label in [("Run nb_load_silver", "silver"), ("Run nb_load_gold", "gold")]:
        if activity in run["activities_s"]:
            run[f"rows_per_s_{label}"] = round(size / run["activities_s"][activity], 1)
    run["state"] = state_after
    result["scale"][str(size)] = run
    save()
    return run


MIN_EXTRA_S = 10  # a time difference below this is run-to-run noise, not a rate


def marginal():
    """Rows per second between two sizes, which leaves out the fixed cost of starting each Spark session."""
    rows = sorted(result["scale"].values(), key=lambda r: r["rows"])
    out = {}
    for small, big in zip(rows, rows[1:]):
        entry = {"extra_rows": big["rows"] - small["rows"]}
        for activity, label in [("Run nb_load_silver", "silver"), ("Run nb_load_gold", "gold")]:
            extra = big["activities_s"].get(activity, 0) - small["activities_s"].get(activity, 0)
            entry[f"extra_s_{label}"] = round(extra, 1)
            entry[f"rows_per_s_{label}"] = round(entry["extra_rows"] / extra, 1) if extra >= MIN_EXTRA_S else None
        extra = big["seconds"] - small["seconds"]
        entry["extra_s_pipeline"] = round(extra, 1)
        entry["rows_per_s_pipeline"] = round(entry["extra_rows"] / extra, 1) if extra >= MIN_EXTRA_S else None
        out[f"{small['rows']}_to_{big['rows']}"] = entry
    return out


failures = {}
for size in SIZES:
    try:
        scale_run(size)
    except Exception:
        failures[f"scale_{size}"] = traceback.format_exc()
        result["failures"] = failures
        save()

result["marginal_rows_per_s"] = marginal() if len(result["scale"]) > 1 else {}
result["failures"] = failures
save()
if not failures:
    drop_all()  # on a failure the tables stay for diagnosis
print(json.dumps(result, indent=2, default=str)[:6000])
if failures:
    raise RuntimeError("benchmark steps failed: " + ", ".join(failures))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
