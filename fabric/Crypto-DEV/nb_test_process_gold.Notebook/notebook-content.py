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

# Seam 2 test of pl_process_gold (issue #10), run in Crypto-DEV: a fixture is loaded into a scratch
# Bronze table (schema `test`, prefix it_) and the real pipeline runs on it, through the Fabric job
# API with the table names as pipeline parameters. The real tables are never touched.
#   run 1  fixture batch                                  -> Gold matches the expected rows
#   run 2  overlapping batch (resends, new trades, an untracked pair) -> still matches, nothing doubled
#   run 3  new batch with the Gold failure forced         -> pipeline fails, Gold watermark unchanged
#   run 4  same pipeline without the failure              -> reads the same range, Gold matches
#   run 5  a duplicate key written into Silver (issue #12) -> the quality gate fails the pipeline, Gold and
#                                                            its watermark stay, an error row lands in
#                                                            ops.dq_result and the alert event is sent
#   run 6  the duplicate removed, same pipeline            -> passes, Gold matches, the watermark moves
# Runs 1 to 4 also show that warn-level failures (stale fixtures, trade_id gaps) are recorded and do not
# block Gold. Fixtures are real Kraken trades (REST /0/public/Trades, 2026-10-28) in the Bronze shape the
# collector sends. Expected rows come from a plain-Python oracle, not from the code under test.
# Results go to Files/checks/nb_test_process_gold.json; the run fails if a step fails.

import json
import time
import traceback

import requests
from pyspark.sql import functions as F

RESULT_PATH = "Files/checks/nb_test_process_gold.json"
PIPELINE = "pl_process_gold"
T = {name: f"test.it_{name}" for name in
     ["bronze_trades", "silver_trades", "quarantine_trades", "etl_watermark", "fact_trade", "dim_pair", "dim_date", "agg_pair_daily",
      "dq_result"]}
BRONZE_SCHEMA = ("symbol STRING, side STRING, price DOUBLE, qty DOUBLE, ord_type STRING, "
                 "trade_id BIGINT, timestamp STRING, _ingest_path STRING, _load_ts STRING")

# (symbol, trade_id) -> (side, price, qty, ord_type, timestamp)
REAL = {
    ("BTC/USD", 113191288): ("buy", 80800.9, 0.00040314, "limit", "2026-10-27T23:59:59.047657Z"),
    ("BTC/USD", 113191289): ("buy", 80800.9, 0.00542817, "market", "2026-10-27T23:59:59.528693Z"),
    ("BTC/USD", 113191290): ("buy", 80800.9, 0.00012284, "limit", "2026-10-28T00:00:00.209477Z"),
    ("BTC/USD", 113191291): ("buy", 80800.9, 0.00094992, "limit", "2026-10-28T00:00:00.229830Z"),
    ("BTC/USD", 113261941): ("sell", 80691.6, 0.01228705, "limit", "2026-10-28T17:49:39.713645Z"),
    ("BTC/USD", 113261942): ("buy", 80691.7, 0.00261820, "market", "2026-10-28T17:49:40.718137Z"),
    ("BTC/USD", 113261943): ("buy", 80691.7, 0.05947431, "market", "2026-10-28T17:49:40.718137Z"),
    ("BTC/USD", 113261944): ("buy", 80691.7, 0.00018589, "market", "2026-10-28T17:49:43.292547Z"),
    ("ETH/USD", 68421366): ("sell", 2341.65, 2.26825323, "limit", "2026-10-28T17:49:38.309843Z"),
    ("ETH/USD", 68421367): ("sell", 2341.68, 0.20498103, "limit", "2026-10-28T17:49:38.321315Z"),
}
KEYS = list(REAL)
DAY27 = KEYS[0:2]      # BTC/USD, the last trades before midnight UTC
MIDNIGHT = KEYS[2:4]   # BTC/USD, the first trades after midnight UTC
EVENING = KEYS[4:8]    # BTC/USD, 17:49 UTC; the last two share a timestamp
ETH = KEYS[8:10]       # ETH/USD, 17:49 UTC


def trade(key, path="ws", **override):
    symbol, trade_id = key
    side, price, qty, ord_type, ts = REAL[key]
    row = {"symbol": symbol, "side": side, "price": price, "qty": qty, "ord_type": ord_type, "trade_id": trade_id,
           "timestamp": ts, "_ingest_path": path, "_load_ts": "2026-10-28T18:00:00.000000Z"}
    row.update(override)
    return row


# Four Bronze batches, one per pipeline run (each is one Bronze commit).
BATCH_1_KEYS = DAY27 + MIDNIGHT[:1] + EVENING[:2] + ETH[:1]
BATCH_1 = [trade(k) for k in BATCH_1_KEYS] + [trade(EVENING[1])]                # one trade sent twice (snapshot after a reconnect)
BATCH_2 = ([trade(k) for k in MIDNIGHT[:1] + EVENING[:2]]                        # overlap with batch 1
           + [trade(k, path="backfill") for k in MIDNIGHT[1:] + EVENING[2:]]     # new BTC trades, from a backfill
           + [trade(MIDNIGHT[0], symbol="XYZ/USD", trade_id=900, price=0.2, qty=10.0)])  # a pair not in vl_crypto: not in Silver or Gold
BATCH_3 = [trade(ETH[1]), trade(EVENING[3])]                                    # a new ETH trade and a resend
EXPECTED_AFTER = {1: BATCH_1_KEYS,
                  2: DAY27 + MIDNIGHT + EVENING + ETH[:1],
                  3: KEYS, 4: KEYS}


def expected_agg(keys):
    by_day = {}
    for key in keys:
        t = trade(key)
        by_day.setdefault((t["symbol"], t["timestamp"][:10]), []).append(t)
    out = {}
    for day, day_trades in by_day.items():
        day_trades.sort(key=lambda t: (t["timestamp"], t["trade_id"]))
        notional = sum(t["price"] * t["qty"] for t in day_trades)
        volume = sum(t["qty"] for t in day_trades)
        out[day] = {"open": day_trades[0]["price"], "high": max(t["price"] for t in day_trades),
                    "low": min(t["price"] for t in day_trades), "close": day_trades[-1]["price"],
                    "volume": volume, "notional": notional, "vwap": notional / volume,
                    "trade_count": len(day_trades),
                    "taker_buy_notional": sum(t["price"] * t["qty"] for t in day_trades if t["side"] == "buy")}
    return out


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def check_equal(actual, expected, what):
    if actual != expected:
        raise AssertionError(f"{what}: expected {expected!r}, got {actual!r}")


def check_gold_matches(run):
    keys = EXPECTED_AFTER[run]
    check_equal(sorted((r["symbol"], r["trade_id"]) for r in spark.table(T["fact_trade"]).collect()), sorted(keys), f"run {run} fact keys")
    agg_rows = spark.table(T["agg_pair_daily"]).collect()
    actual = {(r["symbol"], str(r["trade_date"])): r.asDict() for r in agg_rows}
    expected = expected_agg(keys)
    check_equal(len(agg_rows), len(actual), f"run {run} aggregate rows per (symbol, trade_date)")  # the key holds
    check_equal(sorted(actual), sorted(expected), f"run {run} aggregate days")
    for day, want in expected.items():
        for column, value in want.items():
            got = actual[day][column]
            check(abs(got - value) <= 1e-9 * max(1.0, abs(value)), f"run {run} {day} {column}: expected {value!r}, got {got!r}")
    check_equal(sorted(r["symbol"] for r in spark.table(T["dim_pair"]).collect()), ["BTC/USD", "ETH/USD"], f"run {run} dim_pair")
    check_equal(sorted(str(r["date"]) for r in spark.table(T["dim_date"]).collect()), ["2026-10-27", "2026-10-28"], f"run {run} dim_date")
    check_equal(spark.table(T["quarantine_trades"]).count(), 0, f"run {run} quarantine")


def table_version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def watermarks():
    rows = {r["source_table"]: r["last_version"] for r in spark.table(T["etl_watermark"]).collect()}
    return rows.get(T["bronze_trades"]), rows.get(T["silver_trades"])


def reset():
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql("CREATE SCHEMA IF NOT EXISTS test")
    spark.sql(f"DROP MATERIALIZED LAKE VIEW IF EXISTS {T['agg_pair_daily']}")
    for name, table in T.items():
        if name != "agg_pair_daily":
            spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {T['bronze_trades']} ({BRONZE_SCHEMA}) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    # The quality gate needs every pair of the batch in dim_pair; in DEV nb_build_dim_pair has put them there.
    spark.sql(f"CREATE TABLE {T['dim_pair']} (symbol STRING, base STRING, quote STRING, status STRING, is_tracked BOOLEAN) USING DELTA")
    spark.sql(f"INSERT INTO {T['dim_pair']} VALUES ('BTC/USD', 'BTC', 'USD', 'online', true), ('ETH/USD', 'ETH', 'USD', 'online', true)")


def land(rows):
    spark.createDataFrame(rows, BRONZE_SCHEMA).write.format("delta").mode("append").saveAsTable(T["bronze_trades"])


# Fabric job API, as the notebook's own identity. The token is not printed.
API = "https://api.fabric.microsoft.com/v1"
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]


def api_headers():
    return {"Authorization": f"Bearer {notebookutils.credentials.getToken('pbi')}", "x-ms-fabric-skill": "spark-cli"}


def pipeline_id():
    items = requests.get(f"{API}/workspaces/{WORKSPACE_ID}/items?type=DataPipeline", headers=api_headers()).json()["value"]
    matches = [i["id"] for i in items if i["displayName"] == PIPELINE]
    check_equal(len(matches), 1, f"items named {PIPELINE}")
    return matches[0]


def run_pipeline(fail_gold_after_write=False, send_dq_alerts=False, timeout_s=1200):
    # Alerts stay off except in the run that proves the alert is sent: every error-level failure would mail the operator.
    parameters = {"bronze_table": T["bronze_trades"], "silver_table": T["silver_trades"],
                  "quarantine_table": T["quarantine_trades"], "fact_table": T["fact_trade"],
                  "dim_pair_table": T["dim_pair"], "dim_date_table": T["dim_date"],
                  "watermark_table": T["etl_watermark"], "mlv_table": T["agg_pair_daily"],
                  "dq_result_table": T["dq_result"], "send_dq_alerts": send_dq_alerts,
                  "fail_gold_after_write": fail_gold_after_write}
    response = requests.post(f"{API}/workspaces/{WORKSPACE_ID}/items/{pipeline_id()}/jobs/instances?jobType=Pipeline",
                             headers=api_headers(), json={"executionData": {"parameters": parameters}})
    check_equal(response.status_code, 202, "start pipeline")
    location = response.headers["Location"]
    started = time.time()
    while time.time() - started < timeout_s:
        time.sleep(20)
        job = requests.get(location, headers=api_headers()).json()
        if job["status"] in ("Completed", "Failed", "Cancelled", "Deduped"):
            return {"id": job["id"], "status": job["status"], "seconds": round(time.time() - started)}
    raise TimeoutError(f"pipeline run {location} did not finish in {timeout_s} s")


evidence = {}


def step(name, fn):
    try:
        evidence[name] = fn() or "passed"
        return True
    except Exception:
        evidence[name] = "FAILED: " + traceback.format_exc()
        return False


def run_1():
    reset()
    land(BATCH_1)
    run = run_pipeline()
    check_equal(run["status"], "Completed", "run 1 status")
    check_gold_matches(1)
    wm_bronze, wm_silver = watermarks()
    check_equal(wm_bronze, table_version(T["bronze_trades"]), "run 1 bronze watermark")
    check_equal(wm_silver, table_version(T["silver_trades"]), "run 1 silver watermark")
    evidence["watermarks_after_run_1"] = {"bronze": wm_bronze, "silver": wm_silver}
    # The fixtures are a day old and have trade_id gaps: both are warns, recorded, and Gold was loaded anyway.
    results = {r["check_id"]: r for r in spark.table(T["dq_result"]).collect()}
    check_equal(sorted(results), ["freshness", "key_unique", "ref_integrity_dim_pair", "row_count", "schema", "trade_id_contiguity"],
                "run 1 recorded checks")
    check_equal(sorted(c for c, r in results.items() if not r["passed"]), ["freshness", "trade_id_contiguity"], "run 1 failed checks")
    check_equal({r["severity"] for r in results.values() if not r["passed"]}, {"warn"}, "run 1 failed severities")
    return run


def run_2():
    before = watermarks()
    land(BATCH_2)
    run = run_pipeline()
    check_equal(run["status"], "Completed", "run 2 status")
    check_gold_matches(2)
    after = watermarks()
    check(after[0] > before[0] and after[1] > before[1], f"watermarks should advance: {before} -> {after}")
    check_equal(after, (table_version(T["bronze_trades"]), table_version(T["silver_trades"])), "run 2 watermarks at the tables' versions")
    evidence["watermarks_after_run_2"] = {"bronze": after[0], "silver": after[1]}
    return run


def run_3_forced_gold_failure():
    before = watermarks()
    aggregate_before = [r.asDict() for r in spark.table(T["agg_pair_daily"]).orderBy("symbol", "trade_date").collect()]
    land(BATCH_3)
    run = run_pipeline(fail_gold_after_write=True)
    check_equal(run["status"], "Failed", "run 3 status")
    # The refresh comes after Gold, so a failed Gold step must leave the view as it was.
    aggregate_after = [r.asDict() for r in spark.table(T["agg_pair_daily"]).orderBy("symbol", "trade_date").collect()]
    check_equal(aggregate_after, aggregate_before, "agg_pair_daily after the failed run (refresh skipped)")
    after = watermarks()
    # Silver completed before Gold was told to fail, so its watermark moved; Gold's did not.
    check_equal(after[1], before[1], "gold watermark after the forced failure")
    check_equal(after[0], table_version(T["bronze_trades"]), "bronze watermark after the failed run (silver step succeeded)")
    evidence["watermarks_after_run_3"] = {"bronze": after[0], "silver": after[1]}
    return run


def run_4_rerun_reads_the_same_range():
    before = watermarks()
    run = run_pipeline()
    check_equal(run["status"], "Completed", "run 4 status")
    check_gold_matches(4)
    after = watermarks()
    check_equal(after[0], before[0], "nothing new in bronze: bronze watermark stays")
    check_equal(after[1], table_version(T["silver_trades"]), "gold watermark after the rerun")
    evidence["watermarks_after_run_4"] = {"bronze": after[0], "silver": after[1]}
    return run


def gate_summary():
    # The gate writes its summary before it raises, so the file describes the run that just failed.
    with open("/lakehouse/default/Files/checks/nb_check_silver_batch.json") as f:
        return json.load(f)


def run_5_duplicate_key_blocks_gold():
    before = watermarks()
    fact_version_before = table_version(T["fact_trade"])
    fact_before = sorted((r["symbol"], r["trade_id"]) for r in spark.table(T["fact_trade"]).collect())
    aggregate_before = [r.asDict() for r in spark.table(T["agg_pair_daily"]).orderBy("symbol", "trade_date").collect()]
    # A copy of a trade Gold already holds, written straight into Silver as a faulty Silver load would.
    spark.sql(f"""INSERT INTO {T['silver_trades']}
                  SELECT symbol, trade_id, side, price, qty, ord_type, trade_ts, _ingest_path, _load_ts, 999999
                  FROM {T['silver_trades']} WHERE symbol = 'BTC/USD' AND trade_id = {KEYS[4][1]}""")
    run = run_pipeline(send_dq_alerts=True)
    check_equal(run["status"], "Failed", "run 5 status")
    check_equal(table_version(T["fact_trade"]), fact_version_before, "fact_trade must not be written")
    check_equal(sorted((r["symbol"], r["trade_id"]) for r in spark.table(T["fact_trade"]).collect()), fact_before, "fact keys after the blocked run")
    check_equal([r.asDict() for r in spark.table(T["agg_pair_daily"]).orderBy("symbol", "trade_date").collect()], aggregate_before,
                "agg_pair_daily after the blocked run")
    after = watermarks()
    check_equal(after, before, "both watermarks after the blocked run (nothing new in Bronze, Gold held)")
    errors = spark.table(T["dq_result"]).where("severity = 'error' AND NOT passed").collect()
    check_equal([r["check_id"] for r in errors], ["key_unique"], "failed error-level checks")
    summary = gate_summary()
    check_equal(summary["errors"], ["key_unique"], "gate summary errors")
    check_equal(summary["alert"], "sent", "alert event to es_dq_alerts")
    evidence["watermarks_after_run_5"] = {"bronze": after[0], "silver": after[1]}
    evidence["dq_error_row_run_5"] = {"check_id": errors[0]["check_id"], "unexpected_count": errors[0]["unexpected_count"]}
    return run


def run_6_after_the_fix():
    before = watermarks()
    spark.sql(f"DELETE FROM {T['silver_trades']} WHERE _bronze_version = 999999")
    run = run_pipeline()
    check_equal(run["status"], "Completed", "run 6 status")
    check_gold_matches(4)  # the same trades as before: the faulty copy never reached Gold
    after = watermarks()
    check_equal(after[1], table_version(T["silver_trades"]), "gold watermark after the fix")
    check(after[1] > before[1], f"the Gold watermark should move: {before} -> {after}")
    evidence["watermarks_after_run_6"] = {"bronze": after[0], "silver": after[1]}
    return run


for name, fn in [("run_1_fixture", run_1), ("run_2_overlapping_batch", run_2),
                 ("run_3_forced_gold_failure_keeps_gold_watermark", run_3_forced_gold_failure),
                 ("run_4_rerun_reads_the_same_range", run_4_rerun_reads_the_same_range),
                 ("run_5_duplicate_key_blocks_gold", run_5_duplicate_key_blocks_gold),
                 ("run_6_after_the_fix", run_6_after_the_fix)]:
    if not step(name, fn):
        break  # the steps share state: after a failure the next one proves nothing

failed = {n: r for n, r in evidence.items() if isinstance(r, str) and r.startswith("FAILED")}
summary = {"passed": sum(1 for name, r in evidence.items() if name.startswith("run_") and r not in failed.values()),
           "failed": len(failed), "steps": evidence}
notebookutils.fs.put(RESULT_PATH, json.dumps(summary, indent=2), True)
print(json.dumps(summary, indent=2))
if failed:
    raise RuntimeError(f"seam 2 failed: {sorted(failed)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
