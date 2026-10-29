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

library_only = True  # nb_backfill_trades then defines its functions and skips its backfill

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_backfill_trades

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Tests of nb_backfill_trades (issue #11), run in Fabric on scratch tables in schema `test` (prefix bf_);
# the real tables are never touched. %run also runs the parameters cell of nb_backfill_trades, so the
# names symbol, start, end and bronze_table are taken here and not used.
#   1 ws_with_gap          WS rows 113261941 and 113261944 are loaded, pl_process_gold runs  -> Gold has 2 trades, 2 are missing
#   2 backfill_missed      backfill() pages a fixture exchange (3 rows per page, `since` inclusive) from before
#                          the first trade, pl_process_gold runs  -> Gold gains exactly 942 and 943, the two
#                          loaded trades are untouched, the trade_id range is contiguous, aggregates match the oracle
#   3 backfill_loaded      the same backfill again, pl_process_gold runs  -> Silver, Gold and the aggregate are unchanged
#   4 rest_to_end_of_data  pl_backfill_bronze_trades against the real Kraken REST for BTC/USD, last 3 minutes,
#                          no end -> rows land in a scratch Bronze table, contiguous, path `backfill`
#   5 rest_with_end        the same with an end time -> no trade at or after it
# Fixtures (1 to 3) are real Kraken trades (REST /0/public/Trades, 2026-10-28) in the REST row shape; the
# expected rows come from a plain-Python oracle, not from the code under test.
# Results go to Files/checks/nb_test_backfill_trades.json; the run fails if a step fails.

import json
import time
import traceback
from datetime import datetime, timedelta, timezone

import requests

RESULT_PATH = "Files/checks/nb_test_backfill_trades.json"
T = {name: f"test.bf_{name}" for name in
     ["bronze_trades", "silver_trades", "quarantine_trades", "etl_watermark", "fact_trade", "dim_pair", "dim_date", "agg_pair_daily",
      "dq_result", "reconcile_daily"]}
SMOKE_BRONZE = ["test.bf_smoke_bronze_open", "test.bf_smoke_bronze_ended"]
BRONZE_DDL = ("symbol STRING, side STRING, price DOUBLE, qty DOUBLE, ord_type STRING, "
              "trade_id BIGINT, timestamp STRING, _ingest_path STRING, _load_ts STRING")

# REST rows [price, volume, time, side, ord_type, misc, trade_id] of BTC/USD, and the Bronze row each must become.
# Epoch seconds checked with `date -u -d 2026-10-28T17:49:39 +%s`.
REST = {
    113261941: ["80691.60000", "0.01228705", 1793209779.713645, "s", "l", "", 113261941],
    113261942: ["80691.70000", "0.00261820", 1793209780.718137, "b", "m", "", 113261942],
    113261943: ["80691.70000", "0.05947431", 1793209780.718137, "b", "m", "", 113261943],
    113261944: ["80691.70000", "0.00018589", 1793209783.292547, "b", "m", "", 113261944],
}
EXPECTED_BRONZE = {  # trade_id -> (side, price, qty, ord_type, timestamp)
    113261941: ("sell", 80691.6, 0.01228705, "limit", "2026-10-28T17:49:39.713645Z"),
    113261942: ("buy", 80691.7, 0.00261820, "market", "2026-10-28T17:49:40.718137Z"),
    113261943: ("buy", 80691.7, 0.05947431, "market", "2026-10-28T17:49:40.718137Z"),
    113261944: ("buy", 80691.7, 0.00018589, "market", "2026-10-28T17:49:43.292547Z"),
}
ALL_IDS = sorted(EXPECTED_BRONZE)
WS_IDS = [113261941, 113261944]
FIXTURE_START = parse_utc("2026-10-28T17:00:00Z")


def ws_row(trade_id):
    side, price, qty, ord_type, ts = EXPECTED_BRONZE[trade_id]
    return {"symbol": "BTC/USD", "side": side, "price": price, "qty": qty, "ord_type": ord_type, "trade_id": trade_id,
            "timestamp": ts, "_ingest_path": "ws", "_load_ts": "2026-10-28T18:00:00.000000Z"}


def expected_agg(ids):
    trades = sorted((EXPECTED_BRONZE[i] + (i,) for i in ids), key=lambda t: (t[4], t[5]))
    notional = sum(t[1] * t[2] for t in trades)
    volume = sum(t[2] for t in trades)
    return {"open": trades[0][1], "high": max(t[1] for t in trades), "low": min(t[1] for t in trades),
            "close": trades[-1][1], "volume": volume, "notional": notional, "vwap": notional / volume,
            "trade_count": len(trades), "taker_buy_notional": sum(t[1] * t[2] for t in trades if t[0] == "buy")}


class FixtureExchange:
    """/0/public/Trades as observed on 2026-10-29: trades at or after `since` (inclusive), `page` rows at most,
    `last` = the ns time of the last row, `since` echoed when nothing is returned."""

    def __init__(self, page):
        self.page, self.calls = page, 0

    @staticmethod
    def ns(row):
        return int(round(row[2] * 10**9))

    def fetch(self, since_ns):
        self.calls += 1
        rows = [REST[i] for i in ALL_IDS if self.ns(REST[i]) >= since_ns][: self.page]
        return rows, (self.ns(rows[-1]) if rows else since_ns)


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def check_equal(actual, expected, what):
    if actual != expected:
        raise AssertionError(f"{what}: expected {expected!r}, got {actual!r}")


def rows_of(table, order_by):
    return [r.asDict() for r in spark.table(table).orderBy(*order_by).collect()]


def snapshot():
    return {"silver": rows_of(T["silver_trades"], ["symbol", "trade_id"]),
            "fact": rows_of(T["fact_trade"], ["symbol", "trade_id"]),
            "dim_pair": rows_of(T["dim_pair"], ["symbol"]),
            "dim_date": rows_of(T["dim_date"], ["date"]),
            "agg": rows_of(T["agg_pair_daily"], ["symbol", "trade_date"])}


def fact_ids():
    return sorted(r["trade_id"] for r in spark.table(T["fact_trade"]).collect())


def check_contiguous(ids, what):
    check_equal(max(ids) - min(ids) + 1, len(ids), f"{what}: max - min + 1 versus count")


def check_agg(ids, what):
    rows = spark.table(T["agg_pair_daily"]).collect()
    check_equal(len(rows), 1, f"{what}: aggregate rows (one pair, one day)")
    got = rows[0].asDict()
    for column, value in expected_agg(ids).items():
        check(abs(got[column] - value) <= 1e-9 * max(1.0, abs(value)), f"{what} {column}: expected {value!r}, got {got[column]!r}")


def reset():
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql("CREATE SCHEMA IF NOT EXISTS test")
    spark.sql(f"DROP MATERIALIZED LAKE VIEW IF EXISTS {T['agg_pair_daily']}")
    for name, table in T.items():
        if name != "agg_pair_daily":
            spark.sql(f"DROP TABLE IF EXISTS {table}")
    create_bronze(T["bronze_trades"])
    # The quality gate of pl_process_gold needs every pair of the batch in dim_pair; in DEV nb_build_dim_pair has put them there.
    spark.sql(f"CREATE TABLE {T['dim_pair']} (symbol STRING, base STRING, quote STRING, status STRING, is_tracked BOOLEAN) USING DELTA")
    spark.sql(f"INSERT INTO {T['dim_pair']} VALUES ('BTC/USD', 'BTC', 'USD', 'online', true)")


def create_bronze(table):
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {table} ({BRONZE_DDL}) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)")


# Fabric job API, as the notebook's own identity. The token is not printed.
API = "https://api.fabric.microsoft.com/v1"
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]


def api_headers():
    return {"Authorization": f"Bearer {notebookutils.credentials.getToken('pbi')}", "x-ms-fabric-skill": "spark-cli"}


def pipeline_id(name):
    items = requests.get(f"{API}/workspaces/{WORKSPACE_ID}/items?type=DataPipeline", headers=api_headers()).json()["value"]
    matches = [i["id"] for i in items if i["displayName"] == name]
    check_equal(len(matches), 1, f"items named {name}")
    return matches[0]


def run_pipeline(name, parameters, timeout_s=1500):
    response = requests.post(f"{API}/workspaces/{WORKSPACE_ID}/items/{pipeline_id(name)}/jobs/instances?jobType=Pipeline",
                             headers=api_headers(), json={"executionData": {"parameters": parameters}})
    check_equal(response.status_code, 202, f"start {name}")
    location = response.headers["Location"]
    started = time.time()
    while time.time() - started < timeout_s:
        time.sleep(20)
        job = requests.get(location, headers=api_headers()).json()
        if job["status"] in ("Completed", "Failed", "Cancelled", "Deduped"):
            check_equal(job["status"], "Completed", f"{name} status")
            return {"id": job["id"], "seconds": round(time.time() - started)}
    raise TimeoutError(f"{name} run {location} did not finish in {timeout_s} s")


def process_gold():
    return run_pipeline("pl_process_gold", {
        "bronze_table": T["bronze_trades"], "silver_table": T["silver_trades"], "quarantine_table": T["quarantine_trades"],
        "fact_table": T["fact_trade"], "dim_pair_table": T["dim_pair"], "dim_date_table": T["dim_date"],
        "watermark_table": T["etl_watermark"], "mlv_table": T["agg_pair_daily"], "fail_gold_after_write": False,
        "dq_result_table": T["dq_result"], "reconcile_table": T["reconcile_daily"], "send_dq_alerts": False})  # alerts off: a failing gate would mail the operator


def run_backfill_fixture():
    return backfill(FixtureExchange(page=3).fetch, "BTC/USD", to_ns(FIXTURE_START), None, T["bronze_trades"])


evidence = {}
state = {}


def step_1_ws_with_gap():
    reset()
    spark.createDataFrame([ws_row(i) for i in WS_IDS], BRONZE_DDL).write.format("delta").mode("append").saveAsTable(T["bronze_trades"])
    evidence["process_gold_1"] = process_gold()
    check_equal(fact_ids(), WS_IDS, "fact trade_ids before the backfill")
    silver_ids = sorted(r["trade_id"] for r in spark.table(T["silver_trades"]).collect())
    check_equal(max(silver_ids) - min(silver_ids) + 1 - len(silver_ids), 2, "missing trade_ids before the backfill")
    state["before_backfill"] = snapshot()


def step_2_backfill_missed():
    result = run_backfill_fixture()
    check_equal((result["pages"], result["trades"], result["first_trade_id"], result["last_trade_id"]),
                (2, 4, ALL_IDS[0], ALL_IDS[-1]), "backfill result (pages, trades, first, last)")
    # What landed in Bronze is the REST row converted: compare with the expected Bronze rows.
    landed = {r["trade_id"]: r for r in spark.table(T["bronze_trades"]).where("_ingest_path = 'backfill'").collect()}
    check_equal(sorted(landed), ALL_IDS, "backfilled trade_ids")
    for trade_id, (side, price, qty, ord_type, ts) in EXPECTED_BRONZE.items():
        r = landed[trade_id]
        check_equal((r["symbol"], r["side"], r["price"], r["qty"], r["ord_type"], r["timestamp"]),
                    ("BTC/USD", side, price, qty, ord_type, ts), f"bronze row {trade_id}")
    evidence["process_gold_2"] = process_gold()
    check_equal(fact_ids(), ALL_IDS, "fact trade_ids after the backfill")
    check_contiguous(fact_ids(), "fact after the backfill")
    check_agg(ALL_IDS, "aggregate after the backfill")
    after = snapshot()
    before = state["before_backfill"]
    for table in ("silver", "fact"):
        kept = [r for r in after[table] if r["trade_id"] in WS_IDS]
        check_equal(kept, before[table], f"{table} rows of the trades that were already loaded")
        added = sorted(r["trade_id"] for r in after[table] if r["trade_id"] not in WS_IDS)
        check_equal(added, [113261942, 113261943], f"{table} rows added by the backfill")
    check_equal(after["dim_pair"], before["dim_pair"], "dim_pair")
    check_equal(after["dim_date"], before["dim_date"], "dim_date")
    state["after_backfill"] = after


def step_3_backfill_loaded():
    run_backfill_fixture()
    evidence["process_gold_3"] = process_gold()
    check_equal(snapshot(), state["after_backfill"], "silver, fact, dims and aggregate after backfilling loaded data")
    # The second backfill did land its rows in Bronze (4 more): Silver dropped them.
    check_equal(spark.table(T["bronze_trades"]).where("_ingest_path = 'backfill'").count(), 8, "backfill rows in bronze")


def smoke(table, start_at, end_at):
    create_bronze(table)
    run = run_pipeline("pl_backfill_bronze_trades", {
        "symbol": "BTC/USD", "start": start_at.isoformat(), "end": end_at.isoformat() if end_at else "", "bronze_table": table},
        timeout_s=900)
    rows = spark.table(table).orderBy("trade_id").collect()
    check(len(rows) > 0, f"no trades landed in {table}")
    check_equal({r["_ingest_path"] for r in rows}, {"backfill"}, "ingest path")
    check_equal({r["symbol"] for r in rows}, {"BTC/USD"}, "symbol")
    ids = [r["trade_id"] for r in rows]
    check_equal(len(set(ids)), len(ids), "a trade_id landed twice in one backfill")
    check_contiguous(ids, f"trade_ids in {table}")  # observed, not documented (reference/lessons.md)
    stamps = [r["timestamp"] for r in rows]
    check(min(stamps) >= (start_at - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S"), f"trade before start: {min(stamps)}")
    return run, rows


def step_4_rest_to_end_of_data():
    started = datetime.now(timezone.utc)
    run, rows = smoke(SMOKE_BRONZE[0], started - timedelta(minutes=3), None)
    evidence["rest_to_end_of_data_rows"] = len(rows)
    check(rows[-1]["timestamp"] >= started.strftime("%Y-%m-%dT%H:%M:%S"), "the run did not reach the newest trade")
    return run


def step_5_rest_with_end():
    now = datetime.now(timezone.utc)
    end_at = now - timedelta(minutes=2)
    run, rows = smoke(SMOKE_BRONZE[1], now - timedelta(minutes=5), end_at)
    evidence["rest_with_end_rows"] = len(rows)
    check(max(r["timestamp"] for r in rows) < end_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), "a trade at or after the end time")
    return run


def step(name, fn):
    try:
        evidence[name] = fn() or "passed"
        return True
    except Exception:
        evidence[name] = "FAILED: " + traceback.format_exc()
        return False


for name, fn in [("1_ws_with_gap", step_1_ws_with_gap), ("2_backfill_missed", step_2_backfill_missed),
                 ("3_backfill_loaded", step_3_backfill_loaded)]:
    if not step(name, fn):
        break  # these steps share state: after a failure the next one proves nothing
for name, fn in [("4_rest_to_end_of_data", step_4_rest_to_end_of_data), ("5_rest_with_end", step_5_rest_with_end)]:
    step(name, fn)

failed = {n: r for n, r in evidence.items() if isinstance(r, str) and r.startswith("FAILED")}
summary = {"passed": sum(1 for n in evidence if n[0].isdigit() and n not in failed), "failed": len(failed), "steps": evidence}
notebookutils.fs.put(RESULT_PATH, json.dumps(summary, indent=2, default=str), True)
print(json.dumps(summary, indent=2, default=str))
if failed:
    raise RuntimeError(f"backfill tests failed: {sorted(failed)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
