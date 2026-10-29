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

# Benchmark of Bronze layout choices (issue #16), run in Crypto-DEV on a replicated copy: real Bronze holds
# only hours of data, so rows in the Bronze shape are generated up to DAYS days at ROWS_PER_DAY, with the pair mix,
# median prices and median quantities of the live rows in bronze.trades. Everything lands in scratch tables
# `test.bm_*`; the real tables are only read (DESCRIBE DETAIL, a profile of the live rows). The numbers are
# synthetic volume on real shape: say so wherever they are quoted.
#   1. layouts      flat, partitioned by day, partitioned by pair: files, size, four typical queries
#   2. small files  files of 500 to 500,000 rows (what the Eventstream setting "minimum rows" controls),
#                   queries before and after OPTIMIZE
#   3. joins        trades to dim_pair and dim_date, broadcast against sort-merge, with the shuffle of each
# Query time is wall time of collect(), RUNS runs each. Shuffle and file figures come from the executed plan's
# metrics (the Spark UI numbers), read after each run. `test.bm_src` is kept: nb_benchmark_process_gold slices it.
# Results go to Files/checks/nb_benchmark_bronze.json.

import json
import math
import time
import traceback
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import functions as F

RESULT_PATH = "Files/checks/nb_benchmark_bronze.json"
DAYS = 28
ROWS_PER_DAY = 350_000        # about what the 20 tracked pairs trade on Kraken in a day (trades-pipeline.md)
START = "2026-09-01"
SUBSET_DAYS = 7               # the small-files experiment writes thousands of files: one week is enough
FILE_ROWS = [500, 5_000, 50_000, 500_000]
RUNS = 3
SRC = "test.bm_src"
BRONZE_COLUMNS = ["symbol", "side", "price", "qty", "ord_type", "trade_id", "timestamp", "_ingest_path", "_load_ts"]

result = {"settings": {"days": DAYS, "rows_per_day": ROWS_PER_DAY, "runs": RUNS, "start": START}, "failures": []}


def save():
    notebookutils.fs.put(RESULT_PATH, json.dumps(result, indent=2, default=str), True)


def children(node):
    cls = node.getClass().getName()
    if cls.endswith("AdaptiveSparkPlanExec"):
        return [node.executedPlan()]
    if "QueryStageExec" in cls:
        return [node.plan()]
    found = []
    it = node.children().iterator()
    while it.hasNext():
        found.append(it.next())
    return found


def plan_stats(df):
    """Join strategy, shuffle, broadcast and scan figures of a collected DataFrame's final plan."""
    nodes = []

    def walk(node):
        metrics = {}
        it = node.metrics().iterator()
        while it.hasNext():
            pair = it.next()
            metrics[pair._1()] = pair._2().value()
        nodes.append((node.getClass().getSimpleName(), metrics))
        for child in children(node):
            walk(child)

    walk(df._jdf.queryExecution().executedPlan())
    scan = {}
    for name, metrics in nodes:
        if "Scan" in name:
            for key, value in metrics.items():
                scan[key] = scan.get(key, 0) + value
    exchanges = [m for name, m in nodes if name == "ShuffleExchangeExec"]
    return {"joins": [name for name, _ in nodes if "Join" in name],
            "shuffle_exchanges": len(exchanges),
            "shuffle_bytes_written": sum(m.get("shuffleBytesWritten", 0) for m in exchanges),
            "shuffle_data_size": sum(m.get("dataSize", 0) for m in exchanges),
            "broadcast_data_size": sum(m.get("dataSize", 0) for name, m in nodes if name == "BroadcastExchangeExec"),
            "scan": scan}


def timed(sql_text, runs=RUNS):
    walls = []
    for _ in range(runs):
        df = spark.sql(sql_text)
        started = time.perf_counter()
        rows = df.collect()
        walls.append(round(time.perf_counter() - started, 2))
    return {"wall_s": walls, "median_s": sorted(walls)[len(walls) // 2], "result_rows": len(rows), **plan_stats(df)}


def detail(table):
    d = spark.sql(f"DESCRIBE DETAIL {table}").first()
    rows = spark.table(table).count()
    return {"rows": rows, "files": d["numFiles"], "bytes": d["sizeInBytes"],
            "avg_file_kb": round(d["sizeInBytes"] / d["numFiles"] / 1024, 1) if d["numFiles"] else None,
            "partition_columns": list(d["partitionColumns"])}


def stage(name, fn):
    try:
        result[name] = fn()
    except Exception:
        result[name] = {"error": traceback.format_exc()}
        result["failures"].append(name)
    save()


def build_source():
    lib = notebookutils.variableLibrary.getLibrary("vl_crypto")
    tracked = [p.strip() for p in lib.tracked_pairs.split(",") if p.strip()]
    live = spark.table("bronze.trades").where("_ingest_path = 'ws'").where(F.col("symbol").isin(tracked))
    profile = {r["symbol"]: r for r in live.groupBy("symbol").agg(
        F.count("*").alias("n"), F.expr("percentile_approx(price, 0.5)").alias("price"),
        F.expr("percentile_approx(qty, 0.5)").alias("qty")).collect()}
    total = sum(r["n"] for r in profile.values())
    slots = []
    for symbol, r in profile.items():
        slots += [symbol] * max(1, round(1000 * r["n"] / total))
    n = ROWS_PER_DAY * DAYS
    step_us = DAYS * 86_400_000_000 / n
    start_us = int(datetime.fromisoformat(START).replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
    price_of = F.create_map(*[x for s, r in profile.items() for x in (F.lit(s), F.lit(float(r["price"])))])
    qty_of = F.create_map(*[x for s, r in profile.items() for x in (F.lit(s), F.lit(float(r["qty"])))])

    gen = (spark.range(n)
           .withColumn("symbol", F.element_at(F.array(*[F.lit(s) for s in slots]),
                                              (F.expr(f"pmod(xxhash64(id), {len(slots)})") + 1).cast("int")))
           .withColumn("price", F.element_at(price_of, F.col("symbol")) * (1 + (F.rand(7) - 0.5) * 0.02))
           .withColumn("qty", F.element_at(qty_of, F.col("symbol")) * (-F.log(1 - F.rand(11))) / math.log(2) + 1e-8)
           .withColumn("side", F.when(F.rand(13) < 0.5, "buy").otherwise("sell"))
           .withColumn("ord_type", F.when(F.rand(17) < 0.6, "limit").otherwise("market"))
           .withColumn("trade_id", F.col("id") + 1)  # unique over the table; contiguity per pair is not needed here
           .withColumn("ts", F.expr(f"timestamp_micros({start_us} + cast(id * {step_us} as bigint))"))
           .withColumn("timestamp", F.date_format("ts", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"))
           .withColumn("_ingest_path", F.lit("ws"))
           .withColumn("_load_ts", F.col("timestamp"))
           .withColumn("trade_date", F.to_date("ts")))
    started = time.perf_counter()
    gen.select(*BRONZE_COLUMNS, "trade_date").write.format("delta").mode("overwrite").saveAsTable(SRC)
    write_s = round(time.perf_counter() - started, 1)
    out = {"write_s": write_s, "profile_pairs": len(profile), "slots": len(slots),
           "real_live_rows_profiled": total, **detail(SRC)}
    if out["rows"] != n:
        raise AssertionError(f"expected {n} rows, got {out['rows']}")
    return out


def session_facts():
    return {"spark_version": spark.version, "default_parallelism": spark.sparkContext.defaultParallelism,
            "optimize_write_default": spark.conf.get("spark.microsoft.delta.optimizeWrite.enabled", "unset"),
            "vorder_default": spark.conf.get("spark.sql.parquet.vorder.default", "unset"),
            "real_bronze": detail("bronze.trades")}


def queries(table, span_days=DAYS):
    mid_day = (date.fromisoformat(START) + timedelta(days=span_days // 2)).isoformat()
    mid_id = ROWS_PER_DAY * DAYS // 2 + 1
    row = spark.table(SRC).where(F.col("trade_id") == mid_id).first()
    return {"one_day": f"SELECT symbol, count(*) trades, sum(qty * price) notional FROM {table} WHERE trade_date = DATE'{mid_day}' GROUP BY symbol",
            "one_pair": f"SELECT trade_date, count(*) trades, sum(qty * price) notional FROM {table} WHERE symbol = 'ETH/USD' GROUP BY trade_date",
            "all_pairs_daily": f"SELECT symbol, trade_date, count(*) trades, sum(qty * price) notional FROM {table} GROUP BY symbol, trade_date",
            "point_lookup": f"SELECT * FROM {table} WHERE symbol = '{row['symbol']}' AND trade_id = {mid_id}"}


def layouts():
    out = {}
    for name, partition in [("flat", None), ("by_day", ["trade_date"]), ("by_pair", ["symbol"])]:
        table = SRC if name == "flat" else f"test.bm_{name}"
        entry = {}
        if name != "flat":
            writer = spark.table(SRC).write.format("delta").mode("overwrite").partitionBy(*partition)
            started = time.perf_counter()
            writer.saveAsTable(table)
            entry["write_s"] = round(time.perf_counter() - started, 1)
        entry.update(detail(table))
        entry["queries"] = {q: timed(sql) for q, sql in queries(table).items()}
        out[name] = entry
        result["layouts"] = out
        save()
    rows = {name: e["rows"] for name, e in out.items()}
    if len(set(rows.values())) != 1:
        raise AssertionError(f"layouts hold different row counts: {rows}")
    return out


def small_files():
    keep = {key: spark.conf.get(key, None) for key in
            ["spark.microsoft.delta.optimizeWrite.enabled", "spark.databricks.delta.optimizeWrite.enabled",
             "spark.databricks.delta.autoCompact.enabled"]}
    for key in keep:
        spark.conf.set(key, "false")  # the write must keep the file count it is asked for
    out = {"conf_before": keep, "subset_days": SUBSET_DAYS}
    try:
        subset_end = (date.fromisoformat(START) + timedelta(days=SUBSET_DAYS)).isoformat()
        subset = spark.table(SRC).where(f"trade_date < DATE'{subset_end}'")
        subset_rows = subset.count()
        for rows_per_file in FILE_ROWS:
            table = f"test.bm_files_{rows_per_file}"
            files_wanted = max(1, math.ceil(subset_rows / rows_per_file))
            subset.repartitionByRange(files_wanted, "trade_id").write.format("delta").mode("overwrite").saveAsTable(table)
            qs = {q: sql for q, sql in queries(table, SUBSET_DAYS).items() if q != "point_lookup"}
            entry = {"files_wanted": files_wanted, "before": detail(table), "before_queries": {q: timed(s) for q, s in qs.items()}}
            started = time.perf_counter()
            spark.sql(f"OPTIMIZE {table}").collect()
            entry["optimize_s"] = round(time.perf_counter() - started, 1)
            entry["after"] = detail(table)
            entry["after_queries"] = {q: timed(s) for q, s in qs.items()}
            out[f"rows_per_file_{rows_per_file}"] = entry
            result["small_files"] = out
            save()
    finally:
        for key, value in keep.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)
    return out


def joins():
    spark.sql("CREATE OR REPLACE TABLE test.bm_dim_pair AS SELECT * FROM gold.dim_pair")
    (spark.range(DAYS).select(F.date_add(F.lit(START).cast("date"), F.col("id").cast("int")).alias("date"))
     .select("date", F.year("date").alias("year"), F.quarter("date").alias("quarter"), F.month("date").alias("month"),
             F.dayofmonth("date").alias("day"), (F.expr("weekday(date)") + 1).alias("day_of_week"),
             F.date_format("date", "EEEE").alias("day_name"), (F.expr("weekday(date)") >= 5).alias("is_weekend"))
     .write.format("delta").mode("overwrite").saveAsTable("test.bm_dim_date"))
    out = {"dim_pair": detail("test.bm_dim_pair"), "dim_date": detail("test.bm_dim_date")}
    template = ("SELECT /*+ {hint}(p, d) */ d.day_name, p.base, count(*) trades, sum(t.qty * t.price) notional "
                f"FROM {SRC} t JOIN test.bm_dim_pair p ON t.symbol = p.symbol JOIN test.bm_dim_date d ON t.trade_date = d.date "
                "GROUP BY d.day_name, p.base")
    for name, hint in [("broadcast", "BROADCAST"), ("sort_merge", "MERGE")]:
        out[name] = timed(template.format(hint=hint))
        result["joins"] = out
        save()
    return out


stage("session", session_facts)
stage("source", build_source)
stage("layouts", layouts)
stage("small_files", small_files)
stage("joins", joins)

for table in [f"test.bm_{n}" for n in ["by_day", "by_pair", "dim_pair", "dim_date"]] + [f"test.bm_files_{r}" for r in FILE_ROWS]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

save()
print(json.dumps(result, indent=2, default=str)[:6000])
if result["failures"]:
    raise RuntimeError("benchmark stages failed: " + ", ".join(result["failures"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
