# Bronze benchmark and Spark versus DuckDB

Issue #16. Measured on 2026-10-29 in `Crypto-DEV` by `nb_benchmark_bronze` (results in `Files/checks/nb_benchmark_bronze.json`) on Fabric Spark 4.1.1, default session settings (V-Order off, optimize write not set), default parallelism 8.

## What the numbers are

Real Bronze holds hours of data, not the 2 to 4 weeks the issue first assumed: the collector runs in sessions. So the benchmark runs on **9.8 million Bronze-shaped rows, 28 days at 350,000 a day, generated in scratch tables `test.bm_*`**. The pair mix, median price and median quantity come from the live rows of `bronze.trades`; trade times run in order, as the Eventstream appends them. The rows are synthetic, the shape is real. Nothing in the production Bronze table was written; it was only read (`DESCRIBE DETAIL`, a profile).

Wall time is `collect()` time, three runs each, the median quoted with the range. At this size most queries finish under a second, so differences of a few tenths of a second are noise. **Bytes and files read** come from the executed plan's metrics (what the Spark UI shows) and do not vary between runs: trust those more than the times.

| | Rows | Files | Size |
|---|---|---|---|
| Generated Bronze copy (28 days, unpartitioned) | 9,800,000 | 8 | 354 MB (36 B a row) |
| Real `bronze.trades` on the day of the benchmark | 23,017 | 158 | 2.0 MB (90 B a row, 146 rows a file, 12.7 KB a file) |

## Partitioning that hurt

Four typical queries on the same 9.8 M rows, laid out three ways. Cells are median wall seconds (range); files and MB are what the query read.

| Query | Flat | Partitioned by day | Partitioned by pair |
|---|---|---|---|
| One day, all pairs | 0.51 s (0.51–1.59), 1 file, 44 MB | 0.35 s (0.33–0.81), 1 file, 13 MB | 0.70 s (0.66–1.73), **20 files, 383 MB** |
| One pair, all days | 1.01 s (0.52–1.75), 8 files, 354 MB | 0.66 s (0.58–2.35), 28 files, 373 MB | 0.44 s (0.41–0.84), 1 file, 29 MB |
| Every pair, every day | 0.73 s (0.53–1.14) | 0.77 s (0.75–1.13) | 0.93 s (0.86–**7.37**) |
| One trade by (`symbol`, `trade_id`) | 0.68 s (0.45–0.96), 1 file, 44 MB | 0.44 s (0.30–0.67), 1 file, 13 MB | 0.44 s (0.19–0.68), 1 file, 14 MB |
| Table size | 354 MB | 373 MB (+5%) | 383 MB (+8%) |

- **Partitioning by pair made the question this pipeline asks most, a day, read every file.** The load reads Bronze and Silver by commit version and Gold by day (`agg_pair_daily` refreshes only the days that changed), so the natural key is time. The by-pair layout wins only the one-pair query (0.44 s against 1.01 s) and reads the whole table for a day (383 MB against 13 MB by day).
- **Partitioning by day helped, but the flat table was close.** The flat table's one-day query read a single 44 MB file, not all 354 MB: rows land in time order, so Delta's per-file min and max on `trade_date` skips the other files. By-day reads 13 MB; the saving is real in bytes and about 0.15 s in time, at this size.
- **The 7.37 s is the first, cold run of one query on the by-pair layout**; the next two took 0.9 s. I did not find why, so I do not draw a conclusion from it.
- **Each layout costs files and bytes.** By day adds 20 files and 5% size, by pair 12 files and 8%, probably because each writing task makes one file per partition value (not checked). A partition column also has a price in the small-files problem below: a partition holding a day of Eventstream files is a day of tiny files.
- **Not tested:** whether the Eventstream writer accepts a pre-created partitioned table. Bronze stays unpartitioned in this project; the partition (`trade_date`) is on `gold.fact_trade`, where `agg_pair_daily` needs it for incremental refresh (`reference/lessons.md`).

## Small files

The Eventstream Lakehouse destination controls file size with *Minimum rows* and *Maximum duration*. The setting was not changed on the real Eventstream. Instead, one week of the copy (2.45 M rows, 87.7 MB once compacted) was written as files of 500 to 500,000 rows, queried, compacted with `OPTIMIZE`, and queried again. The real Bronze has **146 rows a file**, so the 500-row case is better than what production writes.

| Rows a file | Files | Size | `OPTIMIZE` | One pair, all days: before → after | One day: before → after |
|---|---|---|---|---|---|
| 500 | 4,900 | 130 MB | 30.4 s → 1 file, 88 MB | **9.64 s → 0.79 s** | 2.62 s → 0.78 s |
| 5,000 | 490 | 109 MB | 11.5 s → 1 file | 0.92 s → 0.78 s | 0.34 s → 0.93 s |
| 50,000 | 49 | 108 MB | 6.2 s → 1 file | 0.54 s → 0.76 s | 0.33 s → 0.27 s |
| 500,000 | 5 | 91 MB | 6.1 s → 1 file | 0.53 s → 0.74 s | 0.53 s → 0.84 s |

- **Below a few thousand rows a file, the file count is the cost.** At 500 rows, a scan of the week took 7 to 12 s and reads 49% more bytes (130 MB against 88 MB: each file repeats its footer and statistics); after `OPTIMIZE`, medians of 0.7 to 0.8 s. At 5,000 rows a file (490 files) no difference was visible. Real Bronze is on the bad side of that line and has to be compacted.
- **`OPTIMIZE` is not free of cost on the other side.** Every case ended as one 88 MB file, because the table is below the target file size. A one-day query on that file reads 88 MB where the 50,000-row layout read 18 MB; at this size the extra time is a few tenths of a second, but the file skipping is gone. A larger table would split into several files; this one did not, so this cost is only shown at this size.
- **`OPTIMIZE` time grows with the file count, not the bytes**: 30 s for 4,900 files, 6 s for 5 to 49.
- The Eventstream destination commits about every 2 minutes regardless of *Maximum duration* (`reference/lessons.md`), so the setting does not remove the need to compact.

## Broadcast and sort-merge joins

Trades (9.8 M rows) joined to `dim_pair` (20 rows) and `dim_date` (28 rows), grouped by weekday and base asset, with the join forced by a hint.

| | Broadcast | Sort-merge |
|---|---|---|
| Plan | 2 × `BroadcastHashJoinExec`, 1 shuffle (the final aggregation) | 2 × `SortMergeJoinExec`, 5 shuffles |
| Wall time, median (range) | 1.45 s (1.18–1.79) | 2.77 s (2.70–3.94) |
| Shuffle written | 55 KB | 366 MB |
| Shuffle data size (uncompressed) | 36 KB | 941 MB |

The sort-merge join moves the whole fact through the network twice for dimensions that fit in a few kilobytes: 6,700 times more shuffle and about twice the time. Spark picks the broadcast join by itself below `spark.sql.autoBroadcastJoinThreshold`; the hint only shows the price of the alternative. The plan metric reports 134 MB of broadcast data for the two dimensions, which is about 128 MiB of allocated hash-relation memory and not the dimension data (22 KB and a few KB on disk); I did not verify that reading.

## The real pipeline at three batch sizes

`nb_benchmark_process_gold` runs the real `pl_process_gold` on scratch tables, through the Fabric job API, with a fresh run per batch size: 17,000 rows (what an hour of the live session produced), 350,000 (a day) and 3,500,000 (ten days), rows taken from the synthetic copy. After each run Silver, Gold and `agg_pair_daily` were checked against Bronze (same row count, same notional): all three matched. One run per size; none overlapped a scheduled run of the real pipeline.

| Batch | Pipeline | Silver | Quality gate | Gold | MLV refresh | Reconcile | Rows a second, whole pipeline |
|---|---|---|---|---|---|---|---|
| 17,000 | 390 s | 70 s | 99 s | 81 s | 66 s | 52 s | 44 |
| 350,000 | 386 s | 66 s | 111 s | 80 s | 49 s | 65 s | 907 |
| 3,500,000 | 436 s | 85 s | 140 s | 80 s | 50 s | 66 s | 8,026 |

- **Up to a day of trades, the time is the cost of starting.** 200 times more rows added 46 s to the pipeline. Each activity is a notebook that starts its own Spark session, and a batch of 17,000 rows takes as long as one of 350,000. The "rows a second" column is therefore a property of the batch size, not of the code: it rises because the divisor does not move.
- **The rate that does mean something is between the two largest sizes**: 3.15 M more rows cost 19 s in Silver (about 167,000 rows a second) and 50 s in the whole pipeline (about 63,000 rows a second). Gold's step stayed at 80 s, so its rate is not measurable here. These come from one run each.
- **The real hourly runs agree with the 17,000-row run.** The two scheduled runs that carried the live session's batch, at 11:27 and 12:27, took 400 s and 381 s; the scale run of 17,000 rows took 390 s. Scheduled runs with nothing or little to load (the collector runs in sessions) took 147 to 296 s, 13 of them: the floor is the cost of starting five Spark sessions. One run per size is a small sample, so 386 s against 436 s is not evidence that 3.5 M rows are slower than 350,000; the gap in the quality gate (111 s to 140 s) and in Silver is more likely to be data.
- **The quality gate is the slowest activity** at every size, 99 to 140 s, more than Silver and Gold.

## Spark or DuckDB

At this project's volume, one node is enough. Measured: the Gold fact takes 33 bytes a row, so 350,000 trades a day is about 11 MB a day, about 4 GB a year; the 28-day Bronze copy is 354 MB, and every query on the compact layouts above, scanning all of it, had a median under two seconds. DuckDB would have been enough to build Gold from Bronze here.

That agrees with the benchmarks other people published. Each figure below was read from its source on 2026-10-29.

| Source | Setting | Finding |
|---|---|---|
| [endjin, *Fabric Performance Benchmarking: Spark versus Python Notebooks*](https://endjin.com/blog/fabric-performance-benchmarking-spark-versus-python-notebooks) (22 April 2026) | Fabric, about 30 M rows (5 GB of CSV), Pandas, PySpark, Polars, DuckDB | Elapsed time with spin-up on 2 vCores: PySpark 125.7 s, DuckDB 133.6 s. Execution without spin-up: DuckDB 47.0 s, PySpark 107.1 s. Cheapest run: DuckDB 107.9 CU, PySpark 598.5 CU, "more than 5x". Spin-up: about 24 s for a Python notebook, 3 to 3.5 min for Spark. Advice: reserve Spark for data beyond one node's memory, or where V-Order or Liquid Clustering are demonstrably valuable. |
| [Coiled, *DataFrames at Scale Comparison: TPC-H*](https://docs.coiled.io/blog/tpch) (14 May 2024) | TPC-H at 10 GB to 10 TB, local and cloud | At 10 GB Spark is "very out-performed at small scale by everyone (by about 10x+)"; at 100 GB local DuckDB is the "clear winner" and Spark "fails here pretty easily". At 1 TB or less in the cloud: "Choose Dask or DuckDB". Beyond that the authors recommend Dask or Spark. No per-query times in the page text. |
| [DuckDB Labs, *Updates to the H2O.ai db-benchmark*](https://duckdb.org/2023/11/03/db-benchmark-update) (3 November 2023) | group by and join at 0.5, 5, 50 GB on a 128-core machine | DuckDB "the fastest library for both join and group by queries at almost every data size" and one of two solutions to finish the 50 GB join. The page has no Spark numbers; the per-solution times are charts on the [results page](https://duckdblabs.github.io/db-benchmark/), which I could not read as text. |
| [MotherDuck, *Big Data is Dead*](https://motherduck.com/blog/big-data-is-dead/) (about 2023, undated) | the author's experience with BigQuery customers | "90% of queries processed less than 100 MB"; most customers under 1 TB; heavy users' median under 100 GB. MotherDuck sells a DuckDB service: read it as a vendor's argument. |

Two of those are from vendors or DuckDB's own maintainers, and Coiled's and endjin's tests use other data than ours. None measures this pipeline. They support one claim only: for data this size a single node is cheaper and often as fast.

**Why Spark is used anyway.** Not because the volume needs it.
- The owner's stated aim for Silver and Gold is to show Spark skills (`reference/decisions/0003-pyspark-notebooks-and-mlv-no-dbt.md`).
- `agg_pair_daily` is a Materialized Lake View, whose DDL is Fabric Spark only (same ADR); a DuckDB build would have to replace it with a table it refreshes itself.
- The load reads Bronze and Silver by Delta commit version (Change Data Feed, ADR 0002), which the Spark path of this project already does.

**What it costs.** Each notebook in `pl_process_gold` starts its own Spark session, and for an hourly batch of about 17,000 rows the five activities took 54 to 111 s each; with an empty batch the whole pipeline still takes 147 to 296 s, so start-up is most of it (see the table above). endjin reports the same gap: 24 s to start a Python notebook against 3 to 3.5 min for Spark. If this pipeline only ever stays at a few hundred thousand rows a day, a Python notebook with DuckDB reading the same Delta tables is the cheaper design, and nothing in the Gold model would change.
