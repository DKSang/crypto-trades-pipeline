# crypto-trades-pipeline

Kraken spot trades for 20 USD pairs on Microsoft Fabric: Eventstream hot path and a Gold star. Design: `reference/architecture/trades-pipeline.md`.

## Numbers

Taken on 2026-10-29 in `Crypto-DEV`. **Read** = counted from the real tables, **measured** = a run made for this purpose, **estimated** = worked out from other numbers, and the reasoning is given. The live data is one 60-minute collector session (16,748 events) on top of a day of development runs; the large volumes are synthetic rows in the Bronze shape (generated from the live pair mix, prices and quantities), and say so. How each was measured and the full tables: [`docs/bronze-benchmark.md`](docs/bronze-benchmark.md).

| Group | Metric | Value | |
|---|---|---|---|
| Data volume | Total records | Bronze 23,017 · Silver 22,957 · Gold `fact_trade` 22,957. Scale runs processed 3.5 M rows, layout benchmark 9.8 M (synthetic) | read, measured |
| | Data size | Bronze 2.0 MB in 158 files (90 B a row) · Silver 0.64 MB · Gold fact 0.75 MB (33 B a row). Synthetic 28-day copy: 354 MB | read, measured |
| | Daily ingestion volume | About 350,000 trades a day (4.4 live trades a second in the session, about 378,000 a day) ≈ 11 MB a day in Gold, 31 MB a day as Bronze lands it, ≈ 4 GB a year in Gold | estimated: Kraken 24 h count of 2026-10-28 and the session rate, times bytes a row above |
| Performance | Batch processing time | Pipeline for one hourly batch (17,000 rows): 390 s; one day (350,000): 386 s; ten days (3.5 M): 436 s. Real hourly runs with the session's batch: 381–400 s. Quality gate is the slowest step (99–140 s) | measured, one run per size |
| | Throughput | Whole pipeline 44 / 907 / 8,026 rows a second at 17,000 / 350,000 / 3.5 M, which mostly shows the fixed cost of five Spark sessions. Between the two largest batches: Silver about 167,000 rows a second, pipeline about 63,000 | measured |
| | P95 processing latency | Kraken trade to Eventhouse: **1.6 s** (median 0.7 s, P99 13.4 s). Kraken trade to a Bronze file: **117 s** (median 62 s, P99 122 s), landing alone is 62 s at the median, consistent with the Eventstream's roughly 2-minute commit. Gold follows on the next hourly run | measured, 15,758 live trades |
| Data quality | Data quality pass rate | 100% of the session's rows passed Silver's rules (0 of 16,748 quarantined). Gate: all error-level checks passed; of 12 results since the session start, 10 passed and the 2 failures are the warn-level `trade_id_contiguity` (not investigated) | read |
| | Duplicate records | 0 in the session (no key was already in Bronze). Whole Bronze: 54 duplicate rows, from development runs; Silver drops them | read |
| | Reconciliation accuracy | 2026-10-29: hot path 18,657 trades, Gold 18,653. The gap of 4 trades and 800,000 USD is the 4 test events of `nb_test_whale_alert`; without them both paths hold 18,653 trades and 12,384,735.43 USD (raw 99.98%, otherwise exact) | read |
| Reliability | Pipeline success rate | Scheduled runs 15 of 16 (93.8%), 2026-10-29 00:01 to 13:27. The one failure: a Spark capacity limit (HTTP 430) at `nb_check_silver_batch`, Gold's watermark held and the next run succeeded. Manual runs are left out (they include forced failures of the tests) | read |
| | Recovery time | After that failure, 62 minutes to the next green run, which is the hourly schedule. After a forced Gold failure and a rerun started at once: the failed run took 263 s and the green rerun **342 s** (`nb_test_process_gold` runs 3 and 4, a small fixture; a rerun on a bigger batch is about one pipeline run, 4–7 minutes) | read (62 min); measured (342 s, small batch); estimated (4–7 min for bigger batches) |
| | Idempotency validation | A rerun with nothing new left Silver, Gold, the aggregate and both watermarks unchanged, on 3.5 M rows. A batch overlapping earlier trades, a trade sent twice, a forced Gold failure and its rerun, a duplicate key written into Silver and its fix: `nb_test_process_gold`, 6 of 6 runs passed | measured (rerun at scale); test result (the rest, on a small fixture). A batch of 100% duplicates was not run at scale |

## What it shows about Spark

At this volume one machine is enough, and the pipeline's time is mostly the start of five Spark sessions: [`docs/bronze-benchmark.md`](docs/bronze-benchmark.md) has the layout, small-file and join measurements, published DuckDB-versus-Spark benchmarks, and why Spark is used anyway.
