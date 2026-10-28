# PySpark notebooks for Silver and Gold facts, Materialized Lake Views for aggregates, no dbt

Silver, `fact_trade` and the dimensions are PySpark notebooks (dedupe, `MERGE`, row-level quarantine written by hand, since those are the skills on show). Aggregate tables are Materialized Lake Views over `fact_trade`: always recomputed from the deduplicated fact, so a backfill cannot double them, with lineage and refresh built in. Table-level checks run with Great Expectations before Gold; an error fails the Gold step and holds the watermark.

## Considered options

- **Notebook Silver plus dbt Gold (first attempt's ADR 0013).** Needed an OAuth Lakehouse connection created by hand in the UI, ran 5–7 min per job, duplicated items for test scenarios, and meant two engines and two test styles.
- **MLV for everything.** Window functions and `DISTINCT` force full refresh, the DDL is Fabric-only and hard to unit test, and it would hide the dedupe and merge logic.
- **Soda (first attempt) instead of Great Expectations.** Owner preference for GX.
