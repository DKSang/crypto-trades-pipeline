# Incremental loads read Change Data Feed by commit version, not by timestamp

Bronze to Silver and Silver to Gold both read Delta Change Data Feed from the last processed `_commit_version`, stored per source table in `ops.etl_watermark`. Each run captures the current version at start, reads `table_changes(watermark + 1, snapshot_version)` and advances the watermark only on success, so a failed run rereads exactly the same range. Commit versions are assigned at commit and only grow, so no row can fall behind the watermark.

## Considered options

- **Run-start timestamp compared with a row `process_timestamp` (first attempt's ADR 0008).** A row stamped at T1 but committed at T3 is invisible to a Gold run starting at T2 (T1 < T2 < T3); that run saves T2 and the row is skipped forever. The first attempt had to forbid Silver and Gold from ever overlapping.
- **Spark Structured Streaming (`availableNow`) on Bronze.** Bronze is append-only, so it adds a checkpoint to manage and nothing CDF doesn't give. Event-time windows and watermarks live in the Eventhouse hot path instead.
