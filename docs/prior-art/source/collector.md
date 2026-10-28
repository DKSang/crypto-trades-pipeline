# Polymarket collector

The collector reads the published metadata/watchlist, rechecks markets, captures WebSocket frames verbatim, and sends envelopes through a bounded in-memory buffer to Event Hubs in batches. Frame IDs use `frame_capture_id`; child entries use stable `capture_id` values derived from `(session_id, frame_seq, entry_index)`. Asset IDs remain strings. The Eventstream Variable Library value is the complete custom-endpoint connection string, including `EntityPath`.

## Recheck and subscription

Requests have a 5-second timeout; a watchlist recheck shares a 30-second deadline. Transient 429, 5xx, timeout and connection failures retry up to three times after the initial attempt, with exponential backoff from 0.25 seconds. `Retry-After` is capped by the remaining deadline. Isolated exhausted markets are excluded; if all fail transiently the session stops as `SOURCE_UNAVAILABLE`. The subscription defaults `custom_feature_enabled` to false.

## Buffering, gaps and manifests

The session-local buffer defaults to 1,000 records / 64 MiB; batches contain up to 100 records. Sink writes retry three times with 0.25-second linear backoff. There is no Files queue or cross-session replay. Every submitted record ends as `sent`, `lost` or `oversize` (`records_sent_current_session`, `records_lost`, `records_oversize` in the manifest). Unsent records after retry/drain count as lost, including a batch cancelled mid-send by the drain timeout; that batch may already have reached the hub, so `lost` is an upper bound. Any lost or oversize record makes the session `incomplete` and the exit code non-zero. The collector attempts one `sink_failure` gap envelope; if that send also fails, the manifest records the loss.

Session manifests are written under `Files/raw/market_intelligence/polymarket/ws_market/sessions/YYYY/MM/DD/<session_id>.json`, partitioned by UTC session start. Metrics include sent batches/records, maximum buffer depth, send latency, and dropped-record count. No collector data is written under `ws_market/spool/` or `ws_market/stream/`.

Exit status is 0 only after planned end, successful sink close and zero pending records; otherwise the run is nonzero. Required dependencies/invalid CLI arguments return 2.

## Offline tests

From repository root in PowerShell:

```powershell
.\.venv\Scripts\python.exe -m pytest fabric/tests -q
```

Tests use captured samples and fake HTTP, WebSocket and sink implementations. They do not make network or cloud calls. On Windows, use a short pytest `--basetemp` path; deep checkout paths and generated fixture names can exceed legacy MAX_PATH.

# Collector implementation notes

## Contract interpretation

- DEC-41-02 names `source_ts`; DEC-41-05 also names `source_ts_raw`. Both fields are emitted. `source_ts` is UTC at millisecond precision when every parsed event has the same parseable Unix-millisecond timestamp; `source_ts_raw` preserves that common value as read. A frame containing missing or differing timestamps has no single frame-level timestamp, so both fields are null; the original per-event timestamps remain in `payload_raw`.
- DEC-41-05 requires child entries with `capture_id` but does not specify their serialization. The envelope uses an `entries` array containing each helper-generated child ID in source order; all child values remain only in the full original frame in `payload_raw`.
- The contract names `frame_kind`, `schema_version`, and `watchlist_hash` but does not define their value formats or hash algorithm. `frame_kind` is `heartbeat` for PONG and `data` otherwise. `schema_version` is preserved when all parsed events provide one common value; it is null when absent or ambiguous. `event_type` is emitted at frame level only when parsed events agree. `watchlist_hash` is SHA-256 of the sorted, unique accepted asset ID list serialized as compact JSON. These choices need contract-owner confirmation if downstream depends on different semantics.
- A session pins the published manifest's `criteria_version`, which must equal `criteria_version` in `vl_polymarket`; `load_published_watchlist` also recomputes the Watchlist hash from the mapping's asset IDs and refuses a mismatch before any capture. The source contract does not define a schema-version registry or a canonical watchlist-hash representation.
- The contract requires retry with stable IDs but does not set a sink retry budget or backoff. Defaults are three retries after the initial attempt and linear 0.25-second backoff (`SessionRunner` defaults, not configurable); they have not been tuned against a live sink.
- D3-08 supersedes durable Files spool and cross-session replay. The collector has a bounded in-memory buffer (1,000 records / 64 MiB by default); losses after sink retry/drain are counted and made visible through a `sink_failure` gap and manifest `records_lost`.

## Owner decisions D2-05, D2-08, and D2-10

- **D2-05 / CL-03:** Each Gamma or CLOB HTTP request has a 5-second timeout. Transient 429, 5xx, timeout, and connection failures receive three retries after the initial request, with exponential backoff starting at 0.25 seconds (`HttpxMarketRechecker` defaults). A numeric or HTTP-date `Retry-After` is honored as a minimum wait. These settings apply independently to each request. Exhaustion excludes only that market with `EXCLUDED_RECHECK_TRANSIENT_EXHAUSTED`; deterministic market/metadata failures retain specific exclusion codes or use `EXCLUDED_RECHECK_INVALID_METADATA`. The runner continues with remaining valid markets and blocks before opening a WebSocket when none remain.
- **D2-08:** Subscriptions include `custom_feature_enabled: false` (`SessionRunner` default). Collector logic does not require `best_bid_ask` or `new_market` events.
- **D2-10:** `frame_kind`, `watchlist_hash`, sink retry defaults (three retries with 0.25-second linear backoff), and in-memory buffer limits (1,000 records / 64 MiB) remain as described above. Recheck retry settings are documented under D2-05.

## Owner decision D3-07, superseded queue scope D3-08

`bronze.ws_messages` is the canonical raw store for WebSocket envelopes; Eventhouse `raw_stream_envelope` is its one-day hot copy, reconciled by `frame_capture_id`. D3-08 removes the Files queue and cross-session replay. The collector uses a bounded in-memory buffer and writes only session manifests to `ws_market/sessions/YYYY/MM/DD/<session_id>.json`; it writes nothing under `ws_market/spool/` or `ws_market/stream/`.

## Source and local development

The single source of truth is `fabric/Polymarket-DEV/nb_collector_session.Notebook/notebook-content.py`; there is no collector package or wheel. Public runtime dependencies are pinned in its install cell. There is one entry point, `run_collector_session(files_root, duration_s, inject_disconnect_after_s)`; the `# main` cell calls it with `/lakehouse/default/Files` and the notebook parameters. Configuration comes only from `vl_polymarket`: each variable is a `CollectorConfig` field of the same name (a test reads the checked-in `variables.json`), and an unknown name fails the run.

Local tests load the notebook with `fabric.testkit.notebook.load`; use pytest==9.1.1 (formerly the development dependency in collector/requirements.txt). No repository test requirements file exists.

For notebook snippets, use the [interactive `fntk` loop](../reference/fabric-cli.md#notebook-toolkit-fntk) against `scn_<name>` tables, never production schemas. Copy the working snippet into the repo notebook above, run `pytest fabric -q`, preview then deploy only `nb_collector_session`, and run one full `play` before commit ([Test loop](environment.md#test-loop)). A batch play does not prove live collector transport; retain the collector's offline tests.
