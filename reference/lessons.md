# Lessons

## Fabric: Eventstream Lakehouse destination

- **Small files are a setting.** DOCUMENTED ([Learn](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/add-destination-lakehouse)): Advanced > *Minimum rows* (1 to 2 million per file) and *Maximum duration* (1 min to 2 h per file). Larger values give fewer, bigger files; still run `OPTIMIZE`.
- **Schema comes from the first record.** DOCUMENTED (same page): a new table takes its schema from the first event; later extra fields are dropped, missing ones become null. Send a stable shape.
- **Events before routing starts can be lost.** DOCUMENTED (same page): uncheck *Activate ingestion*, publish, then resume with a custom earlier start time.
- **The writer rejects table features.** OBSERVED 2026-10-28: a table pre-created by Fabric Spark (deletion vectors on by default, so protocol 3/7 with `invariants`, `deletionVectors`…) failed every write: "Table has unsupported features. Unsupported writer features: invariants". Pre-create with `delta.enableDeletionVectors = false, delta.minReaderVersion = 1, delta.minWriterVersion = 4` (v4 is the lowest that allows Change Data Feed); the writer then appends and CDF by commit version returns every row (`nb_setup_lakehouse`, `nb_check_bronze_trades`).
- **Commits come every ~2 min, not every *Maximum duration*.** OBSERVED 2026-10-28: with `maximumDurationInSeconds: 900` and `minimumRows: 2000000` the destination still committed every ~2 min. Expect small files; `OPTIMIZE`.
- **An ISO 8601 string doesn't convert to a `TIMESTAMP` column.** OBSERVED 2026-10-28 (`nb_check_es_types`, one probe event per case): events with `"2026-10-28T15:15:10.708706Z"` or `"…+00:00"` in a field whose Bronze column is `TIMESTAMP` were dropped (`OutputDataConversionError.TypeConversionError`); events with only strings, bigints and floats (including `4.3712e-06` and `80395.0`) landed. A single such field drops every event, so the table gets no commit at all. Keep time fields as `STRING` in Bronze and cast in Silver.
- **Dropping the destination table deactivates the destination.** OBSERVED 2026-10-28: after `DROP TABLE` and `CREATE TABLE`, the Lakehouse destination went inactive until reactivated in the portal; `CREATE OR REPLACE TABLE` left it `Running`. But a running destination keeps the column types it read at start: after `CREATE OR REPLACE` changed `_load_ts` from `TIMESTAMP` to `STRING`, every event was still dropped until the destination was paused and resumed (`startType: CustomTime` to replay what was sent meanwhile). After any change to the table's columns, restart the destination.
- **CDF can't be read across a schema change.** OBSERVED 2026-10-28: `table_changes` from version 1 to 3 of `bronze.trades` failed with `DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA` because version 2 was a `CREATE OR REPLACE` that changed column types. Start CDF reads after the last `CREATE`/`REPLACE` commit (`nb_check_bronze_trades`); after a Bronze schema change, reset the Silver watermark (ADR 0002) to that version.
- **Dropping the destination table deactivates the destination.** OBSERVED 2026-10-28: after `DROP TABLE` and `CREATE TABLE bronze.trades` (`nb_setup_lakehouse`), the Lakehouse destination went inactive and nothing landed (table stayed at version 0); reactivating it in the portal fixed it. After recreating a destination table, reactivate the destination, with a custom start time if events sent meanwhile must land.

## Fabric: Eventstream HTTP source

- **The connection must be type `WebForPipeline`.** OBSERVED 2026-10-28: `RestService` and `HttpServer` connections fail import with "Error converting value … to type ExtensionDataSourceKind". `WebForPipeline` (Anonymous) works. The definition has no path field: the connection's `baseUrl` is the full endpoint (e.g. `https://api.kraken.com/0/public/Trades`); query parameters go in `requestParameters`.
- **A JSON array response becomes one event per element.** OBSERVED 2026-10-28: a JSON array response landed as one row per element, none as an array.
- **`fab import` works despite the CI/CD caveat.** DOCUMENTED ([Learn](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/add-source-http)): the HTTP source "doesn't support CI/CD features… might result in errors"; OBSERVED 2026-10-28: create through `fab import` succeeded.
- **No WebSocket source.** DOCUMENTED ([Learn](https://learn.microsoft.com/fabric/real-time-intelligence/user-flow-2)): the source list has HTTP, Kafka, MQTT, Pub/Sub, CDC…, no WebSocket; a WebSocket feed needs a collector sending to a custom endpoint.

## Source: Kraken spot WS v2 `trade` and REST `/0/public/Trades`

- **WS and REST describe the same trades.** OBSERVED 2026-10-28 (45 s on `BTC/USD`, `ETH/USD`, `DOGE/USD`): 145 trades in both had the same `trade_id`, price, qty, side (`buy`/`sell` vs `b`/`s`) and order type (`limit`/`market` vs `l`/`m`). REST rows are arrays `[price, volume, time, side, ord_type, misc, trade_id]` with price and volume as strings and time as epoch seconds (float).
- **`side` is the taker's.** DOCUMENTED ([Kraken](https://docs.kraken.com/api/docs/websocket-v2/trade)). Coinbase's `matches` uses the maker's side instead.
- **`trade_id` counts up by one per pair.** OBSERVED 2026-10-28: 1,000 consecutive REST trades on each of 4 pairs had no jump; `BTC/USD` starts at `trade_id` 1 (2013). Docs say only "unique per book", so treat contiguity as observed, not guaranteed.
- **Full history over REST.** OBSERVED 2026-10-28: `since=1` returns the first `BTC/USD` trades of 2013. `since` is epoch nanoseconds; `count` ≤ 1,000; the response's `last` is the next `since`. Rate limits not checked yet.
- **Pair names.** OBSERVED 2026-10-28: REST `Trades` accepts the WS v2 name (`pair=BTC/USD`) and returns it as the result key; `AssetPairs` still lists legacy names (`XXBTZUSD`, wsname `XBT/USD`, `XDG/USD`). Use WS v2 names everywhere.
- **Subscribe snapshot.** DOCUMENTED (same page): `snapshot: true` sends the last 50 trades per pair, `type: snapshot`; they can predate a REST window, so dedupe on (`symbol`, `trade_id`).
- **Status message lists maintenance.** OBSERVED 2026-10-28: the first `status` message carries `upcoming_maintenance` (spot REST and WS down ~15 min); expect a reconnect then.
- **A Fabric Python notebook holds the WS.** OBSERVED 2026-10-28 (`nb_check_kraken_ws`, 20 pairs, 6 min): one connection, no errors, longest silence 1.2 s (heartbeats), 2,947 trades, no `trade_id` jump on any pair, snapshot of 50 per pair. `websocket-client` is not preinstalled; the notebook installs it from code. Longer runs not tested yet.

## Fabric: notebooks

- **`spark.read.option("wholetext", True).text(path)` returned one line.** OBSERVED 2026-10-28 (`nb_build_dim_pair`, pretty-printed JSON in `Files/`): `.first()[0]` was `{`, so `json.loads` failed with "Expecting property name… line 1 column 2". Read a whole small file with `open("/lakehouse/default/Files/<path>")` instead.
- **A failed notebook run's `failureReason` carries no traceback.** OBSERVED 2026-10-28: only `System_Cancelled_Session_Statements_Failed`. Write the traceback to `Files/checks/` from a try/except (as `nb_check_*` do) to see it.
- **The SQL analytics endpoint lags new tables.** OBSERVED 2026-10-28: `gold.dim_pair` was `Invalid object name` right after creation; read the table through a notebook or `fab table schema` meanwhile.

- **A `%run` line in a `.py` notebook has no `# MAGIC` prefix.** OBSERVED 2026-10-29: `fab import --format .py` turned `# MAGIC %run nb_x` into the cell `%run nb_x%run nb_x`, so the job failed with "Cannot read notebook nb_x%run nb_x"; a plain `%run nb_x` line is stored and exported as written (only cell magics such as `%%configure` use `# MAGIC`). Find a hidden cell error by calling the notebook with `notebookutils.notebook.run` inside try/except from a throwaway notebook and writing the traceback to `Files/checks/`.
- **`%run` shares variables both ways.** DOCUMENTED ([Learn](https://learn.microsoft.com/fabric/data-engineering/author-execute-notebook)): all variables of the referenced notebook are available in the caller. A caller can set a flag before the `%run` and the referenced notebook can read it with `globals().get(...)` (`nb_test_load_silver` sets `library_only`).

## Fabric: pair metadata

- **`AssetPairs.wsname` is the legacy name, keys are older still.** OBSERVED 2026-10-28: keys `XXBTZUSD`, `XDGUSD`, `XETHZUSD`; `wsname` `XBT/USD`, `XDG/USD`, `ETH/USD`; `base`/`quote` carry `X`/`Z` prefixes (`XXBT`, `ZUSD`). Derive base and quote from the mapped `wsname`. All 20 tracked pairs are present; statuses seen: `online`, `cancel_only`, `post_only`.

## Fabric: data pipeline Copy (REST)

- **REST → JSON file copies the response as-is, with a BOM.** OBSERVED 2026-10-28 (`pl_ingest_bronze_asset_pairs`): no mapping needed, the file is the compact API response (`{"error":[],"result":{…}}`, ~680 KB) starting with `﻿`; `json.load` with `utf-8` fails ("Unexpected UTF-8 BOM"), use `utf-8-sig`.
- **A failed Trident notebook activity carries the notebook's exception.** OBSERVED 2026-10-28: `queryactivityruns` returns it in `error.message` (`errorName`, `errorValue`), unlike the notebook job's `failureReason`.
- **The schedule is part of the exported definition.** OBSERVED 2026-10-28: `fab export` of a pipeline yields a `.schedules` file (`type: Daily`, `times: ["00:15"]`, `localTimeZoneId: UTC`); `fab import` with it leaves exactly one schedule (it replaces the old one). Create the first one with `POST /workspaces/{ws}/items/{id}/jobs/Execute/schedules` (Fabric API `CreateDataPipelineExecuteSchedule`).
