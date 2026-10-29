# Trades pipeline

Scope agreed 2026-10-28 (source switched from Polymarket to Kraken spot the same day). Why each choice: `reference/decisions/`.

## Flow

| Step | Item | Schedule |
|---|---|---|
| Hold a Kraken WS v2 `trade` subscription for the tracked pairs (no REST; gaps between runs accepted, ADR 0007), normalise, send to `es_trades` custom endpoint | `nb_collect_trades` (Python notebook) | 55-minute runs on a schedule (pause outside demos) |
| Hot path: `es_trades` Eventhouse destination → `eh_trades.trades_raw` (1 day) → update policy → `whale_trades` (notional ≥ `whale_threshold_usd`); `volume_5m` view; the tracked-pair rule (`dim_pair.is_tracked`) is applied when reading, by `tracked_whale_trades()` and the dashboard. Whale email by Activator. Only whale trades and 5-minute volume live here, no Gold KPI (ADR 0004) | `eh_trades`, `rtd_trades_live`, `act_whale_trades` | live |
| Land raw trades | `es_trades` Lakehouse destination → `lh_crypto.bronze.trades` | commits every ~2 min |
| Backfill a pair from a start time by REST `since` cursor into the same Bronze table | `nb_backfill_trades` via `pl_backfill_bronze_trades` | manual |
| Pair metadata: Copy (REST) lands `AssetPairs` in `Files/bronze/asset_pairs/`, then `nb_build_dim_pair` merges tracked pairs into `gold.dim_pair` | `pl_ingest_bronze_asset_pairs` | daily 00:15 UTC |
| Silver (dedupe on `symbol` + `trade_id`, quarantine, gap check) → `nb_load_silver`; Gold (`fact_trade`, `dim_pair`, `dim_date`) → `nb_load_gold`; MLV create and refresh → `nb_refresh_agg_pair_daily`. GX and hot vs Gold reconcile come later | `pl_process_gold` | hourly (Cron, every 60 min, UTC) |
| Serve | `sm_trades` (Direct Lake), `rpt_trades_overview` | |

## Testing

Spark logic is tested in Fabric, not locally (decided 2026-10-29, issue #9): a `nb_test_<name>` notebook `%run`s the notebook under test (the test sets `library_only = True` first, and the notebook's main cell skips itself) and runs each case on scratch tables in schema `test` of `lh_crypto`. Seam 2 (`nb_test_process_gold`, issue #10) runs the real `pl_process_gold` through the Fabric job API on scratch tables: the pipeline's table names are parameters whose defaults are the real tables. Pure-Python logic with no Spark (as `nb_build_dim_pair`'s mapping cell) stays unit-tested locally by `python -m unittest discover tests`.

## Tracked pairs

The 20 USD spot pairs with the largest 24 h notional on Kraken on 2026-10-28, stablecoins and fiat excluded, fixed in `vl_crypto`:
`BTC/USD`, `ETH/USD`, `XRP/USD`, `ZEC/USD`, `NEAR/USD`, `SOL/USD`, `HYPE/USD`, `SUI/USD`, `XMR/USD`, `QNT/USD`, `PEPE/USD`, `TAO/USD`, `ENA/USD`, `XLM/USD`, `LTC/USD`, `LINK/USD`, `ADA/USD`, `DASH/USD`, `DOT/USD`, `DOGE/USD`.
About 350,000 trades a day (~4/s) on that date.

## Bronze

`bronze.trades` keeps the WS v2 field names: `symbol`, `side`, `price`, `qty`, `ord_type`, `trade_id`, `timestamp` (ISO 8601 string), plus `_ingest_path` (`ws` or `backfill`) and `_load_ts` (ISO 8601 string). No casting in Bronze; Silver casts the time columns. Backfill converts REST rows to the same shape.

## Silver

`silver.trades` (`nb_load_silver`): key (`symbol`, `trade_id`), `trade_ts` and `_load_ts` cast to `TIMESTAMP`, `_bronze_version` = the Bronze commit that carried the first copy. Only tracked pairs. A row breaking a rule goes to `ops.quarantine_trades` with one rule ID, the first that fails: `null_key`, `non_positive_qty`, `non_positive_price` (null, zero, negative, NaN and Infinity all fail), `invalid_side`, `invalid_ord_type`, `invalid_timestamp` (a `timestamp` Silver can't cast). Each run reports missing `trade_id`s per pair (`gaps`); gaps never stop the load.

## Gold

Grain, keys and the double-counting rules of every table: `reference/architecture/gold-model.md`. `nb_load_gold` reads `silver.trades` by Change Data Feed commit version (watermark row `silver.trades` in `ops.etl_watermark`) and loads with insert-only `MERGE`s. `agg_pair_daily` is created and refreshed (optimal; `FULL` on Sundays) by `nb_refresh_agg_pair_daily`, not by the pipeline's Refresh Materialized Lake View activity (no service principal support, refreshes the whole lakehouse; [Learn](https://learn.microsoft.com/fabric/data-factory/refresh-materialized-lake-view-activity)).

- `fact_trade`: one trade, key (`symbol`, `trade_id`), with `notional` = qty × price; partitioned by `trade_date`.
- `dim_pair` (`symbol`, base, quote, status, tracked flag; one row per pair ever tracked, an untracked pair keeps its row with the flag off), `dim_date`.
- `agg_pair_daily` (MLV): pair × UTC day, with open, high, low, close, volume, notional, VWAP, trade count and taker-buy notional.
- Whale share of notional and taker buy/sell pressure are measures in `sm_trades`, never stored or summed.

## Business questions

- Hot path: R1 whale trade just happened. R2 5-minute volume chart.
- Gold: B1 which pairs draw money. B2 taker buy vs sell pressure and whale share of notional. B3 daily OHLC and VWAP per pair.

## Spikes before building

1. WS and REST describe the same trades. Passed locally 2026-10-28 (see `reference/lessons.md`).
2. A Python notebook in Fabric holds the WS connection for a whole run. Passed for 6 minutes on 2026-10-28 (`nb_check_kraken_ws`); the full run length and sending to `es_trades` are checked in #7.
3. CDF can be enabled on the Eventstream-written Bronze table. Passed 2026-10-28.
4. GX 1.x runs on Spark DataFrames in `env_crypto`.
5. KQL database reads `gold.dim_pair` through a OneLake shortcut. Partly failed 2026-10-29 (#13): a shortcut made through the Items API did not become an external table; `eh_trades` reads `gold.dim_pair` through an external table over the lakehouse path instead, without query acceleration (`reference/lessons.md`).

## Out of scope

Futures and other derivatives, order book (`book` channel), minute grid and movement/ranking/shock, trader identity (public feeds have none), dbt, Soda, Structured Streaming, oracle and scenario test kit, Azure outside Fabric, Dataflow Gen2, Warehouse, Mirroring, UDF, agents, ML, Ontology, Purview, Fabric Git integration (for now).
