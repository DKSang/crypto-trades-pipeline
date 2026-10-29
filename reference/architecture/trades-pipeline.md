# Trades pipeline

Scope agreed 2026-10-28 (source switched from Polymarket to Kraken spot the same day). Why each choice: `reference/decisions/`.

## Flow

| Step | Item | Schedule |
|---|---|---|
| Hold a Kraken WS v2 `trade` subscription for the tracked pairs (no REST; gaps between runs accepted, ADR 0007), normalise, send to `es_trades` custom endpoint | `nb_collect_trades` (Python notebook) | 55-minute runs on a schedule (pause outside demos) |
| Hot path: `es_trades` Eventhouse destination → `eh_trades.trades_raw` (1 day) → update policy → `whale_trades` (notional ≥ `whale_threshold_usd`); `volume_5m` view; the tracked-pair rule (`dim_pair.is_tracked`) is applied when reading, by `tracked_whale_trades()` and the dashboard. Whale email by Activator. Only whale trades, 5-minute volume and the daily event totals `hot_daily` (for the reconcile) live here, no Gold KPI (ADR 0004) | `eh_trades`, `rtd_trades_live`, `act_whale_trades` | live |
| Land raw trades | `es_trades` Lakehouse destination → `lh_crypto.bronze.trades` | commits every ~2 min |
| Backfill a pair from a start time by REST `since` cursor into the same Bronze table | `nb_backfill_trades` via `pl_backfill_bronze_trades` | manual |
| Pair metadata: Copy (REST) lands `AssetPairs` in `Files/bronze/asset_pairs/`, then `nb_build_dim_pair` merges tracked pairs into `gold.dim_pair` | `pl_ingest_bronze_asset_pairs` | daily 00:15 UTC |
| Silver (dedupe on `symbol` + `trade_id`, quarantine, gap check) → `nb_load_silver`; quality gate on the Silver batch → `nb_check_silver_batch` (GX, `env_crypto`); Gold (`fact_trade`, `dim_pair`, `dim_date`) → `nb_load_gold`; MLV create and refresh → `nb_refresh_agg_pair_daily`; daily reconcile of the hot path against Gold → `nb_reconcile_hot_gold` (last step, writes `ops.reconcile_daily` only) | `pl_process_gold` | hourly (Cron, every 60 min, UTC) |
| Alerts: no trade for 10 minutes (`es_trades` → count per minute → Activator), error-level DQ check failed (`nb_check_silver_batch` → `es_dq_alerts` → Activator) | `act_pipeline_alerts` | live |
| Serve: Direct Lake model on the four Gold tables (relationships on the foreign keys of `gold-model.md`; ratios and roll-ups are measures), and a three-page report, one page per batch question B1-B3, each showing when the data was last refreshed (newest trade in Gold). Tested by `nb_test_sm_trades` | `sm_trades` (Direct Lake), `rpt_trades_overview` | none of its own: Direct Lake frames a table when its Delta data changes (DOCUMENTED, [Learn](https://learn.microsoft.com/fabric/fundamentals/direct-lake-how-it-works#automatic-updates); the setting on `sm_trades` was not read) |

## Testing

Spark logic is tested in Fabric, not locally (decided 2026-10-29, issue #9): a `nb_test_<name>` notebook `%run`s the notebook under test (the test sets `library_only = True` first, and the notebook's main cell skips itself) and runs each case on scratch tables in schema `test` of `lh_crypto`. Seam 2 (`nb_test_process_gold`, issue #10) runs the real `pl_process_gold` through the Fabric job API on scratch tables: the pipeline's table names are parameters whose defaults are the real tables. Pure-Python logic with no Spark (as `nb_build_dim_pair`'s mapping cell) stays unit-tested locally by `python -m unittest discover tests`.

## Tracked pairs

The 20 USD spot pairs with the largest 24 h notional on Kraken on 2026-10-28, stablecoins and fiat excluded, fixed in `vl_crypto`:
`BTC/USD`, `ETH/USD`, `XRP/USD`, `ZEC/USD`, `NEAR/USD`, `SOL/USD`, `HYPE/USD`, `SUI/USD`, `XMR/USD`, `QNT/USD`, `PEPE/USD`, `TAO/USD`, `ENA/USD`, `XLM/USD`, `LTC/USD`, `LINK/USD`, `ADA/USD`, `DASH/USD`, `DOT/USD`, `DOGE/USD`.
About 350,000 trades a day (~4/s) on that date.

## Bronze

`bronze.trades` keeps the WS v2 field names: `symbol`, `side`, `price`, `qty`, `ord_type`, `trade_id`, `timestamp` (ISO 8601 string), plus `_ingest_path` (`ws` or `backfill`; a test sender such as `nb_test_whale_alert` uses `test`) and `_load_ts` (ISO 8601 string). No casting in Bronze; Silver casts the time columns. Backfill converts REST rows to the same shape; it only appends, and a re-run adds duplicate Bronze rows that Silver drops. It refuses a symbol that is not a tracked pair, since Silver would drop all of it.

## Silver

`silver.trades` (`nb_load_silver`): key (`symbol`, `trade_id`), `trade_ts` and `_load_ts` cast to `TIMESTAMP`, `_bronze_version` = the Bronze commit that carried the first copy. Only tracked pairs. A row breaking a rule goes to `ops.quarantine_trades` with one rule ID, the first that fails: `null_key`, `non_positive_qty`, `non_positive_price` (null, zero, negative, NaN and Infinity all fail), `invalid_side`, `invalid_ord_type`, `invalid_timestamp` (a `timestamp` Silver can't cast). Each run reports missing `trade_id`s per pair (`gaps`); gaps never stop the load.

## Quality gate

`nb_check_silver_batch` runs in `pl_process_gold` between Silver and Gold. The batch is what Silver committed after Gold's watermark (the same Change Data Feed range `nb_load_gold` reads). Table-level Great Expectations checks (`env_crypto`, spike 4) run on it; every result goes to `ops.dq_result` (`run_id`, `checked_ts`, `table_name`, `from_version`, `to_version`, `check_id`, `severity`, `passed`, `unexpected_count`, `details`). An empty batch checks and records nothing.

| `check_id` | Severity | Passes when |
|---|---|---|
| `schema` | error | the batch has exactly Silver's columns, in order, with Silver's types |
| `key_unique` | error | (`symbol`, `trade_id`) is unique in Silver among the batch's keys, old rows included |
| `ref_integrity_dim_pair` | error | every `symbol` of the batch has a row in `gold.dim_pair` |
| `row_count` | warn | the batch is at most 3 × the average daily row count of Silver's last 7 days before it (skipped with fewer than 2 days) |
| `freshness` | warn | the newest `_load_ts` in the batch is under 3 hours old |
| `trade_id_contiguity` | warn | no missing `trade_id` between a batch pair's first and last in Silver |

An error makes the notebook raise, which skips the Gold activities of the pipeline and so holds Gold's watermark; it also sends one event per failed error check to `es_dq_alerts`. A warn is only recorded. The volume, age and contiguity checks are warns because a backfill or a collector pause legitimately breaks their baselines and none of them makes a row wrong (ADR 0007). The gate never writes Gold or moves a watermark. After the cause is fixed (for example a duplicate removed from Silver) the next run reads the same range again; Gold's own dedupe makes the earlier insert in the Change Data Feed harmless.

`gold.dim_pair` must hold every tracked pair before the first Gold run: a new tracked pair blocks Gold until `pl_ingest_bronze_asset_pairs` (or `nb_build_dim_pair`) has added it. The gate does not run on an empty batch (no new Silver commit), so a stalled Silver is not visible in `ops.dq_result`; the "no trade for 10 minutes" alert watches the events `es_trades` receives from the collector, which is the same stream Bronze is fed from.

## Reconcile

`nb_reconcile_hot_gold` is the last activity of `pl_process_gold`. For every tracked pair and UTC day it compares what the hot path counted (`eh_trades.hot_daily`: every event `trades_raw` received, duplicates included, with its own 30-day retention because `trades_raw` keeps one day) with `gold.fact_trade`, and writes one row per (`trade_date`, `symbol`) to `ops.reconcile_daily` with a `MERGE`, so a rerun replaces the row. It reads yesterday and today by default (`from_date` parameter, pipeline parameter `reconcile_from_date`, for an older day, for example after a backfill). A day the hot path has no total for gets no row. It changes no Gold number.

Let R be the Bronze rows whose `_ingest_path` is not `backfill` (what was sent live, by the collector or a test sender such as `nb_test_whale_alert`, so what the hot path got too), U their distinct (`symbol`, `trade_id`) keys and G the keys in `fact_trade`. Every measure comes as `_trades` and `_notional`:

| Column | Meaning |
|---|---|
| `hot`, `gold` | totals of the two paths |
| `gap` | `hot` − `gold` (the reconcile gap; positive: the hot path counted more) |
| `duplicates` | R − U: events sent more than once (snapshot after a reconnect, overlapping runs), removed by the Silver dedupe |
| `late` | keys of U not in G: live trades Gold does not have yet (arrived after the last Gold load, held by the quality gate, or rejected by Silver: quarantined, or a test event) |
| `backfill` | keys of G not in U: trades the live stream never delivered, added by `nb_backfill_trades` |
| `unexplained` | `hot` − R: the hot path and Bronze got a different number of events |

`gap` = `duplicates` + `late` − `backfill` + `unexplained`, in count and in notional. A trade the collector missed (a **Gap** of `trade_id`s, ADR 0007) and nothing backfilled is in neither path, so it is not in the reconcile gap; backfilling it is what makes Gold exceed the hot path, and that shows as `backfill`. Today's row is a day in progress: both paths move, so `late` is normally above 0 until the next Gold load.

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
4. GX 1.x runs on Spark DataFrames in `env_crypto`. Passed 2026-10-29 (`nb_check_gx_spark`, see `reference/lessons.md`).
5. KQL database reads `gold.dim_pair` through a OneLake shortcut. Partly failed 2026-10-29 (#13): a shortcut made through the Items API did not become an external table; `eh_trades` reads `gold.dim_pair` through an external table over the lakehouse path instead, without query acceleration (`reference/lessons.md`).

## Out of scope

Futures and other derivatives, order book (`book` channel), minute grid and movement/ranking/shock, trader identity (public feeds have none), dbt, Soda, Structured Streaming, oracle and scenario test kit, Azure outside Fabric, Dataflow Gen2, Warehouse, Mirroring, UDF, agents, ML, Ontology, Purview, Fabric Git integration (for now).
