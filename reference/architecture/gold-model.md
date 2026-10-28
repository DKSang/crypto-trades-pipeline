# Gold model

Star schema in `lh_crypto.gold`, built hourly by `pl_process_gold` (`nb_load_silver` → `nb_load_gold` → `nb_refresh_agg_pair_daily`). Words are the glossary's (`reference/glossary.md`). Delta tables don't enforce keys: each key below holds because of how the table is written, and the DEV tests check it.

```
dim_date (date) ──┐
                  ├── fact_trade (symbol, trade_id) ── agg_pair_daily (symbol, trade_date)
dim_pair (symbol) ┘        the fact and the aggregate both point to dim_pair and dim_date
```

## Tables

| Table | Grain (one row is…) | Primary key | Foreign keys |
|---|---|---|---|
| `fact_trade` | one trade on one pair | (`symbol`, `trade_id`) | `symbol` → `dim_pair.symbol`; `trade_date` → `dim_date.date` |
| `dim_pair` | one pair ever tracked (an untracked pair keeps its row, `is_tracked` off) | `symbol` | none |
| `dim_date` | one UTC calendar day | `date` | none |
| `agg_pair_daily` (Materialized Lake View) | one pair on one UTC day | (`symbol`, `trade_date`) | `symbol` → `dim_pair.symbol`; `trade_date` → `dim_date.date` |

- **`fact_trade`** columns: `symbol`, `trade_id`, `trade_ts` (UTC), `trade_date` (the UTC day of `trade_ts`), `side` (the taker's), `ord_type`, `price`, `qty`, `notional` (= `qty` × `price`, USD), `_silver_version` (the Silver commit that carried the trade). Has Change Data Feed on.
- **`dim_pair`** columns: `symbol` (WS v2 name), `base`, `quote`, `status`, `is_tracked`. `nb_build_dim_pair` owns it (daily, from `AssetPairs`); `nb_load_gold` only inserts a row, with `status` empty, for a pair that has trades and no row yet, so a fact never points at nothing.
- **`dim_date`** columns: `date`, `year`, `quarter`, `month`, `day`, `day_of_week` (ISO, Monday = 1), `day_name`, `is_weekend`. `nb_load_gold` inserts every day from the first to the last day of each batch, including days without trades.
- **`agg_pair_daily`** measures: `open` and `close` (price of the first and last trade of the day by `trade_ts`, then `trade_id`), `high`, `low`, `volume` (sum of `qty`, in base units), `notional`, `vwap` (`notional` / `volume`), `trade_count`, `taker_buy_notional` (notional of trades whose taker side is `buy`; taker sell notional is `notional` − `taker_buy_notional`).

## How double counting is avoided

1. **One row per trade, everywhere.** The subscribe snapshot after a reconnect, overlapping runs, backfills, gap fills and the rerun of a failed run all deliver trades that are already loaded. Silver dedupes on (`symbol`, `trade_id`) inside the batch and does an insert-only `MERGE` on that key; `nb_load_gold` repeats both on its own batch, so even a duplicate inside Silver could not reach `fact_trade`. A repeat is dropped, never added or updated.
2. **Aggregates are recomputed, not accumulated.** `agg_pair_daily` is a `SELECT … GROUP BY symbol, trade_date` over the deduplicated `fact_trade`, refreshed in full. A rerun or a backfill changes the fact first and the aggregate follows; nothing is ever added to an old total. A late trade changes the row of its own day only.
3. **Watermarks make reruns safe.** A step moves its watermark after its writes (ADR 0002). A failure in between rereads the same range, and the insert-only `MERGE`s add nothing the first attempt already added. Each step has its own watermark (`bronze.trades` for Silver, `silver.trades` for Gold): when Gold fails, Silver's watermark has already moved because Silver's output is saved, and only Gold's holds.
4. **Additive and non-additive values are kept apart.** `volume`, `notional`, `trade_count` and `taker_buy_notional` add up across days (and `notional`, in USD, across pairs; `volume` is in base units, so only within a pair). `open`, `close`, `high`, `low` and `vwap` do not: they stay as stored measures and are rolled up by rule (first open, last close, max of highs, min of lows, `vwap` = Σ `notional` / Σ `volume`), never summed or averaged. Ratios such as the whale share of notional or taker buy/sell pressure are measures in `sm_trades`, never stored.
5. **The day is UTC.** `trade_date` is cut from the UTC `trade_ts`, so a trade cannot fall on two days.

## Where it is tested

`nb_test_load_gold` (seam 1: the load and the MLV `SELECT`, run with `spark.sql` on scratch tables) and `nb_test_process_gold` (seam 2: `pl_process_gold` itself, run on scratch tables through its table-name parameters). Results: `Files/checks/nb_test_load_gold.json`, `Files/checks/nb_test_process_gold.json`.
