# Trades pipeline

Scope agreed in the 2026-10-28 grilling. Why each choice: `reference/decisions/0001`–`0004`.

## Flow

| Step | Item | Schedule |
|---|---|---|
| Poll Data API `/trades?limit=1000` (taker-only, all markets) | `es_trades` HTTP source | every 15 s, always on (pause outside demos) |
| Hot path: whale trades, 5-minute volume per tracked market | `eh_trades`, `rtd_trades_live`, Activator | live |
| Land raw trades | `es_trades` Lakehouse destination → `lh_polymarket.bronze` | file every 15 min |
| Backfill by `start`/`end` window into the same Bronze table | `nb_backfill_trades` via `pl_backfill_bronze_trades` | manual |
| Gamma snapshot, model-name filter (`Claude`, `GPT`/`ChatGPT`, `Gemini`) → `dim_market` | `pl_ingest_bronze_gamma` (Copy, REST) | daily 00:15 UTC |
| Silver (dedupe on `trade_key`, quarantine), GX, Gold, MLV refresh, hot vs Gold reconcile | `pl_process_gold` | hourly |
| Serve | `sm_trades` (Direct Lake), `rpt_trades_overview` | |

## Gold

- `fact_trade`: one taker trade, key `trade_key`.
- `dim_trader`, `dim_market` (with `tracked_from`/`tracked_to`), `dim_outcome`, `dim_date`.
- `agg_outcome_daily` (MLV): outcome × UTC day, with volume, trade count, VWAP and last price.
- Distinct traders and top-10 trader share are measures in `sm_trades`, never stored or summed.

## Business questions

- Hot path: R1 whale trade just happened. R2 5-minute volume chart.
- Gold: B1 which markets draw money. B2 whale concentration. B3 daily price per outcome.

## Spikes before building

1. HTTP source splits the JSON array into one event per trade (fallback: notebook collector to a custom endpoint, or RTDS).
2. CDF can be enabled on the Eventstream-written Bronze table.
3. GX 1.x runs on Spark DataFrames in `env_polymarket`.
4. KQL database reads `gold.dim_market` through a OneLake shortcut.

Unverified: `size` units (notional assumed size × price, USDC).

## Out of scope

Order book and WebSocket market channel, minute grid and movement/ranking/shock, dbt, Soda, Structured Streaming, oracle and scenario test kit, Azure outside Fabric, Dataflow Gen2, Warehouse, Mirroring, UDF, agents, ML, Ontology, Purview, Fabric Git integration (for now).
