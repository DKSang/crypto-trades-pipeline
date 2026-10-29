# Fabric environment

## Tenant

- **Tenant ID**: `370fa88c-de52-4400-b6cd-027ef6a8ddf2`
- **Agent identity**: service principal, app ID `b593a632-afcd-4c1b-b0bd-43080c0317ab`, principal ID `a93074e9-54c0-4ced-b846-143d8a51162c`. Secret in `.env` (`sp_secret`), never written here. `fab` and `az` both sign in as it via `scripts/fabric-login.sh`.

## Scope

| Workspace | ID | Stage | Agent may write? |
| --- | --- | --- | --- |
| Crypto-DEV | `c8d6538e-e60c-467b-8440-dec5dae4d897` | DEV | yes, without asking first; report what changed |

`Crypto-DEV` was renamed from `Polymarket-DEV` on 2026-10-28 (same ID).

### Key items in Crypto-DEV

| Item | ID | Notes |
| --- | --- | --- |
| `lh_crypto` (Lakehouse, schemas on, recreated 2026-10-28) | `039e4fc7-5892-4238-9c2f-a260cb73fc9f` | schemas `bronze`, `silver`, `gold`, `ops`; `bronze.trades` pinned to Delta protocol 1/4 with CDF |
| `es_trades` (Eventstream, recreated 2026-10-28) | `d13aace9-0268-4a87-b579-f21c91cdd3fa` | custom endpoint `TradesCollector` → `bronze.trades` |
| `nb_collect_trades` (Python notebook) | | Kraken WS collector (ADR 0007); stats in `Files/checks/nb_collect_trades.json` |
| `nb_setup_lakehouse`, `nb_check_bronze_trades` (Notebooks) | | idempotent setup; read-only Bronze checks, results in `Files/checks/` |
| `vl_crypto` (Variable library) | `4c6d103a-b8bf-461d-9b61-d73823babf1d` | `tracked_pairs` (String, comma-separated WS v2 names), `whale_threshold_usd` (Integer, 100000 confirmed by the owner 2026-10-29); no value sets, DEV only |
| `nb_build_dim_pair` (Notebook), `nb_check_dim_pair` | | newest `Files/bronze/asset_pairs/*.json` snapshot → `gold.dim_pair` (MERGE); read-only check, result in `Files/checks/nb_check_dim_pair.json`. Logic unit-tested by `python -m unittest discover tests` |
| `pl_ingest_bronze_asset_pairs` (Data pipeline) | `8db3d36b-fa68-4e37-a3af-94923d77b1b5` | Copy REST `0/public/AssetPairs` → `Files/bronze/asset_pairs/asset_pairs_<UTC ts>.json`, then `nb_build_dim_pair`; daily 00:15 UTC (schedule is in `.schedules`) |
| `nb_load_silver` (Notebook), `nb_test_load_silver`, `nb_check_silver` | | Bronze → `silver.trades` by Change Data Feed commit version; watermark in `ops.etl_watermark`, bad rows in `ops.quarantine_trades`, run summary in `Files/checks/nb_load_silver.json`. The test notebook sets `library_only = True`, `%run`s it and runs each case on scratch tables in schema `test` (7 cases passed 2026-10-29); result in `Files/checks/nb_test_load_silver.json`. `nb_check_silver` is the read-only check of the real tables |
| `nb_load_gold` (Notebook), `nb_refresh_agg_pair_daily`, `nb_test_load_gold`, `nb_test_process_gold` | | Silver → `gold.fact_trade`, `gold.dim_pair` (missing pairs only), `gold.dim_date` by CDF commit version (watermark row `silver.trades`); `nb_refresh_agg_pair_daily` creates and refreshes the MLV `gold.agg_pair_daily` (optimal; `FULL` on the first run of each Sunday UTC). `nb_test_refresh_agg_pair_daily` (3.6 M synthetic rows, partitioned vs unpartitioned `fact_trade`) is the experiment behind the `trade_date` partition. Seam 1 (`nb_test_load_gold`, 8 cases) and seam 2 (`nb_test_process_gold`, runs `pl_process_gold` on scratch tables `test.it_*`) write to `Files/checks/` |
| `pl_process_gold` (Data pipeline) | `2375f8bf-29e3-4f23-879a-e9e84aa7419b` | hourly: `nb_load_silver` → `nb_load_gold` → `nb_refresh_agg_pair_daily`; parameters = table names (defaults are the real tables) and `fail_gold_after_write` |
| `Kraken REST` (tenant Connection, not in a workspace) | `32dc1617-c9f4-43a7-a694-f896c06e446a` | `RestService`, `https://api.kraken.com`, Anonymous; created 2026-10-28 with the user's explicit OK |
| `nb_check_kraken_ws`, `nb_check_es_types` (Python notebooks) | | spikes: WS hold test, Eventstream type-conversion probe (`PROBE/*` rows, see `reference/lessons.md`) |

The service principal can see (and may be able to write to) `Polymarket-old-DEV`; only the rule above keeps the agent out of it.

Every workspace not listed here is out of scope: read only if the task needs it, never modify.
