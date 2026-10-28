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
| `vl_crypto` (Variable library) | `4c6d103a-b8bf-461d-9b61-d73823babf1d` | `tracked_pairs` (String, comma-separated WS v2 names), `whale_threshold_usd` (Integer, 100000 assumed); no value sets, DEV only |
| `nb_build_dim_pair` (Notebook), `nb_check_dim_pair` | | newest `Files/bronze/asset_pairs/*.json` snapshot → `gold.dim_pair` (MERGE); read-only check, result in `Files/checks/nb_check_dim_pair.json`. Logic unit-tested by `python -m unittest discover tests` |
| `nb_check_kraken_ws`, `nb_check_es_types` (Python notebooks) | | spikes: WS hold test, Eventstream type-conversion probe (`PROBE/*` rows, see `reference/lessons.md`) |

The service principal can see (and may be able to write to) `Polymarket-old-DEV`; only the rule above keeps the agent out of it.

Every workspace not listed here is out of scope: read only if the task needs it, never modify.
