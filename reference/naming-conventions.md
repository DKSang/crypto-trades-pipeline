# Naming conventions

Starter set accepted 2026-10-28; edit when the team states a rule.

## Items

| Item type | Pattern | Example |
| --- | --- | --- |
| Workspace | `<Project>-<STAGE>` | `Crypto-DEV` |
| Lakehouse | `lh_<layer or purpose>` | `lh_bronze`, `lh_config` |
| Warehouse | `wh_<purpose>` | `wh_gold` |
| Notebook | `nb_<verb>_<object>` | `nb_load_bronze` |
| Data pipeline | `pl_<verb>_<layer>_<source>` | `pl_ingest_bronze_websocket` |
| Dataflow Gen2 | `df_<verb>_<object>` | `df_clean_pairs` |
| Semantic model | `sm_<subject>` | `sm_trades` |
| Report | `rpt_<subject>` | `rpt_trades_overview` |
| Variable library | `vl_<scope>` | `vl_crypto` |
| Eventstream | `es_<subject>` | `es_trades` |
| Eventhouse / KQL database | `eh_<subject>` | `eh_trades` |
| Real-Time dashboard | `rtd_<subject>_<purpose>` | `rtd_trades_live` |
| Activator | `act_<subject>` | `act_whale_trades` |
| Environment | `env_<scope>` | `env_crypto` |
| Test notebook (runs on scratch tables in schema `test`) | `nb_test_<notebook under test minus nb_>` | `nb_test_load_silver` |

A test that runs a whole pipeline (seam 2) is named `nb_test_<pipeline minus pl_>`, e.g. `nb_test_process_gold` (used in issue #10, not yet confirmed by the owner). `pl_process_gold` has no source part either, for the same reason.

One lakehouse, `lh_crypto`, with schemas `bronze`, `silver`, `gold`, `ops` (accepted 2026-10-28).

## Pipeline activities

`<Verb> <object>` in title case, e.g. `Copy markets`, `Run nb_load_bronze`.

## Tables and columns

- Tables and columns in `snake_case`.
- Technical metadata columns start with `_` (e.g. `_load_ts`, `_source_file`).
- Exception: Bronze keeps source column names as the API returns them (e.g. `ord_type`, `trade_id` in `bronze.trades`; a camelCase source field stays camelCase); Silver renames to `snake_case` (accepted 2026-10-28).
- Bronze is raw and immutable: source names **and types** as sent, no casting. Time fields stay ISO 8601 strings (also our own `_load_ts`); Silver casts them (accepted 2026-10-28).
- Semantic model objects (tables, columns, measures) use readable names with spaces and no `fact`/`dim` prefix, e.g. `Trade`, `Pair Daily`, `Taker Buy Share` (accepted 2026-10-29, issue #15, following the semantic-model-authoring skill; overrides the snake_case bullet above for semantic model objects only: the `sm_` item name and the Lakehouse tables keep snake_case). Hidden key and base columns are named so they never equal a measure: `Daily Notional` under the measure `Notional`.
- `nb_test_sm_trades` tests a semantic model, not a notebook or pipeline: it is read-only on the real Gold tables and queries the deployed model (issue #15; same `nb_test_` prefix, different pattern from the scratch-table tests above).
