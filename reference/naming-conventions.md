# Naming conventions

Starter set accepted 2026-10-28; edit when the team states a rule.

## Items

| Item type | Pattern | Example |
| --- | --- | --- |
| Workspace | `<Project>-<STAGE>` | `Polymarket-DEV` |
| Lakehouse | `lh_<layer or purpose>` | `lh_bronze`, `lh_config` |
| Warehouse | `wh_<purpose>` | `wh_gold` |
| Notebook | `nb_<verb>_<object>` | `nb_load_bronze` |
| Data pipeline | `pl_<verb>_<layer>_<source>` | `pl_ingest_bronze_websocket` |
| Dataflow Gen2 | `df_<verb>_<object>` | `df_clean_markets` |
| Semantic model | `sm_<subject>` | `sm_markets` |
| Report | `rpt_<subject>` | `rpt_markets_overview` |
| Variable library | `vl_<scope>` | `vl_polymarket` |

## Pipeline activities

`<Verb> <object>` in title case, e.g. `Copy markets`, `Run nb_load_bronze`.

## Tables and columns

- Tables and columns in `snake_case`.
- Technical metadata columns start with `_` (e.g. `_load_ts`, `_source_file`).
