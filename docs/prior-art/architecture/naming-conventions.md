# Naming conventions

Derived from the 22 items observed in `Polymarket-DEV` on 2026-10-25. Edit when the team states a rule.

## Items

| Item type | Pattern | Example |
| --- | --- | --- |
| Workspace | `<Project>-<STAGE>` | `Polymarket-DEV` |
| Lakehouse | `lh_<project>` | `lh_polymarket` |
| Eventhouse / KQL database | `eh_<project>` | `eh_polymarket` |
| Eventstream | `es_<project>_<source>` | `es_polymarket_ws` |
| Environment | `env_<project>` | `env_polymarket` |
| Variable library | `vl_<project>` | `vl_polymarket` |
| Notebook | `nb_<object>[_<stage>]` | `nb_session_silver`, `nb_probe_engine` |
| Data pipeline | `pl_<scope>_<purpose>` | `pl_daily_batch`, `pl_snapshot_gamma` |
| dbt scenario job | `dbt_<scope>_<layer>` | `dbt_scenario_gold` |
| Real-Time dashboard | `rtd_<project>_<purpose>` | `rtd_polymarket_tracer` |

The Lakehouse SQL endpoint and the KQL database take the name of their parent item.

## Not captured

Tables, columns, pipeline activities: no evidence recorded yet. Add when stated.

## Exceptions

`dbtJob_1` keeps its owner-created production name; the derived scenario adapter uses `dbt_scenario_gold` (2026-10-28).
