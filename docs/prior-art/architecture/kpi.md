# KPI contract

**Status:** target contract (restart grilling, 2026-10-24). The Python oracle (package `fabric/testkit/oracle/`: Silver, KPI and live modules) defines the reference behavior. Scenario runs use `nb_session_silver` and Gold SQL in the separate `dbt_scenario_gold` item ([ADR 0013](../adr/0013-hybrid-silver-notebook-gold-sql.md)); production retains `nb_session_gold` until cutover. `loop check` compares every table of a scenario run with the oracle on Fabric (OBSERVED 2026-10-25; historical evidence removed 2026-10-28; see git history; cases in [scenarios.md](scenarios.md)). KQL live functions must follow this contract too; remaining work is listed below. Axis and coverage rationale: [ADR 0003](../adr/0003-time-coverage-and-kpi.md).

## Gold model (batch)

| Table | Grain / PK | Notes |
|---|---|---|
| `fact_quote` | `capture_id` | Deduped Silver quote entry: bid, ask, quote state, midpoint, spread, `received_at` |
| `fact_trade_observation` | `capture_id` | Received `last_trade_price`: price, size (nullable), side, `source_ts`, `received_at` |
| `fact_outcome_minute` | session × asset × UTC minute | Rectangular grid: every asset seen in session quotes or trades × every minute from floor(first connected start) to floor(last connected end), inclusive. `midpoint`, `spread_pp`, `covered`; for W=5/15/60: `status_{W}m`, `reason_{W}m`, `movement_{W}m_pp`, `rank_{W}m`; `n_trade_obs`, `n_with_size`, `sum_size_known`. Probe time is the minute start. |
| `fact_coverage_segment` | session × `segment_id` | connected/gap intervals from envelopes; latest `EventProcessedUtcTime` wins on redelivery |
| `dim_outcome_asset` → `dim_market` | (`metadata_version_id`, `asset_id`) → (`metadata_version_id`, `condition_id`) | outcome label, market, event; restricted to the manifest's pinned metadata version |
| `dim_session` | `session_id` | metadata version, criteria version, watchlist hash, start/end, stop reason, status, collector counts, package version; no manifest means no Gold rows; Silver still includes the session |
| shock episodes | derived view over `fact_outcome_minute` | not stored as a fact |

Rules against double counting:
- Dedupe on `capture_id` only, never on payload.
- Never sum outcomes into a market-level volume.
- Activity counts only minutes where `covered` is true. All three activity columns are NULL in uncovered minutes; covered minutes with no trades have counts 0, 0 and NULL size sum.

## Rules

| Rule | Behaviour |
|---|---|
| Time axis | `received_at` UTC. One-minute UTC grid; windows W ∈ {5, 15, 60} minutes. |
| Capture dedupe | Latest ingestion per `capture_id` within a new batch; insert-only MERGE keeps the first copy across runs. KEY-02 is dropped. Reset rebuilds from one snapshot, so conflicting cross-run payloads can differ after rebuild (ADR 0008). No source-event dedupe. |
| Quote state | Missing either side → `missing_side`; bid > ask → `crossed`; otherwise `valid`. |
| Midpoint | `(bid + ask) / 2` only when `valid`; otherwise null. |
| Spread | `(ask − bid) × 100` pp when both sides are present (negative if crossed); otherwise null. |
| Book extraction | Max bid / min ask, regardless of array position. |
| Coverage | From connected/gap envelopes only. A window `[t−W, t]` is eligible only if it lies inside one connected segment of the same session. Nothing is filled across gaps or sessions. |
| Price as-of | Take the latest quote at or before the probe within the segment, then validate it. An invalid latest quote blocks the result; do not fall back to an older valid quote. |
| Same-time tie | Order by `received_at`, then numeric `frame_seq`, then numeric `entry_idx` parsed from `capture_id`. Malformed capture IDs follow numeric IDs, ordered by `capture_id`; the last quote wins. Asset IDs remain strings. |
| Movement | `100 × (mid(t) − mid(t−W))`, signed pp, endpoint difference. |
| Missing movement | `insufficient_data` with null movement and rank. Reason precedence: `no_coverage` (no connected segment contains t; bounds inclusive), then `gap_in_window` (a segment contains t, but none contains the whole window), then `no_valid_quote` (no quote, or an invalid one, at the baseline or at t). Point containment is separate from full-minute `covered`: movement can be eligible at a connected segment's closing boundary while activity is NULL. |
| Ranking | Descending absolute movement among `status = ok`; ties ordered by ascending asset ID string; sequential ranks, NULL otherwise. Batch grid universe: every asset seen in session quotes or trades, including before its first quote. Live/oracle `movers_at` universe remains assets quoted by t. |
| Health | Stale if there is no connected segment, no heartbeat by the probe, or the latest heartbeat is more than 20 s old (exactly 20 s is healthy). Connection health and quote state are separate. |
| Live open segment | KQL only: while the latest heartbeat is ≤20 s old and no closing envelope exists, the open segment ends at that heartbeat. Batch never infers coverage. |
| Shock | A flag on a minute where the 5-minute movement is eligible and its absolute value is ≥ θ (default 5.0 pp). Consecutive flagged minutes for the same session × asset form an episode (gaps-and-islands), even if the sign changes. Peak = largest absolute movement, earliest wins ties; direction comes from the peak sign. **No `pending` state.** |
| Activity | `covered` means `[m, m+1min]` lies wholly in one connected segment (inclusive bounds). Per covered grid minute, trades are selected on `received_at` in `[m, m+1)`: `n_trade_obs`, `n_with_size`, `sum_size_known` (missing size is never treated as 0; NULL when no size is known). Uncovered minutes have all three values NULL. Covered minutes with no trades have 0, 0, NULL. Windows and sessions sum minutes. Rate = Σ `n_trade_obs` / number of covered minutes, computed at query time. Label: `no_trades` if `n_trade_obs` = 0; `unknown` if `n_with_size` = 0; `partial` if `n_with_size` < `n_trade_obs`; otherwise `complete`. |
| Referential integrity | Every quote/trade asset must be in the session's pinned watchlist; otherwise the row is quarantined and kept out of Gold facts ([ADR 0012](../adr/0012-row-level-gate.md)). |
| Numeric type | `decimal(18,6)`. Oracle comparisons use tolerance 1e-6. |

## Event-time evidence (not a KPI)

For trade observations, report `source_ts` vs `received_at`: the delay distribution (p50/p95/max) and the count of late or regressed timestamps. This is evidence only; it does not change the axis.

## Implementation notes and open items

Done in the oracle: three reasons, no shock `pending`, three activity counts, the row rules of [ADR 0012](../adr/0012-row-level-gate.md) (`dedup_dropped` is a metric, not a rule).

- The production pipeline still implements [ADR 0008](../adr/0008-session-batch-arrival-watermark.md) with two notebooks: `nb_session_silver` consumes Bronze CDF by commit timestamp, then `nb_session_gold` reads Silver `process_timestamp >=` the previous successful Gold start. `ops.etl_watermark` advances per layer only on success. Silver includes every session; Gold requires a manifest. Late manifests with Silver and no `dim_session` self-heal, including row facts, without new Bronze.
- Gold row facts and pinned dimensions merge insert-only; the time-series tables rebuild the SET of changed sessions from WHOLE Silver, one `replaceWhere session_id IN (...)` per table. As-of quotes use the last quote per asset/minute forward-filled as a struct (invalid latest quotes block fallback), and movement uses LAG on the same grid plus base/current segment membership. Coverage uses only changed sessions' Bronze gap frames through a plain filtered read, not CDF. Shocks remain derived from persisted `fact_outcome_minute`.
- No Gold watermark row means a full Gold rebuild; reset both layer rows for a full Bronze rebuild or Silver logic changes. Silver and Gold must never overlap, and multiple pipeline runs must be serialized by the operator (`fabric/README.md`). Keep Bronze CDF enabled and Delta logs longer than the longest run pause.
- Historical scenario runs recorded oracle parity per table, late arrival, rerun, late manifest, rebuild and gate outcomes (OBSERVED, dated per file; removed 2026-10-28; see git history). Earlier per-session and wheel-era runs remain in `docs/evidence/session-batch/`. Current verification uses layer tests while iterating and one full play before merge ([Test loop](../environment.md#test-loop)); Gold rebuild and `same` play steps are not ported.
- KQL live functions still return the older six reasons and the v1 activity fields until the KQL cleanup ticket; their comparator key is `fabric/tests/fixtures/kpi/kql_answer_key_v1.json`, tied to this contract's key by `fabric/tests/test_kpi_answer_keys.py` (one known defect: M14, a probe inside a gap, is `window_not_in_one_connected_segment` in KQL instead of `no_coverage`).
- The old Silver notebooks, HWM/ledger/guard helpers and four warm pipelines are removed from the repo and the workspace ([environment](../environment.md), "Deleted items"). Session Silver is `fabric/Polymarket-DEV/nb_session_silver.Notebook` with the row rules of [ADR 0006](../adr/0006-dq-gate.md). The warm SQL parse, the `rules.json` registry and KQL `HotQuality*` are deleted (architecture review 2026-10-25).
- Referential integrity (asset in the pinned watchlist) is a row rule in the oracle and the scenario dbt model `quarantine_ri_01` ([ADR 0012](../adr/0012-row-level-gate.md)); scenario Soda checks only warn. The production Gold notebook retains the old blocking gate until cutover.

## Unverified

- Size/currency units (totals are observed values, not market-wide volume).
- Agreement with the source REST API (F-05).
- Real crossed, stale and partial-size cases (F-11).
