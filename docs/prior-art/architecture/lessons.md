# Lessons from recorded evidence

OBSERVED rows below refer to historical evidence, not reruns. ENGINE means a recorded engine incident or owner-reported engine result; DOC means the authoring guide's documentation claim. GEN/UNVERIFIED rules from that guide remain unverified. [gsd-final:.planning/phases/04-build-transform-kpi/04-FABRIC-GUIDE.md §Purpose/Labels]

## Source and transport

| Trap | Lesson / certainty / provenance |
|---|---|
| AI tag treated as the product universe | OBSERVED 2026-10-21/22: tag is too broad; 1,675 ambiguous “AI Lab” markets remain an exclusion blind spot. Preserve reason counts and review scope separately from transport coverage. [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.6]; `docs/evidence/ingest/CLOUD-LOG.md` |
| Open event implies open nested markets | OBSERVED 2026-10-21: nested closed/inactive markets; validate each market and recheck at session start. `docs/evidence/source/gamma_profile.json`; [gsd-final:fabric/gamma_logic/selector.py _market_valid] |
| JSON-string arrays / numeric asset coercion | OBSERVED 2026-10-21: parse nested JSON lists, retain 72–78 digit strings and positional outcome mapping. [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.2] |
| One message = one fact | OBSERVED 2026-10-21: array/object/PONG frames and price-change children have different grains. Reconcile frame IDs and entry IDs separately. `docs/evidence/source/ws_summary.json`; [gsd-final:collector/envelope.py] |
| First level is best price | OBSERVED 2026-10-21: best was at array end in samples; compute max bid/min ask regardless of order. `docs/evidence/source/clob_samples.json`; [gsd-final:fabric/sql/ws_parse.sql] |
| Reconnect “recovers history” | INFERRED: book snapshot recovers state, not missed trades. Expose collector gap intervals. [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.4] |
| Same content = same economic event | UNVERIFIED source uniqueness; dedupe collector capture retries only, preserve separate trade observations. [gsd-final:.planning/REQUIREMENTS.md REL-02]; [gsd-final:fabric/kpi_logic/reference.py dedupe_by_capture_id] |
| Send success = every record sent | DOCUMENTED static defect: oversize sink can return normally with zero sent; runner cancellation loses in-flight accounting. Require explicit disposition at this seam. [gsd-final:collector/sinks.py write_batch]; [gsd-final:collector/runner.py finish]; [verification/baseline.md](verification/baseline.md) |
| Ignored secret file = verified security | DOCUMENTED owner acceptance is limited to local retention; effective access/Git exposure still UNVERIFIED. [gsd-final:.planning/phases/03-build-ingest/03-CONTEXT.md D3-10]; [gsd-final:.planning/phases/02-design/02-DATA-VERIFY.md V-35/V-76] |

## Storage and recovery

| Trap | Lesson / certainty / provenance |
|---|---|
| Non-transactional update-policy failures assumed harmless | ENGINE OBSERVED 2026-10-22/23: raw survived, but 189 price-change frames / 378 quote rows were absent from hot facts. Book-only backfill did not repair those. No later backfill; accepted risk, not resolved loss. `docs/evidence/transform/CLOUD-LOG.md`; [gsd-final:.planning/phases/04-build-transform-kpi/04-DATA-VERIFY.md F-02] |
| Payload copied through every expanded child | ENGINE OBSERVED 2026-10-22: ~549 KB initial books fanned out with raw payload, causing E_LOW_MEMORY_CONDITION. Project bulky payload away before child expansion; preserve canonical raw separately. `docs/evidence/ingest/CLOUD-LOG.md` W9c |
| Eventstream raw schema assumed typed | OBSERVED 2026-10-22: Bronze timestamps are strings, PartitionId shape caused Delta merge-field failure. Cast/project at staging boundary; do not mutate canonical Bronze. [gsd-final:.planning/phases/04-build-transform-kpi/04-CONTEXT.md §Coordinator facts]; `docs/evidence/transform/CLOUD-LOG.md`; [gsd-final:fabric/notebooks/nb_raw_to_stg.py] |
| Normal restart = crash safety | OBSERVED 2026-10-23: rebuild EXCEPT ALL zero both ways, but no mid-stream kill/partial-MERGE/concurrent writer injection. [gsd-final:.planning/phases/04-build-transform-kpi/04-DATA-VERIFY.md V-53/V-56/V-59] |
| Helper tests prove notebook guards | DOCUMENTED: `run_guard.py` is used by offline `ws_clean.py`, not active notebooks. Static/fixture proof must follow actual call path. [gsd-final:fabric/silver_logic/ws_clean.py]; [gsd-final:fabric/notebooks/nb_stg_to_silver_ws.py] |
| 120 s overlap proves completeness | DOCUMENTED/INFERRED: receive-time filter can miss late-staged rows older than overlap. No active beyond-overlap reconciliation proven. [gsd-final:fabric/notebooks/nb_stg_to_silver_ws.py]; [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md DEC-51] |
| SQL equality matches null quarantine keys | DOCUMENTED/INFERRED: frame-level capture_id null does not match `=` in MERGE, so retries can insert duplicates. Test twice on actual engine. [gsd-final:fabric/notebooks/nb_stg_to_silver_ws.py quarantine MERGE] |
| One-day hot retention supports indefinite history | DOCUMENTED: history needs Bronze/warm or another tested route; do not retire warm while still promising KQL history without retention/recovery proof. [gsd-final:fabric/kql/04_policies.kql]; [adr/0001-serving-and-history.md](adr/0001-serving-and-history.md) |
| Daily partitions assumed efficient | DOCUMENTED: initial “unpartitioned until measured” replaced for selected tables; file-size/pruning/CU evidence still incomplete. [gsd-final:.planning/phases/04-build-transform-kpi/04-CONTEXT.md D4-05]; [gsd-final:.planning/phases/04-build-transform-kpi/04-DATA-VERIFY.md V-57] |
| Watchlist metadata assumed full Gamma snapshot | DOCUMENTED: published watchlist drops source question/tick/status/lineage; absent values remain null in active warm row builder. Scope membership is `published_watchlist`; inspect active soft-delete scope checks separately from helper tests. [gsd-final:fabric/gamma_logic/selector.py build_watchlist]; [gsd-final:fabric/notebooks/nb_stg_to_silver_metadata.py] |
| Delta UPDATE subquery assumed supported | ENGINE OBSERVED 2026-10-22/23: DELTA_UNSUPPORTED_SUBQUERY; active metadata notebook uses anti-join absent-key views then MERGE. `docs/evidence/transform/CLOUD-LOG.md`; [gsd-final:fabric/notebooks/nb_stg_to_silver_metadata.py] |

## Measurement and KPI

| Trap | Lesson / certainty / provenance |
|---|---|
| Book source age called latency | OBSERVED 2026-10-21: source age p95 ~584 s/max 708 s on 36 books; receipt-to-render starts at collector receive. `docs/evidence/source/ws_extra_output.txt`; [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.4] |
| Quote age and connection health conflated | DOCUMENTED: quiet valid quote can coexist with healthy PONG. Retain source age, receive time and heartbeat health separately. [gsd-final:fabric/kpi_logic/reference.py spread_connection_state] |
| Approximate dcount used for reconciliation | ENGINE OBSERVED 2026-10-23: intermediate 143 missing frames became exact 189; approximate frame count 228,765 was discarded. Use exact count/distinct key sets and EXCEPT ALL. `docs/evidence/transform/CLOUD-LOG.md` |
| Tie resolved lexically or arbitrarily | OBSERVED 2026-10-23: 18 aggregate groups differed until numeric frame/entry order. Preserve receive order, then test equal-millisecond inputs. `docs/evidence/transform/CLOUD-LOG.md`; [gsd-final:fabric/kpi_logic/reference.py _quote_order] |
| Implementations agree, therefore source-correct | OBSERVED 2026-10-23: zero aggregate differences on one session; live REST source comparison still NOT-RUN. [gsd-final:.planning/phases/04-build-transform-kpi/04-DATA-VERIFY.md V-32] |
| Replay invariance without usable KPIs | OBSERVED 2026-10-23: hot replay session had no coverage, so unchanged null KPI was trivial. Replay a covered session with nonnull results. [gsd-final:.planning/phases/04-build-transform-kpi/04-DATA-VERIFY.md V-25] |
| Synthetic examples called live edge-case proof | DOCUMENTED: 38/38 KQL and 8/8 Spark rule cases are fixtures; no deployed invalid-frame injection. Mark fixture vs real separately. `docs/evidence/transform/kql-fixture-round5.txt`; `docs/evidence/transform/b10-engine-check.txt` |
| 118 s traffic sample used as capacity benchmark | OBSERVED 2026-10-22: 428 assets, 53/s p50 and 107/s p95 in 10 s bins; send p50 7.8 ms/p95 11.9 ms is producer-only. CU/headroom and dashboard freshness unmeasured. [gsd-final:.planning/phases/03-build-ingest/03-DATA-VERIFY.md V-33/V-40] |

## Fabric: engine incidents

| Rule / incident | Label / scope / provenance |
|---|---|
| `fab exists/ls` negative does not establish KQL absence | OBSERVED tool false negatives 2026-10-22; use catalog and engine `.show`/query. [gsd-final:.planning/phases/02-design/02-WORKSPACE-INVENTORY.md INV-05/INV-18/INV-25] |
| `%pip` before parameter cell | ENGINE OBSERVED 2026-10-22: package install restarted interpreter, losing earlier parameters and causing NameError; install first, parameters second. [gsd-final:.planning/phases/04-build-transform-kpi/04-FABRIC-GUIDE.md §2]; `docs/evidence/transform/CLOUD-LOG.md` |
| Keep Python outside `%%sql` cell | ENGINE OBSERVED 2026-10-23: run 10 PARSE_SYNTAX_ERROR near Python counts; explicit cell boundary fixed run 11. `docs/evidence/transform/CLOUD-LOG.md`; [gsd-final:fabric/notebooks/build_notebook_items.py render] |
| SQL-cell errors need failure reconciliation | ENGINE OBSERVED 2026-10-23: five ledger rows stayed RUNNING after failed run and successful retry; post_run_cell callback did not catch SQL failure. 180-minute next-run cleanup is not prompt failure accounting. `docs/evidence/transform/b10-readback-after-fail.json`; `docs/evidence/transform/b10-readback-after-run.json`; [gsd-final:fabric/silver_logic/ledger_runtime.py] |
| Group per asset, avoid repeated linear as-of scans | ENGINE OBSERVED 2026-10-22/23: session-wide Gold >85 min cancelled; per-asset/bisect rewrite later 253–275 s. Not a general benchmark. `docs/evidence/transform/CLOUD-LOG.md`; [gsd-final:fabric/kpi_logic/fast.py] |
| Spark-shaped Pandas input differs from pure Python input | ENGINE OBSERVED 2026-10-23: naive grid timestamps caused failure; localize known UTC grid, set Spark timezone UTC, ISO-format row JSON. `docs/evidence/transform/CLOUD-LOG.md`; [gsd-final:fabric/kpi_logic/gold_transform.py] |
| KQL unmatched strings can be empty, not null | ENGINE OBSERVED 2026-10-23: `isnotnull` misclassified missing joined strings; `isnotempty` fixed Q-FRM-04 counts. `docs/evidence/transform/CLOUD-LOG.md` |
| KQL syntax constraints | ENGINE owner-reported 2026-10-22: parenthesize tabular pipe arguments; no trailing management-command comment/semicolon; decimal default literal not conversion call; split dependent extends; avoid `kind`, use tostring and not(matches regex). Guide compilation did not rerun these. [gsd-final:.planning/phases/04-build-transform-kpi/04-FABRIC-GUIDE.md §1.1–1.5] |

## Fabric: documentation and Unverified

| Guidance | Certainty / provenance |
|---|---|
| Serialized order for prev/scan; explicit join kind; small broadcast lookup side | DOC in 2026-10-22 guide, not new project-engine evidence. [gsd-final:.planning/phases/04-build-transform-kpi/04-FABRIC-GUIDE.md §1.7–1.8] |
| Runtime 1.3/2.0 version and support tables | DOC claims captured 2026-10-22; actual workspace default and current vendor lifecycle UNVERIFIED here. [gsd-final:.planning/phases/04-build-transform-kpi/04-FABRIC-GUIDE.md §0] |
| Notebook exit string, typed pipeline parameters, full item GUIDs, dependency edges | DOC/REPO guide rules; specific active artifacts have deployment evidence, generic API spellings are not all engine-tested. [gsd-final:.planning/phases/04-build-transform-kpi/04-FABRIC-GUIDE.md §2–4]; `docs/evidence/transform/deploy-apply-7.txt` |
| `in(tabular)` in extend/project, scan key-reset variant, decimal/real mixed result, arbitrary-column replaceWhere, NaN/Decimal handling | GEN/UNVERIFIED in guide. Preserve uncertainty; use focused engine probes before relying on these variants. [gsd-final:.planning/phases/04-build-transform-kpi/04-FABRIC-GUIDE.md §5 verification debt] |
| OneLake checkpoint URI variants, pipeline bool/exitValue spelling, high-concurrency/session reuse, Environment runtime | DOC or GEN/UNVERIFIED as individually labelled in guide; not validated for this workspace by this task. [gsd-final:.planning/phases/04-build-transform-kpi/04-FABRIC-GUIDE.md §2–5] |

## Fabric items as source (2026-10-25)

| Trap | Lesson / certainty / provenance |
|---|---|
| METADATA block after a MARKDOWN cell in `notebook-content.py` | OBSERVED 2026-10-25: Fabric stores the markdown cell with null metadata; write MARKDOWN cells with no METADATA block (`fabric/tests/test_notebook_contract.py` guards it). |
| Word `cells` anywhere in `notebook-content.py` | OBSERVED 2026-10-25, fab 1.7.0: `fab export --format .py` fails with `[UnexpectedError] string indices must be integers`. Cause: `fabric_cli/utils/fab_cmd_export_utils.py clean_notebook_cells` tests `"cells" in payload` on the decoded `.py` text (a substring match) and then indexes it. Keep the word out of notebook source (contract test guards it); bare `#` lines inside markdown are dropped by Fabric. |
| `fab import` of `vl_polymarket` from the repo folder | OBSERVED 2026-10-26: the import replaces every variable value, so the checked-in empty `eventstream_connection_string` would wipe the owner-set secret. Export the cloud item, copy that one value into a temporary copy of the repo folder (never print it), import the copy, delete it, and read back names/values masked. `loop deploy` never imports the VariableLibrary unless named. `docs/evidence/ingest/2026-10-26-collector-session-start.md` |

## dbt job (2026-10-26)

Owner decision 2026-10-27 ([ADR 0013](adr/0013-hybrid-silver-notebook-gold-sql.md)): Silver CDF, parsing, quarantine and insert-only writes stay in `nb_session_silver`; Gold is dbt SQL. Scenario tables use `scn_<name>`, production names stay unchanged. The sample loader owns input loading; Silver has no `batch` or path mode. Gold uses one `run_started_at` stamp and reads the previous timestamp as formatted microsecond text. Only all-success model results advance its watermark. 2026-10-28: `play` and Gold layer tests deploy vars only to the derived `dbt_scenario_gold` item; `dbtJob_1` stays unchanged. The restore path is deleted (ADR 0013 dated note).

2026-10-28: Deleted the retired notebook scenario engine and Gold notebook scenario mode. Only `scn_<name>` catalog tables remain as scenario run areas; the production Gold notebook remains until ticket 10.

OBSERVED ticket 11: a completed clean loader/Silver/Gold chain failed only during OneLake read-back (`URLError`, Windows 10060). The read-back now retries transport errors twice, then raises; HTTP authorization and Delta/data errors still fail immediately. Evidence: `docs/evidence/cloud-sample-tests/2026-10-27-110814-play-clean.md` (removed 2026-10-28; see git history).

| Trap | Lesson / certainty / provenance |
|---|---|
| Lakehouse dbt job as service principal | OBSERVED 2026-10-26: item create, `updateDefinition` and run work, but the profile needs a Fabric `Lakehouse` connection (OAuth2 only), so the run fails with "connection referenced by profile is not found" / "datasource type LinkedService is invalid". With the connection created once by the owner in the UI the run works (second probe, `dbtJob_1`, about 5 min for one model). `docs/evidence/elt-dbt-soda/2026-10-26-ticket-02-dbt-job-probe.md` |
| Reusing the owner's Lakehouse profile for a derived dbt item | OBSERVED 2026-10-28 in DEV: REST-created `dbt_scenario_gold`, bootstrapped its empty profile from `dbtJob_1` while retaining the new `.platform`, and ran Gold as the service principal. Gold layer `gate-rules` passed all eight output tables, job `03b0a848-d878-4aef-a5a9-596250a6dc9f` (435 s). The existing owner's OAuth binding was sufficient for this item; no new connection or manual sign-in was needed. |
| Doctoring Silver in an end-to-end play | Owner decision 2026-10-28: represent duplicate/phantom rows as `Scenario.silver_edits`, applied only to `soda-silver` seeds. Compute warn values from these rows; never declare Soda outcomes in a Plan. The play compares every table from unedited inputs; failure-injection probes remain. |
| Choosing cloud checks from a diff | Owner decision 2026-10-28: use only `gate-rules` per affected layer to limit cloud cost. Map or explicitly ignore every item folder and testkit module; fail for unknown paths. Combine separate staged and unstaged diffs so a worktree edit that reverses an index edit does not hide the staged change. |
| dbt job definition part name | OBSERVED 2026-10-26, rechecked 2026-10-28: the live item uses `dbt-content.json`; [REST docs](https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/dbtjob-definition) say `dbtjob-content.json`. The overlay accepts either single profile part, preserving its name. Project files appear as `Code/...` parts. `fab` 1.7.0 `desc .DataBuildToolJob` still rejects the element; use REST. Fabric MCP `docs_item-definitions(dataBuildToolJob)` returned 404 on 2026-10-28; the API spec and Learn page were available. |
| Ad-hoc Python scripts in the shared Temp folder | OBSERVED 2026-10-28: `Temp/inspect.py` shadowed stdlib `inspect` during `dataclasses` import and executed its own failing definition read before the planned verification began. Run such scripts with `.venv/Scripts/python.exe -I` and add only the repo root explicitly to `sys.path`; do not alter unrelated Temp files. |
| Restricted sandbox | OBSERVED 2026-10-27: local `fab auth status` could not read its config and the REST client could not open a socket; the Fabric MCP workspace listing worked. |
| REST access does not imply CLI sign-in | OBSERVED 2026-10-27: REST read `dbtJob_1` while `fab` was signed out. Owner preference 2026-10-28 supersedes the earlier service-principal login path: keep `fab` on the owner account; owner sign-in is `fab auth login`, not `loop login`. |
| Disabled dbt models still parse | OBSERVED 2026-10-27: an unconditional `exceptions.raise_compiler_error` in disabled `ss_frames` failed the dbt job during parsing; guard runtime validation with `execute`. |
| Long plays outlive the `fab` token | OBSERVED 2026-10-27: `play late-arrival` failed at step 5 with `AuthenticationFailed` while polling a job (~25 min into the play); data was fine. Keep or refresh the owner's user sign-in, then rerun. As of 2026-10-28, scenario vars live only on `dbt_scenario_gold`; production restoration is no longer part of recovery. |
| Soda warn check in Silver | OBSERVED 2026-10-27: `nb_session_silver` runs SodaCL `G-UNQ-01` (`warn: when > 0`) on Silver quotes before the watermark advances and merges the result into `dq_result` on `(session_id, rule_id)`; the `warn` branch is not yet proven on Fabric. `docs/evidence/elt-dbt-soda/2026-10-27-ticket-03-quote-path.md` |
| `fntk` session has no default lakehouse | OBSERVED 2026-10-28: a session started with `fntk notebook start-session` on `nb_session_silver` failed `spark.sql` with "Default context not available, please attach a lakehouse"; `get-lakehouse` read null although the item metadata names `lh_polymarket`. A session-only `%%configure -f` with `defaultLakehouse` fixed it (`reference/fabric-cli.md`). |

Owner decision (2026-10-25): collector code lives only in `fabric/Polymarket-DEV/nb_collector_session.Notebook/notebook-content.py`; no collector wheel. Local tests load its definition cells through `fabric.testkit.notebook.load`. Collector documentation is in `docs/collector.md`.

Owner decision (2026-10-25): Eventhouse, KQLDatabase, KQLDashboard and Eventstream source lives in `fabric/Polymarket-DEV/` item folders; numbered KQL apply/migration scripts are retired. Keep the exported embedded queryset. Read-only operator queries and fixture tooling remain under `fabric/kql/`; see its [README](../fabric/kql/README.md) for import and manual-drop rules. The supplied live schema still contains legacy hot-quality functions and `ZzT1`; do not infer their deletion from earlier migration notes.
