# Product brief

DOCUMENTED owner choices and UNVERIFIED hypotheses are separated below; this is a portfolio brief, not evidence of customer demand. [gsd-final:.planning/PROJECT.md §What This Is]

| Subject | Disposition | Provenance |
|---|---|---|
| Owner, maintainer, escalation | DOCUMENTED: Duong Kim Sang; one person, no second operator. | [gsd-final:.planning/phases/01-inception/01-CONTEXT.md §Owner] |
| Audience | UNVERIFIED: recruiters/interviewers assessing an entry-level data engineer; the imagined product user follows AI prediction markets. No interviews or usage measurements. | [gsd-final:.planning/PROJECT.md §What This Is] |
| Value | DOCUMENTED: demonstrate a real stream from source to a Fabric dashboard with inspectable, trustworthy numbers. | [gsd-final:.planning/PROJECT.md §Core Value] |
| Platform and budget | DOCUMENTED owner choice: Microsoft Fabric, trial budget, no assumed paid extension. UNVERIFIED: expiry and available capacity. | [gsd-final:.planning/PROJECT.md §Constraints] |
| Runtime | DOCUMENTED: manual finite sessions; operator supplies duration; stop receiving at expiry, drain and close with an explicit outcome. | [gsd-final:.planning/REQUIREMENTS.md CAP-01] |
| Discovery | DOCUMENTED: daily 07:15 Asia/Saigon (00:15 UTC), complete before absolute 08:15 (01:15 UTC); schedule/alert delivery remain UNVERIFIED. | [gsd-final:.planning/REQUIREMENTS.md META-01/META-07] |
| Fallback | DOCUMENTED: last complete metadata, no maximum age; show timestamp, alert failure, recheck markets before subscribing. New listings may be missed. | [gsd-final:.planning/phases/01-inception/01-CONTEXT.md D-14] |
| Serving/history | DOCUMENTED implementation: KQL live plus Bronze/Delta history. Restart: KQL live only, history in batch Gold (accepted). | [gsd-final:.planning/phases/03-build-ingest/03-CONTEXT.md D3-08]; [adr/0001-serving-and-history.md](adr/0001-serving-and-history.md) |

## Three questions

| Question | Chosen behaviour | Hypothesis still unverified |
|---|---|---|
| What moved most? | Rank outcomes by absolute midpoint movement over 5/15/60 minutes; show signed percentage points and insufficient history. | A follower uses this to choose an outcome to inspect. |
| Is there observed trading? | Separate quote updates from trade observations; show observations per covered minute, known-size totals, spread. | This helps a follower distinguish quoted activity from observed trading. |
| Was there a price shock? | Absolute endpoint midpoint movement ≥5 pp in 5 minutes, configurable; show context and pending post-window. | This threshold is useful; it is not a validated statistical anomaly rule. |

All three behaviours are DOCUMENTED owner selections; consumer benefits are UNVERIFIED hypotheses. [gsd-final:.planning/phases/01-inception/01-DATA-SPEC.md §1.1, §1.4]; implementation details: [contracts/kpi.md](contracts/kpi.md).

## Non-goals

- DOCUMENTED: orders, wallets, investment advice, causal news explanations, ML anomaly detection, all AI-tagged markets, 24/7 collection, pre-capture backfill, and promises of complete trading/exactly-once source delivery are outside scope. [gsd-final:.planning/REQUIREMENTS.md §Out of Scope]
- DOCUMENTED: full book reconstruction, mid-session watchlist refresh and Power BI were deferred proposals; later Spark implementation does not make Power BI a requirement. [gsd-final:.planning/REQUIREMENTS.md §v2 Requirements]; [gsd-final:.planning/phases/04-build-transform-kpi/04-CONTEXT.md D4-01]

## Success and missing measures

- DOCUMENTED target: ≥95% eligible collector-receipt-to-visible samples ≤30 s; 15 s is an aspiration, not another pass condition. Report p50/p95/max, per-session and pooled results, minute p95, dashboard-open denominator and excluded gap/reconnect duration. No measured dashboard result exists in the baseline. [gsd-final:.planning/phases/01-inception/01-CONTEXT.md D-15]; [gsd-final:.planning/phases/02-design/02-CONTEXT.md D2-15]
- UNVERIFIED: portfolio usefulness, actual users, warm-history freshness target, retention/RPO/RTO, alert delivery, trial CU/headroom and source REST comparison. Counts and implementation agreement do not establish these. [gsd-final:.planning/PROJECT.md §Open Questions and Assumptions]; [verification/baseline.md](verification/baseline.md)
- Proposed restart acceptance behaviours and measurement protocol are in [specs/demo.md](specs/demo.md); ADRs 0001–0007 are accepted.
