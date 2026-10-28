# The hot path detects events; official numbers come only from Gold

Eventstream writes to both Eventhouse and the lakehouse. The Eventhouse side answers only "right now" questions: whale trades (notional at or above a threshold in `vl_crypto`, alerted by Activator) and a 5-minute volume chart per pair on the Real-Time dashboard. It reads the tracked pairs from `gold.dim_pair` through a OneLake shortcut. No KPI is computed in both paths; a daily reconcile compares hot-path totals with Gold and records the difference and its causes (duplicates, late data, backfill).

## Considered options

- **Live KPIs in KQL mirroring batch Gold (first attempt).** The same rules written twice in KQL and Spark drifted apart (old reason codes, case M14) and needed a Python oracle to keep them aligned.
- **No hot path.** A whale trade or a news reaction is only useful within minutes; daily batch answers it a day late.
- **24-hour baseline anomaly rule in KQL.** Dropped: it is KPI logic in the hot path.
