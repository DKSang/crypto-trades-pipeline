# Lessons

## Fabric: Eventstream Lakehouse destination

- **Small files are a setting.** DOCUMENTED ([Learn](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/add-destination-lakehouse)): Advanced > *Minimum rows* (1 to 2 million per file) and *Maximum duration* (1 min to 2 h per file). Larger values give fewer, bigger files; still run `OPTIMIZE`.
- **Schema comes from the first record.** DOCUMENTED (same page): a new table takes its schema from the first event; later extra fields are dropped, missing ones become null. Send a stable shape.
- **Events before routing starts can be lost.** DOCUMENTED (same page): uncheck *Activate ingestion*, publish, then resume with a custom earlier start time.

## Source: Polymarket Data API `/trades`

- **Maker and taker rows double count.** OBSERVED 2026-10-28: `takerOnly=false` returned 2,000 rows for 794 `transactionHash` values (up to 18 rows per hash, maker and taker of the same fill as separate rows, 8 byte-identical rows). With the default `takerOnly=true`, 2,000 rows had 2,000 distinct hashes. Sum volume from taker rows only. Hash uniqueness is a bounded sample, not a source guarantee.
- **Backfill window.** DOCUMENTED (docs.polymarket.com, `/trades`): `start`/`end` epoch seconds, `limit` ≤10,000, `offset` ≤10,000 (400 past it); page by time window to go further. Rate limits not documented.
- **Global volume.** OBSERVED 2026-10-28: 2,000 taker trades spanned 150 s across all markets (~13 trades/s); a filtered AI-model universe will be far smaller.
