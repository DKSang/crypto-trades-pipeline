# One day of hot path against Gold: BTC/USD, 29 October 2026

The hot path (`eh_trades`) and Gold (`gold.fact_trade`) are fed by the same Eventstream, yet they disagree. `ops.reconcile_daily` records by how much, per pair and UTC day, and why. This is one real day, read from that table.

Run of 2026-10-29 10:26 UTC. The day was still in progress: both paths were moving.

## The numbers

| `BTC/USD`, 2026-10-29 | Trades | Notional (USD) |
|---|---|---|
| Hot path (`hot_daily`) | 285 | 910,605.53 |
| Gold (`fact_trade`) | 281 | 110,605.53 |
| **Reconcile gap** | **4** | **800,000.00** |
| duplicates | 0 | 0 |
| late | 3 | 600,000.00 |
| backfill | 0 | 0 |
| unexplained | 1 | 200,000.00 |

The other 19 tracked pairs had a gap of 0 in trades and notional, at that moment.

## What the four trades are

All four are the same kind of event: a `BTC/USD` buy of 2 at 100,000 USD (200,000 USD), sent by `nb_test_whale_alert` to prove the whale alert. They are not Kraken trades. The hot path counts every event it receives, so it counted them; Gold only holds trades that pass Silver's rules.

- **Late (3 events).** Three of them reached Bronze (`_ingest_path` `test`, `ord_type` `test`), so the live stream delivered them, but they are not in Gold. Silver's `invalid_ord_type` rule quarantines that `ord_type`, so Gold will never load them. The word *late* is therefore generous here: the reconcile says "delivered live, not in Gold at run time", and for these events that stays true on every rerun. A later cause that separates "rejected" from "not yet loaded" would make this row clearer; today the two share one column. I did not read the quarantine rows directly (the SQL analytics endpoint does not show the `ops` tables yet), so "Silver rejects them" rests on the rule, not on a row I looked at.
- **Unexplained (1 event).** The fourth, sent at 07:16:56 with `ord_type` `market` and `trade_id` 9000000001, is in `trades_raw` and in no Bronze row. The hot path and Bronze received different numbers of events, and nothing in the reconcile can say why. The likely reason is that it was sent while the Lakehouse destination of `es_trades` was inactive: dropping the destination table deactivates the destination (`reference/lessons.md`). That is a hypothesis; I did not check the destination's history. A cause that stays in `unexplained` is the table doing its job: it points at a delivery difference to investigate.

## Vocabulary

- **Duplicates.** The same trade can reach the stream twice: after a reconnect Kraken's subscribe snapshot repeats the last 50 trades per pair (`reference/lessons.md`), and overlapping collector runs do the same. The hot path counts each copy; Silver keeps one row per (`symbol`, `trade_id`). The cause `duplicates` is that difference. It was 0 on this day for every pair. Case 4 of `nb_test_reconcile_hot_gold` shows it adding up when it is not 0.
- **Late data.** A trade is late when it is not in Gold yet at the time the reconcile runs, either because the hourly load has not read it or because Gold refused it. Gold is a batch: the reconcile of a day in progress shows `late` above 0 until the next load, and the next hourly run replaces the row.
- **Consistency limits.** The hot path is fast and unchecked: it counts what arrives. Gold is hourly, deduplicated and gated by data-quality rules, so it is slower, and every row in it passed the rules. The two agree only eventually, and only for the events that pass the rules. The reconcile does not make them equal; it records the distance and names it. A trade the collector missed (a `trade_id` gap, ADR 0007) is in neither path and so is not in the gap. A backfill makes Gold exceed the hot path, and that shows as `backfill`.

## How to read it

`gap` = `duplicates` + `late` − `backfill` + `unexplained`, in trades and in notional (`reference/architecture/trades-pipeline.md`). Here 4 = 0 + 3 − 0 + 1. `unexplained` is the residual of the other three (`hot` minus the live Bronze rows), not an independent measurement: whatever the first three causes cannot place lands there, which is why that column is the one to watch.
