# A Python-notebook collector holds the WebSocket; gaps between runs are accepted

Live trades come from `nb_collect_trades`, a Python notebook (no Spark) that subscribes to the Kraken WS v2 `trade` channel for the tracked pairs and sends each normalised trade to the `es_trades` custom endpoint, which writes it unchanged to `bronze.trades`. It reads the endpoint key from the Topology API at run time, reconnects if the socket dies, runs for a fixed time and is run again on a schedule. It never calls REST. Trades missed between runs, during a reconnect, a Kraken maintenance window or a pause are accepted as gaps: Silver's `trade_id` contiguity check (warn) reports them, and `nb_backfill_trades` fills one by hand when a demo needs it. This is a portfolio project, not production; a simpler collector is worth more than complete data.

## Considered options

- **Eventstream source reading the WebSocket directly.** Eventstream has no WebSocket source (see `reference/lessons.md`).
- **Collector fills gaps from REST on start and reconnect.** Complete data, but a state file and a second code path in the collector. The subscribe snapshot (50 trades per pair) covers well under a minute of `BTC/USD`, so without it each run boundary loses trades.
- **Overlapping runs.** Would close run-boundary gaps without REST, but needs two concurrent runs of one notebook, which is not verified.

## Consequences

Gold undercounts by whatever the gaps miss, unevenly across pairs; the daily reconcile and the contiguity check make that visible. Duplicates from the subscribe snapshot after a reconnect still reach Bronze and are removed by the Silver dedupe on (`symbol`, `trade_id`).
