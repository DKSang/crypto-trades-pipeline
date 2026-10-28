# Trades come from the Data API, taker rows only, with a built key

The fact is one taker trade from Polymarket's Data API `/trades`, not WebSocket quotes. That gives a real trader (wallet) and market for the model, and one schema shared by the live poll (Eventstream HTTP source) and the backfill (`start`/`end` windows). The API has no guaranteed trade ID, so `trade_key` is a hash of (`transactionHash`, `asset`, `proxyWallet`, `side`, `size`, `price`, `timestamp`), and a warn-level check watches whether `transactionHash` alone stays unique. Only taker rows are read (`takerOnly=true`): with maker rows included, one fill appears twice and volume doubles (see `reference/lessons.md`).

## Considered options

- **WebSocket market channel (the first attempt).** No trader, no source key: it needed home-made capture identity, coverage segments and gaps just to avoid double counting.
- **RTDS `activity/trades`.** Real streaming, but not in Polymarket's official docs and a second schema next to the backfill. Kept as the fallback if the HTTP source fails.

## Consequences

Two genuinely identical trades in one transaction collapse into one row. Accepted and documented.
