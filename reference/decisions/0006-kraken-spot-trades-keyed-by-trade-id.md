# Trades come from Kraken spot, keyed by pair and trade_id

The fact is one spot trade on Kraken for a tracked USD pair, read from the public WS v2 `trade` channel and, for backfill, from REST `/0/public/Trades`. Both give the same `trade_id`, price, quantity, taker side and order type (see `reference/lessons.md`), so one Bronze shape serves live and backfill. The key is (`symbol`, `trade_id`): Kraken assigns `trade_id` per pair, counting up by one, so no hash is built and Silver can prove completeness per pair (`max − min + 1 = count`). Spot only; the tracked pairs are a fixed list in `vl_crypto`.

## Considered options

- **Polymarket Data API (the first design, ADRs 0001 and 0005, deleted).** Dropped 2026-10-28: Vietnamese law treats Polymarket as disguised gambling.
- **Coinbase Exchange `matches` channel.** `side` is the maker's side, so every row must be flipped to get the taker, and the docs warn messages can be dropped.
- **Kraken Futures.** A separate API and schema, with leverage, funding and expiries to explain and no business question that needs them.

## Consequences

Public feeds carry no trader identity, so there is no `dim_trader` and no concentration-by-trader question. Crypto is still a grey area in Vietnam; only public market data is read and nothing is traded.
