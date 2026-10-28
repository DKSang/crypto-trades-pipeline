# Crypto trades

Spot trades on Kraken for the 20 most liquid USD pairs, used to show which pairs draw money, who presses the price (taker buyers or sellers), when and for how much.

## Language

**Pair**:
One spot market on Kraken, a base asset priced in a quote asset, named in WebSocket v2 form (e.g. `BTC/USD`, `DOGE/USD`); plays the role of the "merchant" in the portfolio brief.
_Avoid_: market, symbol (except as the column name), ticker, `XBT/USD` or `XDG/USD` (legacy REST names)

**Base asset**:
The coin a pair buys and sells (e.g. `BTC` in `BTC/USD`); its quantity is the trade's `qty`.
_Avoid_: coin, token

**Quote asset**:
The asset a pair is priced in; always `USD` for tracked pairs.
_Avoid_: currency, counter

**Trade**:
One fill on one pair, seen from the taker's side: a side (buy or sell), a quantity, a price, a time and a `trade_id` that counts up by one per pair.
_Avoid_: transaction, order, match

**Taker side**:
Whether the order that crossed the book bought or sold; Kraken's `side` is always the taker's.
_Avoid_: direction, maker side

**Notional**:
The cash value of a trade: qty times price, in USD, before fees.
_Avoid_: amount, value, volume (volume means summed qty)

**Whale trade**:
A single trade whose notional is at or above the whale threshold.
_Avoid_: big order, large transaction

**Tracked pair**:
A pair on the fixed list in `vl_crypto`, edited by hand; only tracked pairs are collected.
_Avoid_: watchlist, selected market

**Gap**:
A run of missing `trade_id` values in one pair; accepted between collector runs (ADR 0007), reported by Silver's contiguity check and filled by backfill when needed.
_Avoid_: hole, missing data
