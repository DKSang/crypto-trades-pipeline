# Polymarket trades

Trades on Polymarket prediction markets about AI models, used to show who trades which outcome, when and for how much.

## Language

**Event**:
A Polymarket grouping of related markets around one topic.
_Avoid_: trade event

**Market**:
One yes/no-style prediction question, identified by its condition ID; plays the role of the "merchant" in the portfolio brief.
_Avoid_: event, asset

**Outcome**:
One labelled possible result of a market (e.g. "Yes", "No"), traded as its own token.
_Avoid_: market, answer

**Asset**:
The token ID of one outcome; always a decimal string, never a number.
_Avoid_: market ID, token number

**Trade**:
One filled order on one outcome, seen from the taker's side: a trader, a side (buy or sell), a size and a price at a point in time.
_Avoid_: transaction, fill pair, order

**Notional**:
The cash value of a trade: size times price, in USDC.
_Avoid_: amount, value

**Whale trade**:
A single trade whose notional is at or above the whale threshold.
_Avoid_: big order, large transaction

**Tracked market**:
A market that matches the daily model-name filter and so is in scope from its first match until it stops matching.
_Avoid_: watchlist, selected market

**Trader**:
A wallet that takes part in trades; plays the role of the "customer" in the portfolio brief.
_Avoid_: user, customer, account
