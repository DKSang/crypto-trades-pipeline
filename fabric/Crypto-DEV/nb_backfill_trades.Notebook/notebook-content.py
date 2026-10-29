# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "039e4fc7-5892-4238-9c2f-a260cb73fc9f",
# META       "default_lakehouse_name": "lh_crypto",
# META       "default_lakehouse_workspace_id": "c8d6538e-e60c-467b-8440-dec5dae4d897",
# META       "known_lakehouses": [
# META         { "id": "039e4fc7-5892-4238-9c2f-a260cb73fc9f" }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Set by pl_backfill_bronze_trades. start and end are UTC ISO 8601 (e.g. 2026-10-29T00:00:00Z);
# end is exclusive and empty means up to the newest trade. bronze_table is a parameter so the
# tests can land the rows in a scratch table.
symbol = ""
start = ""
end = ""
bronze_table = "bronze.trades"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pure REST-to-Bronze conversion and paging (no Spark, unit-tested in tests/test_backfill_trades.py).
# A /0/public/Trades row is [price, volume, time, side, ord_type, misc, trade_id] with price and volume
# as strings, time as epoch seconds and side / ord_type as one letter (reference/lessons.md).
from datetime import datetime, timedelta, timezone

SIDES = {"b": "buy", "s": "sell"}
ORD_TYPES = {"l": "limit", "m": "market"}
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def to_bronze_row(symbol, rest_row, load_ts):
    """One REST trade in the bronze.trades shape the collector sends (WS v2 names, ISO 8601 time)."""
    price, volume, time, side, ord_type, _misc, trade_id = rest_row
    timestamp = datetime.fromtimestamp(time, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {"symbol": symbol, "side": SIDES[side], "price": float(price), "qty": float(volume),
            "ord_type": ORD_TYPES[ord_type], "trade_id": int(trade_id), "timestamp": timestamp,
            "_ingest_path": "backfill", "_load_ts": load_ts}


def parse_utc(text):
    """ISO 8601 text to an aware UTC datetime; a time without an offset is read as UTC."""
    parsed = datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def to_ns(moment):
    """The `since` cursor of /0/public/Trades: nanoseconds since the epoch (integer maths, no float)."""
    return (moment - EPOCH) // timedelta(microseconds=1) * 1000


def iter_pages(fetch, since_ns, end_epoch=None):
    """Yields the REST trades from since_ns on, one list per page, until the newest trade or end_epoch.

    fetch(since_ns) returns (rows, last_ns): the page and the cursor for the next call. `since` is
    inclusive, so the last trade of a page comes again at the top of the next: rows already yielded
    are dropped by trade_id. end_epoch (epoch seconds, exclusive) cuts the page it falls in and stops
    the range; None runs to the newest trade, where the page holds nothing new.
    """
    last_id = None
    while True:
        rows, last_ns = fetch(since_ns)
        new = [r for r in rows if last_id is None or r[6] > last_id]
        kept = [r for r in new if end_epoch is None or r[2] < end_epoch]
        if kept:
            yield kept
            last_id = kept[-1][6]
        if len(kept) < len(new) or not new or last_ns <= since_ns:
            return
        since_ns = last_ns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Backfill: pages Kraken REST into Bronze with ingestion path `backfill`. It only appends. A trade that is
# already loaded arrives again and Silver drops it (insert-only MERGE on symbol + trade_id, the first copy
# stays), so a backfill over loaded data changes no Gold number (reference/architecture/gold-model.md).
# fetch is an argument so nb_test_backfill_trades can serve fixtures instead of calling Kraken.
import json
import time

import requests

REST_URL = "https://api.kraken.com/0/public/Trades"
PAGE_SIZE = 1000          # the most REST returns per call
PAUSE_SECONDS = 1.0       # conservative: REST rate limits are not verified (reference/lessons.md)
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 5.0     # times the attempt number, after a rate limit or a failed call
FLUSH_PAGES = 20          # one Bronze commit per 20 pages (~20,000 trades), so a long backfill keeps its progress
BRONZE_SCHEMA = ("symbol STRING, side STRING, price DOUBLE, qty DOUBLE, ord_type STRING, "
                 "trade_id BIGINT, timestamp STRING, _ingest_path STRING, _load_ts STRING")


def kraken_fetch(symbol):
    def fetch(since_ns):
        for attempt in range(1, MAX_ATTEMPTS + 1):
            time.sleep(PAUSE_SECONDS)
            try:
                response = requests.get(REST_URL, params={"pair": symbol, "since": since_ns, "count": PAGE_SIZE},
                                        timeout=30)
                response.raise_for_status()
                body = response.json()
                if body["error"]:
                    raise RuntimeError(f"Kraken: {body['error']}")
                return body["result"][symbol], int(body["result"]["last"])
            except (requests.RequestException, RuntimeError):
                if attempt == MAX_ATTEMPTS:
                    raise
                time.sleep(BACKOFF_SECONDS * attempt)
    return fetch


def backfill(fetch, symbol, start_ns, end_epoch, bronze_table):
    load_ts = datetime.now(timezone.utc).isoformat()
    result = {"symbol": symbol, "pages": 0, "trades": 0, "first_trade_id": None, "last_trade_id": None}
    buffer = []

    def flush():
        if buffer:
            spark.createDataFrame(buffer, BRONZE_SCHEMA).write.format("delta").mode("append").saveAsTable(bronze_table)
            buffer.clear()

    for page in iter_pages(fetch, start_ns, end_epoch):
        buffer.extend(to_bronze_row(symbol, r, load_ts) for r in page)
        result["pages"] += 1
        result["trades"] += len(page)
        result["first_trade_id"] = result["first_trade_id"] or page[0][6]
        result["last_trade_id"] = page[-1][6]
        if result["pages"] % FLUSH_PAGES == 0:
            flush()
    flush()
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# nb_test_backfill_trades sets library_only before it %runs this notebook, to get the functions without a backfill.
RESULT_PATH = "Files/checks/nb_backfill_trades.json"

if not globals().get("library_only", False):
    lib = notebookutils.variableLibrary.getLibrary("vl_crypto")
    tracked_pairs = [p.strip() for p in lib.tracked_pairs.split(",") if p.strip()]
    if symbol not in tracked_pairs:
        # Silver drops an untracked pair, so the backfill would land rows that go nowhere.
        raise ValueError(f"symbol {symbol!r} is not a tracked pair: {tracked_pairs}")
    if not start.strip():
        raise ValueError("start is required (UTC ISO 8601, e.g. 2026-10-29T00:00:00Z)")
    start_at = parse_utc(start)
    end_at = parse_utc(end) if end.strip() else None
    if end_at is not None and end_at <= start_at:
        raise ValueError(f"end {end} must be after start {start}")

    summary = backfill(kraken_fetch(symbol), symbol, to_ns(start_at),
                       end_at.timestamp() if end_at else None, bronze_table)
    summary.update(start=start_at.isoformat(), end=end_at.isoformat() if end_at else None, bronze_table=bronze_table)
    notebookutils.fs.put(RESULT_PATH, json.dumps(summary, indent=2), True)
    print(summary)
    notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
