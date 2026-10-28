# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
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

# CELL ********************

# Collects Kraken spot trades from the WS v2 trade channel and sends them to es_trades
# (custom endpoint), which writes them to bronze.trades unchanged (ADR 0007).
# It never calls REST: trades missed between runs or during a reconnect are accepted
# as gaps, which Silver's trade_id contiguity check reports. The subscribe snapshot
# re-sends recent trades after a reconnect; those already seen in this run are skipped
# here, the rest are removed by the Silver dedupe on (symbol, trade_id).
# Runs for RUN_MINUTES, then exits; schedule it to run again.

import json
import subprocess
import sys
import time
from datetime import datetime, timezone

import requests

try:
    import websocket
    from azure.eventhub import EventData, EventHubProducerClient
except ImportError:  # %pip is off in scheduled runs, so install from code
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "websocket-client", "azure-eventhub"])
    import websocket
    from azure.eventhub import EventData, EventHubProducerClient

RUN_MINUTES = 5  # tuned later with the schedule
RECV_TIMEOUT_SECONDS = 30  # heartbeats come every second, so 30 s of silence means a dead socket
WS_URL = "wss://ws.kraken.com/v2"
# Tracked pairs (reference/architecture/trades-pipeline.md); move to vl_crypto in #8.
PAIRS = ["BTC/USD", "ETH/USD", "XRP/USD", "ZEC/USD", "NEAR/USD", "SOL/USD", "HYPE/USD", "SUI/USD",
         "XMR/USD", "QNT/USD", "PEPE/USD", "TAO/USD", "ENA/USD", "XLM/USD", "LTC/USD", "LINK/USD",
         "ADA/USD", "DASH/USD", "DOT/USD", "DOGE/USD"]
EVENTSTREAM_NAME = "es_trades"
SOURCE_NAME = "TradesCollector"
STATS_PATH = "/lakehouse/default/Files/checks/nb_collect_trades.json"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def fabric_get(path):
    token = notebookutils.credentials.getToken("pbi")
    r = requests.get(f"https://api.fabric.microsoft.com/v1/{path}",
                     headers={"Authorization": f"Bearer {token}"}, timeout=30)
    r.raise_for_status()
    return r.json()


def connection_string():
    """Reads the custom endpoint key at run time, so it is never stored or printed."""
    ws = notebookutils.runtime.context["currentWorkspaceId"]
    es = next(i["id"] for i in fabric_get(f"workspaces/{ws}/eventstreams")["value"]
              if i["displayName"] == EVENTSTREAM_NAME)
    src = next(s["id"] for s in fabric_get(f"workspaces/{ws}/eventstreams/{es}/topology")["sources"]
               if s["name"] == SOURCE_NAME)
    return fabric_get(f"workspaces/{ws}/eventstreams/{es}/sources/{src}/connection")["accessKeys"]["primaryConnectionString"]


def connect():
    ws = websocket.create_connection(WS_URL, timeout=RECV_TIMEOUT_SECONDS)
    ws.send(json.dumps({"method": "subscribe", "params": {"channel": "trade", "symbol": PAIRS, "snapshot": True}}))
    return ws


def normalise(raw, load_ts):
    """One WS v2 trade in the bronze.trades column order; field names stay as Kraken sends them."""
    return {"symbol": raw["symbol"], "side": raw["side"], "price": float(raw["price"]), "qty": float(raw["qty"]),
            "ord_type": raw["ord_type"], "trade_id": int(raw["trade_id"]), "timestamp": raw["timestamp"],
            "_ingest_path": "ws", "_load_ts": load_ts}


def send(producer, trades):
    batch = producer.create_batch()
    for t in trades:
        event = EventData(json.dumps(t))
        try:
            batch.add(event)
        except ValueError:  # batch full
            producer.send_batch(batch)
            batch = producer.create_batch()
            batch.add(event)
    if len(batch):
        producer.send_batch(batch)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

stats = {"started_utc": datetime.now(timezone.utc).isoformat(), "connects": 0, "errors": 0, "last_error": None,
         "received": 0, "sent": 0, "skipped_seen": 0, "max_silence_seconds": 0.0, "subscribe_failures": 0,
         "jumps": {}}  # pair -> trade_ids missing inside this run (a reconnect gap)
last_id = {}  # pair -> newest trade_id sent in this run
deadline = time.time() + RUN_MINUTES * 60
producer = EventHubProducerClient.from_connection_string(connection_string())
last_stats_write = 0.0
ws = None

try:
    while time.time() < deadline:
        try:
            if ws is None:
                ws = connect()
                stats["connects"] += 1
                last_msg = time.time()
            m = json.loads(ws.recv())
            now = time.time()
            stats["max_silence_seconds"] = max(stats["max_silence_seconds"], round(now - last_msg, 1))
            last_msg = now
            if m.get("method") == "subscribe" and not m.get("success"):
                stats["subscribe_failures"] += 1
                stats["last_error"] = json.dumps(m)[:500]
            if m.get("channel") == "trade":
                load_ts = datetime.now(timezone.utc).isoformat()
                fresh = []
                for t in (normalise(r, load_ts) for r in m["data"]):
                    stats["received"] += 1
                    prev = last_id.get(t["symbol"])
                    if prev is not None and t["trade_id"] <= prev:
                        stats["skipped_seen"] += 1
                        continue
                    if prev is not None and t["trade_id"] > prev + 1:
                        stats["jumps"][t["symbol"]] = stats["jumps"].get(t["symbol"], 0) + t["trade_id"] - prev - 1
                    last_id[t["symbol"]] = t["trade_id"]
                    fresh.append(t)
                if fresh:
                    send(producer, fresh)
                    stats["sent"] += len(fresh)
        except Exception as e:  # reconnect; what was missed meanwhile is an accepted gap
            stats["errors"] += 1
            stats["last_error"] = f"{type(e).__name__}: {e}"[:500]
            try:
                ws.close()
            except Exception:
                pass
            ws = None
            time.sleep(2)
        if time.time() - last_stats_write > 60:
            stats["updated_utc"] = datetime.now(timezone.utc).isoformat()
            with open(STATS_PATH, "w") as f:
                json.dump(stats, f, indent=2)
            last_stats_write = time.time()
finally:
    if ws is not None:
        ws.close()
    producer.close()

stats["finished_utc"] = datetime.now(timezone.utc).isoformat()
stats["last_trade_id"] = last_id
with open(STATS_PATH, "w") as f:
    json.dump(stats, f, indent=2)
print(json.dumps(stats, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
