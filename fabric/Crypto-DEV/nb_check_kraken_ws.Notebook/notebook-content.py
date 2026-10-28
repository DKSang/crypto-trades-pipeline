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

# Spike 2 (ADR 0007): can a Python notebook hold the Kraken WS v2 trade subscription for a
# whole run? Counts reconnects, silences and trade_id jumps per pair; sends nothing.
# Stats are rewritten every minute to STATS_PATH, so a running spike can be read.

import json
import subprocess
import sys
import time
from datetime import datetime, timezone

try:
    import websocket
except ImportError:  # %pip is off in scheduled runs, so install from code
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "websocket-client"])
    import websocket

RUN_MINUTES = 5
RECV_TIMEOUT_SECONDS = 30  # heartbeats come every second, so 30 s of silence means a dead socket
WS_URL = "wss://ws.kraken.com/v2"
PAIRS = ["BTC/USD", "ETH/USD", "XRP/USD", "ZEC/USD", "NEAR/USD", "SOL/USD", "HYPE/USD", "SUI/USD",
         "XMR/USD", "QNT/USD", "PEPE/USD", "TAO/USD", "ENA/USD", "XLM/USD", "LTC/USD", "LINK/USD",
         "ADA/USD", "DASH/USD", "DOT/USD", "DOGE/USD"]
STATS_PATH = "/lakehouse/default/Files/checks/nb_check_kraken_ws.json"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def connect():
    ws = websocket.create_connection(WS_URL, timeout=RECV_TIMEOUT_SECONDS)
    ws.send(json.dumps({"method": "subscribe", "params": {"channel": "trade", "symbol": PAIRS, "snapshot": True}}))
    return ws


stats = {"started_utc": datetime.now(timezone.utc).isoformat(), "connects": 0, "errors": [],
         "max_silence_seconds": 0.0, "subscribe_failures": [], "pairs": {}}
pairs = {p: {"trades": 0, "snapshot_rows": 0, "repeats": 0, "jumps": 0, "missing_ids": 0,
             "first_id": None, "last_id": None} for p in PAIRS}
stats["pairs"] = pairs
deadline = time.time() + RUN_MINUTES * 60
last_stats_write = 0.0
ws = None

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
            stats["subscribe_failures"].append(m)
        if m.get("channel") == "trade":
            for t in m["data"]:
                p = pairs[t["symbol"]]
                if m["type"] == "snapshot":
                    p["snapshot_rows"] += 1
                tid = t["trade_id"]
                if p["last_id"] is not None and tid <= p["last_id"]:
                    p["repeats"] += 1  # snapshot after a reconnect re-sends trades already seen
                    continue
                if p["last_id"] is not None and tid > p["last_id"] + 1:
                    p["jumps"] += 1
                    p["missing_ids"] += tid - p["last_id"] - 1
                p["first_id"] = p["first_id"] or tid
                p["last_id"] = tid
                p["trades"] += 1
    except Exception as e:  # count it, then reconnect; the next snapshot shows what was missed
        stats["errors"].append({"utc": datetime.now(timezone.utc).isoformat(), "error": f"{type(e).__name__}: {e}"[:300]})
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

if ws is not None:
    ws.close()
stats["finished_utc"] = datetime.now(timezone.utc).isoformat()
stats["total_trades"] = sum(p["trades"] for p in pairs.values())
stats["total_missing_ids"] = sum(p["missing_ids"] for p in pairs.values())
with open(STATS_PATH, "w") as f:
    json.dump(stats, f, indent=2)
print(json.dumps({k: v for k, v in stats.items() if k != "pairs"}, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
