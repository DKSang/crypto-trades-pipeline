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

# Hot path test (issue #13): sends two whale-sized events to the es_trades custom endpoint, one on a
# tracked pair (BTC/USD) and one on an untracked pair (FAKE/USD). Both reach eh_trades.trades_raw and
# bronze.trades. The first must fire the whale alert, the second must not.
# ord_type is "test" so Silver quarantines the BTC/USD event (invalid_ord_type) and it never reaches Gold;
# FAKE/USD is dropped by Silver as untracked. trade_id is the run's epoch milliseconds + 9e12, so two runs never share ids.

import json
import subprocess
import sys
import time
from datetime import datetime, timezone

import requests

try:
    from azure.eventhub import EventData, EventHubProducerClient
except ImportError:  # %pip is off in scheduled runs, so install from code
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "azure-eventhub"])
    from azure.eventhub import EventData, EventHubProducerClient

EVENTSTREAM_NAME = "es_trades"
SOURCE_NAME = "TradesCollector"
RESULT_PATH = "/lakehouse/default/Files/checks/nb_test_whale_alert.json"

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


now = datetime.now(timezone.utc)
trade_id = 9_000_000_000_000 + int(now.timestamp() * 1000)
stamp = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
events = [
    {"symbol": symbol, "side": side, "price": 100000.0, "qty": 2.0, "ord_type": "test", "trade_id": trade_id + offset,
     "timestamp": stamp, "_ingest_path": "test", "_load_ts": stamp}
    for offset, (symbol, side) in enumerate([("BTC/USD", "buy"), ("FAKE/USD", "sell")])
]

producer = EventHubProducerClient.from_connection_string(connection_string())
with producer:
    batch = producer.create_batch()
    for event in events:
        batch.add(EventData(json.dumps(event)))
    producer.send_batch(batch)

with open(RESULT_PATH, "w", encoding="utf-8") as f:
    json.dump({"sent_utc": stamp, "events": events}, f, indent=2)
print("sent", [(e["symbol"], e["trade_id"]) for e in events])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
