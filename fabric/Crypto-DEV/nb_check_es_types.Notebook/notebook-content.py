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

# Finds which field makes es_trades drop collector events with TypeConversionError.
# Sends one probe event per case, each adding fields; symbol "PROBE/<case>" and
# _ingest_path "probe" mark them. The cases that reach bronze.trades show which
# fields convert; the first missing case names the culprit. Delete the probe rows after.

import json
import subprocess
import sys
from datetime import datetime, timezone

import requests

try:
    from azure.eventhub import EventData, EventHubProducerClient
except ImportError:  # %pip is off in scheduled runs, so install from code
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "azure-eventhub"])
    from azure.eventhub import EventData, EventHubProducerClient

EVENTSTREAM_NAME = "es_trades"
SOURCE_NAME = "TradesCollector"

load_ts = datetime.now(timezone.utc).isoformat()
CASES = {
    "A_keys": {"trade_id": 1},
    "B_strings": {"trade_id": 2, "side": "buy", "ord_type": "limit", "_ingest_path": "probe"},
    "C_floats": {"trade_id": 3, "price": 80395.1, "qty": 0.00016836},
    "D_float_whole": {"trade_id": 4, "price": 80395.0, "qty": 2.0},
    "E_float_exp": {"trade_id": 5, "price": 4.3712e-06, "qty": 1.5e-05},
    "F_timestamp_z": {"trade_id": 6, "timestamp": "2026-10-28T15:15:10.708706Z"},
    "G_load_ts": {"trade_id": 7, "_load_ts": load_ts},
    "H_full_row": {"trade_id": 8, "side": "sell", "price": 80395.1, "qty": 0.00016836, "ord_type": "market",
                   "timestamp": "2026-10-28T15:15:10.708706Z", "_ingest_path": "probe", "_load_ts": load_ts},
}

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


producer = EventHubProducerClient.from_connection_string(connection_string())
try:
    for case, fields in CASES.items():
        batch = producer.create_batch()  # one event per batch, so a bad case can't take others with it
        batch.add(EventData(json.dumps({"symbol": f"PROBE/{case}", **fields})))
        producer.send_batch(batch)
        print(case, json.dumps(fields))
finally:
    producer.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
