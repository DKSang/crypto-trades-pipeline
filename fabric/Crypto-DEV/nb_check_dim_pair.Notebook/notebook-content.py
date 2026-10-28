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

# CELL ********************

# Read-only checks of gold.dim_pair: the tracked rows are exactly the pairs in vl_crypto, under
# WS v2 names (no XBT/XDG), and the Delta history shows which commits changed data.
# Results go to Files/checks/nb_check_dim_pair.json; the run fails if a check fails.

import json
import traceback

RESULT_PATH = "Files/checks/nb_check_dim_pair.json"


def run_checks():
    lib = notebookutils.variableLibrary.getLibrary("vl_crypto")
    expected = sorted(p.strip() for p in lib.tracked_pairs.split(",") if p.strip())
    rows = [r.asDict() for r in spark.table("gold.dim_pair").orderBy("symbol").collect()]
    tracked = sorted(r["symbol"] for r in rows if r["is_tracked"])
    untracked = sorted(r["symbol"] for r in rows if not r["is_tracked"])
    history = [{"version": h["version"], "operation": h["operation"],
                "metrics": h["operationMetrics"]}
               for h in spark.sql("DESCRIBE HISTORY gold.dim_pair").orderBy("version").collect()]
    result = {"rows": len(rows), "tracked": tracked, "untracked": untracked, "history": history}

    failures = []
    if tracked != expected:
        failures.append("tracked rows differ from vl_crypto")
    if len({r["symbol"] for r in rows}) != len(rows):
        failures.append("duplicate symbol")
    if any("XBT" in r["symbol"] or "XDG" in r["symbol"] for r in rows):
        failures.append("legacy name in dim_pair")
    if any(not r["base"] or not r["quote"] or not r["status"] for r in rows):
        failures.append("base, quote or status empty")
    result["failures"] = failures
    return result


try:
    result = run_checks()
except Exception:
    result = {"failures": ["check crashed"], "error": traceback.format_exc()}

notebookutils.fs.put(RESULT_PATH, json.dumps(result, indent=2), True)
print(json.dumps(result, indent=2))
if result["failures"]:
    raise RuntimeError("; ".join(result["failures"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
