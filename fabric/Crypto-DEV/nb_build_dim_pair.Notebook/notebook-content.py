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

# Pure mapping from the raw /0/public/AssetPairs response to dim_pair rows (no Spark, unit-tested
# in tests/test_dim_pair.py). AssetPairs still lists legacy names (wsname XBT/USD, XDG/USD);
# every name in the pipeline is the WS v2 name (reference/lessons.md).

LEGACY_TO_WS_V2 = {"XBT": "BTC", "XDG": "DOGE"}


def ws_v2_name(wsname):
    return "/".join(LEGACY_TO_WS_V2.get(asset, asset) for asset in wsname.split("/"))


def parse_tracked(tracked_pairs):
    return [pair.strip() for pair in tracked_pairs.split(",") if pair.strip()]


def tracked_rows(asset_pairs, tracked):
    by_symbol = {ws_v2_name(p["wsname"]): p for p in asset_pairs.values() if p.get("wsname")}
    missing = [symbol for symbol in tracked if symbol not in by_symbol]
    if missing:
        # A tracked pair Kraken doesn't list must not silently lose its flag.
        raise ValueError(f"Tracked pairs missing from the AssetPairs snapshot: {missing}")
    rows = []
    for symbol in tracked:
        base, quote = symbol.split("/")
        rows.append({"symbol": symbol, "base": base, "quote": quote,
                     "status": by_symbol[symbol]["status"], "is_tracked": True})
    return rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Builds gold.dim_pair from the newest raw snapshot that pl_ingest_bronze_asset_pairs landed.
# Idempotent: the MERGE only touches a row whose values differ, so a second run on the same
# snapshot changes nothing. A pair removed from vl_crypto keeps its row with is_tracked = false.
import json

lib = notebookutils.variableLibrary.getLibrary("vl_crypto")
tracked = parse_tracked(lib.tracked_pairs)

snapshots = sorted(f.name for f in notebookutils.fs.ls("Files/bronze/asset_pairs") if f.name.endswith(".json"))
if not snapshots:
    raise FileNotFoundError("No AssetPairs snapshot in Files/bronze/asset_pairs")
snapshot = snapshots[-1]
with open(f"/lakehouse/default/Files/bronze/asset_pairs/{snapshot}", encoding="utf-8-sig") as f:  # Copy writes a BOM
    response = json.load(f)
if response.get("error"):
    raise RuntimeError(f"Kraken returned errors in {snapshot}: {response['error']}")

rows = tracked_rows(response["result"], tracked)

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_pair (
    symbol STRING,
    base STRING,
    quote STRING,
    status STRING,
    is_tracked BOOLEAN
) USING DELTA
""")

spark.createDataFrame(rows, "symbol STRING, base STRING, quote STRING, status STRING, is_tracked BOOLEAN") \
     .createOrReplaceTempView("pairs_snapshot")

spark.sql("""
MERGE INTO gold.dim_pair AS t
USING pairs_snapshot AS s
ON t.symbol = s.symbol
WHEN MATCHED AND NOT (t.base <=> s.base AND t.quote <=> s.quote AND t.status <=> s.status AND t.is_tracked <=> s.is_tracked)
    THEN UPDATE SET t.base = s.base, t.quote = s.quote, t.status = s.status, t.is_tracked = s.is_tracked
WHEN NOT MATCHED THEN INSERT (symbol, base, quote, status, is_tracked)
    VALUES (s.symbol, s.base, s.quote, s.status, s.is_tracked)
WHEN NOT MATCHED BY SOURCE AND t.is_tracked THEN UPDATE SET t.is_tracked = false
""")

summary = {"snapshot": snapshot, "tracked": len(tracked),
           "dim_pair_rows": spark.table("gold.dim_pair").count(),
           "dim_pair_tracked": spark.table("gold.dim_pair").where("is_tracked").count()}
print(summary)
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
