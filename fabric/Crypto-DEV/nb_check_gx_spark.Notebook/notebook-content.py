# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "01a138e9-6b56-461d-8df6-a037f9c2afdc",
# META       "workspaceId": "c8d6538e-e60c-467b-8440-dec5dae4d897"
# META     },
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

# Spike 4 (issue #12): does Great Expectations 1.x validate Spark DataFrames in env_crypto?
# One tiny DataFrame, one run of each expectation the quality gate uses; each is tried on its own so
# one failure doesn't hide the others. Results go to Files/checks/nb_check_gx_spark.json; the run
# fails if an expectation can't run or gives the wrong answer.

import json
import platform
import traceback
from datetime import datetime

import pyspark
from pyspark.sql import functions as F

RESULT_PATH = "Files/checks/nb_check_gx_spark.json"
evidence = {"python": platform.python_version(), "pyspark": pyspark.__version__}

try:
    import great_expectations as gx
    import great_expectations.expectations as gxe
    evidence["great_expectations"] = gx.__version__
except Exception:
    evidence["import"] = "FAILED: " + traceback.format_exc()
    notebookutils.fs.put(RESULT_PATH, json.dumps(evidence, indent=2), True)
    raise

df = spark.createDataFrame(
    [("BTC/USD", 1, "buy", 1.5, datetime(2026, 10, 28, 17, 0, 0), 0),
     ("BTC/USD", 2, "sell", 2.5, datetime(2026, 10, 28, 17, 0, 1), 0),
     ("ETH/USD", 1, "buy", 3.5, datetime(2026, 10, 28, 17, 0, 2), 2),
     ("ETH/USD", 1, "buy", 3.5, datetime(2026, 10, 28, 17, 0, 2), 0)],
    "symbol STRING, trade_id BIGINT, side STRING, price DOUBLE, trade_ts TIMESTAMP, missing BIGINT")

context = gx.get_context(mode="ephemeral")
asset = context.data_sources.add_spark("spark").add_dataframe_asset("probe")
batch_definition = asset.add_batch_definition_whole_dataframe("whole")


def validate(expectation):
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    return batch.validate(expectation)


# name -> (expectation, expected success on the DataFrame above)
CASES = {
    "compound_unique_fails_on_the_repeated_key": (gxe.ExpectCompoundColumnsToBeUnique(column_list=["symbol", "trade_id"]), False),
    "in_set_passes": (gxe.ExpectColumnValuesToBeInSet(column="symbol", value_set=["BTC/USD", "ETH/USD"]), True),
    "in_set_fails": (gxe.ExpectColumnValuesToBeInSet(column="symbol", value_set=["BTC/USD"]), False),
    "row_count_between": (gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=10), True),
    "columns_match_ordered_list": (gxe.ExpectTableColumnsToMatchOrderedList(
        column_list=["symbol", "trade_id", "side", "price", "trade_ts", "missing"]), True),
    "column_type_bigint": (gxe.ExpectColumnValuesToBeOfType(column="trade_id", type_="LongType"), True),
    "column_type_timestamp": (gxe.ExpectColumnValuesToBeOfType(column="trade_ts", type_="TimestampType"), True),
    "max_between_timestamps": (gxe.ExpectColumnMaxToBeBetween(
        column="trade_ts", min_value=datetime(2026, 10, 28, 17, 0, 0), max_value=datetime(2026, 10, 28, 18, 0, 0)), True),
    "values_between_fails_on_missing": (gxe.ExpectColumnValuesToBeBetween(column="missing", min_value=0, max_value=0), False),
}
evidence["cases"] = {}
for name, (expectation, expected) in CASES.items():
    try:
        result = validate(expectation)
        info = {"success": bool(result.success), "expected": expected, "result": json.loads(json.dumps(result.result, default=str))}
        if result.success != expected:
            info["FAILED"] = "wrong answer"
        evidence["cases"][name] = info
    except Exception:
        evidence["cases"][name] = {"FAILED": traceback.format_exc()}

failed = sorted(n for n, c in evidence["cases"].items() if "FAILED" in c)
evidence["failed"] = failed
notebookutils.fs.put(RESULT_PATH, json.dumps(evidence, indent=2), True)
print(json.dumps(evidence, indent=2)[:6000])
if failed:
    raise RuntimeError(f"spike 4: {failed}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
