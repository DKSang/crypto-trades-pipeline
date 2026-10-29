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

# Seam: the DAX interface of sm_trades (issue #15). Each case queries the deployed model with semantic
# link and compares it with a direct calculation by Spark on gold.fact_trade / gold.agg_pair_daily.
# Read-only on the real tables. Cases: relationships, data as of (and its independence from filters), whale share,
# VWAP, taker buy share, high/low/open/close, price measures blank across pairs, untracked pairs keep history,
# whale threshold equals vl_crypto. Results go to Files/checks/nb_test_sm_trades.json; the run fails if a case fails.

import datetime
import functools
import json
import traceback

import pandas as pd
import sempy.fabric as fabric

DATASET = "sm_trades"
WORKSPACE = fabric.get_notebook_workspace_id()
RESULT_PATH = "Files/checks/nb_test_sm_trades.json"

# Foreign keys of reference/architecture/gold-model.md, as (many table, many column, one table, one column).
EXPECTED_RELATIONSHIPS = {
    ("Trade", "Pair Symbol", "Pair", "Pair"),
    ("Trade", "Trade Date", "Date", "Date"),
    ("Pair Daily", "Pair Symbol", "Pair", "Pair"),
    ("Pair Daily", "Trade Date", "Date", "Date"),
}


TOLERANCE = 1e-9


@functools.lru_cache(maxsize=None)
def whale_threshold():
    # The model's own constant; Spark uses the same number. (A query-scoped DEFINE MEASURE did not override it for
    # the measures that depend on it, see lessons.md.)
    return int(scalar('EVALUATE ROW("v", [Whale Threshold (USD)])'))


@functools.lru_cache(maxsize=None)
def window():
    """The last 7 UTC days that have trades, as (first, last) dates."""
    last = spark.sql("SELECT MAX(trade_date) AS d FROM gold.fact_trade").first()["d"]
    return last - datetime.timedelta(days=6), last


def in_window(table_expression):
    """DAX table expression restricted to the week: boolean filters on Date[Date]."""
    first, last = window()
    return (f"CALCULATETABLE({table_expression}, "
            f"'Date'[Date] >= DATE({first.year}, {first.month}, {first.day}), "
            f"'Date'[Date] <= DATE({last.year}, {last.month}, {last.day}))")


def model_by_pair(measures):
    """{pair: {measure: value}} from the model over the week."""
    cols = ", ".join(f'"{m}", [{m}]' for m in measures)
    df = dax("EVALUATE " + in_window(f"SUMMARIZECOLUMNS('Pair'[Pair], {cols})"))
    return {r["Pair"]: {m: r[m] for m in measures} for _, r in df.iterrows()}


def spark_by_pair(sql_select):
    """{pair: row dict} from Spark; the SQL gets {first}, {last} and {threshold} and must return a symbol column."""
    first, last = window()
    rows = spark.sql(sql_select.format(first=first, last=last, threshold=whale_threshold())).collect()
    return {r["symbol"]: r.asDict() for r in rows}


def assert_close(model, expected, label):
    assert set(model) == set(expected), (
        f"{label}: pairs differ, model-only {sorted(set(model) - set(expected))}, spark-only {sorted(set(expected) - set(model))}")
    bad = {k: (model[k], expected[k]) for k in expected
           if abs((model[k] or 0) - (expected[k] or 0)) > TOLERANCE * max(1, abs(expected[k] or 0))}
    assert not bad, f"{label}: model vs spark differ for {bad}"


def dax(query):
    # Columns come back as "Table[Column]" or "[Measure]"; keep the name inside the brackets.
    df = fabric.evaluate_dax(DATASET, query, workspace=WORKSPACE)
    df.columns = [c[c.rindex("[") + 1:-1] if "[" in c else c for c in df.columns]
    return df


def scalar(query):
    df = dax(query)
    assert len(df) == 1 and len(df.columns) == 1, f"expected one value, got {df.shape}"
    return df.iloc[0, 0]


def case_relationships_follow_the_documented_foreign_keys():
    rel = fabric.list_relationships(DATASET, workspace=WORKSPACE)
    found = {(r["From Table"], r["From Column"], r["To Table"], r["To Column"]) for _, r in rel.iterrows()}
    assert found == EXPECTED_RELATIONSHIPS, f"found {sorted(found)}"
    return {"relationships": sorted(found)}


def case_data_as_of_is_the_newest_trade_in_fact_trade():
    expected = spark.sql("SELECT MAX(trade_ts) AS v FROM gold.fact_trade").first()["v"]
    shown = scalar('EVALUATE ROW("v", [Data as of (UTC)])')
    # The model shows UTC without a zone; Spark returns the session-zone datetime.
    if expected.tzinfo is not None:
        expected = expected.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    # The model returned a datetime 53 microseconds off Spark's on the first run (lessons.md), so compare to 10 ms.
    gap = abs((shown.to_pydatetime().replace(tzinfo=None) - expected).total_seconds())
    assert gap < 0.01, f"model {shown} vs fact_trade {expected}"
    return {"data_as_of": str(shown)}


def case_whale_share_matches_fact_trade_and_is_not_the_average_of_daily_values():
    model = model_by_pair(["Whale Share of Notional"])
    expected = spark_by_pair("""
        SELECT symbol, SUM(CASE WHEN notional >= {threshold} THEN notional ELSE 0 END) / SUM(notional) AS v
        FROM gold.fact_trade WHERE trade_date BETWEEN '{first}' AND '{last}' GROUP BY symbol""")
    assert_close({k: v["Whale Share of Notional"] for k, v in model.items()}, {k: v["v"] for k, v in expected.items()}, "whale share")
    daily = spark_by_pair("""
        SELECT symbol, AVG(share) AS v FROM (
            SELECT symbol, trade_date, SUM(CASE WHEN notional >= {threshold} THEN notional ELSE 0 END) / SUM(notional) AS share
            FROM gold.fact_trade WHERE trade_date BETWEEN '{first}' AND '{last}' GROUP BY symbol, trade_date)
        GROUP BY symbol""")
    differing = [k for k in expected if abs(expected[k]["v"] - daily[k]["v"]) > 1e-6]
    assert differing, "no pair has a whale share different from the average of its daily shares; the check proves nothing"
    return {"pairs_checked": len(expected), "pairs_where_average_of_daily_differs": len(differing)}


def case_vwap_matches_fact_trade_and_is_not_the_average_of_daily_vwaps():
    model = model_by_pair(["VWAP"])
    expected = spark_by_pair("""
        SELECT symbol, SUM(notional) / SUM(qty) AS v
        FROM gold.fact_trade WHERE trade_date BETWEEN '{first}' AND '{last}' GROUP BY symbol""")
    assert_close({k: v["VWAP"] for k, v in model.items()}, {k: v["v"] for k, v in expected.items()}, "vwap")
    daily = spark_by_pair("""
        SELECT symbol, AVG(vwap) AS v FROM (
            SELECT symbol, trade_date, SUM(notional) / SUM(qty) AS vwap
            FROM gold.fact_trade WHERE trade_date BETWEEN '{first}' AND '{last}' GROUP BY symbol, trade_date)
        GROUP BY symbol""")
    differing = [k for k in expected if abs(expected[k]["v"] - daily[k]["v"]) > 1e-9 * max(1, abs(expected[k]["v"]))]
    assert differing, "no pair has a week VWAP different from the average of its daily VWAPs; the check proves nothing"
    return {"pairs_checked": len(expected), "pairs_where_average_of_daily_differs": len(differing)}


def case_taker_buy_share_and_notional_match_fact_trade():
    model = model_by_pair(["Notional", "Taker Buy Share", "# Trades"])
    expected = spark_by_pair("""
        SELECT symbol, SUM(notional) AS notional, COUNT(*) AS trades,
               SUM(CASE WHEN side = 'buy' THEN notional ELSE 0 END) / SUM(notional) AS buy_share
        FROM gold.fact_trade WHERE trade_date BETWEEN '{first}' AND '{last}' GROUP BY symbol""")
    assert_close({k: v["Notional"] for k, v in model.items()}, {k: v["notional"] for k, v in expected.items()}, "notional")
    assert_close({k: v["# Trades"] for k, v in model.items()}, {k: v["trades"] for k, v in expected.items()}, "trades")
    assert_close({k: v["Taker Buy Share"] for k, v in model.items()}, {k: v["buy_share"] for k, v in expected.items()}, "taker buy share")
    return {"pairs_checked": len(expected)}


def case_high_low_open_close_roll_up_by_rule():
    model = model_by_pair(["High", "Low", "Open", "Close"])
    fact = spark_by_pair("""
        SELECT symbol, MAX(price) AS high, MIN(price) AS low
        FROM gold.fact_trade WHERE trade_date BETWEEN '{first}' AND '{last}' GROUP BY symbol""")
    assert_close({k: v["High"] for k, v in model.items()}, {k: v["high"] for k, v in fact.items()}, "high")
    assert_close({k: v["Low"] for k, v in model.items()}, {k: v["low"] for k, v in fact.items()}, "low")
    # Open is the open of the first day and close the close of the last day with trades in the week.
    first_open = spark_by_pair("""
        SELECT a.symbol, a.open AS v FROM gold.agg_pair_daily a JOIN (
            SELECT symbol, MIN(trade_date) AS d FROM gold.agg_pair_daily WHERE trade_date BETWEEN '{first}' AND '{last}' GROUP BY symbol) f
        ON a.symbol = f.symbol AND a.trade_date = f.d""")
    last_close = spark_by_pair("""
        SELECT a.symbol, a.close AS v FROM gold.agg_pair_daily a JOIN (
            SELECT symbol, MAX(trade_date) AS d FROM gold.agg_pair_daily WHERE trade_date BETWEEN '{first}' AND '{last}' GROUP BY symbol) f
        ON a.symbol = f.symbol AND a.trade_date = f.d""")
    assert_close({k: v["Open"] for k, v in model.items()}, {k: v["v"] for k, v in first_open.items()}, "open")
    assert_close({k: v["Close"] for k, v in model.items()}, {k: v["v"] for k, v in last_close.items()}, "close")
    return {"pairs_checked": len(fact)}


def case_pairs_no_longer_tracked_keep_their_history():
    untracked = [r["symbol"] for r in spark.sql(
        "SELECT p.symbol FROM gold.dim_pair p JOIN (SELECT DISTINCT symbol FROM gold.fact_trade) f "
        "ON p.symbol = f.symbol WHERE NOT p.is_tracked").collect()]
    total_model = scalar('EVALUATE ROW("v", [Notional])')
    total_fact = spark.sql("SELECT SUM(notional) AS v FROM gold.fact_trade").first()["v"]
    assert abs(total_model - total_fact) <= TOLERANCE * max(1, total_fact), f"all-pairs notional: model {total_model} vs fact_trade {total_fact}"
    if untracked:
        df = dax("EVALUATE SUMMARIZECOLUMNS('Pair'[Pair], \"v\", [Notional])")
        shown = {r["Pair"] for _, r in df.iterrows() if r["v"]}
        assert set(untracked) <= shown, f"untracked pairs with trades missing from the model: {sorted(set(untracked) - shown)}"
    return {"untracked_pairs_with_trades": untracked, "total_notional": total_fact}


def case_whale_threshold_in_the_model_matches_vl_crypto():
    lib = notebookutils.variableLibrary.getLibrary("vl_crypto")
    assert whale_threshold() == int(lib.whale_threshold_usd), f"model {whale_threshold()} vs vl_crypto {lib.whale_threshold_usd}"
    return {"threshold": whale_threshold()}


def case_price_measures_are_blank_unless_one_pair_is_in_context():
    # Open, high, low, close and VWAP mix base units and prices of different pairs, so they only answer for one pair.
    row = dax('EVALUATE ROW("open", [Open], "high", [High], "low", [Low], "close", [Close], "vwap", [VWAP])').iloc[0]
    shown = {k: v for k, v in row.items() if not pd.isna(v)}
    assert not shown, f"price measures shown across all pairs: {shown}"
    return {"all_pairs": "blank"}


def case_data_as_of_ignores_pair_and_date_filters():
    first, _ = window()
    pair = spark.sql("SELECT MIN(symbol) AS s FROM gold.fact_trade").first()["s"]
    unfiltered = scalar('EVALUATE ROW("v", [Data as of (UTC)])')
    filtered = scalar(f"EVALUATE ROW(\"v\", CALCULATE([Data as of (UTC)], 'Pair'[Pair] = \"{pair}\", "
                      f"'Date'[Date] = DATE({first.year}, {first.month}, {first.day})))")
    assert filtered == unfiltered, f"filtered {filtered} vs unfiltered {unfiltered}"
    return {"pair": pair, "date": str(first)}


CASES = [
    case_relationships_follow_the_documented_foreign_keys,
    case_data_as_of_is_the_newest_trade_in_fact_trade,
    case_whale_share_matches_fact_trade_and_is_not_the_average_of_daily_values,
    case_vwap_matches_fact_trade_and_is_not_the_average_of_daily_vwaps,
    case_taker_buy_share_and_notional_match_fact_trade,
    case_high_low_open_close_roll_up_by_rule,
    case_pairs_no_longer_tracked_keep_their_history,
    case_whale_threshold_in_the_model_matches_vl_crypto,
    case_price_measures_are_blank_unless_one_pair_is_in_context,
    case_data_as_of_ignores_pair_and_date_filters,
]

results = []
for case in CASES:
    try:
        results.append({"case": case.__name__, "passed": True, "detail": case()})
    except Exception:
        results.append({"case": case.__name__, "passed": False, "error": traceback.format_exc()})

summary = {"passed": sum(r["passed"] for r in results), "failed": sum(not r["passed"] for r in results), "cases": results}
notebookutils.fs.put(RESULT_PATH, json.dumps(summary, indent=2, default=str), True)
print(json.dumps(summary, indent=2, default=str))
if summary["failed"]:
    raise RuntimeError(f"{summary['failed']} case(s) failed: " + ", ".join(r["case"] for r in results if not r["passed"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
