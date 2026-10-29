"""Runs KQL on a Fabric KQL database as the signed-in `az` identity.

    python scripts/kql-run.py <query|mgmt> <query-uri> <database> <file.kql>

`mgmt` posts to /v1/rest/mgmt (anything starting with `.`), `query` to /v1/rest/query.
A `mgmt` file holds one command per blank-line-separated block; `//` comment lines are dropped
and the commands run in order, stopping at the first failure. {{name}} in the file becomes the
value of that variable in vl_crypto. Run it from the repo root. Prints each row of the last
result table as JSON. The token stays in this process.
"""
import json
import re
import subprocess
import sys
import urllib.error
import urllib.request

endpoint, uri, database, path = sys.argv[1:5]
if endpoint not in ("query", "mgmt"):
    sys.exit("first argument must be query or mgmt")

token = subprocess.run(
    "az account get-access-token --resource https://kusto.kusto.windows.net --query accessToken -o tsv",
    capture_output=True, text=True, check=True, shell=True,
).stdout.strip()

with open(path, encoding="utf-8") as f:
    text = f.read()

with open("fabric/Crypto-DEV/vl_crypto.VariableLibrary/variables.json", encoding="utf-8") as f:
    for variable in json.load(f)["variables"]:
        text = text.replace("{{" + variable["name"] + "}}", str(variable["value"]))

if endpoint == "mgmt":
    blocks = re.split(r"\n\s*\n", text)
    commands = ["\n".join(l for l in b.splitlines() if not l.strip().startswith("//")).strip() for b in blocks]
    commands = [c for c in commands if c]
else:
    commands = [text]

for csl in commands:
    request = urllib.request.Request(
        f"{uri}/v1/rest/{endpoint}",
        data=json.dumps({"db": database, "csl": csl}).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request) as response:
            body = json.load(response)
    except urllib.error.HTTPError as error:
        sys.exit(f"HTTP {error.code} on `{csl.splitlines()[0][:100]}`: {error.read().decode('utf-8')}")

table = body["Tables"][0]
columns = [c["ColumnName"] for c in table["Columns"]]
for row in table["Rows"]:
    print(json.dumps(dict(zip(columns, row)), default=str))
