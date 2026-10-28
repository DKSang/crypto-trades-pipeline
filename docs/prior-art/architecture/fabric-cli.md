# Fabric CLI notes

- **Version**: fab 1.7.0
- **Deploy identity**: owner preference 2026-10-28: keep `fab` and `fntk` on the signed-in user account. Do not call `loop login` or CLI paths that call `login()`; import directly and call loop functions directly ([Test loop](../docs/environment.md#test-loop)). REST `default_client()` may use service principal app `b593a632-afcd-4c1b-b0bd-43080c0317ab`, tenant `370fa88c-de52-4400-b6cd-027ef6a8ddf2`, which sees only `Polymarket-DEV`. Secret in `.env`; never written here.
- **Verified**: 2026-10-25 `/setup-fabric-engineering-skills verify`: fab login, workspace visibility, az same tenant + Fabric token, MCP `microsoft-learn` / `fabric-mcp` / `fabric-rti-mcp` answer. `powerbi-modeling-mcp` (optional) fails with FeatureNotAvailable.
- **Docs**: https://aka.ms/fabric-cli. Run `fab <command> --help` before using a command for the first time in a session; flags change between versions.

## Paths

- Workspace: `"<Workspace name>.Workspace"`; item: `"<Workspace>.Workspace/<Item>.<Type>"` (e.g. `"Polymarket-DEV.Workspace/lh_polymarket.Lakehouse"`).
- Quote every path; names can contain spaces.
- Lakehouse content: `.../<Lakehouse>.Lakehouse/Files/...` and `.../<Lakehouse>.Lakehouse/Tables/<schema>/<table>`.

## Everyday commands

| Task | Command |
| --- | --- |
| List workspaces / items | `fab ls` / `fab ls "Polymarket-DEV.Workspace" -l` |
| Does it exist? | `fab exists "<ws>.Workspace/<item>.<Type>"` |
| Properties (IDs etc.) | `fab get "<ws>.Workspace/<item>.<Type>" -q id` |
| What can I do with this? | `fab desc .<Type>` |
| Export a definition to local files | `fab export "<ws>.Workspace/<item>.<Type>" -o ./<folder>` |
| Upload a local file | `fab cp ./data/<file> "<ws>.Workspace/<lh>.Lakehouse/Files/<path>/<file>"` |
| Import a definition folder | `fab import "<ws>.Workspace/<item>.<Type>" -i <folder> -f` (notebooks in `.py` format: add `--format .py`) |
| Start a job, then poll | `fab job start "<path>"`, `fab job run-list "<path>"`, `fab job run-status "<path>" --id <job-id>` |
| Raw REST call | `fab api -X get workspaces` |

The repo's deploy/run/check loop wraps these: `python -m fabric.deploy.loop` (see `docs/environment.md`, "Test loop").

## Gotchas

- `fab import` always asks "Are you sure?", which the agent's shell can't answer: it needs `-f`.
- `fab job run --timeout N` cancels the job when the timeout hits (config `job_cancel_ontimeout`, default true). For long jobs use `fab job start` and poll.
- Notebook import defaults to `.ipynb`; a `notebook-content.py` folder needs `--format .py`.
- `fab exists` / `fab ls` give false negatives for KQL items (observed 2026-10-22); trust a catalog or engine query over them. See `docs/environment.md`.

## Notebook toolkit (`fntk`)

Interactive loop on one live Spark session: run a snippet, read output and logs, edit, rerun, without redeploying or rerunning the notebook. Preview, public alpha; `fabric-notebook-toolkit` 0.0.1a12 installed 2026-10-28. Docs: https://learn.microsoft.com/en-us/fabric/data-engineering/fabric-notebook-toolkit

- **Identity**: verified 2026-10-28 with the owner's user account (`fab auth login` + `az login`, same tenant). Still restricted to `Polymarket-DEV` only. Service-principal `fntk` is unverified; REST may use it. Keep `fab` on the user identity too. `fntk discover whoami --json` shows who it runs as; a first call after a sign-in change may return `AUTH_STATE_CHANGED`, just rerun.
- **IDs**: workspace `89c341b0-55ba-4b20-ac0f-18de88049aad`; notebook ids from `fntk discover list-notebooks -w <ws> --json` (`nb_session_silver` = `8ca6b2a1-16fb-4417-8508-bbce8b575a0f`).
- **Session**: `fntk notebook start-session -w <ws> -a <nb> --json`, poll `get-session` until `livy_session_state` is `idle` (about 10 s). End with `fntk notebook delete-session ... --session-id <id>` and `fntk server stop`; stopping the local server does not end the remote session.
- **Lakehouse**: the session starts without a default lakehouse (`fntk notebook get-lakehouse` shows null even though the item metadata has `lh_polymarket`). First run, session-only, no item change:
  ```
  %%configure -f
  {"defaultLakehouse": {"name": "lh_polymarket", "id": "8d330969-9f75-4974-ac37-988194246f64", "workspaceId": "89c341b0-55ba-4b20-ac0f-18de88049aad"}}
  ```
- **Run**: `fntk code run -w <ws> -a <nb> --code "<python>" --json` returns an `op_id` at once; poll `fntk code get-result ... --execution-id <op_id> --json` until not `running`. Outputs are in `data.outputs[].text`; exceptions in `error.message`. Variables persist between runs (OBSERVED: table count 13 s, reuse of the variable 1 s).
- **Rules**: the repo stays the source (ADR 0009). Use `code run` to try code; do not `fntk cell update` / `put-content` / `set-lakehouse` (they write the item and the next `fab import` overwrites them). Point snippets at `scn_<name>` tables, never write production schemas.
- **Edit loop**: after attaching the lakehouse, run setup once, then iterate on single snippets with `code run` / `code get-result`. Copy the working snippet into `fabric/Polymarket-DEV/<nb>.Notebook/notebook-content.py`, run `pytest fabric -q`, preview `python -m fabric.deploy.loop deploy <nb>`, then deploy with `--apply`. Run one full `python -m fabric.deploy.loop play late-arrival --apply` as evidence before commit ([Test loop](../docs/environment.md#test-loop)); end the remote session and stop the local server as above.
- **Gold**: `fntk` helps Silver/notebook code. Gold is a dbt job, not a notebook; only compiled Gold SQL can be tried with `spark.sql` in a session against scenario tables. `dbt run` stays in the dbt job.
