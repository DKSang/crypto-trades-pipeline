# Fabric CLI notes

- **Version**: fab 1.7.0 (1.8.0 available)
- **Signed in as**: service principal from `.env` (verified 2026-10-28: `fab` and `az` same tenant, both workspaces visible). Never write the secret.
- **Sign-in**: `sh scripts/fabric-login.sh` signs in both `fab` and `az` (Fabric MCP uses the `az` sign-in). Check with `fab auth status`.
- **Docs**: https://aka.ms/fabric-cli. Run `fab <command> --help` before using a command for the first time in a session; flags change between versions.

## Paths

- Workspace: `"<Workspace name>.Workspace"`; item: `"<Workspace>.Workspace/<Item>.<Type>"` (e.g. `"Crypto-DEV.Workspace/lh_crypto.Lakehouse"`).
- Quote every path; names can contain spaces.
- Lakehouse content: `.../<Lakehouse>.Lakehouse/Files/...` and `.../<Lakehouse>.Lakehouse/Tables/<schema>/<table>`.

## Everyday commands

| Task | Command |
| --- | --- |
| List workspaces / items | `fab ls` / `fab ls "<ws>.Workspace" -l` |
| Does it exist? | `fab exists "<ws>.Workspace/<item>.<Type>"` |
| Properties (IDs etc.) | `fab get "<ws>.Workspace/<item>.<Type>" -q id` |
| What can I do with this? | `fab desc .<Type>` |
| Export a definition to local files | `fab export "<ws>.Workspace/<item>.<Type>" -o ./<folder>` |
| Create a lakehouse with schemas | `fab mkdir "<ws>.Workspace/<lh>.Lakehouse" -P enableSchemas=true` |
| Upload a local file | `fab cp ./data/<file> "<ws>.Workspace/<lh>.Lakehouse/Files/<path>/<file>"` |
| Import a definition folder | `fab import "<ws>.Workspace/<item>.<Type>" -i <folder> -f` (notebooks in `.py` format: add `--format .py`) |
| Start a job, then poll | `fab job start "<path>"`, `fab job run-list "<path>"`, `fab job run-status "<path>" --id <job-id>` |
| Raw REST call | `fab api -X get workspaces` |

## Gotchas

- `fab import` always asks "Are you sure?", which the agent's shell can't answer: it needs `-f`.
- `fab job run --timeout N` cancels the job when the timeout hits (config `job_cancel_ontimeout`, default true). For long jobs use `fab job start` and poll.
- Notebook import defaults to `.ipynb`; a `notebook-content.py` folder needs `--format .py`.
- `fab set "<ws>.Workspace" -q displayName -i <new>` needs the workspace Admin role; as Member/Contributor it fails with `InsufficientPrivileges` (observed 2026-10-28). Item renames (`fab set "<ws>.Workspace/<item>" -q displayName -i <new> -f`) work without Admin.
- A REST connection is created with `fab mkdir ".connections/<name>.Connection" -P connectionDetails.type=RestService,connectionDetails.creationMethod=RestService.Contents,connectionDetails.parameters.baseUrl=<url>,connectionDetails.parameters.serverCertificateValidation=true,credentialDetails.type=Anonymous,privacyLevel=Public` (OBSERVED 2026-10-28; without `creationMethod` it fails "Invalid creation method"). Connections live in the tenant, not in `Crypto-DEV`: ask before creating one.
- `fab` tokens expire after about an hour: `[AuthenticationFailed] Failed to get access token` → `sh scripts/fabric-login.sh`.
