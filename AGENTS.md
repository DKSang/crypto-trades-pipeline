<!-- fabric-engineering-skills:start -->
# Fabric brain

This folder is the second brain for the Polymarket pipeline's Microsoft Fabric development. It carries our conventions, projects and rules into every AI session, so nobody has to explain them twice. Read this file at the start of every session. It is the canonical instruction file for any AI tool working in this folder.

## Start of session

Before any Fabric work, read the `reference/` files relevant to the task (index below). Use their names and vocabulary verbatim. If a request contradicts them, say so before acting.

`docs/prior-art/` is an old design with known wrong decisions. Never use it as a source for conventions or design.

## Guardrails

- **Scope.** You may create or modify items only in `Polymarket-DEV`. Every other workspace (including `Polymarket-old-DEV`) is read-only; never modify it, even if asked casually, without explicit confirmation naming the workspace.
- **No confirmation in DEV.** In `Polymarket-DEV`, run writes (create, import, run, delete) without asking first. After each change, say what you ran and what it changed. `fab` asks its own confirmation that your shell can't answer, so some commands (e.g. `fab import`) need `-f`.
- **Secrets.** Never print, log or write tokens, passwords, client secrets or connection strings. Never read or echo `.env` values. Don't run commands that print access tokens.
- **Identity.** `fab` and `az` run as the service principal whose credentials are in `.env`. If `fab auth status` shows signed out or a token expired, run `sh scripts/fabric-login.sh`.

## Verify before you claim

Fabric changes monthly and your training data has a cutoff. Before stating that Fabric supports (or doesn't support) a feature, setting, API or limit, verify it against Microsoft Learn using the `microsoft-learn` MCP server and cite the page. If you can't verify it, say so.

## Build, verify, test

If the `build-in-fabric` skill is available, use it for any build. Otherwise:

1. Plan from `reference/` (naming, architecture) and confirm the plan.
2. Build in the writable workspace only, following the guardrails.
3. Verify each item exists and matches the plan (`fab ls`, `fab get`).
4. Offer an end-to-end test: run it, check the output data, and if it fails, diagnose, fix, and re-run. Report what failed and why.

## Keep the brain current

When the user explains something durable (a convention, an environment fact, a gotcha, a correction), update the right `reference/` file in the same turn and say in one line what you recorded. Corrections matter most: fix the line that misled you. Never record secrets. If the `fabric-brain` skill is available, use it: it has the routing and formats.

## Reference

- `reference/environment.md`: tenant, workspaces, what is in scope
- `reference/naming-conventions.md`: how items, tables, columns and activities are named
- `reference/fabric-cli.md`: how to use `fab` in this repo, including sign-in

Created when first needed, then listed here: `reference/architecture/` (one file per pattern), `reference/lessons.md` (gotchas and fixes), `reference/glossary.md` (business terms), `reference/decisions/` (hard-to-reverse choices), `reference/workspaces/` (per-workspace inventories).

## Folders

- `data/`: sample files for development. Never production or personal data.
- `scripts/fabric-login.sh`: signs `fab` and `az` in as the service principal from `.env` without printing secrets.

## Tools

- **Fabric CLI** (`fab`): create, inspect and run items. Notes in `reference/fabric-cli.md`.
- **Microsoft Learn MCP** (`microsoft-learn`): current Microsoft documentation.
- **Fabric MCP** (`fabric-mcp`): Fabric API specs, item definition schemas and best practices; read OneLake files, tables and workspaces. Read-only: make changes with `fab`. Before writing or changing an item definition, check it against `docs_item-definitions`.
- **Fabric RTI MCP** (`fabric-rti-mcp`): KQL queries against the Eventhouse named by the `KUSTO_SERVICE_URI` environment variable. Not configured until an Eventhouse exists.
<!-- fabric-engineering-skills:end -->

## Agent skills

### Issue tracker

Issues live in GitHub Issues for `DKSang/Polymarket-pipeline`, via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

The five default labels: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context, kept in the brain: `reference/glossary.md` and `reference/decisions/`. See `docs/agents/domain.md`.
