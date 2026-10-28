# Fabric environment

## Tenant

- **Tenant ID**: `370fa88c-de52-4400-b6cd-027ef6a8ddf2`
- **Agent identity**: service principal, app ID `b593a632-afcd-4c1b-b0bd-43080c0317ab`, principal ID `a93074e9-54c0-4ced-b846-143d8a51162c`. Secret in `.env` (`sp_secret`), never written here. `fab` and `az` both sign in as it via `scripts/fabric-login.sh`.

## Scope

| Workspace | ID | Stage | Agent may write? |
| --- | --- | --- | --- |
| Polymarket-DEV | `c8d6538e-e60c-467b-8440-dec5dae4d897` | DEV | yes, without asking first; report what changed |

The service principal can see (and may be able to write to) `Polymarket-old-DEV`; only the rule above keeps the agent out of it.

Every workspace not listed here is out of scope: read only if the task needs it, never modify.
