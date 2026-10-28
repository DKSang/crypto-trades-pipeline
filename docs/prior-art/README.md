# Prior art

Copied 2026-10-28 from `polymarket-rebuild-kit` (the first attempt). Use it as evidence, not as design: source behaviour, samples and Fabric gotchas were measured and are trusted; KPI rules, glossary terms and naming are inputs to re-judge. Old ADRs, workspace IDs and `.env` were left in the kit on purpose.

| Path | Why it is here |
|---|---|
| `business/product.md` | Owner, audience, the three questions, non-goals |
| `source/source.md`, `collector.md`, `gamma-snapshot.md` | Gamma, CLOB and WebSocket behaviour already profiled |
| `source/samples/` | Profiled source samples (2026-10-21) |
| `source/real-session-sample/` | One real captured session (Bronze, manifest, mapping): test fixture |
| `architecture/lessons.md` | Fabric, source and measurement traps already paid for |
| `architecture/kpi.md` | Previous KPI rules and Gold star (re-judge, likely simplify) |
| `architecture/GLOSSARY.md`, `naming-conventions.md`, `fabric-cli.md` | Vocabulary, item naming, `fab` usage |
