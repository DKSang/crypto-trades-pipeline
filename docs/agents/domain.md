# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

Layout: **single-context**, but placed inside the Fabric brain (`reference/`) instead of the repo root, so it matches `AGENTS.md`.

## Before exploring, read these

- **`reference/glossary.md`**: business terms (this repo's equivalent of `GLOSSARY.md`).
- **`reference/decisions/`**: hard-to-reverse choices (this repo's equivalent of `docs/adr/`). Read the ones that touch the area you're about to work in.
- The other `reference/` files relevant to the task (see the index in `AGENTS.md`).

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. They are created lazily when terms or decisions actually get resolved.

Never read `docs/prior-art/` as a source of terms or decisions: it is an old design with known wrong decisions.

## File structure

```
/
├── AGENTS.md
└── reference/
    ├── glossary.md
    └── decisions/
        ├── 0001-<decision>.md
        └── 0002-<decision>.md
```

When `/domain-modeling` (or any skill) would write `GLOSSARY.md` or `docs/adr/NNNN-*.md`, write `reference/glossary.md` or `reference/decisions/NNNN-*.md` instead, and add the file to the reference index in `AGENTS.md` the first time it is created.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `reference/glossary.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal: either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag decision conflicts

If your output contradicts an existing decision in `reference/decisions/`, surface it explicitly rather than silently overriding:

> _Contradicts decision 0007 (event-sourced orders), but worth reopening because…_
