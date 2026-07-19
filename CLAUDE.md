# CLAUDE.md

## What this is

A scheduled **Path of Exile ladder build collector**. It reads the legacy public
ladder endpoints into a growing per-league character roster, snapshots every
known character's full build data (gear + passives) via parallel worker jobs,
transforms the raw data to Parquet with DuckDB, and publishes it incrementally
to an S3-compatible object store (Cloudflare R2). Runs on free tiers
(GitHub Actions + R2).

## Layout

```
├── .github/workflows/   # ci, create-snapshot (cadence), snapshot (collect), claude-diagnostics
├── collector/           # TypeScript: coordinate / work / finalize + transform
├── shared/              # data contracts (SCHEMA_VERSION, R2 key layout, schema)
└── config/              # collector.json + leagues.json (league → tree version)
```

`shared/` is the schema contract. It is a **copy** kept in sync with a separate
consumer (the web reader) by hand — a change here is a schema change: bump
`SCHEMA_VERSION` and update both copies together.

## Hard rules (do not violate)

1. **Respect the API.** Identifiable `User-Agent` with a reachable contact
   address on every request (`COLLECTOR_CONTACT_EMAIL`; the collector throws
   without it). Parse `X-Rate-Limit-*`, honor `Retry-After`, back off on 429.
   Repeated 4xx trips a client block — checkpoint and stop when in doubt.
2. **Public profiles only.** Private profiles are marked and skipped. Never
   attempt any privacy bypass.
3. **Resumable by construction.** State lives in R2; each run is a re-entrant
   continuation off the checkpoint + chunk files. Never assume a snapshot fits in
   one run. A chunk has exactly one owning worker per run
   (`chunkIndex % workerCount`) — never let two writers share an R2 object.
4. **Completed snapshots are immutable.** An in-progress snapshot
   (`complete: false`) is republished in place; the moment it publishes as
   complete it is frozen. Fixes are new transforms under a bumped
   `SCHEMA_VERSION`, not in-place edits.
5. **Stay inside free tiers.** No paid features, no always-on servers.
6. **The product token sent to GGG must stay neutral** — identifiability is
   carried by the contact address, not by leaking a project name.

## Conventions

- TypeScript everywhere; transform logic in DuckDB SQL.
- Testing trophy, integration-first. Priority targets: collector state machine
  (checkpoint/resume/abort), rate limiter (header parsing, backoff), transform
  SQL (golden-file inputs → expected Parquet/aggregates).
- Commits: imperative mood, scope prefix when useful (`collector:`, `docs:`).

## Diagnostics (for agents)

To inspect or repair live infrastructure (R2 state, GGG connectivity), **do not
ask for credentials**. Work through the **`ClaudeDiagnostics`** workflow
(`.github/workflows/claude-diagnostics.yml`), which injects the encrypted
`R2_*` / `COLLECTOR_CONTACT_EMAIL` secrets from GitHub's secret store.

1. Write a script at `collector/scripts/diagnostics/<name>.ts`, reading what it
   needs from `process.env`. A script that **mutates** state MUST default to a
   dry run and require an explicit `--apply` flag.
2. Commit it to the branch you dispatch from (`workflow_dispatch` only lists the
   workflow once it's on the default branch).
3. Run **ClaudeDiagnostics** with `script = scripts/diagnostics/<name>.ts`. The
   runner constrains `script` to `scripts/diagnostics/`, so only committed
   diagnostics can execute.
