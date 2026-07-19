# pou-collector

A scheduled collector for Path of Exile ladder build data. It reads the legacy
public ladder endpoints into a growing per-league character roster, snapshots
every known character's full build (gear + passives) via parallel worker jobs,
transforms the raw data to Parquet, and publishes it incrementally to an
S3-compatible object store (Cloudflare R2). Runs entirely on free tiers
(GitHub Actions + R2).

## Layout

```
├── collector/   # TypeScript: coordinate / work / finalize + DuckDB transform
├── shared/      # data contracts (schema version, R2 key layout, table schema)
└── config/      # collector.json (league, depth, budgets, cadence)
```

## Pipeline

A cron fire runs three stages against the R2 state (see `collector/`):

- **coordinate** — capture the ladder, merge it into the per-league roster, seed
  the snapshot's pending chunks; emits the worker matrix.
- **work** — N parallel jobs, each on its own runner/IP with its own rate
  limiter, each owning a disjoint set of chunks.
- **finalize** — roll up chunk outcomes, publish (incrementally while
  incomplete, then a final immutable publish once the queue drains), retention.

## Hard rules

1. **Respect the API.** Identifiable `User-Agent` with a contact address on every
   request (set `COLLECTOR_CONTACT_EMAIL`). Honor `X-Rate-Limit-*` / `Retry-After`;
   back off on 429; checkpoint and stop on repeated 4xx.
2. **Public profiles only.** Private profiles are marked and skipped — never
   bypassed.
3. **Resumable by construction.** State lives in R2; each run is a re-entrant
   continuation off the checkpoint + chunk files. A chunk has exactly one owning
   worker per run.
4. **Completed snapshots are immutable.** An incomplete snapshot is republished
   in place; once it publishes as complete it is frozen.

## Local development

```bash
pnpm install
pnpm typecheck && pnpm lint && pnpm format && pnpm test
```

The collector talks to R2 and GGG only in CI; unit tests use in-memory stores
and recorded fixtures, so the suite runs offline.

## Configuration

Runtime secrets are read from the environment (never committed) — see
`.github/workflows/snapshot.yml` for the full list. At minimum the collector
needs the `R2_*` object-store credentials and `COLLECTOR_CONTACT_EMAIL`.
