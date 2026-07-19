import { describe, expect, it } from 'vitest';
import { rawChunkShardPrefix } from '@pou/shared';
import { ChunkStore, assignedChunkIndices } from '../chunks/chunk-store.js';
import { LEAGUE, entry, makeRunHarness } from '../../test/run-harness.js';
import { buildLadder } from '../../test/mock-api.js';
import { readAllShards, tallyOutcomes } from '../../test/helpers.js';

describe('assignedChunkIndices (the no-shared-chunk guarantee)', () => {
  it('partitions pending chunks disjointly and completely across workers', () => {
    const pending = [0, 2, 3, 7, 8, 11];
    const w0 = assignedChunkIndices(pending, 0, 3);
    const w1 = assignedChunkIndices(pending, 1, 3);
    const w2 = assignedChunkIndices(pending, 2, 3);
    expect(w0).toEqual([0, 3]);
    expect(w1).toEqual([7]);
    expect(w2).toEqual([2, 8, 11]);
    // Disjoint and complete.
    expect([...w0, ...w1, ...w2].sort((a, b) => a - b)).toEqual(pending);
    // Stable: ownership is keyed on the chunk index, so a worker that reads the
    // chunk states later (after another worker already resolved some) still
    // computes the same ownership for the chunks that remain.
    expect(assignedChunkIndices([3, 7, 11], 0, 3)).toEqual([3]);
  });
});

describe('Worker chunk processing', () => {
  it('two workers resolve disjoint chunks; together they resolve everything exactly once', async () => {
    const entries = buildLadder(20);
    const h = makeRunHarness({ entries, config: { chunkSize: 5, workerCount: 2 } });
    await h.createFire();

    const w0 = await h.newWorker(0).runOnce();
    const w1 = await h.newWorker(1).runOnce();

    expect(w0.assignedChunks).toBe(2);
    expect(w1.assignedChunks).toBe(2);
    expect(w0.chunksResolved + w1.chunksResolved).toBe(4);

    const chunks = await new ChunkStore(h.objectStore).loadAll(LEAGUE, 'snap-fixed', 4);
    const tally = tallyOutcomes(chunks.flatMap((c) => c.characters));
    expect(tally.pending).toBe(0);
    expect(tally.ok).toBeGreaterThan(0);

    // Ownership is visible in the chunk files and disjoint by construction:
    // chunkIndex % 2 → w0 owns {0,2}, w1 owns {1,3}.
    expect(chunks.map((c) => c.workerIndex)).toEqual([0, 1, 0, 1]);

    // Every ok character was fetched exactly once, by exactly one worker.
    expect((await readAllShards(h.objectStore)).length).toBe(tally.ok);
    expect(h.api.itemCalls.get('acct-1/char-1')).toBe(1);
  });

  it('checkpoints mid-chunk on budget exhaustion and resumes without re-fetching', async () => {
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({
      entries,
      config: { chunkSize: 10, workerCount: 1, maxRunMillis: 20_000 },
    });
    await h.createFire();

    const first = await h.newWorker(0).runOnce();
    expect(first.stopReason).toBe('budget_exhausted');

    const store = new ChunkStore(h.objectStore);
    const afterFirst = await store.load(LEAGUE, 'snap-fixed', 0);
    const resolved = afterFirst.characters.filter((c) => c.outcome === 'ok');
    expect(resolved.length).toBeGreaterThan(0);
    expect(resolved.length).toBeLessThan(10);
    // The partial visit produced a durable shard with exactly those records.
    expect((await readAllShards(h.objectStore)).length).toBe(resolved.length);

    // Next run (fresh worker process) finishes without re-fetching.
    const second = await h.newWorker(0).runOnce();
    expect(second.stopReason).toBe('assigned_drained');
    const done = await store.load(LEAGUE, 'snap-fixed', 0);
    expect(done.characters.every((c) => c.outcome === 'ok')).toBe(true);
    for (const c of resolved) {
      expect(h.api.itemCalls.get(`${c.account}/${c.character}`)).toBe(1);
    }
    // Two visits → two shards for the chunk, under its own prefix.
    const shardKeys = h.objectStore
      .keys()
      .filter((k) => k.startsWith(rawChunkShardPrefix(LEAGUE, 'snap-fixed', 0)));
    expect(shardKeys).toHaveLength(2);
    expect(done.shardsWritten).toBe(2);
  });

  it('stops resumably on a rate-limit block, keeping computed outcomes durable', async () => {
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'throttle' }));
    const h = makeRunHarness({ entries, config: { chunkSize: 5, workerCount: 1 } });
    await h.createFire();

    const summary = await h.newWorker(0).runOnce();

    expect(summary.stopReason).toBe('rate_limited');
    const chunks = await new ChunkStore(h.objectStore).loadAll(LEAGUE, 'snap-fixed', 2);
    const tally = tallyOutcomes(chunks.flatMap((c) => c.characters));
    // Nothing was falsely resolved; the block left entries pending for later.
    expect(tally.pending).toBeGreaterThan(0);
    expect(tally.dead).toBe(0);
  });

  it('applies the outcome policy per character (ok / private / dead / retryable→dead)', async () => {
    const entries = [
      entry('ok', { kind: 'ok' }),
      entry('priv', { kind: 'private' }),
      entry('dead', { kind: 'dead' }),
      entry('recovers', { kind: 'flaky', fails: 1 }), // retryable → ok next run
      entry('gone', { kind: 'flaky', fails: 10 }), // retryable → dead (attempts exhausted)
    ];
    const h = makeRunHarness({ entries, config: { chunkSize: 5, workerCount: 1 } });
    await h.createFire();

    // Three runs: enough for `recovers` to heal and `gone` to exhaust 3 attempts.
    await h.newWorker(0).runOnce();
    await h.newWorker(0).runOnce();
    await h.newWorker(0).runOnce();

    const chunk = await new ChunkStore(h.objectStore).load(LEAGUE, 'snap-fixed', 0);
    const byKey = new Map(chunk.characters.map((q) => [`${q.account}/${q.character}`, q]));
    expect(byKey.get('acct-ok/char-ok')?.outcome).toBe('ok');
    expect(byKey.get('acct-priv/char-priv')?.outcome).toBe('private');
    expect(byKey.get('acct-dead/char-dead')?.outcome).toBe('dead');
    expect(byKey.get('acct-recovers/char-recovers')?.outcome).toBe('ok');
    const gone = byKey.get('acct-gone/char-gone');
    expect(gone?.outcome).toBe('dead');
    expect(gone?.attempts).toBe(3);
  });

  it('does nothing when no snapshot is collecting', async () => {
    const h = makeRunHarness({ entries: buildLadder(5) });
    const summary = await h.newWorker(0).runOnce();
    expect(summary.stopReason).toBe('no_work');
    expect(summary.requests).toBe(0);
  });

  it('leaves an over-age snapshot alone (finalize owns the abort)', async () => {
    const h = makeRunHarness({ entries: buildLadder(5), config: { maxAgeHours: 1 } });
    await h.createFire();
    h.clock.advance(2 * 3_600_000);

    const summary = await h.newWorker(0).runOnce();
    expect(summary.stopReason).toBe('no_work');
    expect(summary.requests).toBe(0);
  });
});
