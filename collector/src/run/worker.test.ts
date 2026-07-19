import { describe, expect, it } from 'vitest';
import { rawChunkShardPrefix } from '@pou/shared';
import { getJson, putJson } from '../checkpoint/object-store.js';
import { ChunkStore, assignedChunkIndices, pendingChunkIndices } from '../chunks/chunk-store.js';
import { LimiterStateStore, workerSlot } from '../rate-limit/limiter-store.js';
import type { HttpClient } from '../sources/types.js';
import { workerDonePath, type WorkerDoneMarker } from './worker-quorum.js';
import { COLLECT_CRON_GAP_MS, LEAGUE, entry, makeRunHarness } from '../../test/run-harness.js';
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

    // Next run (fresh worker process, a cron-gap later so the paced windows
    // have drained) finishes without re-fetching.
    h.clock.advance(COLLECT_CRON_GAP_MS);
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

  it('detects a saturated long window and checkpoints instead of idling (rate_limit_stall)', async () => {
    // The production incident: the limiter memory restored from the previous
    // run's checkpoint holds a full 90-req/30-min window, so the next request
    // slot is >20 minutes out. The worker must detect that and exit at once —
    // not sleep through its whole run budget on an idle runner.
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({ entries, config: { chunkSize: 10, workerCount: 1 } });
    await h.createFire();

    const saturatedAt = h.clock.now() - 1_000;
    await new LimiterStateStore(h.objectStore).save(
      LEAGUE,
      workerSlot(0),
      {
        observedRules: [
          {
            name: 'Ip',
            limits: [
              { hits: 30, periodSec: 60, penaltySec: 120 },
              { hits: 90, periodSec: 1800, penaltySec: 600 },
            ],
            state: [],
          },
        ],
        penaltyUntil: 0,
        consecutiveThrottles: 0,
        consecutiveErrors: 0,
        // floor(90 * 0.9) = 81 recent hits — the 30-min window is full and the
        // next slot is ~1799s away, far past maxWaitMillis (300s).
        recentAcquires: Array.from({ length: 81 }, () => saturatedAt),
      },
      new Date(h.clock.now()).toISOString(),
    );

    const start = h.clock.now();
    const summary = await h.newWorker(0).runOnce();

    expect(summary.stopReason).toBe('rate_limit_stall');
    expect(summary.requests).toBe(0);
    expect(h.clock.now()).toBe(start); // exited immediately — never slept the ~30 min
    expect(h.logs.some((l) => l.includes('too long to idle'))).toBe(true);

    // A cron-gap later the window has drained and collection proceeds normally.
    h.clock.advance(COLLECT_CRON_GAP_MS);
    const next = await h.newWorker(0).runOnce();
    expect(next.stopReason).toBe('assigned_drained');
    expect(next.requests).toBeGreaterThan(0);
  });

  it("collects immediately on a new runner IP instead of stalling on the old IP's spend", async () => {
    // The 2026-07-19 incident, second act: the checkpointed saturated window
    // belonged to the PREVIOUS runner's IP. On a fresh runner (fresh per-IP
    // budget server-side) the carried spend must not self-throttle the run.
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({ entries, config: { chunkSize: 10, workerCount: 1 } });
    await h.createFire();

    const saturatedAt = h.clock.now() - 1_000;
    await new LimiterStateStore(h.objectStore).save(
      LEAGUE,
      workerSlot(0),
      {
        observedRules: [
          {
            name: 'Ip',
            limits: [
              { hits: 30, periodSec: 60, penaltySec: 120 },
              { hits: 90, periodSec: 1800, penaltySec: 600 },
            ],
            state: [],
          },
        ],
        penaltyUntil: 0,
        consecutiveThrottles: 0,
        consecutiveErrors: 0,
        recentAcquires: Array.from({ length: 81 }, () => saturatedAt),
        originIp: '203.0.113.7',
      },
      new Date(h.clock.now()).toISOString(),
    );

    const summary = await h.newWorker(0, h.api.client, 'run-0', '198.51.100.9').runOnce();
    expect(summary.stopReason).toBe('assigned_drained');
    expect(summary.requests).toBeGreaterThan(0);
    expect(h.logs.some((l) => l.includes('pace windows start fresh'))).toBe(true);

    // The saved state now belongs to the new IP.
    const saved = await new LimiterStateStore(h.objectStore).load(LEAGUE, workerSlot(0));
    expect(saved?.originIp).toBe('198.51.100.9');
  });

  it('keeps serving a client penalty across an IP change (no Retry-After evasion)', async () => {
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({ entries, config: { chunkSize: 10, workerCount: 1 } });
    await h.createFire();

    // A 429's Retry-After parked this slot far past maxWaitMillis (300s).
    await new LimiterStateStore(h.objectStore).save(
      LEAGUE,
      workerSlot(0),
      {
        observedRules: [],
        penaltyUntil: h.clock.now() + 3_600_000,
        consecutiveThrottles: 1,
        consecutiveErrors: 0,
        recentAcquires: [],
        originIp: '203.0.113.7',
      },
      new Date(h.clock.now()).toISOString(),
    );

    const start = h.clock.now();
    const summary = await h.newWorker(0, h.api.client, 'run-0', '198.51.100.9').runOnce();
    expect(summary.stopReason).toBe('rate_limit_stall'); // new IP did NOT clear the penalty
    expect(summary.requests).toBe(0);
    expect(h.clock.now()).toBe(start);
  });

  it('defers instead of sleeping past the run budget deadline', async () => {
    // When the next slot fits under maxWaitMillis but falls beyond the run
    // budget, sleeping can produce no more work — stop without the sleep.
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({
      entries,
      config: { chunkSize: 10, workerCount: 1, maxRunMillis: 15_000, maxWaitMillis: 600_000 },
    });
    await h.createFire();

    const start = h.clock.now();
    const summary = await h.newWorker(0).runOnce();

    expect(summary.stopReason).toBe('budget_exhausted');
    // The observed 15:60:120 window fills after 13 paced requests; the next
    // slot (start+60s) is past the 15s budget — the worker stopped right there
    // instead of sleeping to it.
    expect(h.clock.now() - start).toBeLessThan(15_000);
    // The mid-character stall left the half-fetched character workable: items
    // were fetched once, no outcome was recorded.
    const chunk = await new ChunkStore(h.objectStore).load(LEAGUE, 'snap-fixed', 0);
    const tally = tallyOutcomes(chunk.characters);
    expect(tally.ok).toBeGreaterThan(0);
    expect(tally.pending).toBeGreaterThan(0);
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

  it('stays inert with the quorum disabled or without a fire runId', async () => {
    const h = makeRunHarness({
      entries: buildLadder(10),
      config: { chunkSize: 5, workerCount: 2 },
    });
    await h.createFire();

    // earlyStopQuorum defaults to 0: a drained worker writes no done marker.
    const disabled = await h.newWorker(0).runOnce();
    expect(disabled.stopReason).toBe('assigned_drained');
    expect(await getJson(h.objectStore, workerDonePath(LEAGUE, 0))).toBeUndefined();

    // Quorum set but no runId (local run outside Actions): equally inert.
    const h2 = makeRunHarness({
      entries: buildLadder(10),
      config: { chunkSize: 5, workerCount: 2, earlyStopQuorum: 1 },
    });
    await h2.createFire();
    const noRunId = await h2.newWorker(1, h2.api.client, '').runOnce();
    expect(noRunId.stopReason).toBe('assigned_drained');
    expect(await getJson(h2.objectStore, workerDonePath(LEAGUE, 1))).toBeUndefined();
  });

  it('stops a straggler once the quorum of siblings drained, resuming its chunks next fire', async () => {
    const h = makeRunHarness({
      entries: buildLadder(20),
      config: { chunkSize: 5, workerCount: 2, earlyStopQuorum: 1 },
    });
    await h.createFire();

    // w0 drains its whole assignment and publishes a done marker for the fire.
    const w0 = await h.newWorker(0, h.api.client, 'run-A').runOnce();
    expect(w0.stopReason).toBe('assigned_drained');
    const marker = await getJson<WorkerDoneMarker>(h.objectStore, workerDonePath(LEAGUE, 0));
    expect(marker).toMatchObject({ slot: 'w0', runId: 'run-A' });

    // w1 (same fire) sees the quorum met before its first fetch and stops —
    // without marking ITSELF done and without touching its chunks.
    const w1 = await h.newWorker(1, h.api.client, 'run-A').runOnce();
    expect(w1.stopReason).toBe('quorum_stopped');
    expect(w1.requests).toBe(0);
    expect(await getJson(h.objectStore, workerDonePath(LEAGUE, 1))).toBeUndefined();
    const store = new ChunkStore(h.objectStore);
    expect(pendingChunkIndices(await store.loadAll(LEAGUE, 'snap-fixed', 4))).toEqual([1, 3]);

    // Next fire (new runId): w0's stale run-A marker no longer counts, so w1
    // drains its leftover chunks normally — early stop lost no work.
    h.clock.advance(COLLECT_CRON_GAP_MS);
    const w1Next = await h.newWorker(1, h.api.client, 'run-B').runOnce();
    expect(w1Next.stopReason).toBe('assigned_drained');
    expect(pendingChunkIndices(await store.loadAll(LEAGUE, 'snap-fixed', 4))).toEqual([]);
    expect(h.api.itemCalls.get('acct-6/char-6')).toBe(1); // fetched exactly once
  });

  it('checkpoints partial progress durably when the quorum lands mid-chunk', async () => {
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({
      entries,
      config: { chunkSize: 10, workerCount: 2, earlyStopQuorum: 1 },
    });
    await h.createFire();

    // w1's done marker appears while w0 is mid-chunk (as it would when a
    // sibling matrix job finishes first): after the 5th API call, w1 is done.
    let calls = 0;
    const client: HttpClient = async (req) => {
      calls += 1;
      if (calls === 5) {
        const marker: WorkerDoneMarker = {
          slot: workerSlot(1),
          runId: 'run-A',
          finishedAt: new Date(h.clock.now()).toISOString(),
        };
        await putJson(h.objectStore, workerDonePath(LEAGUE, 1), marker);
      }
      return h.api.client(req);
    };

    const w0 = await h.newWorker(0, client, 'run-A').runOnce();
    expect(w0.stopReason).toBe('quorum_stopped');
    expect(w0.requests).toBeGreaterThan(0);

    // The partial visit checkpointed: resolved outcomes durable in the chunk,
    // their records in exactly one shard, the rest still workable.
    const chunk = await new ChunkStore(h.objectStore).load(LEAGUE, 'snap-fixed', 0);
    const resolved = chunk.characters.filter((c) => c.outcome === 'ok');
    expect(resolved.length).toBeGreaterThan(0);
    expect(resolved.length).toBeLessThan(10);
    expect((await readAllShards(h.objectStore)).length).toBe(resolved.length);

    // The next fire finishes the chunk without re-fetching what was resolved.
    h.clock.advance(COLLECT_CRON_GAP_MS);
    const next = await h.newWorker(0, h.api.client, 'run-B').runOnce();
    expect(next.stopReason).toBe('assigned_drained');
    for (const c of resolved) {
      expect(h.api.itemCalls.get(`${c.account}/${c.character}`)).toBe(1);
    }
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
