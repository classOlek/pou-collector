import { describe, expect, it } from 'vitest';
import type { SnapshotCharacter } from '@classolek/shared';
import { getJson, putJson } from '../checkpoint/object-store.js';
import { pendingIdentities, readState } from '../snapshot-state/state-store.js';
import { LimiterStateStore, workerSlot } from '../rate-limit/limiter-store.js';
import { PaceStateStore } from '../rate-limit/pace-store.js';
import type { HttpClient } from '../sources/types.js';
import { workerDonePath, type WorkerDoneMarker } from './worker-quorum.js';
import { COLLECT_CRON_GAP_MS, LEAGUE, entry, makeRunHarness } from '../../test/run-harness.js';
import { buildLadder } from '../../test/mock-api.js';
import { readWorkerResult, tallyOutcomes } from '../../test/helpers.js';

type Harness = ReturnType<typeof makeRunHarness>;

/** Drain a snapshot's state file into an array (small in these fixtures). */
async function stateOf(h: Harness, snapshotId = 'snap-fixed'): Promise<SnapshotCharacter[]> {
  const out: SnapshotCharacter[] = [];
  for await (const c of readState(h.objectStore, LEAGUE, snapshotId)) out.push(c);
  return out;
}

/** Identity keys (account/character) still pending/retryable in the state file. */
async function pendingKeys(h: Harness, snapshotId = 'snap-fixed'): Promise<string[]> {
  const ids = await pendingIdentities(readState(h.objectStore, LEAGUE, snapshotId));
  return ids.map((p) => `${p.account}/${p.character}`);
}

describe('Worker: state-ordinal split + result files', () => {
  it('two workers resolve disjoint identities; together they cover everything exactly once', async () => {
    const entries = buildLadder(20);
    const h = makeRunHarness({ entries, config: { workerCount: 2 } });
    await h.createFire();

    const w0 = await h.newWorker(0).runOnce();
    const w1 = await h.newWorker(1).runOnce();

    // Ordinal round-robin: w0 owns the 10 even lines, w1 the 10 odd lines.
    expect(w0.assignedCharacters).toBe(10);
    expect(w1.assignedCharacters).toBe(10);
    expect(w0.charactersResolved + w1.charactersResolved).toBe(20);

    // Each slot's result file holds exactly its share; the two are disjoint and
    // together cover all 20 characters once.
    const r0 = (await readWorkerResult(h.objectStore, LEAGUE, 'snap-fixed', 0)) ?? [];
    const r1 = (await readWorkerResult(h.objectStore, LEAGUE, 'snap-fixed', 1)) ?? [];
    const ids0 = r0.map((r) => `${r.account}/${r.character}`);
    const ids1 = r1.map((r) => `${r.account}/${r.character}`);
    expect(ids0.filter((id) => ids1.includes(id))).toEqual([]);
    expect(new Set([...ids0, ...ids1]).size).toBe(20);
    expect([...r0, ...r1].every((r) => r.outcome === 'ok')).toBe(true);

    // finalize merges both result files, drains and publishes immutably (which
    // deletes the state file — the raw). Everything resolved once.
    const fin = await h.newFinalizer().runOnce();
    expect(fin.stopReason).toBe('published_final');
    expect(fin.transform?.coverage.ok).toBe(20);
    expect(h.api.itemCalls.get('acct-1/char-1')).toBe(1);
  });

  it('mirrors each resolution into its result file with raw payloads inline for ok lines', async () => {
    const entries = [
      entry('ok0', { kind: 'ok' }),
      entry('ok1', { kind: 'ok' }),
      entry('priv', { kind: 'private' }),
      entry('dead', { kind: 'dead' }),
    ];
    const h = makeRunHarness({ entries, config: { workerCount: 1 } });
    await h.createFire();

    const summary = await h.newWorker(0).runOnce();
    expect(summary.stopReason).toBe('assigned_drained');

    const results = await readWorkerResult(h.objectStore, LEAGUE, 'snap-fixed', 0);
    const byId = new Map((results ?? []).map((r) => [`${r.account}/${r.character}`, r]));
    expect(byId.size).toBe(4);

    // `ok` lines carry the raw items/passives inline; non-`ok` lines carry none.
    const ok0 = byId.get('acct-ok0/char-ok0');
    expect(ok0?.outcome).toBe('ok');
    expect(ok0?.characterData).toBeDefined();
    expect(ok0?.passiveTree).toBeDefined();
    expect(ok0?.fetchedAt).toBeDefined();

    const priv = byId.get('acct-priv/char-priv');
    expect(priv?.outcome).toBe('private');
    expect(priv?.characterData).toBeUndefined();
    expect(priv?.passiveTree).toBeUndefined();
    expect(byId.get('acct-dead/char-dead')?.outcome).toBe('dead');

    // The worker never writes the state file — that stays all-pending until finalize.
    expect((await stateOf(h)).every((c) => c.outcome === 'pending')).toBe(true);
    expect(h.api.itemCalls.get('acct-ok0/char-ok0')).toBe(1);
  });

  it('checkpoints mid-run on budget exhaustion and resumes without re-fetching', async () => {
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({
      entries,
      config: { workerCount: 1, maxRunMillis: 20_000 },
    });
    await h.createFire();

    const first = await h.newWorker(0).runOnce();
    expect(first.stopReason).toBe('budget_exhausted');

    // The partial run flushed a result file with exactly the characters it resolved.
    const partial = (await readWorkerResult(h.objectStore, LEAGUE, 'snap-fixed', 0)) ?? [];
    expect(partial.length).toBeGreaterThan(0);
    expect(partial.length).toBeLessThan(10);
    expect(partial.every((r) => r.outcome === 'ok')).toBe(true);

    // finalize #1 merges those into the state file (incomplete — pending remain,
    // state kept), so they leave the pending set.
    const inc = await h.newFinalizer().runOnce();
    expect(inc.stopReason).toBe('published_partial');
    const resolvedKeys = partial.map((r) => `${r.account}/${r.character}`);
    const stillPending = await pendingKeys(h);
    expect(resolvedKeys.some((k) => stillPending.includes(k))).toBe(false);

    // The next run (a cron gap later, paced windows drained) finishes the rest
    // without re-fetching anything already resolved; finalize then publishes.
    h.clock.advance(COLLECT_CRON_GAP_MS);
    const second = await h.newWorker(0).runOnce();
    expect(second.stopReason).toBe('assigned_drained');
    const fin = await h.newFinalizer().runOnce();
    expect(fin.stopReason).toBe('published_final');
    for (const r of partial) {
      expect(h.api.itemCalls.get(`${r.account}/${r.character}`)).toBe(1);
    }
  });

  it('checkpoints the result file periodically so a mid-run crash forfeits few fetches', async () => {
    const entries = Array.from({ length: 5 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({
      entries,
      config: { workerCount: 1, resultCheckpointEvery: 1 },
    });
    await h.createFire();

    // Crash (the client throws) on the 5th call — after char 0 and char 1 have
    // each resolved (2 calls apiece) and, with cadence 1, been flushed.
    let calls = 0;
    const client: HttpClient = (req) => {
      calls += 1;
      if (calls === 5) throw new Error('runner died mid-fetch');
      return h.api.client(req);
    };

    await expect(h.newWorker(0, client).runOnce()).rejects.toThrow('runner died mid-fetch');

    // The periodic checkpoints made the first two resolutions durable, so the
    // crash forfeits only the in-flight character's fetch — not the whole run.
    const results = (await readWorkerResult(h.objectStore, LEAGUE, 'snap-fixed', 0)) ?? [];
    expect(results).toHaveLength(2);
    expect(results.every((r) => r.outcome === 'ok' && r.characterData !== undefined)).toBe(true);
  });

  it('applies the outcome policy per character (ok / private / dead / retryable→dead)', async () => {
    const entries = [
      entry('ok', { kind: 'ok' }),
      entry('priv', { kind: 'private' }),
      entry('dead', { kind: 'dead' }),
      entry('recovers', { kind: 'flaky', fails: 1 }), // retryable → ok next run
      entry('gone', { kind: 'flaky', fails: 10 }), // retryable → dead (attempts exhausted)
    ];
    const h = makeRunHarness({ entries, config: { workerCount: 1 } });
    await h.createFire();

    // Fires 1 & 2 (worker → finalize): incremental publishes that keep the state
    // file, so each fire's worker re-derives the still-pending/retryable work
    // from the merged attempts. After fire 2, `recovers` has healed to ok and
    // `gone` is on its second retry — the four settled outcomes are readable.
    for (let i = 0; i < 2; i += 1) {
      await h.newWorker(0).runOnce();
      await h.newFinalizer().runOnce();
      h.clock.advance(COLLECT_CRON_GAP_MS);
    }
    const settled = new Map((await stateOf(h)).map((c) => [`${c.account}/${c.character}`, c]));
    expect(settled.get('acct-ok/char-ok')?.outcome).toBe('ok');
    expect(settled.get('acct-priv/char-priv')?.outcome).toBe('private');
    expect(settled.get('acct-dead/char-dead')?.outcome).toBe('dead');
    expect(settled.get('acct-recovers/char-recovers')?.outcome).toBe('ok');

    // Fire 3 resolves the last retryable `gone` — its 3rd attempt exhausts the
    // ceiling and it turns terminal `dead` (read from the run's result file
    // before finalize drains and deletes the state file).
    await h.newWorker(0).runOnce();
    const last = (await readWorkerResult(h.objectStore, LEAGUE, 'snap-fixed', 0)) ?? [];
    const gone = last.find((r) => r.account === 'acct-gone');
    expect(gone?.outcome).toBe('dead');
    expect(gone?.attempts).toBe(3);
  });
});

describe('Worker: rate-limit resumability', () => {
  it('stops resumably on a rate-limit block, leaving its characters pending', async () => {
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'throttle' }));
    const h = makeRunHarness({ entries, config: { workerCount: 1 } });
    await h.createFire();

    const summary = await h.newWorker(0).runOnce();

    expect(summary.stopReason).toBe('rate_limited');
    // Nothing was falsely resolved: no terminal outcome recorded, nothing to merge.
    const results = (await readWorkerResult(h.objectStore, LEAGUE, 'snap-fixed', 0)) ?? [];
    expect(results.every((r) => r.outcome === 'retryable')).toBe(true);
    await h.newFinalizer().runOnce();
    expect(tallyOutcomes(await stateOf(h)).dead).toBe(0);
    expect((await pendingKeys(h)).length).toBeGreaterThan(0);
  });

  it('detects a saturated long window and checkpoints instead of idling (rate_limit_stall)', async () => {
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({ entries, config: { workerCount: 1 } });
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
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({ entries, config: { workerCount: 1 } });
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

    const saved = await new LimiterStateStore(h.objectStore).load(LEAGUE, workerSlot(0));
    expect(saved?.originIp).toBe('198.51.100.9');
  });

  it('paces against the shared per-IP spend, whichever slot lands on the IP', async () => {
    // IP 203.0.113.7's 30-min window is already saturated — recorded by SOME
    // worker in a prior fire and stored in the shared per-IP file. A different
    // slot landing on that IP must see the spend and stall; a sibling on a fresh
    // IP does not inherit it. With 15 workers over 10 lines, slot 7 owns line 7
    // and slot 8 owns line 8 (ordinal round-robin).
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({ entries, config: { workerCount: 15 } });
    await h.createFire();

    const saturatedAt = h.clock.now() - 1_000;
    await new PaceStateStore(h.objectStore).save(
      LEAGUE,
      '203.0.113.7',
      Array.from({ length: 81 }, () => saturatedAt),
      new Date(h.clock.now()).toISOString(),
    );
    const rules = {
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
      recentAcquires: [],
    };
    await new LimiterStateStore(h.objectStore).save(
      LEAGUE,
      workerSlot(7),
      rules,
      new Date(h.clock.now()).toISOString(),
    );
    await new LimiterStateStore(h.objectStore).save(
      LEAGUE,
      workerSlot(8),
      rules,
      new Date(h.clock.now()).toISOString(),
    );

    const stalled = await h.newWorker(7, h.api.client, 'run-0', '203.0.113.7').runOnce();
    expect(stalled.stopReason).toBe('rate_limit_stall');
    expect(stalled.requests).toBe(0);

    const fresh = await h.newWorker(8, h.api.client, 'run-0', '198.51.100.9').runOnce();
    expect(fresh.stopReason).toBe('assigned_drained');
    expect(fresh.requests).toBeGreaterThan(0);
  });

  it('keeps serving a client penalty across an IP change (no Retry-After evasion)', async () => {
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({ entries, config: { workerCount: 1 } });
    await h.createFire();

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
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({
      entries,
      config: { workerCount: 1, maxRunMillis: 15_000, maxWaitMillis: 600_000 },
    });
    await h.createFire();

    const start = h.clock.now();
    const summary = await h.newWorker(0).runOnce();

    expect(summary.stopReason).toBe('budget_exhausted');
    // The observed 15:60:120 window fills after 13 paced requests; the next slot
    // (start+60s) is past the 15s budget — the worker stopped instead of sleeping.
    expect(h.clock.now() - start).toBeLessThan(15_000);
    // A partial run resolved some but not all; the rest stay pending after merge.
    await h.newFinalizer().runOnce();
    const tally = tallyOutcomes(await stateOf(h));
    expect(tally.ok).toBeGreaterThan(0);
    expect(tally.pending).toBeGreaterThan(0);
  });
});

describe('Worker: early-stop quorum', () => {
  it('stays inert with the quorum disabled or without a fire runId', async () => {
    const h = makeRunHarness({
      entries: buildLadder(10),
      config: { workerCount: 2 },
    });
    await h.createFire();

    const disabled = await h.newWorker(0).runOnce();
    expect(disabled.stopReason).toBe('assigned_drained');
    expect(await getJson(h.objectStore, workerDonePath(LEAGUE, 0))).toBeUndefined();

    const h2 = makeRunHarness({
      entries: buildLadder(10),
      config: { workerCount: 2, earlyStopQuorum: 1 },
    });
    await h2.createFire();
    const noRunId = await h2.newWorker(1, h2.api.client, '').runOnce();
    expect(noRunId.stopReason).toBe('assigned_drained');
    expect(await getJson(h2.objectStore, workerDonePath(LEAGUE, 1))).toBeUndefined();
  });

  it('stops a straggler once the quorum of siblings drained, resuming its work next fire', async () => {
    const h = makeRunHarness({
      entries: buildLadder(20),
      config: { workerCount: 2, earlyStopQuorum: 1 },
    });
    await h.createFire();

    // w0 drains its whole assignment and publishes a done marker for the fire.
    const w0 = await h.newWorker(0, h.api.client, 'run-A').runOnce();
    expect(w0.stopReason).toBe('assigned_drained');
    const marker = await getJson<WorkerDoneMarker>(h.objectStore, workerDonePath(LEAGUE, 0));
    expect(marker).toMatchObject({ slot: 'w0', runId: 'run-A', stopReason: 'assigned_drained' });

    // w1 (same fire) sees the quorum met before its first fetch and stops without
    // touching its owned characters; its exit publishes a marker of its own.
    const w1 = await h.newWorker(1, h.api.client, 'run-A').runOnce();
    expect(w1.stopReason).toBe('quorum_stopped');
    expect(w1.requests).toBe(0);
    expect(await getJson(h.objectStore, workerDonePath(LEAGUE, 1))).toMatchObject({
      slot: 'w1',
      runId: 'run-A',
      stopReason: 'quorum_stopped',
    });

    // finalize merges w0's results; w1's odd-ordinal share is still pending.
    await h.newFinalizer().runOnce();
    const w1Share = (await readWorkerResult(h.objectStore, LEAGUE, 'snap-fixed', 0)) ?? [];
    const stillPending = await pendingKeys(h);
    expect(stillPending.length).toBe(10); // the 10 odd lines w1 skipped
    expect(
      w1Share.map((r) => `${r.account}/${r.character}`).some((k) => stillPending.includes(k)),
    ).toBe(false);

    // Next fire (new runId): w1 drains its leftover work — early stop lost
    // nothing. finalize then drains and publishes (deleting the state file).
    h.clock.advance(COLLECT_CRON_GAP_MS);
    const w1Next = await h.newWorker(1, h.api.client, 'run-B').runOnce();
    expect(w1Next.stopReason).toBe('assigned_drained');
    const fin = await h.newFinalizer().runOnce();
    expect(fin.stopReason).toBe('published_final');
    expect(fin.transform?.coverage.ok).toBe(20);
    expect(h.api.itemCalls.get('acct-7/char-7')).toBe(1); // fetched exactly once
  });

  it('counts rate-limit-stalled siblings toward the quorum (any clean stop ends the job)', async () => {
    const h = makeRunHarness({
      entries: buildLadder(20),
      config: { workerCount: 2, earlyStopQuorum: 1 },
    });
    await h.createFire();

    // A 429's Retry-After parked w0's slot far past maxWaitMillis: it stalls out
    // before its first fetch — and that exit still publishes its marker.
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
    const w0 = await h.newWorker(0, h.api.client, 'run-A').runOnce();
    expect(w0.stopReason).toBe('rate_limit_stall');
    expect(await getJson(h.objectStore, workerDonePath(LEAGUE, 0))).toMatchObject({
      slot: 'w0',
      runId: 'run-A',
      stopReason: 'rate_limit_stall',
    });

    const w1 = await h.newWorker(1, h.api.client, 'run-A').runOnce();
    expect(w1.stopReason).toBe('quorum_stopped');
    expect(w1.requests).toBe(0);
  });

  it('checkpoints partial progress durably when the quorum lands mid-run', async () => {
    const entries = Array.from({ length: 10 }, (_, i) => entry(`${i}`, { kind: 'ok' }));
    const h = makeRunHarness({
      entries,
      config: { workerCount: 2, earlyStopQuorum: 1 },
    });
    await h.createFire();

    // w1's done marker appears while w0 is mid-run (as when a sibling matrix job
    // finishes first): after the 5th API call, w1 is done.
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

    // The partial run checkpointed a durable result file with the resolved chars.
    const partial = (await readWorkerResult(h.objectStore, LEAGUE, 'snap-fixed', 0)) ?? [];
    expect(partial.length).toBeGreaterThan(0);
    expect(partial.length).toBeLessThan(5); // w0 owns 5 even lines, stopped mid-way
    expect(partial.every((r) => r.outcome === 'ok')).toBe(true);

    // The next fire finishes without re-fetching what was resolved.
    await h.newFinalizer().runOnce();
    h.clock.advance(COLLECT_CRON_GAP_MS);
    const next = await h.newWorker(0, h.api.client, 'run-B').runOnce();
    expect(next.stopReason).toBe('assigned_drained');
    for (const r of partial) {
      expect(h.api.itemCalls.get(`${r.account}/${r.character}`)).toBe(1);
    }
  });
});

describe('Worker: nothing to do', () => {
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
