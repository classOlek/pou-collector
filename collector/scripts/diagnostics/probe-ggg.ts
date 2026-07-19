/**
 * Read-only probe of the live GGG legacy endpoints, to diagnose why collection
 * aborts (the run summary only reports outcome tallies, not the actual HTTP
 * responses). Makes a SMALL, identifiable, respectful number of requests (one
 * ladder page + one character), and prints each response's status, the headers
 * that reveal a Cloudflare bot-challenge (server, cf-ray, cf-mitigated,
 * content-type, retry-after, x-rate-limit-*), and a short body snippet.
 *
 * It changes nothing. Run via the ClaudeDiagnostics workflow so the identifiable
 * User-Agent (COLLECTOR_CONTACT_EMAIL) is set from the encrypted secret.
 *
 *   script = scripts/diagnostics/probe-ggg.ts
 */
import { buildUserAgent } from '../../src/config.js';
import { createFetchHttpClient } from '../../src/http/fetch-client.js';
import { classifyResponse } from '../../src/sources/classify.js';
import type { HttpResponse } from '../../src/sources/types.js';

const LADDER_BASE = 'https://www.pathofexile.com/api/ladders';
const CHARACTER_BASE = 'https://www.pathofexile.com/character-window';
const INTERESTING_HEADERS = [
  'server',
  'cf-ray',
  'cf-mitigated',
  'cf-cache-status',
  'content-type',
  'retry-after',
  'location',
  'x-rate-limit-account',
  'x-rate-limit-account-state',
  'x-rate-limit-ip',
  'x-rate-limit-ip-state',
  'x-rate-limit-rules',
];

function report(label: string, url: string, res: HttpResponse): void {
  const category = classifyResponse(res);
  console.log(`\n=== ${label} ===`);
  console.log(`url:      ${url}`);
  console.log(`status:   ${res.status}`);
  console.log(`category: ${category.tag}`);
  for (const h of INTERESTING_HEADERS) {
    if (res.headers[h] !== undefined) console.log(`  ${h}: ${res.headers[h]}`);
  }
  const snippet = res.body.replace(/\s+/g, ' ').trim().slice(0, 400);
  console.log(`body[0:400]: ${snippet}`);
}

async function main(): Promise<void> {
  const userAgent = buildUserAgent();
  const league = process.env.COLLECTOR_LEAGUE?.trim() || 'Standard';
  const http = createFetchHttpClient();
  console.log(`User-Agent: ${userAgent}`);
  console.log(`League:     ${league}`);

  const ladderUrl = `${LADDER_BASE}?id=${encodeURIComponent(league)}&offset=0&limit=5`;
  const ladder = await http({ url: ladderUrl, headers: { 'user-agent': userAgent } });
  report('LADDER', ladderUrl, ladder);

  // Compare a PUBLIC-flagged profile against a non-public one: the run reports
  // get-items failures, and the hypothesis is that private profiles return a 403
  // *HTML* page (misclassified as a Cloudflare `challenge` instead of `private`).
  type Entry = { public?: boolean; account?: { name?: string }; character?: { name?: string } };
  let entries: Entry[] = [];
  try {
    entries = (JSON.parse(ladder.body) as { entries?: Entry[] }).entries ?? [];
  } catch {
    console.log('\n(could not parse ladder body as JSON — skipping character probes)');
  }

  const usable = (e: Entry): boolean => Boolean(e.account?.name && e.character?.name);
  const probes: { label: string; entry: Entry | undefined }[] = [
    { label: 'PUBLIC profile', entry: entries.find((e) => e.public === true && usable(e)) },
    { label: 'NON-PUBLIC profile', entry: entries.find((e) => e.public !== true && usable(e)) },
  ];

  for (const { label, entry } of probes) {
    if (!entry) {
      console.log(`\n(no ${label} entry found in ladder page — skipped)`);
      continue;
    }
    const account = entry.account!.name!;
    const character = entry.character!.name!;
    const itemsUrl = `${CHARACTER_BASE}/get-items?accountName=${encodeURIComponent(account)}&character=${encodeURIComponent(character)}`;
    const items = await http({ url: itemsUrl, headers: { 'user-agent': userAgent } });
    report(`${label} get-items (${account}/${character})`, itemsUrl, items);
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
