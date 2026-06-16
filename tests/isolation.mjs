// Black-box tenant-isolation suite. Runs against a real worker (auth + D1
// path, no mocks): boot one with `npm run dev:isolation`, then run
// `BASE_URL=http://127.0.0.1:8787 npm run test:isolation`.
//
// Pattern for every case: Alice (attacker) aims an operation at Bob's
// (victim's) data. The operation must neither reveal Bob's data to Alice nor
// change anything on Bob's side. Any new tool that touches memories must add
// a case here in the same PR.
import test from 'node:test';
import assert from 'node:assert/strict';

const BASE_URL = (process.env.BASE_URL ?? 'http://127.0.0.1:8787').replace(/\/+$/, '');

const runId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const PASSWORD = 'isolation-suite-password-1';

async function signup(label) {
  const email = `iso-${label}-${runId}@example.com`;
  const res = await fetch(`${BASE_URL}/auth/signup`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      // Unique per-run synthetic IP so repeated local runs do not trip the
      // signup rate limiter. Production Cloudflare overwrites this header,
      // so it cannot be spoofed against a deployed worker.
      'CF-Connecting-IP': `198.51.100.${(Date.now() % 200) + 1}`,
    },
    body: JSON.stringify({ email, password: PASSWORD, brain_name: `Isolation ${label} ${runId}` }),
  });
  assert.equal(res.status, 200, `signup for ${label} failed: ${await res.text()}`);
  const cookies = res.headers.getSetCookie();
  const authCookie = cookies.find((c) => c.startsWith('auth_token='));
  assert.ok(authCookie, `signup for ${label} did not set auth_token cookie`);
  const token = decodeURIComponent(authCookie.split(';')[0].slice('auth_token='.length));
  assert.ok(token.length > 0);
  return { label, email, token };
}

async function mcp(user, name, args = {}) {
  const res = await fetch(`${BASE_URL}/mcp`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${user.token}`,
    },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: `${name}-${Math.random().toString(36).slice(2, 8)}`,
      method: 'tools/call',
      params: { name, arguments: args },
    }),
  });
  assert.equal(res.status, 200, `tools/call ${name} returned HTTP ${res.status}`);
  const body = await res.json();
  assert.ok(!body.error, `tools/call ${name} returned JSON-RPC error: ${JSON.stringify(body.error)}`);
  const text = body.result?.content?.[0]?.text ?? '';
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }
  return { text, json };
}

async function api(user, path, init = {}) {
  const headers = { ...(init.headers ?? {}) };
  if (user) headers.Authorization = `Bearer ${user.token}`;
  if (init.body) headers['Content-Type'] = 'application/json';
  const res = await fetch(`${BASE_URL}${path}`, { ...init, headers });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }
  return { status: res.status, text, json };
}

async function saveMemory(user, fields = {}) {
  const { json } = await mcp(user, 'memory_save', {
    type: 'fact',
    content: `content-${user.label}-${Math.random().toString(36).slice(2, 10)}-${runId}`,
    ...fields,
  });
  assert.ok(json?.id, `memory_save did not return an id: ${JSON.stringify(json)}`);
  return json.id;
}

async function getMemory(user, id) {
  const { text, json } = await mcp(user, 'memory_get', { id });
  if (text === 'Memory not found.') return null;
  assert.ok(json?.id === id, `unexpected memory_get response: ${text.slice(0, 200)}`);
  return json;
}

const alice = await signup('alice');
const bob = await signup('bob');

// Bob's standing fixtures: two linked memories that most cases attack.
const bobSecret = `bob-secret-${runId}`;
const bobMemA = await saveMemory(bob, { key: `bob-a-${runId}`, content: `${bobSecret} alpha`, tags: `iso-bob-${runId}` });
const bobMemB = await saveMemory(bob, { key: `bob-b-${runId}`, content: `${bobSecret} beta`, tags: `iso-bob-${runId}` });
await mcp(bob, 'memory_link', { from_id: bobMemA, to_id: bobMemB, relation_type: 'supports' });
const aliceMem = await saveMemory(alice, { key: `alice-a-${runId}` });

async function assertBobIntact() {
  const a = await getMemory(bob, bobMemA);
  const b = await getMemory(bob, bobMemB);
  assert.ok(a && b, "Bob's fixture memories must still exist");
  assert.equal(a.archived_at ?? null, null, "Bob's memory A must not be archived");
  assert.equal(b.archived_at ?? null, null, "Bob's memory B must not be archived");
  assert.ok(String(a.content).includes(bobSecret), "Bob's memory A content must be unchanged");
}

test('unauthenticated requests are rejected', async () => {
  for (const path of ['/mcp', '/api/memories', '/api/memories/signature', '/api/export', '/api/graph', '/api/viewer-settings', `/api/links/${bobMemA}`]) {
    const { status } = await api(null, path);
    assert.equal(status, 401, `${path} must require auth, got ${status}`);
  }
  for (const path of ['/api/import', '/api/purge']) {
    const { status } = await api(null, path, { method: 'POST', body: '{}' });
    assert.equal(status, 401, `${path} must require auth, got ${status}`);
  }
  const unauthPut = await api(null, '/api/viewer-settings', { method: 'PUT', body: '{}' });
  assert.equal(unauthPut.status, 401, `PUT /api/viewer-settings must require auth, got ${unauthPut.status}`);
});

test('memory_get / memory_get_fact do not cross brains', async () => {
  assert.equal(await getMemory(alice, bobMemA), null);
  const { text } = await mcp(alice, 'memory_get_fact', { key: `bob-a-${runId}` });
  assert.ok(text.startsWith('No fact found'), `Alice must not read Bob's fact by key: ${text.slice(0, 120)}`);
});

test('memory_search and /api/memories do not leak across brains', async () => {
  // Lexical search tokenizes, so Alice can legitimately match her own rows;
  // the requirement is that nothing of Bob's appears in her results.
  const { text } = await mcp(alice, 'memory_search', { query: bobSecret });
  assert.ok(!text.includes(bobMemA) && !text.includes(bobMemB), "Alice's search output must not contain Bob's memory ids");
  assert.ok(!text.includes('bob-secret-'), "Alice's search output must not contain Bob's content");
  const rest = await api(alice, `/api/memories?search=${encodeURIComponent(bobSecret)}&limit=50`);
  assert.equal(rest.status, 200);
  assert.equal(rest.json.memories.length, 0, "Alice's /api/memories must not return Bob's content");
});

test('memory_update / memory_delete cannot touch the other brain', async () => {
  const upd = await mcp(alice, 'memory_update', { id: bobMemA, content: 'hijacked-by-alice' });
  assert.ok(!upd.json?.id || upd.json.id !== bobMemA || upd.text.includes('not found'), `unexpected: ${upd.text.slice(0, 120)}`);
  const del = await mcp(alice, 'memory_delete', { id: bobMemA });
  assert.ok(del.text.includes('not found') || del.text.includes('Not found') || del.json?.deleted === 0, `unexpected: ${del.text.slice(0, 120)}`);
  await assertBobIntact();
});

test('memory_reinforce rejects a foreign memory id', async () => {
  const before = await getMemory(bob, bobMemA);
  const { text } = await mcp(alice, 'memory_reinforce', { id: bobMemA, delta_confidence: 0.5, delta_importance: 0.5 });
  assert.ok(text.includes('Memory not found'), `expected not-found, got: ${text.slice(0, 120)}`);
  const after = await getMemory(bob, bobMemA);
  assert.equal(after.confidence, before.confidence, "Bob's confidence must be unchanged");
  assert.equal(after.importance, before.importance, "Bob's importance must be unchanged");
});

test('memory_forget (soft and hard) rejects a foreign memory id', async () => {
  const soft = await mcp(alice, 'memory_forget', { id: bobMemA, mode: 'soft' });
  assert.ok(soft.text.includes('not found'), `expected not-found, got: ${soft.text.slice(0, 120)}`);
  const hard = await mcp(alice, 'memory_forget', { id: bobMemA, mode: 'hard' });
  assert.ok(hard.text.includes('Memory not found'), `expected not-found, got: ${hard.text.slice(0, 120)}`);
  await assertBobIntact();
});

test("memory_consolidate only consolidates the caller's brain", async () => {
  const dupKey = `bob-dup-${runId}`;
  const dup1 = await saveMemory(bob, { key: dupKey, content: `dup ${bobSecret}`, tags: `iso-dup-${runId}` });
  const dup2 = await saveMemory(bob, { key: dupKey, content: `dup ${bobSecret}`, tags: `iso-dup-${runId}` });
  const { json } = await mcp(alice, 'memory_consolidate', { tag: `iso-dup-${runId}` });
  assert.equal(json?.archived_count ?? 0, 0, "Alice's consolidate must not archive anything from Bob's brain");
  const d1 = await getMemory(bob, dup1);
  const d2 = await getMemory(bob, dup2);
  assert.ok(d1 && d2, "Bob's duplicate memories must survive Alice's consolidate");
  assert.equal(d1.archived_at ?? null, null);
  assert.equal(d2.archived_at ?? null, null);
});

test('memory_merge rejects foreign memory ids', async () => {
  const { text } = await mcp(alice, 'memory_merge', { memory_ids: [bobMemA, bobMemB] });
  assert.ok(text.includes('Found only 0 active memories'), `expected 0 reachable memories, got: ${text.slice(0, 160)}`);
  await assertBobIntact();
});

test('memory_entity_resolve cannot alias foreign memories', async () => {
  const { text } = await mcp(alice, 'memory_entity_resolve', {
    mode: 'resolve',
    canonical_id: bobMemA,
    alias_id: bobMemB,
  });
  assert.ok(text.includes('not found'), `expected rejection, got: ${text.slice(0, 160)}`);
  const bobList = await mcp(bob, 'memory_entity_resolve', { mode: 'list' });
  assert.equal(bobList.json?.count ?? 0, 0, "no alias rows may appear in Bob's brain");
  const aliceList = await mcp(alice, 'memory_entity_resolve', { mode: 'list' });
  assert.equal(aliceList.json?.count ?? 0, 0, "no alias rows may appear in Alice's brain either");
});

test('memory_conflict_resolve rejects foreign memory ids', async () => {
  const { text } = await mcp(alice, 'memory_conflict_resolve', {
    a_id: bobMemA,
    b_id: bobMemB,
    status: 'resolved',
    canonical_id: bobMemA,
  });
  assert.ok(text.includes('must exist in this brain'), `expected rejection, got: ${text.slice(0, 160)}`);
});

test('memory_link / memory_unlink cannot reach foreign links', async () => {
  const link = await mcp(alice, 'memory_link', { from_id: aliceMem, to_id: bobMemA });
  assert.ok(link.text.includes('Memory not found'), `cross-brain link must be rejected: ${link.text.slice(0, 120)}`);
  const unlink = await mcp(alice, 'memory_unlink', { from_id: bobMemA, to_id: bobMemB });
  assert.ok(unlink.text.includes('No link found'), `expected no-link, got: ${unlink.text.slice(0, 120)}`);
  const bobLinks = await mcp(bob, 'memory_links', { id: bobMemA });
  const count = bobLinks.json?.links?.length ?? bobLinks.json?.count ?? (Array.isArray(bobLinks.json) ? bobLinks.json.length : 0);
  assert.ok(count >= 1, `Bob's link must survive Alice's unlink attempt: ${bobLinks.text.slice(0, 200)}`);
});

test('brain_snapshot_restore rejects a foreign snapshot id', async () => {
  const created = await mcp(bob, 'brain_snapshot_create', { label: `iso-${runId}` });
  const snapshotId = created.json?.snapshot_id ?? created.json?.id;
  assert.ok(snapshotId, `snapshot_create did not return an id: ${created.text.slice(0, 200)}`);
  const { text } = await mcp(alice, 'brain_snapshot_restore', { snapshot_id: snapshotId, mode: 'replace' });
  assert.ok(text.includes('Snapshot not found'), `expected not-found, got: ${text.slice(0, 160)}`);
  await assertBobIntact();
  const aliceStillThere = await getMemory(alice, aliceMem);
  assert.ok(aliceStillThere, "Alice's own data must be untouched by the failed restore");
});

test("/api/export only contains the caller's brain", async () => {
  const aliceExport = await api(alice, '/api/export');
  assert.equal(aliceExport.status, 200);
  assert.ok(!aliceExport.text.includes(bobSecret), "Alice's export must not contain Bob's content");
  assert.ok(!aliceExport.text.includes(bobMemA), "Alice's export must not contain Bob's memory ids");
  const bobExport = await api(bob, '/api/export');
  assert.ok(bobExport.text.includes(bobSecret), "Bob's export must contain his own content");
});

test('/api/import cannot overwrite a foreign memory via id collision', async () => {
  const before = await getMemory(bob, bobMemA);
  const res = await api(alice, '/api/import', {
    method: 'POST',
    body: JSON.stringify({
      schema: 'memoryvault_export_v1',
      strategy: 'merge',
      data: {
        memories: [{
          id: bobMemA,
          type: 'fact',
          content: 'hijacked-via-import',
          created_at: 1,
          updated_at: 1,
        }],
      },
    }),
  });
  assert.equal(res.status, 200, `import failed outright: ${res.text.slice(0, 200)}`);
  const after = await getMemory(bob, bobMemA);
  assert.equal(after.content, before.content, "Bob's memory must be unchanged after Alice's id-collision import");
  assert.equal(await getMemory(alice, bobMemA), null, "Alice must not gain a memory under Bob's id");
});

test("/api/purge only purges the caller's brain", async () => {
  const purger = await signup('purger');
  await saveMemory(purger, { key: `purger-${runId}` });
  const res = await api(purger, '/api/purge', {
    method: 'POST',
    body: JSON.stringify({ confirm: 'PURGE ALL DATA' }),
  });
  assert.equal(res.status, 200, `purge failed: ${res.text.slice(0, 200)}`);
  assert.ok(res.json.ok, 'purge should report ok');
  await assertBobIntact();
  const aliceStillThere = await getMemory(alice, aliceMem);
  assert.ok(aliceStillThere, "another tenant's purge must not delete Alice's data");
});

test('/api/links/:id rejects a foreign memory id', async () => {
  const res = await api(alice, `/api/links/${bobMemA}`);
  assert.equal(res.status, 404, `expected 404 for foreign memory id, got ${res.status}: ${res.text.slice(0, 120)}`);
});

test("/api/graph only contains the caller's brain", async () => {
  const res = await api(alice, '/api/graph');
  assert.equal(res.status, 200);
  assert.ok(!res.text.includes(bobMemA) && !res.text.includes(bobSecret), "Alice's graph must not contain Bob's nodes");
  const bobRes = await api(bob, '/api/graph');
  assert.ok(bobRes.text.includes(bobMemA), "Bob's graph must contain his own nodes");
});

test('/api/viewer-settings is brain-scoped: B can neither read nor overwrite A', async () => {
  // Alice saves a distinctive custom palette.
  const aliceAccent = '#abcdef';
  const savePut = await api(alice, '/api/viewer-settings', {
    method: 'PUT',
    body: JSON.stringify({ settings: { theme: 'custom', custom_theme: { butter: aliceAccent } } }),
  });
  assert.equal(savePut.status, 200, `Alice PUT should succeed, got ${savePut.status}`);

  // Bob's GET must return his own row (none yet -> defaults), never Alice's.
  const bobGet = await api(bob, '/api/viewer-settings');
  assert.equal(bobGet.status, 200);
  assert.equal(bobGet.json.settings, null, "Bob must not see Alice's saved settings");
  assert.ok(!bobGet.text.includes(aliceAccent), "Bob's response must not contain Alice's accent");

  // Bob writes his own; it must not touch Alice's row.
  const bobPut = await api(bob, '/api/viewer-settings', {
    method: 'PUT',
    body: JSON.stringify({ settings: { theme: 'ember' } }),
  });
  assert.equal(bobPut.status, 200);

  const aliceGet = await api(alice, '/api/viewer-settings');
  assert.equal(aliceGet.status, 200);
  assert.equal(aliceGet.json.settings.theme, 'custom', "Alice's theme must be unchanged by Bob's write");
  assert.equal(aliceGet.json.settings.custom_theme.butter, aliceAccent, "Alice's accent must survive Bob's write");

  const bobGet2 = await api(bob, '/api/viewer-settings');
  assert.equal(bobGet2.json.settings.theme, 'ember', "Bob must read back his own theme");
});

test('auth rate limiter fires after the configured attempt budget', async () => {
  // The worker keys the limiter on this header verbatim, so a per-run value
  // guarantees this case neither inherits stale counters from earlier runs
  // (15-minute TTL) nor pollutes the signup counters used elsewhere.
  const ip = `rate-limit-probe-${runId}`;
  const attempt = () => fetch(`${BASE_URL}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'CF-Connecting-IP': ip },
    body: JSON.stringify({ email: alice.email, password: 'definitely-wrong-password' }),
  });
  for (let i = 1; i <= 10; i++) {
    const res = await attempt();
    assert.equal(res.status, 401, `attempt ${i} should be rejected as bad credentials, got ${res.status}`);
  }
  const throttled = await attempt();
  assert.equal(throttled.status, 429, `attempt 11 should be rate limited, got ${throttled.status}`);
});

test('final cross-check: nothing leaked into either brain', async () => {
  await assertBobIntact();
  const aliceSearch = await api(alice, `/api/memories?search=${encodeURIComponent(bobSecret)}&limit=50`);
  assert.equal(aliceSearch.json.memories.length, 0);
  const bobSearch = await api(bob, `/api/memories?search=hijacked&limit=50`);
  assert.equal(bobSearch.json.memories.length, 0, 'no hijack artifacts may exist in Bob brain');
});

test('hosted waitlist: public write works, but exposes no read or cross-tenant data', async () => {
  const email = `wl-${runId}@example.com`;
  // Unauthenticated POST captures the email and reports an honest success.
  const first = await api(null, '/api/waitlist', { method: 'POST', body: JSON.stringify({ email }) });
  assert.equal(first.status, 200, `waitlist POST should succeed, got ${first.status}: ${first.text.slice(0, 120)}`);
  assert.ok(first.json && first.json.ok, 'waitlist POST should report ok');
  assert.equal(first.json.status, 'added', 'first submission should be recorded as added');

  // Re-submitting the same address is an idempotent no-op, not a fake success.
  const dupe = await api(null, '/api/waitlist', { method: 'POST', body: JSON.stringify({ email }) });
  assert.equal(dupe.json.status, 'exists', 'duplicate submission should dedupe to exists');

  // A malformed email is rejected.
  const bad = await api(null, '/api/waitlist', { method: 'POST', body: JSON.stringify({ email: 'not-an-email' }) });
  assert.equal(bad.status, 400, `invalid email must be rejected, got ${bad.status}`);

  // There is no public read of the waitlist, and GET is not allowed.
  const get = await api(null, '/api/waitlist');
  assert.ok(get.status === 405 || get.status === 404, `GET /api/waitlist must not read the list, got ${get.status}`);
  assert.ok(!get.text.includes(email), 'the waitlist must never be readable from the public endpoint');
});
