// Live cross-tenant test for the SEMANTIC retrieval path. The local
// isolation suite runs against a worker without AI/Vectorize bindings, so it
// only proves the lexical path; this script proves the vector path
// (per-brain namespaces + brain-scoped re-fetch) against a deployed worker
// that has real bindings.
//
// Requires VECTORIZE_TEST_URL pointing at such a worker. Without it the
// script skips cleanly so credential-less local/CI runs never fail.
//   VECTORIZE_TEST_URL=https://<worker>.workers.dev npm run test:vectorize:integration

const BASE_URL = (process.env.VECTORIZE_TEST_URL ?? '').replace(/\/+$/, '');
if (!BASE_URL) {
  console.log('skipped: no VECTORIZE_TEST_URL — semantic-path isolation needs a deployed worker with AI + Vectorize bindings (see tests/README.md). The local isolation suite covers the lexical path.');
  process.exit(0);
}

const runId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const SETTLE_TIMEOUT_MS = 180_000; // Vectorize mutations are async; allow generous settle time
const POLL_INTERVAL_MS = 5_000;

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exit(1);
}

async function signup(label) {
  const res = await fetch(`${BASE_URL}/auth/signup`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      email: `vec-iso-${label}-${runId}@example.com`,
      password: 'vectorize-suite-password-1',
      brain_name: `Vectorize Isolation ${label} ${runId}`,
    }),
  });
  if (res.status !== 200) fail(`signup ${label} returned ${res.status}: ${await res.text()}`);
  const cookie = res.headers.getSetCookie().find((c) => c.startsWith('auth_token='));
  if (!cookie) fail(`signup ${label} did not set auth_token`);
  return { label, token: decodeURIComponent(cookie.split(';')[0].slice('auth_token='.length)) };
}

async function mcp(user, name, args = {}) {
  const res = await fetch(`${BASE_URL}/mcp`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${user.token}` },
    body: JSON.stringify({ jsonrpc: '2.0', id: name, method: 'tools/call', params: { name, arguments: args } }),
  });
  if (res.status !== 200) fail(`${name} returned HTTP ${res.status}`);
  const body = await res.json();
  if (body.error) fail(`${name} JSON-RPC error: ${JSON.stringify(body.error)}`);
  return body.result?.content?.[0]?.text ?? '';
}

console.log(`semantic-path isolation against ${BASE_URL} (run ${runId})`);

const alice = await signup('alice');
const bob = await signup('bob');

// Distinctive content with no lexical overlap with the probe query, so a hit
// can only come from the vector index.
const secretPhrase = `zanzibar quantum heliotrope ${runId}`;
const saveText = await mcp(bob, 'memory_save', {
  type: 'fact',
  key: `vec-iso-${runId}`,
  content: `The launch codeword is ${secretPhrase}.`,
});
const saved = JSON.parse(saveText);
if (!saved.id) fail(`memory_save did not return an id: ${saveText.slice(0, 200)}`);
const bobMemoryId = saved.id;

const query = `zanzibar heliotrope codeword ${runId}`;

console.log('waiting for the vector index to settle (Bob must see his own memory semantically)...');
const startedAt = Date.now();
let bobSawIt = false;
while (Date.now() - startedAt < SETTLE_TIMEOUT_MS) {
  const text = await mcp(bob, 'memory_search', { query, mode: 'semantic', min_score: 0 });
  if (text.includes(bobMemoryId)) { bobSawIt = true; break; }
  await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
}
if (!bobSawIt) fail('Bob never saw his own memory via semantic search; cannot validate isolation (check AI/Vectorize bindings on the target worker)');
console.log(`Bob sees his memory semantically after ${Math.round((Date.now() - startedAt) / 1000)}s`);

for (const mode of ['semantic', 'hybrid']) {
  const text = await mcp(alice, 'memory_search', { query, mode, min_score: 0 });
  if (text.includes(bobMemoryId)) fail(`Alice's ${mode} search returned Bob's memory id (cross-tenant vector leak)`);
  if (text.includes(secretPhrase)) fail(`Alice's ${mode} search returned Bob's content (cross-tenant vector leak)`);
}
console.log("Alice's semantic and hybrid searches return nothing of Bob's");

console.log('PASS: semantic-path tenant isolation holds');
