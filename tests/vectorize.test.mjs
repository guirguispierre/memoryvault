import test from 'node:test';
import assert from 'node:assert/strict';

let vectorize;
let constants;
try {
  vectorize = await import('../src/vectorize.ts');
  constants = await import('../src/constants.ts');
} catch (err) {
  if (err?.code === 'ERR_MODULE_NOT_FOUND' || err?.code === 'ERR_UNKNOWN_FILE_EXTENSION') {
    console.error(
      'This suite imports TypeScript sources directly. Run it via `npm run test:vectorize` '
      + '(which registers tests/ts-loader.mjs; requires Node 22.18+), not bare `node --test`.'
    );
  }
  throw err;
}

const {
  makeVectorId,
  makeLegacyVectorId,
  parseMemoryIdFromVectorId,
  looksLikeMemoryId,
  buildSemanticQueryOptions,
  filterSemanticMatches,
  buildMemoryVectorMetadata,
} = vectorize;
const { VECTOR_ID_PREFIX, VECTOR_ID_MAX_MEMORY_ID_LENGTH, VECTORIZE_QUERY_TOP_K_MAX } = constants;

const BRAIN_A = 'brain-aaaaaaaa-1111-2222-3333-444444444444';
const BRAIN_B = 'brain-bbbbbbbb-5555-6666-7777-888888888888';

function match(overrides = {}) {
  return {
    id: `${VECTOR_ID_PREFIX}11111111-2222-3333-4444-555555555555`,
    score: 0.9,
    metadata: { brain_id: BRAIN_A, memory_id: '11111111-2222-3333-4444-555555555555' },
    ...overrides,
  };
}

test('vector ids are prefixed and round-trip through the parser', async () => {
  const memoryId = 'c0ffee00-1111-2222-3333-444444444444';
  const vectorId = await makeVectorId(BRAIN_A, memoryId);
  assert.equal(vectorId, `${VECTOR_ID_PREFIX}${memoryId}`);
  assert.ok(vectorId.length <= 64, 'vector id must respect the 64-byte Vectorize limit');
  assert.equal(parseMemoryIdFromVectorId(vectorId), memoryId);
});

test('over-length memory ids fall back to the digest form, still length-bounded', async () => {
  const longId = 'x'.repeat(VECTOR_ID_MAX_MEMORY_ID_LENGTH + 1);
  const vectorId = await makeVectorId(BRAIN_A, longId);
  assert.ok(vectorId.startsWith('m_'), 'legacy/digest ids use the m_ prefix');
  assert.ok(vectorId.length <= 64, 'digest vector id must respect the 64-byte limit');
  assert.equal(parseMemoryIdFromVectorId(vectorId), '', 'digest ids are not parseable back to a memory id');
});

test('legacy vector ids are brain-salted: same memory id in two brains never collides', async () => {
  const memoryId = 'shared-memory-id-with-enough-length-to-overflow-the-plain-form-x';
  const a = await makeLegacyVectorId(BRAIN_A, memoryId);
  const b = await makeLegacyVectorId(BRAIN_B, memoryId);
  assert.notEqual(a, b);
});

test('looksLikeMemoryId rejects digest ids and short/odd strings', () => {
  assert.equal(looksLikeMemoryId('11111111-2222-3333-4444-555555555555'), true);
  assert.equal(looksLikeMemoryId('m_abc'), false);
  assert.equal(looksLikeMemoryId('short'), false);
  assert.equal(looksLikeMemoryId('has spaces in it which are not allowed'), false);
});

test('query options always carry the caller brain as the namespace', () => {
  for (const topK of [-5, 0, 1, 7, 10_000]) {
    const options = buildSemanticQueryOptions(BRAIN_A, topK);
    assert.equal(options.namespace, BRAIN_A, 'namespace must be exactly the caller brainId');
    assert.ok(options.topK >= 1 && options.topK <= VECTORIZE_QUERY_TOP_K_MAX, `topK ${options.topK} out of bounds`);
    assert.equal(options.returnValues, false);
  }
  assert.equal(buildSemanticQueryOptions(BRAIN_B, 5).namespace, BRAIN_B);
});

test('filterSemanticMatches drops matches whose metadata names a different brain', () => {
  const own = match();
  const foreign = match({
    id: `${VECTOR_ID_PREFIX}99999999-8888-7777-6666-555555555555`,
    metadata: { brain_id: BRAIN_B, memory_id: '99999999-8888-7777-6666-555555555555' },
  });
  const candidates = filterSemanticMatches([own, foreign], BRAIN_A, 0);
  assert.equal(candidates.length, 1);
  assert.equal(candidates[0].memory_id, '11111111-2222-3333-4444-555555555555');
});

test('filterSemanticMatches respects minScore, dedupes, and sorts by score', () => {
  const low = match({ score: 0.1 });
  const dupHigh = match({ score: 0.95 });
  const other = match({
    id: `${VECTOR_ID_PREFIX}22222222-3333-4444-5555-666666666666`,
    score: 0.5,
    metadata: { brain_id: BRAIN_A, memory_id: '22222222-3333-4444-5555-666666666666' },
  });
  const candidates = filterSemanticMatches([low, other, dupHigh], BRAIN_A, 0.3);
  assert.deepEqual(candidates.map((c) => c.score), [0.95, 0.5]);
  assert.equal(new Set(candidates.map((c) => c.memory_id)).size, candidates.length);
});

test('filterSemanticMatches tolerates malformed matches without throwing', () => {
  const junk = [null, 42, 'string', [], { score: 'NaN' }, { id: 123 }];
  assert.deepEqual(filterSemanticMatches(junk, BRAIN_A, 0), []);
});

test('matches without any resolvable memory id are dropped', () => {
  const noId = { score: 0.9, metadata: { brain_id: BRAIN_A } };
  const digestIdNoMetadata = { id: 'm_somedigestvalue1234567890', score: 0.9 };
  assert.deepEqual(filterSemanticMatches([noId, digestIdNoMetadata], BRAIN_A, 0), []);
});

test('upsert metadata always stamps the owning brain', () => {
  const metadata = buildMemoryVectorMetadata(BRAIN_A, {
    id: '11111111-2222-3333-4444-555555555555',
    type: 'fact',
    content: 'hello',
  });
  assert.equal(metadata.brain_id, BRAIN_A);
  assert.equal(metadata.memory_id, '11111111-2222-3333-4444-555555555555');
});
