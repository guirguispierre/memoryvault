import test from 'node:test';
import assert from 'node:assert/strict';

const {
  hashPassword,
  verifyPassword,
  signAccessToken,
  verifyAccessToken,
  bytesToBase64Url,
  base64UrlToBytes,
} = await import('../src/crypto.ts');

const SECRET = 'test-secret-for-crypto-suite';
const OTHER_SECRET = 'a-completely-different-secret';

function validPayload(overrides = {}) {
  const nowSec = Math.floor(Date.now() / 1000);
  return {
    typ: 'access',
    sub: 'user-1',
    bid: 'brain-1',
    sid: 'session-1',
    iat: nowSec,
    exp: nowSec + 3600,
    ...overrides,
  };
}

function encodeJson(value) {
  return bytesToBase64Url(new TextEncoder().encode(JSON.stringify(value)));
}

test('hashPassword/verifyPassword round-trips a correct password', async () => {
  const stored = await hashPassword('correct horse battery staple');
  assert.match(stored, /^pbkdf2_sha256\$600000\$/);
  assert.equal(await verifyPassword('correct horse battery staple', stored), true);
});

test('verifyPassword rejects a wrong password', async () => {
  const stored = await hashPassword('right-password-123');
  assert.equal(await verifyPassword('wrong-password-123', stored), false);
});

test('verifyPassword rejects malformed stored strings', async () => {
  assert.equal(await verifyPassword('pw', ''), false);
  assert.equal(await verifyPassword('pw', 'not-a-hash'), false);
  assert.equal(await verifyPassword('pw', 'md5$1000$salt$hash'), false);
  assert.equal(await verifyPassword('pw', 'pbkdf2_sha256$$salt$hash'), false);
  assert.equal(await verifyPassword('pw', 'pbkdf2_sha256$abc$salt$hash'), false);
});

test('verifyPassword rejects iteration counts below the sanity floor', async () => {
  assert.equal(await verifyPassword('pw', 'pbkdf2_sha256$9999$c2FsdA$aGFzaA'), false);
});

test('verifyPassword accepts legacy hashes with lower (but sane) iteration counts', async () => {
  const stored = await hashPassword('legacy-password-1');
  const [algo, , salt, hash] = stored.split('$');
  assert.equal(algo, 'pbkdf2_sha256');
  // Recompute at 100k to emulate a hash written before the iteration bump.
  const { derivePasswordHash } = await import('../src/crypto.ts');
  const legacyHash = await derivePasswordHash('legacy-password-1', base64UrlToBytes(salt), 100_000);
  const legacyStored = `pbkdf2_sha256$100000$${salt}$${legacyHash}`;
  assert.equal(await verifyPassword('legacy-password-1', legacyStored), true);
  assert.equal(await verifyPassword('wrong', legacyStored), false);
  assert.notEqual(legacyHash, hash);
});

test('signAccessToken/verifyAccessToken round-trips a valid payload', async () => {
  const payload = validPayload();
  const token = await signAccessToken(payload, SECRET);
  const verified = await verifyAccessToken(token, SECRET);
  assert.deepEqual(verified, payload);
});

test('token signed with a different secret is rejected', async () => {
  const token = await signAccessToken(validPayload(), OTHER_SECRET);
  assert.equal(await verifyAccessToken(token, SECRET), null);
});

test('token with a flipped signature bit is rejected', async () => {
  const token = await signAccessToken(validPayload(), SECRET);
  const [header, payload, sig] = token.split('.');
  const sigBytes = base64UrlToBytes(sig);
  sigBytes[0] ^= 0x01;
  const tampered = `${header}.${payload}.${bytesToBase64Url(sigBytes)}`;
  assert.equal(await verifyAccessToken(tampered, SECRET), null);
});

test('token re-encoded with alg "none" header is rejected', async () => {
  const token = await signAccessToken(validPayload(), SECRET);
  const [, payload, sig] = token.split('.');
  const noneHeader = encodeJson({ alg: 'none', typ: 'JWT' });
  assert.equal(await verifyAccessToken(`${noneHeader}.${payload}.${sig}`, SECRET), null);
  assert.equal(await verifyAccessToken(`${noneHeader}.${payload}.`, SECRET), null);
  const hs512Header = encodeJson({ alg: 'HS512', typ: 'JWT' });
  assert.equal(await verifyAccessToken(`${hs512Header}.${payload}.${sig}`, SECRET), null);
});

test('expired token is rejected', async () => {
  const nowSec = Math.floor(Date.now() / 1000);
  const token = await signAccessToken(validPayload({ iat: nowSec - 7200, exp: nowSec - 3600 }), SECRET);
  assert.equal(await verifyAccessToken(token, SECRET), null);
});

test('token missing required claims is rejected', async () => {
  for (const claim of ['sub', 'bid', 'sid']) {
    const payload = validPayload();
    delete payload[claim];
    const token = await signAccessToken(payload, SECRET);
    assert.equal(await verifyAccessToken(token, SECRET), null, `missing ${claim} should be rejected`);
  }
  const wrongTyp = await signAccessToken(validPayload({ typ: 'refresh' }), SECRET);
  assert.equal(await verifyAccessToken(wrongTyp, SECRET), null);
});

test('garbage tokens are rejected without throwing', async () => {
  assert.equal(await verifyAccessToken('', SECRET), null);
  assert.equal(await verifyAccessToken('a.b', SECRET), null);
  assert.equal(await verifyAccessToken('!!!.???.###', SECRET), null);
});
