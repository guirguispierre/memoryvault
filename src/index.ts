export interface Env {
  DB: D1Database;
  AUTH_SECRET: string;
}

const SERVER_NAME = 'ai-memory-mcp';
const SERVER_VERSION = '1.5.0';
const LEGACY_BRAIN_ID = 'legacy-default-brain';
const ACCESS_TOKEN_TTL_SECONDS = 60 * 60; // 1 hour
const REFRESH_TOKEN_TTL_SECONDS = 60 * 60 * 24 * 30; // 30 days
const PBKDF2_ITERATIONS = 100_000;

type SessionTokens = {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  refresh_expires_in: number;
  session_id: string;
};

function generateId(): string {
  return crypto.randomUUID();
}

function now(): number {
  return Math.floor(Date.now() / 1000);
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function protectedResourceMetadataUrl(url: URL): string {
  return `${url.origin}/.well-known/oauth-protected-resource`;
}

function oauthChallengeHeader(url: URL): string {
  return `Bearer realm="mcp", resource_metadata="${protectedResourceMetadataUrl(url)}"`;
}

function unauthorized(url?: URL): Response {
  const headers: Record<string, string> = { ...CORS_HEADERS, 'Content-Type': 'application/json' };
  if (url) headers['WWW-Authenticate'] = oauthChallengeHeader(url);
  return new Response(JSON.stringify({ error: 'Unauthorized' }), {
    status: 401,
    headers,
  });
}

type AuthContext = {
  kind: 'legacy' | 'user';
  brainId: string;
  userId: string | null;
  sessionId: string | null;
};

type AccessTokenPayload = {
  typ: 'access';
  sub: string;
  bid: string;
  sid: string;
  iat: number;
  exp: number;
};

function parseBearerToken(request: Request): string | null {
  const auth = request.headers.get('Authorization');
  if (!auth) return null;
  const parts = auth.split(' ');
  if (parts.length !== 2 || parts[0] !== 'Bearer') return null;
  return parts[1] || null;
}

function bytesToBase64Url(bytes: Uint8Array): string {
  let bin = '';
  for (const byte of bytes) bin += String.fromCharCode(byte);
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function base64UrlToBytes(input: string): Uint8Array {
  const base64 = input.replace(/-/g, '+').replace(/_/g, '/').padEnd(Math.ceil(input.length / 4) * 4, '=');
  const bin = atob(base64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

async function hmacSha256(secret: string, data: string): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(data));
  return new Uint8Array(sig);
}

async function sha256DigestBase64Url(value: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return bytesToBase64Url(new Uint8Array(digest));
}

async function derivePasswordHash(password: string, salt: Uint8Array, iterations = PBKDF2_ITERATIONS): Promise<string> {
  const keyMaterial = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(password),
    'PBKDF2',
    false,
    ['deriveBits']
  );
  const bits = await crypto.subtle.deriveBits(
    { name: 'PBKDF2', hash: 'SHA-256', iterations, salt },
    keyMaterial,
    256
  );
  return bytesToBase64Url(new Uint8Array(bits));
}

async function hashPassword(password: string): Promise<string> {
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const hash = await derivePasswordHash(password, salt, PBKDF2_ITERATIONS);
  return `pbkdf2_sha256$${PBKDF2_ITERATIONS}$${bytesToBase64Url(salt)}$${hash}`;
}

async function verifyPassword(password: string, stored: string): Promise<boolean> {
  const [algo, iterRaw, saltRaw, hashRaw] = stored.split('$');
  if (algo !== 'pbkdf2_sha256' || !iterRaw || !saltRaw || !hashRaw) return false;
  const iterations = Number(iterRaw);
  if (!Number.isFinite(iterations) || iterations < 10_000) return false;
  const recomputed = await derivePasswordHash(password, base64UrlToBytes(saltRaw), iterations);
  return recomputed === hashRaw;
}

function randomToken(size = 32): string {
  const bytes = crypto.getRandomValues(new Uint8Array(size));
  return bytesToBase64Url(bytes);
}

function normalizeEmail(email: string): string {
  return email.trim().toLowerCase();
}

function isValidEmail(email: string): boolean {
  const v = normalizeEmail(email);
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v) && v.length <= 254;
}

function isStrongEnoughPassword(password: string): boolean {
  return password.length >= 10;
}

async function signAccessToken(payload: AccessTokenPayload, secret: string): Promise<string> {
  const header = { alg: 'HS256', typ: 'JWT' };
  const headerPart = bytesToBase64Url(new TextEncoder().encode(JSON.stringify(header)));
  const payloadPart = bytesToBase64Url(new TextEncoder().encode(JSON.stringify(payload)));
  const body = `${headerPart}.${payloadPart}`;
  const sig = await hmacSha256(secret, body);
  return `${body}.${bytesToBase64Url(sig)}`;
}

async function verifyAccessToken(token: string, secret: string): Promise<AccessTokenPayload | null> {
  const [headerPart, payloadPart, sigPart] = token.split('.');
  if (!headerPart || !payloadPart || !sigPart) return null;
  try {
    const payloadJson = new TextDecoder().decode(base64UrlToBytes(payloadPart));
    const payload = JSON.parse(payloadJson) as Partial<AccessTokenPayload>;
    if (payload.typ !== 'access' || typeof payload.sub !== 'string' || typeof payload.bid !== 'string' || typeof payload.sid !== 'string') {
      return null;
    }
    if (typeof payload.iat !== 'number' || typeof payload.exp !== 'number') return null;
    if (payload.exp < now()) return null;
    const expectedSig = await hmacSha256(secret, `${headerPart}.${payloadPart}`);
    const givenSig = base64UrlToBytes(sigPart);
    if (expectedSig.length !== givenSig.length) return null;
    for (let i = 0; i < expectedSig.length; i++) {
      if (expectedSig[i] !== givenSig[i]) return null;
    }
    return payload as AccessTokenPayload;
  } catch {
    return null;
  }
}

async function createSessionTokens(userId: string, brainId: string, env: Env): Promise<{
  access_token: string;
  refresh_token: string;
  expires_in: number;
  refresh_expires_in: number;
  session_id: string;
}> {
  const ts = now();
  const sessionId = generateId();
  const refreshToken = randomToken(32);
  const refreshHash = await sha256DigestBase64Url(refreshToken);
  const refreshExpiresAt = ts + REFRESH_TOKEN_TTL_SECONDS;
  await env.DB.prepare(
    `INSERT INTO auth_sessions
      (id, user_id, brain_id, refresh_hash, expires_at, created_at, used_at, revoked_at, replaced_by)
     VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)`
  ).bind(sessionId, userId, brainId, refreshHash, refreshExpiresAt, ts, ts).run();

  const accessPayload: AccessTokenPayload = {
    typ: 'access',
    sub: userId,
    bid: brainId,
    sid: sessionId,
    iat: ts,
    exp: ts + ACCESS_TOKEN_TTL_SECONDS,
  };
  const accessToken = await signAccessToken(accessPayload, env.AUTH_SECRET);
  return {
    access_token: accessToken,
    refresh_token: refreshToken,
    expires_in: ACCESS_TOKEN_TTL_SECONDS,
    refresh_expires_in: REFRESH_TOKEN_TTL_SECONDS,
    session_id: sessionId,
  };
}

async function rotateSession(refreshToken: string, env: Env): Promise<{
  userId: string;
  brainId: string;
  tokens: SessionTokens;
} | null> {
  const refreshHash = await sha256DigestBase64Url(refreshToken);
  const ts = now();
  const session = await env.DB.prepare(
    `SELECT id, user_id, brain_id, expires_at, revoked_at
     FROM auth_sessions
     WHERE refresh_hash = ?
     LIMIT 1`
  ).bind(refreshHash).first<{ id: string; user_id: string; brain_id: string; expires_at: number; revoked_at: number | null }>();
  if (!session) return null;
  if (session.revoked_at !== null || session.expires_at <= ts) return null;
  const tokens = await createSessionTokens(session.user_id, session.brain_id, env);
  await env.DB.prepare(
    'UPDATE auth_sessions SET revoked_at = ?, replaced_by = ? WHERE id = ?'
  ).bind(ts, tokens.session_id, session.id).run();
  return { userId: session.user_id, brainId: session.brain_id, tokens };
}

async function revokeSession(refreshToken: string, env: Env): Promise<boolean> {
  const refreshHash = await sha256DigestBase64Url(refreshToken);
  const ts = now();
  const result = await env.DB.prepare(
    'UPDATE auth_sessions SET revoked_at = ? WHERE refresh_hash = ? AND revoked_at IS NULL'
  ).bind(ts, refreshHash).run();
  return (result.meta.changes ?? 0) > 0;
}

async function authenticateRequest(request: Request, env: Env): Promise<AuthContext | null> {
  const token = parseBearerToken(request);
  if (!token) return null;
  if (token === env.AUTH_SECRET) {
    return { kind: 'legacy', brainId: LEGACY_BRAIN_ID, userId: null, sessionId: null };
  }

  const payload = await verifyAccessToken(token, env.AUTH_SECRET);
  if (!payload) return null;
  const ts = now();
  const row = await env.DB.prepare(
    `SELECT s.id, s.user_id, s.brain_id
     FROM auth_sessions s
     JOIN brain_memberships bm ON bm.user_id = s.user_id AND bm.brain_id = s.brain_id
     WHERE s.id = ?
       AND s.user_id = ?
       AND s.brain_id = ?
       AND s.expires_at > ?
       AND s.revoked_at IS NULL
     LIMIT 1`
  ).bind(payload.sid, payload.sub, payload.bid, ts).first<{ id: string; user_id: string; brain_id: string }>();
  if (!row) return null;
  await env.DB.prepare('UPDATE auth_sessions SET used_at = ? WHERE id = ?').bind(ts, row.id).run();
  return { kind: 'user', brainId: row.brain_id, userId: row.user_id, sessionId: row.id };
}

async function isRateLimited(ip: string, env: Env): Promise<boolean> {
  const window = Math.floor(Date.now() / (15 * 60 * 1000));
  const row = await env.DB.prepare(
    'SELECT count FROM rate_limits WHERE ip = ? AND window = ?'
  ).bind(ip, window).first<{ count: number }>();
  return (row?.count ?? 0) >= 10;
}

async function recordFailedAttempt(ip: string, env: Env): Promise<void> {
  const window = Math.floor(Date.now() / (15 * 60 * 1000));
  await env.DB.prepare(
    'INSERT INTO rate_limits (ip, window, count) VALUES (?, ?, 1) ON CONFLICT(ip, window) DO UPDATE SET count = count + 1'
  ).bind(ip, window).run();
  // 1% chance: delete rows older than 2 hours (8 windows) to prevent unbounded growth
  if (Math.random() < 0.01) {
    const cutoff = window - 8;
    await env.DB.prepare('DELETE FROM rate_limits WHERE window < ?').bind(cutoff).run();
  }
}

async function clearRateLimit(ip: string, env: Env): Promise<void> {
  const window = Math.floor(Date.now() / (15 * 60 * 1000));
  await env.DB.prepare(
    'DELETE FROM rate_limits WHERE ip = ? AND window = ?'
  ).bind(ip, window).run();
}

const VALID_TYPES = ['note', 'fact', 'journal'] as const;
type MemoryType = typeof VALID_TYPES[number];
const RELATION_TYPES = ['related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of'] as const;
type RelationType = typeof RELATION_TYPES[number];

function isValidType(t: unknown): t is MemoryType {
  return typeof t === 'string' && (VALID_TYPES as readonly string[]).includes(t);
}

function isValidRelationType(t: unknown): t is RelationType {
  return typeof t === 'string' && (RELATION_TYPES as readonly string[]).includes(t);
}

function clampToRange(input: unknown, fallback: number, min = 0, max = 1): number {
  const n = typeof input === 'number' ? input : Number(input);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(n, min), max);
}

async function runMigrationStatement(env: Env, sql: string): Promise<void> {
  try {
    await env.DB.prepare(sql).run();
  } catch (err) {
    const msg = err instanceof Error ? err.message.toLowerCase() : '';
    if (msg.includes('duplicate column name') || msg.includes('already exists')) return;
    throw err;
  }
}

let schemaReady: Promise<void> | null = null;
async function ensureSchema(env: Env): Promise<void> {
  if (!schemaReady) {
    schemaReady = (async () => {
      await runMigrationStatement(env, "ALTER TABLE memories ADD COLUMN source TEXT");
      await runMigrationStatement(env, "ALTER TABLE memories ADD COLUMN confidence REAL NOT NULL DEFAULT 0.7");
      await runMigrationStatement(env, "ALTER TABLE memories ADD COLUMN importance REAL NOT NULL DEFAULT 0.5");
      await runMigrationStatement(env, "ALTER TABLE memories ADD COLUMN archived_at INTEGER");
      await runMigrationStatement(env, "ALTER TABLE memories ADD COLUMN brain_id TEXT");
      await runMigrationStatement(env, "ALTER TABLE memory_links ADD COLUMN relation_type TEXT NOT NULL DEFAULT 'related'");
      await runMigrationStatement(env, "ALTER TABLE memory_links ADD COLUMN brain_id TEXT");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_archived ON memories(archived_at)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_importance ON memories(importance DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_confidence ON memories(confidence DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_links_relation_type ON memory_links(relation_type)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_memories_brain_created ON memories(brain_id, created_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_memories_brain_key ON memories(brain_id, key)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_links_brain_from ON memory_links(brain_id, from_id)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_links_brain_to ON memory_links(brain_id, to_id)");
      await runMigrationStatement(env, "UPDATE memories SET confidence = 0.7 WHERE confidence IS NULL");
      await runMigrationStatement(env, "UPDATE memories SET importance = 0.5 WHERE importance IS NULL");
      await runMigrationStatement(env, "UPDATE memory_links SET relation_type = 'related' WHERE relation_type IS NULL");
      await runMigrationStatement(env, `UPDATE memories SET brain_id = '${LEGACY_BRAIN_ID}' WHERE brain_id IS NULL OR brain_id = ''`);
      await runMigrationStatement(env, `UPDATE memory_links SET brain_id = '${LEGACY_BRAIN_ID}' WHERE brain_id IS NULL OR brain_id = ''`);
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS memory_changelog (
          id TEXT PRIMARY KEY,
          brain_id TEXT NOT NULL DEFAULT '${LEGACY_BRAIN_ID}',
          event_type TEXT NOT NULL,
          entity_type TEXT NOT NULL,
          entity_id TEXT NOT NULL,
          summary TEXT NOT NULL,
          payload TEXT,
          created_at INTEGER NOT NULL
        )`
      );
      await runMigrationStatement(env, `ALTER TABLE memory_changelog ADD COLUMN brain_id TEXT NOT NULL DEFAULT '${LEGACY_BRAIN_ID}'`);
      await runMigrationStatement(env, `UPDATE memory_changelog SET brain_id = '${LEGACY_BRAIN_ID}' WHERE brain_id IS NULL OR brain_id = ''`);
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_changelog_created ON memory_changelog(created_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_changelog_entity ON memory_changelog(entity_type, entity_id, created_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_changelog_brain_created ON memory_changelog(brain_id, created_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_changelog_brain_entity ON memory_changelog(brain_id, entity_type, entity_id, created_at DESC)");

      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS users (
          id TEXT PRIMARY KEY,
          email TEXT NOT NULL UNIQUE,
          password_hash TEXT NOT NULL,
          display_name TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        )`
      );
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS brains (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          slug TEXT UNIQUE,
          owner_user_id TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE SET NULL
        )`
      );
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS brain_memberships (
          brain_id TEXT NOT NULL,
          user_id TEXT NOT NULL,
          role TEXT NOT NULL DEFAULT 'member',
          created_at INTEGER NOT NULL,
          PRIMARY KEY (brain_id, user_id),
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE,
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_brain_memberships_user ON brain_memberships(user_id, brain_id)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_brains_owner ON brains(owner_user_id)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS auth_sessions (
          id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          brain_id TEXT NOT NULL,
          refresh_hash TEXT NOT NULL UNIQUE,
          expires_at INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          used_at INTEGER NOT NULL,
          revoked_at INTEGER,
          replaced_by TEXT,
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(user_id, expires_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_auth_sessions_brain ON auth_sessions(brain_id, expires_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires ON auth_sessions(expires_at)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS oauth_clients (
          id TEXT PRIMARY KEY,
          client_id TEXT NOT NULL UNIQUE,
          client_name TEXT,
          redirect_uris TEXT NOT NULL,
          grant_types TEXT NOT NULL,
          response_types TEXT NOT NULL,
          token_endpoint_auth_method TEXT NOT NULL DEFAULT 'none',
          client_secret_hash TEXT,
          client_secret_expires_at INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_oauth_clients_client_id ON oauth_clients(client_id)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS oauth_authorization_codes (
          id TEXT PRIMARY KEY,
          code TEXT NOT NULL UNIQUE,
          client_id TEXT NOT NULL,
          redirect_uri TEXT NOT NULL,
          user_id TEXT NOT NULL,
          brain_id TEXT NOT NULL,
          code_challenge TEXT NOT NULL,
          code_challenge_method TEXT NOT NULL DEFAULT 'S256',
          scope TEXT,
          resource TEXT,
          created_at INTEGER NOT NULL,
          expires_at INTEGER NOT NULL,
          used_at INTEGER
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_oauth_codes_code ON oauth_authorization_codes(code)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_oauth_codes_expires ON oauth_authorization_codes(expires_at)");

      const ts = now();
      await env.DB.prepare(
        'INSERT OR IGNORE INTO brains (id, name, slug, owner_user_id, created_at, updated_at) VALUES (?, ?, ?, NULL, ?, ?)'
      ).bind(LEGACY_BRAIN_ID, 'Legacy Shared Brain', 'legacy-shared-brain', ts, ts).run();
    })().catch((err) => {
      schemaReady = null;
      throw err;
    });
  }
  await schemaReady;
}

type LinkStats = {
  link_count: number;
  supports_count: number;
  contradicts_count: number;
  supersedes_count: number;
  causes_count: number;
  example_of_count: number;
};

const EMPTY_LINK_STATS: LinkStats = {
  link_count: 0,
  supports_count: 0,
  contradicts_count: 0,
  supersedes_count: 0,
  causes_count: 0,
  example_of_count: 0,
};

function toFiniteNumber(value: unknown, fallback: number): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp01(value: number): number {
  return Math.min(Math.max(value, 0), 1);
}

function round3(value: number): number {
  return Math.round(value * 1000) / 1000;
}

function countKeywordHits(haystack: string, terms: string[]): number {
  let count = 0;
  for (const term of terms) {
    if (haystack.includes(term)) count++;
  }
  return count;
}

function normalizeLinkStats(raw?: Partial<LinkStats>): LinkStats {
  return {
    link_count: toFiniteNumber(raw?.link_count, 0),
    supports_count: toFiniteNumber(raw?.supports_count, 0),
    contradicts_count: toFiniteNumber(raw?.contradicts_count, 0),
    supersedes_count: toFiniteNumber(raw?.supersedes_count, 0),
    causes_count: toFiniteNumber(raw?.causes_count, 0),
    example_of_count: toFiniteNumber(raw?.example_of_count, 0),
  };
}

async function loadLinkStatsMap(env: Env, brainId: string): Promise<Map<string, LinkStats>> {
  const rows = await env.DB.prepare(
    `SELECT
      rel.memory_id,
      COUNT(*) AS link_count,
      SUM(CASE WHEN rel.relation_type = 'supports' THEN 1 ELSE 0 END) AS supports_count,
      SUM(CASE WHEN rel.relation_type = 'contradicts' THEN 1 ELSE 0 END) AS contradicts_count,
      SUM(CASE WHEN rel.relation_type = 'supersedes' THEN 1 ELSE 0 END) AS supersedes_count,
      SUM(CASE WHEN rel.relation_type = 'causes' THEN 1 ELSE 0 END) AS causes_count,
      SUM(CASE WHEN rel.relation_type = 'example_of' THEN 1 ELSE 0 END) AS example_of_count
    FROM (
      SELECT from_id AS memory_id, relation_type FROM memory_links WHERE brain_id = ?
      UNION ALL
      SELECT to_id AS memory_id, relation_type FROM memory_links WHERE brain_id = ?
    ) AS rel
    GROUP BY rel.memory_id`
  ).bind(brainId, brainId).all<Record<string, unknown>>();

  const statsMap = new Map<string, LinkStats>();
  for (const row of rows.results) {
    const memoryId = typeof row.memory_id === 'string' ? row.memory_id : '';
    if (!memoryId) continue;
    statsMap.set(memoryId, {
      link_count: toFiniteNumber(row.link_count, 0),
      supports_count: toFiniteNumber(row.supports_count, 0),
      contradicts_count: toFiniteNumber(row.contradicts_count, 0),
      supersedes_count: toFiniteNumber(row.supersedes_count, 0),
      causes_count: toFiniteNumber(row.causes_count, 0),
      example_of_count: toFiniteNumber(row.example_of_count, 0),
    });
  }
  return statsMap;
}

function computeDynamicScores(
  memory: Record<string, unknown>,
  rawStats?: Partial<LinkStats>,
  tsNow = now()
): Record<string, unknown> {
  const stats = normalizeLinkStats(rawStats);
  const baseConfidence = clamp01(toFiniteNumber(memory.confidence, 0.7));
  const baseImportance = clamp01(toFiniteNumber(memory.importance, 0.5));
  const createdAt = toFiniteNumber(memory.created_at, tsNow);
  const updatedAt = toFiniteNumber(memory.updated_at, createdAt);
  const ageDays = Math.max(0, (tsNow - updatedAt) / 86400);
  const memoryType = typeof memory.type === 'string' ? memory.type.toLowerCase() : '';
  const sourceText = typeof memory.source === 'string' ? memory.source.trim().toLowerCase() : '';
  const textBlob = [
    typeof memory.title === 'string' ? memory.title : '',
    typeof memory.key === 'string' ? memory.key : '',
    typeof memory.content === 'string' ? memory.content : '',
    typeof memory.tags === 'string' ? memory.tags : '',
  ].join(' ').toLowerCase();

  const certaintyHits = countKeywordHits(textBlob, ['verified', 'confirmed', 'exact', 'measured', 'token', 'id', 'official', 'passed']);
  const hedgeHits = countKeywordHits(textBlob, ['maybe', 'might', 'perhaps', 'guess', 'probably', 'vague', 'unsure', 'i think']);
  const importanceHits = countKeywordHits(textBlob, ['goal', 'strategy', 'deadline', 'todo', 'must', 'critical', 'priority', 'plan', 'task', 'decision', 'launch', 'ship']);
  const highSignalSource = sourceText && countKeywordHits(sourceText, ['api', 'system', 'log', 'metric', 'official', 'doc', 'test', 'monitor']);
  const lowSignalSource = sourceText && countKeywordHits(sourceText, ['rumor', 'guess', 'hearsay', 'vibe', 'idea']);
  const contentLength = textBlob.replace(/\s+/g, '').length;

  const sourceBonus = highSignalSource
    ? 0.09
    : sourceText
      ? 0.04
      : 0;
  const sourcePenalty = lowSignalSource ? 0.07 : 0;
  const certaintySignal = Math.min(0.2, certaintyHits * 0.04);
  const hedgePenalty = Math.min(0.2, hedgeHits * 0.055);
  const importanceKeywordSignal = Math.min(0.24, importanceHits * 0.045);
  const contentDepthSignal = Math.min(0.08, Math.max(0, (contentLength - 80) / 420) * 0.08);
  const typeConfidenceBias = memoryType === 'fact' ? 0.08 : memoryType === 'journal' ? -0.06 : 0;
  const typeImportanceBias = memoryType === 'note' ? 0.04 : memoryType === 'fact' ? 0.02 : 0.01;
  const linkSignal = Math.min(0.18, Math.log1p(stats.link_count) * 0.06);
  const supportSignal = Math.min(0.22, stats.supports_count * 0.05);
  const contradictionPenalty = Math.min(0.28, stats.contradicts_count * 0.09);
  const causeSignal = Math.min(0.14, stats.causes_count * 0.04);
  const exampleSignal = Math.min(0.08, stats.example_of_count * 0.02);
  const supersedeSignal = Math.min(0.08, stats.supersedes_count * 0.02);
  const stalePenalty = Math.min(0.2, ageDays / 365 * 0.16);
  const recencyImportance = ageDays < 3
    ? 0.12
    : ageDays < 14
      ? 0.07
      : ageDays < 60
        ? 0.03
        : -Math.min(0.18, (ageDays - 60) / 365 * 0.18);

  const dynamicConfidence = round3(clamp01(
    baseConfidence
      + sourceBonus
      + certaintySignal
      + typeConfidenceBias
      + supportSignal
      + (linkSignal * 0.35)
      + (exampleSignal * 0.25)
      - contradictionPenalty
      - hedgePenalty
      - sourcePenalty
      - stalePenalty
  ));

  const dynamicImportance = round3(clamp01(
    baseImportance
      + importanceKeywordSignal
      + contentDepthSignal
      + typeImportanceBias
      + linkSignal
      + causeSignal
      + exampleSignal
      + supersedeSignal
      + recencyImportance
      - (contradictionPenalty * 0.25)
  ));

  return {
    ...stats,
    dynamic_confidence: dynamicConfidence,
    dynamic_importance: dynamicImportance,
  };
}

function enrichMemoryRowsWithDynamics(
  rows: Array<Record<string, unknown>>,
  linkStatsMap: Map<string, LinkStats>,
  tsNow = now()
): Array<Record<string, unknown>> {
  return rows.map((row) => {
    const id = typeof row.id === 'string' ? row.id : '';
    const stats = id ? (linkStatsMap.get(id) ?? EMPTY_LINK_STATS) : EMPTY_LINK_STATS;
    return {
      ...row,
      ...computeDynamicScores(row, stats, tsNow),
    };
  });
}

function projectMemoryForClient(row: Record<string, unknown>): Record<string, unknown> {
  const baseConfidence = clamp01(toFiniteNumber(row.confidence, 0.7));
  const baseImportance = clamp01(toFiniteNumber(row.importance, 0.5));
  const dynConfidence = clamp01(toFiniteNumber(row.dynamic_confidence, baseConfidence));
  const dynImportance = clamp01(toFiniteNumber(row.dynamic_importance, baseImportance));
  return {
    ...row,
    base_confidence: round3(baseConfidence),
    base_importance: round3(baseImportance),
    confidence: round3(dynConfidence),
    importance: round3(dynImportance),
    dynamic_confidence: round3(dynConfidence),
    dynamic_importance: round3(dynImportance),
  };
}

async function enrichAndProjectRows(
  env: Env,
  brainId: string,
  rows: Array<Record<string, unknown>>,
  tsNow = now()
): Promise<Array<Record<string, unknown>>> {
  if (!rows.length) return [];
  const linkStatsMap = await loadLinkStatsMap(env, brainId);
  return enrichMemoryRowsWithDynamics(rows, linkStatsMap, tsNow).map(projectMemoryForClient);
}

type GraphEdge = { from: string; to: string; relation_type: RelationType };
type GraphNeighbor = { id: string; relation_type: RelationType };

function relationSignalWeight(relationType: RelationType): number {
  switch (relationType) {
    case 'supports': return 0.88;
    case 'causes': return 0.82;
    case 'example_of': return 0.7;
    case 'supersedes': return 0.65;
    case 'contradicts': return -0.75;
    case 'related':
    default:
      return 0.62;
  }
}

function relationSpreadWeight(relationType: RelationType): number {
  switch (relationType) {
    case 'supports': return 1;
    case 'causes': return 0.9;
    case 'example_of': return 0.75;
    case 'supersedes': return 0.72;
    case 'contradicts': return -0.65;
    case 'related':
    default:
      return 0.68;
  }
}

function normalizeRelation(raw: unknown): RelationType {
  return isValidRelationType(raw) ? raw : 'related';
}

function buildAdjacencyFromEdges(edges: GraphEdge[]): Map<string, GraphNeighbor[]> {
  const adjacency = new Map<string, GraphNeighbor[]>();
  for (const edge of edges) {
    const rel = normalizeRelation(edge.relation_type);
    const fromArr = adjacency.get(edge.from);
    if (fromArr) fromArr.push({ id: edge.to, relation_type: rel });
    else adjacency.set(edge.from, [{ id: edge.to, relation_type: rel }]);
    const toArr = adjacency.get(edge.to);
    if (toArr) toArr.push({ id: edge.from, relation_type: rel });
    else adjacency.set(edge.to, [{ id: edge.from, relation_type: rel }]);
  }
  return adjacency;
}

function slugify(input: string): string {
  return input
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 64) || 'objective';
}

function stableJson(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return JSON.stringify({ note: 'unserializable payload' });
  }
}

async function logChangelog(
  env: Env,
  brainId: string,
  eventType: string,
  entityType: string,
  entityId: string,
  summary: string,
  payload?: unknown
): Promise<void> {
  await env.DB.prepare(
    'INSERT INTO memory_changelog (id, brain_id, event_type, entity_type, entity_id, summary, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
  ).bind(
    generateId(),
    brainId,
    eventType,
    entityType,
    entityId,
    summary,
    payload === undefined ? null : stableJson(payload),
    now()
  ).run();
}

async function ensureObjectiveRoot(env: Env, brainId: string): Promise<string> {
  const key = 'autonomous_objectives_root';
  const existing = await env.DB.prepare(
    'SELECT id FROM memories WHERE brain_id = ? AND key = ? AND archived_at IS NULL ORDER BY created_at ASC LIMIT 1'
  ).bind(brainId, key).first<{ id: string }>();
  if (existing?.id) return existing.id;

  const ts = now();
  const id = generateId();
  await env.DB.prepare(
    'INSERT INTO memories (id, brain_id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)'
  ).bind(
    id,
    brainId,
    'note',
    'Autonomous Objectives Network',
    key,
    'Root node for long-term goals and curiosities.',
    'objective_root,autonomous_objectives,system_node',
    'system',
    0.9,
    0.95,
    ts,
    ts
  ).run();
  await logChangelog(env, brainId, 'objective_root_created', 'memory', id, 'Created autonomous objectives root');
  return id;
}

const TOOLS = [
  {
    name: 'memory_save',
    description: 'Save a new memory. Use type="note" for titled knowledge entries, type="fact" for key=value pairs, type="journal" for free-form thoughts.',
    inputSchema: {
      type: 'object',
      properties: {
        type: { type: 'string', enum: ['note', 'fact', 'journal'], description: 'Memory type' },
        content: { type: 'string', description: 'The memory content' },
        title: { type: 'string', description: 'Title (for notes)' },
        key: { type: 'string', description: 'Key name (for facts, e.g. "user_name")' },
        tags: { type: 'string', description: 'Comma-separated tags' },
        source: { type: 'string', description: 'Source system/person for this memory' },
        confidence: { type: 'number', minimum: 0, maximum: 1, description: 'Reliability score from 0 to 1 (default 0.7)' },
        importance: { type: 'number', minimum: 0, maximum: 1, description: 'Priority score from 0 to 1 (default 0.5)' },
      },
      required: ['type', 'content'],
    },
  },
  {
    name: 'memory_get',
    description: 'Retrieve a specific memory by its ID.',
    inputSchema: {
      type: 'object',
      properties: { id: { type: 'string', description: 'Memory ID' } },
      required: ['id'],
    },
  },
  {
    name: 'memory_get_fact',
    description: 'Fast lookup of a fact memory by its key name.',
    inputSchema: {
      type: 'object',
      properties: { key: { type: 'string', description: 'The fact key to look up' } },
      required: ['key'],
    },
  },
  {
    name: 'memory_search',
    description: 'Search memories by name/title, key, id, source, or text content across all memory types.',
    inputSchema: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Search query' },
        type: { type: 'string', enum: ['note', 'fact', 'journal'], description: 'Optionally filter by type' },
      },
      required: ['query'],
    },
  },
  {
    name: 'memory_list',
    description: 'List memories with optional filters by type or tag.',
    inputSchema: {
      type: 'object',
      properties: {
        type: { type: 'string', enum: ['note', 'fact', 'journal'], description: 'Filter by type' },
        tag: { type: 'string', description: 'Filter by tag' },
        limit: { type: 'number', description: 'Max results (1-100, default 20)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_update',
    description: 'Update an existing memory by ID. Only provided fields are updated.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Memory ID to update' },
        content: { type: 'string', description: 'New content' },
        title: { type: 'string', description: 'New title' },
        tags: { type: 'string', description: 'New comma-separated tags' },
        source: { type: 'string', description: 'New source' },
        confidence: { type: 'number', minimum: 0, maximum: 1, description: 'Updated confidence score (0..1)' },
        importance: { type: 'number', minimum: 0, maximum: 1, description: 'Updated importance score (0..1)' },
        archived: { type: 'boolean', description: 'Set true to archive, false to restore' },
      },
      required: ['id'],
    },
  },
  {
    name: 'memory_delete',
    description: 'Permanently delete a memory by ID.',
    inputSchema: {
      type: 'object',
      properties: { id: { type: 'string', description: 'Memory ID to delete' } },
      required: ['id'],
    },
  },
  {
    name: 'memory_stats',
    description: 'Get statistics about stored memories: counts by type and recent activity.',
    inputSchema: { type: 'object', properties: {}, required: [] },
  },
  {
    name: 'memory_link',
    description: 'Create or update a relationship between two memories. Set relation_type for graph reasoning (supports/contradicts/supersedes/etc).',
    inputSchema: {
      type: 'object',
      properties: {
        from_id: { type: 'string', description: 'Source memory ID' },
        to_id: { type: 'string', description: 'Target memory ID' },
        relation_type: { type: 'string', enum: ['related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of'], description: 'Structured relationship type (default "related")' },
        label: { type: 'string', description: 'Optional free-text description of the relationship' },
      },
      required: ['from_id', 'to_id'],
    },
  },
  {
    name: 'memory_unlink',
    description: 'Remove a link between two memories.',
    inputSchema: {
      type: 'object',
      properties: {
        from_id: { type: 'string', description: 'Source memory ID' },
        to_id: { type: 'string', description: 'Target memory ID' },
        relation_type: { type: 'string', enum: ['related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of'], description: 'Optional relation type filter when unlinking' },
      },
      required: ['from_id', 'to_id'],
    },
  },
  {
    name: 'memory_links',
    description: 'Get all memories linked to a given memory, including relation_type and labels.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Memory ID to get connections for' },
      },
      required: ['id'],
    },
  },
  {
    name: 'memory_consolidate',
    description: 'Consolidate likely-duplicate memories by keeping one canonical memory and archiving duplicates with supersedes links.',
    inputSchema: {
      type: 'object',
      properties: {
        type: { type: 'string', enum: ['note', 'fact', 'journal'], description: 'Optional memory type filter' },
        tag: { type: 'string', description: 'Optional tag filter' },
        older_than_days: { type: 'number', description: 'Only consolidate memories older than this age' },
        limit: { type: 'number', description: 'Max memories to scan (default 300, max 1000)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_forget',
    description: 'Archive or delete memories by ID or policy filters for controlled forgetting.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Specific memory ID to forget' },
        mode: { type: 'string', enum: ['soft', 'hard'], description: 'soft archives; hard deletes (default soft)' },
        tag: { type: 'string', description: 'Optional tag filter for batch mode' },
        older_than_days: { type: 'number', description: 'Optional minimum age in days for batch mode' },
        max_importance: { type: 'number', minimum: 0, maximum: 1, description: 'Only forget memories with importance <= this threshold' },
        limit: { type: 'number', description: 'Batch size (default 25, max 200)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_activate',
    description: 'Run spreading-activation retrieval from seed memories (id/query) across the memory graph.',
    inputSchema: {
      type: 'object',
      properties: {
        seed_id: { type: 'string', description: 'Optional seed memory id' },
        query: { type: 'string', description: 'Optional query to select seed memories by id/name/content' },
        hops: { type: 'number', description: 'Propagation depth (1-4, default 2)' },
        limit: { type: 'number', description: 'Max returned activations (1-100, default 20)' },
        include_inferred: { type: 'boolean', description: 'Include tag-based inferred synapses (default true)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_reinforce',
    description: 'Apply Hebbian-style reinforcement to a memory and optionally spread updates to connected neighbors.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Memory id to reinforce' },
        delta_confidence: { type: 'number', description: 'Base confidence delta (default +0.04)' },
        delta_importance: { type: 'number', description: 'Base importance delta (default +0.06)' },
        spread: { type: 'number', minimum: 0, maximum: 1, description: 'How much update spreads to neighbors (default 0.35)' },
        hops: { type: 'number', description: 'Spread depth (0-3, default 1)' },
      },
      required: ['id'],
    },
  },
  {
    name: 'memory_decay',
    description: 'Apply homeostatic decay to stale low-connectivity memories.',
    inputSchema: {
      type: 'object',
      properties: {
        older_than_days: { type: 'number', description: 'Only decay memories older than N days (default 30)' },
        max_link_count: { type: 'number', description: 'Only decay memories with links <= this count (default 1)' },
        decay_confidence: { type: 'number', description: 'Confidence decrement per memory (default 0.01)' },
        decay_importance: { type: 'number', description: 'Importance decrement per memory (default 0.03)' },
        limit: { type: 'number', description: 'Max memories to decay (default 200)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_changelog',
    description: 'Read the memory changelog so agents can quickly detect what changed since last run.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: { type: 'number', description: 'Max entries to return (default 25, max 200)' },
        since: { type: 'number', description: 'Unix timestamp (seconds). Return entries after this time' },
        event_type: { type: 'string', description: 'Optional event type filter' },
        entity_id: { type: 'string', description: 'Optional entity id filter' },
      },
      required: [],
    },
  },
  {
    name: 'memory_conflicts',
    description: 'Detect contradictions between high-confidence facts (explicit contradict links + conflicting fact keys).',
    inputSchema: {
      type: 'object',
      properties: {
        min_confidence: { type: 'number', minimum: 0, maximum: 1, description: 'Only include conflicts when both sides are >= this confidence (default 0.7)' },
        limit: { type: 'number', description: 'Max conflicts returned (default 40)' },
      },
      required: [],
    },
  },
  {
    name: 'objective_set',
    description: 'Create or update a dedicated autonomous objective node (goal or curiosity) and connect it to the root objective node.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Existing objective memory id (optional)' },
        title: { type: 'string', description: 'Objective title' },
        content: { type: 'string', description: 'Objective details or rationale' },
        kind: { type: 'string', enum: ['goal', 'curiosity'], description: 'Objective type (default goal)' },
        horizon: { type: 'string', enum: ['short', 'medium', 'long'], description: 'Planning horizon (default long)' },
        status: { type: 'string', enum: ['active', 'paused', 'done'], description: 'Objective status (default active)' },
        priority: { type: 'number', minimum: 0, maximum: 1, description: 'Base priority/importance (default 0.8)' },
        tags: { type: 'string', description: 'Additional comma-separated tags' },
      },
      required: ['title'],
    },
  },
  {
    name: 'objective_list',
    description: 'List autonomous objective nodes (goals/curiosities) for planning.',
    inputSchema: {
      type: 'object',
      properties: {
        kind: { type: 'string', enum: ['goal', 'curiosity'], description: 'Optional objective kind filter' },
        status: { type: 'string', enum: ['active', 'paused', 'done'], description: 'Optional status filter' },
        limit: { type: 'number', description: 'Max objectives to return (default 50)' },
      },
      required: [],
    },
  },
];

type ToolArgs = Record<string, unknown>;
type McpResult = { content: Array<{ type: string; text: string }> };

async function callTool(name: string, args: ToolArgs, env: Env, brainId: string): Promise<McpResult> {
  switch (name) {
    case 'memory_save': {
      const { type, content, title, key, tags, source, confidence, importance } = args as {
        type: unknown;
        content: unknown;
        title?: unknown;
        key?: unknown;
        tags?: unknown;
        source?: unknown;
        confidence?: unknown;
        importance?: unknown;
      };
      if (!isValidType(type)) return { content: [{ type: 'text', text: 'Invalid type. Must be note, fact, or journal.' }] };
      if (typeof content !== 'string' || content.trim() === '') return { content: [{ type: 'text', text: 'content must be a non-empty string.' }] };
      if (source !== undefined && typeof source !== 'string') return { content: [{ type: 'text', text: 'source must be a string when provided.' }] };
      const id = generateId();
      const ts = now();
      const confidenceVal = clampToRange(confidence, 0.7);
      const importanceVal = clampToRange(importance, 0.5);
      await env.DB.prepare(
        'INSERT INTO memories (id, brain_id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)'
      ).bind(
        id,
        brainId,
        type,
        typeof title === 'string' ? title : null,
        typeof key === 'string' ? key : null,
        content.trim(),
        typeof tags === 'string' ? tags : null,
        typeof source === 'string' ? source : null,
        confidenceVal,
        importanceVal,
        ts,
        ts
      ).run();
      // Find up to 5 existing memories sharing at least one tag (for suggested linking)
      let suggestedLinks: unknown[] = [];
      if (typeof tags === 'string' && tags.trim()) {
        const tagList = tags.split(',').map((t: string) => t.trim()).filter(Boolean);
        if (tagList.length > 0) {
          const conditions = tagList.map(() => 'tags LIKE ?').join(' OR ');
          const bindings = tagList.map((t: string) => `%${t}%`);
          const suggestions = await env.DB.prepare(
            `SELECT id, type, title, key, tags FROM memories WHERE brain_id = ? AND archived_at IS NULL AND id != ? AND (${conditions}) LIMIT 5`
          ).bind(brainId, id, ...bindings).all();
          suggestedLinks = suggestions.results;
        }
      }

      const insertedRow: Record<string, unknown> = {
        id,
        type,
        title: typeof title === 'string' ? title : null,
        key: typeof key === 'string' ? key : null,
        content: content.trim(),
        tags: typeof tags === 'string' ? tags : null,
        source: typeof source === 'string' ? source : null,
        confidence: confidenceVal,
        importance: importanceVal,
        created_at: ts,
        updated_at: ts,
      };
      const scoredMemory = projectMemoryForClient({
        ...insertedRow,
        ...computeDynamicScores(insertedRow, EMPTY_LINK_STATS, ts),
      });

      const saveResult: Record<string, unknown> = {
        id,
        message: `Saved memory with id: ${id}`,
        confidence: scoredMemory.confidence,
        importance: scoredMemory.importance,
        dynamic_confidence: scoredMemory.dynamic_confidence,
        dynamic_importance: scoredMemory.dynamic_importance,
        base_confidence: scoredMemory.base_confidence,
        base_importance: scoredMemory.base_importance,
      };
      if (suggestedLinks.length > 0) saveResult.suggested_links = suggestedLinks;
      await logChangelog(env, brainId, 'memory_created', 'memory', id, 'Created memory', {
        type,
        title: typeof title === 'string' ? title : null,
        key: typeof key === 'string' ? key : null,
      });
      return { content: [{ type: 'text', text: JSON.stringify(saveResult) }] };
    }

    case 'memory_get': {
      const { id } = args as { id: unknown };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };
      const row = await env.DB.prepare('SELECT * FROM memories WHERE brain_id = ? AND id = ?').bind(brainId, id).first<Record<string, unknown>>();
      if (!row) return { content: [{ type: 'text', text: 'Memory not found.' }] };
      const [scored] = await enrichAndProjectRows(env, brainId, [row]);
      return { content: [{ type: 'text', text: JSON.stringify(scored ?? row, null, 2) }] };
    }

    case 'memory_get_fact': {
      const { key } = args as { key: unknown };
      if (typeof key !== 'string' || !key) return { content: [{ type: 'text', text: 'key must be a non-empty string.' }] };
      const row = await env.DB.prepare(
        'SELECT * FROM memories WHERE brain_id = ? AND type = ? AND key = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT 1'
      ).bind(brainId, 'fact', key).first<Record<string, unknown>>();
      if (!row) return { content: [{ type: 'text', text: `No fact found with key: ${key}` }] };
      const [scored] = await enrichAndProjectRows(env, brainId, [row]);
      return { content: [{ type: 'text', text: JSON.stringify(scored ?? row, null, 2) }] };
    }

    case 'memory_search': {
      const { query, type } = args as { query: unknown; type?: unknown };
      if (typeof query !== 'string' || query.trim() === '') return { content: [{ type: 'text', text: 'query must be a non-empty string.' }] };
      if (type !== undefined && !isValidType(type)) return { content: [{ type: 'text', text: 'Invalid type filter.' }] };
      const like = `%${query.trim()}%`;
      let stmt;
      if (type) {
        stmt = env.DB.prepare(
          'SELECT * FROM memories WHERE brain_id = ? AND archived_at IS NULL AND type = ? AND (id LIKE ? OR content LIKE ? OR title LIKE ? OR key LIKE ? OR source LIKE ?) ORDER BY created_at DESC LIMIT 20'
        ).bind(brainId, type, like, like, like, like, like);
      } else {
        stmt = env.DB.prepare(
          'SELECT * FROM memories WHERE brain_id = ? AND archived_at IS NULL AND (id LIKE ? OR content LIKE ? OR title LIKE ? OR key LIKE ? OR source LIKE ?) ORDER BY created_at DESC LIMIT 20'
        ).bind(brainId, like, like, like, like, like);
      }
      const results = await stmt.all<Record<string, unknown>>();
      if (!results.results.length) return { content: [{ type: 'text', text: 'No memories found.' }] };
      const scored = await enrichAndProjectRows(env, brainId, results.results);
      return { content: [{ type: 'text', text: JSON.stringify(scored, null, 2) }] };
    }

    case 'memory_list': {
      const { type, tag, limit: rawLimit } = args as { type?: unknown; tag?: unknown; limit?: unknown };
      if (type !== undefined && !isValidType(type)) return { content: [{ type: 'text', text: 'Invalid type filter.' }] };
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 20, 1), 100);
      let query = 'SELECT * FROM memories WHERE brain_id = ? AND archived_at IS NULL';
      const params: unknown[] = [brainId];
      if (type) { query += ' AND type = ?'; params.push(type); }
      if (typeof tag === 'string' && tag) { query += ' AND tags LIKE ?'; params.push(`%${tag}%`); }
      query += ' ORDER BY created_at DESC LIMIT ?';
      params.push(limit);
      const results = await env.DB.prepare(query).bind(...params).all<Record<string, unknown>>();
      if (!results.results.length) return { content: [{ type: 'text', text: 'No memories found.' }] };
      const scored = await enrichAndProjectRows(env, brainId, results.results);
      return { content: [{ type: 'text', text: JSON.stringify(scored, null, 2) }] };
    }

    case 'memory_update': {
      const { id, content, title, tags, source, confidence, importance, archived } = args as {
        id: unknown;
        content?: unknown;
        title?: unknown;
        tags?: unknown;
        source?: unknown;
        confidence?: unknown;
        importance?: unknown;
        archived?: unknown;
      };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };
      if (source !== undefined && typeof source !== 'string') return { content: [{ type: 'text', text: 'source must be a string when provided.' }] };
      if (archived !== undefined && typeof archived !== 'boolean') return { content: [{ type: 'text', text: 'archived must be a boolean when provided.' }] };
      const existing = await env.DB.prepare('SELECT * FROM memories WHERE brain_id = ? AND id = ?').bind(brainId, id).first<{
        content: string;
        title: string | null;
        tags: string | null;
        source: string | null;
        confidence: number | null;
        importance: number | null;
        archived_at: number | null;
      }>();
      if (!existing) return { content: [{ type: 'text', text: 'Memory not found.' }] };
      const nextArchivedAt = typeof archived === 'boolean'
        ? (archived ? now() : null)
        : (existing.archived_at ?? null);
      await env.DB.prepare(
        'UPDATE memories SET content = ?, title = ?, tags = ?, source = ?, confidence = ?, importance = ?, archived_at = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
      ).bind(
        typeof content === 'string' && content.trim() ? content.trim() : existing.content,
        typeof title === 'string' ? title : existing.title,
        typeof tags === 'string' ? tags : existing.tags,
        typeof source === 'string' ? source : existing.source,
        confidence === undefined ? clampToRange(existing.confidence, 0.7) : clampToRange(confidence, 0.7),
        importance === undefined ? clampToRange(existing.importance, 0.5) : clampToRange(importance, 0.5),
        nextArchivedAt,
        now(),
        brainId,
        id
      ).run();
      const updated = await env.DB.prepare('SELECT * FROM memories WHERE brain_id = ? AND id = ?').bind(brainId, id).first<Record<string, unknown>>();
      if (!updated) return { content: [{ type: 'text', text: `Memory ${id} updated.` }] };
      const [scored] = await enrichAndProjectRows(env, brainId, [updated]);
      await logChangelog(env, brainId, 'memory_updated', 'memory', id, 'Updated memory', {
        updated_fields: {
          content: content !== undefined,
          title: title !== undefined,
          tags: tags !== undefined,
          source: source !== undefined,
          confidence: confidence !== undefined,
          importance: importance !== undefined,
          archived: archived !== undefined,
        },
      });
      return { content: [{ type: 'text', text: JSON.stringify({ message: `Memory ${id} updated.`, memory: scored ?? updated }) }] };
    }

    case 'memory_delete': {
      const { id } = args as { id: unknown };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };
      const result = await env.DB.prepare('DELETE FROM memories WHERE brain_id = ? AND id = ?').bind(brainId, id).run();
      if (result.meta.changes === 0) return { content: [{ type: 'text', text: 'Memory not found.' }] };
      await logChangelog(env, brainId, 'memory_deleted', 'memory', id, 'Deleted memory');
      return { content: [{ type: 'text', text: `Memory ${id} deleted.` }] };
    }

    case 'memory_stats': {
      const total = await env.DB.prepare('SELECT COUNT(*) as count FROM memories WHERE brain_id = ? AND archived_at IS NULL').bind(brainId).first<{ count: number }>();
      const archived = await env.DB.prepare('SELECT COUNT(*) as count FROM memories WHERE brain_id = ? AND archived_at IS NOT NULL').bind(brainId).first<{ count: number }>();
      const byType = await env.DB.prepare('SELECT type, COUNT(*) as count FROM memories WHERE brain_id = ? AND archived_at IS NULL GROUP BY type').bind(brainId).all();
      const relationStats = await env.DB.prepare('SELECT relation_type, COUNT(*) as count FROM memory_links WHERE brain_id = ? GROUP BY relation_type').bind(brainId).all();
      const recent = await env.DB.prepare(
        'SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance FROM memories WHERE brain_id = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT 5'
      ).bind(brainId).all<Record<string, unknown>>();
      const recentScored = await enrichAndProjectRows(env, brainId, recent.results);
      const avgDynamicConfidence = recentScored.length
        ? round3(recentScored.reduce((sum, m) => sum + toFiniteNumber(m.dynamic_confidence, 0.7), 0) / recentScored.length)
        : null;
      const avgDynamicImportance = recentScored.length
        ? round3(recentScored.reduce((sum, m) => sum + toFiniteNumber(m.dynamic_importance, 0.5), 0) / recentScored.length)
        : null;
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            total: total?.count ?? 0,
            archived: archived?.count ?? 0,
            by_type: byType.results,
            by_relation: relationStats.results,
            avg_recent_dynamic_confidence: avgDynamicConfidence,
            avg_recent_dynamic_importance: avgDynamicImportance,
            recent_5: recentScored,
          }, null, 2),
        }],
      };
    }

    case 'memory_link': {
      const { from_id, to_id, label, relation_type } = args as {
        from_id: unknown;
        to_id: unknown;
        label?: unknown;
        relation_type?: unknown;
      };
      if (typeof from_id !== 'string' || !from_id) return { content: [{ type: 'text', text: 'from_id must be a non-empty string.' }] };
      if (typeof to_id !== 'string' || !to_id) return { content: [{ type: 'text', text: 'to_id must be a non-empty string.' }] };
      if (from_id === to_id) return { content: [{ type: 'text', text: 'Cannot link a memory to itself.' }] };
      if (relation_type !== undefined && !isValidRelationType(relation_type)) return { content: [{ type: 'text', text: 'Invalid relation_type.' }] };
      const relationType = isValidRelationType(relation_type) ? relation_type : 'related';

      // Verify both memories exist
      const fromMem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL').bind(brainId, from_id).first();
      if (!fromMem) return { content: [{ type: 'text', text: `Memory not found: ${from_id}` }] };
      const toMem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL').bind(brainId, to_id).first();
      if (!toMem) return { content: [{ type: 'text', text: `Memory not found: ${to_id}` }] };

      // De-duplicate links (treating pair as undirected)
      const existing = await env.DB.prepare(
        'SELECT id FROM memory_links WHERE brain_id = ? AND ((from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?))'
      ).bind(brainId, from_id, to_id, to_id, from_id).first<{ id: string }>();

      const labelVal = typeof label === 'string' && label.trim() ? label.trim() : null;
      if (existing?.id) {
        await env.DB.prepare(
          'UPDATE memory_links SET relation_type = ?, label = ? WHERE brain_id = ? AND id = ?'
        ).bind(relationType, labelVal, brainId, existing.id).run();
        await logChangelog(env, brainId, 'memory_link_updated', 'memory_link', existing.id, 'Updated link relation', {
          from_id,
          to_id,
          relation_type: relationType,
        });
        return { content: [{ type: 'text', text: JSON.stringify({ link_id: existing.id, from_id, to_id, relation_type: relationType, label: labelVal, updated: true }) }] };
      }

      const link_id = generateId();
      await env.DB.prepare(
        'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
      ).bind(link_id, brainId, from_id, to_id, relationType, labelVal, now()).run();
      await logChangelog(env, brainId, 'memory_link_created', 'memory_link', link_id, 'Created memory link', {
        from_id,
        to_id,
        relation_type: relationType,
      });

      return { content: [{ type: 'text', text: JSON.stringify({ link_id, from_id, to_id, relation_type: relationType, label: labelVal }) }] };
    }

    case 'memory_unlink': {
      const { from_id, to_id, relation_type } = args as { from_id: unknown; to_id: unknown; relation_type?: unknown };
      if (typeof from_id !== 'string' || !from_id) return { content: [{ type: 'text', text: 'from_id must be a non-empty string.' }] };
      if (typeof to_id !== 'string' || !to_id) return { content: [{ type: 'text', text: 'to_id must be a non-empty string.' }] };
      if (relation_type !== undefined && !isValidRelationType(relation_type)) return { content: [{ type: 'text', text: 'Invalid relation_type.' }] };

      let sql = 'DELETE FROM memory_links WHERE brain_id = ? AND ((from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?))';
      const params: unknown[] = [brainId, from_id, to_id, to_id, from_id];
      if (relation_type) {
        sql += ' AND relation_type = ?';
        params.push(relation_type);
      }
      const result = await env.DB.prepare(sql).bind(...params).run();

      if (result.meta.changes === 0) return { content: [{ type: 'text', text: 'No link found between these memories.' }] };
      await logChangelog(env, brainId, 'memory_link_removed', 'memory_link', `${from_id}::${to_id}`, 'Removed memory link', {
        from_id,
        to_id,
        relation_type: relation_type ?? null,
      });
      return { content: [{ type: 'text', text: `Link removed between ${from_id} and ${to_id}.` }] };
    }

    case 'memory_links': {
      const { id } = args as { id: unknown };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };

      // Verify memory exists
      const mem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL').bind(brainId, id).first();
      if (!mem) return { content: [{ type: 'text', text: 'Memory not found.' }] };

      // Fetch links in both directions with full memory data
      const fromLinks = await env.DB.prepare(
        'SELECT ml.id as link_id, ml.label, ml.relation_type, ml.to_id as connected_id, m.* FROM memory_links ml JOIN memories m ON m.id = ml.to_id WHERE ml.brain_id = ? AND m.brain_id = ? AND ml.from_id = ? AND m.archived_at IS NULL'
      ).bind(brainId, brainId, id).all();

      const toLinks = await env.DB.prepare(
        'SELECT ml.id as link_id, ml.label, ml.relation_type, ml.from_id as connected_id, m.* FROM memory_links ml JOIN memories m ON m.id = ml.from_id WHERE ml.brain_id = ? AND m.brain_id = ? AND ml.to_id = ? AND m.archived_at IS NULL'
      ).bind(brainId, brainId, id).all();

      const tsNow = now();
      const linkStatsMap = await loadLinkStatsMap(env, brainId);
      const toScoredMemory = (r: Record<string, unknown>): Record<string, unknown> => {
        const base = {
          id: r.id,
          type: r.type,
          title: r.title,
          key: r.key,
          content: r.content,
          tags: r.tags,
          source: r.source,
          confidence: r.confidence,
          importance: r.importance,
          created_at: r.created_at,
          updated_at: r.updated_at,
        } as Record<string, unknown>;
        const scored = computeDynamicScores(base, linkStatsMap.get(String(r.id ?? '')), tsNow);
        return projectMemoryForClient({ ...base, ...scored });
      };

      const results = [
        ...fromLinks.results.map((r: Record<string, unknown>) => ({
          link_id: r.link_id,
          relation_type: r.relation_type,
          label: r.label,
          direction: 'from',
          memory: toScoredMemory(r),
        })),
        ...toLinks.results.map((r: Record<string, unknown>) => ({
          link_id: r.link_id,
          relation_type: r.relation_type,
          label: r.label,
          direction: 'to',
          memory: toScoredMemory(r),
        })),
      ];

      if (!results.length) return { content: [{ type: 'text', text: 'No links found for this memory.' }] };
      return { content: [{ type: 'text', text: JSON.stringify(results, null, 2) }] };
    }

    case 'memory_consolidate': {
      const { type, tag, older_than_days, limit: rawLimit } = args as {
        type?: unknown;
        tag?: unknown;
        older_than_days?: unknown;
        limit?: unknown;
      };
      if (type !== undefined && !isValidType(type)) return { content: [{ type: 'text', text: 'Invalid type filter.' }] };
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 300, 1), 1000);
      const params: unknown[] = [brainId];
      let query = 'SELECT id, type, title, key, content, tags, importance, created_at FROM memories WHERE brain_id = ? AND archived_at IS NULL';
      if (type) {
        query += ' AND type = ?';
        params.push(type);
      }
      if (typeof tag === 'string' && tag.trim()) {
        query += ' AND tags LIKE ?';
        params.push(`%${tag.trim()}%`);
      }
      if (older_than_days !== undefined) {
        const days = Number(older_than_days);
        if (!Number.isFinite(days) || days < 0) return { content: [{ type: 'text', text: 'older_than_days must be a non-negative number.' }] };
        query += ' AND created_at <= ?';
        params.push(now() - Math.floor(days * 86400));
      }
      query += ' ORDER BY created_at DESC LIMIT ?';
      params.push(limit);

      const rows = await env.DB.prepare(query).bind(...params).all<Record<string, unknown>>();
      const byFingerprint = new Map<string, Array<Record<string, unknown>>>();

      for (const row of rows.results) {
        const kind = String(row.type ?? '');
        const keyVal = typeof row.key === 'string' ? row.key.trim().toLowerCase() : '';
        const titleVal = typeof row.title === 'string' ? row.title.trim().toLowerCase() : '';
        const contentVal = typeof row.content === 'string'
          ? row.content.trim().toLowerCase().replace(/\s+/g, ' ').slice(0, 160)
          : '';
        const fingerprint = keyVal
          ? `${kind}|key|${keyVal}`
          : titleVal
            ? `${kind}|title|${titleVal}`
            : `${kind}|content|${contentVal}`;
        if (!contentVal && !titleVal && !keyVal) continue;
        const arr = byFingerprint.get(fingerprint);
        if (arr) arr.push(row);
        else byFingerprint.set(fingerprint, [row]);
      }

      const ts = now();
      const groups: Array<{ canonical_id: string; archived_ids: string[]; fingerprint: string }> = [];
      let archivedCount = 0;
      let linkedCount = 0;

      for (const [fingerprint, group] of byFingerprint) {
        if (group.length < 2) continue;
        const sorted = [...group].sort((a, b) => {
          const impA = clampToRange(a.importance, 0.5);
          const impB = clampToRange(b.importance, 0.5);
          if (impB !== impA) return impB - impA;
          const createdA = Number(a.created_at ?? 0);
          const createdB = Number(b.created_at ?? 0);
          return createdB - createdA;
        });
        const canonical = sorted[0];
        const canonicalId = String(canonical.id ?? '');
        if (!canonicalId) continue;

        const archivedIds: string[] = [];
        for (const dup of sorted.slice(1)) {
          const dupId = String(dup.id ?? '');
          if (!dupId) continue;
          archivedIds.push(dupId);
          await env.DB.prepare(
            'UPDATE memories SET archived_at = ?, updated_at = ? WHERE brain_id = ? AND id = ? AND archived_at IS NULL'
          ).bind(ts, ts, brainId, dupId).run();
          archivedCount++;

          const existingLink = await env.DB.prepare(
            'SELECT id FROM memory_links WHERE brain_id = ? AND ((from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?))'
          ).bind(brainId, canonicalId, dupId, dupId, canonicalId).first<{ id: string }>();
          if (existingLink?.id) {
            await env.DB.prepare(
              'UPDATE memory_links SET relation_type = ?, label = ? WHERE brain_id = ? AND id = ?'
            ).bind('supersedes', 'consolidated duplicate', brainId, existingLink.id).run();
          } else {
            await env.DB.prepare(
              'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
            ).bind(generateId(), brainId, canonicalId, dupId, 'supersedes', 'consolidated duplicate', ts).run();
          }
          linkedCount++;
        }

        if (archivedIds.length > 0) {
          groups.push({ canonical_id: canonicalId, archived_ids: archivedIds, fingerprint });
        }
      }

      if (groups.length > 0) {
        await logChangelog(env, brainId, 'memory_consolidated', 'memory', groups[0].canonical_id, 'Consolidated duplicate memories', {
          groups_consolidated: groups.length,
          archived_count: archivedCount,
        });
      }

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            scanned: rows.results.length,
            groups_consolidated: groups.length,
            archived_count: archivedCount,
            supersedes_links_written: linkedCount,
            groups,
          }, null, 2),
        }],
      };
    }

    case 'memory_forget': {
      const { id, mode: rawMode, tag, older_than_days, max_importance, limit: rawLimit } = args as {
        id?: unknown;
        mode?: unknown;
        tag?: unknown;
        older_than_days?: unknown;
        max_importance?: unknown;
        limit?: unknown;
      };
      const mode = rawMode === 'hard' ? 'hard' : 'soft';
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 25, 1), 200);

      if (typeof id === 'string' && id.trim()) {
        if (mode === 'hard') {
          const result = await env.DB.prepare('DELETE FROM memories WHERE brain_id = ? AND id = ?').bind(brainId, id).run();
          if (result.meta.changes === 0) return { content: [{ type: 'text', text: 'Memory not found.' }] };
          return { content: [{ type: 'text', text: JSON.stringify({ mode, deleted: 1, ids: [id] }) }] };
        }
        const ts = now();
        const result = await env.DB.prepare(
          'UPDATE memories SET archived_at = ?, updated_at = ? WHERE brain_id = ? AND id = ? AND archived_at IS NULL'
        ).bind(ts, ts, brainId, id).run();
        if (result.meta.changes === 0) return { content: [{ type: 'text', text: 'Memory not found or already archived.' }] };
        return { content: [{ type: 'text', text: JSON.stringify({ mode, archived: 1, ids: [id] }) }] };
      }

      const where: string[] = ['brain_id = ?', 'archived_at IS NULL'];
      const params: unknown[] = [brainId];
      if (typeof tag === 'string' && tag.trim()) {
        where.push('tags LIKE ?');
        params.push(`%${tag.trim()}%`);
      }
      if (older_than_days !== undefined) {
        const days = Number(older_than_days);
        if (!Number.isFinite(days) || days < 0) return { content: [{ type: 'text', text: 'older_than_days must be a non-negative number.' }] };
        where.push('created_at <= ?');
        params.push(now() - Math.floor(days * 86400));
      }
      if (max_importance !== undefined) {
        const maxImportance = clampToRange(max_importance, 0.5);
        where.push('importance <= ?');
        params.push(maxImportance);
      }
      if (where.length === 1) {
        return { content: [{ type: 'text', text: 'Batch forgetting requires at least one filter (tag, older_than_days, or max_importance).' }] };
      }

      const idsResult = await env.DB.prepare(
        `SELECT id FROM memories WHERE ${where.join(' AND ')} ORDER BY importance ASC, created_at ASC LIMIT ?`
      ).bind(...params, limit).all<{ id: string }>();
      const ids = idsResult.results.map((r) => r.id).filter(Boolean);
      if (!ids.length) return { content: [{ type: 'text', text: 'No memories matched forgetting policy.' }] };

      const placeholders = ids.map(() => '?').join(', ');
      if (mode === 'hard') {
        await env.DB.prepare(`DELETE FROM memories WHERE brain_id = ? AND id IN (${placeholders})`).bind(brainId, ...ids).run();
        await logChangelog(env, brainId, 'memory_forget_hard', 'memory', ids[0], 'Hard-forgot memories', { count: ids.length, ids });
        return { content: [{ type: 'text', text: JSON.stringify({ mode, deleted: ids.length, ids }, null, 2) }] };
      }

      const ts = now();
      await env.DB.prepare(
        `UPDATE memories SET archived_at = ?, updated_at = ? WHERE brain_id = ? AND id IN (${placeholders})`
      ).bind(ts, ts, brainId, ...ids).run();
      await logChangelog(env, brainId, 'memory_forget_soft', 'memory', ids[0], 'Soft-forgot memories', { count: ids.length, ids });
      return { content: [{ type: 'text', text: JSON.stringify({ mode, archived: ids.length, ids }, null, 2) }] };
    }

    case 'memory_activate': {
      const { seed_id, query, hops: rawHops, limit: rawLimit, include_inferred } = args as {
        seed_id?: unknown;
        query?: unknown;
        hops?: unknown;
        limit?: unknown;
        include_inferred?: unknown;
      };
      if (seed_id !== undefined && typeof seed_id !== 'string') return { content: [{ type: 'text', text: 'seed_id must be a string when provided.' }] };
      if (query !== undefined && typeof query !== 'string') return { content: [{ type: 'text', text: 'query must be a string when provided.' }] };
      const hops = Math.min(Math.max(Number.isInteger(rawHops) ? (rawHops as number) : 2, 1), 4);
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 20, 1), 100);
      const includeInferred = include_inferred === undefined ? true : Boolean(include_inferred);

      const memoriesResult = await env.DB.prepare(
        'SELECT id, type, title, key, content, tags, source, confidence, importance, created_at, updated_at FROM memories WHERE brain_id = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT 2000'
      ).bind(brainId).all<Record<string, unknown>>();
      const memories = memoriesResult.results;
      if (!memories.length) return { content: [{ type: 'text', text: 'No active memories found.' }] };

      const memoryMap = new Map<string, Record<string, unknown>>();
      for (const m of memories) {
        const id = typeof m.id === 'string' ? m.id : '';
        if (id) memoryMap.set(id, m);
      }

      const seedIds = new Set<string>();
      if (typeof seed_id === 'string' && seed_id.trim()) {
        if (!memoryMap.has(seed_id)) return { content: [{ type: 'text', text: `Seed memory not found: ${seed_id}` }] };
        seedIds.add(seed_id);
      }
      if (typeof query === 'string' && query.trim()) {
        const q = query.trim().toLowerCase();
        const scoredMatches = memories.map((m) => {
          const id = String(m.id ?? '');
          const title = String(m.title ?? '');
          const key = String(m.key ?? '');
          const content = String(m.content ?? '');
          const source = String(m.source ?? '');
          const tags = String(m.tags ?? '');
          const idLc = id.toLowerCase();
          const titleLc = title.toLowerCase();
          const keyLc = key.toLowerCase();
          const contentLc = content.toLowerCase();
          const sourceLc = source.toLowerCase();
          const tagsLc = tags.toLowerCase();

          let score = 0;
          if (idLc === q) score += 9;
          else if (idLc.startsWith(q)) score += 6;
          else if (idLc.includes(q)) score += 4;
          if (titleLc.includes(q)) score += 4.5;
          if (keyLc.includes(q)) score += 3.8;
          if (sourceLc.includes(q)) score += 2.4;
          if (tagsLc.includes(q)) score += 2.2;
          if (contentLc.includes(q)) score += 1.2;
          return { id, score };
        }).filter((m) => m.score > 0);

        scoredMatches.sort((a, b) => b.score - a.score);
        for (const match of scoredMatches.slice(0, 5)) seedIds.add(match.id);
      }
      if (!seedIds.size) return { content: [{ type: 'text', text: 'Provide seed_id or query that matches at least one memory.' }] };

      const linksResult = await env.DB.prepare(
        'SELECT from_id, to_id, relation_type FROM memory_links WHERE brain_id = ? LIMIT 12000'
      ).bind(brainId).all<Record<string, unknown>>();
      const edges: GraphEdge[] = [];
      for (const row of linksResult.results) {
        const from = typeof row.from_id === 'string' ? row.from_id : '';
        const to = typeof row.to_id === 'string' ? row.to_id : '';
        if (!from || !to || !memoryMap.has(from) || !memoryMap.has(to)) continue;
        edges.push({ from, to, relation_type: normalizeRelation(row.relation_type) });
      }
      const adjacency = buildAdjacencyFromEdges(edges);

      const tagToIds = new Map<string, string[]>();
      for (const memory of memories) {
        const id = String(memory.id ?? '');
        const tagsRaw = typeof memory.tags === 'string' ? memory.tags : '';
        if (!id || !tagsRaw) continue;
        for (const raw of tagsRaw.split(',')) {
          const tag = raw.trim().toLowerCase();
          if (!tag) continue;
          const ids = tagToIds.get(tag);
          if (ids) ids.push(id);
          else tagToIds.set(tag, [id]);
        }
      }

      const inferredNeighborsFor = (id: string): Array<{ id: string; weight: number; shared: number }> => {
        if (!includeInferred) return [];
        const memory = memoryMap.get(id);
        const tagsRaw = typeof memory?.tags === 'string' ? memory.tags : '';
        if (!tagsRaw) return [];
        const explicitNeighborIds = new Set((adjacency.get(id) ?? []).map((n) => n.id));
        const sharedCounts = new Map<string, number>();
        for (const raw of tagsRaw.split(',')) {
          const tag = raw.trim().toLowerCase();
          if (!tag) continue;
          const ids = tagToIds.get(tag) ?? [];
          for (const candidateId of ids) {
            if (candidateId === id || explicitNeighborIds.has(candidateId)) continue;
            sharedCounts.set(candidateId, (sharedCounts.get(candidateId) ?? 0) + 1);
          }
        }
        return Array.from(sharedCounts.entries())
          .map(([neighborId, shared]) => ({
            id: neighborId,
            shared,
            weight: Math.min(0.42, 0.16 + shared * 0.08),
          }))
          .filter((e) => e.shared >= 1)
          .sort((a, b) => b.weight - a.weight)
          .slice(0, 6);
      };

      const activation = new Map<string, number>();
      let frontier = new Map<string, number>();
      const contributions = new Map<string, Array<{ from_id: string; relation: string; delta: number }>>();
      for (const id of seedIds) {
        activation.set(id, 1);
        frontier.set(id, 1);
      }

      for (let hop = 1; hop <= hops; hop++) {
        const next = new Map<string, number>();
        for (const [sourceId, sourceSignal] of frontier) {
          const explicit = adjacency.get(sourceId) ?? [];
          for (const neighbor of explicit) {
            const delta = sourceSignal * relationSignalWeight(neighbor.relation_type) * Math.pow(0.78, hop - 1);
            if (Math.abs(delta) < 0.01) continue;
            next.set(neighbor.id, (next.get(neighbor.id) ?? 0) + delta);
            const arr = contributions.get(neighbor.id);
            const item = { from_id: sourceId, relation: neighbor.relation_type, delta: round3(delta) };
            if (arr) arr.push(item);
            else contributions.set(neighbor.id, [item]);
          }
          for (const neighbor of inferredNeighborsFor(sourceId)) {
            const delta = sourceSignal * neighbor.weight * Math.pow(0.72, hop - 1);
            if (Math.abs(delta) < 0.008) continue;
            next.set(neighbor.id, (next.get(neighbor.id) ?? 0) + delta);
            const arr = contributions.get(neighbor.id);
            const item = { from_id: sourceId, relation: `inferred(shared:${neighbor.shared})`, delta: round3(delta) };
            if (arr) arr.push(item);
            else contributions.set(neighbor.id, [item]);
          }
        }

        frontier = new Map<string, number>();
        for (const [id, signal] of next) {
          const damped = signal * 0.74;
          if (Math.abs(damped) < 0.006) continue;
          frontier.set(id, damped);
          activation.set(id, (activation.get(id) ?? 0) + damped);
        }
      }

      const scoredMemories = await enrichAndProjectRows(env, brainId, memories);
      const scoredMap = new Map<string, Record<string, unknown>>();
      for (const memory of scoredMemories) {
        const id = typeof memory.id === 'string' ? memory.id : '';
        if (id) scoredMap.set(id, memory);
      }

      const ranked = Array.from(activation.entries())
        .map(([id, act]) => {
          const memory = scoredMap.get(id);
          if (!memory) return null;
          const conf = toFiniteNumber(memory.confidence, 0.7);
          const imp = toFiniteNumber(memory.importance, 0.5);
          const seedBonus = seedIds.has(id) ? 0.45 : 0;
          const neuralScore = round3(act + imp * 0.45 + conf * 0.2 + seedBonus);
          const contribs = (contributions.get(id) ?? [])
            .sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta))
            .slice(0, 3);
          return {
            id,
            type: memory.type,
            title: memory.title,
            key: memory.key,
            confidence: memory.confidence,
            importance: memory.importance,
            activation: round3(act),
            neural_score: neuralScore,
            top_signals: contribs,
          };
        })
        .filter((r): r is NonNullable<typeof r> => Boolean(r))
        .sort((a, b) => b.neural_score - a.neural_score)
        .slice(0, limit);

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            seeds: Array.from(seedIds),
            hops,
            include_inferred: includeInferred,
            results: ranked,
          }, null, 2),
        }],
      };
    }

    case 'memory_reinforce': {
      const { id, delta_confidence, delta_importance, spread, hops } = args as {
        id: unknown;
        delta_confidence?: unknown;
        delta_importance?: unknown;
        spread?: unknown;
        hops?: unknown;
      };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };
      const deltaConf = clampToRange(delta_confidence, 0.04, -0.5, 0.5);
      const deltaImp = clampToRange(delta_importance, 0.06, -0.5, 0.5);
      const spreadFactor = clampToRange(spread, 0.35);
      const spreadHops = Math.min(Math.max(Number.isInteger(hops) ? (hops as number) : 1, 0), 3);

      const memoriesResult = await env.DB.prepare(
        'SELECT id, confidence, importance FROM memories WHERE brain_id = ? AND archived_at IS NULL'
      ).bind(brainId).all<Record<string, unknown>>();
      const memoryMap = new Map<string, { confidence: number; importance: number }>();
      for (const row of memoriesResult.results) {
        const memoryId = typeof row.id === 'string' ? row.id : '';
        if (!memoryId) continue;
        memoryMap.set(memoryId, {
          confidence: clamp01(toFiniteNumber(row.confidence, 0.7)),
          importance: clamp01(toFiniteNumber(row.importance, 0.5)),
        });
      }
      if (!memoryMap.has(id)) return { content: [{ type: 'text', text: `Memory not found: ${id}` }] };

      const linksResult = await env.DB.prepare(
        'SELECT from_id, to_id, relation_type FROM memory_links WHERE brain_id = ? LIMIT 12000'
      ).bind(brainId).all<Record<string, unknown>>();
      const edges: GraphEdge[] = [];
      for (const row of linksResult.results) {
        const from = typeof row.from_id === 'string' ? row.from_id : '';
        const to = typeof row.to_id === 'string' ? row.to_id : '';
        if (!from || !to || !memoryMap.has(from) || !memoryMap.has(to)) continue;
        edges.push({ from, to, relation_type: normalizeRelation(row.relation_type) });
      }
      const adjacency = buildAdjacencyFromEdges(edges);

      const updates = new Map<string, { delta_confidence: number; delta_importance: number; hops: number }>();
      updates.set(id, { delta_confidence: deltaConf, delta_importance: deltaImp, hops: 0 });

      let frontier = new Map<string, number>([[id, 1]]);
      for (let depth = 1; depth <= spreadHops; depth++) {
        const next = new Map<string, number>();
        for (const [sourceId, sourceEnergy] of frontier) {
          const neighbors = adjacency.get(sourceId) ?? [];
          for (const neighbor of neighbors) {
            const signal = sourceEnergy * relationSpreadWeight(neighbor.relation_type);
            if (Math.abs(signal) < 0.04) continue;
            next.set(neighbor.id, (next.get(neighbor.id) ?? 0) + signal);
          }
        }
        frontier = new Map<string, number>();
        for (const [targetId, signal] of next) {
          const dampedSignal = signal * Math.pow(0.62, depth - 1);
          if (Math.abs(dampedSignal) < 0.04) continue;
          frontier.set(targetId, dampedSignal);
          if (targetId === id) continue;
          const prev = updates.get(targetId) ?? { delta_confidence: 0, delta_importance: 0, hops: depth };
          prev.delta_confidence += deltaConf * spreadFactor * dampedSignal;
          prev.delta_importance += deltaImp * spreadFactor * dampedSignal;
          prev.hops = Math.min(prev.hops, depth);
          updates.set(targetId, prev);
        }
      }

      const rankedUpdateIds = Array.from(updates.entries())
        .sort((a, b) => {
          const absA = Math.abs(a[1].delta_confidence) + Math.abs(a[1].delta_importance);
          const absB = Math.abs(b[1].delta_confidence) + Math.abs(b[1].delta_importance);
          return absB - absA;
        })
        .slice(0, 300)
        .map(([memoryId]) => memoryId);

      const ts = now();
      const changedIds: string[] = [];
      const changeSummary: Array<Record<string, unknown>> = [];
      for (const memoryId of rankedUpdateIds) {
        const current = memoryMap.get(memoryId);
        const update = updates.get(memoryId);
        if (!current || !update) continue;
        const newConfidence = round3(clamp01(current.confidence + update.delta_confidence));
        const newImportance = round3(clamp01(current.importance + update.delta_importance));
        if (newConfidence === current.confidence && newImportance === current.importance) continue;
        await env.DB.prepare(
          'UPDATE memories SET confidence = ?, importance = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
        ).bind(newConfidence, newImportance, ts, brainId, memoryId).run();
        changedIds.push(memoryId);
        changeSummary.push({
          id: memoryId,
          hops: update.hops,
          confidence_before: round3(current.confidence),
          confidence_after: newConfidence,
          importance_before: round3(current.importance),
          importance_after: newImportance,
        });
      }

      const scoredChanged = changedIds.length
        ? await enrichAndProjectRows(
          env,
          brainId,
          (await env.DB.prepare(
            `SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance FROM memories WHERE brain_id = ? AND id IN (${changedIds.map(() => '?').join(',')})`
          ).bind(brainId, ...changedIds).all<Record<string, unknown>>()).results
        )
        : [];

      if (changedIds.length > 0) {
        await logChangelog(env, brainId, 'memory_reinforced', 'memory', id, 'Reinforced memory graph', {
          updated_count: changedIds.length,
          spread_hops: spreadHops,
          spread: spreadFactor,
        });
      }

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            seed_id: id,
            spread_hops: spreadHops,
            spread: spreadFactor,
            updated_count: changedIds.length,
            updates: changeSummary.slice(0, 25),
            updated_memories: scoredChanged.slice(0, 25),
          }, null, 2),
        }],
      };
    }

    case 'memory_decay': {
      const { older_than_days, max_link_count, decay_confidence, decay_importance, limit: rawLimit } = args as {
        older_than_days?: unknown;
        max_link_count?: unknown;
        decay_confidence?: unknown;
        decay_importance?: unknown;
        limit?: unknown;
      };
      const olderThanDays = Math.max(0, Number.isFinite(Number(older_than_days)) ? Number(older_than_days) : 30);
      const maxLinkCount = Math.max(0, Number.isFinite(Number(max_link_count)) ? Math.floor(Number(max_link_count)) : 1);
      const decayConf = Math.min(Math.max(Number.isFinite(Number(decay_confidence)) ? Number(decay_confidence) : 0.01, 0), 0.5);
      const decayImp = Math.min(Math.max(Number.isFinite(Number(decay_importance)) ? Number(decay_importance) : 0.03, 0), 0.5);
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 200, 1), 1000);
      const cutoffTs = now() - Math.floor(olderThanDays * 86400);

      const candidates = await env.DB.prepare(
        `SELECT
          m.id,
          m.confidence,
          m.importance,
          m.updated_at,
          (SELECT COUNT(*) FROM memory_links ml WHERE ml.brain_id = ? AND (ml.from_id = m.id OR ml.to_id = m.id)) AS link_count
        FROM memories m
        WHERE m.brain_id = ?
          AND m.archived_at IS NULL
          AND m.updated_at <= ?
          AND (SELECT COUNT(*) FROM memory_links ml2 WHERE ml2.brain_id = ? AND (ml2.from_id = m.id OR ml2.to_id = m.id)) <= ?
        ORDER BY m.updated_at ASC
        LIMIT ?`
      ).bind(brainId, brainId, cutoffTs, brainId, maxLinkCount, limit).all<Record<string, unknown>>();

      const ts = now();
      const decayedIds: string[] = [];
      const updates: Array<Record<string, unknown>> = [];
      for (const row of candidates.results) {
        const memoryId = typeof row.id === 'string' ? row.id : '';
        if (!memoryId) continue;
        const currentConf = clamp01(toFiniteNumber(row.confidence, 0.7));
        const currentImp = clamp01(toFiniteNumber(row.importance, 0.5));
        const newConf = round3(clamp01(currentConf - decayConf));
        const newImp = round3(clamp01(currentImp - decayImp));
        if (newConf === currentConf && newImp === currentImp) continue;
        await env.DB.prepare(
          'UPDATE memories SET confidence = ?, importance = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
        ).bind(newConf, newImp, ts, brainId, memoryId).run();
        decayedIds.push(memoryId);
        updates.push({
          id: memoryId,
          link_count: toFiniteNumber(row.link_count, 0),
          confidence_before: round3(currentConf),
          confidence_after: newConf,
          importance_before: round3(currentImp),
          importance_after: newImp,
        });
      }

      if (decayedIds.length > 0) {
        await logChangelog(env, brainId, 'memory_decayed', 'memory', decayedIds[0], 'Applied memory decay', {
          decayed_count: decayedIds.length,
          older_than_days: olderThanDays,
          max_link_count: maxLinkCount,
        });
      }

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            older_than_days: olderThanDays,
            max_link_count: maxLinkCount,
            decay_confidence: decayConf,
            decay_importance: decayImp,
            candidate_count: candidates.results.length,
            decayed_count: decayedIds.length,
            updates: updates.slice(0, 50),
          }, null, 2),
        }],
      };
    }

    case 'memory_changelog': {
      const { limit: rawLimit, since, event_type, entity_id } = args as {
        limit?: unknown;
        since?: unknown;
        event_type?: unknown;
        entity_id?: unknown;
      };
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 25, 1), 200);
      const where: string[] = ['brain_id = ?'];
      const params: unknown[] = [brainId];
      if (since !== undefined) {
        const sinceVal = Number(since);
        if (!Number.isFinite(sinceVal) || sinceVal < 0) return { content: [{ type: 'text', text: 'since must be a non-negative unix timestamp.' }] };
        where.push('created_at >= ?');
        params.push(Math.floor(sinceVal));
      }
      if (typeof event_type === 'string' && event_type.trim()) {
        where.push('event_type = ?');
        params.push(event_type.trim());
      }
      if (typeof entity_id === 'string' && entity_id.trim()) {
        where.push('entity_id = ?');
        params.push(entity_id.trim());
      }
      const rows = await env.DB.prepare(
        `SELECT id, event_type, entity_type, entity_id, summary, payload, created_at
         FROM memory_changelog
         WHERE ${where.join(' AND ')}
         ORDER BY created_at DESC
         LIMIT ?`
      ).bind(...params, limit).all<Record<string, unknown>>();

      const entries = rows.results.map((row) => {
        let parsedPayload: unknown = row.payload;
        if (typeof row.payload === 'string' && row.payload) {
          try {
            parsedPayload = JSON.parse(row.payload);
          } catch {
            parsedPayload = row.payload;
          }
        }
        return {
          id: row.id,
          event_type: row.event_type,
          entity_type: row.entity_type,
          entity_id: row.entity_id,
          summary: row.summary,
          payload: parsedPayload,
          created_at: row.created_at,
        };
      });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            server_version: SERVER_VERSION,
            count: entries.length,
            entries,
          }, null, 2),
        }],
      };
    }

    case 'memory_conflicts': {
      const { min_confidence, limit: rawLimit } = args as { min_confidence?: unknown; limit?: unknown };
      const minConfidence = clampToRange(min_confidence, 0.7);
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 40, 1), 200);

      const factsResult = await env.DB.prepare(
        'SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance FROM memories WHERE brain_id = ? AND archived_at IS NULL AND type = ? LIMIT 3000'
      ).bind(brainId, 'fact').all<Record<string, unknown>>();
      const scoredFacts = await enrichAndProjectRows(env, brainId, factsResult.results);
      const factMap = new Map<string, Record<string, unknown>>();
      for (const fact of scoredFacts) {
        const id = typeof fact.id === 'string' ? fact.id : '';
        if (id) factMap.set(id, fact);
      }

      const conflicts: Array<Record<string, unknown>> = [];
      const seenPairs = new Set<string>();
      const normalizedContent = (v: unknown) => String(v ?? '').trim().toLowerCase().replace(/\s+/g, ' ');
      const pairKey = (a: string, b: string) => (a < b ? `${a}|${b}` : `${b}|${a}`);

      // Explicit contradiction edges between fact memories.
      const contradictionLinks = await env.DB.prepare(
        `SELECT ml.id as link_id, ml.label, ml.from_id, ml.to_id
         FROM memory_links ml
         JOIN memories m1 ON m1.id = ml.from_id AND m1.brain_id = ? AND m1.type = 'fact' AND m1.archived_at IS NULL
         JOIN memories m2 ON m2.id = ml.to_id AND m2.brain_id = ? AND m2.type = 'fact' AND m2.archived_at IS NULL
         WHERE ml.brain_id = ?
           AND ml.relation_type = 'contradicts'
         LIMIT 2000`
      ).bind(brainId, brainId, brainId).all<Record<string, unknown>>();
      for (const row of contradictionLinks.results) {
        const aId = String(row.from_id ?? '');
        const bId = String(row.to_id ?? '');
        const a = factMap.get(aId);
        const b = factMap.get(bId);
        if (!a || !b) continue;
        const confA = toFiniteNumber(a.confidence, 0.7);
        const confB = toFiniteNumber(b.confidence, 0.7);
        if (confA < minConfidence || confB < minConfidence) continue;
        const key = pairKey(aId, bId);
        if (seenPairs.has(key)) continue;
        seenPairs.add(key);
        conflicts.push({
          conflict_type: 'explicit_contradiction_link',
          confidence_pair: [round3(confA), round3(confB)],
          link_id: row.link_id,
          link_label: row.label,
          a: { id: aId, key: a.key, title: a.title, content: a.content, confidence: a.confidence, importance: a.importance },
          b: { id: bId, key: b.key, title: b.title, content: b.content, confidence: b.confidence, importance: b.importance },
        });
      }

      // Key-based fact conflicts: same key with materially different values.
      const byKey = new Map<string, Array<Record<string, unknown>>>();
      for (const fact of scoredFacts) {
        const keyRaw = typeof fact.key === 'string' ? fact.key.trim().toLowerCase() : '';
        if (!keyRaw) continue;
        const arr = byKey.get(keyRaw);
        if (arr) arr.push(fact);
        else byKey.set(keyRaw, [fact]);
      }
      for (const [keyName, facts] of byKey) {
        if (facts.length < 2) continue;
        const sorted = [...facts].sort((a, b) => toFiniteNumber(b.confidence, 0) - toFiniteNumber(a.confidence, 0));
        for (let i = 0; i < sorted.length; i++) {
          for (let j = i + 1; j < sorted.length; j++) {
            const a = sorted[i];
            const b = sorted[j];
            const aId = String(a.id ?? '');
            const bId = String(b.id ?? '');
            if (!aId || !bId) continue;
            const confA = toFiniteNumber(a.confidence, 0.7);
            const confB = toFiniteNumber(b.confidence, 0.7);
            if (confA < minConfidence || confB < minConfidence) continue;
            const contentA = normalizedContent(a.content);
            const contentB = normalizedContent(b.content);
            if (!contentA || !contentB || contentA === contentB) continue;
            const key = pairKey(aId, bId);
            if (seenPairs.has(key)) continue;
            seenPairs.add(key);
            conflicts.push({
              conflict_type: 'fact_key_value_conflict',
              fact_key: keyName,
              confidence_pair: [round3(confA), round3(confB)],
              a: { id: aId, content: a.content, confidence: a.confidence, importance: a.importance, updated_at: a.updated_at },
              b: { id: bId, content: b.content, confidence: b.confidence, importance: b.importance, updated_at: b.updated_at },
            });
            if (conflicts.length >= limit) break;
          }
          if (conflicts.length >= limit) break;
        }
        if (conflicts.length >= limit) break;
      }

      conflicts.sort((a, b) => {
        const aPair = Array.isArray(a.confidence_pair) ? a.confidence_pair : [0, 0];
        const bPair = Array.isArray(b.confidence_pair) ? b.confidence_pair : [0, 0];
        const aScore = toFiniteNumber(aPair[0], 0) + toFiniteNumber(aPair[1], 0);
        const bScore = toFiniteNumber(bPair[0], 0) + toFiniteNumber(bPair[1], 0);
        return bScore - aScore;
      });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            min_confidence: minConfidence,
            total_conflicts: conflicts.length,
            conflicts: conflicts.slice(0, limit),
          }, null, 2),
        }],
      };
    }

    case 'objective_set': {
      const { id: rawId, title, content, kind: rawKind, horizon: rawHorizon, status: rawStatus, priority, tags } = args as {
        id?: unknown;
        title: unknown;
        content?: unknown;
        kind?: unknown;
        horizon?: unknown;
        status?: unknown;
        priority?: unknown;
        tags?: unknown;
      };
      if (typeof title !== 'string' || !title.trim()) return { content: [{ type: 'text', text: 'title must be a non-empty string.' }] };
      if (content !== undefined && typeof content !== 'string') return { content: [{ type: 'text', text: 'content must be a string when provided.' }] };
      if (tags !== undefined && typeof tags !== 'string') return { content: [{ type: 'text', text: 'tags must be a comma-separated string when provided.' }] };
      const kind = rawKind === 'curiosity' ? 'curiosity' : 'goal';
      const horizon = rawHorizon === 'short' || rawHorizon === 'medium' || rawHorizon === 'long' ? rawHorizon : 'long';
      const status = rawStatus === 'paused' || rawStatus === 'done' ? rawStatus : 'active';
      const priorityVal = clampToRange(priority, kind === 'goal' ? 0.82 : 0.74);

      const rootId = await ensureObjectiveRoot(env, brainId);
      const ts = now();
      const extraTags = typeof tags === 'string'
        ? tags.split(',').map((t) => t.trim()).filter(Boolean)
        : [];
      const objectiveTags = Array.from(new Set([
        'objective_node',
        'autonomous_objective',
        `kind_${kind}`,
        `horizon_${horizon}`,
        `status_${status}`,
        ...extraTags,
      ])).join(',');
      const objectiveContent = typeof content === 'string' && content.trim()
        ? content.trim()
        : (kind === 'goal'
          ? `Long-term goal: ${title.trim()}`
          : `Curiosity to explore: ${title.trim()}`);

      let objectiveId = typeof rawId === 'string' && rawId.trim() ? rawId.trim() : '';
      if (objectiveId) {
        const exists = await env.DB.prepare(
          'SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL'
        ).bind(brainId, objectiveId).first<{ id: string }>();
        if (!exists?.id) return { content: [{ type: 'text', text: `Objective memory not found: ${objectiveId}` }] };
        await env.DB.prepare(
          'UPDATE memories SET type = ?, title = ?, content = ?, tags = ?, source = ?, importance = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
        ).bind('note', title.trim(), objectiveContent, objectiveTags, 'autonomous_objective', priorityVal, ts, brainId, objectiveId).run();
      } else {
        const key = `objective:${kind}:${slugify(title.trim())}`;
        const existing = await env.DB.prepare(
          'SELECT id FROM memories WHERE brain_id = ? AND key = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT 1'
        ).bind(brainId, key).first<{ id: string }>();
        if (existing?.id) {
          objectiveId = existing.id;
          await env.DB.prepare(
            'UPDATE memories SET title = ?, content = ?, tags = ?, source = ?, importance = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
          ).bind(title.trim(), objectiveContent, objectiveTags, 'autonomous_objective', priorityVal, ts, brainId, objectiveId).run();
        } else {
          objectiveId = generateId();
          await env.DB.prepare(
            'INSERT INTO memories (id, brain_id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)'
          ).bind(
            objectiveId,
            brainId,
            'note',
            title.trim(),
            key,
            objectiveContent,
            objectiveTags,
            'autonomous_objective',
            kind === 'goal' ? 0.84 : 0.72,
            priorityVal,
            ts,
            ts
          ).run();
        }
      }

      const linkRelation: RelationType = kind === 'goal' ? 'supports' : 'example_of';
      const linkLabel = kind === 'goal'
        ? `objective (${horizon})`
        : `curiosity (${horizon})`;
      const existingLink = await env.DB.prepare(
        'SELECT id FROM memory_links WHERE brain_id = ? AND ((from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?)) LIMIT 1'
      ).bind(brainId, rootId, objectiveId, objectiveId, rootId).first<{ id: string }>();
      if (existingLink?.id) {
        await env.DB.prepare(
          'UPDATE memory_links SET relation_type = ?, label = ? WHERE brain_id = ? AND id = ?'
        ).bind(linkRelation, linkLabel, brainId, existingLink.id).run();
      } else {
        await env.DB.prepare(
          'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
        ).bind(generateId(), brainId, rootId, objectiveId, linkRelation, linkLabel, ts).run();
      }

      const objectiveRow = await env.DB.prepare(
        'SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance FROM memories WHERE brain_id = ? AND id = ? LIMIT 1'
      ).bind(brainId, objectiveId).first<Record<string, unknown>>();
      const [objectiveMemory] = objectiveRow ? await enrichAndProjectRows(env, brainId, [objectiveRow]) : [];
      await logChangelog(env, brainId, 'objective_upserted', 'memory', objectiveId, 'Upserted autonomous objective node', {
        kind,
        horizon,
        status,
        root_id: rootId,
      });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            root_objective_id: rootId,
            objective_id: objectiveId,
            kind,
            horizon,
            status,
            objective: objectiveMemory ?? objectiveRow,
          }, null, 2),
        }],
      };
    }

    case 'objective_list': {
      const { kind: rawKind, status: rawStatus, limit: rawLimit } = args as {
        kind?: unknown;
        status?: unknown;
        limit?: unknown;
      };
      const kind = rawKind === 'goal' || rawKind === 'curiosity' ? rawKind : null;
      const status = rawStatus === 'active' || rawStatus === 'paused' || rawStatus === 'done' ? rawStatus : null;
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 50, 1), 200);

      let query = 'SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance FROM memories WHERE brain_id = ? AND archived_at IS NULL AND tags LIKE ?';
      const params: unknown[] = [brainId, '%objective_node%'];
      if (kind) {
        query += ' AND tags LIKE ?';
        params.push(`%kind_${kind}%`);
      }
      if (status) {
        query += ' AND tags LIKE ?';
        params.push(`%status_${status}%`);
      }
      query += ' ORDER BY created_at DESC LIMIT ?';
      params.push(limit);

      const rows = await env.DB.prepare(query).bind(...params).all<Record<string, unknown>>();
      const objectives = await enrichAndProjectRows(env, brainId, rows.results);
      const root = await env.DB.prepare(
        'SELECT id FROM memories WHERE brain_id = ? AND key = ? AND archived_at IS NULL LIMIT 1'
      ).bind(brainId, 'autonomous_objectives_root').first<{ id: string }>();

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            root_objective_id: root?.id ?? null,
            count: objectives.length,
            objectives,
          }, null, 2),
        }],
      };
    }

    default:
      throw Object.assign(new Error(`Unknown tool: ${name}`), { code: -32601 });
  }
}

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept',
};

function corsJsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

async function readJsonBody(request: Request): Promise<Record<string, unknown> | null> {
  try {
    const body = await request.json();
    if (!body || typeof body !== 'object' || Array.isArray(body)) return null;
    return body as Record<string, unknown>;
  } catch {
    return null;
  }
}

async function processMcpBody(
  body: { jsonrpc: string; id?: unknown; method: string; params?: Record<string, unknown> },
  env: Env,
  authCtx: AuthContext
): Promise<unknown> {
  const { id, method, params = {} } = body;

  if (method === 'initialize') {
    return {
      jsonrpc: '2.0', id,
      result: {
        protocolVersion: '2024-11-05',
        capabilities: { tools: {} },
        serverInfo: { name: SERVER_NAME, version: SERVER_VERSION },
      },
    };
  }

  if (method === 'tools/list') {
    return { jsonrpc: '2.0', id, result: { tools: TOOLS } };
  }

  if (method === 'tools/call') {
    const { name, arguments: toolArgs = {} } = params as { name: string; arguments?: ToolArgs };
    const result = await callTool(name, toolArgs, env, authCtx.brainId);
    return { jsonrpc: '2.0', id, result };
  }

  if (method === 'notifications/initialized' || method.startsWith('notifications/')) {
    return null; // notifications get no response
  }

  return {
    jsonrpc: '2.0', id,
    error: { code: -32601, message: `Method not found: ${method}` },
  };
}

async function handleMcp(request: Request, env: Env, url: URL, authCtx: AuthContext): Promise<Response> {
  const acceptsSse = (request.headers.get('Accept') ?? '').includes('text/event-stream');

  // SSE transport: GET /mcp opens the event stream
  if (request.method === 'GET' && acceptsSse) {
    const postUrl = `${url.origin}/mcp`;
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const enc = new TextEncoder();

    // Send the endpoint event immediately then keep-alive
    (async () => {
      // endpoint event tells client where to POST messages
      await writer.write(enc.encode(`event: endpoint\ndata: ${postUrl}\n\n`));
      // Keep the connection alive with periodic pings
      const interval = setInterval(async () => {
        try {
          await writer.write(enc.encode(': ping\n\n'));
        } catch {
          clearInterval(interval);
        }
      }, 15000);
    })();

    return new Response(readable, {
      headers: {
        ...CORS_HEADERS,
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
      },
    });
  }

  // SSE transport: POST sends a message and returns SSE response
  if (request.method === 'POST') {
    let body: { jsonrpc: string; id?: unknown; method: string; params?: Record<string, unknown> };
    try {
      body = await request.json();
    } catch {
      return jsonResponse({ jsonrpc: '2.0', error: { code: -32700, message: 'Parse error' }, id: null });
    }

    let responseObj: unknown;
    try {
      responseObj = await processMcpBody(body, env, authCtx);
    } catch (err) {
      const code = (err instanceof Error && 'code' in err && typeof (err as { code?: unknown }).code === 'number')
        ? (err as { code: number }).code
        : -32603;
      const message = err instanceof Error ? err.message : 'Internal error';
      responseObj = { jsonrpc: '2.0', id: body.id, error: { code, message } };
    }

    // If client accepts SSE, stream the response as an SSE event
    if (acceptsSse || (request.headers.get('Accept') ?? '').includes('text/event-stream')) {
      if (responseObj === null) {
        return new Response(null, { status: 204, headers: CORS_HEADERS });
      }
      const sseBody = `event: message\ndata: ${JSON.stringify(responseObj)}\n\n`;
      return new Response(sseBody, {
        headers: {
          ...CORS_HEADERS,
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
        },
      });
    }

    // Plain HTTP JSON response (for standard MCP HTTP transport)
    if (responseObj === null) {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }
    return new Response(JSON.stringify(responseObj), {
      headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    });
  }

  return jsonResponse({ error: 'Method not allowed' }, 405);
}

async function handleApiMemories(request: Request, env: Env, brainId: string): Promise<Response> {
  const url = new URL(request.url);
  const type = url.searchParams.get('type') ?? '';
  const search = url.searchParams.get('search') ?? '';
  const limitParam = parseInt(url.searchParams.get('limit') ?? '100', 10);
  const limit = Math.min(Math.max(Number.isNaN(limitParam) ? 100 : limitParam, 1), 500);

  let query = 'SELECT m.*, (SELECT COUNT(*) FROM memory_links ml WHERE ml.brain_id = ? AND (ml.from_id = m.id OR ml.to_id = m.id)) as link_count FROM memories m WHERE m.brain_id = ? AND m.archived_at IS NULL';
  const params: unknown[] = [brainId, brainId];
  if (type && VALID_TYPES.includes(type as MemoryType)) {
    query += ' AND type = ?'; params.push(type);
  }
  if (search) {
    const like = `%${search}%`;
    query += ' AND (m.id LIKE ? OR m.content LIKE ? OR m.title LIKE ? OR m.key LIKE ? OR m.source LIKE ?)';
    params.push(like, like, like, like, like);
  }
  query += ' ORDER BY m.created_at DESC LIMIT ?';
  params.push(limit);

  const results = await env.DB.prepare(query).bind(...params).all();
  const tsNow = now();
  const linkStatsMap = await loadLinkStatsMap(env, brainId);
  const enrichedMemories = enrichMemoryRowsWithDynamics(
    results.results as Array<Record<string, unknown>>,
    linkStatsMap,
    tsNow
  );
  const projectedMemories = enrichedMemories.map(projectMemoryForClient);
  const sortedMemories = [...projectedMemories].sort(
    (a, b) => toFiniteNumber(b.created_at, 0) - toFiniteNumber(a.created_at, 0)
  );
  const stats = await env.DB.prepare('SELECT type, COUNT(*) as count FROM memories WHERE brain_id = ? AND archived_at IS NULL GROUP BY type').bind(brainId).all();
  const archived = await env.DB.prepare('SELECT COUNT(*) as count FROM memories WHERE brain_id = ? AND archived_at IS NOT NULL').bind(brainId).first<{ count: number }>();
  return new Response(JSON.stringify({ memories: sortedMemories, stats: stats.results, archived_count: archived?.count ?? 0 }), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

async function handleApiLinks(memoryId: string, env: Env, brainId: string): Promise<Response> {
  const mem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL').bind(brainId, memoryId).first();
  if (!mem) return new Response(JSON.stringify({ error: 'Memory not found.' }), {
    status: 404, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });

  const fromLinks = await env.DB.prepare(
    'SELECT ml.id as link_id, ml.relation_type, ml.label, m.id, m.type, m.title, m.key, m.content, m.tags, m.source, m.confidence, m.importance, m.created_at, m.updated_at FROM memory_links ml JOIN memories m ON m.id = ml.to_id WHERE ml.brain_id = ? AND m.brain_id = ? AND ml.from_id = ? AND m.archived_at IS NULL'
  ).bind(brainId, brainId, memoryId).all();

  const toLinks = await env.DB.prepare(
    'SELECT ml.id as link_id, ml.relation_type, ml.label, m.id, m.type, m.title, m.key, m.content, m.tags, m.source, m.confidence, m.importance, m.created_at, m.updated_at FROM memory_links ml JOIN memories m ON m.id = ml.from_id WHERE ml.brain_id = ? AND m.brain_id = ? AND ml.to_id = ? AND m.archived_at IS NULL'
  ).bind(brainId, brainId, memoryId).all();

  const tsNow = now();
  const linkStatsMap = await loadLinkStatsMap(env, brainId);
  const toScoredMemory = (r: Record<string, unknown>): Record<string, unknown> => {
    const base = {
      id: r.id,
      type: r.type,
      title: r.title,
      key: r.key,
      content: r.content,
      tags: r.tags,
      source: r.source,
      confidence: r.confidence,
      importance: r.importance,
      created_at: r.created_at,
      updated_at: r.updated_at,
    } as Record<string, unknown>;
    return projectMemoryForClient({
      ...base,
      ...computeDynamicScores(base, linkStatsMap.get(String(r.id ?? '')), tsNow),
    });
  };

  const results = [
    ...fromLinks.results.map((r: Record<string, unknown>) => ({
      link_id: r.link_id,
      relation_type: r.relation_type,
      label: r.label,
      direction: 'from',
      memory: toScoredMemory(r),
    })),
    ...toLinks.results.map((r: Record<string, unknown>) => ({
      link_id: r.link_id,
      relation_type: r.relation_type,
      label: r.label,
      direction: 'to',
      memory: toScoredMemory(r),
    })),
  ];

  return new Response(JSON.stringify(results), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

async function handleApiGraph(env: Env, brainId: string): Promise<Response> {
  const memories = await env.DB.prepare(
    'SELECT id, type, title, key, content, tags, source, confidence, importance FROM memories WHERE brain_id = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT 1000'
  ).bind(brainId).all();
  const links = await env.DB.prepare(
    'SELECT ml.id, ml.from_id, ml.to_id, ml.label, ml.relation_type FROM memory_links ml JOIN memories m1 ON m1.id = ml.from_id AND m1.brain_id = ? AND m1.archived_at IS NULL JOIN memories m2 ON m2.id = ml.to_id AND m2.brain_id = ? AND m2.archived_at IS NULL WHERE ml.brain_id = ? LIMIT 5000'
  ).bind(brainId, brainId, brainId).all();

  const tsNow = now();
  const linkStatsMap = await loadLinkStatsMap(env, brainId);
  const nodes = enrichMemoryRowsWithDynamics(
    memories.results as Array<Record<string, unknown>>,
    linkStatsMap,
    tsNow
  ).map(projectMemoryForClient);
  const explicitEdges = links.results as Array<Record<string, unknown>>;

  // Build inferred (non-persisted) graph edges from shared tags.
  // This helps visualization when explicit links are sparse.
  const tagToIds = new Map<string, string[]>();
  for (const n of nodes) {
    const id = typeof n.id === 'string' ? n.id : '';
    if (!id) continue;
    const tags = typeof n.tags === 'string' ? n.tags : '';
    if (!tags) continue;
    for (const rawTag of tags.split(',')) {
      const tag = rawTag.trim().toLowerCase();
      if (!tag) continue;
      const ids = tagToIds.get(tag);
      if (ids) ids.push(id);
      else tagToIds.set(tag, [id]);
    }
  }

  const inferredByPair = new Map<string, { from_id: string; to_id: string; tags: Set<string>; score: number }>();
  for (const [tag, idsRaw] of tagToIds) {
    const ids = Array.from(new Set(idsRaw));
    if (ids.length < 2) continue;
    // Guard against explosive pair counts for broad tags.
    const limited = ids.slice(0, 28);
    const tagWeight = 1 / Math.sqrt(limited.length);
    for (let i = 0; i < limited.length; i++) {
      for (let j = i + 1; j < limited.length; j++) {
        const a = limited[i];
        const b = limited[j];
        const from_id = a < b ? a : b;
        const to_id = a < b ? b : a;
        const key = `${from_id}|${to_id}`;
        const existing = inferredByPair.get(key);
        if (existing) {
          existing.tags.add(tag);
          existing.score += tagWeight;
        } else {
          inferredByPair.set(key, { from_id, to_id, tags: new Set([tag]), score: tagWeight });
        }
      }
    }
  }

  const explicitPairs = new Set(
    explicitEdges.map((e) => {
      const a = String(e.from_id ?? '');
      const b = String(e.to_id ?? '');
      return a < b ? `${a}|${b}` : `${b}|${a}`;
    })
  );

  const inferredCandidates = Array.from(inferredByPair.entries())
    .filter(([pair]) => !explicitPairs.has(pair))
    .map(([pair, v]) => {
      const tags = Array.from(v.tags).sort();
      const preview = tags.slice(0, 3);
      const suffix = tags.length > 3 ? ` +${tags.length - 3}` : '';
      const score = Number(v.score.toFixed(3));
      return {
        id: `inf-${pair.replace('|', '-')}`,
        from_id: v.from_id,
        to_id: v.to_id,
        label: `shared: ${preview.join(', ')}${suffix}`,
        tags,
        strength: tags.length,
        score,
        inferred: true,
      };
    })
    // Keep only meaningful suggestions from shared context.
    .filter((e) => e.strength >= 2 || e.score >= 0.85)
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return b.strength - a.strength;
    });

  // Greedy sparsification to prevent inferred hubs from collapsing the graph.
  const inferredEdges: Array<{
    id: string;
    from_id: string;
    to_id: string;
    label: string;
    tags: string[];
    strength: number;
    score: number;
    inferred: boolean;
  }> = [];
  const inferredDegreeByNode = new Map<string, number>();
  const inferredMax = 360;
  const inferredPerNodeCap = 7;
  for (const edge of inferredCandidates) {
    if (inferredEdges.length >= inferredMax) break;
    const fromDeg = inferredDegreeByNode.get(edge.from_id) ?? 0;
    const toDeg = inferredDegreeByNode.get(edge.to_id) ?? 0;
    if (fromDeg >= inferredPerNodeCap || toDeg >= inferredPerNodeCap) continue;
    inferredEdges.push(edge);
    inferredDegreeByNode.set(edge.from_id, fromDeg + 1);
    inferredDegreeByNode.set(edge.to_id, toDeg + 1);
  }

  return new Response(JSON.stringify({ nodes, edges: explicitEdges, inferred_edges: inferredEdges }), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

function handleApiTools(): Response {
  return new Response(JSON.stringify({
    server: { name: SERVER_NAME, version: SERVER_VERSION },
    tool_count: TOOLS.length,
    tool_names: TOOLS.map((t) => t.name),
    relation_types: RELATION_TYPES,
  }), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

type UserRow = {
  id: string;
  email: string;
  password_hash: string;
  display_name: string | null;
  created_at: number;
};

type BrainSummary = {
  id: string;
  name: string;
  slug: string | null;
  role: string;
  created_at: number;
  updated_at: number;
};

function sanitizeDisplayName(raw: unknown, email: string): string | null {
  if (typeof raw === 'string') {
    const trimmed = raw.trim();
    if (trimmed) return trimmed.slice(0, 120);
  }
  const local = email.split('@')[0]?.replace(/[^a-z0-9]+/gi, ' ').trim();
  return local ? local.slice(0, 120) : null;
}

function sanitizeBrainName(raw: unknown, email: string): string {
  if (typeof raw === 'string') {
    const trimmed = raw.trim();
    if (trimmed) return trimmed.slice(0, 120);
  }
  const local = email.split('@')[0]?.replace(/[^a-z0-9]+/gi, ' ').trim();
  return local ? `${local.slice(0, 64)}'s Second Brain` : 'Second Brain';
}

function userPayload(row: { id: string; email: string; display_name: string | null; created_at: number }): Record<string, unknown> {
  return {
    id: row.id,
    email: row.email,
    display_name: row.display_name,
    created_at: row.created_at,
  };
}

async function listBrainsForUser(userId: string, env: Env): Promise<BrainSummary[]> {
  const rows = await env.DB.prepare(
    `SELECT
      b.id,
      b.name,
      b.slug,
      b.created_at,
      b.updated_at,
      bm.role
     FROM brain_memberships bm
     JOIN brains b ON b.id = bm.brain_id
     WHERE bm.user_id = ?
     ORDER BY CASE WHEN bm.role = 'owner' THEN 0 ELSE 1 END ASC, bm.created_at ASC`
  ).bind(userId).all<BrainSummary>();
  return rows.results;
}

function findActiveBrain(brains: BrainSummary[], preferredBrainId: string): BrainSummary | null {
  if (!brains.length) return null;
  const explicit = brains.find((b) => b.id === preferredBrainId);
  if (explicit) return explicit;
  return brains[0];
}

async function handleAuthSignup(request: Request, env: Env): Promise<Response> {
  const body = await readJsonBody(request);
  if (!body) return corsJsonResponse({ error: 'Invalid JSON body.' }, 400);

  const emailRaw = body.email;
  const passwordRaw = body.password;
  if (typeof emailRaw !== 'string' || !isValidEmail(emailRaw)) {
    return corsJsonResponse({ error: 'A valid email is required.' }, 400);
  }
  if (typeof passwordRaw !== 'string' || !isStrongEnoughPassword(passwordRaw)) {
    return corsJsonResponse({ error: 'Password must be at least 10 characters.' }, 400);
  }

  const email = normalizeEmail(emailRaw);
  const existing = await env.DB.prepare(
    'SELECT id FROM users WHERE email = ? LIMIT 1'
  ).bind(email).first<{ id: string }>();
  if (existing?.id) return corsJsonResponse({ error: 'Email already registered.' }, 409);

  const ts = now();
  const userId = generateId();
  const passwordHash = await hashPassword(passwordRaw);
  const displayName = sanitizeDisplayName(body.display_name ?? body.name, email);
  await env.DB.prepare(
    'INSERT INTO users (id, email, password_hash, display_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)'
  ).bind(userId, email, passwordHash, displayName, ts, ts).run();

  const brainId = generateId();
  const brainName = sanitizeBrainName(body.brain_name, email);
  const slug = `${slugify(brainName)}-${brainId.slice(0, 8)}`;
  await env.DB.prepare(
    'INSERT INTO brains (id, name, slug, owner_user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)'
  ).bind(brainId, brainName, slug, userId, ts, ts).run();
  await env.DB.prepare(
    "INSERT INTO brain_memberships (brain_id, user_id, role, created_at) VALUES (?, ?, 'owner', ?)"
  ).bind(brainId, userId, ts).run();

  const tokens = await createSessionTokens(userId, brainId, env);
  const brains = await listBrainsForUser(userId, env);
  const activeBrain = findActiveBrain(brains, brainId);

  return corsJsonResponse({
    user: userPayload({ id: userId, email, display_name: displayName, created_at: ts }),
    active_brain: activeBrain,
    brains,
    ...tokens,
  }, 201);
}

async function handleAuthLogin(request: Request, env: Env): Promise<Response> {
  const body = await readJsonBody(request);
  if (!body) return corsJsonResponse({ error: 'Invalid JSON body.' }, 400);

  const emailRaw = body.email;
  const passwordRaw = body.password;
  const preferredBrainId = typeof body.brain_id === 'string' ? body.brain_id.trim() : '';
  if (typeof emailRaw !== 'string' || !isValidEmail(emailRaw)) {
    return corsJsonResponse({ error: 'A valid email is required.' }, 400);
  }
  if (typeof passwordRaw !== 'string' || passwordRaw.length === 0) {
    return corsJsonResponse({ error: 'Password is required.' }, 400);
  }

  const email = normalizeEmail(emailRaw);
  const user = await env.DB.prepare(
    'SELECT id, email, password_hash, display_name, created_at FROM users WHERE email = ? LIMIT 1'
  ).bind(email).first<UserRow>();
  if (!user || !(await verifyPassword(passwordRaw, user.password_hash))) {
    return corsJsonResponse({ error: 'Invalid email or password.' }, 401);
  }

  const brains = await listBrainsForUser(user.id, env);
  const activeBrain = findActiveBrain(brains, preferredBrainId);
  if (!activeBrain) return corsJsonResponse({ error: 'No brain membership found for user.' }, 403);
  if (preferredBrainId && activeBrain.id !== preferredBrainId) {
    return corsJsonResponse({ error: 'Requested brain is not available for this user.' }, 403);
  }

  const tokens = await createSessionTokens(user.id, activeBrain.id, env);
  return corsJsonResponse({
    user: userPayload(user),
    active_brain: activeBrain,
    brains,
    ...tokens,
  });
}

async function handleAuthRefresh(request: Request, env: Env): Promise<Response> {
  const body = await readJsonBody(request);
  if (!body) return corsJsonResponse({ error: 'Invalid JSON body.' }, 400);
  const refreshToken = typeof body.refresh_token === 'string' ? body.refresh_token.trim() : '';
  if (!refreshToken) return corsJsonResponse({ error: 'refresh_token is required.' }, 400);

  const rotated = await rotateSession(refreshToken, env);
  if (!rotated) return corsJsonResponse({ error: 'Invalid or expired refresh token.' }, 401);

  const user = await env.DB.prepare(
    'SELECT id, email, display_name, created_at FROM users WHERE id = ? LIMIT 1'
  ).bind(rotated.userId).first<{ id: string; email: string; display_name: string | null; created_at: number }>();
  if (!user) return corsJsonResponse({ error: 'Session user not found.' }, 401);

  const brains = await listBrainsForUser(rotated.userId, env);
  const activeBrain = findActiveBrain(brains, rotated.brainId);
  if (!activeBrain) return corsJsonResponse({ error: 'No brain membership found for session user.' }, 403);

  return corsJsonResponse({
    user: userPayload(user),
    active_brain: activeBrain,
    brains,
    ...rotated.tokens,
  });
}

async function handleAuthLogout(request: Request, env: Env): Promise<Response> {
  const body = await readJsonBody(request);
  const refreshFromBody = typeof body?.refresh_token === 'string' ? body.refresh_token.trim() : '';
  const refreshFromHeader = parseBearerToken(request) ?? '';
  const refreshToken = refreshFromBody || refreshFromHeader;
  if (!refreshToken) return corsJsonResponse({ error: 'refresh_token is required.' }, 400);
  await revokeSession(refreshToken, env);
  return corsJsonResponse({ ok: true });
}

async function handleAuthMe(authCtx: AuthContext, env: Env): Promise<Response> {
  if (authCtx.kind === 'legacy') {
    return corsJsonResponse({
      kind: 'legacy',
      user: null,
      active_brain: {
        id: LEGACY_BRAIN_ID,
        name: 'Legacy Shared Brain',
        role: 'legacy',
      },
      brains: [{
        id: LEGACY_BRAIN_ID,
        name: 'Legacy Shared Brain',
        role: 'legacy',
      }],
    });
  }
  if (!authCtx.userId) return corsJsonResponse({ error: 'Unauthorized' }, 401);

  const user = await env.DB.prepare(
    'SELECT id, email, display_name, created_at FROM users WHERE id = ? LIMIT 1'
  ).bind(authCtx.userId).first<{ id: string; email: string; display_name: string | null; created_at: number }>();
  if (!user) return corsJsonResponse({ error: 'User not found.' }, 401);

  const brains = await listBrainsForUser(user.id, env);
  const activeBrain = findActiveBrain(brains, authCtx.brainId);
  if (!activeBrain) return corsJsonResponse({ error: 'No brain membership found for user.' }, 403);

  return corsJsonResponse({
    kind: 'user',
    user: userPayload(user),
    active_brain: activeBrain,
    brains,
  });
}

type OAuthClientRow = {
  id: string;
  client_id: string;
  client_name: string | null;
  redirect_uris: string[];
  grant_types: string[];
  response_types: string[];
  token_endpoint_auth_method: string;
  client_secret_hash: string | null;
  client_secret_expires_at: number;
  created_at: number;
  updated_at: number;
};

type OAuthCodeRow = {
  id: string;
  code: string;
  client_id: string;
  redirect_uri: string;
  user_id: string;
  brain_id: string;
  code_challenge: string;
  code_challenge_method: string;
  scope: string | null;
  resource: string | null;
  created_at: number;
  expires_at: number;
  used_at: number | null;
};

const OAUTH_CODE_TTL_SECONDS = 10 * 60;

function noStoreJsonHeaders(extra: Record<string, string> = {}): Record<string, string> {
  return {
    ...CORS_HEADERS,
    'Content-Type': 'application/json',
    'Cache-Control': 'no-store',
    'Pragma': 'no-cache',
    ...extra,
  };
}

function oauthError(error: string, errorDescription: string, status = 400): Response {
  return new Response(JSON.stringify({
    error,
    error_description: errorDescription,
  }), {
    status,
    headers: noStoreJsonHeaders(),
  });
}

function parseJsonStringArray(raw: string, fallback: string[] = []): string[] {
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) return fallback;
    return parsed.filter((v): v is string => typeof v === 'string');
  } catch {
    return fallback;
  }
}

function isValidRedirectUri(raw: string): boolean {
  try {
    const url = new URL(raw);
    if (url.hash) return false;
    if (url.protocol === 'https:') return true;
    if (url.protocol !== 'http:') return false;
    return url.hostname === 'localhost' || url.hostname === '127.0.0.1';
  } catch {
    return false;
  }
}

function escapeHtml(input: string): string {
  return input
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function readFormBody(request: Request): Promise<URLSearchParams | null> {
  const contentType = (request.headers.get('content-type') ?? '').toLowerCase();
  if (contentType.includes('application/x-www-form-urlencoded')) {
    try {
      const text = await request.text();
      return new URLSearchParams(text);
    } catch {
      return null;
    }
  }
  if (contentType.includes('application/json')) {
    const body = await readJsonBody(request);
    if (!body) return null;
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(body)) {
      if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') {
        params.set(k, String(v));
      }
    }
    return params;
  }
  return null;
}

async function getOAuthClient(clientId: string, env: Env): Promise<OAuthClientRow | null> {
  const row = await env.DB.prepare(
    `SELECT id, client_id, client_name, redirect_uris, grant_types, response_types,
            token_endpoint_auth_method, client_secret_hash, client_secret_expires_at, created_at, updated_at
     FROM oauth_clients
     WHERE client_id = ?
     LIMIT 1`
  ).bind(clientId).first<{
    id: string;
    client_id: string;
    client_name: string | null;
    redirect_uris: string;
    grant_types: string;
    response_types: string;
    token_endpoint_auth_method: string;
    client_secret_hash: string | null;
    client_secret_expires_at: number;
    created_at: number;
    updated_at: number;
  }>();
  if (!row) return null;
  return {
    ...row,
    redirect_uris: parseJsonStringArray(row.redirect_uris),
    grant_types: parseJsonStringArray(row.grant_types, ['authorization_code', 'refresh_token']),
    response_types: parseJsonStringArray(row.response_types, ['code']),
  };
}

function isAllowedRedirectForClient(client: OAuthClientRow | null, redirectUri: string): boolean {
  if (!isValidRedirectUri(redirectUri)) return false;
  if (!client) return true;
  return client.redirect_uris.includes(redirectUri);
}

async function ensureOAuthClient(clientId: string, redirectUri: string, env: Env): Promise<OAuthClientRow> {
  const existing = await getOAuthClient(clientId, env);
  if (existing) return existing;
  const ts = now();
  const id = generateId();
  const redirectUris = [redirectUri];
  const grantTypes = ['authorization_code', 'refresh_token'];
  const responseTypes = ['code'];
  await env.DB.prepare(
    `INSERT INTO oauth_clients
      (id, client_id, client_name, redirect_uris, grant_types, response_types, token_endpoint_auth_method, client_secret_hash, client_secret_expires_at, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, 'none', NULL, 0, ?, ?)`
  ).bind(
    id,
    clientId,
    null,
    JSON.stringify(redirectUris),
    JSON.stringify(grantTypes),
    JSON.stringify(responseTypes),
    ts,
    ts
  ).run();
  return {
    id,
    client_id: clientId,
    client_name: null,
    redirect_uris: redirectUris,
    grant_types: grantTypes,
    response_types: responseTypes,
    token_endpoint_auth_method: 'none',
    client_secret_hash: null,
    client_secret_expires_at: 0,
    created_at: ts,
    updated_at: ts,
  };
}

function validateAuthorizeParams(params: URLSearchParams): { ok: true; data: Record<string, string> } | { ok: false; message: string } {
  const responseType = params.get('response_type') ?? '';
  const clientId = params.get('client_id') ?? '';
  const redirectUri = params.get('redirect_uri') ?? '';
  const codeChallenge = params.get('code_challenge') ?? '';
  const codeChallengeMethod = (params.get('code_challenge_method') ?? 'S256').toUpperCase();
  if (responseType !== 'code') return { ok: false, message: 'response_type must be "code".' };
  if (!clientId.trim()) return { ok: false, message: 'client_id is required.' };
  if (!redirectUri.trim()) return { ok: false, message: 'redirect_uri is required.' };
  if (!isValidRedirectUri(redirectUri)) return { ok: false, message: 'redirect_uri is invalid.' };
  if (!codeChallenge.trim()) return { ok: false, message: 'code_challenge is required.' };
  if (codeChallengeMethod !== 'S256' && codeChallengeMethod !== 'PLAIN') {
    return { ok: false, message: 'code_challenge_method must be S256 or plain.' };
  }
  return {
    ok: true,
    data: {
      response_type: responseType,
      client_id: clientId.trim(),
      redirect_uri: redirectUri.trim(),
      state: params.get('state') ?? '',
      scope: params.get('scope') ?? '',
      resource: params.get('resource') ?? '',
      code_challenge: codeChallenge.trim(),
      code_challenge_method: codeChallengeMethod,
    },
  };
}

function renderAuthorizePage(requestData: Record<string, string>, errorMessage: string | null): string {
  const hidden = [
    'response_type',
    'client_id',
    'redirect_uri',
    'state',
    'scope',
    'resource',
    'code_challenge',
    'code_challenge_method',
  ].map((k) => `<input type="hidden" name="${k}" value="${escapeHtml(requestData[k] ?? '')}">`).join('');
  const errorBlock = errorMessage
    ? `<div style="margin:0 0 12px;color:#ef4444;font-size:13px;">${escapeHtml(errorMessage)}</div>`
    : '';

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Connect Second Brain</title>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:0;background:#0b1220;color:#e6edf5;display:flex;min-height:100vh;align-items:center;justify-content:center}
    .card{width:min(440px,92vw);background:#111a2c;border:1px solid #24364f;border-radius:16px;padding:20px}
    h1{font-size:20px;margin:0 0 8px}
    p{margin:0 0 16px;color:#9fb2c8;font-size:14px}
    label{display:block;font-size:12px;color:#9fb2c8;margin:10px 0 6px}
    input{width:100%;box-sizing:border-box;background:#0b1220;border:1px solid #2b3f59;color:#e6edf5;border-radius:10px;padding:10px 12px}
    .row{display:flex;gap:8px;margin-top:14px}
    button{flex:1;border:none;border-radius:10px;padding:11px 12px;font-weight:600;cursor:pointer}
    .primary{background:#22c55e;color:#04110a}
    .secondary{background:#1d4ed8;color:#e8f0ff}
    .meta{margin-top:14px;font-size:11px;color:#7f92a8}
  </style>
</head>
<body>
  <form class="card" method="post" action="/authorize">
    <h1>Connect Your Second Brain</h1>
    <p>Sign in or create an account to authorize this MCP integration.</p>
    ${errorBlock}
    ${hidden}
    <label>Email</label>
    <input type="email" name="email" required autocomplete="username" />
    <label>Password</label>
    <input type="password" name="password" required autocomplete="current-password" />
    <label>Brain Name (used when signing up)</label>
    <input type="text" name="brain_name" placeholder="My Second Brain" />
    <div class="row">
      <button class="primary" type="submit" name="auth_mode" value="login">Sign In</button>
      <button class="secondary" type="submit" name="auth_mode" value="signup">Sign Up</button>
    </div>
    <div class="meta">Client: ${escapeHtml(requestData.client_id ?? '')}</div>
  </form>
</body>
</html>`;
}

async function issueAuthorizationCode(
  clientId: string,
  redirectUri: string,
  userId: string,
  brainId: string,
  codeChallenge: string,
  codeChallengeMethod: string,
  scope: string,
  resource: string,
  env: Env
): Promise<string> {
  const code = randomToken(24);
  const ts = now();
  await env.DB.prepare(
    `INSERT INTO oauth_authorization_codes
      (id, code, client_id, redirect_uri, user_id, brain_id, code_challenge, code_challenge_method, scope, resource, created_at, expires_at, used_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`
  ).bind(
    generateId(),
    code,
    clientId,
    redirectUri,
    userId,
    brainId,
    codeChallenge,
    codeChallengeMethod,
    scope || null,
    resource || null,
    ts,
    ts + OAUTH_CODE_TTL_SECONDS
  ).run();
  return code;
}

async function handleOAuthAuthorize(request: Request, url: URL, env: Env): Promise<Response> {
  if (request.method === 'GET') {
    const parsed = validateAuthorizeParams(url.searchParams);
    if (!parsed.ok) return oauthError('invalid_request', parsed.message, 400);
    const client = await getOAuthClient(parsed.data.client_id, env);
    if (!isAllowedRedirectForClient(client, parsed.data.redirect_uri)) {
      return oauthError('invalid_request', 'redirect_uri is not allowed for this client.', 400);
    }
    return new Response(renderAuthorizePage(parsed.data, null), {
      headers: { 'Content-Type': 'text/html; charset=utf-8' },
    });
  }

  if (request.method !== 'POST') return oauthError('invalid_request', 'Method not allowed.', 405);
  const form = await readFormBody(request);
  if (!form) return oauthError('invalid_request', 'Unable to read authorization form.', 400);
  const parsed = validateAuthorizeParams(form);
  if (!parsed.ok) return oauthError('invalid_request', parsed.message, 400);

  const authMode = (form.get('auth_mode') ?? 'login').toLowerCase();
  const emailRaw = form.get('email') ?? '';
  const passwordRaw = form.get('password') ?? '';
  const brainNameRaw = form.get('brain_name') ?? '';
  if (!isValidEmail(emailRaw)) {
    return new Response(renderAuthorizePage(parsed.data, 'Please enter a valid email.'), {
      status: 400,
      headers: { 'Content-Type': 'text/html; charset=utf-8' },
    });
  }
  if (typeof passwordRaw !== 'string' || passwordRaw.length === 0) {
    return new Response(renderAuthorizePage(parsed.data, 'Please enter your password.'), {
      status: 400,
      headers: { 'Content-Type': 'text/html; charset=utf-8' },
    });
  }

  const email = normalizeEmail(emailRaw);
  let userId = '';
  let brainId = '';

  if (authMode === 'signup') {
    if (!isStrongEnoughPassword(passwordRaw)) {
      return new Response(renderAuthorizePage(parsed.data, 'Password must be at least 10 characters.'), {
        status: 400,
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
      });
    }
    const existing = await env.DB.prepare('SELECT id FROM users WHERE email = ? LIMIT 1').bind(email).first<{ id: string }>();
    if (existing?.id) {
      return new Response(renderAuthorizePage(parsed.data, 'Account already exists. Use Sign In.'), {
        status: 409,
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
      });
    }

    const ts = now();
    userId = generateId();
    const passwordHash = await hashPassword(passwordRaw);
    const displayName = sanitizeDisplayName(undefined, email);
    await env.DB.prepare(
      'INSERT INTO users (id, email, password_hash, display_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)'
    ).bind(userId, email, passwordHash, displayName, ts, ts).run();
    brainId = generateId();
    const brainName = sanitizeBrainName(brainNameRaw, email);
    await env.DB.prepare(
      'INSERT INTO brains (id, name, slug, owner_user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)'
    ).bind(brainId, brainName, `${slugify(brainName)}-${brainId.slice(0, 8)}`, userId, ts, ts).run();
    await env.DB.prepare(
      "INSERT INTO brain_memberships (brain_id, user_id, role, created_at) VALUES (?, ?, 'owner', ?)"
    ).bind(brainId, userId, ts).run();
  } else {
    const user = await env.DB.prepare(
      'SELECT id, email, password_hash FROM users WHERE email = ? LIMIT 1'
    ).bind(email).first<{ id: string; email: string; password_hash: string }>();
    if (!user || !(await verifyPassword(passwordRaw, user.password_hash))) {
      return new Response(renderAuthorizePage(parsed.data, 'Invalid email or password.'), {
        status: 401,
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
      });
    }
    userId = user.id;
    const brains = await listBrainsForUser(userId, env);
    const activeBrain = findActiveBrain(brains, '');
    if (!activeBrain) {
      return new Response(renderAuthorizePage(parsed.data, 'No brain found for this account.'), {
        status: 403,
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
      });
    }
    brainId = activeBrain.id;
  }

  const client = await ensureOAuthClient(parsed.data.client_id, parsed.data.redirect_uri, env);
  if (!isAllowedRedirectForClient(client, parsed.data.redirect_uri)) {
    return new Response(renderAuthorizePage(parsed.data, 'redirect_uri is not allowed for this client.'), {
      status: 400,
      headers: { 'Content-Type': 'text/html; charset=utf-8' },
    });
  }

  const code = await issueAuthorizationCode(
    parsed.data.client_id,
    parsed.data.redirect_uri,
    userId,
    brainId,
    parsed.data.code_challenge,
    parsed.data.code_challenge_method,
    parsed.data.scope,
    parsed.data.resource,
    env
  );
  const redirectUrl = new URL(parsed.data.redirect_uri);
  redirectUrl.searchParams.set('code', code);
  if (parsed.data.state) redirectUrl.searchParams.set('state', parsed.data.state);
  return new Response(null, {
    status: 302,
    headers: { Location: redirectUrl.toString() },
  });
}

async function handleOAuthToken(request: Request, env: Env): Promise<Response> {
  if (request.method !== 'POST') return oauthError('invalid_request', 'Method not allowed.', 405);
  const params = await readFormBody(request);
  if (!params) return oauthError('invalid_request', 'Invalid token request body.', 400);

  const grantType = (params.get('grant_type') ?? '').trim();
  if (grantType === 'authorization_code') {
    const code = (params.get('code') ?? '').trim();
    const redirectUri = (params.get('redirect_uri') ?? '').trim();
    const clientId = (params.get('client_id') ?? '').trim();
    const codeVerifier = (params.get('code_verifier') ?? '').trim();
    if (!code || !redirectUri || !clientId || !codeVerifier) {
      return oauthError('invalid_request', 'grant_type=authorization_code requires code, redirect_uri, client_id, and code_verifier.');
    }

    const authCode = await env.DB.prepare(
      `SELECT id, code, client_id, redirect_uri, user_id, brain_id, code_challenge, code_challenge_method, scope, resource, created_at, expires_at, used_at
       FROM oauth_authorization_codes
       WHERE code = ?
       LIMIT 1`
    ).bind(code).first<OAuthCodeRow>();
    if (!authCode || authCode.used_at !== null || authCode.expires_at <= now()) {
      return oauthError('invalid_grant', 'Authorization code is invalid or expired.');
    }
    if (authCode.client_id !== clientId || authCode.redirect_uri !== redirectUri) {
      return oauthError('invalid_grant', 'Authorization code does not match client_id/redirect_uri.');
    }

    const method = (authCode.code_challenge_method || 'S256').toUpperCase();
    if (method === 'S256') {
      const verifierDigest = await sha256DigestBase64Url(codeVerifier);
      if (verifierDigest !== authCode.code_challenge) {
        return oauthError('invalid_grant', 'code_verifier validation failed.');
      }
    } else if (method === 'PLAIN') {
      if (codeVerifier !== authCode.code_challenge) {
        return oauthError('invalid_grant', 'code_verifier validation failed.');
      }
    } else {
      return oauthError('invalid_grant', 'Unsupported code challenge method.');
    }

    await env.DB.prepare(
      'UPDATE oauth_authorization_codes SET used_at = ? WHERE id = ?'
    ).bind(now(), authCode.id).run();

    const tokens = await createSessionTokens(authCode.user_id, authCode.brain_id, env);
    return new Response(JSON.stringify({
      access_token: tokens.access_token,
      token_type: 'Bearer',
      expires_in: tokens.expires_in,
      refresh_token: tokens.refresh_token,
      scope: authCode.scope ?? 'mcp:full',
    }), {
      headers: noStoreJsonHeaders(),
    });
  }

  if (grantType === 'refresh_token') {
    const refreshToken = (params.get('refresh_token') ?? '').trim();
    if (!refreshToken) return oauthError('invalid_request', 'refresh_token is required.');
    const rotated = await rotateSession(refreshToken, env);
    if (!rotated) return oauthError('invalid_grant', 'Refresh token is invalid or expired.');
    return new Response(JSON.stringify({
      access_token: rotated.tokens.access_token,
      token_type: 'Bearer',
      expires_in: rotated.tokens.expires_in,
      refresh_token: rotated.tokens.refresh_token,
      scope: 'mcp:full',
    }), {
      headers: noStoreJsonHeaders(),
    });
  }

  return oauthError('unsupported_grant_type', 'Supported grant types are authorization_code and refresh_token.');
}

async function handleOAuthRegister(request: Request, env: Env): Promise<Response> {
  if (request.method !== 'POST') return oauthError('invalid_request', 'Method not allowed.', 405);
  const body = await readJsonBody(request);
  if (!body) return oauthError('invalid_request', 'Invalid JSON body.', 400);

  const redirectUrisRaw = body.redirect_uris;
  if (!Array.isArray(redirectUrisRaw) || redirectUrisRaw.length === 0) {
    return oauthError('invalid_client_metadata', 'redirect_uris must be a non-empty array.');
  }
  const redirectUris = redirectUrisRaw
    .filter((v): v is string => typeof v === 'string')
    .map((v) => v.trim())
    .filter(Boolean);
  if (!redirectUris.length || redirectUris.some((uri) => !isValidRedirectUri(uri))) {
    return oauthError('invalid_client_metadata', 'All redirect_uris must be valid HTTPS URIs (or localhost HTTP).');
  }

  const authMethodRaw = typeof body.token_endpoint_auth_method === 'string'
    ? body.token_endpoint_auth_method.trim()
    : 'none';
  const authMethod = authMethodRaw === 'none' || authMethodRaw === 'client_secret_post' || authMethodRaw === 'client_secret_basic'
    ? authMethodRaw
    : 'none';
  const clientName = typeof body.client_name === 'string' ? body.client_name.trim().slice(0, 160) : null;
  const grantTypes = Array.isArray(body.grant_types)
    ? body.grant_types.filter((v): v is string => typeof v === 'string')
    : ['authorization_code', 'refresh_token'];
  const responseTypes = Array.isArray(body.response_types)
    ? body.response_types.filter((v): v is string => typeof v === 'string')
    : ['code'];

  const ts = now();
  const clientId = `mcp_${randomToken(12)}`;
  const clientSecret = authMethod === 'none' ? null : randomToken(24);
  const clientSecretHash = clientSecret ? await sha256DigestBase64Url(clientSecret) : null;
  await env.DB.prepare(
    `INSERT INTO oauth_clients
      (id, client_id, client_name, redirect_uris, grant_types, response_types, token_endpoint_auth_method, client_secret_hash, client_secret_expires_at, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`
  ).bind(
    generateId(),
    clientId,
    clientName,
    JSON.stringify(redirectUris),
    JSON.stringify(grantTypes),
    JSON.stringify(responseTypes),
    authMethod,
    clientSecretHash,
    ts,
    ts
  ).run();

  return new Response(JSON.stringify({
    client_id: clientId,
    client_id_issued_at: ts,
    client_secret: clientSecret,
    client_secret_expires_at: 0,
    redirect_uris: redirectUris,
    token_endpoint_auth_method: authMethod,
    grant_types: grantTypes,
    response_types: responseTypes,
    client_name: clientName,
  }), {
    status: 201,
    headers: noStoreJsonHeaders(),
  });
}

function handleProtectedResourceMetadata(url: URL): Response {
  const prefix = '/.well-known/oauth-protected-resource';
  const suffix = url.pathname.slice(prefix.length);
  const resourcePath = suffix && suffix.startsWith('/') ? suffix : '/mcp';
  return corsJsonResponse({
    resource: `${url.origin}${resourcePath}`,
    authorization_servers: [url.origin],
    bearer_methods_supported: ['header'],
    scopes_supported: ['mcp:full'],
  });
}

function handleAuthorizationServerMetadata(url: URL): Response {
  return corsJsonResponse({
    issuer: url.origin,
    authorization_endpoint: `${url.origin}/authorize`,
    token_endpoint: `${url.origin}/token`,
    registration_endpoint: `${url.origin}/register`,
    response_types_supported: ['code'],
    grant_types_supported: ['authorization_code', 'refresh_token'],
    token_endpoint_auth_methods_supported: ['none', 'client_secret_post', 'client_secret_basic'],
    code_challenge_methods_supported: ['S256', 'plain'],
    scopes_supported: ['mcp:full'],
  });
}

function viewerHtml(): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MEMORY VAULT</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<style>
  :root {
    --bg: #080c10;
    --bg2: #0d1219;
    --bg3: #111820;
    --border: #1e2d3d;
    --border-bright: #2a4060;
    --amber: #f0a500;
    --amber-dim: #7a5200;
    --amber-glow: rgba(240,165,0,0.12);
    --teal: #00c8b4;
    --red: #e05050;
    --text: #c8d8e8;
    --text-dim: #4a6070;
    --text-bright: #e8f4ff;
    --mono: 'Share Tech Mono', monospace;
    --sans: 'Syne', sans-serif;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--mono);
    min-height: 100vh;
    overflow-x: hidden;
  }

  .stat-pill, .refresh-btn, .logout-btn, .login-btn, .card, .connection-chip, .expand-close {
    touch-action: manipulation;
  }

  /* Scanline overlay */
  body::before {
    content: '';
    position: fixed;
    inset: 0;
    background: repeating-linear-gradient(
      0deg,
      transparent,
      transparent 2px,
      rgba(0,0,0,0.08) 2px,
      rgba(0,0,0,0.08) 4px
    );
    pointer-events: none;
    z-index: 9999;
  }

  /* ── LOGIN SCREEN ── */
  #login-screen {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    min-height: 100vh;
    padding: 2rem;
    animation: fadeIn 0.6s ease;
  }
  .login-box {
    width: 100%;
    max-width: 420px;
    border: 1px solid var(--border-bright);
    background: var(--bg2);
    padding: 3rem 2.5rem;
    position: relative;
  }
  .login-box::before {
    content: 'CLASSIFIED';
    position: absolute;
    top: -1px; left: 2rem;
    background: var(--amber);
    color: var(--bg);
    font-family: var(--mono);
    font-size: 0.6rem;
    font-weight: 700;
    letter-spacing: 0.2em;
    padding: 0.2rem 0.6rem;
  }
  .login-box::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, var(--amber), transparent);
  }
  .vault-logo {
    font-family: var(--sans);
    font-weight: 800;
    font-size: 2.2rem;
    letter-spacing: -0.02em;
    color: var(--text-bright);
    margin-bottom: 0.3rem;
  }
  .vault-logo span { color: var(--amber); }
  .vault-sub {
    font-size: 0.68rem;
    letter-spacing: 0.2em;
    color: var(--text-dim);
    text-transform: uppercase;
    margin-bottom: 2.5rem;
  }
  .field-label {
    font-size: 0.65rem;
    letter-spacing: 0.18em;
    color: var(--amber);
    text-transform: uppercase;
    margin-bottom: 0.5rem;
  }
  .token-input {
    width: 100%;
    background: var(--bg3);
    border: 1px solid var(--border);
    color: var(--teal);
    font-family: var(--mono);
    font-size: 0.85rem;
    padding: 0.75rem 1rem;
    outline: none;
    transition: border-color 0.2s;
    letter-spacing: 0.05em;
  }
  .token-input:focus { border-color: var(--amber); }
  .token-input::placeholder { color: var(--text-dim); }
  .login-btn-row {
    display: flex;
    gap: 0.6rem;
    margin-top: 1.1rem;
  }
  .login-btn {
    width: 100%;
    margin-top: 0;
    background: var(--amber);
    color: var(--bg);
    border: none;
    font-family: var(--mono);
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    padding: 0.9rem;
    cursor: pointer;
    transition: background 0.15s, transform 0.1s;
  }
  .login-btn:hover { background: #ffbc20; }
  .login-btn.secondary {
    background: transparent;
    color: var(--text);
    border: 1px solid var(--border-bright);
  }
  .login-btn.secondary:hover {
    background: var(--bg3);
    color: var(--text-bright);
  }
  .token-btn { margin-top: 0.75rem; }
  .login-btn:active { transform: scale(0.99); }
  .login-error {
    margin-top: 1rem;
    font-size: 0.7rem;
    color: var(--red);
    letter-spacing: 0.1em;
    display: none;
  }

  /* ── MAIN APP ── */
  #app { display: none; flex-direction: column; min-height: 100vh; animation: fadeIn 0.4s ease; }

  /* Header */
  .hdr {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 1rem 2rem;
    border-bottom: 1px solid var(--border);
    background: var(--bg2);
    position: sticky;
    top: 0;
    z-index: 100;
  }
  .hdr-brand {
    font-family: var(--sans);
    font-weight: 800;
    font-size: 1.2rem;
    letter-spacing: -0.02em;
    color: var(--text-bright);
  }
  .hdr-brand span { color: var(--amber); }
  .hdr-meta {
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    color: var(--text-dim);
    text-align: right;
  }
  .hdr-meta strong { color: var(--amber); }
  .logout-btn {
    background: none;
    border: 1px solid var(--border);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.65rem;
    letter-spacing: 0.12em;
    padding: 0.35rem 0.8rem;
    cursor: pointer;
    transition: border-color 0.2s, color 0.2s;
    margin-left: 1.5rem;
    text-transform: uppercase;
  }
  .logout-btn:hover { border-color: var(--red); color: var(--red); }

  /* Stats bar */
  .stats-bar {
    display: flex;
    gap: 1px;
    background: var(--border);
    border-bottom: 1px solid var(--border);
  }
  .stat-pill {
    flex: 1;
    padding: 0.6rem 1.5rem;
    background: var(--bg2);
    text-align: center;
    cursor: pointer;
    transition: background 0.15s;
    position: relative;
  }
  .stat-pill:hover, .stat-pill.active { background: var(--bg3); }
  .stat-pill.active::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 2px;
    background: var(--amber);
  }
  .stat-num {
    font-family: var(--sans);
    font-size: 1.6rem;
    font-weight: 800;
    color: var(--amber);
    line-height: 1;
  }
  .stat-label {
    font-size: 0.6rem;
    letter-spacing: 0.18em;
    color: var(--text-dim);
    text-transform: uppercase;
    margin-top: 0.2rem;
  }

  /* Controls */
  .controls {
    display: flex;
    gap: 0.75rem;
    padding: 1rem 2rem;
    background: var(--bg2);
    border-bottom: 1px solid var(--border);
    flex-wrap: wrap;
    align-items: center;
  }
  .search-wrap {
    flex: 1;
    min-width: 200px;
    position: relative;
  }
  .search-wrap::before {
    content: '//';
    position: absolute;
    left: 0.75rem;
    top: 50%;
    transform: translateY(-50%);
    color: var(--amber);
    font-size: 0.75rem;
    pointer-events: none;
  }
  .search-input {
    width: 100%;
    background: var(--bg3);
    border: 1px solid var(--border);
    color: var(--text);
    font-family: var(--mono);
    font-size: 0.8rem;
    padding: 0.55rem 0.75rem 0.55rem 2.2rem;
    outline: none;
    transition: border-color 0.2s;
  }
  .search-input:focus { border-color: var(--amber); }
  .search-input::placeholder { color: var(--text-dim); }
  .filter-btn {
    background: var(--bg3);
    border: 1px solid var(--border);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.65rem;
    letter-spacing: 0.12em;
    padding: 0.55rem 1rem;
    cursor: pointer;
    text-transform: uppercase;
    transition: all 0.15s;
  }
  .filter-btn:hover { border-color: var(--amber-dim); color: var(--text); }
  .filter-btn.active { border-color: var(--amber); color: var(--amber); background: var(--amber-glow); }
  .refresh-btn {
    background: none;
    border: 1px solid var(--border-bright);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.65rem;
    padding: 0.55rem 0.9rem;
    cursor: pointer;
    letter-spacing: 0.1em;
    transition: all 0.15s;
    text-transform: uppercase;
  }
  .refresh-btn:hover { color: var(--teal); border-color: var(--teal); }

  /* Memory grid */
  .grid-wrap {
    flex: 1;
    padding: 1.5rem 2rem;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 1px;
    background: var(--border);
    align-content: start;
  }
  .empty-state {
    grid-column: 1/-1;
    padding: 5rem 2rem;
    text-align: center;
    color: var(--text-dim);
    font-size: 0.75rem;
    letter-spacing: 0.15em;
    text-transform: uppercase;
  }
  .empty-state .empty-icon { font-size: 2.5rem; margin-bottom: 1rem; opacity: 0.3; }

  /* Memory card */
  .card {
    background: var(--bg2);
    padding: 1.25rem 1.5rem;
    position: relative;
    transition: background 0.15s;
    animation: slideUp 0.3s ease backwards;
    cursor: default;
  }
  .card:hover { background: var(--bg3); }
  .card-type-stripe {
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 3px;
  }
  .card[data-type="note"] .card-type-stripe { background: var(--teal); }
  .card[data-type="fact"] .card-type-stripe { background: var(--amber); }
  .card[data-type="journal"] .card-type-stripe { background: #8888ff; }

  .card-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
  }
  .card-type-badge {
    font-size: 0.55rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    padding: 0.2rem 0.5rem;
    border: 1px solid;
    flex-shrink: 0;
  }
  .card[data-type="note"] .card-type-badge { border-color: var(--teal); color: var(--teal); }
  .card[data-type="fact"] .card-type-badge { border-color: var(--amber); color: var(--amber); }
  .card[data-type="journal"] .card-type-badge { border-color: #8888ff; color: #8888ff; }

  .card-title {
    font-family: var(--sans);
    font-size: 0.9rem;
    font-weight: 700;
    color: var(--text-bright);
    letter-spacing: -0.01em;
    line-height: 1.3;
    word-break: break-word;
  }
  .card-key {
    font-size: 0.7rem;
    color: var(--amber);
    letter-spacing: 0.08em;
    margin-bottom: 0.5rem;
  }
  .card-key span { color: var(--text-dim); }
  .card-content {
    font-size: 0.78rem;
    color: var(--text);
    line-height: 1.65;
    word-break: break-word;
    white-space: pre-wrap;
    max-height: 120px;
    overflow: hidden;
    position: relative;
  }
  .card-content::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 40px;
    background: linear-gradient(transparent, var(--bg2));
    pointer-events: none;
  }
  .card:hover .card-content::after {
    background: linear-gradient(transparent, var(--bg3));
  }
  .card-footer {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    margin-top: 1rem;
    padding-top: 0.75rem;
    border-top: 1px solid var(--border);
    gap: 0.6rem;
  }
  .card-meta {
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
    min-width: 0;
  }
  .card-tags {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
  }
  .card-quality {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
  }
  .quality-chip {
    font-size: 0.52rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    border: 1px solid var(--border);
    color: var(--text-dim);
    background: rgba(8, 12, 16, 0.7);
    padding: 0.12rem 0.34rem;
  }
  .quality-chip.conf { border-color: #66a9ff; color: #66a9ff; }
  .quality-chip.imp { border-color: var(--amber); color: var(--amber); }
  .quality-chip.src { border-color: var(--teal); color: var(--teal); }
  .tag {
    font-size: 0.55rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    background: var(--bg);
    border: 1px solid var(--border);
    color: var(--text-dim);
    padding: 0.15rem 0.4rem;
  }
  .card-date {
    font-size: 0.6rem;
    color: var(--text-dim);
    letter-spacing: 0.05em;
    white-space: nowrap;
    flex-shrink: 0;
  }
  .card-id {
    font-size: 0.55rem;
    color: var(--text-dim);
    opacity: 0.5;
    letter-spacing: 0.04em;
    margin-top: 0.3rem;
  }

  /* Expand overlay */
  .expand-overlay {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(4,8,14,0.92);
    z-index: 200;
    padding: 2rem;
    overflow-y: auto;
    animation: fadeIn 0.2s ease;
  }
  .expand-overlay.open { display: flex; align-items: flex-start; justify-content: center; }
  .expand-box {
    width: 100%;
    max-width: 680px;
    background: var(--bg2);
    border: 1px solid var(--border-bright);
    padding: 2rem;
    position: relative;
    margin-top: 3rem;
    animation: slideUp 0.25s ease;
  }
  .expand-close {
    position: absolute;
    top: 1rem; right: 1rem;
    background: none;
    border: 1px solid var(--border);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.7rem;
    padding: 0.3rem 0.6rem;
    cursor: pointer;
    transition: all 0.15s;
    letter-spacing: 0.1em;
  }
  .expand-close:hover { border-color: var(--red); color: var(--red); }
  .expand-content {
    font-size: 0.82rem;
    color: var(--text);
    line-height: 1.75;
    white-space: pre-wrap;
    word-break: break-word;
    margin-top: 1rem;
  }

  /* Loading */
  .loading {
    grid-column: 1/-1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 4rem;
    gap: 0.5rem;
    color: var(--amber);
    font-size: 0.7rem;
    letter-spacing: 0.2em;
  }
  .loading-dot {
    width: 4px; height: 4px;
    background: var(--amber);
    border-radius: 50%;
    animation: blink 1s infinite;
  }
  .loading-dot:nth-child(2) { animation-delay: 0.2s; }
  .loading-dot:nth-child(3) { animation-delay: 0.4s; }

  /* Footer */
  .footer {
    padding: 0.75rem 2rem;
    border-top: 1px solid var(--border);
    background: var(--bg2);
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .footer-text { font-size: 0.6rem; color: var(--text-dim); letter-spacing: 0.1em; text-transform: uppercase; }
  .cursor-blink {
    display: inline-block;
    width: 7px; height: 13px;
    background: var(--amber);
    margin-left: 3px;
    vertical-align: middle;
    animation: blink 1s infinite;
  }

  .card-links-badge {
    font-size: 0.58rem;
    letter-spacing: 0.1em;
    color: var(--teal);
    border: 1px solid var(--teal);
    padding: 0.15rem 0.4rem;
    opacity: 0.8;
  }
  .connections-section { margin-top: 1.5rem; padding-top: 1rem; border-top: 1px solid var(--border); }
  .connections-title { font-size: 0.6rem; letter-spacing: 0.2em; color: var(--amber); text-transform: uppercase; margin-bottom: 0.75rem; }
  .connection-chip {
    display: inline-flex; align-items: center; gap: 0.4rem;
    background: var(--bg3); border: 1px solid var(--border);
    padding: 0.35rem 0.7rem; margin: 0.25rem 0.25rem 0.25rem 0;
    cursor: pointer; transition: border-color 0.15s;
    font-size: 0.72rem; color: var(--text);
  }
  .connection-chip:hover { border-color: var(--amber); color: var(--amber); }
  .connection-chip .chip-type { font-size: 0.55rem; letter-spacing: 0.15em; text-transform: uppercase; opacity: 0.6; }
  .connection-chip .chip-label { font-size: 0.6rem; color: var(--text-dim); font-style: italic; }
  .connection-chip .chip-relation {
    font-size: 0.5rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    border: 1px solid var(--border-bright);
    color: var(--teal);
    padding: 0.12rem 0.3rem;
  }
  .connection-chip .chip-relation.contradicts { border-color: var(--red); color: var(--red); }
  .connection-chip .chip-relation.supersedes { border-color: var(--amber); color: var(--amber); }
  .connection-chip .chip-relation.supports { border-color: #2eca75; color: #2eca75; }
  .graph-node circle { stroke-width: 2px; cursor: pointer; transition: r 0.15s; }
  .graph-node circle:hover { r: 10; }
  .graph-node text { font-family: var(--mono); font-size: 10px; fill: var(--text); pointer-events: none; }
  .graph-link { stroke-width: 1.5px; }
  .graph-link.explicit { stroke: var(--border-bright); opacity: 0.9; }
  .graph-link.explicit.relation-related { stroke: var(--border-bright); }
  .graph-link.explicit.relation-supports { stroke: #2eca75; }
  .graph-link.explicit.relation-contradicts { stroke: var(--red); stroke-dasharray: 6 3; }
  .graph-link.explicit.relation-supersedes { stroke: var(--amber); }
  .graph-link.explicit.relation-causes { stroke: #ff9e4f; }
  .graph-link.explicit.relation-example-of { stroke: #66a9ff; }
  .graph-link.inferred { stroke: var(--teal); opacity: 0.4; stroke-dasharray: 4 4; }
  .graph-link-label { font-family: var(--mono); font-size: 9px; fill: var(--text-dim); pointer-events: none; }
  .graph-toolbar-row {
    display: flex;
    gap: 0.35rem;
    flex-wrap: wrap;
    justify-content: flex-end;
    width: 100%;
  }
  .graph-search-input {
    min-width: 150px;
    background: rgba(8, 12, 16, 0.9);
    border: 1px solid var(--border-bright);
    color: var(--text);
    font-family: var(--mono);
    font-size: 0.58rem;
    letter-spacing: 0.1em;
    padding: 0.35rem 0.5rem;
    min-height: 30px;
    outline: none;
  }
  .graph-search-input:focus { border-color: var(--teal); }
  .graph-search-input::placeholder { color: var(--text-dim); }
  .graph-btn.relation { border-color: var(--border); color: var(--text-dim); }
  .graph-btn.relation.active { border-color: var(--amber); color: var(--amber); }
  .graph-btn.relation.off { opacity: 0.55; }
  .graph-toolbar {
    position: absolute;
    top: 0.75rem;
    right: 0.75rem;
    z-index: 8;
    display: flex;
    gap: 0.4rem;
    flex-direction: column;
    align-items: flex-end;
    max-width: calc(100% - 1.5rem);
  }
  .graph-btn {
    border: 1px solid var(--border-bright);
    background: rgba(8, 12, 16, 0.9);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.58rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 0.35rem 0.5rem;
    cursor: pointer;
    min-height: 30px;
  }
  .graph-btn:hover { border-color: var(--amber); color: var(--amber); }
  .graph-btn.active { color: var(--teal); border-color: var(--teal); }
  .graph-legend {
    position: absolute;
    left: 0.75rem;
    bottom: 0.75rem;
    z-index: 8;
    display: flex;
    gap: 0.5rem;
    flex-wrap: wrap;
    max-width: calc(100% - 1.5rem);
  }
  .graph-legend-item {
    font-size: 0.55rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    border: 1px solid var(--border);
    background: rgba(8, 12, 16, 0.9);
    color: var(--text-dim);
    padding: 0.2rem 0.45rem;
  }

  @media (max-width: 900px) {
    .hdr { padding: 0.85rem 1rem; }
    .controls { padding: 0.75rem 1rem; }
    .grid-wrap { padding: 1rem; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); }
    .footer { padding: 0.65rem 1rem; flex-wrap: wrap; gap: 0.45rem; }
  }

  @media (max-width: 640px) {
    body::before { display: none; }
    #login-screen { padding: 1rem; }
    .login-box { padding: 2rem 1rem 1.5rem; }
    .login-box::before { left: 1rem; }
    .login-btn-row { flex-direction: column; gap: 0.45rem; }
    .vault-logo { font-size: 1.65rem; }
    .vault-sub { margin-bottom: 1.5rem; font-size: 0.62rem; }
    .token-input, .search-input { font-size: 16px; }

    .hdr {
      position: static;
      flex-direction: column;
      align-items: flex-start;
      gap: 0.65rem;
      padding: 0.75rem 0.75rem 0.6rem;
    }
    .hdr-brand { font-size: 1.05rem; }
    .hdr-right {
      width: 100%;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.6rem;
    }
    .hdr-meta { text-align: left; font-size: 0.58rem; letter-spacing: 0.08em; }
    #live-indicator { font-size: 0.54rem !important; letter-spacing: 0.12em !important; }
    .logout-btn {
      margin-left: 0;
      min-height: 38px;
      padding: 0.45rem 0.72rem;
      font-size: 0.62rem;
    }

    .stats-bar {
      overflow-x: auto;
      overflow-y: hidden;
      -webkit-overflow-scrolling: touch;
      scrollbar-width: none;
    }
    .stats-bar::-webkit-scrollbar { display: none; }
    .stat-pill {
      flex: 0 0 88px;
      padding: 0.55rem 0.4rem;
    }
    .stat-num { font-size: 1.1rem; }
    .stat-label { font-size: 0.55rem; letter-spacing: 0.14em; }

    .controls {
      flex-direction: column;
      align-items: stretch;
      padding: 0.65rem 0.75rem;
      gap: 0.55rem;
    }
    .search-wrap { min-width: 0; width: 100%; }
    .refresh-btn {
      width: 100%;
      min-height: 42px;
      font-size: 0.62rem;
    }

    #graph-view { min-height: 54vh !important; }
    #graph-svg { min-height: 54vh !important; height: 54vh !important; }
    .graph-link-label { display: none; }
    .graph-toolbar {
      top: 0.45rem;
      left: 0.45rem;
      right: 0.45rem;
      max-width: none;
      gap: 0.25rem;
      align-items: stretch;
    }
    .graph-toolbar-row { justify-content: flex-start; }
    .graph-search-input { width: 100%; min-height: 28px; }
    .graph-btn { font-size: 0.52rem; letter-spacing: 0.08em; padding: 0.3rem 0.42rem; min-height: 28px; }
    .graph-legend {
      left: 0.45rem;
      right: 0.45rem;
      bottom: 0.45rem;
      max-width: none;
      gap: 0.35rem;
    }
    .graph-legend-item { font-size: 0.5rem; letter-spacing: 0.08em; padding: 0.2rem 0.36rem; }

    .grid-wrap {
      padding: 0.5rem;
      grid-template-columns: 1fr;
      gap: 1px;
    }
    .card { padding: 1rem 1rem 0.95rem; }
    .card-content { max-height: 96px; }
    .card-footer {
      flex-direction: column;
      align-items: flex-start;
      gap: 0.45rem;
    }
    .card-date { align-self: flex-end; font-size: 0.58rem; }

    .expand-overlay {
      padding: 0;
      align-items: stretch;
    }
    .expand-box {
      margin-top: 0;
      max-width: none;
      min-height: 100vh;
      border: none;
      border-top: 1px solid var(--border-bright);
      padding: 3.25rem 1rem 1.25rem;
    }
    .expand-close {
      top: 0.65rem;
      right: 0.65rem;
      padding: 0.45rem 0.7rem;
      font-size: 0.62rem;
    }
    .expand-content { font-size: 0.8rem; line-height: 1.7; }
    .connection-chip {
      display: flex;
      width: 100%;
      margin-right: 0;
    }

    .footer { padding: 0.55rem 0.75rem; }
    .footer-text { font-size: 0.52rem; letter-spacing: 0.08em; }
    .footer .footer-text:last-child { display: none; }
  }
  @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
  @keyframes slideUp { from { opacity: 0; transform: translateY(16px); } to { opacity: 1; transform: translateY(0); } }
  @keyframes blink { 0%,100% { opacity: 1; } 50% { opacity: 0; } }
</style>
</head>
<body>

<!-- LOGIN -->
<div id="login-screen">
  <div class="login-box">
    <div class="vault-logo">MEMORY<span>VAULT</span></div>
    <div class="vault-sub">Secure Access Required</div>
    <div class="field-label">Email</div>
    <input type="email" class="token-input" id="email-input" placeholder="you@example.com" autocomplete="username" autocapitalize="off" autocorrect="off" spellcheck="false">
    <div class="field-label" style="margin-top:0.75rem">Password</div>
    <input type="password" class="token-input" id="password-input" placeholder="Enter password" autocomplete="current-password">
    <div class="field-label" style="margin-top:0.75rem">Brain Name (for signup)</div>
    <input type="text" class="token-input" id="brain-name-input" placeholder="Second Brain name (optional)" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false">
    <div class="login-btn-row">
      <button class="login-btn" onclick="doCredentialAuth('login')">SIGN IN →</button>
      <button class="login-btn secondary" onclick="doCredentialAuth('signup')">SIGN UP →</button>
    </div>
    <div class="field-label" style="margin-top:1rem">Legacy Access Token</div>
    <input type="password" class="token-input" id="token-input" placeholder="Bearer token (legacy mode)" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false">
    <button class="login-btn secondary token-btn" onclick="doTokenLogin()">TOKEN LOGIN →</button>
    <div class="login-error" id="login-error">⚠ ACCESS DENIED</div>
  </div>
</div>

<!-- APP -->
<div id="app">
  <header class="hdr">
    <div class="hdr-brand">MEMORY<span>VAULT</span></div>
    <div class="hdr-right">
      <div class="hdr-meta">
        <div id="hdr-count">— entries</div>
        <div id="hdr-time"></div>
      </div>
      <div id="live-indicator" style="font-size:0.6rem;letter-spacing:0.15em;color:var(--text-dim);display:none;align-items:center">
        <span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--teal);margin-right:4px;animation:blink 2s infinite"></span>LIVE
      </div>
      <button class="logout-btn" onclick="doLogout()">LOCK</button>
    </div>
  </header>

  <div class="stats-bar">
    <div class="stat-pill active" id="stat-all" onclick="setFilter('')">
      <div class="stat-num" id="count-all">0</div>
      <div class="stat-label">All</div>
    </div>
    <div class="stat-pill" id="stat-note" onclick="setFilter('note')">
      <div class="stat-num" id="count-note">0</div>
      <div class="stat-label">Notes</div>
    </div>
    <div class="stat-pill" id="stat-fact" onclick="setFilter('fact')">
      <div class="stat-num" id="count-fact">0</div>
      <div class="stat-label">Facts</div>
    </div>
    <div class="stat-pill" id="stat-journal" onclick="setFilter('journal')">
      <div class="stat-num" id="count-journal">0</div>
      <div class="stat-label">Journal</div>
    </div>
    <div class="stat-pill" id="stat-graph" onclick="showGraph()">
      <div class="stat-num">⬡</div>
      <div class="stat-label">Graph</div>
    </div>
  </div>

  <div class="controls">
    <div class="search-wrap">
      <input type="text" class="search-input" id="search-input" placeholder="Search by name, id, key, or text..." inputmode="search" oninput="onSearch(this.value)">
    </div>
    <button class="refresh-btn" onclick="loadMemories()">↻ REFRESH</button>
  </div>

  <div id="graph-view" style="display:none;flex:1;position:relative;background:var(--bg);min-height:600px">
    <div class="graph-toolbar">
      <div class="graph-toolbar-row">
        <input type="text" class="graph-search-input" id="graph-search-input" placeholder="Search graph..." inputmode="search" oninput="onGraphSearch(this.value)">
      </div>
      <div class="graph-toolbar-row">
        <button class="graph-btn active" id="graph-toggle-inferred" onclick="toggleGraphInferred()">INFERRED ON</button>
        <button class="graph-btn active" id="graph-toggle-labels" onclick="toggleGraphLabels()">LABELS ON</button>
        <button class="graph-btn" onclick="resetGraphView()">RESET VIEW</button>
      </div>
      <div class="graph-toolbar-row">
        <button class="graph-btn relation active" id="graph-rel-related" onclick="toggleGraphRelation('related')">RELATED</button>
        <button class="graph-btn relation active" id="graph-rel-supports" onclick="toggleGraphRelation('supports')">SUPPORTS</button>
        <button class="graph-btn relation active" id="graph-rel-contradicts" onclick="toggleGraphRelation('contradicts')">CONTRA</button>
        <button class="graph-btn relation active" id="graph-rel-supersedes" onclick="toggleGraphRelation('supersedes')">SUPER</button>
        <button class="graph-btn relation active" id="graph-rel-causes" onclick="toggleGraphRelation('causes')">CAUSES</button>
        <button class="graph-btn relation active" id="graph-rel-example_of" onclick="toggleGraphRelation('example_of')">EXAMPLE</button>
      </div>
    </div>
    <div class="graph-legend" id="graph-legend"></div>
    <svg id="graph-svg" style="width:100%;height:100%;min-height:600px"></svg>
    <div id="graph-empty" style="display:none;position:absolute;inset:0;align-items:center;justify-content:center;text-align:center;color:var(--text-dim);font-size:0.72rem;letter-spacing:0.12em;padding:1rem">NO MEMORIES YET</div>
  </div>
  <div class="grid-wrap" id="grid">
    <div class="loading"><div class="loading-dot"></div><div class="loading-dot"></div><div class="loading-dot"></div></div>
  </div>

  <footer class="footer">
    <div class="footer-text">AI MEMORY MCP · CLOUDFLARE D1</div>
    <div class="footer-text">SECURE SESSION<span class="cursor-blink"></span></div>
  </footer>
</div>

<!-- EXPAND OVERLAY -->
<div class="expand-overlay" id="expand-overlay" onclick="closeExpand(event)">
  <div class="expand-box">
    <button class="expand-close" onclick="closeExpandBtn()">✕ CLOSE</button>
    <div id="expand-header"></div>
    <div class="expand-content" id="expand-content"></div>
    <div style="margin-top:1.5rem;padding-top:1rem;border-top:1px solid var(--border);font-size:0.6rem;color:var(--text-dim);letter-spacing:0.08em" id="expand-meta"></div>
    <div id="expand-connections"></div>
  </div>
</div>

<script>
  const BASE = location.origin;
  const GRAPH_RELATION_TYPES = ['related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of'];
  const GRAPH_RELATION_COLOR = {
    related: '#2a4060',
    supports: '#2eca75',
    contradicts: '#e05050',
    supersedes: '#f0a500',
    causes: '#ff9e4f',
    example_of: '#66a9ff',
  };
  let TOKEN = '';
  let REFRESH_TOKEN = '';
  let SESSION_MODE = 'legacy';
  let activeFilter = '';
  let searchTimeout = null;
  let allMemories = [];
  let expandGen = 0;
  let graphVisible = false;
  let lastGraphData = { nodes: [], edges: [], inferred_edges: [] };
  let graphResizeTimer = null;
  let graphShowInferred = true;
  let graphShowLabels = !window.matchMedia('(max-width: 640px)').matches;
  let graphSvgSelection = null;
  let graphZoomBehavior = null;
  let graphAutoTunedLabels = false;
  let graphSearchQuery = '';
  let graphRelationFilter = new Set(GRAPH_RELATION_TYPES);

  function setLoginError(message) {
    const el = document.getElementById('login-error');
    if (!el) return;
    el.textContent = message || '⚠ ACCESS DENIED';
    el.style.display = 'block';
  }

  function clearLoginError() {
    const el = document.getElementById('login-error');
    if (!el) return;
    el.style.display = 'none';
  }

  function enterApp() {
    document.getElementById('login-screen').style.display = 'none';
    document.getElementById('app').style.display = 'flex';
    document.getElementById('app').style.flexDirection = 'column';
    updateTime();
    loadMemories();
    startLivePolling();
  }

  async function tryRefreshSession() {
    if (!REFRESH_TOKEN) return false;
    try {
      const r = await fetch(BASE + '/auth/refresh', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ refresh_token: REFRESH_TOKEN }),
      });
      if (!r.ok) return false;
      const data = await r.json();
      if (!data || !data.access_token) return false;
      TOKEN = data.access_token;
      REFRESH_TOKEN = data.refresh_token || REFRESH_TOKEN;
      SESSION_MODE = 'user';
      return true;
    } catch {
      return false;
    }
  }

  async function apiFetch(url, options = {}, allowRefresh = true) {
    const mergedHeaders = Object.assign({}, options.headers || {}, TOKEN ? { 'Authorization': 'Bearer ' + TOKEN } : {});
    const response = await fetch(url, Object.assign({}, options, { headers: mergedHeaders }));
    if (response.status === 401 && allowRefresh && REFRESH_TOKEN) {
      const refreshed = await tryRefreshSession();
      if (refreshed) return apiFetch(url, options, false);
    }
    return response;
  }

  async function doTokenLogin() {
    clearLoginError();
    const val = document.getElementById('token-input').value.trim();
    if (!val) {
      setLoginError('⚠ ENTER A TOKEN');
      return;
    }
    try {
      const r = await fetch(BASE + '/api/memories?limit=1', {
        headers: { 'Authorization': 'Bearer ' + val },
      });
      if (!r.ok) {
        setLoginError('⚠ ACCESS DENIED — invalid token');
        return;
      }
      TOKEN = val;
      REFRESH_TOKEN = '';
      SESSION_MODE = 'legacy';
      enterApp();
    } catch {
      setLoginError('⚠ NETWORK ERROR');
    }
  }

  async function doCredentialAuth(mode) {
    clearLoginError();
    const email = document.getElementById('email-input').value.trim();
    const password = document.getElementById('password-input').value;
    const brainName = document.getElementById('brain-name-input').value.trim();
    if (!email || !password) {
      setLoginError('⚠ EMAIL + PASSWORD REQUIRED');
      return;
    }

    const payload = { email, password };
    if (mode === 'signup' && brainName) payload.brain_name = brainName;

    try {
      const endpoint = mode === 'signup' ? '/auth/signup' : '/auth/login';
      const r = await fetch(BASE + endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) {
        setLoginError('⚠ ' + (data.error || 'AUTH FAILED'));
        return;
      }
      TOKEN = data.access_token || '';
      REFRESH_TOKEN = data.refresh_token || '';
      SESSION_MODE = 'user';
      if (!TOKEN) {
        setLoginError('⚠ NO ACCESS TOKEN RETURNED');
        return;
      }
      enterApp();
    } catch {
      setLoginError('⚠ NETWORK ERROR');
    }
  }

  function doLogin() {
    return doTokenLogin();
  }

  async function doLogout() {
    if (SESSION_MODE === 'user' && REFRESH_TOKEN) {
      try {
        await fetch(BASE + '/auth/logout', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ refresh_token: REFRESH_TOKEN }),
        });
      } catch {}
    }
    TOKEN = '';
    REFRESH_TOKEN = '';
    SESSION_MODE = 'legacy';
    location.reload();
  }

  function updateTime() {
    const el = document.getElementById('hdr-time');
    if (el) el.textContent = new Date().toISOString().replace('T',' ').slice(0,19) + ' UTC';
    setTimeout(updateTime, 1000);
  }

  async function loadMemories(silent = false) {
    const grid = document.getElementById('grid');
    const scrollY = window.scrollY;
    if (!silent) {
      grid.innerHTML = '<div class="loading"><div class="loading-dot"></div><div class="loading-dot"></div><div class="loading-dot"></div></div>';
    }
    const search = document.getElementById('search-input').value;
    let url = BASE + '/api/memories?limit=500';
    if (activeFilter) url += '&type=' + encodeURIComponent(activeFilter);
    if (search) url += '&search=' + encodeURIComponent(search);
    try {
      const r = await apiFetch(url);
      if (!r.ok) { doLogout(); return; }
      const data = await r.json();
      allMemories = data.memories || [];
      updateStats(data.stats || [], allMemories);
      renderGrid(allMemories);
      if (silent) window.scrollTo(0, scrollY);
    } catch(e) {
      grid.innerHTML = '<div class="empty-state"><div class="empty-icon">⚠</div>CONNECTION ERROR</div>';
    }
  }

  function updateStats(stats, memories = []) {
    const counts = { note: 0, fact: 0, journal: 0 };
    let total = 0;
    stats.forEach(s => { counts[s.type] = s.count; total += s.count; });
    document.getElementById('count-all').textContent = total;
    document.getElementById('count-note').textContent = counts.note;
    document.getElementById('count-fact').textContent = counts.fact;
    document.getElementById('count-journal').textContent = counts.journal;
    const confidenceValues = memories
      .map((m) => Number(m.dynamic_confidence ?? m.confidence))
      .filter((v) => Number.isFinite(v));
    const avgConfidence = confidenceValues.length
      ? Math.round((confidenceValues.reduce((a, b) => a + b, 0) / confidenceValues.length) * 100)
      : null;
    document.getElementById('hdr-count').textContent = avgConfidence === null
      ? (total + ' entries')
      : (total + ' entries · avg conf ' + avgConfidence + '%');
  }

  function renderGrid(memories) {
    const grid = document.getElementById('grid');
    if (!memories.length) {
      grid.innerHTML = '<div class="empty-state"><div class="empty-icon">◈</div>NO MEMORIES FOUND</div>';
      return;
    }
    grid.innerHTML = memories.map((m, i) => {
      const date = new Date(m.created_at * 1000).toISOString().slice(0,10);
      const tags = m.tags ? m.tags.split(',').map(t => \`<span class="tag">\${esc(t.trim())}</span>\`).join('') : '';
      const linkBadge = m.link_count > 0 ? \`<span class="card-links-badge">⬡ \${m.link_count} connections</span>\` : '';
      const titleHtml = m.title ? \`<div class="card-title">\${esc(m.title)}</div>\` : '';
      const keyHtml = m.key ? \`<div class="card-key"><span>KEY /</span> \${esc(m.key)}</div>\` : '';
      const confidenceNum = Number(m.dynamic_confidence ?? m.confidence);
      const importanceNum = Number(m.dynamic_importance ?? m.importance);
      const confidencePct = Number.isFinite(confidenceNum) ? Math.round(Math.min(Math.max(confidenceNum, 0), 1) * 100) : null;
      const importancePct = Number.isFinite(importanceNum) ? Math.round(Math.min(Math.max(importanceNum, 0), 1) * 100) : null;
      const sourceLabel = m.source ? String(m.source).trim() : '';
      const sourceDisplay = sourceLabel.length > 18 ? (sourceLabel.slice(0, 17) + '…') : sourceLabel;
      const sourceChip = sourceDisplay ? \`<span class="quality-chip src">SRC \${esc(sourceDisplay)}</span>\` : '';
      const confChip = confidencePct === null ? '' : \`<span class="quality-chip conf">CONF \${confidencePct}%</span>\`;
      const impChip = importancePct === null ? '' : \`<span class="quality-chip imp">IMP \${importancePct}%</span>\`;
      const qualityChips = sourceChip || confChip || impChip
        ? \`<div class="card-quality">\${sourceChip}\${confChip}\${impChip}</div>\`
        : '';
      return \`<div class="card" data-type="\${m.type}" data-idx="\${i}" onclick="expandCard(\${i})" style="animation-delay:\${Math.min(i*0.04,0.4)}s">
        <div class="card-type-stripe"></div>
        <div class="card-header">
          <div>\${titleHtml}\${keyHtml}\${!m.title && !m.key ? '<div class="card-title" style="opacity:0.4">untitled</div>' : ''}</div>
          <span class="card-type-badge">\${m.type}</span>
        </div>
        <div class="card-content">\${esc(m.content)}</div>
        <div class="card-footer">
          <div class="card-meta">
            <div class="card-tags">\${tags}\${linkBadge}</div>
            \${qualityChips}
          </div>
          <div class="card-date">\${date}</div>
        </div>
        <div class="card-id">\${m.id}</div>
      </div>\`;
    }).join('');
  }

  function expandCard(idx) {
    const m = allMemories[idx];
    if (!m) return;
    const date = new Date(m.created_at * 1000).toLocaleString();
    const updated = m.updated_at !== m.created_at ? '  ·  Updated ' + new Date(m.updated_at * 1000).toLocaleString() : '';
    const typeColors = { note: 'var(--teal)', fact: 'var(--amber)', journal: '#8888ff' };
    const qualityChips = [
      m.source ? \`<span class="tag">src:\${esc(m.source)}</span>\` : '',
      Number.isFinite(Number(m.dynamic_confidence ?? m.confidence)) ? \`<span class="tag">conf:\${Math.round(Number(m.dynamic_confidence ?? m.confidence) * 100)}%</span>\` : '',
      Number.isFinite(Number(m.dynamic_importance ?? m.importance)) ? \`<span class="tag">imp:\${Math.round(Number(m.dynamic_importance ?? m.importance) * 100)}%</span>\` : '',
    ].filter(Boolean).join('');
    document.getElementById('expand-header').innerHTML =
      \`<div style="display:flex;align-items:center;gap:0.75rem;margin-bottom:0.5rem;flex-wrap:wrap">
        <span style="font-size:0.6rem;letter-spacing:0.2em;text-transform:uppercase;border:1px solid \${typeColors[m.type]||'#fff'};color:\${typeColors[m.type]||'#fff'};padding:0.2rem 0.5rem">\${m.type}</span>
        \${m.title ? \`<span style="font-family:var(--sans);font-weight:700;font-size:1.1rem;color:var(--text-bright)">\${esc(m.title)}</span>\` : ''}
        \${m.key ? \`<span style="font-size:0.75rem;color:var(--amber)">KEY: \${esc(m.key)}</span>\` : ''}
      </div>
      \${m.tags ? \`<div style="display:flex;flex-wrap:wrap;gap:0.3rem;margin-bottom:0.25rem">\${m.tags.split(',').map(t => \`<span class="tag">\${esc(t.trim())}</span>\`).join('')}</div>\` : ''}
      \${qualityChips ? \`<div style="display:flex;flex-wrap:wrap;gap:0.3rem;margin-bottom:0.25rem">\${qualityChips}</div>\` : ''}\`;
    document.getElementById('expand-content').textContent = m.content;
    document.getElementById('expand-meta').textContent = 'ID: ' + m.id + '  ·  Created ' + date + updated;
    document.getElementById('expand-overlay').classList.add('open');
    document.body.style.overflow = 'hidden';

    // Lazy-load connections
    const connEl = document.getElementById('expand-connections');
    connEl.innerHTML = '<div style="font-size:0.65rem;color:var(--text-dim);letter-spacing:0.1em;margin-top:1rem">LOADING CONNECTIONS...</div>';
    const myGen = ++expandGen;
    apiFetch(BASE + '/api/links/' + m.id)
      .then(r => {
        if (r.status === 401) { doLogout(); return null; }
        if (!r.ok) throw new Error('fetch failed');
        return r.json();
      })
      .then(links => {
        if (!links) return;
        if (myGen !== expandGen) return; // card changed, discard stale result
        if (!links || !links.length) { connEl.innerHTML = ''; return; }
        connEl.innerHTML = \`<div class="connections-section">
          <div class="connections-title">⬡ Connections (\${links.length})</div>
          \${links.map(l => {
            const cm = l.memory;
            const relationRaw = String(l.relation_type || 'related').toLowerCase();
            const relationLabel = relationRaw.replace(/_/g, ' ');
            const relationClass = relationRaw.replace(/_/g, '-').replace(/[^a-z-]/g, '');
            const label = l.label ? \`<span class="chip-label">"\${esc(l.label)}"</span>\` : '';
            const name = cm.title || cm.key || (cm.content || '').slice(0, 40) + '…';
            const arrow = l.direction === 'from' ? '→' : '←';
            return \`<span class="connection-chip" data-conn-id="\${esc(cm.id)}">
              <span class="chip-type">[\${esc(cm.type)}]</span>
              \${esc(name)}
              <span class="chip-relation \${esc(relationClass)}">\${esc(relationLabel)}</span>
              \${label}
              <span style="opacity:0.4">\${arrow}</span>
            </span>\`;
          }).join('')}
        </div>\`;
        connEl.querySelectorAll('.connection-chip').forEach(chip => {
          chip.addEventListener('click', () => expandById(chip.dataset.connId));
        });
      })
      .catch(() => { if (myGen === expandGen) connEl.innerHTML = ''; });
  }

  function closeExpand(e) {
    if (e.target === document.getElementById('expand-overlay')) closeExpandBtn();
  }
  function closeExpandBtn() {
    document.getElementById('expand-overlay').classList.remove('open');
    document.body.style.overflow = '';
  }

  function setFilter(type) {
    graphVisible = false;
    document.getElementById('graph-view').style.display = 'none';
    document.querySelector('.grid-wrap').style.display = 'grid';
    activeFilter = type;
    ['all','note','fact','journal','graph'].forEach(t => {
      document.getElementById('stat-' + t).classList.toggle('active', (type === '' ? 'all' : type) === t);
    });
    loadMemories();
  }

  function onSearch(val) {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(loadMemories, 300);
  }

  function esc(str) {
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  function expandById(id) {
    const idx = allMemories.findIndex(m => m.id === id);
    if (idx !== -1) {
      expandCard(idx);
    } else {
      // Memory not found in current view (may be filtered out or not yet loaded)
      const connEl = document.getElementById('expand-connections');
      if (connEl) {
        const note = document.createElement('div');
        note.style.cssText = 'font-size:0.65rem;color:var(--text-dim);letter-spacing:0.1em;margin-top:0.5rem';
        note.textContent = '⚠ Linked memory not visible in current filter.';
        const existing = connEl.querySelector('.connections-section');
        if (existing) {
          existing.appendChild(note);
        } else {
          connEl.appendChild(note);
        }
      }
    }
  }

  let lastPollSig = '';
  let pollIntervalId = null;

  function startLivePolling() {
    if (pollIntervalId) return;
    const liveEl = document.getElementById('live-indicator');
    if (liveEl) liveEl.style.display = 'flex';
    pollIntervalId = setInterval(async () => {
      if (!TOKEN) return;
      try {
        const r = await apiFetch(BASE + '/api/memories?limit=1');
        if (!r.ok) return;
        const data = await r.json();
        const sig = (data.stats || []).map(s => s.type + ':' + s.count).join('|');
        if (lastPollSig && sig !== lastPollSig) {
          loadMemories(true); // silent refresh
        }
        lastPollSig = sig;
      } catch {}
    }, 10000);
  }

  function syncGraphToolbarState() {
    const inferredBtn = document.getElementById('graph-toggle-inferred');
    const labelsBtn = document.getElementById('graph-toggle-labels');
    if (inferredBtn) {
      inferredBtn.classList.toggle('active', graphShowInferred);
      inferredBtn.textContent = graphShowInferred ? 'INFERRED ON' : 'INFERRED OFF';
    }
    if (labelsBtn) {
      labelsBtn.classList.toggle('active', graphShowLabels);
      labelsBtn.textContent = graphShowLabels ? 'LABELS ON' : 'LABELS OFF';
    }
    GRAPH_RELATION_TYPES.forEach((relation) => {
      const btn = document.getElementById('graph-rel-' + relation);
      if (!btn) return;
      const active = graphRelationFilter.has(relation);
      btn.classList.toggle('active', active);
      btn.classList.toggle('off', !active);
    });
  }

  function onGraphSearch(value) {
    graphSearchQuery = String(value || '').trim().toLowerCase();
    if (graphVisible) rerenderGraphFromCache();
  }

  function toggleGraphRelation(relation) {
    if (!GRAPH_RELATION_TYPES.includes(relation)) return;
    if (graphRelationFilter.has(relation)) {
      if (graphRelationFilter.size === 1) return;
      graphRelationFilter.delete(relation);
    } else {
      graphRelationFilter.add(relation);
    }
    syncGraphToolbarState();
    if (graphVisible) rerenderGraphFromCache();
  }

  function updateGraphLegend(nodesCount, explicitCount, inferredVisibleCount, inferredTotal, relationCounts = {}, avgConfidence = null, avgImportance = null, matchCount = null) {
    const legend = document.getElementById('graph-legend');
    if (!legend) return;
    const inferredText = graphShowInferred
      ? \`INFERRED \${inferredVisibleCount}/\${inferredTotal}\`
      : \`INFERRED OFF (\${inferredTotal} AVAIL)\`;
    const relationPriority = ['contradicts', 'supports', 'supersedes', 'causes', 'example_of'];
    const relationText = relationPriority
      .filter((key) => relationCounts[key] > 0)
      .slice(0, 2)
      .map((key) => \`\${key.toUpperCase().replace('_', ' ')} \${relationCounts[key]}\`)
      .join(' · ');
    const avgConfText = avgConfidence === null ? '' : \`<span class="graph-legend-item">AVG CONF \${Math.round(avgConfidence * 100)}%</span>\`;
    const avgImpText = avgImportance === null ? '' : \`<span class="graph-legend-item">AVG IMP \${Math.round(avgImportance * 100)}%</span>\`;
    const matchText = matchCount === null ? '' : \`<span class="graph-legend-item">MATCH \${matchCount}</span>\`;
    legend.innerHTML = \`
      <span class="graph-legend-item">NODES \${nodesCount}</span>
      <span class="graph-legend-item">LINKS \${explicitCount}</span>
      <span class="graph-legend-item">\${inferredText}</span>
      \${relationText ? \`<span class="graph-legend-item">\${relationText}</span>\` : ''}
      \${avgConfText}
      \${avgImpText}
      \${matchText}
    \`;
  }

  function cloneGraphData() {
    return {
      nodes: (lastGraphData.nodes || []).map(n => ({ ...n })),
      edges: (lastGraphData.edges || []).map(e => ({ ...e })),
      inferred_edges: (lastGraphData.inferred_edges || []).map(e => ({ ...e })),
    };
  }

  function rerenderGraphFromCache() {
    const data = cloneGraphData();
    renderGraph(data.nodes, data.edges, data.inferred_edges);
  }

  function toggleGraphInferred() {
    graphShowInferred = !graphShowInferred;
    syncGraphToolbarState();
    if (graphVisible) rerenderGraphFromCache();
  }

  function toggleGraphLabels() {
    graphShowLabels = !graphShowLabels;
    syncGraphToolbarState();
    if (graphVisible) rerenderGraphFromCache();
  }

  function resetGraphView() {
    if (!graphSvgSelection || !graphZoomBehavior) return;
    graphSvgSelection.transition().duration(220).call(graphZoomBehavior.transform, d3.zoomIdentity);
    graphRelationFilter = new Set(GRAPH_RELATION_TYPES);
    graphSearchQuery = '';
    const searchInput = document.getElementById('graph-search-input');
    if (searchInput) searchInput.value = '';
    syncGraphToolbarState();
    rerenderGraphFromCache();
  }

  async function showGraph() {
    graphVisible = true;
    syncGraphToolbarState();
    ['all','note','fact','journal'].forEach(t => {
      document.getElementById('stat-' + t).classList.remove('active');
    });
    document.getElementById('stat-graph').classList.add('active');
    document.querySelector('.grid-wrap').style.display = 'none';
    document.getElementById('graph-view').style.display = 'block';
    const emptyEl = document.getElementById('graph-empty');
    if (emptyEl) emptyEl.style.display = 'none';
    const legendEl = document.getElementById('graph-legend');
    if (legendEl) legendEl.innerHTML = '';

    const svg = document.getElementById('graph-svg');
    svg.innerHTML = '<text x="50%" y="50%" text-anchor="middle" style="fill:var(--amber);font-family:var(--mono);font-size:0.7rem;letter-spacing:0.15em">LOADING GRAPH...</text>';

    try {
      const r = await apiFetch(BASE + '/api/graph');
      if (r.status === 401) { doLogout(); return; }
      if (!r.ok) throw new Error('failed');
      const data = await r.json();
      lastGraphData = {
        nodes: (data.nodes || []).map(n => ({ ...n })),
        edges: (data.edges || []).map(e => ({ ...e })),
        inferred_edges: (data.inferred_edges || []).map(e => ({ ...e })),
      };
      if (!graphAutoTunedLabels && (lastGraphData.edges.length + lastGraphData.inferred_edges.length) > 80) {
        graphShowLabels = false;
        graphAutoTunedLabels = true;
      }
      syncGraphToolbarState();
      rerenderGraphFromCache();
    } catch(e) {
      document.getElementById('graph-svg').innerHTML = '<text x="50%" y="50%" text-anchor="middle" style="fill:var(--red);font-family:var(--mono);font-size:0.7rem;letter-spacing:0.15em">ERROR LOADING GRAPH</text>';
    }
  }

  function renderGraph(nodes, edges, inferredEdges = []) {
    const svgEl = document.getElementById('graph-svg');
    const emptyEl = document.getElementById('graph-empty');
    svgEl.innerHTML = '';
    if (emptyEl) emptyEl.style.display = 'none';

    if (!nodes.length) {
      const legendEl = document.getElementById('graph-legend');
      if (legendEl) legendEl.innerHTML = '';
      if (emptyEl) { emptyEl.style.display = 'flex'; }
      return;
    }

    const width = svgEl.clientWidth || 800;
    const height = svgEl.clientHeight || 600;
    const isMobile = window.matchMedia('(max-width: 640px)').matches;
    const typeColor = { note: '#00c8b4', fact: '#f0a500', journal: '#8888ff' };
    const relationDistance = {
      related: isMobile ? 88 : 112,
      supports: isMobile ? 94 : 118,
      contradicts: isMobile ? 106 : 132,
      supersedes: isMobile ? 96 : 120,
      causes: isMobile ? 100 : 126,
      example_of: isMobile ? 90 : 114,
    };

    const nodeMap = new Map(nodes.map(n => [n.id, n]));
    const explicitLinks = edges
      .map((e) => {
        const relation = String(e.relation_type || 'related').toLowerCase();
        return { ...e, source: e.from_id, target: e.to_id, kind: 'explicit', relation_type: relation };
      })
      .filter((e) => nodeMap.has(e.source) && nodeMap.has(e.target))
      .filter((e) => graphRelationFilter.has(e.relation_type));
    const inferredCandidates = graphShowInferred
      ? inferredEdges
        .map((e) => ({
          ...e,
          source: e.from_id,
          target: e.to_id,
          kind: 'inferred',
          score: Number.isFinite(Number(e.score)) ? Number(e.score) : 0,
          strength: Number.isFinite(Number(e.strength)) ? Number(e.strength) : 1,
        }))
        .filter((e) => nodeMap.has(e.source) && nodeMap.has(e.target))
      : [];

    inferredCandidates.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return b.strength - a.strength;
    });
    const inferredPerNodeLimit = isMobile ? 3 : 5;
    const inferredMaxVisible = isMobile ? 120 : 220;
    const inferredNodeDegree = new Map();
    const inferredLinks = [];
    for (const edge of inferredCandidates) {
      if (inferredLinks.length >= inferredMaxVisible) break;
      if (edge.strength < 2 && edge.score < 0.85) continue;
      const fromDeg = inferredNodeDegree.get(edge.source) || 0;
      const toDeg = inferredNodeDegree.get(edge.target) || 0;
      if (fromDeg >= inferredPerNodeLimit || toDeg >= inferredPerNodeLimit) continue;
      inferredLinks.push(edge);
      inferredNodeDegree.set(edge.source, fromDeg + 1);
      inferredNodeDegree.set(edge.target, toDeg + 1);
    }
    const links = [...explicitLinks, ...inferredLinks];

    const normalizedSearch = graphSearchQuery.trim().toLowerCase();
    const matchingNodeIds = new Set();
    if (normalizedSearch) {
      nodes.forEach((n) => {
        const haystack = [
          n.title || '',
          n.key || '',
          n.content || '',
          n.tags || '',
          n.source || '',
        ].join(' ').toLowerCase();
        if (haystack.includes(normalizedSearch)) matchingNodeIds.add(n.id);
      });
    }
    const hasSearch = normalizedSearch.length > 0;
    const isNodeVisible = (id) => !hasSearch || matchingNodeIds.has(id);

    const degreeById = new Map();
    links.forEach((l) => {
      degreeById.set(l.source, (degreeById.get(l.source) || 0) + 1);
      degreeById.set(l.target, (degreeById.get(l.target) || 0) + 1);
    });

    const inferredHeavy = inferredLinks.length > explicitLinks.length;
    const simulation = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links).id(d => d.id).distance((d) => {
        if (d.kind === 'inferred') {
          const score = Math.min(Math.max(Number(d.score) || 0, 0), 1);
          const minDist = isMobile ? 92 : 116;
          const maxDist = isMobile ? 130 : 168;
          return maxDist - score * (maxDist - minDist);
        }
        return relationDistance[d.relation_type] ?? (isMobile ? 96 : 120);
      }).strength((d) => {
        if (d.kind === 'inferred') {
          const score = Math.min(Math.max(Number(d.score) || 0, 0), 1);
          return 0.018 + (score * 0.03);
        }
        if (d.relation_type === 'supports') return 0.5;
        if (d.relation_type === 'contradicts') return 0.35;
        if (d.relation_type === 'supersedes') return 0.55;
        if (d.relation_type === 'causes') return 0.45;
        if (d.relation_type === 'example_of') return 0.42;
        return 0.4;
      }))
      .force('charge', d3.forceManyBody().strength(isMobile ? (inferredHeavy ? -300 : -220) : (inferredHeavy ? -420 : -300)))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('x', d3.forceX((d) => {
        if (isMobile) return width / 2;
        const lane = d.type === 'note' ? 1 : (d.type === 'fact' ? 2 : 3);
        return (width / 4) * lane;
      }).strength(isMobile ? 0.01 : 0.035))
      .force('y', d3.forceY(height / 2).strength(isMobile ? 0.01 : 0.03))
      .force('collision', d3.forceCollide(isMobile ? (inferredHeavy ? 27 : 24) : (inferredHeavy ? 34 : 30)));

    const svg = d3.select('#graph-svg');
    graphSvgSelection = svg;
    const defs = svg.append('defs');
    Object.entries(GRAPH_RELATION_COLOR).forEach(([relation, color]) => {
      const markerId = 'arrow-' + relation.replace(/_/g, '-');
      defs.append('marker')
        .attr('id', markerId)
        .attr('viewBox', '0 0 10 10')
        .attr('refX', 13)
        .attr('refY', 5)
        .attr('markerWidth', 7)
        .attr('markerHeight', 7)
        .attr('orient', 'auto-start-reverse')
        .append('path')
        .attr('d', 'M 0 0 L 10 5 L 0 10 z')
        .attr('fill', color);
    });

    const relationCounts = {};
    explicitLinks.forEach((edge) => {
      const key = String(edge.relation_type || 'related');
      relationCounts[key] = (relationCounts[key] || 0) + 1;
    });
    const confidenceVals = nodes.map((n) => Number(n.dynamic_confidence ?? n.confidence)).filter((n) => Number.isFinite(n));
    const importanceVals = nodes.map((n) => Number(n.dynamic_importance ?? n.importance)).filter((n) => Number.isFinite(n));
    const avgConfidence = confidenceVals.length ? confidenceVals.reduce((a, b) => a + b, 0) / confidenceVals.length : null;
    const avgImportance = importanceVals.length ? importanceVals.reduce((a, b) => a + b, 0) / importanceVals.length : null;
    updateGraphLegend(
      nodes.length,
      explicitLinks.length,
      inferredLinks.length,
      inferredEdges.length,
      relationCounts,
      avgConfidence,
      avgImportance,
      hasSearch ? matchingNodeIds.size : null
    );
    const g = svg.append('g');

    const zoom = d3.zoom().scaleExtent([0.2, 4]).on('zoom', (event) => {
      g.attr('transform', event.transform);
    });
    graphZoomBehavior = zoom;
    svg.call(zoom);

    const getEndpointId = (endpoint) => (typeof endpoint === 'string' ? endpoint : (endpoint && endpoint.id ? endpoint.id : ''));
    const linkOpacity = (d) => {
      if (!hasSearch) return d.kind === 'inferred' ? 0.4 : 0.9;
      const sId = getEndpointId(d.source);
      const tId = getEndpointId(d.target);
      const match = matchingNodeIds.has(sId) || matchingNodeIds.has(tId);
      return match ? (d.kind === 'inferred' ? 0.55 : 1) : 0.06;
    };

    const link = g.append('g').selectAll('line')
      .data(links).join('line').attr('class', d => {
        if (d.kind !== 'explicit') return 'graph-link inferred';
        const relationClass = String(d.relation_type || 'related').replace(/_/g, '-').replace(/[^a-z-]/g, '').toLowerCase();
        return \`graph-link explicit relation-\${relationClass}\`;
      })
      .attr('marker-end', (d) => {
        if (d.kind !== 'explicit') return null;
        const relationClass = String(d.relation_type || 'related').replace(/_/g, '-').replace(/[^a-z-]/g, '').toLowerCase();
        return \`url(#arrow-\${relationClass})\`;
      })
      .attr('stroke-width', (d) => {
        if (d.kind !== 'inferred') return 1.5;
        const score = Math.min(Math.max(Number(d.score) || 0, 0), 1);
        return 0.8 + score * 0.7;
      })
      .attr('stroke-opacity', linkOpacity);

    const linkLabel = g.append('g').selectAll('text')
      .data(links).join('text').attr('class', 'graph-link-label')
      .style('display', graphShowLabels ? null : 'none')
      .style('opacity', (d) => linkOpacity(d) >= 0.5 ? 1 : 0)
      .text(d => {
        if (d.kind !== 'explicit') return '';
        if (d.label) return d.label;
        if (d.relation_type && d.relation_type !== 'related') return String(d.relation_type).replace('_', ' ');
        return '';
      });

    const node = g.append('g').selectAll('g')
      .data(nodes).join('g').attr('class', 'graph-node')
      .call(d3.drag()
        .on('start', (event, d) => { if (!event.active) simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
        .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
        .on('end', (event, d) => { if (!event.active) simulation.alphaTarget(0); d.fx = null; d.fy = null; })
      )
      .on('click', (event, d) => { expandById(d.id); });

    node.append('circle')
      .attr('r', d => {
        const degree = degreeById.get(d.id) || 0;
        const base = isMobile ? 8 : 6;
        const maxR = isMobile ? 17 : 15;
        const importance = Math.min(Math.max(Number.isFinite(Number(d.dynamic_importance ?? d.importance)) ? Number(d.dynamic_importance ?? d.importance) : 0.5, 0), 1);
        return Math.min(maxR, base + degree * 0.4 + importance * (isMobile ? 4.2 : 3.6));
      })
      .attr('fill', d => typeColor[d.type] || '#888')
      .attr('fill-opacity', (d) => {
        const confidence = Math.min(Math.max(Number.isFinite(Number(d.dynamic_confidence ?? d.confidence)) ? Number(d.dynamic_confidence ?? d.confidence) : 0.7, 0), 1);
        const visible = isNodeVisible(d.id);
        const baseOpacity = 0.42 + confidence * 0.5;
        return visible ? baseOpacity : Math.max(0.08, baseOpacity * 0.25);
      })
      .attr('stroke', d => typeColor[d.type] || '#888')
      .attr('stroke-opacity', (d) => isNodeVisible(d.id) ? 1 : 0.2)
      .attr('stroke-width', (d) => {
        const importance = Math.min(Math.max(Number.isFinite(Number(d.dynamic_importance ?? d.importance)) ? Number(d.dynamic_importance ?? d.importance) : 0.5, 0), 1);
        return 1.4 + importance * 1.6;
      });

    node.append('text')
      .attr('dx', 12).attr('dy', 4)
      .style('opacity', (d) => isNodeVisible(d.id) ? 1 : 0.2)
      .text(d => (d.title || d.key || d.content || '').slice(0, isMobile ? 18 : 24));

    node.append('title').text((d) => {
      const label = d.title || d.key || (d.content || '').slice(0, 70) || d.id;
      const confidence = Math.round(Math.min(Math.max(Number(d.dynamic_confidence ?? d.confidence) || 0.7, 0), 1) * 100);
      const importance = Math.round(Math.min(Math.max(Number(d.dynamic_importance ?? d.importance) || 0.5, 0), 1) * 100);
      const source = d.source ? \`\\nsource: \${d.source}\` : '';
      return \`\${label}\\nconfidence: \${confidence}%\\nimportance: \${importance}%\${source}\`;
    });

    simulation.on('tick', () => {
      link
        .attr('x1', d => d.source.x).attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
      linkLabel
        .attr('x', d => (d.source.x + d.target.x) / 2)
        .attr('y', d => (d.source.y + d.target.y) / 2);
      node.attr('transform', d => \`translate(\${d.x},\${d.y})\`);
    });
  }

  window.addEventListener('resize', () => {
    clearTimeout(graphResizeTimer);
    graphResizeTimer = setTimeout(() => {
      if (!graphVisible) return;
      rerenderGraphFromCache();
    }, 120);
  });

  syncGraphToolbarState();

  // Enter key on login
  document.getElementById('token-input').addEventListener('keydown', e => { if (e.key === 'Enter') doTokenLogin(); });
  document.getElementById('email-input').addEventListener('keydown', e => { if (e.key === 'Enter') doCredentialAuth('login'); });
  document.getElementById('password-input').addEventListener('keydown', e => { if (e.key === 'Enter') doCredentialAuth('login'); });
  document.getElementById('brain-name-input').addEventListener('keydown', e => { if (e.key === 'Enter') doCredentialAuth('signup'); });
</script>
</body>
</html>`;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS_HEADERS });
    }

    await ensureSchema(env);

    if (url.pathname === '/') {
      return jsonResponse({ name: SERVER_NAME, version: SERVER_VERSION, status: 'ok', tools: TOOLS.length });
    }

    if (url.pathname === '/view') {
      return new Response(viewerHtml(), {
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
      });
    }

    if (url.pathname === '/.well-known/oauth-authorization-server' || url.pathname === '/.well-known/openid-configuration') {
      return handleAuthorizationServerMetadata(url);
    }

    if (url.pathname === '/.well-known/oauth-protected-resource' || url.pathname.startsWith('/.well-known/oauth-protected-resource/')) {
      return handleProtectedResourceMetadata(url);
    }

    if (url.pathname === '/register') {
      return handleOAuthRegister(request, env);
    }

    if (url.pathname === '/authorize') {
      return handleOAuthAuthorize(request, url, env);
    }

    if (url.pathname === '/token') {
      return handleOAuthToken(request, env);
    }

    if (url.pathname === '/auth/signup') {
      if (request.method !== 'POST') return corsJsonResponse({ error: 'Method not allowed' }, 405);
      const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
      if (await isRateLimited(ip, env)) {
        return corsJsonResponse({ error: 'Too many failed attempts. Try again later.' }, 429);
      }
      const response = await handleAuthSignup(request, env);
      if (response.status >= 400) await recordFailedAttempt(ip, env);
      else await clearRateLimit(ip, env);
      return response;
    }

    if (url.pathname === '/auth/login') {
      if (request.method !== 'POST') return corsJsonResponse({ error: 'Method not allowed' }, 405);
      const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
      if (await isRateLimited(ip, env)) {
        return corsJsonResponse({ error: 'Too many failed attempts. Try again later.' }, 429);
      }
      const response = await handleAuthLogin(request, env);
      if (response.status >= 400) await recordFailedAttempt(ip, env);
      else await clearRateLimit(ip, env);
      return response;
    }

    if (url.pathname === '/auth/refresh') {
      if (request.method !== 'POST') return corsJsonResponse({ error: 'Method not allowed' }, 405);
      const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
      if (await isRateLimited(ip, env)) {
        return corsJsonResponse({ error: 'Too many failed attempts. Try again later.' }, 429);
      }
      const response = await handleAuthRefresh(request, env);
      if (response.status >= 400) await recordFailedAttempt(ip, env);
      else await clearRateLimit(ip, env);
      return response;
    }

    if (url.pathname === '/auth/logout') {
      if (request.method !== 'POST') return corsJsonResponse({ error: 'Method not allowed' }, 405);
      return handleAuthLogout(request, env);
    }

    if (url.pathname === '/auth/me') {
      if (request.method !== 'GET') return corsJsonResponse({ error: 'Method not allowed' }, 405);
      const authCtx = await authenticateRequest(request, env);
      if (!authCtx) return unauthorized(url);
      return handleAuthMe(authCtx, env);
    }

    if (url.pathname === '/api/memories') {
      const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
      if (await isRateLimited(ip, env)) {
        return new Response(JSON.stringify({ error: 'Too many failed attempts. Try again later.' }), {
          status: 429,
          headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', 'Retry-After': '900' },
        });
      }
      const authCtx = await authenticateRequest(request, env);
      if (!authCtx) {
        await recordFailedAttempt(ip, env);
        return unauthorized(url);
      }
      await clearRateLimit(ip, env);
      return handleApiMemories(request, env, authCtx.brainId);
    }

    if (url.pathname === '/api/tools') {
      const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
      if (await isRateLimited(ip, env)) {
        return new Response(JSON.stringify({ error: 'Too many failed attempts. Try again later.' }), {
          status: 429,
          headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', 'Retry-After': '900' },
        });
      }
      const authCtx = await authenticateRequest(request, env);
      if (!authCtx) {
        await recordFailedAttempt(ip, env);
        return unauthorized(url);
      }
      await clearRateLimit(ip, env);
      return handleApiTools();
    }

    if (url.pathname === '/mcp') {
      const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
      if (await isRateLimited(ip, env)) {
        return new Response(JSON.stringify({ error: 'Too many failed attempts. Try again later.' }), {
          status: 429,
          headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', 'Retry-After': '900' },
        });
      }
      const authCtx = await authenticateRequest(request, env);
      if (!authCtx) {
        await recordFailedAttempt(ip, env);
        return unauthorized(url);
      }
      await clearRateLimit(ip, env);
      return handleMcp(request, env, url, authCtx);
    }

    // GET /api/links/:id
    if (url.pathname.startsWith('/api/links/')) {
      const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
      if (await isRateLimited(ip, env)) {
        return new Response(JSON.stringify({ error: 'Too many failed attempts. Try again later.' }), {
          status: 429, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', 'Retry-After': '900' },
        });
      }
      const authCtx = await authenticateRequest(request, env);
      if (!authCtx) { await recordFailedAttempt(ip, env); return unauthorized(url); }
      await clearRateLimit(ip, env);
      const memoryId = url.pathname.slice('/api/links/'.length);
      if (!memoryId) return corsJsonResponse({ error: 'Memory ID required' }, 400);
      return handleApiLinks(memoryId, env, authCtx.brainId);
    }

    // GET /api/graph
    if (url.pathname === '/api/graph') {
      const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
      if (await isRateLimited(ip, env)) {
        return new Response(JSON.stringify({ error: 'Too many failed attempts. Try again later.' }), {
          status: 429, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', 'Retry-After': '900' },
        });
      }
      const authCtx = await authenticateRequest(request, env);
      if (!authCtx) { await recordFailedAttempt(ip, env); return unauthorized(url); }
      await clearRateLimit(ip, env);
      return handleApiGraph(env, authCtx.brainId);
    }

    return jsonResponse({ error: 'Not found' }, 404);
  },
};
