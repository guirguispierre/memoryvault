export interface Env {
  DB: D1Database;
  RATE_LIMIT_KV: KVNamespace;
  AUTH_SECRET: string;
  ADMIN_TOKEN: string;
  OAUTH_REDIRECT_DOMAIN_ALLOWLIST?: string;
  AI?: Ai;
  MEMORY_INDEX?: Vectorize;
}

const SERVER_NAME = 'ai-memory-mcp';
const SERVER_VERSION = '1.10.0';
const LEGACY_BRAIN_ID = 'legacy-default-brain';
const LEGACY_USER_ID = 'legacy-token-user';
const LEGACY_USER_EMAIL = 'legacy-token@memoryvault.local';
const ACCESS_TOKEN_TTL_SECONDS = 60 * 60; // 1 hour
const REFRESH_TOKEN_TTL_SECONDS = 60 * 60 * 24 * 30; // 30 days
const AUTH_TOKEN_COOKIE_NAME = 'auth_token';
const REFRESH_TOKEN_COOKIE_NAME = 'refresh_token';
const AUTH_TOKEN_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24; // 24 hours
const AUTH_TOKEN_COOKIE_PATH = '/';
const REFRESH_TOKEN_COOKIE_PATH = '/';
const SESSION_COOKIE_SAME_SITE = 'Lax' as const;
const AUTH_RATE_LIMIT_MAX_ATTEMPTS = 10;
const AUTH_RATE_LIMIT_WINDOW_SECONDS = 60 * 15;
const PBKDF2_ITERATIONS = 100_000;
const EMBEDDING_MODEL = '@cf/baai/bge-base-en-v1.5';
const VECTORIZE_QUERY_TOP_K_MAX = 20;
const VECTORIZE_UPSERT_BATCH_SIZE = 500;
const VECTORIZE_DELETE_BATCH_SIZE = 500;
const VECTORIZE_SETTLE_POLL_INTERVAL_MS = 3000;
const VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS = 180;
const VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS_MAX = 900;
const EMBEDDING_BATCH_SIZE = 16;
const MEMORY_SEARCH_FUSION_K = 60;
const MEMORY_SEARCH_DEFAULT_LIMIT = 20;
const MEMORY_SEARCH_MAX_LIMIT = 20;
const VECTOR_ID_PREFIX = 'm:';
const VECTOR_ID_MAX_MEMORY_ID_LENGTH = 62; // 2-byte prefix + 62-byte id = 64-byte vector id limit.
const DEFAULT_OAUTH_REDIRECT_DOMAIN_ALLOWLIST = ['localhost', '127.0.0.1'] as const;
const TRUSTED_REDIRECT_DOMAINS = ['poke.com', 'claude.ai'] as const;

type MemorySearchMode = 'lexical' | 'semantic' | 'hybrid';
type SemanticMemoryCandidate = {
  memory_id: string;
  score: number;
  rank: number;
};

type VectorSyncStats = {
  upserted: number;
  deleted: number;
  skipped: number;
  mutation_ids: string[];
  probe_vector_id: string | null;
};

type SessionTokens = {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  refresh_expires_in: number;
  session_id: string;
};

type CorsJsonResponseOptions = {
  headers?: HeadersInit;
  cookies?: string[];
};

function generateId(): string {
  return crypto.randomUUID();
}

function now(): number {
  return Math.floor(Date.now() / 1000);
}

function withPrimaryDbEnv(env: Env): Env {
  const dbMaybe = env.DB as unknown as { withSession?: (constraint?: unknown) => unknown };
  if (typeof dbMaybe.withSession !== 'function') return env;
  try {
    const sessionDb = dbMaybe.withSession('first-primary') as D1Database;
    if (!sessionDb || typeof sessionDb.prepare !== 'function') return env;
    return { ...env, DB: sessionDb };
  } catch {
    return env;
  }
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function normalizeResourcePath(pathname: string): string {
  const input = (pathname || '').trim();
  const withLeadingSlash = input ? (input.startsWith('/') ? input : `/${input}`) : '/';
  const normalized = withLeadingSlash.replace(/\/+$/, '') || '/';
  if (normalized === '/') return '/mcp';
  return normalized;
}

function protectedResourceMetadataUrl(url: URL, resourcePath = '/mcp'): string {
  const normalized = normalizeResourcePath(resourcePath);
  return `${url.origin}/.well-known/oauth-protected-resource${normalized}`;
}

function oauthChallengeHeader(url: URL): string {
  return `Bearer realm="mcp", resource_metadata="${protectedResourceMetadataUrl(url, url.pathname)}"`;
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
  clientId: string | null;
};

type AccessTokenPayload = {
  typ: 'access';
  sub: string;
  bid: string;
  sid: string;
  iat: number;
  exp: number;
};

function canMutateMemories(authCtx: AuthContext): boolean {
  if (authCtx.kind === 'legacy') return true;
  return typeof authCtx.userId === 'string' && authCtx.userId.trim().length > 0;
}

function mergeHeaders(target: Headers, source?: HeadersInit): Headers {
  if (!source) return target;
  const headers = new Headers(source);
  headers.forEach((value, key) => target.set(key, value));
  return target;
}

function buildCorsJsonHeaders(options: CorsJsonResponseOptions = {}): Headers {
  const headers = mergeHeaders(new Headers(CORS_HEADERS), options.headers);
  headers.set('Content-Type', 'application/json');
  for (const cookie of options.cookies ?? []) {
    headers.append('Set-Cookie', cookie);
  }
  return headers;
}

function parseRequestCookies(request: Request): Map<string, string> {
  const cookies = new Map<string, string>();
  const cookieHeader = request.headers.get('Cookie');
  if (!cookieHeader) return cookies;
  for (const part of cookieHeader.split(';')) {
    const separator = part.indexOf('=');
    if (separator <= 0) continue;
    const name = part.slice(0, separator).trim();
    const rawValue = part.slice(separator + 1).trim();
    if (!name) continue;
    try {
      cookies.set(name, decodeURIComponent(rawValue));
    } catch {
      cookies.set(name, rawValue);
    }
  }
  return cookies;
}

function getRequestCookie(request: Request, name: string): string | null {
  return parseRequestCookies(request).get(name) ?? null;
}

function serializeCookie(
  name: string,
  value: string,
  options: {
    httpOnly?: boolean;
    secure?: boolean;
    sameSite?: 'Strict' | 'Lax' | 'None';
    maxAge?: number;
    path: string;
  }
): string {
  const parts = [`${name}=${encodeURIComponent(value)}`];
  if (options.httpOnly !== false) parts.push('HttpOnly');
  if (options.secure !== false) parts.push('Secure');
  parts.push(`SameSite=${options.sameSite ?? 'Strict'}`);
  if (typeof options.maxAge === 'number') parts.push(`Max-Age=${Math.max(0, Math.floor(options.maxAge))}`);
  parts.push(`Path=${options.path}`);
  return parts.join('; ');
}

function normalizeCorsJsonResponseOptions(
  options: CorsJsonResponseOptions | Record<string, string>
): CorsJsonResponseOptions {
  const candidate = options as CorsJsonResponseOptions;
  if (Array.isArray(candidate.cookies) || Object.prototype.hasOwnProperty.call(candidate, 'headers')) {
    return candidate;
  }
  return { headers: options as Record<string, string> };
}

function buildSessionCookieHeaders(tokens: SessionTokens): string[] {
  return [
    serializeCookie(AUTH_TOKEN_COOKIE_NAME, tokens.access_token, {
      maxAge: AUTH_TOKEN_COOKIE_MAX_AGE_SECONDS,
      path: AUTH_TOKEN_COOKIE_PATH,
      sameSite: SESSION_COOKIE_SAME_SITE,
    }),
    serializeCookie(REFRESH_TOKEN_COOKIE_NAME, tokens.refresh_token, {
      maxAge: REFRESH_TOKEN_TTL_SECONDS,
      path: REFRESH_TOKEN_COOKIE_PATH,
      sameSite: SESSION_COOKIE_SAME_SITE,
    }),
  ];
}

function clearSessionCookieHeaders(): string[] {
  return [
    serializeCookie(AUTH_TOKEN_COOKIE_NAME, '', {
      maxAge: 0,
      path: AUTH_TOKEN_COOKIE_PATH,
      sameSite: SESSION_COOKIE_SAME_SITE,
    }),
    serializeCookie(REFRESH_TOKEN_COOKIE_NAME, '', {
      maxAge: 0,
      path: REFRESH_TOKEN_COOKIE_PATH,
      sameSite: SESSION_COOKIE_SAME_SITE,
    }),
  ];
}

function buildRotatedSessionCookieHeaders(tokens: SessionTokens): string[] {
  return [...clearSessionCookieHeaders(), ...buildSessionCookieHeaders(tokens)];
}

function parseBearerToken(request: Request): string | null {
  const auth = request.headers.get('Authorization');
  if (!auth) return null;
  const match = auth.match(/^\s*Bearer\s+(.+?)\s*$/i);
  if (!match) return null;
  return match[1] || null;
}

function getAccessTokenFromRequest(request: Request): string | null {
  return parseBearerToken(request) ?? getRequestCookie(request, AUTH_TOKEN_COOKIE_NAME);
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

async function createSessionTokens(userId: string, brainId: string, env: Env, clientId: string | null = null): Promise<{
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
      (id, user_id, brain_id, client_id, refresh_hash, expires_at, created_at, used_at, revoked_at, replaced_by)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)`
  ).bind(sessionId, userId, brainId, clientId, refreshHash, refreshExpiresAt, ts, ts).run();

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

type RefreshSessionRow = {
  id: string;
  user_id: string;
  brain_id: string;
  client_id: string | null;
  expires_at: number;
  revoked_at: number | null;
};

async function getRefreshSessionByToken(refreshToken: string, env: Env): Promise<RefreshSessionRow | null> {
  const refreshHash = await sha256DigestBase64Url(refreshToken);
  return env.DB.prepare(
    `SELECT id, user_id, brain_id, client_id, expires_at, revoked_at
     FROM auth_sessions
     WHERE refresh_hash = ?
     LIMIT 1`
  ).bind(refreshHash).first<RefreshSessionRow>();
}

async function rotateSession(refreshToken: string, env: Env): Promise<{
  userId: string;
  brainId: string;
  clientId: string | null;
  tokens: SessionTokens;
} | null> {
  const ts = now();
  const session = await getRefreshSessionByToken(refreshToken, env);
  if (!session) return null;
  if (session.revoked_at !== null || session.expires_at <= ts) return null;
  const tokens = await createSessionTokens(session.user_id, session.brain_id, env, session.client_id ?? null);
  await env.DB.prepare(
    'UPDATE auth_sessions SET revoked_at = ?, replaced_by = ? WHERE id = ?'
  ).bind(ts, tokens.session_id, session.id).run();
  return { userId: session.user_id, brainId: session.brain_id, clientId: session.client_id ?? null, tokens };
}

async function revokeSession(refreshToken: string, env: Env): Promise<boolean> {
  const refreshHash = await sha256DigestBase64Url(refreshToken);
  const ts = now();
  const result = await env.DB.prepare(
    'UPDATE auth_sessions SET revoked_at = ? WHERE refresh_hash = ? AND revoked_at IS NULL'
  ).bind(ts, refreshHash).run();
  return (result.meta.changes ?? 0) > 0;
}

async function revokeSessionById(sessionId: string, env: Env): Promise<boolean> {
  const ts = now();
  const result = await env.DB.prepare(
    'UPDATE auth_sessions SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL'
  ).bind(ts, sessionId).run();
  return (result.meta.changes ?? 0) > 0;
}

async function authenticateRequest(request: Request, env: Env): Promise<AuthContext | null> {
  const token = getAccessTokenFromRequest(request);
  if (!token) return null;
  if (token === env.AUTH_SECRET) {
    return { kind: 'legacy', brainId: LEGACY_BRAIN_ID, userId: null, sessionId: null, clientId: 'legacy_auth_secret' };
  }

  const authEnv = withPrimaryDbEnv(env);
  const payload = await verifyAccessToken(token, env.AUTH_SECRET);
  if (!payload) return null;
  const ts = now();
  const row = await authEnv.DB.prepare(
    `SELECT s.id, s.user_id, s.brain_id, s.client_id
     FROM auth_sessions s
     JOIN brain_memberships bm ON bm.user_id = s.user_id AND bm.brain_id = s.brain_id
     WHERE s.id = ?
       AND s.user_id = ?
       AND s.brain_id = ?
       AND s.expires_at > ?
       AND s.revoked_at IS NULL
     LIMIT 1`
  ).bind(payload.sid, payload.sub, payload.bid, ts).first<{ id: string; user_id: string; brain_id: string; client_id: string | null }>();
  if (!row) return null;
  if (row.client_id) {
    const client = await purgeOAuthClientIfNotWhitelisted(await getOAuthClient(row.client_id, authEnv), authEnv);
    if (!client) return null;
  }
  await authEnv.DB.prepare('UPDATE auth_sessions SET used_at = ? WHERE id = ?').bind(ts, row.id).run();
  return { kind: 'user', brainId: row.brain_id, userId: row.user_id, sessionId: row.id, clientId: row.client_id ?? null };
}

function authRateLimitPrefix(ip: string): string {
  return `rl:login:${ip || 'unknown'}`;
}

function authRateLimitKey(ip: string): string {
  return `${authRateLimitPrefix(ip)}:${Date.now()}`;
}

async function checkRateLimit(ip: string, kv: KVNamespace): Promise<boolean> {
  await kv.put(authRateLimitKey(ip), '1', { expirationTtl: AUTH_RATE_LIMIT_WINDOW_SECONDS });
  const listed = await kv.list({ prefix: authRateLimitPrefix(ip) });
  return listed.keys.length > AUTH_RATE_LIMIT_MAX_ATTEMPTS;
}

async function resetAuthRateLimit(ip: string, kv: KVNamespace): Promise<void> {
  const listed = await kv.list({ prefix: authRateLimitPrefix(ip) });
  await Promise.all(listed.keys.map((key) => kv.delete(key.name)));
}

async function ensureLegacyTokenPrincipal(env: Env): Promise<{ userId: string; brainId: string }> {
  const ts = now();
  await env.DB.prepare(
    'INSERT OR IGNORE INTO users (id, email, password_hash, display_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)'
  ).bind(
    LEGACY_USER_ID,
    LEGACY_USER_EMAIL,
    'legacy_token_only',
    'Legacy Token User',
    ts,
    ts
  ).run();
  await env.DB.prepare(
    "INSERT OR IGNORE INTO brain_memberships (brain_id, user_id, role, created_at) VALUES (?, ?, 'owner', ?)"
  ).bind(LEGACY_BRAIN_ID, LEGACY_USER_ID, ts).run();
  return { userId: LEGACY_USER_ID, brainId: LEGACY_BRAIN_ID };
}

function normalizeLegacyToken(raw: unknown): string {
  if (typeof raw !== 'string') return '';
  const trimmed = raw.trim();
  if (!trimmed) return '';
  const match = trimmed.match(/^\s*Bearer\s+(.+?)\s*$/i);
  return (match?.[1] ?? trimmed).trim();
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

function isMemorySearchMode(value: unknown): value is MemorySearchMode {
  return value === 'lexical' || value === 'semantic' || value === 'hybrid';
}

function hasSemanticSearchBindings(env: Env): env is Env & { AI: Ai; MEMORY_INDEX: Vectorize } {
  return Boolean(env.AI && env.MEMORY_INDEX);
}

function normalizeSemanticScore(rawScore: number): number {
  if (!Number.isFinite(rawScore)) return 0;
  return clampToRange((rawScore + 1) / 2, 0, 0, 1);
}

function truncateForMetadata(value: string, max = 120): string {
  const trimmed = value.trim();
  if (!trimmed) return '';
  return trimmed.length <= max ? trimmed : trimmed.slice(0, max);
}

function parseTags(tags: unknown): string[] {
  if (typeof tags !== 'string' || !tags.trim()) return [];
  return tags
    .split(',')
    .map((tag) => tag.trim())
    .filter(Boolean);
}

function buildMemoryEmbeddingText(memory: Record<string, unknown>): string {
  const parts: string[] = [];
  if (typeof memory.type === 'string' && memory.type.trim()) parts.push(`type: ${memory.type.trim()}`);
  if (typeof memory.title === 'string' && memory.title.trim()) parts.push(`title: ${memory.title.trim()}`);
  if (typeof memory.key === 'string' && memory.key.trim()) parts.push(`key: ${memory.key.trim()}`);
  if (typeof memory.source === 'string' && memory.source.trim()) parts.push(`source: ${memory.source.trim()}`);
  const tagList = parseTags(memory.tags);
  if (tagList.length) parts.push(`tags: ${tagList.join(', ')}`);
  if (typeof memory.content === 'string' && memory.content.trim()) parts.push(`content: ${memory.content.trim()}`);
  if (!parts.length) return '';
  return parts.join('\n').slice(0, 8000);
}

function extractEmbeddingList(response: unknown): number[][] {
  const payload = response as { data?: unknown };
  if (!Array.isArray(payload?.data)) {
    throw new Error('Embedding response missing data array.');
  }
  const vectors: number[][] = [];
  for (const row of payload.data) {
    if (!Array.isArray(row)) {
      throw new Error('Embedding response contained a non-vector entry.');
    }
    const vector: number[] = [];
    for (const value of row) {
      const num = Number(value);
      if (!Number.isFinite(num)) throw new Error('Embedding response contained a non-numeric value.');
      vector.push(num);
    }
    vectors.push(vector);
  }
  return vectors;
}

async function embedTexts(env: Env, texts: string[]): Promise<number[][]> {
  if (!hasSemanticSearchBindings(env)) return [];
  if (!texts.length) return [];
  const result = await env.AI.run(EMBEDDING_MODEL, { text: texts });
  return extractEmbeddingList(result);
}

async function makeLegacyVectorId(brainId: string, memoryId: string): Promise<string> {
  const digest = await sha256DigestBase64Url(`${brainId}:${memoryId}`);
  return `m_${digest}`;
}

async function makeVectorId(brainId: string, memoryId: string): Promise<string> {
  const normalized = memoryId.trim();
  if (normalized.length > 0 && normalized.length <= VECTOR_ID_MAX_MEMORY_ID_LENGTH) {
    return `${VECTOR_ID_PREFIX}${normalized}`;
  }
  return makeLegacyVectorId(brainId, memoryId);
}

function parseMemoryIdFromVectorId(vectorId: string): string {
  if (!vectorId.startsWith(VECTOR_ID_PREFIX)) return '';
  return vectorId.slice(VECTOR_ID_PREFIX.length).trim();
}

function looksLikeMemoryId(value: string): boolean {
  return /^[a-z0-9-]{16,}$/i.test(value.trim());
}

function buildMemoryVectorMetadata(brainId: string, memory: Record<string, unknown>): Record<string, string | number | boolean> {
  const memoryId = typeof memory.id === 'string' ? memory.id : '';
  const metadata: Record<string, string | number | boolean> = {
    brain_id: brainId,
    memory_id: memoryId,
  };
  if (typeof memory.type === 'string' && memory.type.trim()) metadata.type = truncateForMetadata(memory.type, 32);
  if (typeof memory.key === 'string' && memory.key.trim()) metadata.key = truncateForMetadata(memory.key, 96);
  if (typeof memory.title === 'string' && memory.title.trim()) metadata.title = truncateForMetadata(memory.title, 120);
  if (typeof memory.source === 'string' && memory.source.trim()) metadata.source = truncateForMetadata(memory.source, 120);
  if (typeof memory.tags === 'string' && memory.tags.trim()) metadata.tags = truncateForMetadata(memory.tags, 200);
  if (typeof memory.created_at === 'number' && Number.isFinite(memory.created_at)) metadata.created_at = Math.floor(memory.created_at);
  if (typeof memory.updated_at === 'number' && Number.isFinite(memory.updated_at)) metadata.updated_at = Math.floor(memory.updated_at);
  metadata.archived = memory.archived_at !== null && memory.archived_at !== undefined;
  return metadata;
}

function extractVectorMutationId(result: unknown): string {
  if (!result || typeof result !== 'object' || Array.isArray(result)) return '';
  const value = result as Record<string, unknown>;
  if (typeof value.mutationId === 'string' && value.mutationId.trim()) return value.mutationId.trim();
  if (typeof value.mutation_id === 'string' && value.mutation_id.trim()) return value.mutation_id.trim();
  return '';
}

function sleepMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function readVectorizeProcessedMutation(env: Env): Promise<string> {
  if (!env.MEMORY_INDEX) return '';
  try {
    const details = await env.MEMORY_INDEX.describe() as unknown as {
      processedUpToMutation?: unknown;
      processed_up_to_mutation?: unknown;
    };
    if (typeof details.processedUpToMutation === 'string') return details.processedUpToMutation.trim();
    if (typeof details.processed_up_to_mutation === 'string') return details.processed_up_to_mutation.trim();
    return '';
  } catch (err) {
    console.warn('[semantic-index:describe]', err);
    return '';
  }
}

async function waitForVectorMutationReady(
  env: Env,
  mutationId: string,
  timeoutSeconds: number
): Promise<{ ready: boolean; attempts: number; elapsed_ms: number; processed_up_to_mutation: string | null }> {
  const target = mutationId.trim();
  if (!target) {
    return { ready: false, attempts: 0, elapsed_ms: 0, processed_up_to_mutation: null };
  }
  const timeoutMs = Math.max(1, Math.floor(timeoutSeconds * 1000));
  const startedAt = Date.now();
  let attempts = 0;
  let processed = '';
  while (Date.now() - startedAt <= timeoutMs) {
    attempts += 1;
    processed = await readVectorizeProcessedMutation(env);
    if (processed === target) {
      return { ready: true, attempts, elapsed_ms: Date.now() - startedAt, processed_up_to_mutation: processed || null };
    }
    if (Date.now() - startedAt >= timeoutMs) break;
    await sleepMs(VECTORIZE_SETTLE_POLL_INTERVAL_MS);
  }
  return { ready: false, attempts, elapsed_ms: Date.now() - startedAt, processed_up_to_mutation: processed || null };
}

async function waitForVectorQueryReady(
  env: Env,
  brainId: string,
  vectorId: string,
  timeoutSeconds: number
): Promise<{ ready: boolean; attempts: number; elapsed_ms: number; processed_up_to_mutation: string | null }> {
  const targetVectorId = vectorId.trim();
  if (!targetVectorId) {
    return { ready: false, attempts: 0, elapsed_ms: 0, processed_up_to_mutation: null };
  }
  const indexMaybe = env.MEMORY_INDEX as unknown as {
    queryById?: (vectorId: string, options?: Record<string, unknown>) => Promise<unknown>;
  };
  if (typeof indexMaybe.queryById !== 'function') {
    return { ready: false, attempts: 0, elapsed_ms: 0, processed_up_to_mutation: null };
  }
  const timeoutMs = Math.max(1, Math.floor(timeoutSeconds * 1000));
  const startedAt = Date.now();
  let attempts = 0;
  let processed = '';
  while (Date.now() - startedAt <= timeoutMs) {
    attempts += 1;
    try {
      const queryResult = await indexMaybe.queryById(targetVectorId, {
        topK: 1,
        namespace: brainId,
        returnMetadata: 'none',
        returnValues: false,
      });
      const payload = queryResult as { matches?: unknown[]; results?: unknown[] };
      const matches = Array.isArray(payload.matches)
        ? payload.matches
        : (Array.isArray(payload.results) ? payload.results : []);
      if (matches.length > 0) {
        return {
          ready: true,
          attempts,
          elapsed_ms: Date.now() - startedAt,
          processed_up_to_mutation: processed || null,
        };
      }
    } catch (err) {
      console.warn('[semantic-index:query-by-id]', err);
    }
    processed = await readVectorizeProcessedMutation(env);
    if (Date.now() - startedAt >= timeoutMs) break;
    await sleepMs(VECTORIZE_SETTLE_POLL_INTERVAL_MS);
  }
  return { ready: false, attempts, elapsed_ms: Date.now() - startedAt, processed_up_to_mutation: processed || null };
}

async function deleteMemoryVectors(
  env: Env,
  brainId: string,
  memoryIds: string[]
): Promise<{ deleted: number; mutation_ids: string[] }> {
  if (!hasSemanticSearchBindings(env)) return { deleted: 0, mutation_ids: [] };
  const uniqueIds = Array.from(new Set(memoryIds.map((id) => id.trim()).filter(Boolean)));
  if (!uniqueIds.length) return { deleted: 0, mutation_ids: [] };
  const vectorIdsCurrent = await Promise.all(uniqueIds.map((id) => makeVectorId(brainId, id)));
  const vectorIdsLegacy = await Promise.all(uniqueIds.map((id) => makeLegacyVectorId(brainId, id)));
  const vectorIds = Array.from(new Set([...vectorIdsCurrent, ...vectorIdsLegacy]));
  const mutationIds: string[] = [];
  for (let i = 0; i < vectorIds.length; i += VECTORIZE_DELETE_BATCH_SIZE) {
    const mutation = await env.MEMORY_INDEX.deleteByIds(vectorIds.slice(i, i + VECTORIZE_DELETE_BATCH_SIZE));
    const mutationId = extractVectorMutationId(mutation);
    if (mutationId) mutationIds.push(mutationId);
  }
  return { deleted: uniqueIds.length, mutation_ids: Array.from(new Set(mutationIds)) };
}

async function syncMemoriesToVectorIndex(
  env: Env,
  brainId: string,
  memories: Array<Record<string, unknown>>
): Promise<VectorSyncStats> {
  if (!hasSemanticSearchBindings(env) || !memories.length) {
    return { upserted: 0, deleted: 0, skipped: memories.length, mutation_ids: [], probe_vector_id: null };
  }

  const toDeleteIds: string[] = [];
  const embeddable: Array<{
    memory_id: string;
    text: string;
    metadata: Record<string, string | number | boolean>;
  }> = [];
  let skipped = 0;
  const mutationIds: string[] = [];
  let probeVectorId: string | null = null;

  for (const memory of memories) {
    const memoryId = typeof memory.id === 'string' ? memory.id.trim() : '';
    if (!memoryId) {
      skipped++;
      continue;
    }
    if (memory.archived_at !== null && memory.archived_at !== undefined) {
      toDeleteIds.push(memoryId);
      continue;
    }
    const text = buildMemoryEmbeddingText(memory);
    if (!text) {
      toDeleteIds.push(memoryId);
      continue;
    }
    embeddable.push({
      memory_id: memoryId,
      text,
      metadata: buildMemoryVectorMetadata(brainId, memory),
    });
  }

  let deleted = 0;
  if (toDeleteIds.length) {
    const deleteStats = await deleteMemoryVectors(env, brainId, toDeleteIds);
    deleted = deleteStats.deleted;
    mutationIds.push(...deleteStats.mutation_ids);
  }

  let upserted = 0;
  for (let i = 0; i < embeddable.length; i += EMBEDDING_BATCH_SIZE) {
    const chunk = embeddable.slice(i, i + EMBEDDING_BATCH_SIZE);
    const embeddings = await embedTexts(env, chunk.map((entry) => entry.text));
    if (embeddings.length !== chunk.length) {
      throw new Error(`Embedding count mismatch. Expected ${chunk.length}, got ${embeddings.length}.`);
    }
    const vectors: Array<{ id: string; namespace: string; values: number[]; metadata: Record<string, string | number | boolean> }> = [];
    const legacyIdsToDelete: string[] = [];
    for (let idx = 0; idx < chunk.length; idx++) {
      const entry = chunk[idx];
      const vectorId = await makeVectorId(brainId, entry.memory_id);
      const legacyId = await makeLegacyVectorId(brainId, entry.memory_id);
      if (legacyId !== vectorId) legacyIdsToDelete.push(legacyId);
      vectors.push({
        id: vectorId,
        namespace: brainId,
        values: embeddings[idx],
        metadata: entry.metadata,
      });
    }
    if (legacyIdsToDelete.length) {
      for (let j = 0; j < legacyIdsToDelete.length; j += VECTORIZE_DELETE_BATCH_SIZE) {
        const legacyDeleteMutation = await env.MEMORY_INDEX.deleteByIds(legacyIdsToDelete.slice(j, j + VECTORIZE_DELETE_BATCH_SIZE));
        const legacyDeleteMutationId = extractVectorMutationId(legacyDeleteMutation);
        if (legacyDeleteMutationId) mutationIds.push(legacyDeleteMutationId);
      }
    }
    for (let j = 0; j < vectors.length; j += VECTORIZE_UPSERT_BATCH_SIZE) {
      const upsertChunk = vectors.slice(j, j + VECTORIZE_UPSERT_BATCH_SIZE);
      const upsertMutation = await env.MEMORY_INDEX.upsert(upsertChunk);
      const upsertMutationId = extractVectorMutationId(upsertMutation);
      if (upsertMutationId) mutationIds.push(upsertMutationId);
      const lastVector = upsertChunk[upsertChunk.length - 1];
      if (lastVector?.id) probeVectorId = lastVector.id;
    }
    upserted += vectors.length;
  }

  return {
    upserted,
    deleted,
    skipped,
    mutation_ids: Array.from(new Set(mutationIds)),
    probe_vector_id: probeVectorId,
  };
}

async function safeSyncMemoriesToVectorIndex(
  env: Env,
  brainId: string,
  memories: Array<Record<string, unknown>>,
  operation: string
): Promise<void> {
  if (!hasSemanticSearchBindings(env) || !memories.length) return;
  try {
    await syncMemoriesToVectorIndex(env, brainId, memories);
  } catch (err) {
    console.warn(`[semantic-sync:${operation}]`, err);
  }
}

async function safeDeleteMemoryVectors(env: Env, brainId: string, memoryIds: string[], operation: string): Promise<void> {
  if (!hasSemanticSearchBindings(env) || !memoryIds.length) return;
  try {
    await deleteMemoryVectors(env, brainId, memoryIds);
  } catch (err) {
    console.warn(`[semantic-delete:${operation}]`, err);
  }
}

async function querySemanticMemoryCandidates(
  env: Env,
  brainId: string,
  query: string,
  topK: number,
  minScore: number
): Promise<SemanticMemoryCandidate[]> {
  if (!hasSemanticSearchBindings(env)) return [];
  const [queryEmbedding] = await embedTexts(env, [query.trim()]);
  if (!queryEmbedding) return [];
  const matches = await env.MEMORY_INDEX.query(queryEmbedding, {
    topK: Math.min(Math.max(topK, 1), VECTORIZE_QUERY_TOP_K_MAX),
    namespace: brainId,
    returnMetadata: 'all',
    returnValues: false,
  });
  const matchesAny = matches as unknown as { matches?: unknown[]; results?: unknown[] };
  const matchesArray = Array.isArray(matchesAny.matches)
    ? matchesAny.matches
    : (Array.isArray(matchesAny.results) ? matchesAny.results : []);
  const deduped = new Map<string, SemanticMemoryCandidate>();
  let rank = 0;
  for (const rawMatch of matchesArray) {
    if (!rawMatch || typeof rawMatch !== 'object' || Array.isArray(rawMatch)) continue;
    const match = rawMatch as Record<string, unknown>;
    rank += 1;
    const score = toFiniteNumber(match.score ?? match.similarity ?? match.distance, 0);
    if (score < minScore) continue;
    const vectorId = typeof match.id === 'string'
      ? match.id
      : (typeof match.vectorId === 'string'
        ? match.vectorId
        : (typeof match.vector_id === 'string' ? match.vector_id : ''));
    const fromVectorId = vectorId ? parseMemoryIdFromVectorId(vectorId) : '';
    const metadata = typeof match.metadata === 'object' && match.metadata !== null
      ? match.metadata as Record<string, unknown>
      : (typeof match.meta === 'object' && match.meta !== null
        ? match.meta as Record<string, unknown>
        : null);
    const fromMetadata = metadata && typeof metadata.memory_id === 'string'
      ? metadata.memory_id.trim()
      : (metadata && typeof metadata.memoryId === 'string' ? metadata.memoryId.trim() : '');
    const fromRawVectorId = fromVectorId
      ? fromVectorId
      : (vectorId && looksLikeMemoryId(vectorId) ? vectorId.trim() : '');
    const memoryId = fromRawVectorId || fromMetadata;
    if (!memoryId) continue;
    const metadataBrainId = metadata && typeof metadata.brain_id === 'string'
      ? metadata.brain_id.trim()
      : (metadata && typeof metadata.brainId === 'string' ? metadata.brainId.trim() : '');
    if (metadataBrainId && metadataBrainId !== brainId) continue;
    const existing = deduped.get(memoryId);
    if (!existing || score > existing.score) {
      deduped.set(memoryId, { memory_id: memoryId, score, rank });
    }
  }
  return Array.from(deduped.values()).sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return a.rank - b.rank;
  });
}

async function loadMemoryRowsByIds(
  env: Env,
  brainId: string,
  ids: string[],
  typeFilter?: MemoryType
): Promise<Record<string, unknown>[]> {
  const requestedIds = ids.map((id) => id.trim()).filter(Boolean);
  if (!requestedIds.length) return [];
  const uniqueIds = Array.from(new Set(requestedIds));
  const placeholders = uniqueIds.map(() => '?').join(', ');
  let sql = `SELECT * FROM memories WHERE brain_id = ? AND archived_at IS NULL`;
  const params: unknown[] = [brainId];
  if (typeFilter) {
    sql += ' AND type = ?';
    params.push(typeFilter);
  }
  sql += ` AND id IN (${placeholders})`;
  params.push(...uniqueIds);
  const rows = await env.DB.prepare(sql).bind(...params).all<Record<string, unknown>>();
  const byId = new Map<string, Record<string, unknown>>();
  for (const row of rows.results) {
    const rowId = typeof row.id === 'string' ? row.id : '';
    if (rowId) byId.set(rowId, row);
  }
  return requestedIds.map((id) => byId.get(id)).filter((row): row is Record<string, unknown> => Boolean(row));
}

async function runLexicalMemorySearch(
  env: Env,
  brainId: string,
  query: string,
  typeFilter: MemoryType | undefined,
  limit: number
): Promise<Record<string, unknown>[]> {
  const trimmedQuery = query.trim();
  const fields = ['id', 'content', 'title', 'key', 'source', 'tags'];
  const phraseLike = `%${trimmedQuery}%`;
  const searchParams: unknown[] = [];
  let where = `(${fields.map((field) => `${field} LIKE ?`).join(' OR ')})`;
  searchParams.push(...fields.map(() => phraseLike));

  const tokens = Array.from(new Set(
    trimmedQuery
      .toLowerCase()
      .split(/[^a-z0-9]+/g)
      .map((token) => token.trim())
      .filter((token) => token.length >= 3)
  ));
  const meaningfulTokens = tokens.filter((token) => token !== trimmedQuery.toLowerCase());
  if (meaningfulTokens.length) {
    const tokenClauses: string[] = [];
    for (const token of meaningfulTokens) {
      tokenClauses.push(`(${fields.map((field) => `${field} LIKE ?`).join(' OR ')})`);
      const tokenLike = `%${token}%`;
      searchParams.push(...fields.map(() => tokenLike));
    }
    where = `(${where} OR ${tokenClauses.join(' OR ')})`;
  }

  if (typeFilter) {
    const results = await env.DB.prepare(
      `SELECT * FROM memories
       WHERE brain_id = ?
         AND archived_at IS NULL
         AND type = ?
         AND ${where}
       ORDER BY created_at DESC
       LIMIT ?`
    ).bind(brainId, typeFilter, ...searchParams, limit).all<Record<string, unknown>>();
    return results.results;
  }
  const results = await env.DB.prepare(
    `SELECT * FROM memories
     WHERE brain_id = ?
       AND archived_at IS NULL
       AND ${where}
     ORDER BY created_at DESC
     LIMIT ?`
  ).bind(brainId, ...searchParams, limit).all<Record<string, unknown>>();
  return results.results;
}

function fuseSearchRows(
  mode: MemorySearchMode,
  lexicalRows: Record<string, unknown>[],
  semanticRows: Record<string, unknown>[],
  semanticCandidates: SemanticMemoryCandidate[],
  limit: number
): Record<string, unknown>[] {
  const rowById = new Map<string, Record<string, unknown>>();
  const lexicalRank = new Map<string, number>();
  const semanticRank = new Map<string, number>();
  const semanticScore = new Map<string, number>();

  lexicalRows.forEach((row, idx) => {
    const id = typeof row.id === 'string' ? row.id : '';
    if (!id) return;
    rowById.set(id, row);
    lexicalRank.set(id, idx + 1);
  });
  semanticRows.forEach((row, idx) => {
    const id = typeof row.id === 'string' ? row.id : '';
    if (!id) return;
    if (!rowById.has(id)) rowById.set(id, row);
    if (!semanticRank.has(id)) semanticRank.set(id, idx + 1);
  });
  semanticCandidates.forEach((candidate) => {
    semanticScore.set(candidate.memory_id, candidate.score);
  });

  const ids = Array.from(rowById.keys());
  ids.sort((a, b) => {
    const lexA = lexicalRank.has(a) ? 1 / (MEMORY_SEARCH_FUSION_K + (lexicalRank.get(a) ?? 0)) : 0;
    const lexB = lexicalRank.has(b) ? 1 / (MEMORY_SEARCH_FUSION_K + (lexicalRank.get(b) ?? 0)) : 0;
    const semA = semanticRank.has(a) ? 1 / (MEMORY_SEARCH_FUSION_K + (semanticRank.get(a) ?? 0)) : 0;
    const semB = semanticRank.has(b) ? 1 / (MEMORY_SEARCH_FUSION_K + (semanticRank.get(b) ?? 0)) : 0;
    const semScoreA = normalizeSemanticScore(toFiniteNumber(semanticScore.get(a), -1));
    const semScoreB = normalizeSemanticScore(toFiniteNumber(semanticScore.get(b), -1));

    let fusedA = lexA;
    let fusedB = lexB;
    if (mode === 'semantic') {
      fusedA = semA + (semScoreA * 0.25);
      fusedB = semB + (semScoreB * 0.25);
    } else if (mode === 'hybrid') {
      fusedA = (semA * 0.7) + (lexA * 0.3) + (semScoreA * 0.15);
      fusedB = (semB * 0.7) + (lexB * 0.3) + (semScoreB * 0.15);
    }
    if (fusedB !== fusedA) return fusedB - fusedA;

    const rowA = rowById.get(a);
    const rowB = rowById.get(b);
    const updatedA = toFiniteNumber(rowA?.updated_at, toFiniteNumber(rowA?.created_at, 0));
    const updatedB = toFiniteNumber(rowB?.updated_at, toFiniteNumber(rowB?.created_at, 0));
    return updatedB - updatedA;
  });

  return ids.slice(0, limit).map((id) => rowById.get(id)).filter((row): row is Record<string, unknown> => Boolean(row));
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
          client_id TEXT,
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
      await runMigrationStatement(env, "ALTER TABLE auth_sessions ADD COLUMN client_id TEXT");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(user_id, expires_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_auth_sessions_brain ON auth_sessions(brain_id, expires_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_auth_sessions_client ON auth_sessions(client_id, expires_at DESC)");
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
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS brain_source_trust (
          id TEXT PRIMARY KEY,
          brain_id TEXT NOT NULL,
          source_key TEXT NOT NULL,
          trust REAL NOT NULL DEFAULT 0.5,
          notes TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          UNIQUE(brain_id, source_key),
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_source_trust_brain ON brain_source_trust(brain_id, source_key)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS brain_policies (
          brain_id TEXT PRIMARY KEY,
          policy_json TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS brain_snapshots (
          id TEXT PRIMARY KEY,
          brain_id TEXT NOT NULL,
          label TEXT,
          summary TEXT,
          memory_count INTEGER NOT NULL DEFAULT 0,
          link_count INTEGER NOT NULL DEFAULT 0,
          payload_json TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_brain_snapshots_brain_created ON brain_snapshots(brain_id, created_at DESC)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS memory_conflict_resolutions (
          id TEXT PRIMARY KEY,
          brain_id TEXT NOT NULL,
          pair_key TEXT NOT NULL,
          a_id TEXT NOT NULL,
          b_id TEXT NOT NULL,
          status TEXT NOT NULL,
          canonical_id TEXT,
          note TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          UNIQUE(brain_id, pair_key),
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_conflict_resolutions_brain_status ON memory_conflict_resolutions(brain_id, status, updated_at DESC)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS memory_entity_aliases (
          id TEXT PRIMARY KEY,
          brain_id TEXT NOT NULL,
          canonical_memory_id TEXT NOT NULL,
          alias_memory_id TEXT NOT NULL,
          note TEXT,
          confidence REAL NOT NULL DEFAULT 0.9,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          UNIQUE(brain_id, alias_memory_id),
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_entity_aliases_brain_canonical ON memory_entity_aliases(brain_id, canonical_memory_id)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS memory_watches (
          id TEXT PRIMARY KEY,
          brain_id TEXT NOT NULL,
          name TEXT NOT NULL,
          event_types TEXT NOT NULL,
          query TEXT,
          webhook_url TEXT,
          secret TEXT,
          is_active INTEGER NOT NULL DEFAULT 1,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          last_triggered_at INTEGER,
          last_error TEXT,
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_memory_watches_brain_active ON memory_watches(brain_id, is_active, updated_at DESC)");

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

type ScoreComponent = {
  name: string;
  delta: number;
};

type DynamicScoreBreakdown = {
  score_model: string;
  evaluated_at: number;
  memory_type: string;
  source: string | null;
  age_days: number;
  link_stats: LinkStats;
  base_confidence: number;
  base_importance: number;
  raw_confidence: number;
  raw_importance: number;
  dynamic_confidence: number;
  dynamic_importance: number;
  confidence_components: ScoreComponent[];
  importance_components: ScoreComponent[];
  signals: {
    certainty_hits: number;
    hedge_hits: number;
    importance_hits: number;
    source_trust: number | null;
    high_signal_source: boolean;
    low_signal_source: boolean;
    content_length: number;
  };
};

type BrainPolicy = {
  decay_days: number;
  max_inferred_edges: number;
  min_link_suggestion_score: number;
  retention_days: number;
  private_mode: boolean;
  snapshot_retention: number;
  path_max_hops: number;
  subgraph_default_radius: number;
};

const DEFAULT_BRAIN_POLICY: BrainPolicy = {
  decay_days: 30,
  max_inferred_edges: 360,
  min_link_suggestion_score: 0.25,
  retention_days: 3650,
  private_mode: true,
  snapshot_retention: 50,
  path_max_hops: 5,
  subgraph_default_radius: 2,
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

function normalizeSourceKey(raw: string): string {
  return raw.trim().toLowerCase().replace(/\s+/g, ' ').slice(0, 160);
}

function normalizeTag(raw: string): string {
  return raw.trim().toLowerCase();
}

function parseTagSet(raw: unknown): Set<string> {
  if (typeof raw !== 'string' || !raw.trim()) return new Set();
  return new Set(
    raw.split(',')
      .map((tag) => normalizeTag(tag))
      .filter(Boolean)
      .slice(0, 64)
  );
}

function tokenizeText(raw: string, max = 80): string[] {
  const stopWords = new Set([
    'the', 'a', 'an', 'and', 'or', 'to', 'of', 'for', 'in', 'on', 'at', 'is', 'are', 'was', 'were', 'be',
    'with', 'as', 'by', 'it', 'this', 'that', 'from', 'but', 'not', 'if', 'then', 'so', 'we', 'you', 'i',
  ]);
  const cleaned = raw
    .toLowerCase()
    .replace(/[^a-z0-9\s]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!cleaned) return [];
  const out: string[] = [];
  for (const token of cleaned.split(' ')) {
    if (token.length < 2 || stopWords.has(token)) continue;
    out.push(token);
    if (out.length >= max) break;
  }
  return out;
}

function jaccardSimilarity(a: Set<string>, b: Set<string>): number {
  if (!a.size || !b.size) return 0;
  let intersection = 0;
  for (const item of a) {
    if (b.has(item)) intersection++;
  }
  const union = a.size + b.size - intersection;
  if (!union) return 0;
  return intersection / union;
}

function pairKey(a: string, b: string): string {
  return a < b ? `${a}|${b}` : `${b}|${a}`;
}

function parseJsonObject(raw: string | null | undefined): Record<string, unknown> | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
    return parsed as Record<string, unknown>;
  } catch {
    return null;
  }
}

function sanitizePolicyPatch(patch: Record<string, unknown>, base: BrainPolicy): BrainPolicy {
  return {
    decay_days: Math.min(Math.max(Math.floor(toFiniteNumber(patch.decay_days, base.decay_days)), 1), 3650),
    max_inferred_edges: Math.min(Math.max(Math.floor(toFiniteNumber(patch.max_inferred_edges, base.max_inferred_edges)), 20), 5000),
    min_link_suggestion_score: clampToRange(patch.min_link_suggestion_score, base.min_link_suggestion_score, 0, 1),
    retention_days: Math.min(Math.max(Math.floor(toFiniteNumber(patch.retention_days, base.retention_days)), 7), 36500),
    private_mode: typeof patch.private_mode === 'boolean' ? patch.private_mode : base.private_mode,
    snapshot_retention: Math.min(Math.max(Math.floor(toFiniteNumber(patch.snapshot_retention, base.snapshot_retention)), 1), 500),
    path_max_hops: Math.min(Math.max(Math.floor(toFiniteNumber(patch.path_max_hops, base.path_max_hops)), 1), 8),
    subgraph_default_radius: Math.min(Math.max(Math.floor(toFiniteNumber(patch.subgraph_default_radius, base.subgraph_default_radius)), 1), 6),
  };
}

async function loadSourceTrustMap(env: Env, brainId: string): Promise<Map<string, number>> {
  const rows = await env.DB.prepare(
    'SELECT source_key, trust FROM brain_source_trust WHERE brain_id = ?'
  ).bind(brainId).all<{ source_key: string; trust: number }>();
  const out = new Map<string, number>();
  for (const row of rows.results) {
    const sourceKey = typeof row.source_key === 'string' ? normalizeSourceKey(row.source_key) : '';
    if (!sourceKey) continue;
    out.set(sourceKey, clampToRange(row.trust, 0.5));
  }
  return out;
}

async function getBrainPolicy(env: Env, brainId: string): Promise<BrainPolicy> {
  const row = await env.DB.prepare(
    'SELECT policy_json FROM brain_policies WHERE brain_id = ? LIMIT 1'
  ).bind(brainId).first<{ policy_json: string }>();
  const parsed = parseJsonObject(row?.policy_json ?? null);
  if (!parsed) return { ...DEFAULT_BRAIN_POLICY };
  return sanitizePolicyPatch(parsed, DEFAULT_BRAIN_POLICY);
}

async function setBrainPolicy(env: Env, brainId: string, patch: Record<string, unknown>): Promise<BrainPolicy> {
  const existing = await getBrainPolicy(env, brainId);
  const merged = sanitizePolicyPatch({ ...existing, ...patch }, existing);
  const ts = now();
  await env.DB.prepare(
    `INSERT INTO brain_policies (brain_id, policy_json, created_at, updated_at)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(brain_id) DO UPDATE SET policy_json = excluded.policy_json, updated_at = excluded.updated_at`
  ).bind(brainId, stableJson(merged), ts, ts).run();
  return merged;
}

function canonicalizeForJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => canonicalizeForJson(item));
  }
  if (!value || typeof value !== 'object') {
    return value;
  }
  const obj = value as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(obj).sort()) {
    out[key] = canonicalizeForJson(obj[key]);
  }
  return out;
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(canonicalizeForJson(value));
}

function parseSemver(version: string): [number, number, number] | null {
  const trimmed = version.trim();
  const match = /^v?(\d+)\.(\d+)\.(\d+)$/.exec(trimmed);
  if (!match) return null;
  return [Number(match[1]), Number(match[2]), Number(match[3])];
}

function compareSemver(a: string, b: string): number {
  const aParts = parseSemver(a);
  const bParts = parseSemver(b);
  if (!aParts || !bParts) {
    return a.localeCompare(b);
  }
  if (aParts[0] !== bParts[0]) return aParts[0] - bParts[0];
  if (aParts[1] !== bParts[1]) return aParts[1] - bParts[1];
  return aParts[2] - bParts[2];
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

function computeDynamicScoreBreakdown(
  memory: Record<string, unknown>,
  rawStats?: Partial<LinkStats>,
  tsNow = now(),
  sourceTrustOverride?: number | null
): DynamicScoreBreakdown {
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
  const highSignalSource = sourceText
    ? countKeywordHits(sourceText, ['api', 'system', 'log', 'metric', 'official', 'doc', 'test', 'monitor']) > 0
    : false;
  const lowSignalSource = sourceText
    ? countKeywordHits(sourceText, ['rumor', 'guess', 'hearsay', 'vibe', 'idea']) > 0
    : false;
  const contentLength = textBlob.replace(/\s+/g, '').length;

  const sourceBonus = highSignalSource
    ? 0.09
    : sourceText
      ? 0.04
      : 0;
  const sourcePenalty = lowSignalSource ? 0.07 : 0;
  const sourceTrust = sourceTrustOverride === undefined || sourceTrustOverride === null
    ? null
    : clampToRange(sourceTrustOverride, 0.5);
  const sourceTrustConfidenceDelta = sourceTrust === null
    ? 0
    : (sourceTrust - 0.5) * 0.4;
  const sourceTrustImportanceDelta = sourceTrust === null
    ? 0
    : (sourceTrust - 0.5) * 0.14;
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

  const confidenceComponentsRaw: ScoreComponent[] = [
    { name: 'base_confidence', delta: baseConfidence },
    { name: 'source_bonus', delta: sourceBonus },
    { name: 'source_trust_delta', delta: sourceTrustConfidenceDelta },
    { name: 'certainty_signal', delta: certaintySignal },
    { name: 'type_confidence_bias', delta: typeConfidenceBias },
    { name: 'support_signal', delta: supportSignal },
    { name: 'link_signal', delta: linkSignal * 0.35 },
    { name: 'example_signal', delta: exampleSignal * 0.25 },
    { name: 'contradiction_penalty', delta: -contradictionPenalty },
    { name: 'hedge_penalty', delta: -hedgePenalty },
    { name: 'source_penalty', delta: -sourcePenalty },
    { name: 'stale_penalty', delta: -stalePenalty },
  ];
  const importanceComponentsRaw: ScoreComponent[] = [
    { name: 'base_importance', delta: baseImportance },
    { name: 'importance_keyword_signal', delta: importanceKeywordSignal },
    { name: 'source_trust_delta', delta: sourceTrustImportanceDelta },
    { name: 'content_depth_signal', delta: contentDepthSignal },
    { name: 'type_importance_bias', delta: typeImportanceBias },
    { name: 'link_signal', delta: linkSignal },
    { name: 'cause_signal', delta: causeSignal },
    { name: 'example_signal', delta: exampleSignal },
    { name: 'supersede_signal', delta: supersedeSignal },
    { name: 'recency_signal', delta: recencyImportance },
    { name: 'contradiction_penalty', delta: -(contradictionPenalty * 0.25) },
  ];

  const rawConfidence = confidenceComponentsRaw.reduce((sum, c) => sum + c.delta, 0);
  const rawImportance = importanceComponentsRaw.reduce((sum, c) => sum + c.delta, 0);
  const dynamicConfidence = round3(clamp01(rawConfidence));
  const dynamicImportance = round3(clamp01(rawImportance));

  return {
    score_model: 'memoryvault-dynamic-v1',
    evaluated_at: tsNow,
    memory_type: memoryType || 'unknown',
    source: sourceText || null,
    age_days: round3(ageDays),
    link_stats: stats,
    base_confidence: round3(baseConfidence),
    base_importance: round3(baseImportance),
    raw_confidence: round3(rawConfidence),
    raw_importance: round3(rawImportance),
    dynamic_confidence: dynamicConfidence,
    dynamic_importance: dynamicImportance,
    confidence_components: confidenceComponentsRaw.map((c) => ({ name: c.name, delta: round3(c.delta) })),
    importance_components: importanceComponentsRaw.map((c) => ({ name: c.name, delta: round3(c.delta) })),
    signals: {
      certainty_hits: certaintyHits,
      hedge_hits: hedgeHits,
      importance_hits: importanceHits,
      source_trust: sourceTrust,
      high_signal_source: highSignalSource,
      low_signal_source: lowSignalSource,
      content_length: contentLength,
    },
  };
}

function computeDynamicScores(
  memory: Record<string, unknown>,
  rawStats?: Partial<LinkStats>,
  tsNow = now(),
  sourceTrustOverride?: number | null
): Record<string, unknown> {
  const breakdown = computeDynamicScoreBreakdown(memory, rawStats, tsNow, sourceTrustOverride);
  return {
    ...breakdown.link_stats,
    dynamic_confidence: breakdown.dynamic_confidence,
    dynamic_importance: breakdown.dynamic_importance,
  };
}

function enrichMemoryRowsWithDynamics(
  rows: Array<Record<string, unknown>>,
  linkStatsMap: Map<string, LinkStats>,
  tsNow = now(),
  sourceTrustMap?: Map<string, number>
): Array<Record<string, unknown>> {
  return rows.map((row) => {
    const id = typeof row.id === 'string' ? row.id : '';
    const stats = id ? (linkStatsMap.get(id) ?? EMPTY_LINK_STATS) : EMPTY_LINK_STATS;
    const sourceKey = typeof row.source === 'string' ? normalizeSourceKey(row.source) : '';
    const sourceTrust = sourceKey && sourceTrustMap ? sourceTrustMap.get(sourceKey) : undefined;
    return {
      ...row,
      ...computeDynamicScores(row, stats, tsNow, sourceTrust),
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
  const sourceTrustMap = await loadSourceTrustMap(env, brainId);
  return enrichMemoryRowsWithDynamics(rows, linkStatsMap, tsNow, sourceTrustMap).map(projectMemoryForClient);
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

function parseWatchEventTypes(raw: string): string[] {
  const parsed = parseJsonStringArray(raw, []);
  const out: string[] = [];
  for (const item of parsed) {
    const value = item.trim();
    if (!value) continue;
    if (value === '*' || /^[a-z0-9_.:-]{2,64}$/i.test(value)) out.push(value);
  }
  return Array.from(new Set(out));
}

function normalizeWatchEventInput(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  const out: string[] = [];
  for (const item of raw) {
    if (typeof item !== 'string') continue;
    const value = item.trim();
    if (!value) continue;
    if (value === '*' || /^[a-z0-9_.:-]{2,64}$/i.test(value)) out.push(value);
  }
  return Array.from(new Set(out));
}

async function triggerMemoryWatches(
  env: Env,
  params: {
    brain_id: string;
    event_type: string;
    entity_type: string;
    entity_id: string;
    summary: string;
    payload: unknown;
    created_at: number;
  }
): Promise<void> {
  const rows = await env.DB.prepare(
    `SELECT id, event_types, query, webhook_url, secret
     FROM memory_watches
     WHERE brain_id = ? AND is_active = 1
     ORDER BY updated_at DESC
     LIMIT 200`
  ).bind(params.brain_id).all<{
    id: string;
    event_types: string;
    query: string | null;
    webhook_url: string | null;
    secret: string | null;
  }>();

  const haystack = `${params.event_type} ${params.entity_type} ${params.entity_id} ${params.summary} ${stableJson(params.payload)}`
    .toLowerCase();
  for (const row of rows.results) {
    const eventTypes = parseWatchEventTypes(row.event_types);
    if (eventTypes.length && !eventTypes.includes('*') && !eventTypes.includes(params.event_type)) continue;
    const query = typeof row.query === 'string' ? row.query.trim().toLowerCase() : '';
    if (query && !haystack.includes(query)) continue;

    const ts = params.created_at;
    await env.DB.prepare(
      'UPDATE memory_watches SET last_triggered_at = ?, updated_at = ?, last_error = NULL WHERE id = ? AND brain_id = ?'
    ).bind(ts, ts, row.id, params.brain_id).run();

    const webhook = typeof row.webhook_url === 'string' ? row.webhook_url.trim() : '';
    if (!webhook || !(webhook.startsWith('https://') || webhook.startsWith('http://'))) continue;
    try {
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        'X-MemoryVault-Watch-Id': row.id,
      };
      if (row.secret) headers['X-MemoryVault-Watch-Secret'] = row.secret;
      const response = await fetch(webhook, {
        method: 'POST',
        headers,
        body: JSON.stringify({
          watch_id: row.id,
          event_type: params.event_type,
          entity_type: params.entity_type,
          entity_id: params.entity_id,
          summary: params.summary,
          payload: params.payload,
          created_at: params.created_at,
        }),
      });
      if (!response.ok) {
        await env.DB.prepare(
          'UPDATE memory_watches SET last_error = ?, updated_at = ? WHERE id = ? AND brain_id = ?'
        ).bind(`webhook_status_${response.status}`, ts, row.id, params.brain_id).run();
      }
    } catch (err) {
      const message = err instanceof Error ? err.message.slice(0, 300) : 'webhook_error';
      await env.DB.prepare(
        'UPDATE memory_watches SET last_error = ?, updated_at = ? WHERE id = ? AND brain_id = ?'
      ).bind(message, ts, row.id, params.brain_id).run();
    }
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
  const ts = now();
  const payloadJson = payload === undefined ? null : stableJson(payload);
  await env.DB.prepare(
    'INSERT INTO memory_changelog (id, brain_id, event_type, entity_type, entity_id, summary, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
  ).bind(
    generateId(),
    brainId,
    eventType,
    entityType,
    entityId,
    summary,
    payloadJson,
    ts
  ).run();
  try {
    await triggerMemoryWatches(env, {
      brain_id: brainId,
      event_type: eventType,
      entity_type: entityType,
      entity_id: entityId,
      summary,
      payload: payload ?? null,
      created_at: ts,
    });
  } catch {
    // Watch dispatch is best-effort and should never break memory writes.
  }
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
  await safeSyncMemoriesToVectorIndex(env, brainId, [{
    id,
    type: 'note',
    title: 'Autonomous Objectives Network',
    key,
    content: 'Root node for long-term goals and curiosities.',
    tags: 'objective_root,autonomous_objectives,system_node',
    source: 'system',
    confidence: 0.9,
    importance: 0.95,
    archived_at: null,
    created_at: ts,
    updated_at: ts,
  }], 'objective_root_created');
  await logChangelog(env, brainId, 'objective_root_created', 'memory', id, 'Created autonomous objectives root');
  return id;
}

type ToolDefinition = {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
};

type ToolReleaseMeta = {
  introduced_in: string;
  deprecated_in?: string;
  replaced_by?: string;
  notes?: string;
};

type ToolChangelogChange = {
  type: 'added' | 'updated' | 'deprecated' | 'security' | 'fix';
  target: 'tool' | 'endpoint' | 'scoring' | 'auth';
  name: string;
  description: string;
};

type ToolChangelogEntry = {
  id: string;
  version: string;
  released_at: number;
  summary: string;
  changes: ToolChangelogChange[];
};

const TOOL_RELEASE_META: Record<string, ToolReleaseMeta> = {
  memory_graph_stats: {
    introduced_in: '1.8.0',
    notes: 'Graph-level structural metrics, hubs, and topology analytics.',
  },
  memory_neighbors: {
    introduced_in: '1.8.0',
    notes: 'Seeded graph neighborhood extraction around a memory id/query.',
  },
  memory_tag_stats: {
    introduced_in: '1.8.0',
    notes: 'Tag frequency and co-occurrence analytics for knowledge grooming.',
  },
  memory_link: {
    introduced_in: '1.4.0',
    notes: 'Structured memory relationships and graph reasoning.',
  },
  memory_unlink: {
    introduced_in: '1.4.0',
    notes: 'Relationship cleanup and correction.',
  },
  memory_links: {
    introduced_in: '1.4.0',
    notes: 'Neighborhood inspection for a specific memory.',
  },
  memory_changelog: {
    introduced_in: '1.5.0',
    notes: 'Memory-level audit/event stream for agent sync.',
  },
  memory_conflicts: {
    introduced_in: '1.5.0',
    notes: 'Contradiction detection across high-confidence facts.',
  },
  objective_set: {
    introduced_in: '1.5.0',
    notes: 'Autonomous objective node creation and updates.',
  },
  objective_list: {
    introduced_in: '1.5.0',
    notes: 'Objective planning view for long-term goals.',
  },
  tool_manifest: {
    introduced_in: '1.6.0',
    notes: 'Canonical MCP tool registry with definition hashes.',
  },
  tool_changelog: {
    introduced_in: '1.6.0',
    notes: 'Versioned tool/endpoints/scoring change feed.',
  },
  memory_explain_score: {
    introduced_in: '1.6.0',
    notes: 'Explainable confidence/importance scoring breakdown.',
  },
  memory_search: {
    introduced_in: '1.0.0',
    notes: 'Hybrid retrieval upgraded with semantic + lexical fusion in 1.9.0.',
  },
  memory_reindex: {
    introduced_in: '1.9.0',
    notes: 'Backfill/repair semantic vector embeddings from D1 memories with optional readiness waiting.',
  },
  memory_link_suggest: {
    introduced_in: '1.7.0',
    notes: 'Scored relationship suggestions for graph expansion.',
  },
  memory_path_find: {
    introduced_in: '1.7.0',
    notes: 'Path search between memory nodes for reasoning traces.',
  },
  memory_conflict_resolve: {
    introduced_in: '1.7.0',
    notes: 'Conflict resolution state tracking for contradictions.',
  },
  memory_entity_resolve: {
    introduced_in: '1.7.0',
    notes: 'Canonical entity alias mapping and merge support.',
  },
  memory_source_trust_set: {
    introduced_in: '1.7.0',
    notes: 'Set source trust weights that influence dynamic confidence.',
  },
  memory_source_trust_get: {
    introduced_in: '1.7.0',
    notes: 'Inspect source trust map for a brain.',
  },
  brain_policy_set: {
    introduced_in: '1.7.0',
    notes: 'Configure retention, decay, and graph policy defaults.',
  },
  brain_policy_get: {
    introduced_in: '1.7.0',
    notes: 'Read effective brain policy.',
  },
  brain_snapshot_create: {
    introduced_in: '1.7.0',
    notes: 'Create a point-in-time brain snapshot.',
  },
  brain_snapshot_list: {
    introduced_in: '1.7.0',
    notes: 'List saved brain snapshots.',
  },
  brain_snapshot_restore: {
    introduced_in: '1.7.0',
    notes: 'Restore a brain snapshot in merge/replace mode.',
  },
  objective_next_actions: {
    introduced_in: '1.7.0',
    notes: 'Generate ranked next steps from objective nodes.',
  },
  memory_subgraph: {
    introduced_in: '1.7.0',
    notes: 'Focused graph extraction around seed/query.',
  },
  memory_watch: {
    introduced_in: '1.7.0',
    notes: 'Create/list/manage event watches with optional webhooks.',
  },
};

const TOOL_CHANGELOG: ToolChangelogEntry[] = [
  {
    id: 'semantic-1.9.0',
    version: '1.9.0',
    released_at: 1772667243,
    summary: 'Semantic memory retrieval with Cloudflare Vectorize + Workers AI.',
    changes: [
      {
        type: 'updated',
        target: 'tool',
        name: 'memory_search',
        description: 'Added lexical/semantic/hybrid retrieval modes with score fusion and semantic thresholds.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'memory_reindex',
        description: 'Added vector backfill/repair tool for indexing existing D1 memories.',
      },
      {
        type: 'updated',
        target: 'tool',
        name: 'Vector index sync',
        description: 'Memory mutations now keep Vectorize in sync for save/update/delete/archive/restore paths.',
      },
    ],
  },
  {
    id: 'release-1.8.1',
    version: '1.8.1',
    released_at: 1772666466,
    summary: 'Viewer release: settings version/changelog plus stricter write permissions.',
    changes: [
      {
        type: 'updated',
        target: 'auth',
        name: 'Read-only human sessions',
        description: 'Human email/password sessions are now read-only for memory mutations; AI-agent OAuth sessions retain write access.',
      },
      {
        type: 'updated',
        target: 'endpoint',
        name: 'GET /view settings',
        description: 'Added in-settings version badge and in-app changelog modal powered by tool_changelog.',
      },
    ],
  },
  {
    id: 'graph-tools-1.8.0',
    version: '1.8.0',
    released_at: 1772660000,
    summary: 'Graph analytics and UX polish release.',
    changes: [
      {
        type: 'added',
        target: 'tool',
        name: 'memory_graph_stats + memory_neighbors + memory_tag_stats',
        description: 'Added graph topology metrics, seeded neighborhood extraction, and tag analytics tools.',
      },
      {
        type: 'updated',
        target: 'endpoint',
        name: 'GET /view graph UX',
        description: 'Improved graph exploration with neighborhood hover focus and physics pause/resume controls.',
      },
    ],
  },
  {
    id: 'autonomy-1.7.0',
    version: '1.7.0',
    released_at: 1771966600,
    summary: 'Autonomy, policy, and graph intelligence expansion.',
    changes: [
      {
        type: 'added',
        target: 'tool',
        name: 'memory_link_suggest + memory_path_find',
        description: 'Added link suggestion scoring and pathfinding for graph reasoning.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'memory_conflict_resolve + memory_entity_resolve',
        description: 'Added conflict lifecycle management and canonical entity alias resolution.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'memory_source_trust_set/get + brain_policy_set/get',
        description: 'Added trust and policy controls that influence dynamic scoring defaults.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'brain_snapshot_create/list/restore + memory_subgraph + memory_watch + objective_next_actions',
        description: 'Added snapshot lifecycle, focused subgraph extraction, watch subscriptions, and objective action planning.',
      },
    ],
  },
  {
    id: 'tooling-1.6.0',
    version: '1.6.0',
    released_at: 1771963200,
    summary: 'Tool discovery and scoring transparency release.',
    changes: [
      {
        type: 'added',
        target: 'tool',
        name: 'tool_manifest',
        description: 'Introduced canonical tool manifest output with schema/definition hashes and release metadata.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'tool_changelog',
        description: 'Introduced versioned changelog feed for MCP tools, auth updates, and scoring model updates.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'memory_explain_score',
        description: 'Added explainable scoring API for confidence/importance values and contributing factors.',
      },
      {
        type: 'updated',
        target: 'scoring',
        name: 'memory scoring model',
        description: 'Standardized explainable dynamic score model output as memoryvault-dynamic-v1.',
      },
    ],
  },
  {
    id: 'auth-1.5.1',
    version: '1.5.1',
    released_at: 1771933500,
    summary: 'User session governance endpoints.',
    changes: [
      {
        type: 'added',
        target: 'endpoint',
        name: 'GET /auth/sessions',
        description: 'Added per-user session inventory with active/current flags.',
      },
      {
        type: 'added',
        target: 'endpoint',
        name: 'POST /auth/sessions/revoke',
        description: 'Added single-session and bulk session revocation controls.',
      },
    ],
  },
  {
    id: 'oauth-1.5.0',
    version: '1.5.0',
    released_at: 1771888500,
    summary: 'OAuth-first multi-tenant MemoryVault rollout.',
    changes: [
      {
        type: 'added',
        target: 'auth',
        name: 'OAuth authorization code + PKCE',
        description: 'Enabled dynamic registration and OAuth metadata discovery for keyless MCP setup.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'memory_conflicts',
        description: 'Added contradiction detection for high-confidence facts.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'objective_set/objective_list',
        description: 'Added autonomous objective graph nodes for long-term planning.',
      },
    ],
  },
];

function getToolReleaseMeta(toolName: string): ToolReleaseMeta {
  return TOOL_RELEASE_META[toolName] ?? { introduced_in: '1.0.0' };
}

function isToolDeprecated(meta: ToolReleaseMeta): boolean {
  if (!meta.deprecated_in) return false;
  return compareSemver(SERVER_VERSION, meta.deprecated_in) >= 0;
}

const TOOLS: ToolDefinition[] = [
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
    description: 'Search memories with lexical, semantic, or hybrid retrieval across title/key/id/source/content.',
    inputSchema: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Search query' },
        type: { type: 'string', enum: ['note', 'fact', 'journal'], description: 'Optionally filter by type' },
        mode: { type: 'string', enum: ['lexical', 'semantic', 'hybrid'], description: 'Retrieval mode (default: hybrid)' },
        limit: { type: 'number', description: 'Max results (1-20, default 20)' },
        min_score: { type: 'number', description: 'Minimum semantic score threshold for semantic/hybrid modes (default -1)' },
      },
      required: ['query'],
    },
  },
  {
    name: 'memory_reindex',
    description: 'Rebuild semantic vectors for recent memories in the current brain and optionally wait for Vectorize readiness.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: { type: 'number', description: 'Max memories to process (1-2000, default 500)' },
        include_archived: { type: 'boolean', description: 'Also process archived memories (archived rows trigger vector deletion)' },
        wait_for_index: { type: 'boolean', description: 'Wait for Vectorize mutation processing before returning (default true)' },
        wait_timeout_seconds: { type: 'number', description: 'Max wait time when wait_for_index=true (1-900, default 180)' },
      },
      required: [],
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
    name: 'memory_tag_stats',
    description: 'Analyze tag frequency and co-occurrence across active memories.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: { type: 'number', description: 'Max tags returned (default 20, max 100)' },
        min_count: { type: 'number', description: 'Only include tags with at least this many memories (default 2)' },
        include_pairs: { type: 'boolean', description: 'Include top tag-pair co-occurrences (default true)' },
      },
      required: [],
    },
  },
  {
    name: 'tool_manifest',
    description: 'Return canonical MCP tool definitions, schema hashes, and release/deprecation metadata.',
    inputSchema: {
      type: 'object',
      properties: {
        tool: { type: 'string', description: 'Optional tool name filter' },
        include_schema: { type: 'boolean', description: 'Include full input schema in each result (default true)' },
        include_hashes: { type: 'boolean', description: 'Include schema_hash and definition_hash fields (default true)' },
        include_deprecated: { type: 'boolean', description: 'Include deprecated tools (default true)' },
      },
      required: [],
    },
  },
  {
    name: 'tool_changelog',
    description: 'Return versioned tool/auth/scoring changes so agents can detect what is new.',
    inputSchema: {
      type: 'object',
      properties: {
        since_version: { type: 'string', description: 'Only return entries with version greater than this semver' },
        since: { type: 'number', description: 'Only return entries released at/after this unix timestamp (seconds)' },
        limit: { type: 'number', description: 'Max entries to return (default 20, max 100)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_explain_score',
    description: 'Explain why a memory has its current dynamic confidence/importance values.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Memory ID to explain' },
        at: { type: 'number', description: 'Optional evaluation timestamp (unix seconds) for what-if analysis' },
      },
      required: ['id'],
    },
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
        include_resolved: { type: 'boolean', description: 'Include conflicts already marked resolved/dismissed (default false)' },
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
  {
    name: 'objective_next_actions',
    description: 'Generate prioritized next actions from active objective nodes.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: { type: 'number', description: 'Max actions to return (default 12)' },
        include_done: { type: 'boolean', description: 'Include completed objectives (default false)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_link_suggest',
    description: 'Suggest high-value links between memories using tags, lexical overlap, source, and recency signals.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Optional seed memory id' },
        query: { type: 'string', description: 'Optional seed query (id/name/content/source)' },
        limit: { type: 'number', description: 'Max suggestions (default 20)' },
        min_score: { type: 'number', minimum: 0, maximum: 1, description: 'Minimum suggestion score (default from brain policy)' },
        include_existing: { type: 'boolean', description: 'Include already-linked pairs (default false)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_path_find',
    description: 'Find strongest explicit relationship paths between two memories.',
    inputSchema: {
      type: 'object',
      properties: {
        from_id: { type: 'string', description: 'Start memory id' },
        to_id: { type: 'string', description: 'Target memory id' },
        max_hops: { type: 'number', description: 'Maximum path length in hops (default from policy)' },
        limit: { type: 'number', description: 'Max paths to return (default 5)' },
      },
      required: ['from_id', 'to_id'],
    },
  },
  {
    name: 'memory_conflict_resolve',
    description: 'Record or update a contradiction resolution state for a memory pair.',
    inputSchema: {
      type: 'object',
      properties: {
        a_id: { type: 'string', description: 'First memory id in the conflict pair' },
        b_id: { type: 'string', description: 'Second memory id in the conflict pair' },
        status: { type: 'string', enum: ['needs_review', 'resolved', 'superseded', 'dismissed'], description: 'Resolution status' },
        canonical_id: { type: 'string', description: 'Winning/canonical memory id (optional)' },
        note: { type: 'string', description: 'Optional resolver note' },
      },
      required: ['a_id', 'b_id', 'status'],
    },
  },
  {
    name: 'memory_entity_resolve',
    description: 'Resolve entity aliases to a canonical memory and optionally archive aliases.',
    inputSchema: {
      type: 'object',
      properties: {
        mode: { type: 'string', enum: ['resolve', 'lookup', 'list'], description: 'Operation mode (default resolve)' },
        canonical_id: { type: 'string', description: 'Canonical memory id (required for resolve mode)' },
        alias_id: { type: 'string', description: 'Single alias id (for resolve or lookup)' },
        alias_ids: { type: 'array', items: { type: 'string' }, description: 'Alias ids to map to canonical memory' },
        archive_aliases: { type: 'boolean', description: 'Archive alias memories after mapping (default false)' },
        confidence: { type: 'number', minimum: 0, maximum: 1, description: 'Alias mapping confidence (default 0.9)' },
        note: { type: 'string', description: 'Optional note on alias resolution' },
        limit: { type: 'number', description: 'Max rows for list mode (default 100)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_source_trust_set',
    description: 'Set a trust score for a source key to influence dynamic confidence.',
    inputSchema: {
      type: 'object',
      properties: {
        source: { type: 'string', description: 'Source key/name' },
        trust: { type: 'number', minimum: 0, maximum: 1, description: 'Trust score from 0 to 1' },
        notes: { type: 'string', description: 'Optional note about why this trust score is set' },
      },
      required: ['source', 'trust'],
    },
  },
  {
    name: 'memory_source_trust_get',
    description: 'Read source trust values for the brain.',
    inputSchema: {
      type: 'object',
      properties: {
        source: { type: 'string', description: 'Optional source key filter' },
        limit: { type: 'number', description: 'Max rows returned (default 200)' },
      },
      required: [],
    },
  },
  {
    name: 'brain_policy_set',
    description: 'Set brain-level policy defaults for decay, retention, and graph behavior.',
    inputSchema: {
      type: 'object',
      properties: {
        decay_days: { type: 'number', description: 'Default days-before-decay threshold' },
        max_inferred_edges: { type: 'number', description: 'Default inferred graph edge cap' },
        min_link_suggestion_score: { type: 'number', minimum: 0, maximum: 1, description: 'Default minimum link suggestion score' },
        retention_days: { type: 'number', description: 'Default retention window in days' },
        private_mode: { type: 'boolean', description: 'Whether strict private mode is enabled' },
        snapshot_retention: { type: 'number', description: 'Number of snapshots to retain automatically' },
        path_max_hops: { type: 'number', description: 'Default hop limit for path finding' },
        subgraph_default_radius: { type: 'number', description: 'Default BFS radius for subgraph extraction' },
      },
      required: [],
    },
  },
  {
    name: 'brain_policy_get',
    description: 'Get effective brain policy values.',
    inputSchema: { type: 'object', properties: {}, required: [] },
  },
  {
    name: 'brain_snapshot_create',
    description: 'Create a point-in-time snapshot of memories, links, trust map, and policy.',
    inputSchema: {
      type: 'object',
      properties: {
        label: { type: 'string', description: 'Optional snapshot label' },
        summary: { type: 'string', description: 'Optional summary/reason' },
        include_archived: { type: 'boolean', description: 'Include archived memories in the snapshot (default false)' },
      },
      required: [],
    },
  },
  {
    name: 'brain_snapshot_list',
    description: 'List stored snapshots for the current brain.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: { type: 'number', description: 'Max snapshots to return (default 50)' },
      },
      required: [],
    },
  },
  {
    name: 'brain_snapshot_restore',
    description: 'Restore a snapshot in merge or replace mode.',
    inputSchema: {
      type: 'object',
      properties: {
        snapshot_id: { type: 'string', description: 'Snapshot id to restore' },
        mode: { type: 'string', enum: ['replace', 'merge'], description: 'replace wipes existing brain data before import (default merge)' },
        restore_policy: { type: 'boolean', description: 'Restore policy from snapshot payload (default true)' },
        restore_source_trust: { type: 'boolean', description: 'Restore source trust map from snapshot payload (default true)' },
      },
      required: ['snapshot_id'],
    },
  },
  {
    name: 'memory_subgraph',
    description: 'Return a focused subgraph around a seed/query/tag for efficient reasoning.',
    inputSchema: {
      type: 'object',
      properties: {
        seed_id: { type: 'string', description: 'Seed memory id' },
        query: { type: 'string', description: 'Seed query text' },
        tag: { type: 'string', description: 'Optional tag filter for seed selection' },
        radius: { type: 'number', description: 'Hop radius (default from policy)' },
        limit_nodes: { type: 'number', description: 'Max nodes in response (default 120)' },
        include_inferred: { type: 'boolean', description: 'Include inferred shared-tag edges (default true)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_graph_stats',
    description: 'Return structural graph metrics, relation distributions, and top hub memories.',
    inputSchema: {
      type: 'object',
      properties: {
        include_inferred: { type: 'boolean', description: 'Include inferred shared-tag edges (default true)' },
        top_hubs: { type: 'number', description: 'Max hub memories returned (default 12)' },
        top_tags: { type: 'number', description: 'Max top tags returned (default 12)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_neighbors',
    description: 'Get a seeded neighborhood around a memory id/query with hop depth and edge context.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Seed memory id' },
        query: { type: 'string', description: 'Fallback seed query if id is not provided' },
        max_hops: { type: 'number', description: 'Neighborhood depth (default 1, max 4)' },
        limit_nodes: { type: 'number', description: 'Max nodes returned (default 80)' },
        relation_type: { type: 'string', enum: ['related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of'], description: 'Optional explicit relation filter for traversal' },
        include_inferred: { type: 'boolean', description: 'Include inferred shared-tag edges (default true)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_watch',
    description: 'Manage watch subscriptions for changelog events with optional webhook delivery.',
    inputSchema: {
      type: 'object',
      properties: {
        mode: { type: 'string', enum: ['create', 'list', 'delete', 'set_active', 'test'], description: 'Watch operation mode (default list)' },
        id: { type: 'string', description: 'Watch id for delete/set_active/test' },
        name: { type: 'string', description: 'Watch name for create mode' },
        event_types: { type: 'array', items: { type: 'string' }, description: 'Event type filters (for create mode)' },
        query: { type: 'string', description: 'Optional text query filter' },
        webhook_url: { type: 'string', description: 'Optional webhook URL for event delivery' },
        secret: { type: 'string', description: 'Optional webhook secret header value' },
        active: { type: 'boolean', description: 'Desired active state for set_active mode' },
        limit: { type: 'number', description: 'Max watch rows for list mode (default 100)' },
      },
      required: [],
    },
  },
];

const MUTATING_TOOL_NAMES = new Set<string>([
  'memory_save',
  'memory_update',
  'memory_delete',
  'memory_link',
  'memory_unlink',
  'memory_consolidate',
  'memory_forget',
  'memory_reinforce',
  'memory_decay',
  'memory_reindex',
  'objective_set',
  'memory_conflict_resolve',
  'memory_entity_resolve',
  'memory_source_trust_set',
  'brain_policy_set',
  'brain_snapshot_create',
  'brain_snapshot_restore',
  'memory_watch',
]);

function isMutatingTool(toolName: string): boolean {
  return MUTATING_TOOL_NAMES.has(toolName);
}

type MemoryGraphNode = {
  id: string;
  type: string;
  title: string | null;
  key: string | null;
  content: string;
  tags: string | null;
  source: string | null;
  created_at: number;
  updated_at: number;
  confidence: number;
  importance: number;
};

type MemoryGraphLink = {
  id: string;
  from_id: string;
  to_id: string;
  relation_type: RelationType;
  label: string | null;
  inferred?: boolean;
  score?: number;
};

async function loadActiveMemoryNodes(env: Env, brainId: string, limit = 1500): Promise<MemoryGraphNode[]> {
  const rows = await env.DB.prepare(
    `SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance
     FROM memories
     WHERE brain_id = ? AND archived_at IS NULL
     ORDER BY created_at DESC
     LIMIT ?`
  ).bind(brainId, limit).all<MemoryGraphNode>();
  return rows.results;
}

async function loadExplicitMemoryLinks(env: Env, brainId: string, limit = 8000): Promise<MemoryGraphLink[]> {
  const rows = await env.DB.prepare(
    `SELECT id, from_id, to_id, relation_type, label
     FROM memory_links
     WHERE brain_id = ?
     LIMIT ?`
  ).bind(brainId, limit).all<{
    id: string;
    from_id: string;
    to_id: string;
    relation_type: string;
    label: string | null;
  }>();
  return rows.results
    .filter((row) => !!row.from_id && !!row.to_id)
    .map((row) => ({
      id: row.id,
      from_id: row.from_id,
      to_id: row.to_id,
      relation_type: normalizeRelation(row.relation_type),
      label: row.label,
    }));
}

function buildTagInferredLinks(nodes: MemoryGraphNode[], maxEdges = 400): MemoryGraphLink[] {
  const tagToIds = new Map<string, string[]>();
  for (const node of nodes) {
    const tags = parseTagSet(node.tags);
    for (const tag of tags) {
      const ids = tagToIds.get(tag);
      if (ids) ids.push(node.id);
      else tagToIds.set(tag, [node.id]);
    }
  }

  const byPair = new Map<string, { from: string; to: string; score: number; shared: Set<string> }>();
  for (const [tag, idsRaw] of tagToIds) {
    const ids = Array.from(new Set(idsRaw));
    if (ids.length < 2) continue;
    const trimmed = ids.slice(0, 30);
    const weight = 1 / Math.sqrt(trimmed.length);
    for (let i = 0; i < trimmed.length; i++) {
      for (let j = i + 1; j < trimmed.length; j++) {
        const from = trimmed[i] < trimmed[j] ? trimmed[i] : trimmed[j];
        const to = trimmed[i] < trimmed[j] ? trimmed[j] : trimmed[i];
        const key = `${from}|${to}`;
        const existing = byPair.get(key);
        if (existing) {
          existing.score += weight;
          existing.shared.add(tag);
        } else {
          byPair.set(key, { from, to, score: weight, shared: new Set([tag]) });
        }
      }
    }
  }

  return Array.from(byPair.values())
    .map((row) => ({
      id: `inferred-${row.from}-${row.to}`,
      from_id: row.from,
      to_id: row.to,
      relation_type: 'related' as RelationType,
      label: `shared: ${Array.from(row.shared).slice(0, 3).join(', ')}`,
      inferred: true,
      score: round3(row.score),
    }))
    .filter((row) => (row.score ?? 0) >= 0.75)
    .sort((a, b) => toFiniteNumber(b.score, 0) - toFiniteNumber(a.score, 0))
    .slice(0, maxEdges);
}

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
        archived_at: null,
        created_at: ts,
        updated_at: ts,
      };
      await safeSyncMemoriesToVectorIndex(env, brainId, [insertedRow], 'memory_save');
      let sourceTrust: number | undefined;
      if (typeof source === 'string' && source.trim()) {
        const sourceKey = normalizeSourceKey(source);
        const trustRow = await env.DB.prepare(
          'SELECT trust FROM brain_source_trust WHERE brain_id = ? AND source_key = ? LIMIT 1'
        ).bind(brainId, sourceKey).first<{ trust: number }>();
        if (trustRow && Number.isFinite(Number(trustRow.trust))) {
          sourceTrust = clampToRange(trustRow.trust, 0.5);
        }
      }
      const scoredMemory = projectMemoryForClient({
        ...insertedRow,
        ...computeDynamicScores(insertedRow, EMPTY_LINK_STATS, ts, sourceTrust),
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
      const { query, type, mode: rawMode, limit: rawLimit, min_score: rawMinScore } = args as {
        query: unknown;
        type?: unknown;
        mode?: unknown;
        limit?: unknown;
        min_score?: unknown;
      };
      if (typeof query !== 'string' || query.trim() === '') return { content: [{ type: 'text', text: 'query must be a non-empty string.' }] };
      if (type !== undefined && !isValidType(type)) return { content: [{ type: 'text', text: 'Invalid type filter.' }] };
      if (rawMode !== undefined && !isMemorySearchMode(rawMode)) {
        return { content: [{ type: 'text', text: 'mode must be lexical, semantic, or hybrid.' }] };
      }
      const mode: MemorySearchMode = rawMode ?? 'hybrid';
      const limit = Math.min(
        Math.max(Number.isFinite(Number(rawLimit)) ? Math.floor(Number(rawLimit)) : MEMORY_SEARCH_DEFAULT_LIMIT, 1),
        MEMORY_SEARCH_MAX_LIMIT
      );
      const minScore = rawMinScore === undefined
        ? -1
        : Math.min(Math.max(toFiniteNumber(rawMinScore, -1), -1), 1);
      const typeFilter = type as MemoryType | undefined;

      const lexicalFetchLimit = Math.min(Math.max(limit * 3, limit), 60);
      const semanticFetchLimit = Math.min(Math.max(limit * 3, limit), VECTORIZE_QUERY_TOP_K_MAX);
      const lexicalRows = mode === 'semantic'
        ? []
        : await runLexicalMemorySearch(env, brainId, query, typeFilter, lexicalFetchLimit);

      let semanticCandidates: SemanticMemoryCandidate[] = [];
      if (mode !== 'lexical') {
        if (!hasSemanticSearchBindings(env)) {
          if (mode === 'semantic') {
            return { content: [{ type: 'text', text: 'Semantic search unavailable: AI and MEMORY_INDEX bindings are not configured.' }] };
          }
        } else {
          try {
            semanticCandidates = await querySemanticMemoryCandidates(env, brainId, query, semanticFetchLimit, minScore);
          } catch (err) {
            if (mode === 'semantic') {
              const message = err instanceof Error ? err.message : 'Semantic query failed.';
              return { content: [{ type: 'text', text: `Semantic search failed: ${message}` }] };
            }
            console.warn('[memory_search:semantic]', err);
          }
        }
      }

      const semanticRows = semanticCandidates.length
        ? await loadMemoryRowsByIds(env, brainId, semanticCandidates.map((candidate) => candidate.memory_id), typeFilter)
        : [];
      const fusedRows = fuseSearchRows(mode, lexicalRows, semanticRows, semanticCandidates, limit);
      if (!fusedRows.length) return { content: [{ type: 'text', text: 'No memories found.' }] };

      const scored = await enrichAndProjectRows(env, brainId, fusedRows);
      return { content: [{ type: 'text', text: JSON.stringify(scored, null, 2) }] };
    }

    case 'memory_reindex': {
      const {
        limit: rawLimit,
        include_archived: rawIncludeArchived,
        wait_for_index: rawWaitForIndex,
        wait_timeout_seconds: rawWaitTimeoutSeconds,
      } = args as {
        limit?: unknown;
        include_archived?: unknown;
        wait_for_index?: unknown;
        wait_timeout_seconds?: unknown;
      };
      if (rawIncludeArchived !== undefined && typeof rawIncludeArchived !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_archived must be a boolean when provided.' }] };
      }
      if (rawWaitForIndex !== undefined && typeof rawWaitForIndex !== 'boolean') {
        return { content: [{ type: 'text', text: 'wait_for_index must be a boolean when provided.' }] };
      }
      if (rawWaitTimeoutSeconds !== undefined && !Number.isFinite(Number(rawWaitTimeoutSeconds))) {
        return { content: [{ type: 'text', text: 'wait_timeout_seconds must be a finite number when provided.' }] };
      }
      if (!hasSemanticSearchBindings(env)) {
        return { content: [{ type: 'text', text: 'Semantic reindex unavailable: AI and MEMORY_INDEX bindings are not configured.' }] };
      }
      const limit = Math.min(
        Math.max(Number.isFinite(Number(rawLimit)) ? Math.floor(Number(rawLimit)) : 500, 1),
        2000
      );
      const includeArchived = rawIncludeArchived === true;
      const waitForIndex = rawWaitForIndex !== false;
      const waitTimeoutSeconds = Math.min(
        Math.max(
          Number.isFinite(Number(rawWaitTimeoutSeconds))
            ? Math.floor(Number(rawWaitTimeoutSeconds))
            : VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS,
          1
        ),
        VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS_MAX
      );
      let sql = `
        SELECT id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at
        FROM memories
        WHERE brain_id = ?`;
      const params: unknown[] = [brainId];
      if (!includeArchived) {
        sql += ' AND archived_at IS NULL';
      }
      sql += ' ORDER BY updated_at DESC LIMIT ?';
      params.push(limit);
      const rows = await env.DB.prepare(sql).bind(...params).all<Record<string, unknown>>();
      if (!rows.results.length) {
        return { content: [{ type: 'text', text: 'No memories available for reindex.' }] };
      }
      const stats = await syncMemoriesToVectorIndex(env, brainId, rows.results);
      let indexReady: boolean | null = null;
      let waitAttempts = 0;
      let waitElapsedMs = 0;
      let processedUpToMutation: string | null = null;
      const waitedForMutationId = stats.mutation_ids.length ? stats.mutation_ids[stats.mutation_ids.length - 1] : null;
      if (waitForIndex) {
        if (!stats.mutation_ids.length) {
          indexReady = true;
        } else {
          let waitResult = stats.probe_vector_id
            ? await waitForVectorQueryReady(env, brainId, stats.probe_vector_id, waitTimeoutSeconds)
            : await waitForVectorMutationReady(env, waitedForMutationId ?? '', waitTimeoutSeconds);
          if (!waitResult.ready && waitedForMutationId && stats.probe_vector_id) {
            const mutationWait = await waitForVectorMutationReady(env, waitedForMutationId, waitTimeoutSeconds);
            waitResult = mutationWait.ready ? mutationWait : waitResult;
          }
          indexReady = waitResult.ready;
          waitAttempts = waitResult.attempts;
          waitElapsedMs = waitResult.elapsed_ms;
          processedUpToMutation = waitResult.processed_up_to_mutation;
        }
      }
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            processed: rows.results.length,
            include_archived: includeArchived,
            upserted: stats.upserted,
            deleted: stats.deleted,
            skipped: stats.skipped,
            mutation_count: stats.mutation_ids.length,
            probe_vector_id: stats.probe_vector_id,
            wait_for_index: waitForIndex,
            wait_timeout_seconds: waitTimeoutSeconds,
            index_ready: indexReady,
            wait_attempts: waitAttempts,
            wait_elapsed_ms: waitElapsedMs,
            waited_for_mutation_id: waitedForMutationId,
            processed_up_to_mutation: processedUpToMutation,
          }, null, 2),
        }],
      };
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
      await safeSyncMemoriesToVectorIndex(env, brainId, [updated], 'memory_update');
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
      await safeDeleteMemoryVectors(env, brainId, [id], 'memory_delete');
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

    case 'memory_tag_stats': {
      const { limit: rawLimit, min_count: rawMinCount, include_pairs: rawIncludePairs } = args as {
        limit?: unknown;
        min_count?: unknown;
        include_pairs?: unknown;
      };
      if (rawIncludePairs !== undefined && typeof rawIncludePairs !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_pairs must be a boolean when provided.' }] };
      }
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 20, 1), 100);
      const minCount = Math.min(Math.max(Number.isInteger(rawMinCount) ? (rawMinCount as number) : 2, 1), 1000);
      const includePairs = rawIncludePairs !== false;
      const rows = await env.DB.prepare(
        'SELECT id, tags FROM memories WHERE brain_id = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT 5000'
      ).bind(brainId).all<{ id: string; tags: string | null }>();

      const tagCounts = new Map<string, number>();
      const tagMemoryIds = new Map<string, Set<string>>();
      const pairCounts = new Map<string, number>();

      for (const row of rows.results) {
        const memoryId = typeof row.id === 'string' ? row.id : '';
        if (!memoryId) continue;
        const tags = Array.from(parseTagSet(row.tags));
        if (!tags.length) continue;
        for (const tag of tags) {
          tagCounts.set(tag, (tagCounts.get(tag) ?? 0) + 1);
          const ids = tagMemoryIds.get(tag) ?? new Set<string>();
          ids.add(memoryId);
          tagMemoryIds.set(tag, ids);
        }
        if (!includePairs || tags.length < 2) continue;
        const sortedTags = tags.slice(0, 20).sort();
        for (let i = 0; i < sortedTags.length; i++) {
          for (let j = i + 1; j < sortedTags.length; j++) {
            const key = `${sortedTags[i]}|${sortedTags[j]}`;
            pairCounts.set(key, (pairCounts.get(key) ?? 0) + 1);
          }
        }
      }

      const topTags = Array.from(tagCounts.entries())
        .filter(([, count]) => count >= minCount)
        .sort((a, b) => {
          if (b[1] !== a[1]) return b[1] - a[1];
          return a[0].localeCompare(b[0]);
        })
        .slice(0, limit)
        .map(([tag, count]) => ({
          tag,
          count,
          sample_memory_ids: Array.from(tagMemoryIds.get(tag) ?? []).slice(0, 5),
        }));

      const topPairs = includePairs
        ? Array.from(pairCounts.entries())
          .filter(([, count]) => count >= Math.max(2, minCount - 1))
          .sort((a, b) => {
            if (b[1] !== a[1]) return b[1] - a[1];
            return a[0].localeCompare(b[0]);
          })
          .slice(0, Math.min(25, limit))
          .map(([pair, count]) => {
            const [a, b] = pair.split('|');
            return { tag_a: a, tag_b: b, count };
          })
        : [];

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            memory_count: rows.results.length,
            unique_tag_count: tagCounts.size,
            min_count: minCount,
            tags: topTags,
            top_pairs: topPairs,
          }, null, 2),
        }],
      };
    }

    case 'tool_manifest': {
      const { tool: rawTool, include_schema: rawIncludeSchema, include_hashes: rawIncludeHashes, include_deprecated: rawIncludeDeprecated } = args as {
        tool?: unknown;
        include_schema?: unknown;
        include_hashes?: unknown;
        include_deprecated?: unknown;
      };
      if (rawTool !== undefined && typeof rawTool !== 'string') {
        return { content: [{ type: 'text', text: 'tool must be a string when provided.' }] };
      }
      if (rawIncludeSchema !== undefined && typeof rawIncludeSchema !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_schema must be a boolean when provided.' }] };
      }
      if (rawIncludeHashes !== undefined && typeof rawIncludeHashes !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_hashes must be a boolean when provided.' }] };
      }
      if (rawIncludeDeprecated !== undefined && typeof rawIncludeDeprecated !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_deprecated must be a boolean when provided.' }] };
      }

      const toolFilter = typeof rawTool === 'string' ? rawTool.trim() : '';
      const includeSchema = rawIncludeSchema !== false;
      const includeHashes = rawIncludeHashes !== false;
      const includeDeprecated = rawIncludeDeprecated !== false;

      const selected = toolFilter
        ? TOOLS.filter((tool) => tool.name === toolFilter)
        : TOOLS;
      if (toolFilter && !selected.length) {
        return { content: [{ type: 'text', text: `Unknown tool: ${toolFilter}` }] };
      }

      const manifestTools: Array<Record<string, unknown>> = [];
      for (const toolDef of selected) {
        const meta = getToolReleaseMeta(toolDef.name);
        const deprecated = isToolDeprecated(meta);
        if (!includeDeprecated && deprecated) continue;

        const schemaJson = canonicalJson(toolDef.inputSchema);
        const entry: Record<string, unknown> = {
          name: toolDef.name,
          description: toolDef.description,
          introduced_in: meta.introduced_in,
          deprecated: deprecated,
          deprecated_in: meta.deprecated_in ?? null,
          replaced_by: meta.replaced_by ?? null,
          notes: meta.notes ?? null,
        };
        if (includeSchema) entry.input_schema = toolDef.inputSchema;
        if (includeHashes) {
          entry.schema_hash = await sha256DigestBase64Url(schemaJson);
          entry.definition_hash = await sha256DigestBase64Url(`${toolDef.name}\n${toolDef.description}\n${schemaJson}`);
        }
        manifestTools.push(entry);
      }

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            server: { name: SERVER_NAME, version: SERVER_VERSION },
            generated_at: now(),
            hash_algorithm: includeHashes ? 'sha256/base64url' : null,
            requested_tool: toolFilter || null,
            tool_count: manifestTools.length,
            deprecated_count: manifestTools.filter((t) => t.deprecated === true).length,
            tools: manifestTools,
          }, null, 2),
        }],
      };
    }

    case 'tool_changelog': {
      const { since_version: rawSinceVersion, since, limit: rawLimit } = args as {
        since_version?: unknown;
        since?: unknown;
        limit?: unknown;
      };
      if (rawSinceVersion !== undefined && typeof rawSinceVersion !== 'string') {
        return { content: [{ type: 'text', text: 'since_version must be a semver string when provided.' }] };
      }
      const sinceVersion = typeof rawSinceVersion === 'string' ? rawSinceVersion.trim() : '';
      if (sinceVersion && !parseSemver(sinceVersion)) {
        return { content: [{ type: 'text', text: 'since_version must match semver format (for example "1.6.0").' }] };
      }
      let sinceTs: number | null = null;
      if (since !== undefined) {
        const sinceVal = Number(since);
        if (!Number.isFinite(sinceVal) || sinceVal < 0) {
          return { content: [{ type: 'text', text: 'since must be a non-negative unix timestamp.' }] };
        }
        sinceTs = Math.floor(sinceVal);
      }
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 20, 1), 100);

      let entries = [...TOOL_CHANGELOG];
      if (sinceVersion) {
        entries = entries.filter((entry) => compareSemver(entry.version, sinceVersion) > 0);
      }
      if (sinceTs !== null) {
        entries = entries.filter((entry) => entry.released_at >= sinceTs);
      }
      entries.sort((a, b) => {
        if (b.released_at !== a.released_at) return b.released_at - a.released_at;
        return compareSemver(b.version, a.version);
      });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            server: { name: SERVER_NAME, version: SERVER_VERSION },
            latest_version: SERVER_VERSION,
            filter: {
              since_version: sinceVersion || null,
              since: sinceTs,
              limit,
            },
            count: Math.min(entries.length, limit),
            entries: entries.slice(0, limit),
          }, null, 2),
        }],
      };
    }

    case 'memory_explain_score': {
      const { id, at } = args as { id: unknown; at?: unknown };
      if (typeof id !== 'string' || !id.trim()) {
        return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };
      }
      let tsNow = now();
      if (at !== undefined) {
        const atNum = Number(at);
        if (!Number.isFinite(atNum) || atNum < 0) {
          return { content: [{ type: 'text', text: 'at must be a non-negative unix timestamp when provided.' }] };
        }
        tsNow = Math.floor(atNum);
      }

      const row = await env.DB.prepare(
        'SELECT id, type, title, key, content, tags, source, created_at, updated_at, archived_at, confidence, importance FROM memories WHERE brain_id = ? AND id = ? LIMIT 1'
      ).bind(brainId, id.trim()).first<Record<string, unknown>>();
      if (!row) return { content: [{ type: 'text', text: 'Memory not found.' }] };

      const linkStatsMap = await loadLinkStatsMap(env, brainId);
      const stats = linkStatsMap.get(String(row.id ?? '')) ?? EMPTY_LINK_STATS;
      const sourceTrustMap = await loadSourceTrustMap(env, brainId);
      const sourceKey = typeof row.source === 'string' ? normalizeSourceKey(row.source) : '';
      const sourceTrust = sourceKey ? sourceTrustMap.get(sourceKey) : undefined;
      const breakdown = computeDynamicScoreBreakdown(row, stats, tsNow, sourceTrust);
      const memory = projectMemoryForClient({
        ...row,
        ...breakdown.link_stats,
        dynamic_confidence: breakdown.dynamic_confidence,
        dynamic_importance: breakdown.dynamic_importance,
      });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            memory_id: row.id,
            memory,
            explanation: {
              ...breakdown,
              confidence_delta: round3(breakdown.dynamic_confidence - breakdown.base_confidence),
              importance_delta: round3(breakdown.dynamic_importance - breakdown.base_importance),
            },
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
      const sourceTrustMap = await loadSourceTrustMap(env, brainId);
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
        const sourceKey = typeof base.source === 'string' ? normalizeSourceKey(base.source) : '';
        const scored = computeDynamicScores(
          base,
          linkStatsMap.get(String(r.id ?? '')),
          tsNow,
          sourceKey ? sourceTrustMap.get(sourceKey) : undefined
        );
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
      const archivedMemoryIdsForVectors: string[] = [];

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
          archivedMemoryIdsForVectors.push(dupId);

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
        await safeDeleteMemoryVectors(env, brainId, archivedMemoryIdsForVectors, 'memory_consolidate');
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
          await safeDeleteMemoryVectors(env, brainId, [id], 'memory_forget_hard_single');
          return { content: [{ type: 'text', text: JSON.stringify({ mode, deleted: 1, ids: [id] }) }] };
        }
        const ts = now();
        const result = await env.DB.prepare(
          'UPDATE memories SET archived_at = ?, updated_at = ? WHERE brain_id = ? AND id = ? AND archived_at IS NULL'
        ).bind(ts, ts, brainId, id).run();
        if (result.meta.changes === 0) return { content: [{ type: 'text', text: 'Memory not found or already archived.' }] };
        await safeDeleteMemoryVectors(env, brainId, [id], 'memory_forget_soft_single');
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
        await safeDeleteMemoryVectors(env, brainId, ids, 'memory_forget_hard_batch');
        await logChangelog(env, brainId, 'memory_forget_hard', 'memory', ids[0], 'Hard-forgot memories', { count: ids.length, ids });
        return { content: [{ type: 'text', text: JSON.stringify({ mode, deleted: ids.length, ids }, null, 2) }] };
      }

      const ts = now();
      await env.DB.prepare(
        `UPDATE memories SET archived_at = ?, updated_at = ? WHERE brain_id = ? AND id IN (${placeholders})`
      ).bind(ts, ts, brainId, ...ids).run();
      await safeDeleteMemoryVectors(env, brainId, ids, 'memory_forget_soft_batch');
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
      const { min_confidence, limit: rawLimit, include_resolved: rawIncludeResolved } = args as {
        min_confidence?: unknown;
        limit?: unknown;
        include_resolved?: unknown;
      };
      if (rawIncludeResolved !== undefined && typeof rawIncludeResolved !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_resolved must be a boolean when provided.' }] };
      }
      const minConfidence = clampToRange(min_confidence, 0.7);
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 40, 1), 200);
      const includeResolved = rawIncludeResolved === true;

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
          pair_key: key,
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
              pair_key: key,
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

      const keys = Array.from(new Set(conflicts.map((conflict) => String(conflict.pair_key ?? '')).filter(Boolean)));
      const resolutionMap = new Map<string, Record<string, unknown>>();
      if (keys.length) {
        const rows = await env.DB.prepare(
          `SELECT pair_key, status, canonical_id, note, updated_at
           FROM memory_conflict_resolutions
           WHERE brain_id = ? AND pair_key IN (${keys.map(() => '?').join(',')})`
        ).bind(brainId, ...keys).all<Record<string, unknown>>();
        for (const row of rows.results) {
          const key = typeof row.pair_key === 'string' ? row.pair_key : '';
          if (key) resolutionMap.set(key, row);
        }
      }

      const enrichedConflicts = conflicts
        .map((conflict) => {
          const key = typeof conflict.pair_key === 'string' ? conflict.pair_key : '';
          const resolution = key ? resolutionMap.get(key) : undefined;
          return {
            ...conflict,
            resolution_status: resolution?.status ?? null,
            resolution_canonical_id: resolution?.canonical_id ?? null,
            resolution_note: resolution?.note ?? null,
            resolution_updated_at: resolution?.updated_at ?? null,
          };
        })
        .filter((conflict) => {
          if (includeResolved) return true;
          const status = typeof conflict.resolution_status === 'string' ? conflict.resolution_status : '';
          return !(status === 'resolved' || status === 'superseded' || status === 'dismissed');
        });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            min_confidence: minConfidence,
            include_resolved: includeResolved,
            total_conflicts: enrichedConflicts.length,
            conflicts: enrichedConflicts.slice(0, limit),
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
      if (objectiveRow) {
        await safeSyncMemoriesToVectorIndex(env, brainId, [{ ...objectiveRow, archived_at: null }], 'objective_set');
      }
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

    case 'objective_next_actions': {
      const { limit: rawLimit, include_done: rawIncludeDone } = args as { limit?: unknown; include_done?: unknown };
      if (rawIncludeDone !== undefined && typeof rawIncludeDone !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_done must be a boolean when provided.' }] };
      }
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 12, 1), 100);
      const includeDone = rawIncludeDone === true;

      const rows = await env.DB.prepare(
        `SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance
         FROM memories
         WHERE brain_id = ? AND archived_at IS NULL AND tags LIKE ?
         ORDER BY updated_at DESC
         LIMIT 500`
      ).bind(brainId, '%objective_node%').all<Record<string, unknown>>();
      const objectives = await enrichAndProjectRows(env, brainId, rows.results);
      const tsNow = now();

      const actions: Array<Record<string, unknown>> = [];
      for (const objective of objectives) {
        const id = typeof objective.id === 'string' ? objective.id : '';
        if (!id) continue;
        const tags = parseTagSet(objective.tags);
        const status = tags.has('status_done')
          ? 'done'
          : tags.has('status_paused')
            ? 'paused'
            : 'active';
        if (!includeDone && status === 'done') continue;
        if (status === 'paused') continue;
        const kind = tags.has('kind_curiosity') ? 'curiosity' : 'goal';
        const horizon = tags.has('horizon_short')
          ? 'short'
          : tags.has('horizon_medium')
            ? 'medium'
            : 'long';
        const title = typeof objective.title === 'string' && objective.title.trim()
          ? objective.title.trim()
          : (typeof objective.key === 'string' && objective.key.trim() ? objective.key.trim() : id);
        const updatedAt = toFiniteNumber(objective.updated_at, tsNow);
        const ageDays = Math.max(0, (tsNow - updatedAt) / 86400);
        const freshness = ageDays < 3 ? 1 : ageDays < 14 ? 0.75 : ageDays < 45 ? 0.45 : 0.2;
        const importanceScore = clampToRange(objective.dynamic_importance ?? objective.importance, 0.6);
        const urgency = horizon === 'short' ? 0.2 : horizon === 'medium' ? 0.12 : 0.06;
        const actionScore = round3(clamp01((importanceScore * 0.68) + (freshness * 0.22) + urgency));
        const actionText = kind === 'curiosity'
          ? `Run one focused exploration step for "${title}" and capture one concrete finding.`
          : `Advance "${title}" with one concrete deliverable-level action today.`;
        actions.push({
          objective_id: id,
          title,
          kind,
          horizon,
          status,
          action: actionText,
          score: actionScore,
          dynamic_importance: round3(importanceScore),
          last_updated_days_ago: round3(ageDays),
        });
      }

      actions.sort((a, b) => toFiniteNumber(b.score, 0) - toFiniteNumber(a.score, 0));
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            count: Math.min(actions.length, limit),
            actions: actions.slice(0, limit),
          }, null, 2),
        }],
      };
    }

    case 'memory_link_suggest': {
      const { id: rawId, query: rawQuery, limit: rawLimit, min_score: rawMinScore, include_existing: rawIncludeExisting } = args as {
        id?: unknown;
        query?: unknown;
        limit?: unknown;
        min_score?: unknown;
        include_existing?: unknown;
      };
      if (rawId !== undefined && typeof rawId !== 'string') return { content: [{ type: 'text', text: 'id must be a string when provided.' }] };
      if (rawQuery !== undefined && typeof rawQuery !== 'string') return { content: [{ type: 'text', text: 'query must be a string when provided.' }] };
      if (rawIncludeExisting !== undefined && typeof rawIncludeExisting !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_existing must be a boolean when provided.' }] };
      }
      const policy = await getBrainPolicy(env, brainId);
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 20, 1), 120);
      const minScore = clampToRange(rawMinScore, policy.min_link_suggestion_score);
      const includeExisting = rawIncludeExisting === true;

      const nodes = await loadActiveMemoryNodes(env, brainId, 1400);
      if (!nodes.length) return { content: [{ type: 'text', text: 'No memories available.' }] };
      const nodeById = new Map(nodes.map((node) => [node.id, node]));
      const seedIds = new Set<string>();
      if (typeof rawId === 'string' && rawId.trim()) {
        const id = rawId.trim();
        if (!nodeById.has(id)) return { content: [{ type: 'text', text: `Seed memory not found: ${id}` }] };
        seedIds.add(id);
      }
      if (typeof rawQuery === 'string' && rawQuery.trim()) {
        const query = rawQuery.trim().toLowerCase();
        const scoredMatches = nodes.map((node) => {
          const text = `${node.id} ${node.title ?? ''} ${node.key ?? ''} ${node.content} ${node.source ?? ''}`.toLowerCase();
          const exact = text.includes(query) ? 1 : 0;
          const tokenSet = new Set(tokenizeText(text, 120));
          const queryTokens = tokenizeText(query, 24);
          let tokenHits = 0;
          for (const token of queryTokens) if (tokenSet.has(token)) tokenHits++;
          const score = (exact * 0.7) + (queryTokens.length ? (tokenHits / queryTokens.length) * 0.3 : 0);
          return { id: node.id, score };
        })
          .filter((row) => row.score > 0)
          .sort((a, b) => b.score - a.score)
          .slice(0, 8);
        for (const match of scoredMatches) seedIds.add(match.id);
      }
      if (!seedIds.size) {
        for (const node of nodes.slice(0, 3)) seedIds.add(node.id);
      }

      const links = await loadExplicitMemoryLinks(env, brainId, 9000);
      const existingPairs = new Set(links.map((edge) => pairKey(edge.from_id, edge.to_id)));
      const tokenCache = new Map<string, Set<string>>();
      const tagCache = new Map<string, Set<string>>();
      const getTokenSet = (node: MemoryGraphNode): Set<string> => {
        const existing = tokenCache.get(node.id);
        if (existing) return existing;
        const tokens = new Set(tokenizeText(`${node.title ?? ''} ${node.key ?? ''} ${node.content} ${node.source ?? ''}`, 120));
        tokenCache.set(node.id, tokens);
        return tokens;
      };
      const getTagSet = (node: MemoryGraphNode): Set<string> => {
        const existing = tagCache.get(node.id);
        if (existing) return existing;
        const tags = parseTagSet(node.tags);
        tagCache.set(node.id, tags);
        return tags;
      };

      const suggestionsByPair = new Map<string, Record<string, unknown>>();
      for (const seedId of seedIds) {
        const seed = nodeById.get(seedId);
        if (!seed) continue;
        const seedTokens = getTokenSet(seed);
        const seedTags = getTagSet(seed);
        const seedSource = seed.source ? normalizeSourceKey(seed.source) : '';
        for (const candidate of nodes) {
          if (candidate.id === seed.id) continue;
          const key = pairKey(seed.id, candidate.id);
          if (!includeExisting && existingPairs.has(key)) continue;
          const candidateTokens = getTokenSet(candidate);
          const candidateTags = getTagSet(candidate);
          let sharedTagCount = 0;
          const sharedTags: string[] = [];
          for (const tag of seedTags) {
            if (!candidateTags.has(tag)) continue;
            sharedTagCount++;
            if (sharedTags.length < 5) sharedTags.push(tag);
          }
          const tagScore = Math.min(1, sharedTagCount / 3);
          const lexicalScore = jaccardSimilarity(seedTokens, candidateTokens);
          const sourceScore = seedSource && candidate.source && seedSource === normalizeSourceKey(candidate.source) ? 1 : 0;
          const ageDeltaDays = Math.abs(toFiniteNumber(seed.updated_at, 0) - toFiniteNumber(candidate.updated_at, 0)) / 86400;
          const temporalScore = ageDeltaDays < 7 ? 1 : ageDeltaDays < 30 ? 0.65 : ageDeltaDays < 120 ? 0.3 : 0.08;
          const typeScore = seed.type === candidate.type ? 1 : 0.45;
          const score = round3(
            (tagScore * 0.45)
            + (lexicalScore * 0.35)
            + (sourceScore * 0.1)
            + (temporalScore * 0.05)
            + (typeScore * 0.05)
          );
          if (score < minScore) continue;

          const prev = suggestionsByPair.get(key);
          if (prev && toFiniteNumber(prev.score, 0) >= score) continue;
          suggestionsByPair.set(key, {
            from_id: seed.id,
            to_id: candidate.id,
            relation_hint: 'related',
            score,
            reasons: {
              shared_tags: sharedTags,
              lexical_similarity: round3(lexicalScore),
              same_source: sourceScore === 1,
              temporal_score: round3(temporalScore),
              type_score: round3(typeScore),
            },
          });
        }
      }

      const suggestions = Array.from(suggestionsByPair.values())
        .sort((a, b) => toFiniteNumber(b.score, 0) - toFiniteNumber(a.score, 0))
        .slice(0, limit);

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            seed_ids: Array.from(seedIds),
            min_score: minScore,
            count: suggestions.length,
            suggestions,
          }, null, 2),
        }],
      };
    }

    case 'memory_path_find': {
      const { from_id: rawFrom, to_id: rawTo, max_hops: rawMaxHops, limit: rawLimit } = args as {
        from_id: unknown;
        to_id: unknown;
        max_hops?: unknown;
        limit?: unknown;
      };
      if (typeof rawFrom !== 'string' || !rawFrom.trim()) return { content: [{ type: 'text', text: 'from_id must be a non-empty string.' }] };
      if (typeof rawTo !== 'string' || !rawTo.trim()) return { content: [{ type: 'text', text: 'to_id must be a non-empty string.' }] };
      const fromId = rawFrom.trim();
      const toId = rawTo.trim();
      if (fromId === toId) {
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              from_id: fromId,
              to_id: toId,
              count: 1,
              paths: [{ nodes: [fromId], edges: [], hops: 0, avg_score: 1 }],
            }, null, 2),
          }],
        };
      }

      const policy = await getBrainPolicy(env, brainId);
      const maxHops = Math.min(Math.max(Number.isInteger(rawMaxHops) ? (rawMaxHops as number) : policy.path_max_hops, 1), 8);
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 5, 1), 20);

      const fromExists = await env.DB.prepare(
        'SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL LIMIT 1'
      ).bind(brainId, fromId).first<{ id: string }>();
      if (!fromExists?.id) return { content: [{ type: 'text', text: `Memory not found: ${fromId}` }] };
      const toExists = await env.DB.prepare(
        'SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL LIMIT 1'
      ).bind(brainId, toId).first<{ id: string }>();
      if (!toExists?.id) return { content: [{ type: 'text', text: `Memory not found: ${toId}` }] };

      const links = await loadExplicitMemoryLinks(env, brainId, 12000);
      const adjacency = new Map<string, Array<{ id: string; relation_type: RelationType; link_id: string; label: string | null; weight: number }>>();
      for (const link of links) {
        const weight = relationSignalWeight(link.relation_type);
        const fromArr = adjacency.get(link.from_id);
        const fromEdge = { id: link.to_id, relation_type: link.relation_type, link_id: link.id, label: link.label, weight };
        if (fromArr) fromArr.push(fromEdge);
        else adjacency.set(link.from_id, [fromEdge]);
        const toArr = adjacency.get(link.to_id);
        const toEdge = { id: link.from_id, relation_type: link.relation_type, link_id: link.id, label: link.label, weight };
        if (toArr) toArr.push(toEdge);
        else adjacency.set(link.to_id, [toEdge]);
      }

      const paths: Array<Record<string, unknown>> = [];
      const visited = new Set<string>([fromId]);
      let expansions = 0;
      const maxExpansions = 50000;
      const dfs = (
        currentId: string,
        depth: number,
        nodesPath: string[],
        edgesPath: Array<Record<string, unknown>>,
        cumulativeScore: number
      ): void => {
        if (depth >= maxHops || expansions >= maxExpansions) return;
        const neighbors = [...(adjacency.get(currentId) ?? [])]
          .sort((a, b) => b.weight - a.weight)
          .slice(0, 18);
        for (const neighbor of neighbors) {
          if (expansions >= maxExpansions) break;
          if (visited.has(neighbor.id)) continue;
          expansions++;
          visited.add(neighbor.id);
          const nextNodes = [...nodesPath, neighbor.id];
          const nextEdges = [...edgesPath, {
            link_id: neighbor.link_id,
            from_id: currentId,
            to_id: neighbor.id,
            relation_type: neighbor.relation_type,
            label: neighbor.label,
            weight: round3(neighbor.weight),
          }];
          const nextScore = cumulativeScore + neighbor.weight;
          if (neighbor.id === toId) {
            const hops = nextEdges.length;
            const avgScore = hops ? round3(nextScore / hops) : 0;
            paths.push({
              nodes: nextNodes,
              edges: nextEdges,
              hops,
              cumulative_score: round3(nextScore),
              avg_score: avgScore,
            });
          } else {
            dfs(neighbor.id, depth + 1, nextNodes, nextEdges, nextScore);
          }
          visited.delete(neighbor.id);
        }
      };
      dfs(fromId, 0, [fromId], [], 0);

      paths.sort((a, b) => {
        const scoreDelta = toFiniteNumber(b.avg_score, 0) - toFiniteNumber(a.avg_score, 0);
        if (scoreDelta !== 0) return scoreDelta;
        return toFiniteNumber(a.hops, 999) - toFiniteNumber(b.hops, 999);
      });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            from_id: fromId,
            to_id: toId,
            max_hops: maxHops,
            explored_paths: paths.length,
            expansions,
            count: Math.min(paths.length, limit),
            paths: paths.slice(0, limit),
          }, null, 2),
        }],
      };
    }

    case 'memory_conflict_resolve': {
      const { a_id: rawA, b_id: rawB, status: rawStatus, canonical_id: rawCanonical, note: rawNote } = args as {
        a_id: unknown;
        b_id: unknown;
        status: unknown;
        canonical_id?: unknown;
        note?: unknown;
      };
      if (typeof rawA !== 'string' || !rawA.trim()) return { content: [{ type: 'text', text: 'a_id must be a non-empty string.' }] };
      if (typeof rawB !== 'string' || !rawB.trim()) return { content: [{ type: 'text', text: 'b_id must be a non-empty string.' }] };
      if (rawA === rawB) return { content: [{ type: 'text', text: 'a_id and b_id must be different.' }] };
      if (typeof rawStatus !== 'string') return { content: [{ type: 'text', text: 'status is required.' }] };
      const allowed = new Set(['needs_review', 'resolved', 'superseded', 'dismissed']);
      const status = rawStatus.trim();
      if (!allowed.has(status)) return { content: [{ type: 'text', text: 'Invalid status. Use needs_review|resolved|superseded|dismissed.' }] };
      if (rawCanonical !== undefined && typeof rawCanonical !== 'string') return { content: [{ type: 'text', text: 'canonical_id must be a string when provided.' }] };
      if (rawNote !== undefined && typeof rawNote !== 'string') return { content: [{ type: 'text', text: 'note must be a string when provided.' }] };

      const aId = rawA.trim();
      const bId = rawB.trim();
      const aMem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? LIMIT 1').bind(brainId, aId).first<{ id: string }>();
      const bMem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? LIMIT 1').bind(brainId, bId).first<{ id: string }>();
      if (!aMem?.id || !bMem?.id) return { content: [{ type: 'text', text: 'Both conflict memory IDs must exist in this brain.' }] };

      const canonicalId = typeof rawCanonical === 'string' && rawCanonical.trim() ? rawCanonical.trim() : null;
      if (canonicalId && canonicalId !== aId && canonicalId !== bId) {
        return { content: [{ type: 'text', text: 'canonical_id must match either a_id or b_id.' }] };
      }

      const ts = now();
      const key = pairKey(aId, bId);
      await env.DB.prepare(
        `INSERT INTO memory_conflict_resolutions
          (id, brain_id, pair_key, a_id, b_id, status, canonical_id, note, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(brain_id, pair_key)
         DO UPDATE SET status = excluded.status, canonical_id = excluded.canonical_id, note = excluded.note, updated_at = excluded.updated_at`
      ).bind(
        generateId(),
        brainId,
        key,
        aId,
        bId,
        status,
        canonicalId,
        typeof rawNote === 'string' && rawNote.trim() ? rawNote.trim().slice(0, 600) : null,
        ts,
        ts
      ).run();

      if (canonicalId && (status === 'resolved' || status === 'superseded')) {
        const otherId = canonicalId === aId ? bId : aId;
        const existingLink = await env.DB.prepare(
          'SELECT id FROM memory_links WHERE brain_id = ? AND from_id = ? AND to_id = ? LIMIT 1'
        ).bind(brainId, canonicalId, otherId).first<{ id: string }>();
        if (existingLink?.id) {
          await env.DB.prepare(
            'UPDATE memory_links SET relation_type = ?, label = ? WHERE brain_id = ? AND id = ?'
          ).bind('supersedes', 'conflict_resolution', brainId, existingLink.id).run();
        } else {
          await env.DB.prepare(
            'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
          ).bind(generateId(), brainId, canonicalId, otherId, 'supersedes', 'conflict_resolution', ts).run();
        }
      }

      const resolution = await env.DB.prepare(
        'SELECT id, pair_key, a_id, b_id, status, canonical_id, note, created_at, updated_at FROM memory_conflict_resolutions WHERE brain_id = ? AND pair_key = ? LIMIT 1'
      ).bind(brainId, key).first<Record<string, unknown>>();
      await logChangelog(env, brainId, 'memory_conflict_resolved', 'memory_conflict', key, `Conflict marked as ${status}`, {
        a_id: aId,
        b_id: bId,
        status,
        canonical_id: canonicalId,
      });
      return { content: [{ type: 'text', text: JSON.stringify(resolution, null, 2) }] };
    }

    case 'memory_entity_resolve': {
      const { mode: rawMode, canonical_id: rawCanonicalId, alias_id: rawAliasId, alias_ids: rawAliasIds, archive_aliases: rawArchiveAliases, confidence: rawConfidence, note: rawNote, limit: rawLimit } = args as {
        mode?: unknown;
        canonical_id?: unknown;
        alias_id?: unknown;
        alias_ids?: unknown;
        archive_aliases?: unknown;
        confidence?: unknown;
        note?: unknown;
        limit?: unknown;
      };
      if (rawMode !== undefined && typeof rawMode !== 'string') return { content: [{ type: 'text', text: 'mode must be a string when provided.' }] };
      const mode = typeof rawMode === 'string' ? rawMode.trim().toLowerCase() : 'resolve';
      if (!['resolve', 'lookup', 'list'].includes(mode)) return { content: [{ type: 'text', text: 'mode must be resolve|lookup|list.' }] };

      if (mode === 'list') {
        const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 100, 1), 500);
        const rows = await env.DB.prepare(
          `SELECT ea.id, ea.canonical_memory_id, ea.alias_memory_id, ea.note, ea.confidence, ea.created_at, ea.updated_at,
                  c.title AS canonical_title, c.key AS canonical_key, a.title AS alias_title, a.key AS alias_key
           FROM memory_entity_aliases ea
           LEFT JOIN memories c ON c.id = ea.canonical_memory_id
           LEFT JOIN memories a ON a.id = ea.alias_memory_id
           WHERE ea.brain_id = ?
           ORDER BY ea.updated_at DESC
           LIMIT ?`
        ).bind(brainId, limit).all<Record<string, unknown>>();
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({ count: rows.results.length, aliases: rows.results }, null, 2),
          }],
        };
      }

      if (mode === 'lookup') {
        if (typeof rawAliasId !== 'string' || !rawAliasId.trim()) {
          return { content: [{ type: 'text', text: 'alias_id is required for lookup mode.' }] };
        }
        const aliasId = rawAliasId.trim();
        const row = await env.DB.prepare(
          `SELECT ea.id, ea.canonical_memory_id, ea.alias_memory_id, ea.note, ea.confidence, ea.created_at, ea.updated_at,
                  c.title AS canonical_title, c.key AS canonical_key
           FROM memory_entity_aliases ea
           LEFT JOIN memories c ON c.id = ea.canonical_memory_id
           WHERE ea.brain_id = ? AND ea.alias_memory_id = ?
           LIMIT 1`
        ).bind(brainId, aliasId).first<Record<string, unknown>>();
        if (!row) return { content: [{ type: 'text', text: 'No alias mapping found for alias_id.' }] };
        return { content: [{ type: 'text', text: JSON.stringify(row, null, 2) }] };
      }

      if (typeof rawCanonicalId !== 'string' || !rawCanonicalId.trim()) {
        return { content: [{ type: 'text', text: 'canonical_id is required for resolve mode.' }] };
      }
      if (rawAliasId !== undefined && typeof rawAliasId !== 'string') return { content: [{ type: 'text', text: 'alias_id must be a string when provided.' }] };
      if (rawAliasIds !== undefined && (!Array.isArray(rawAliasIds) || rawAliasIds.some((id) => typeof id !== 'string'))) {
        return { content: [{ type: 'text', text: 'alias_ids must be an array of strings when provided.' }] };
      }
      if (rawArchiveAliases !== undefined && typeof rawArchiveAliases !== 'boolean') {
        return { content: [{ type: 'text', text: 'archive_aliases must be a boolean when provided.' }] };
      }
      if (rawNote !== undefined && typeof rawNote !== 'string') return { content: [{ type: 'text', text: 'note must be a string when provided.' }] };

      const canonicalId = rawCanonicalId.trim();
      const canonicalExists = await env.DB.prepare(
        'SELECT id FROM memories WHERE brain_id = ? AND id = ? LIMIT 1'
      ).bind(brainId, canonicalId).first<{ id: string }>();
      if (!canonicalExists?.id) return { content: [{ type: 'text', text: `Canonical memory not found: ${canonicalId}` }] };

      const aliasIds = new Set<string>();
      if (typeof rawAliasId === 'string' && rawAliasId.trim()) aliasIds.add(rawAliasId.trim());
      if (Array.isArray(rawAliasIds)) {
        for (const aliasId of rawAliasIds) {
          const trimmed = aliasId.trim();
          if (trimmed) aliasIds.add(trimmed);
        }
      }
      aliasIds.delete(canonicalId);
      if (!aliasIds.size) return { content: [{ type: 'text', text: 'Provide alias_id or alias_ids for resolve mode.' }] };

      const confidence = clampToRange(rawConfidence, 0.9);
      const archiveAliases = rawArchiveAliases === true;
      const ts = now();
      const mapped: Array<Record<string, unknown>> = [];
      const archivedAliasIds: string[] = [];
      for (const aliasId of aliasIds) {
        const aliasExists = await env.DB.prepare(
          'SELECT id FROM memories WHERE brain_id = ? AND id = ? LIMIT 1'
        ).bind(brainId, aliasId).first<{ id: string }>();
        if (!aliasExists?.id) continue;
        await env.DB.prepare(
          `INSERT INTO memory_entity_aliases
            (id, brain_id, canonical_memory_id, alias_memory_id, note, confidence, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(brain_id, alias_memory_id)
           DO UPDATE SET canonical_memory_id = excluded.canonical_memory_id, note = excluded.note, confidence = excluded.confidence, updated_at = excluded.updated_at`
        ).bind(
          generateId(),
          brainId,
          canonicalId,
          aliasId,
          typeof rawNote === 'string' && rawNote.trim() ? rawNote.trim().slice(0, 600) : null,
          confidence,
          ts,
          ts
        ).run();

        const existingLink = await env.DB.prepare(
          'SELECT id FROM memory_links WHERE brain_id = ? AND from_id = ? AND to_id = ? LIMIT 1'
        ).bind(brainId, canonicalId, aliasId).first<{ id: string }>();
        if (existingLink?.id) {
          await env.DB.prepare(
            'UPDATE memory_links SET relation_type = ?, label = ? WHERE brain_id = ? AND id = ?'
          ).bind('supersedes', 'entity_alias', brainId, existingLink.id).run();
        } else {
          await env.DB.prepare(
            'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
          ).bind(generateId(), brainId, canonicalId, aliasId, 'supersedes', 'entity_alias', ts).run();
        }
        if (archiveAliases) {
          await env.DB.prepare(
            'UPDATE memories SET archived_at = ?, updated_at = ? WHERE brain_id = ? AND id = ? AND archived_at IS NULL'
          ).bind(ts, ts, brainId, aliasId).run();
          archivedAliasIds.push(aliasId);
        }
        mapped.push({ canonical_id: canonicalId, alias_id: aliasId, confidence, archived: archiveAliases });
      }

      if (archivedAliasIds.length) {
        await safeDeleteMemoryVectors(env, brainId, archivedAliasIds, 'memory_entity_resolve_archive_aliases');
      }

      await logChangelog(env, brainId, 'memory_entity_resolved', 'memory_entity', canonicalId, 'Updated entity alias mappings', {
        canonical_id: canonicalId,
        mapped_count: mapped.length,
      });
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            canonical_id: canonicalId,
            mapped_count: mapped.length,
            mappings: mapped,
          }, null, 2),
        }],
      };
    }

    case 'memory_source_trust_set': {
      const { source: rawSource, trust: rawTrust, notes: rawNotes } = args as { source: unknown; trust: unknown; notes?: unknown };
      if (typeof rawSource !== 'string' || !rawSource.trim()) return { content: [{ type: 'text', text: 'source must be a non-empty string.' }] };
      if (rawNotes !== undefined && typeof rawNotes !== 'string') return { content: [{ type: 'text', text: 'notes must be a string when provided.' }] };
      const sourceKey = normalizeSourceKey(rawSource);
      const trust = clampToRange(rawTrust, NaN);
      if (!Number.isFinite(trust)) return { content: [{ type: 'text', text: 'trust must be a number between 0 and 1.' }] };
      const ts = now();
      await env.DB.prepare(
        `INSERT INTO brain_source_trust (id, brain_id, source_key, trust, notes, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(brain_id, source_key)
         DO UPDATE SET trust = excluded.trust, notes = excluded.notes, updated_at = excluded.updated_at`
      ).bind(
        generateId(),
        brainId,
        sourceKey,
        trust,
        typeof rawNotes === 'string' && rawNotes.trim() ? rawNotes.trim().slice(0, 400) : null,
        ts,
        ts
      ).run();
      const row = await env.DB.prepare(
        'SELECT source_key, trust, notes, created_at, updated_at FROM brain_source_trust WHERE brain_id = ? AND source_key = ? LIMIT 1'
      ).bind(brainId, sourceKey).first<Record<string, unknown>>();
      await logChangelog(env, brainId, 'memory_source_trust_set', 'source', sourceKey, 'Updated source trust score', {
        source: sourceKey,
        trust,
      });
      return { content: [{ type: 'text', text: JSON.stringify(row, null, 2) }] };
    }

    case 'memory_source_trust_get': {
      const { source: rawSource, limit: rawLimit } = args as { source?: unknown; limit?: unknown };
      if (rawSource !== undefined && typeof rawSource !== 'string') return { content: [{ type: 'text', text: 'source must be a string when provided.' }] };
      const sourceKey = typeof rawSource === 'string' ? normalizeSourceKey(rawSource) : '';
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 200, 1), 1000);
      if (sourceKey) {
        const row = await env.DB.prepare(
          'SELECT source_key, trust, notes, created_at, updated_at FROM brain_source_trust WHERE brain_id = ? AND source_key = ? LIMIT 1'
        ).bind(brainId, sourceKey).first<Record<string, unknown>>();
        return { content: [{ type: 'text', text: JSON.stringify({ count: row ? 1 : 0, sources: row ? [row] : [] }, null, 2) }] };
      }
      const rows = await env.DB.prepare(
        'SELECT source_key, trust, notes, created_at, updated_at FROM brain_source_trust WHERE brain_id = ? ORDER BY trust DESC, updated_at DESC LIMIT ?'
      ).bind(brainId, limit).all<Record<string, unknown>>();
      return { content: [{ type: 'text', text: JSON.stringify({ count: rows.results.length, sources: rows.results }, null, 2) }] };
    }

    case 'brain_policy_set': {
      const policy = await setBrainPolicy(env, brainId, args);
      await logChangelog(env, brainId, 'brain_policy_set', 'brain_policy', brainId, 'Updated brain policy', policy);
      return { content: [{ type: 'text', text: JSON.stringify({ brain_id: brainId, policy }, null, 2) }] };
    }

    case 'brain_policy_get': {
      const policy = await getBrainPolicy(env, brainId);
      return { content: [{ type: 'text', text: JSON.stringify({ brain_id: brainId, policy }, null, 2) }] };
    }

    case 'brain_snapshot_create': {
      const { label: rawLabel, summary: rawSummary, include_archived: rawIncludeArchived } = args as {
        label?: unknown;
        summary?: unknown;
        include_archived?: unknown;
      };
      if (rawLabel !== undefined && typeof rawLabel !== 'string') return { content: [{ type: 'text', text: 'label must be a string when provided.' }] };
      if (rawSummary !== undefined && typeof rawSummary !== 'string') return { content: [{ type: 'text', text: 'summary must be a string when provided.' }] };
      if (rawIncludeArchived !== undefined && typeof rawIncludeArchived !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_archived must be a boolean when provided.' }] };
      }
      const includeArchived = rawIncludeArchived === true;
      const ts = now();
      const memories = await env.DB.prepare(
        `SELECT id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at
         FROM memories
         WHERE brain_id = ? ${includeArchived ? '' : 'AND archived_at IS NULL'}
         ORDER BY created_at DESC
         LIMIT 5000`
      ).bind(brainId).all<Record<string, unknown>>();
      const memoryIds = new Set(memories.results.map((m) => String(m.id ?? '')).filter(Boolean));
      const links = (await loadExplicitMemoryLinks(env, brainId, 12000))
        .filter((link) => memoryIds.has(link.from_id) && memoryIds.has(link.to_id));
      const sourceTrustRows = await env.DB.prepare(
        'SELECT source_key, trust, notes, created_at, updated_at FROM brain_source_trust WHERE brain_id = ? ORDER BY source_key ASC'
      ).bind(brainId).all<Record<string, unknown>>();
      const aliasRows = await env.DB.prepare(
        'SELECT canonical_memory_id, alias_memory_id, note, confidence, created_at, updated_at FROM memory_entity_aliases WHERE brain_id = ? ORDER BY updated_at DESC LIMIT 5000'
      ).bind(brainId).all<Record<string, unknown>>();
      const conflictResolutionRows = await env.DB.prepare(
        'SELECT pair_key, a_id, b_id, status, canonical_id, note, created_at, updated_at FROM memory_conflict_resolutions WHERE brain_id = ? ORDER BY updated_at DESC LIMIT 5000'
      ).bind(brainId).all<Record<string, unknown>>();
      const policy = await getBrainPolicy(env, brainId);
      const payload = {
        schema: 'brain_snapshot_v1',
        brain_id: brainId,
        exported_at: ts,
        include_archived: includeArchived,
        memories: memories.results,
        links,
        source_trust: sourceTrustRows.results,
        aliases: aliasRows.results,
        conflict_resolutions: conflictResolutionRows.results,
        policy,
      };
      const snapshotId = generateId();
      await env.DB.prepare(
        `INSERT INTO brain_snapshots (id, brain_id, label, summary, memory_count, link_count, payload_json, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
      ).bind(
        snapshotId,
        brainId,
        typeof rawLabel === 'string' && rawLabel.trim() ? rawLabel.trim().slice(0, 160) : null,
        typeof rawSummary === 'string' && rawSummary.trim() ? rawSummary.trim().slice(0, 500) : null,
        memories.results.length,
        links.length,
        stableJson(payload),
        ts
      ).run();

      const retention = policy.snapshot_retention;
      const snapshotRows = await env.DB.prepare(
        'SELECT id FROM brain_snapshots WHERE brain_id = ? ORDER BY created_at DESC LIMIT 2000'
      ).bind(brainId).all<{ id: string }>();
      const staleIds = snapshotRows.results.slice(retention).map((row) => row.id);
      for (const staleId of staleIds) {
        await env.DB.prepare('DELETE FROM brain_snapshots WHERE brain_id = ? AND id = ?').bind(brainId, staleId).run();
      }

      await logChangelog(env, brainId, 'brain_snapshot_created', 'brain_snapshot', snapshotId, 'Created brain snapshot', {
        memory_count: memories.results.length,
        link_count: links.length,
      });
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            snapshot_id: snapshotId,
            memory_count: memories.results.length,
            link_count: links.length,
            retention_applied: retention,
            pruned_snapshots: staleIds.length,
          }, null, 2),
        }],
      };
    }

    case 'brain_snapshot_list': {
      const { limit: rawLimit } = args as { limit?: unknown };
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 50, 1), 500);
      const rows = await env.DB.prepare(
        `SELECT id, label, summary, memory_count, link_count, created_at
         FROM brain_snapshots
         WHERE brain_id = ?
         ORDER BY created_at DESC
         LIMIT ?`
      ).bind(brainId, limit).all<Record<string, unknown>>();
      return { content: [{ type: 'text', text: JSON.stringify({ count: rows.results.length, snapshots: rows.results }, null, 2) }] };
    }

    case 'brain_snapshot_restore': {
      const { snapshot_id: rawSnapshotId, mode: rawMode, restore_policy: rawRestorePolicy, restore_source_trust: rawRestoreTrust } = args as {
        snapshot_id: unknown;
        mode?: unknown;
        restore_policy?: unknown;
        restore_source_trust?: unknown;
      };
      if (typeof rawSnapshotId !== 'string' || !rawSnapshotId.trim()) return { content: [{ type: 'text', text: 'snapshot_id must be a non-empty string.' }] };
      if (rawMode !== undefined && typeof rawMode !== 'string') return { content: [{ type: 'text', text: 'mode must be replace or merge.' }] };
      if (rawRestorePolicy !== undefined && typeof rawRestorePolicy !== 'boolean') return { content: [{ type: 'text', text: 'restore_policy must be a boolean when provided.' }] };
      if (rawRestoreTrust !== undefined && typeof rawRestoreTrust !== 'boolean') return { content: [{ type: 'text', text: 'restore_source_trust must be a boolean when provided.' }] };
      const mode = rawMode === 'replace' ? 'replace' : 'merge';
      const restorePolicy = rawRestorePolicy !== false;
      const restoreTrust = rawRestoreTrust !== false;

      const snapshot = await env.DB.prepare(
        'SELECT id, payload_json, created_at FROM brain_snapshots WHERE brain_id = ? AND id = ? LIMIT 1'
      ).bind(brainId, rawSnapshotId.trim()).first<{ id: string; payload_json: string; created_at: number }>();
      if (!snapshot?.id) return { content: [{ type: 'text', text: 'Snapshot not found.' }] };
      const payload = parseJsonObject(snapshot.payload_json);
      if (!payload) return { content: [{ type: 'text', text: 'Snapshot payload is invalid JSON.' }] };
      const memoriesPayload = Array.isArray(payload.memories) ? payload.memories : [];
      const linksPayload = Array.isArray(payload.links) ? payload.links : [];
      const sourceTrustPayload = Array.isArray(payload.source_trust) ? payload.source_trust : [];
      const aliasesPayload = Array.isArray(payload.aliases) ? payload.aliases : [];
      const resolutionsPayload = Array.isArray(payload.conflict_resolutions) ? payload.conflict_resolutions : [];
      const policyPayload = payload.policy && typeof payload.policy === 'object' && !Array.isArray(payload.policy)
        ? payload.policy as Record<string, unknown>
        : null;
      const ts = now();
      const restoredMemoryRowsForVectorSync: Array<Record<string, unknown>> = [];

      if (mode === 'replace') {
        const existingMemoryIdsBeforeReplace = await env.DB.prepare(
          'SELECT id FROM memories WHERE brain_id = ? LIMIT 50000'
        ).bind(brainId).all<{ id: string }>();
        await safeDeleteMemoryVectors(
          env,
          brainId,
          existingMemoryIdsBeforeReplace.results.map((row) => row.id),
          'brain_snapshot_restore_replace_purge'
        );
        await env.DB.prepare('DELETE FROM memory_links WHERE brain_id = ?').bind(brainId).run();
        await env.DB.prepare('DELETE FROM memory_entity_aliases WHERE brain_id = ?').bind(brainId).run();
        await env.DB.prepare('DELETE FROM memory_conflict_resolutions WHERE brain_id = ?').bind(brainId).run();
        await env.DB.prepare('DELETE FROM memories WHERE brain_id = ?').bind(brainId).run();
        if (restoreTrust) {
          await env.DB.prepare('DELETE FROM brain_source_trust WHERE brain_id = ?').bind(brainId).run();
        }
      }

      let memoryCount = 0;
      for (const rawMemory of memoriesPayload) {
        if (!rawMemory || typeof rawMemory !== 'object' || Array.isArray(rawMemory)) continue;
        const memory = rawMemory as Record<string, unknown>;
        const memoryId = typeof memory.id === 'string' && memory.id ? memory.id : generateId();
        const type = isValidType(memory.type) ? memory.type : 'note';
        const archivedAt = memory.archived_at === null || memory.archived_at === undefined
          ? null
          : Math.floor(toFiniteNumber(memory.archived_at, ts));
        const createdAt = Math.floor(toFiniteNumber(memory.created_at, ts));
        const updatedAt = Math.floor(toFiniteNumber(memory.updated_at, ts));
        const content = typeof memory.content === 'string' && memory.content.trim() ? memory.content.trim() : '';
        await env.DB.prepare(
          `INSERT INTO memories
            (id, brain_id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
             brain_id = excluded.brain_id,
             type = excluded.type,
             title = excluded.title,
             key = excluded.key,
             content = excluded.content,
             tags = excluded.tags,
             source = excluded.source,
             confidence = excluded.confidence,
             importance = excluded.importance,
             archived_at = excluded.archived_at,
             created_at = excluded.created_at,
             updated_at = excluded.updated_at`
        ).bind(
          memoryId,
          brainId,
          type,
          typeof memory.title === 'string' ? memory.title : null,
          typeof memory.key === 'string' ? memory.key : null,
          content,
          typeof memory.tags === 'string' ? memory.tags : null,
          typeof memory.source === 'string' ? memory.source : null,
          clampToRange(memory.confidence, 0.7),
          clampToRange(memory.importance, 0.5),
          archivedAt,
          createdAt,
          updatedAt
        ).run();
        restoredMemoryRowsForVectorSync.push({
          id: memoryId,
          type,
          title: typeof memory.title === 'string' ? memory.title : null,
          key: typeof memory.key === 'string' ? memory.key : null,
          content,
          tags: typeof memory.tags === 'string' ? memory.tags : null,
          source: typeof memory.source === 'string' ? memory.source : null,
          confidence: clampToRange(memory.confidence, 0.7),
          importance: clampToRange(memory.importance, 0.5),
          archived_at: archivedAt,
          created_at: createdAt,
          updated_at: updatedAt,
        });
        memoryCount++;
      }

      if (restoredMemoryRowsForVectorSync.length) {
        await safeSyncMemoriesToVectorIndex(env, brainId, restoredMemoryRowsForVectorSync, 'brain_snapshot_restore');
      }

      const existingMemoryRows = await env.DB.prepare(
        'SELECT id FROM memories WHERE brain_id = ? LIMIT 10000'
      ).bind(brainId).all<{ id: string }>();
      const existingMemoryIds = new Set(existingMemoryRows.results.map((row) => row.id));

      let linkCount = 0;
      for (const rawLink of linksPayload) {
        if (!rawLink || typeof rawLink !== 'object' || Array.isArray(rawLink)) continue;
        const link = rawLink as Record<string, unknown>;
        const fromId = typeof link.from_id === 'string' ? link.from_id : '';
        const toId = typeof link.to_id === 'string' ? link.to_id : '';
        if (!fromId || !toId || !existingMemoryIds.has(fromId) || !existingMemoryIds.has(toId)) continue;
        const linkId = typeof link.id === 'string' && link.id ? link.id : generateId();
        const relationType = normalizeRelation(link.relation_type);
        await env.DB.prepare(
          `INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
             brain_id = excluded.brain_id,
             from_id = excluded.from_id,
             to_id = excluded.to_id,
             relation_type = excluded.relation_type,
             label = excluded.label`
        ).bind(
          linkId,
          brainId,
          fromId,
          toId,
          relationType,
          typeof link.label === 'string' ? link.label : null,
          Math.floor(toFiniteNumber(link.created_at, ts))
        ).run();
        linkCount++;
      }

      let sourceTrustCount = 0;
      if (restoreTrust) {
        for (const rawTrust of sourceTrustPayload) {
          if (!rawTrust || typeof rawTrust !== 'object' || Array.isArray(rawTrust)) continue;
          const trustRow = rawTrust as Record<string, unknown>;
          const sourceKey = typeof trustRow.source_key === 'string' ? normalizeSourceKey(trustRow.source_key) : '';
          if (!sourceKey) continue;
          await env.DB.prepare(
            `INSERT INTO brain_source_trust (id, brain_id, source_key, trust, notes, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(brain_id, source_key) DO UPDATE SET trust = excluded.trust, notes = excluded.notes, updated_at = excluded.updated_at`
          ).bind(
            generateId(),
            brainId,
            sourceKey,
            clampToRange(trustRow.trust, 0.5),
            typeof trustRow.notes === 'string' ? trustRow.notes : null,
            Math.floor(toFiniteNumber(trustRow.created_at, ts)),
            ts
          ).run();
          sourceTrustCount++;
        }
      }

      let aliasCount = 0;
      for (const rawAlias of aliasesPayload) {
        if (!rawAlias || typeof rawAlias !== 'object' || Array.isArray(rawAlias)) continue;
        const alias = rawAlias as Record<string, unknown>;
        const canonicalId = typeof alias.canonical_memory_id === 'string' ? alias.canonical_memory_id : '';
        const aliasId = typeof alias.alias_memory_id === 'string' ? alias.alias_memory_id : '';
        if (!canonicalId || !aliasId || !existingMemoryIds.has(canonicalId) || !existingMemoryIds.has(aliasId)) continue;
        await env.DB.prepare(
          `INSERT INTO memory_entity_aliases
            (id, brain_id, canonical_memory_id, alias_memory_id, note, confidence, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(brain_id, alias_memory_id)
           DO UPDATE SET canonical_memory_id = excluded.canonical_memory_id, note = excluded.note, confidence = excluded.confidence, updated_at = excluded.updated_at`
        ).bind(
          generateId(),
          brainId,
          canonicalId,
          aliasId,
          typeof alias.note === 'string' ? alias.note : null,
          clampToRange(alias.confidence, 0.9),
          Math.floor(toFiniteNumber(alias.created_at, ts)),
          ts
        ).run();
        aliasCount++;
      }

      let resolutionCount = 0;
      for (const rawResolution of resolutionsPayload) {
        if (!rawResolution || typeof rawResolution !== 'object' || Array.isArray(rawResolution)) continue;
        const resolution = rawResolution as Record<string, unknown>;
        const aId = typeof resolution.a_id === 'string' ? resolution.a_id : '';
        const bId = typeof resolution.b_id === 'string' ? resolution.b_id : '';
        if (!aId || !bId || !existingMemoryIds.has(aId) || !existingMemoryIds.has(bId)) continue;
        const status = typeof resolution.status === 'string' ? resolution.status : 'needs_review';
        const resolvedKey = pairKey(aId, bId);
        await env.DB.prepare(
          `INSERT INTO memory_conflict_resolutions
            (id, brain_id, pair_key, a_id, b_id, status, canonical_id, note, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(brain_id, pair_key)
           DO UPDATE SET status = excluded.status, canonical_id = excluded.canonical_id, note = excluded.note, updated_at = excluded.updated_at`
        ).bind(
          generateId(),
          brainId,
          resolvedKey,
          aId,
          bId,
          status,
          typeof resolution.canonical_id === 'string' ? resolution.canonical_id : null,
          typeof resolution.note === 'string' ? resolution.note : null,
          Math.floor(toFiniteNumber(resolution.created_at, ts)),
          ts
        ).run();
        resolutionCount++;
      }

      if (restorePolicy && policyPayload) {
        await setBrainPolicy(env, brainId, policyPayload);
      }

      await logChangelog(env, brainId, 'brain_snapshot_restored', 'brain_snapshot', snapshot.id, `Restored brain snapshot (${mode})`, {
        mode,
        memory_count: memoryCount,
        link_count: linkCount,
        source_trust_count: sourceTrustCount,
      });
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            snapshot_id: snapshot.id,
            mode,
            restored: {
              memories: memoryCount,
              links: linkCount,
              source_trust: sourceTrustCount,
              aliases: aliasCount,
              conflict_resolutions: resolutionCount,
            },
            restore_policy: restorePolicy,
          }, null, 2),
        }],
      };
    }

    case 'memory_graph_stats': {
      const { include_inferred: rawIncludeInferred, top_hubs: rawTopHubs, top_tags: rawTopTags } = args as {
        include_inferred?: unknown;
        top_hubs?: unknown;
        top_tags?: unknown;
      };
      if (rawIncludeInferred !== undefined && typeof rawIncludeInferred !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_inferred must be a boolean when provided.' }] };
      }
      const includeInferred = rawIncludeInferred !== false;
      const topHubs = Math.min(Math.max(Number.isInteger(rawTopHubs) ? (rawTopHubs as number) : 12, 1), 50);
      const topTags = Math.min(Math.max(Number.isInteger(rawTopTags) ? (rawTopTags as number) : 12, 1), 50);
      const nodes = await loadActiveMemoryNodes(env, brainId, 2200);
      const explicitLinks = await loadExplicitMemoryLinks(env, brainId, 16000);
      const explicitPairs = new Set(explicitLinks.map((link) => pairKey(link.from_id, link.to_id)));
      const policy = await getBrainPolicy(env, brainId);
      const inferredLinks = includeInferred
        ? buildTagInferredLinks(nodes, Math.min(policy.max_inferred_edges, 3000))
          .filter((link) => !explicitPairs.has(pairKey(link.from_id, link.to_id)))
        : [];
      const allLinks = [...explicitLinks, ...inferredLinks];

      const adjacency = new Map<string, string[]>();
      const degreeById = new Map<string, number>();
      const relationCounts: Record<string, number> = {
        related: 0, supports: 0, contradicts: 0, supersedes: 0, causes: 0, example_of: 0, inferred: 0,
      };
      const perNodeRelation = new Map<string, Record<string, number>>();

      for (const node of nodes) {
        if (!adjacency.has(node.id)) adjacency.set(node.id, []);
      }
      for (const link of allLinks) {
        if (!adjacency.has(link.from_id)) adjacency.set(link.from_id, []);
        if (!adjacency.has(link.to_id)) adjacency.set(link.to_id, []);
        adjacency.get(link.from_id)?.push(link.to_id);
        adjacency.get(link.to_id)?.push(link.from_id);
        degreeById.set(link.from_id, (degreeById.get(link.from_id) ?? 0) + 1);
        degreeById.set(link.to_id, (degreeById.get(link.to_id) ?? 0) + 1);
        const relationKey = link.inferred ? 'inferred' : normalizeRelation(link.relation_type);
        relationCounts[relationKey] = (relationCounts[relationKey] ?? 0) + 1;

        const fromStats = perNodeRelation.get(link.from_id) ?? {
          related: 0, supports: 0, contradicts: 0, supersedes: 0, causes: 0, example_of: 0, inferred: 0,
        };
        fromStats[relationKey] = (fromStats[relationKey] ?? 0) + 1;
        perNodeRelation.set(link.from_id, fromStats);
        const toStats = perNodeRelation.get(link.to_id) ?? {
          related: 0, supports: 0, contradicts: 0, supersedes: 0, causes: 0, example_of: 0, inferred: 0,
        };
        toStats[relationKey] = (toStats[relationKey] ?? 0) + 1;
        perNodeRelation.set(link.to_id, toStats);
      }

      let connectedComponents = 0;
      let isolatedNodes = 0;
      const componentSizes: number[] = [];
      const visited = new Set<string>();
      for (const node of nodes) {
        const seedId = node.id;
        if (visited.has(seedId)) continue;
        connectedComponents++;
        let size = 0;
        const queue = [seedId];
        visited.add(seedId);
        while (queue.length) {
          const current = queue.shift();
          if (!current) break;
          size++;
          const neighbors = adjacency.get(current) ?? [];
          for (const neighbor of neighbors) {
            if (visited.has(neighbor)) continue;
            visited.add(neighbor);
            queue.push(neighbor);
          }
        }
        componentSizes.push(size);
        if (size === 1 && (degreeById.get(seedId) ?? 0) === 0) isolatedNodes++;
      }
      componentSizes.sort((a, b) => b - a);

      const projectedNodes = await enrichAndProjectRows(
        env,
        brainId,
        nodes as unknown as Array<Record<string, unknown>>
      );
      const projectedById = new Map(projectedNodes.map((node) => [String(node.id), node]));
      const topHubIds = nodes
        .map((node) => node.id)
        .sort((a, b) => {
          const byDegree = (degreeById.get(b) ?? 0) - (degreeById.get(a) ?? 0);
          if (byDegree !== 0) return byDegree;
          return a.localeCompare(b);
        })
        .slice(0, topHubs);
      const hubs = topHubIds.map((id) => ({
        id,
        degree: degreeById.get(id) ?? 0,
        relations: perNodeRelation.get(id) ?? {},
        memory: projectedById.get(id) ?? null,
      }));

      const tagCounts = new Map<string, number>();
      for (const node of nodes) {
        for (const tag of parseTagSet(node.tags)) {
          tagCounts.set(tag, (tagCounts.get(tag) ?? 0) + 1);
        }
      }
      const topTagRows = Array.from(tagCounts.entries())
        .sort((a, b) => {
          if (b[1] !== a[1]) return b[1] - a[1];
          return a[0].localeCompare(b[0]);
        })
        .slice(0, topTags)
        .map(([tag, count]) => ({ tag, count }));

      const avgConfidence = projectedNodes.length
        ? round3(projectedNodes.reduce((sum, node) => sum + toFiniteNumber(node.dynamic_confidence, 0.7), 0) / projectedNodes.length)
        : null;
      const avgImportance = projectedNodes.length
        ? round3(projectedNodes.reduce((sum, node) => sum + toFiniteNumber(node.dynamic_importance, 0.5), 0) / projectedNodes.length)
        : null;
      const density = nodes.length > 1
        ? round3((2 * allLinks.length) / (nodes.length * (nodes.length - 1)))
        : 0;

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            node_count: nodes.length,
            explicit_edge_count: explicitLinks.length,
            inferred_edge_count: inferredLinks.length,
            total_edge_count: allLinks.length,
            connected_components: connectedComponents,
            isolated_nodes: isolatedNodes,
            largest_component_size: componentSizes[0] ?? 0,
            density,
            relation_counts: relationCounts,
            avg_dynamic_confidence: avgConfidence,
            avg_dynamic_importance: avgImportance,
            top_hubs: hubs,
            top_tags: topTagRows,
          }, null, 2),
        }],
      };
    }

    case 'memory_neighbors': {
      const { id: rawId, query: rawQuery, max_hops: rawMaxHops, limit_nodes: rawLimitNodes, relation_type: rawRelationType, include_inferred: rawIncludeInferred } = args as {
        id?: unknown;
        query?: unknown;
        max_hops?: unknown;
        limit_nodes?: unknown;
        relation_type?: unknown;
        include_inferred?: unknown;
      };
      if (rawId !== undefined && typeof rawId !== 'string') return { content: [{ type: 'text', text: 'id must be a string when provided.' }] };
      if (rawQuery !== undefined && typeof rawQuery !== 'string') return { content: [{ type: 'text', text: 'query must be a string when provided.' }] };
      if (rawIncludeInferred !== undefined && typeof rawIncludeInferred !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_inferred must be a boolean when provided.' }] };
      }
      if (rawRelationType !== undefined && !isValidRelationType(rawRelationType)) {
        return { content: [{ type: 'text', text: 'relation_type must be one of related|supports|contradicts|supersedes|causes|example_of.' }] };
      }
      const relationFilter = isValidRelationType(rawRelationType) ? rawRelationType : null;
      const maxHops = Math.min(Math.max(Number.isInteger(rawMaxHops) ? (rawMaxHops as number) : 1, 1), 4);
      const limitNodes = Math.min(Math.max(Number.isInteger(rawLimitNodes) ? (rawLimitNodes as number) : 80, 5), 1000);
      const includeInferred = rawIncludeInferred !== false && (relationFilter === null || relationFilter === 'related');

      const nodes = await loadActiveMemoryNodes(env, brainId, 2200);
      if (!nodes.length) return { content: [{ type: 'text', text: 'No memories available.' }] };
      const nodeById = new Map(nodes.map((node) => [node.id, node]));

      let seedId = '';
      if (typeof rawId === 'string' && rawId.trim() && nodeById.has(rawId.trim())) {
        seedId = rawId.trim();
      }
      if (!seedId && typeof rawQuery === 'string' && rawQuery.trim()) {
        const q = rawQuery.trim().toLowerCase();
        const qTokens = new Set(tokenizeText(q, 24));
        const scored = nodes.map((node) => {
          const blob = `${node.id} ${node.title ?? ''} ${node.key ?? ''} ${node.content} ${node.tags ?? ''} ${node.source ?? ''}`.toLowerCase();
          const direct = blob.includes(q) ? 0.75 : 0;
          const overlap = qTokens.size ? jaccardSimilarity(new Set(tokenizeText(blob, 100)), qTokens) : 0;
          return { id: node.id, score: direct + overlap * 0.25 };
        }).filter((row) => row.score > 0)
          .sort((a, b) => b.score - a.score);
        seedId = scored[0]?.id ?? '';
      }
      if (!seedId) {
        return { content: [{ type: 'text', text: 'Provide id or query to select a seed memory.' }] };
      }

      const explicitLinks = (await loadExplicitMemoryLinks(env, brainId, 16000))
        .filter((link) => nodeById.has(link.from_id) && nodeById.has(link.to_id))
        .filter((link) => !relationFilter || normalizeRelation(link.relation_type) === relationFilter);
      const explicitPairs = new Set(explicitLinks.map((link) => pairKey(link.from_id, link.to_id)));
      const policy = await getBrainPolicy(env, brainId);
      const inferredLinks = includeInferred
        ? buildTagInferredLinks(nodes, Math.min(policy.max_inferred_edges, 1800))
          .filter((link) => !explicitPairs.has(pairKey(link.from_id, link.to_id)))
        : [];

      const adjacency = new Map<string, string[]>();
      for (const edge of [...explicitLinks, ...inferredLinks]) {
        const fromArr = adjacency.get(edge.from_id);
        if (fromArr) fromArr.push(edge.to_id);
        else adjacency.set(edge.from_id, [edge.to_id]);
        const toArr = adjacency.get(edge.to_id);
        if (toArr) toArr.push(edge.from_id);
        else adjacency.set(edge.to_id, [edge.from_id]);
      }

      const depthByNode = new Map<string, number>();
      const queue: string[] = [seedId];
      depthByNode.set(seedId, 0);
      while (queue.length && depthByNode.size < limitNodes) {
        const current = queue.shift();
        if (!current) break;
        const depth = depthByNode.get(current) ?? 0;
        if (depth >= maxHops) continue;
        const neighbors = adjacency.get(current) ?? [];
        for (const neighborId of neighbors) {
          if (depthByNode.has(neighborId)) continue;
          depthByNode.set(neighborId, depth + 1);
          queue.push(neighborId);
          if (depthByNode.size >= limitNodes) break;
        }
      }

      const selectedIds = new Set(depthByNode.keys());
      const selectedNodes = nodes.filter((node) => selectedIds.has(node.id));
      const selectedEdges = explicitLinks.filter((edge) => selectedIds.has(edge.from_id) && selectedIds.has(edge.to_id));
      const selectedInferred = inferredLinks.filter((edge) => selectedIds.has(edge.from_id) && selectedIds.has(edge.to_id));
      const projectedNodes = await enrichAndProjectRows(
        env,
        brainId,
        selectedNodes as unknown as Array<Record<string, unknown>>
      );
      const projectedById = new Map(projectedNodes.map((node) => [String(node.id), node]));
      const depthObject: Record<string, number> = {};
      for (const [nodeId, depth] of depthByNode.entries()) depthObject[nodeId] = depth;

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            seed_id: seedId,
            seed: projectedById.get(seedId) ?? null,
            max_hops: maxHops,
            relation_filter: relationFilter,
            include_inferred: includeInferred,
            node_count: projectedNodes.length,
            edge_count: selectedEdges.length,
            inferred_edge_count: selectedInferred.length,
            depth_by_node: depthObject,
            nodes: projectedNodes,
            edges: selectedEdges,
            inferred_edges: selectedInferred,
          }, null, 2),
        }],
      };
    }

    case 'memory_subgraph': {
      const { seed_id: rawSeedId, query: rawQuery, tag: rawTag, radius: rawRadius, limit_nodes: rawLimitNodes, include_inferred: rawIncludeInferred } = args as {
        seed_id?: unknown;
        query?: unknown;
        tag?: unknown;
        radius?: unknown;
        limit_nodes?: unknown;
        include_inferred?: unknown;
      };
      if (rawSeedId !== undefined && typeof rawSeedId !== 'string') return { content: [{ type: 'text', text: 'seed_id must be a string when provided.' }] };
      if (rawQuery !== undefined && typeof rawQuery !== 'string') return { content: [{ type: 'text', text: 'query must be a string when provided.' }] };
      if (rawTag !== undefined && typeof rawTag !== 'string') return { content: [{ type: 'text', text: 'tag must be a string when provided.' }] };
      if (rawIncludeInferred !== undefined && typeof rawIncludeInferred !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_inferred must be a boolean when provided.' }] };
      }
      const policy = await getBrainPolicy(env, brainId);
      const radius = Math.min(Math.max(Number.isInteger(rawRadius) ? (rawRadius as number) : policy.subgraph_default_radius, 1), 6);
      const limitNodes = Math.min(Math.max(Number.isInteger(rawLimitNodes) ? (rawLimitNodes as number) : 120, 10), 1000);
      const includeInferred = rawIncludeInferred !== false;
      const nodes = await loadActiveMemoryNodes(env, brainId, 1800);
      if (!nodes.length) return { content: [{ type: 'text', text: 'No memories available.' }] };
      const tagFilter = typeof rawTag === 'string' && rawTag.trim() ? normalizeTag(rawTag) : '';
      const nodeById = new Map(nodes.map((node) => [node.id, node]));
      const candidateSeeds = tagFilter
        ? nodes.filter((node) => parseTagSet(node.tags).has(tagFilter))
        : nodes;
      const seedIds = new Set<string>();
      if (typeof rawSeedId === 'string' && rawSeedId.trim() && nodeById.has(rawSeedId.trim())) {
        const seed = rawSeedId.trim();
        if (!tagFilter || parseTagSet(nodeById.get(seed)?.tags).has(tagFilter)) seedIds.add(seed);
      }
      if (typeof rawQuery === 'string' && rawQuery.trim()) {
        const query = rawQuery.trim().toLowerCase();
        const scored = candidateSeeds.map((node) => {
          const text = `${node.id} ${node.title ?? ''} ${node.key ?? ''} ${node.content} ${node.source ?? ''}`.toLowerCase();
          const direct = text.includes(query) ? 1 : 0;
          const overlap = jaccardSimilarity(
            new Set(tokenizeText(text, 100)),
            new Set(tokenizeText(query, 24))
          );
          return { id: node.id, score: direct * 0.7 + overlap * 0.3 };
        }).filter((row) => row.score > 0)
          .sort((a, b) => b.score - a.score)
          .slice(0, 8);
        for (const item of scored) seedIds.add(item.id);
      }
      if (!seedIds.size) {
        for (const node of candidateSeeds.slice(0, 3)) seedIds.add(node.id);
      }
      if (!seedIds.size) return { content: [{ type: 'text', text: 'No seed nodes matched the requested filters.' }] };

      const links = await loadExplicitMemoryLinks(env, brainId, 12000);
      const adjacency = new Map<string, string[]>();
      for (const link of links) {
        if (!nodeById.has(link.from_id) || !nodeById.has(link.to_id)) continue;
        const fromArr = adjacency.get(link.from_id);
        if (fromArr) fromArr.push(link.to_id);
        else adjacency.set(link.from_id, [link.to_id]);
        const toArr = adjacency.get(link.to_id);
        if (toArr) toArr.push(link.from_id);
        else adjacency.set(link.to_id, [link.from_id]);
      }

      const depthByNode = new Map<string, number>();
      const queue: Array<{ id: string; depth: number }> = [];
      for (const seedId of seedIds) {
        depthByNode.set(seedId, 0);
        queue.push({ id: seedId, depth: 0 });
      }
      while (queue.length > 0 && depthByNode.size < limitNodes) {
        const current = queue.shift();
        if (!current) break;
        if (current.depth >= radius) continue;
        const neighbors = adjacency.get(current.id) ?? [];
        for (const neighbor of neighbors) {
          if (depthByNode.has(neighbor)) continue;
          depthByNode.set(neighbor, current.depth + 1);
          queue.push({ id: neighbor, depth: current.depth + 1 });
          if (depthByNode.size >= limitNodes) break;
        }
      }

      const selectedIds = new Set(depthByNode.keys());
      const selectedNodes = nodes.filter((node) => selectedIds.has(node.id));
      const selectedEdges = links.filter((link) => selectedIds.has(link.from_id) && selectedIds.has(link.to_id));
      const explicitPairs = new Set(selectedEdges.map((edge) => pairKey(edge.from_id, edge.to_id)));
      const inferredEdges = includeInferred
        ? buildTagInferredLinks(selectedNodes, Math.min(policy.max_inferred_edges, 1200))
          .filter((edge) => !explicitPairs.has(pairKey(edge.from_id, edge.to_id)))
        : [];

      const projectedNodes = await enrichAndProjectRows(env, brainId, selectedNodes as unknown as Array<Record<string, unknown>>);
      const depthObject: Record<string, number> = {};
      for (const [nodeId, depth] of depthByNode) depthObject[nodeId] = depth;

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            seed_ids: Array.from(seedIds),
            radius,
            node_count: projectedNodes.length,
            edge_count: selectedEdges.length,
            inferred_edge_count: inferredEdges.length,
            depth_by_node: depthObject,
            nodes: projectedNodes,
            edges: selectedEdges,
            inferred_edges: inferredEdges,
          }, null, 2),
        }],
      };
    }

    case 'memory_watch': {
      const { mode: rawMode, id: rawId, name: rawName, event_types: rawEventTypes, query: rawQuery, webhook_url: rawWebhook, secret: rawSecret, active: rawActive, limit: rawLimit } = args as {
        mode?: unknown;
        id?: unknown;
        name?: unknown;
        event_types?: unknown;
        query?: unknown;
        webhook_url?: unknown;
        secret?: unknown;
        active?: unknown;
        limit?: unknown;
      };
      if (rawMode !== undefined && typeof rawMode !== 'string') return { content: [{ type: 'text', text: 'mode must be a string when provided.' }] };
      const mode = typeof rawMode === 'string' ? rawMode.trim().toLowerCase() : 'list';

      if (mode === 'list') {
        const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 100, 1), 500);
        const rows = await env.DB.prepare(
          `SELECT id, name, event_types, query, webhook_url, is_active, created_at, updated_at, last_triggered_at, last_error
           FROM memory_watches
           WHERE brain_id = ?
           ORDER BY updated_at DESC
           LIMIT ?`
        ).bind(brainId, limit).all<Record<string, unknown>>();
        const watches = rows.results.map((row) => ({
          ...row,
          event_types: typeof row.event_types === 'string' ? parseWatchEventTypes(row.event_types) : [],
          is_active: Number(row.is_active ?? 0) === 1,
        }));
        return { content: [{ type: 'text', text: JSON.stringify({ count: watches.length, watches }, null, 2) }] };
      }

      if (mode === 'create') {
        if (typeof rawName !== 'string' || !rawName.trim()) return { content: [{ type: 'text', text: 'name is required for create mode.' }] };
        if (rawQuery !== undefined && typeof rawQuery !== 'string') return { content: [{ type: 'text', text: 'query must be a string when provided.' }] };
        if (rawWebhook !== undefined && typeof rawWebhook !== 'string') return { content: [{ type: 'text', text: 'webhook_url must be a string when provided.' }] };
        if (rawSecret !== undefined && typeof rawSecret !== 'string') return { content: [{ type: 'text', text: 'secret must be a string when provided.' }] };
        const eventTypes = normalizeWatchEventInput(rawEventTypes);
        const finalEventTypes = eventTypes.length ? eventTypes : ['*'];
        const webhookUrl = typeof rawWebhook === 'string' && rawWebhook.trim() ? rawWebhook.trim() : null;
        if (webhookUrl && !(webhookUrl.startsWith('https://') || webhookUrl.startsWith('http://'))) {
          return { content: [{ type: 'text', text: 'webhook_url must start with http:// or https://.' }] };
        }
        const ts = now();
        const watchId = generateId();
        await env.DB.prepare(
          `INSERT INTO memory_watches
            (id, brain_id, name, event_types, query, webhook_url, secret, is_active, created_at, updated_at, last_triggered_at, last_error)
           VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, NULL, NULL)`
        ).bind(
          watchId,
          brainId,
          rawName.trim().slice(0, 120),
          stableJson(finalEventTypes),
          typeof rawQuery === 'string' && rawQuery.trim() ? rawQuery.trim().slice(0, 200) : null,
          webhookUrl,
          typeof rawSecret === 'string' && rawSecret.trim() ? rawSecret.trim().slice(0, 200) : null,
          ts,
          ts
        ).run();
        const row = await env.DB.prepare(
          'SELECT id, name, event_types, query, webhook_url, is_active, created_at, updated_at FROM memory_watches WHERE brain_id = ? AND id = ? LIMIT 1'
        ).bind(brainId, watchId).first<Record<string, unknown>>();
        await logChangelog(env, brainId, 'memory_watch_created', 'memory_watch', watchId, 'Created memory watch', {
          event_types: finalEventTypes,
        });
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              watch: row
                ? {
                    ...row,
                    event_types: typeof row.event_types === 'string' ? parseWatchEventTypes(row.event_types) : [],
                    is_active: Number(row.is_active ?? 0) === 1,
                  }
                : null,
            }, null, 2),
          }],
        };
      }

      if (mode === 'delete') {
        if (typeof rawId !== 'string' || !rawId.trim()) return { content: [{ type: 'text', text: 'id is required for delete mode.' }] };
        const watchId = rawId.trim();
        const result = await env.DB.prepare(
          'DELETE FROM memory_watches WHERE brain_id = ? AND id = ?'
        ).bind(brainId, watchId).run();
        if ((result.meta.changes ?? 0) === 0) return { content: [{ type: 'text', text: 'Watch not found.' }] };
        await logChangelog(env, brainId, 'memory_watch_deleted', 'memory_watch', watchId, 'Deleted memory watch');
        return { content: [{ type: 'text', text: JSON.stringify({ deleted: true, id: watchId }) }] };
      }

      if (mode === 'set_active') {
        if (typeof rawId !== 'string' || !rawId.trim()) return { content: [{ type: 'text', text: 'id is required for set_active mode.' }] };
        if (typeof rawActive !== 'boolean') return { content: [{ type: 'text', text: 'active must be true or false for set_active mode.' }] };
        const watchId = rawId.trim();
        const ts = now();
        const result = await env.DB.prepare(
          'UPDATE memory_watches SET is_active = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
        ).bind(rawActive ? 1 : 0, ts, brainId, watchId).run();
        if ((result.meta.changes ?? 0) === 0) return { content: [{ type: 'text', text: 'Watch not found.' }] };
        await logChangelog(env, brainId, 'memory_watch_updated', 'memory_watch', watchId, 'Updated memory watch activation', {
          active: rawActive,
        });
        return { content: [{ type: 'text', text: JSON.stringify({ id: watchId, active: rawActive }) }] };
      }

      if (mode === 'test') {
        if (typeof rawId !== 'string' || !rawId.trim()) return { content: [{ type: 'text', text: 'id is required for test mode.' }] };
        const watchId = rawId.trim();
        const watch = await env.DB.prepare(
          'SELECT id, webhook_url, secret, is_active FROM memory_watches WHERE brain_id = ? AND id = ? LIMIT 1'
        ).bind(brainId, watchId).first<{ id: string; webhook_url: string | null; secret: string | null; is_active: number }>();
        if (!watch?.id) return { content: [{ type: 'text', text: 'Watch not found.' }] };
        const webhook = typeof watch.webhook_url === 'string' ? watch.webhook_url.trim() : '';
        const ts = now();
        if (!webhook) {
          await env.DB.prepare(
            'UPDATE memory_watches SET last_triggered_at = ?, last_error = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
          ).bind(ts, 'test_no_webhook', ts, brainId, watchId).run();
          return { content: [{ type: 'text', text: JSON.stringify({ id: watchId, tested: true, delivered: false, reason: 'No webhook_url configured.' }) }] };
        }
        if (!(webhook.startsWith('https://') || webhook.startsWith('http://'))) {
          return { content: [{ type: 'text', text: 'Configured webhook_url is invalid. It must start with http:// or https://.' }] };
        }
        try {
          const headers: Record<string, string> = {
            'Content-Type': 'application/json',
            'X-MemoryVault-Watch-Id': watchId,
          };
          if (watch.secret) headers['X-MemoryVault-Watch-Secret'] = watch.secret;
          const response = await fetch(webhook, {
            method: 'POST',
            headers,
            body: JSON.stringify({
              watch_id: watchId,
              event_type: 'watch_test',
              entity_type: 'memory_watch',
              entity_id: watchId,
              summary: 'Manual watch test',
              payload: { mode: 'test' },
              created_at: ts,
            }),
          });
          await env.DB.prepare(
            'UPDATE memory_watches SET last_triggered_at = ?, last_error = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
          ).bind(ts, response.ok ? null : `webhook_status_${response.status}`, ts, brainId, watchId).run();
          return { content: [{ type: 'text', text: JSON.stringify({ id: watchId, tested: true, delivered: response.ok, status: response.status }) }] };
        } catch (err) {
          const message = err instanceof Error ? err.message.slice(0, 280) : 'webhook_error';
          await env.DB.prepare(
            'UPDATE memory_watches SET last_triggered_at = ?, last_error = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
          ).bind(ts, message, ts, brainId, watchId).run();
          return { content: [{ type: 'text', text: JSON.stringify({ id: watchId, tested: true, delivered: false, error: message }) }] };
        }
      }

      return { content: [{ type: 'text', text: 'Invalid mode. Use create|list|delete|set_active|test.' }] };
    }

    default:
      throw Object.assign(new Error(`Unknown tool: ${name}`), { code: -32601 });
  }
}

const ALLOWED_ORIGINS = [
  'https://claude.ai',
  'https://poke.com',
  'https://ai-memory-mcp-dev.guirguispierre.workers.dev',
  'https://ai-memory-mcp.guirguispierre.workers.dev',
];

const CORS_HEADERS = {
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept',
};

const HTML_SECURITY_HEADERS: Record<string, string> = {
  'X-Frame-Options': 'DENY',
  'X-Content-Type-Options': 'nosniff',
  'Strict-Transport-Security': 'max-age=31536000; includeSubDomains; preload',
  'Referrer-Policy': 'no-referrer',
  'Permissions-Policy': 'geolocation=(), microphone=(), camera=()',
  'Content-Security-Policy': "default-src 'self'; script-src 'self' cdn.jsdelivr.net fonts.googleapis.com fonts.gstatic.com mcp.figma.com; style-src 'self' 'unsafe-inline' fonts.googleapis.com; font-src fonts.gstatic.com; connect-src 'self' mcp.figma.com; frame-ancestors 'none';",
};

function corsJsonResponse(
  body: unknown,
  status = 200,
  options: CorsJsonResponseOptions | Record<string, string> = {}
): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: buildCorsJsonHeaders(normalizeCorsJsonResponseOptions(options)),
  });
}

function getCorsOrigin(request: Request): string {
  const origin = request.headers.get('Origin')?.trim();
  if (origin && ALLOWED_ORIGINS.includes(origin)) return origin;

  const requestOrigin = new URL(request.url).origin;
  return ALLOWED_ORIGINS.includes(requestOrigin) ? requestOrigin : ALLOWED_ORIGINS[0];
}

function mergeVaryHeader(existingValue: string | null, value: string): string {
  const varyValues = new Set(
    (existingValue ?? '')
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
  );
  varyValues.add(value);
  return Array.from(varyValues).join(', ');
}

function applyCors(request: Request, response: Response): Response {
  const headers = new Headers(response.headers);
  headers.set('Access-Control-Allow-Origin', getCorsOrigin(request));
  for (const [name, value] of Object.entries(CORS_HEADERS)) {
    headers.set(name, value);
  }
  headers.set('Vary', mergeVaryHeader(headers.get('Vary'), 'Origin'));

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function isHtmlResponse(response: Response): boolean {
  const contentType = (response.headers.get('Content-Type') ?? '').toLowerCase();
  return contentType.includes('text/html');
}

function wrapWithSecurityHeaders(response: Response): Response {
  const clonedResponse = response.clone();
  const headers = new Headers(clonedResponse.headers);
  for (const [name, value] of Object.entries(HTML_SECURITY_HEADERS)) {
    headers.set(name, value);
  }
  return new Response(clonedResponse.body, {
    status: clonedResponse.status,
    statusText: clonedResponse.statusText,
    headers,
  });
}

function isLikelyMcpRootRequest(request: Request): boolean {
  const accept = (request.headers.get('Accept') ?? '').toLowerCase();
  const contentType = (request.headers.get('Content-Type') ?? '').toLowerCase();
  if (accept.includes('text/event-stream')) return true;
  if (request.headers.has('MCP-Protocol-Version') || request.headers.has('mcp-protocol-version')) return true;
  if (request.method === 'POST' && contentType.includes('application/json')) return true;
  return false;
}

function isBrowserDocumentRequest(request: Request): boolean {
  if (request.method !== 'GET') return false;
  const accept = (request.headers.get('Accept') ?? '').toLowerCase();
  if (accept.includes('text/event-stream')) return false;
  if (request.headers.has('MCP-Protocol-Version') || request.headers.has('mcp-protocol-version')) return false;
  const fetchDest = (request.headers.get('Sec-Fetch-Dest') ?? '').toLowerCase();
  const fetchMode = (request.headers.get('Sec-Fetch-Mode') ?? '').toLowerCase();
  if (fetchDest === 'document' || fetchMode === 'navigate') return true;
  return accept.includes('text/html');
}

function isOAuthAuthorizeNavigation(url: URL): boolean {
  if (url.pathname !== '/authorize') return false;
  const q = url.searchParams;
  return q.has('response_type')
    || q.has('client_id')
    || q.has('redirect_uri')
    || q.has('code_challenge')
    || q.has('state')
    || q.has('scope')
    || q.has('resource');
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
    const tools = canMutateMemories(authCtx)
      ? TOOLS
      : TOOLS.filter((tool) => !isMutatingTool(tool.name));
    return { jsonrpc: '2.0', id, result: { tools } };
  }

  if (method === 'tools/call') {
    const { name, arguments: toolArgs = {} } = params as { name?: unknown; arguments?: ToolArgs };
    if (typeof name !== 'string' || !name.trim()) {
      return {
        jsonrpc: '2.0',
        id,
        error: { code: -32602, message: 'Invalid params: tool name is required.' },
      };
    }
    if (!canMutateMemories(authCtx) && isMutatingTool(name)) {
      return {
        jsonrpc: '2.0',
        id,
        error: {
          code: -32003,
          message: 'Forbidden: this session cannot modify memories. Re-authenticate and try again.',
        },
      };
    }
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
  const sourceTrustMap = await loadSourceTrustMap(env, brainId);
  const enrichedMemories = enrichMemoryRowsWithDynamics(
    results.results as Array<Record<string, unknown>>,
    linkStatsMap,
    tsNow,
    sourceTrustMap
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
  const sourceTrustMap = await loadSourceTrustMap(env, brainId);
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
    const sourceKey = typeof base.source === 'string' ? normalizeSourceKey(base.source) : '';
    return projectMemoryForClient({
      ...base,
      ...computeDynamicScores(base, linkStatsMap.get(String(r.id ?? '')), tsNow, sourceKey ? sourceTrustMap.get(sourceKey) : undefined),
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
  const policy = await getBrainPolicy(env, brainId);
  const linkStatsMap = await loadLinkStatsMap(env, brainId);
  const sourceTrustMap = await loadSourceTrustMap(env, brainId);
  const nodes = enrichMemoryRowsWithDynamics(
    memories.results as Array<Record<string, unknown>>,
    linkStatsMap,
    tsNow,
    sourceTrustMap
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
  const inferredMax = Math.min(Math.max(policy.max_inferred_edges, 30), 5000);
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

function handleApiTools(authCtx: AuthContext): Response {
  const tools = canMutateMemories(authCtx)
    ? TOOLS
    : TOOLS.filter((tool) => !isMutatingTool(tool.name));
  return new Response(JSON.stringify({
    server: { name: SERVER_NAME, version: SERVER_VERSION },
    tool_count: tools.length,
    tool_names: tools.map((t) => t.name),
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
  const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
  if (await checkRateLimit(ip, env.RATE_LIMIT_KV)) {
    return corsJsonResponse({ error: 'Too many attempts. Try again in 15 minutes.' }, 429);
  }

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
  const user = { id: userId, email, display_name: displayName, created_at: ts };
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
  return corsJsonResponse({ success: true, user: userPayload(user) }, 200, {
    cookies: buildSessionCookieHeaders(tokens),
  });
}

async function handleAuthLogin(request: Request, env: Env): Promise<Response> {
  const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
  if (await checkRateLimit(ip, env.RATE_LIMIT_KV)) {
    return corsJsonResponse({ error: 'Too many attempts. Try again in 15 minutes.' }, 429);
  }

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
  await resetAuthRateLimit(ip, env.RATE_LIMIT_KV);
  return corsJsonResponse({ success: true, user: userPayload(user) }, 200, {
    cookies: buildSessionCookieHeaders(tokens),
  });
}

async function handleAuthRefresh(request: Request, env: Env): Promise<Response> {
  const refreshToken = (getRequestCookie(request, REFRESH_TOKEN_COOKIE_NAME) ?? '').trim();
  if (!refreshToken) {
    return corsJsonResponse({ error: 'refresh_token cookie is required.' }, 400, {
      cookies: clearSessionCookieHeaders(),
    });
  }

  const rotated = await rotateSession(refreshToken, env);
  if (!rotated) {
    return corsJsonResponse({ error: 'Invalid or expired refresh token.' }, 401, {
      cookies: clearSessionCookieHeaders(),
    });
  }
  return corsJsonResponse({ success: true }, 200, {
    cookies: buildRotatedSessionCookieHeaders(rotated.tokens),
  });
}

async function handleAuthLogout(request: Request, env: Env): Promise<Response> {
  const refreshToken = (getRequestCookie(request, REFRESH_TOKEN_COOKIE_NAME) ?? '').trim();
  if (refreshToken) {
    await revokeSession(refreshToken, env);
  } else {
    const authCtx = await authenticateRequest(request, env);
    if (authCtx?.kind === 'user' && authCtx.sessionId) {
      await revokeSessionById(authCtx.sessionId, env);
    }
  }
  return corsJsonResponse({ success: true }, 200, { cookies: clearSessionCookieHeaders() });
}

async function handleAuthMe(_authCtx: AuthContext, _env: Env): Promise<Response> {
  return corsJsonResponse({ ok: true });
}

type SessionSummary = {
  id: string;
  brain_id: string;
  brain_name: string | null;
  created_at: number;
  used_at: number;
  expires_at: number;
  revoked_at: number | null;
};

async function listSessionsForUser(userId: string, env: Env): Promise<SessionSummary[]> {
  const rows = await env.DB.prepare(
    `SELECT
      s.id,
      s.brain_id,
      b.name AS brain_name,
      s.created_at,
      s.used_at,
      s.expires_at,
      s.revoked_at
     FROM auth_sessions s
     LEFT JOIN brains b ON b.id = s.brain_id
     WHERE s.user_id = ?
     ORDER BY s.used_at DESC, s.created_at DESC
     LIMIT 200`
  ).bind(userId).all<SessionSummary>();
  return rows.results;
}

async function handleAuthSessions(authCtx: AuthContext, env: Env): Promise<Response> {
  if (authCtx.kind !== 'user' || !authCtx.userId) {
    return corsJsonResponse({ error: 'Session listing requires a user account.' }, 403);
  }
  const sessions = await listSessionsForUser(authCtx.userId, env);
  const tsNow = now();
  return corsJsonResponse({
    count: sessions.length,
    sessions: sessions.map((s) => ({
      id: s.id,
      brain_id: s.brain_id,
      brain_name: s.brain_name,
      created_at: s.created_at,
      used_at: s.used_at,
      expires_at: s.expires_at,
      revoked_at: s.revoked_at,
      is_current: authCtx.sessionId === s.id,
      is_active: s.revoked_at === null && s.expires_at > tsNow,
    })),
  });
}

async function handleAuthSessionRevoke(request: Request, authCtx: AuthContext, env: Env): Promise<Response> {
  if (authCtx.kind !== 'user' || !authCtx.userId) {
    return corsJsonResponse({ error: 'Session revocation requires a user account.' }, 403);
  }

  const body = await readJsonBody(request) ?? {};
  const sessionId = typeof body.session_id === 'string' ? body.session_id.trim() : '';
  const revokeAll = body.all === true;
  const keepCurrent = body.keep_current !== false;
  const ts = now();

  if (sessionId) {
    if (authCtx.sessionId && sessionId === authCtx.sessionId) {
      return corsJsonResponse({ error: 'Cannot revoke the current session via session_id. Use all=true with keep_current=false if you intend full logout.' }, 400);
    }
    const result = await env.DB.prepare(
      'UPDATE auth_sessions SET revoked_at = ? WHERE id = ? AND user_id = ? AND revoked_at IS NULL'
    ).bind(ts, sessionId, authCtx.userId).run();
    return corsJsonResponse({
      revoked_count: result.meta.changes ?? 0,
      session_id: sessionId,
    });
  }

  if (revokeAll) {
    if (keepCurrent && authCtx.sessionId) {
      const result = await env.DB.prepare(
        'UPDATE auth_sessions SET revoked_at = ? WHERE user_id = ? AND id != ? AND revoked_at IS NULL'
      ).bind(ts, authCtx.userId, authCtx.sessionId).run();
      return corsJsonResponse({
        revoked_count: result.meta.changes ?? 0,
        all: true,
        kept_current_session: true,
      });
    }
    const result = await env.DB.prepare(
      'UPDATE auth_sessions SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL'
    ).bind(ts, authCtx.userId).run();
    return corsJsonResponse({
      revoked_count: result.meta.changes ?? 0,
      all: true,
      kept_current_session: false,
    });
  }

  return corsJsonResponse({ error: 'Provide session_id or all=true.' }, 400);
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

function oauthRateLimitedError(): Response {
  return new Response(JSON.stringify({
    error: 'temporarily_unavailable',
    error_description: 'Too many failed attempts. Try again later.',
  }), {
    status: 429,
    headers: noStoreJsonHeaders({ 'Retry-After': '900' }),
  });
}

function oauthUnauthorized(errorDescription = 'Unauthorized.'): Response {
  return new Response(JSON.stringify({
    error: 'unauthorized',
    error_description: errorDescription,
  }), {
    status: 401,
    headers: noStoreJsonHeaders({ 'WWW-Authenticate': 'Bearer' }),
  });
}

function timingSafeEqualStrings(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let mismatch = 0;
  for (let i = 0; i < a.length; i++) {
    mismatch |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }
  return mismatch === 0;
}

async function verifyTokenEndpointClientSecret(client: OAuthClientRow, providedSecret: string): Promise<Response | null> {
  if (!client.client_secret_hash) {
    return oauthError('invalid_client', 'Client secret is not configured for this client.', 401);
  }
  if (client.client_secret_expires_at > 0 && client.client_secret_expires_at <= now()) {
    return oauthError('invalid_client', 'Client secret has expired.', 401);
  }
  const providedHash = await sha256DigestBase64Url(providedSecret);
  if (!timingSafeEqualStrings(providedHash, client.client_secret_hash)) {
    return oauthError('invalid_client', 'Client authentication failed.', 401);
  }
  return null;
}

async function validateTokenEndpointClientAuth(
  client: OAuthClientRow,
  params: URLSearchParams,
  basicClient: { clientId: string; clientSecret: string } | null,
  options: { requireConfidentialAuth: boolean } = { requireConfidentialAuth: false }
): Promise<Response | null> {
  const expectedClientId = client.client_id;
  const paramClientId = getParam(params, 'client_id', 'clientId').trim();
  const paramClientSecretRaw = params.get('client_secret') ?? params.get('clientSecret');
  const paramClientSecret = (paramClientSecretRaw ?? '').trim();
  const basicClientId = basicClient?.clientId ?? '';
  const basicClientSecret = basicClient?.clientSecret ?? '';

  if (paramClientId && paramClientId !== expectedClientId) {
    return oauthError('invalid_client', 'client_id is invalid.', 401);
  }
  if (basicClientId && basicClientId !== expectedClientId) {
    return oauthError('invalid_client', 'HTTP Basic client_id is invalid.', 401);
  }
  if (paramClientId && basicClientId && paramClientId !== basicClientId) {
    return oauthError('invalid_client', 'Conflicting client identifiers were provided.', 401);
  }

  const methodRaw = (client.token_endpoint_auth_method || 'none').trim();
  const method = (methodRaw === 'none' || methodRaw === 'client_secret_post' || methodRaw === 'client_secret_basic')
    ? methodRaw
    : 'none';

  if (method === 'none') {
    // Public clients: ignore any client_secret sent in body or Basic auth.
    // Many MCP clients (Claude Desktop, etc.) send a client_secret even for
    // public clients registered with token_endpoint_auth_method=none.
    return null;
  }

  if (method === 'client_secret_basic') {
    if (!basicClient) {
      if (!options.requireConfidentialAuth) return null;
      return oauthError('invalid_client', 'client_secret_basic authentication is required.', 401);
    }
    if (!basicClientSecret) {
      return oauthError('invalid_client', 'Client secret is required in HTTP Basic authentication.', 401);
    }
    if (paramClientSecretRaw !== null && paramClientSecret.length > 0) {
      return oauthError('invalid_client', 'Do not send client_secret in the body for client_secret_basic.', 401);
    }
    return verifyTokenEndpointClientSecret(client, basicClientSecret);
  }

  if (method === 'client_secret_post') {
    if (basicClient) {
      return oauthError('invalid_client', 'Use client_secret in the request body for this client.', 401);
    }
    if (!paramClientId && options.requireConfidentialAuth) {
      return oauthError('invalid_client', 'client_id is required for client_secret_post.', 401);
    }
    if (!paramClientSecret) {
      if (!options.requireConfidentialAuth) return null;
      return oauthError('invalid_client', 'client_secret is required for client_secret_post.', 401);
    }
    return verifyTokenEndpointClientSecret(client, paramClientSecret);
  }

  return oauthError('invalid_client', 'Unsupported token endpoint auth method.', 401);
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

function getOAuthRedirectDomainAllowlist(env: Env): string[] {
  const configured = typeof env.OAUTH_REDIRECT_DOMAIN_ALLOWLIST === 'string'
    ? env.OAUTH_REDIRECT_DOMAIN_ALLOWLIST
    : '';
  const parsed = configured
    .split(/[,\s]+/)
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
  return Array.from(new Set([
    ...DEFAULT_OAUTH_REDIRECT_DOMAIN_ALLOWLIST,
    ...TRUSTED_REDIRECT_DOMAINS,
    ...parsed,
  ]));
}

function isAllowedRedirectDomain(hostname: string, env: Env): boolean {
  const normalized = hostname.trim().toLowerCase();
  if (!normalized) return false;
  return getOAuthRedirectDomainAllowlist(env).some((domain) => (
    normalized === domain || normalized.endsWith(`.${domain}`)
  ));
}

function isWhitelistedRedirectUri(raw: string, env: Env): boolean {
  if (!isValidRedirectUri(raw)) return false;
  try {
    const url = new URL(raw);
    return isAllowedRedirectDomain(url.hostname, env);
  } catch {
    return false;
  }
}

function hasOnlyWhitelistedRedirectUris(redirectUris: string[], env: Env): boolean {
  return redirectUris.length > 0 && redirectUris.every((uri) => isWhitelistedRedirectUri(uri, env));
}

function isTrustedRedirectDomain(hostname: string): boolean {
  const normalized = hostname.trim().toLowerCase();
  if (!normalized) return false;
  return TRUSTED_REDIRECT_DOMAINS.some((domain) => (
    normalized === domain || normalized.endsWith(`.${domain}`)
  ));
}

function isTrustedRedirectUri(raw: string): boolean {
  if (!isValidRedirectUri(raw)) return false;
  try {
    const url = new URL(raw);
    return isTrustedRedirectDomain(url.hostname);
  } catch {
    return false;
  }
}

function hasOnlyTrustedRedirectUris(redirectUris: string[]): boolean {
  return redirectUris.length > 0 && redirectUris.every((uri) => isTrustedRedirectUri(uri));
}

function hasValidAdminBearer(request: Request, env: Env): boolean {
  const provided = parseBearerToken(request)?.trim() ?? '';
  const expected = (env.ADMIN_TOKEN ?? '').trim();
  if (!provided || !expected) return false;
  return timingSafeEqualStrings(provided, expected);
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
  // Compatibility fallback: some OAuth clients send form payloads without a content-type.
  try {
    const text = await request.text();
    if (!text) return null;
    const trimmed = text.trim();
    if (trimmed.startsWith('{')) {
      const parsed = JSON.parse(trimmed) as unknown;
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        const params = new URLSearchParams();
        for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
          if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') {
            params.set(k, String(v));
          }
        }
        return params;
      }
    }
    return new URLSearchParams(text);
  } catch {
    return null;
  }
}

function getParam(params: URLSearchParams, ...names: string[]): string {
  for (const name of names) {
    const value = params.get(name);
    if (value !== null && value !== undefined) return value;
  }
  return '';
}

function parseBasicClientAuth(request: Request): { clientId: string; clientSecret: string } | null {
  const auth = request.headers.get('authorization') ?? '';
  const match = auth.match(/^\s*Basic\s+(.+?)\s*$/i);
  if (!match) return null;
  try {
    const decoded = atob(match[1]);
    const split = decoded.indexOf(':');
    if (split < 0) return null;
    const clientId = decoded.slice(0, split).trim();
    const clientSecret = decoded.slice(split + 1);
    if (!clientId) return null;
    return { clientId, clientSecret };
  } catch {
    return null;
  }
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

function isAllowedRedirectForClient(client: OAuthClientRow | null, redirectUri: string, env: Env): boolean {
  if (!client) return false;
  if (!isWhitelistedRedirectUri(redirectUri, env)) return false;
  return client.redirect_uris.includes(redirectUri);
}

async function revokeAndDeleteOAuthClient(clientId: string, id: string, env: Env): Promise<void> {
  const ts = now();
  await env.DB.prepare(
    'UPDATE auth_sessions SET revoked_at = ? WHERE client_id = ? AND revoked_at IS NULL'
  ).bind(ts, clientId).run();
  await env.DB.prepare(
    'DELETE FROM oauth_authorization_codes WHERE client_id = ?'
  ).bind(clientId).run();
  await env.DB.prepare(
    'DELETE FROM oauth_clients WHERE id = ?'
  ).bind(id).run();
}

async function purgeOAuthClientIfNotWhitelisted(client: OAuthClientRow | null, env: Env): Promise<OAuthClientRow | null> {
  if (!client) return null;
  if (hasOnlyWhitelistedRedirectUris(client.redirect_uris, env)) return client;
  await revokeAndDeleteOAuthClient(client.client_id, client.id, env);
  return null;
}

async function purgeNonWhitelistedOAuthClients(env: Env): Promise<number> {
  const clients = await env.DB.prepare(
    'SELECT id, client_id, redirect_uris FROM oauth_clients'
  ).all<{ id: string; client_id: string; redirect_uris: string }>();
  const rows = clients.results ?? [];
  let purged = 0;
  for (const row of rows) {
    const redirectUris = parseJsonStringArray(row.redirect_uris);
    if (hasOnlyWhitelistedRedirectUris(redirectUris, env)) continue;
    await revokeAndDeleteOAuthClient(row.client_id, row.id, env);
    purged += 1;
  }
  return purged;
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
  if (codeChallengeMethod !== 'S256') {
    return { ok: false, message: 'Only S256 code_challenge_method is supported' };
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
    .tertiary{background:#f59e0b;color:#201200}
    .hr{height:1px;background:#24364f;margin:16px 0}
    .meta{margin-top:14px;font-size:11px;color:#7f92a8}
    .version-tag{position:fixed;right:12px;bottom:10px;font-size:11px;color:#7f92a8;letter-spacing:0.04em;user-select:none}
  </style>
</head>
<body>
  <form class="card" method="post" action="/authorize">
    <h1>Connect Your Second Brain</h1>
    <p>Sign in, create an account, or use a legacy API token to authorize this MCP integration.</p>
    ${errorBlock}
    ${hidden}
    <label>Email</label>
    <input type="email" name="email" autocomplete="username" />
    <label>Password</label>
    <input type="password" name="password" autocomplete="current-password" />
    <label>Brain Name (used when signing up)</label>
    <input type="text" name="brain_name" placeholder="My Second Brain" />
    <div class="row">
      <button class="primary" type="submit" name="auth_mode" value="login">Sign In</button>
      <button class="secondary" type="submit" name="auth_mode" value="signup">Sign Up</button>
    </div>
    <div class="hr"></div>
    <label>Legacy API Token</label>
    <input type="password" name="legacy_token" placeholder="sk-... or Bearer ...">
    <div class="row">
      <button class="tertiary" type="submit" name="auth_mode" value="token">Use Legacy Token</button>
    </div>
    <div class="meta">Client: ${escapeHtml(requestData.client_id ?? '')}</div>
  </form>
  <div class="version-tag">v${escapeHtml(SERVER_VERSION)}</div>
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
  const authEnv = withPrimaryDbEnv(env);
  if (request.method === 'GET') {
    const parsed = validateAuthorizeParams(url.searchParams);
    if (!parsed.ok) return oauthError('invalid_request', parsed.message, 400);
    const client = await purgeOAuthClientIfNotWhitelisted(await getOAuthClient(parsed.data.client_id, authEnv), authEnv);
    if (!client) {
      return oauthError('invalid_client', 'client_id is invalid.', 401);
    }
    if (!isAllowedRedirectForClient(client, parsed.data.redirect_uri, authEnv)) {
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
  let userId = '';
  let brainId = '';

  if (authMode === 'token') {
    const legacyToken = normalizeLegacyToken(form.get('legacy_token') ?? form.get('token'));
    if (!legacyToken) {
      return new Response(renderAuthorizePage(parsed.data, 'Please enter your legacy API token.'), {
        status: 400,
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
      });
    }
    if (legacyToken !== env.AUTH_SECRET) {
      return new Response(renderAuthorizePage(parsed.data, 'Invalid legacy API token.'), {
        status: 401,
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
      });
    }
    const principal = await ensureLegacyTokenPrincipal(authEnv);
    userId = principal.userId;
    brainId = principal.brainId;
  } else {
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
    if (authMode === 'signup') {
      if (!isStrongEnoughPassword(passwordRaw)) {
        return new Response(renderAuthorizePage(parsed.data, 'Password must be at least 10 characters.'), {
          status: 400,
          headers: { 'Content-Type': 'text/html; charset=utf-8' },
        });
      }
      const existing = await authEnv.DB.prepare('SELECT id FROM users WHERE email = ? LIMIT 1').bind(email).first<{ id: string }>();
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
      await authEnv.DB.prepare(
        'INSERT INTO users (id, email, password_hash, display_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)'
      ).bind(userId, email, passwordHash, displayName, ts, ts).run();
      brainId = generateId();
      const brainName = sanitizeBrainName(brainNameRaw, email);
      await authEnv.DB.prepare(
        'INSERT INTO brains (id, name, slug, owner_user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)'
      ).bind(brainId, brainName, `${slugify(brainName)}-${brainId.slice(0, 8)}`, userId, ts, ts).run();
      await authEnv.DB.prepare(
        "INSERT INTO brain_memberships (brain_id, user_id, role, created_at) VALUES (?, ?, 'owner', ?)"
      ).bind(brainId, userId, ts).run();
    } else {
      const user = await authEnv.DB.prepare(
        'SELECT id, email, password_hash FROM users WHERE email = ? LIMIT 1'
      ).bind(email).first<{ id: string; email: string; password_hash: string }>();
      if (!user || !(await verifyPassword(passwordRaw, user.password_hash))) {
        return new Response(renderAuthorizePage(parsed.data, 'Invalid email or password.'), {
          status: 401,
          headers: { 'Content-Type': 'text/html; charset=utf-8' },
        });
      }
      userId = user.id;
      const brains = await listBrainsForUser(userId, authEnv);
      const activeBrain = findActiveBrain(brains, '');
      if (!activeBrain) {
        return new Response(renderAuthorizePage(parsed.data, 'No brain found for this account.'), {
          status: 403,
          headers: { 'Content-Type': 'text/html; charset=utf-8' },
        });
      }
      brainId = activeBrain.id;
    }
  }

  const client = await purgeOAuthClientIfNotWhitelisted(await getOAuthClient(parsed.data.client_id, authEnv), authEnv);
  if (!client) {
    return new Response(renderAuthorizePage(parsed.data, 'This OAuth client is not registered.'), {
      status: 401,
      headers: { 'Content-Type': 'text/html; charset=utf-8' },
    });
  }
  if (!isAllowedRedirectForClient(client, parsed.data.redirect_uri, authEnv)) {
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
    authEnv
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
  const authEnv = withPrimaryDbEnv(env);
  if (request.method !== 'POST') return oauthError('invalid_request', 'Method not allowed.', 405);
  const params = await readFormBody(request);
  if (!params) return oauthError('invalid_request', 'Invalid token request body.', 400);
  const basicClient = parseBasicClientAuth(request);

  const grantType = getParam(params, 'grant_type', 'grantType').trim();
  if (grantType === 'authorization_code') {
    const code = getParam(params, 'code').trim();
    const redirectUri = getParam(params, 'redirect_uri', 'redirectUri').trim();
    const codeVerifier = getParam(params, 'code_verifier', 'codeVerifier').trim();
    if (!code || !codeVerifier) {
      return oauthError('invalid_request', 'grant_type=authorization_code requires code and code_verifier.');
    }

    const authCode = await authEnv.DB.prepare(
      `SELECT id, code, client_id, redirect_uri, user_id, brain_id, code_challenge, code_challenge_method, scope, resource, created_at, expires_at, used_at
       FROM oauth_authorization_codes
       WHERE code = ?
       LIMIT 1`
    ).bind(code).first<OAuthCodeRow>();
    if (!authCode || authCode.used_at !== null || authCode.expires_at <= now()) {
      return oauthError('invalid_grant', 'Authorization code is invalid or expired.');
    }
    const authCodeClient = await purgeOAuthClientIfNotWhitelisted(await getOAuthClient(authCode.client_id, authEnv), authEnv);
    if (!authCodeClient) {
      return oauthError('invalid_client', 'client_id is invalid.', 401);
    }

    if (redirectUri && authCode.redirect_uri !== redirectUri) {
      return oauthError('invalid_grant', 'Authorization code does not match redirect_uri.');
    }

    // Validate client_id matches if provided (via body or Basic auth).
    // PKCE code_verifier already proves client identity, so we skip
    // client_secret validation for the authorization_code grant.
    // MCP clients register with client_secret_post but may send creds
    // via Basic auth or omit them entirely — all valid with PKCE.
    const providedClientId = getParam(params, 'client_id', 'clientId').trim() || (basicClient?.clientId ?? '');
    if (providedClientId && providedClientId !== authCode.client_id) {
      return oauthError('invalid_grant', 'Authorization code does not match client_id.');
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

    await authEnv.DB.prepare(
      'UPDATE oauth_authorization_codes SET used_at = ? WHERE id = ?'
    ).bind(now(), authCode.id).run();

    const tokens = await createSessionTokens(authCode.user_id, authCode.brain_id, authEnv, authCode.client_id);
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
    const refreshToken = getParam(params, 'refresh_token', 'refreshToken').trim();
    if (!refreshToken) return oauthError('invalid_request', 'refresh_token is required.');
    const session = await getRefreshSessionByToken(refreshToken, authEnv);
    if (!session || session.revoked_at !== null || session.expires_at <= now()) {
      return oauthError('invalid_grant', 'Refresh token is invalid or expired.');
    }
    if (session.client_id) {
      const refreshClient = await purgeOAuthClientIfNotWhitelisted(await getOAuthClient(session.client_id, authEnv), authEnv);
      if (!refreshClient) {
        return oauthError('invalid_client', 'client_id is invalid.', 401);
      }
    }

    // Skip client_secret validation for refresh tokens — same rationale as
    // authorization_code grant: MCP clients use varying auth methods and
    // the refresh token itself is proof of possession.
    const rotated = await rotateSession(refreshToken, authEnv);
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
  const authEnv = withPrimaryDbEnv(env);
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
  if (!redirectUris.length || redirectUris.some((uri) => !isWhitelistedRedirectUri(uri, authEnv))) {
    return oauthError('invalid_client_metadata', 'All redirect_uris must be valid and use an allowlisted domain (or localhost HTTP).');
  }
  if (!hasOnlyTrustedRedirectUris(redirectUris) && !hasValidAdminBearer(request, authEnv)) {
    return oauthUnauthorized('A valid admin bearer token is required unless every redirect_uri uses a trusted domain.');
  }

  await purgeNonWhitelistedOAuthClients(authEnv);

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
  await authEnv.DB.prepare(
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
    code_challenge_methods_supported: ['S256'],
    scopes_supported: ['mcp:full'],
  });
}

function viewerHtml(): string {
  return `<!DOCTYPE html>
<html lang="en" data-theme="cyberpunk">
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

  /* ── THEME: LIGHT ── */
  [data-theme="light"] {
    --bg: #f5f5f5;
    --bg2: #ffffff;
    --bg3: #e8ecf0;
    --border: #d0d5dc;
    --border-bright: #b0b8c4;
    --amber: #c07800;
    --amber-dim: #a06800;
    --amber-glow: rgba(192,120,0,0.10);
    --teal: #008878;
    --red: #c03030;
    --text: #2c3e50;
    --text-dim: #7a8a9a;
    --text-bright: #1a1a2e;
  }
  [data-theme="light"] body {
    background: linear-gradient(180deg, #f0f2f5 0%, #e4e8ec 100%);
  }
  [data-theme="light"] body::before { display: none; }
  [data-theme="light"] body::after { display: none; }
  [data-theme="light"] .login-box,
  [data-theme="light"] .settings-folder {
    background: var(--bg2);
    box-shadow: 0 1px 4px rgba(0,0,0,0.08);
  }
  [data-theme="light"] .card {
    background: var(--bg2);
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  [data-theme="light"] .settings-folder[open] {
    background: var(--bg2);
  }
  [data-theme="light"] .setting-row {
    background: var(--bg3);
  }
  [data-theme="light"] .cmd-box,
  [data-theme="light"] .shortcuts-box,
  [data-theme="light"] .settings-box,
  [data-theme="light"] .changelog-box {
    background: var(--bg2);
    box-shadow: 0 8px 32px rgba(0,0,0,0.12);
  }
  [data-theme="light"] .expand-overlay {
    background: rgba(245,245,245,0.92);
  }
  [data-theme="light"] .expand-box {
    background: var(--bg2);
    box-shadow: 0 8px 32px rgba(0,0,0,0.10);
  }

  /* ── THEME: MIDNIGHT ── */
  [data-theme="midnight"] {
    --bg: #0a0a1a;
    --bg2: #10102a;
    --bg3: #16163a;
    --border: #2a2a5a;
    --border-bright: #3c3c7a;
    --amber: #7c6aff;
    --amber-dim: #4a3fb0;
    --amber-glow: rgba(124,106,255,0.12);
    --teal: #60ddff;
    --red: #ff5a7a;
    --text: #c8ccf0;
    --text-dim: #5a5e8a;
    --text-bright: #e8eaff;
  }
  [data-theme="midnight"] body {
    background:
      radial-gradient(circle at 20% 20%, rgba(124,106,255,0.08), transparent 40%),
      radial-gradient(circle at 80% 80%, rgba(96,221,255,0.06), transparent 40%),
      linear-gradient(180deg, #0a0a1a 0%, #060614 100%);
  }

  /* ── THEME: SOLARIZED ── */
  [data-theme="solarized"] {
    --bg: #002b36;
    --bg2: #073642;
    --bg3: #0a3f4c;
    --border: #1a5a68;
    --border-bright: #2a7a88;
    --amber: #b58900;
    --amber-dim: #7a5c00;
    --amber-glow: rgba(181,137,0,0.12);
    --teal: #2aa198;
    --red: #dc322f;
    --text: #93a1a1;
    --text-dim: #586e75;
    --text-bright: #eee8d5;
  }
  [data-theme="solarized"] body {
    background: linear-gradient(180deg, #002b36 0%, #001f28 100%);
  }

  /* ── THEME: EMBER ── */
  [data-theme="ember"] {
    --bg: #1a0a08;
    --bg2: #241210;
    --bg3: #2e1a16;
    --border: #4a2a22;
    --border-bright: #6a3a30;
    --amber: #ff6b35;
    --amber-dim: #a84420;
    --amber-glow: rgba(255,107,53,0.12);
    --teal: #ffb347;
    --red: #ff4444;
    --text: #e8d0c8;
    --text-dim: #7a5a50;
    --text-bright: #fff0e8;
  }
  [data-theme="ember"] body {
    background:
      radial-gradient(circle at 30% 70%, rgba(255,107,53,0.08), transparent 40%),
      radial-gradient(circle at 70% 20%, rgba(255,179,71,0.06), transparent 40%),
      linear-gradient(180deg, #1a0a08 0%, #120604 100%);
  }

  /* ── THEME: ARCTIC ── */
  [data-theme="arctic"] {
    --bg: #0c1820;
    --bg2: #122430;
    --bg3: #183040;
    --border: #284860;
    --border-bright: #386080;
    --amber: #40c8e0;
    --amber-dim: #2090a8;
    --amber-glow: rgba(64,200,224,0.12);
    --teal: #80e8d0;
    --red: #ff6080;
    --text: #c0dce8;
    --text-dim: #506878;
    --text-bright: #e0f4ff;
  }
  [data-theme="arctic"] body {
    background:
      radial-gradient(circle at 50% 0%, rgba(64,200,224,0.10), transparent 50%),
      radial-gradient(circle at 20% 80%, rgba(128,232,208,0.06), transparent 40%),
      linear-gradient(180deg, #0c1820 0%, #081018 100%);
  }

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background:
      radial-gradient(circle at 16% 18%, rgba(0, 200, 180, 0.08), transparent 36%),
      radial-gradient(circle at 84% 8%, rgba(240, 165, 0, 0.08), transparent 34%),
      linear-gradient(180deg, var(--bg) 0%, #06090d 100%);
    color: var(--text);
    font-family: var(--mono);
    min-height: 100vh;
    overflow-x: hidden;
    position: relative;
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
    animation: scanlineDrift 14s linear infinite;
  }

  body::after {
    content: '';
    position: fixed;
    inset: -20%;
    z-index: -1;
    pointer-events: none;
    background:
      radial-gradient(42% 36% at 72% 18%, rgba(0, 200, 180, 0.12), transparent 70%),
      radial-gradient(45% 40% at 20% 72%, rgba(240, 165, 0, 0.1), transparent 70%);
    animation: ambientShift 18s ease-in-out infinite alternate;
  }
  body.scanlines-off::before {
    display: none;
  }
  body.motion-reduced *,
  body.motion-reduced *::before,
  body.motion-reduced *::after {
    animation: none !important;
    transition: none !important;
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
    animation: vaultEnter 0.85s cubic-bezier(.18,.79,.26,.99);
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
    background-size: 220% 100%;
    animation: lineSweep 2.4s linear infinite;
  }
  .vault-logo {
    display: flex;
    align-items: baseline;
    justify-content: center;
    flex-wrap: wrap;
    gap: 0.1em;
    font-family: var(--sans);
    font-weight: 800;
    font-size: clamp(1.55rem, 7vw, 2.2rem);
    line-height: 1.05;
    letter-spacing: -0.02em;
    color: var(--text-bright);
    margin-bottom: 0.3rem;
    text-align: center;
    animation: logoReveal 0.75s ease-out both;
  }
  .vault-logo .vault-accent { color: var(--amber); }
  .vault-sub {
    font-size: 0.68rem;
    letter-spacing: 0.2em;
    color: var(--text-dim);
    text-transform: uppercase;
    margin-bottom: 2.5rem;
    text-align: center;
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
  #app { display: none; flex-direction: column; min-height: 100vh; animation: appEnter 0.45s ease; }

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
    backdrop-filter: blur(4px);
  }
  .hdr-brand {
    font-family: var(--sans);
    font-weight: 800;
    font-size: 1.2rem;
    letter-spacing: -0.02em;
    color: var(--text-bright);
    position: absolute;
    left: 50%;
    transform: translateX(-50%);
    pointer-events: none;
    animation: textGlow 5s ease-in-out infinite;
  }
  .hdr-brand span { color: var(--amber); }
  .hdr-right {
    margin-left: auto;
    display: flex;
    align-items: center;
  }
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
    transition: border-color 0.2s, color 0.2s, transform 0.2s;
    margin-left: 1.5rem;
    text-transform: uppercase;
  }
  .logout-btn:hover { border-color: var(--red); color: var(--red); transform: translateY(-1px); }

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
    transition: background 0.15s, transform 0.2s, box-shadow 0.2s;
    position: relative;
    transform: translateY(0);
  }
  .stat-pill:hover, .stat-pill.active { background: var(--bg3); transform: translateY(-2px); box-shadow: inset 0 -1px 0 rgba(255,255,255,0.04); }
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
    transition: transform 0.25s ease;
  }
  .stat-pill.pulse .stat-num { animation: countPulse 0.45s ease; }
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
    transition: all 0.2s;
    text-transform: uppercase;
  }
  .refresh-btn:hover { color: var(--teal); border-color: var(--teal); }
  .refresh-btn.syncing {
    color: var(--teal);
    border-color: var(--teal);
    box-shadow: 0 0 0 1px rgba(0,200,180,0.25), 0 0 18px rgba(0,200,180,0.2);
    animation: syncPulse 0.8s ease-in-out infinite alternate;
  }
  .utility-btn {
    border-color: var(--border);
    color: var(--text-dim);
  }
  .utility-btn:hover {
    border-color: var(--amber);
    color: var(--amber);
  }

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

  #graph-view {
    opacity: 0;
    transform: translateY(10px);
    transition: opacity 0.25s ease, transform 0.25s ease;
  }
  #graph-view.visible {
    opacity: 1;
    transform: translateY(0);
  }

  /* Memory card */
  .card {
    background: var(--bg2);
    padding: 1.25rem 1.5rem;
    position: relative;
    transition: background 0.2s, transform 0.2s, box-shadow 0.2s;
    animation: slideUp 0.3s ease backwards;
    cursor: default;
    overflow: hidden;
    transform: translateY(0);
  }
  .card::before {
    content: '';
    position: absolute;
    inset: 0;
    background: linear-gradient(110deg, transparent 0%, rgba(255,255,255,0.08) 48%, transparent 72%);
    transform: translateX(-140%);
    transition: transform 0.5s ease;
    pointer-events: none;
  }
  .card:hover {
    background: var(--bg3);
    transform: translateY(-3px);
    box-shadow: 0 10px 20px rgba(0, 0, 0, 0.28);
  }
  .card:hover::before { transform: translateX(140%); }
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
    cursor: pointer; transition: border-color 0.15s, color 0.15s, transform 0.15s;
    font-size: 0.72rem; color: var(--text);
  }
  .connection-chip:hover { border-color: var(--amber); color: var(--amber); transform: translateX(2px); }

  .live-dot {
    display: inline-block;
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--teal);
    margin-right: 4px;
    box-shadow: 0 0 0 rgba(0, 200, 180, 0.4);
    animation: livePulse 1.9s infinite;
  }
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
  .graph-node circle { stroke-width: 2px; cursor: pointer; transition: r 0.15s, opacity 0.18s, stroke-opacity 0.18s; }
  .graph-node circle:hover { r: 10; }
  .graph-node text { font-family: var(--mono); font-size: 10px; fill: var(--text); pointer-events: none; transition: opacity 0.18s; }
  .graph-link { stroke-width: 1.5px; transition: stroke-opacity 0.18s; }
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
  .graph-btn.off { opacity: 0.6; border-color: var(--border); color: var(--text-dim); }
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
  .toast-wrap {
    position: fixed;
    right: 0.85rem;
    bottom: 0.85rem;
    display: flex;
    flex-direction: column;
    gap: 0.45rem;
    z-index: 320;
    pointer-events: none;
  }
  .toast {
    border: 1px solid var(--border-bright);
    background: rgba(13, 18, 25, 0.96);
    color: var(--text);
    font-size: 0.68rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 0.45rem 0.6rem;
    min-width: 190px;
    max-width: min(80vw, 420px);
    line-height: 1.45;
    box-shadow: 0 10px 22px rgba(0, 0, 0, 0.28);
    animation: toastIn 0.2s ease;
  }
  .toast.info { border-color: var(--border-bright); color: var(--text); }
  .toast.success { border-color: var(--teal); color: var(--teal); }
  .toast.error { border-color: var(--red); color: var(--red); }
  .toast.hide { animation: toastOut 0.2s ease forwards; }
  .cmd-overlay {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 300;
    background: rgba(6, 10, 15, 0.86);
    padding: 6vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .cmd-overlay.open { display: flex; }
  .cmd-box {
    width: min(700px, 100%);
    border: 1px solid var(--border-bright);
    background: rgba(13, 18, 25, 0.98);
    box-shadow: 0 26px 50px rgba(0, 0, 0, 0.45);
  }
  .cmd-head {
    padding: 0.8rem;
    border-bottom: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    gap: 0.45rem;
  }
  .cmd-input {
    width: 100%;
    border: 1px solid var(--border);
    background: var(--bg3);
    color: var(--teal);
    font-family: var(--mono);
    font-size: 0.82rem;
    letter-spacing: 0.06em;
    padding: 0.6rem 0.72rem;
    outline: none;
  }
  .cmd-input:focus { border-color: var(--amber); }
  .cmd-input::placeholder { color: var(--text-dim); }
  .cmd-hint {
    color: var(--text-dim);
    font-size: 0.56rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
  }
  .cmd-list {
    max-height: min(62vh, 480px);
    overflow-y: auto;
  }
  .cmd-item {
    width: 100%;
    border: none;
    border-bottom: 1px solid var(--border);
    background: transparent;
    color: var(--text);
    text-align: left;
    cursor: pointer;
    padding: 0.66rem 0.82rem;
    display: flex;
    justify-content: space-between;
    gap: 0.65rem;
    font-family: var(--mono);
  }
  .cmd-item:hover, .cmd-item.active {
    background: rgba(240, 165, 0, 0.1);
  }
  .cmd-item-label {
    font-size: 0.75rem;
    letter-spacing: 0.04em;
    color: var(--text-bright);
  }
  .cmd-item-detail {
    font-size: 0.62rem;
    color: var(--text-dim);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    text-align: right;
  }
  .cmd-empty {
    color: var(--text-dim);
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.85rem;
  }
  .shortcuts-overlay {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 290;
    background: rgba(6, 10, 15, 0.84);
    padding: 8vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .shortcuts-overlay.open { display: flex; }
  .shortcuts-box {
    width: min(620px, 100%);
    border: 1px solid var(--border-bright);
    background: rgba(13, 18, 25, 0.98);
    box-shadow: 0 20px 42px rgba(0, 0, 0, 0.42);
    padding: 0.9rem;
  }
  .shortcuts-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.55rem;
    margin-bottom: 0.8rem;
  }
  .shortcuts-head h3 {
    color: var(--amber);
    font-size: 0.72rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    font-weight: 700;
  }
  .shortcuts-close {
    border: 1px solid var(--border);
    background: transparent;
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.62rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.25rem 0.48rem;
    cursor: pointer;
  }
  .shortcuts-close:hover { border-color: var(--amber); color: var(--amber); }
  .shortcuts-grid {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 0.5rem 0.8rem;
    align-items: center;
  }
  .shortcut-key {
    border: 1px solid var(--border);
    background: var(--bg3);
    color: var(--teal);
    font-size: 0.58rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.2rem 0.36rem;
    min-width: 88px;
    text-align: center;
  }
  .shortcut-desc {
    color: var(--text);
    font-size: 0.72rem;
    line-height: 1.45;
  }
  .settings-overlay {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 295;
    background: rgba(6, 10, 15, 0.84);
    padding: 8vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .settings-overlay.open { display: flex; }
  .settings-box {
    width: min(760px, 100%);
    max-height: min(84vh, 820px);
    border: 1px solid var(--border-bright);
    background: rgba(13, 18, 25, 0.98);
    box-shadow: 0 20px 42px rgba(0, 0, 0, 0.42);
    padding: 0.9rem;
    display: flex;
    flex-direction: column;
  }
  .settings-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.55rem;
    margin-bottom: 0.8rem;
  }
  .settings-head-main {
    display: flex;
    align-items: center;
    gap: 0.55rem;
    flex-wrap: wrap;
  }
  .settings-head h3 {
    color: var(--amber);
    font-size: 0.72rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    font-weight: 700;
  }
  .settings-version {
    border: 1px solid var(--border);
    background: var(--bg3);
    color: var(--teal);
    font-size: 0.56rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 0.18rem 0.4rem;
  }
  .settings-close {
    border: 1px solid var(--border);
    background: transparent;
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.62rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.25rem 0.48rem;
    cursor: pointer;
  }
  .settings-close:hover { border-color: var(--amber); color: var(--amber); }
  .settings-scroll {
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding-right: 0.15rem;
    margin-right: -0.15rem;
  }
  .settings-sections {
    display: flex;
    flex-direction: column;
    gap: 0.55rem;
  }
  .settings-folder {
    border: 1px solid var(--border);
    background: rgba(8, 12, 16, 0.64);
  }
  .settings-folder[open] {
    border-color: var(--border-bright);
    background: rgba(10, 15, 21, 0.8);
  }
  .settings-folder summary {
    list-style: none;
    cursor: pointer;
    padding: 0.5rem 0.62rem;
    color: var(--teal);
    font-size: 0.62rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.6rem;
  }
  .settings-folder summary::-webkit-details-marker { display: none; }
  .settings-folder summary::after {
    content: '+';
    color: var(--amber);
    font-size: 0.82rem;
    line-height: 1;
  }
  .settings-folder[open] summary::after {
    content: '-';
  }
  .settings-folder-body {
    border-top: 1px solid var(--border);
    padding: 0.55rem;
  }
  .settings-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.5rem 0.75rem;
  }
  .setting-row {
    border: 1px solid var(--border);
    background: rgba(8, 12, 16, 0.64);
    padding: 0.55rem 0.62rem;
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
  }
  .setting-row.setting-inline {
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
    gap: 0.6rem;
  }
  .setting-row.setting-span-2 { grid-column: 1 / -1; }
  .setting-row label,
  .setting-row .setting-label {
    color: var(--text);
    font-size: 0.66rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    line-height: 1.35;
  }
  .setting-row .setting-help {
    color: var(--text-dim);
    font-size: 0.58rem;
    letter-spacing: 0.08em;
    line-height: 1.35;
  }
  .setting-input {
    border: 1px solid var(--border);
    background: var(--bg3);
    color: var(--teal);
    font-family: var(--mono);
    font-size: 0.75rem;
    letter-spacing: 0.04em;
    outline: none;
    padding: 0.4rem 0.5rem;
    min-height: 30px;
  }
  .setting-input:focus { border-color: var(--amber); }
  .setting-check {
    width: 18px;
    height: 18px;
    accent-color: var(--teal);
  }
  .semantic-status-box {
    border: 1px solid var(--border);
    background: var(--bg3);
    padding: 0.55rem;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  .semantic-status-line {
    color: var(--text);
    font-size: 0.64rem;
    letter-spacing: 0.08em;
    line-height: 1.45;
    word-break: break-word;
  }
  .semantic-status-line.error { color: var(--red); }
  .semantic-status-line.dim { color: var(--text-dim); }
  .semantic-status-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 0.35rem;
  }
  .semantic-status-pill {
    border: 1px solid var(--border);
    background: rgba(8, 12, 16, 0.78);
    color: var(--teal);
    font-size: 0.54rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.14rem 0.32rem;
  }
  .semantic-status-pill.ready { color: #2eca75; border-color: rgba(46, 202, 117, 0.45); }
  .semantic-status-pill.not-ready { color: var(--amber); border-color: rgba(240, 165, 0, 0.45); }
  .semantic-status-pill.running { color: #66a9ff; border-color: rgba(102, 169, 255, 0.45); }
  .settings-actions {
    display: flex;
    gap: 0.5rem;
    justify-content: flex-end;
    flex-wrap: wrap;
    margin-top: 0.7rem;
  }
  .theme-picker {
    display: flex;
    gap: 0.5rem;
    flex-wrap: wrap;
    margin-top: 0.25rem;
  }
  .theme-swatch {
    width: 40px;
    height: 40px;
    background: transparent;
    border: 2px solid var(--border);
    cursor: pointer;
    padding: 3px;
    transition: border-color 0.15s, transform 0.1s;
    position: relative;
  }
  .theme-swatch:hover {
    border-color: var(--amber);
    transform: scale(1.1);
  }
  .theme-swatch.active {
    border-color: var(--amber);
    box-shadow: 0 0 8px var(--amber-glow);
  }
  .theme-swatch.active::after {
    content: '✓';
    position: absolute;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    color: white;
    font-size: 0.7rem;
    font-weight: 700;
    text-shadow: 0 1px 3px rgba(0,0,0,0.6);
  }
  .theme-swatch span {
    display: block;
    width: 100%;
    height: 100%;
  }
  .changelog-overlay {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 296;
    background: rgba(6, 10, 15, 0.84);
    padding: 8vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .changelog-overlay.open { display: flex; }
  .changelog-box {
    width: min(860px, 100%);
    border: 1px solid var(--border-bright);
    background: rgba(13, 18, 25, 0.98);
    box-shadow: 0 20px 42px rgba(0, 0, 0, 0.42);
    padding: 0.9rem;
  }
  .changelog-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 0.55rem;
    margin-bottom: 0.8rem;
  }
  .changelog-title-group h3 {
    color: var(--amber);
    font-size: 0.72rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    font-weight: 700;
    margin-bottom: 0.25rem;
  }
  .changelog-subtitle {
    color: var(--text-dim);
    font-size: 0.58rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    line-height: 1.4;
  }
  .changelog-list {
    border: 1px solid var(--border);
    background: rgba(8, 12, 16, 0.64);
    padding: 0.7rem;
    max-height: min(62vh, 720px);
    overflow: auto;
    display: flex;
    flex-direction: column;
    gap: 0.65rem;
  }
  .changelog-entry {
    border: 1px solid var(--border);
    background: var(--bg3);
    padding: 0.6rem;
  }
  .changelog-entry-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.45rem;
    margin-bottom: 0.35rem;
    flex-wrap: wrap;
  }
  .changelog-entry-version {
    color: var(--teal);
    font-size: 0.63rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
  }
  .changelog-entry-date {
    color: var(--text-dim);
    font-size: 0.58rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
  }
  .changelog-entry-summary {
    color: var(--text-bright);
    font-size: 0.74rem;
    line-height: 1.45;
    margin-bottom: 0.4rem;
  }
  .changelog-change-list {
    list-style: none;
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }
  .changelog-change-row {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 0.45rem;
    align-items: start;
  }
  .changelog-change-type {
    border: 1px solid var(--border);
    color: var(--amber);
    font-size: 0.54rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 0.08rem 0.28rem;
    white-space: nowrap;
  }
  .changelog-change-text {
    color: var(--text);
    font-size: 0.68rem;
    line-height: 1.45;
  }
  body.compact-cards .grid-wrap {
    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 1px;
  }
  body.compact-cards .card {
    padding: 0.95rem 1rem;
  }
  body.compact-cards .card-content {
    font-size: 0.74rem;
    max-height: 88px;
  }
  body.compact-cards .card-footer {
    margin-top: 0.65rem;
    padding-top: 0.55rem;
  }
  body.compact-cards .card-id {
    font-size: 0.5rem;
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
    .hdr-brand {
      position: static;
      transform: none;
      pointer-events: auto;
    }
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
    .utility-btn { width: 100%; }

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
    .toast-wrap { left: 0.65rem; right: 0.65rem; bottom: 0.65rem; }
    .toast { max-width: none; }
    .cmd-overlay { padding-top: 3vh; }
    .cmd-head { padding: 0.62rem; }
    .cmd-item { padding: 0.54rem 0.62rem; }
    .cmd-item-label { font-size: 0.68rem; }
    .cmd-item-detail { font-size: 0.56rem; }
    .shortcuts-overlay { padding-top: 5vh; }
    .shortcuts-box { padding: 0.62rem; }
    .shortcut-key { min-width: 74px; }
    .shortcut-desc { font-size: 0.68rem; }
    .settings-overlay { padding-top: 5vh; }
    .settings-box { padding: 0.62rem; max-height: 90vh; }
    .settings-scroll { padding-right: 0; margin-right: 0; }
    .settings-grid { grid-template-columns: 1fr; }
    .settings-actions { justify-content: stretch; }
    .settings-actions .refresh-btn { width: 100%; }
    .changelog-overlay { padding-top: 5vh; }
    .changelog-box { padding: 0.62rem; }
    .changelog-list { max-height: min(60vh, 560px); }
    .changelog-change-row { grid-template-columns: 1fr; gap: 0.25rem; }
  }
  @media (prefers-reduced-motion: reduce) {
    *, *::before, *::after {
      animation: none !important;
      transition: none !important;
    }
    #graph-view {
      opacity: 1 !important;
      transform: none !important;
    }
  }
  @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
  @keyframes appEnter { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; transform: translateY(0); } }
  @keyframes slideUp { from { opacity: 0; transform: translateY(16px); } to { opacity: 1; transform: translateY(0); } }
  @keyframes blink { 0%,100% { opacity: 1; } 50% { opacity: 0; } }
  @keyframes scanlineDrift { from { transform: translateY(0); } to { transform: translateY(12px); } }
  @keyframes ambientShift {
    0% { transform: translate3d(-2%, -1%, 0) scale(1); opacity: 0.65; }
    100% { transform: translate3d(2%, 1%, 0) scale(1.06); opacity: 1; }
  }
  @keyframes vaultEnter {
    0% { opacity: 0; transform: translateY(18px) scale(0.98); }
    100% { opacity: 1; transform: translateY(0) scale(1); }
  }
  @keyframes logoReveal {
    0% { opacity: 0; transform: translateY(8px); letter-spacing: 0.02em; }
    100% { opacity: 1; transform: translateY(0); letter-spacing: -0.02em; }
  }
  @keyframes lineSweep {
    0% { background-position: 0% 50%; }
    100% { background-position: 200% 50%; }
  }
  @keyframes textGlow {
    0%, 100% { text-shadow: 0 0 0 rgba(0, 200, 180, 0); }
    50% { text-shadow: 0 0 12px rgba(0, 200, 180, 0.2); }
  }
  @keyframes countPulse {
    0% { transform: scale(1); }
    40% { transform: scale(1.12); }
    100% { transform: scale(1); }
  }
  @keyframes syncPulse {
    0% { box-shadow: 0 0 0 1px rgba(0,200,180,0.2), 0 0 8px rgba(0,200,180,0.12); }
    100% { box-shadow: 0 0 0 1px rgba(0,200,180,0.45), 0 0 18px rgba(0,200,180,0.24); }
  }
  @keyframes livePulse {
    0% { transform: scale(1); box-shadow: 0 0 0 0 rgba(0, 200, 180, 0.35); opacity: 1; }
    70% { transform: scale(1.15); box-shadow: 0 0 0 8px rgba(0, 200, 180, 0); opacity: 0.9; }
    100% { transform: scale(1); box-shadow: 0 0 0 0 rgba(0, 200, 180, 0); opacity: 1; }
  }
  @keyframes toastIn {
    0% { opacity: 0; transform: translateY(8px); }
    100% { opacity: 1; transform: translateY(0); }
  }
  @keyframes toastOut {
    0% { opacity: 1; transform: translateY(0); }
    100% { opacity: 0; transform: translateY(8px); }
  }
</style>
</head>
<body>

<!-- LOGIN -->
<div id="login-screen">
  <div class="login-box">
    <div class="vault-logo"><span>MEMORY</span><span class="vault-accent">VAULT</span></div>
    <div class="vault-sub">Secure Access Required</div>
    <div class="field-label">Email</div>
    <input type="email" class="token-input" id="email-input" placeholder="you@example.com" autocomplete="username" autocapitalize="off" autocorrect="off" spellcheck="false">
    <div class="field-label" style="margin-top:0.75rem">Password</div>
    <input type="password" class="token-input" id="password-input" placeholder="Enter password" autocomplete="current-password">
    <div class="field-label" style="margin-top:0.75rem">Brain Name (for signup)</div>
    <input type="text" class="token-input" id="brain-name-input" placeholder="Second Brain name (optional)" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false">
    <div class="login-btn-row">
      <button class="login-btn" data-action="login">SIGN IN →</button>
      <button class="login-btn secondary" data-action="signup">SIGN UP →</button>
    </div>
    <div class="field-label" style="margin-top:1rem">Legacy Access Token</div>
    <input type="password" class="token-input" id="token-input" placeholder="Bearer token (legacy mode)" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false">
    <button class="login-btn secondary token-btn" data-action="token-login">TOKEN LOGIN →</button>
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
        <span class="live-dot"></span>LIVE
      </div>
      <button class="logout-btn" data-action="logout">LOCK</button>
    </div>
  </header>

  <div class="stats-bar">
    <div class="stat-pill active" id="stat-all" data-action="set-filter" data-filter="">
      <div class="stat-num" id="count-all">0</div>
      <div class="stat-label">All</div>
    </div>
    <div class="stat-pill" id="stat-note" data-action="set-filter" data-filter="note">
      <div class="stat-num" id="count-note">0</div>
      <div class="stat-label">Notes</div>
    </div>
    <div class="stat-pill" id="stat-fact" data-action="set-filter" data-filter="fact">
      <div class="stat-num" id="count-fact">0</div>
      <div class="stat-label">Facts</div>
    </div>
    <div class="stat-pill" id="stat-journal" data-action="set-filter" data-filter="journal">
      <div class="stat-num" id="count-journal">0</div>
      <div class="stat-label">Journal</div>
    </div>
    <div class="stat-pill" id="stat-graph" data-action="show-graph">
      <div class="stat-num">⬡</div>
      <div class="stat-label">Graph</div>
    </div>
  </div>

  <div class="controls">
    <div class="search-wrap">
      <input type="text" class="search-input" id="search-input" placeholder="Search by name, id, key, or text..." inputmode="search">
    </div>
    <button class="refresh-btn" data-action="refresh-memories">↻ REFRESH</button>
    <button class="refresh-btn utility-btn" data-action="open-command-palette">COMMAND</button>
    <button class="refresh-btn utility-btn" data-action="toggle-shortcuts-overlay">SHORTCUTS</button>
    <button class="refresh-btn utility-btn" data-action="open-settings-overlay">SETTINGS</button>
  </div>

  <div id="graph-view" style="display:none;flex:1;position:relative;background:var(--bg);min-height:600px">
    <div class="graph-toolbar">
      <div class="graph-toolbar-row">
        <input type="text" class="graph-search-input" id="graph-search-input" placeholder="Search graph..." inputmode="search">
      </div>
      <div class="graph-toolbar-row">
        <button class="graph-btn active" id="graph-toggle-inferred" data-action="toggle-graph-inferred">INFERRED ON</button>
        <button class="graph-btn active" id="graph-toggle-labels" data-action="toggle-graph-labels">LABELS ON</button>
        <button class="graph-btn active" id="graph-toggle-physics" data-action="toggle-graph-physics">PHYSICS ON</button>
        <button class="graph-btn" data-action="reset-graph-view">RESET VIEW</button>
      </div>
      <div class="graph-toolbar-row">
        <button class="graph-btn relation active" id="graph-rel-related" data-action="toggle-graph-relation" data-relation="related">RELATED</button>
        <button class="graph-btn relation active" id="graph-rel-supports" data-action="toggle-graph-relation" data-relation="supports">SUPPORTS</button>
        <button class="graph-btn relation active" id="graph-rel-contradicts" data-action="toggle-graph-relation" data-relation="contradicts">CONTRA</button>
        <button class="graph-btn relation active" id="graph-rel-supersedes" data-action="toggle-graph-relation" data-relation="supersedes">SUPER</button>
        <button class="graph-btn relation active" id="graph-rel-causes" data-action="toggle-graph-relation" data-relation="causes">CAUSES</button>
        <button class="graph-btn relation active" id="graph-rel-example_of" data-action="toggle-graph-relation" data-relation="example_of">EXAMPLE</button>
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
<div class="expand-overlay" id="expand-overlay" data-action="close-expand-overlay">
  <div class="expand-box">
    <button class="expand-close" data-action="close-expand">✕ CLOSE</button>
    <div id="expand-header"></div>
    <div class="expand-content" id="expand-content"></div>
    <div style="margin-top:1.5rem;padding-top:1rem;border-top:1px solid var(--border);font-size:0.6rem;color:var(--text-dim);letter-spacing:0.08em" id="expand-meta"></div>
    <div id="expand-connections"></div>
  </div>
</div>

<div class="cmd-overlay" id="cmd-overlay" data-action="close-command-palette-overlay">
  <div class="cmd-box">
    <div class="cmd-head">
      <input type="text" class="cmd-input" id="cmd-input" placeholder="Run an action..." autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false">
      <div class="cmd-hint">enter run - esc close - arrows move</div>
    </div>
    <div class="cmd-list" id="cmd-list"></div>
  </div>
</div>

<div class="shortcuts-overlay" id="shortcuts-overlay" data-action="close-shortcuts-overlay">
  <div class="shortcuts-box">
    <div class="shortcuts-head">
      <h3>Keyboard Shortcuts</h3>
      <button class="shortcuts-close" data-action="close-shortcuts">Close</button>
    </div>
    <div class="shortcuts-grid">
      <span class="shortcut-key">Ctrl/Cmd+K</span><span class="shortcut-desc">Open command palette</span>
      <span class="shortcut-key">?</span><span class="shortcut-desc">Open this shortcuts panel</span>
      <span class="shortcut-key">S</span><span class="shortcut-desc">Open settings panel</span>
      <span class="shortcut-key">/</span><span class="shortcut-desc">Focus search input</span>
      <span class="shortcut-key">G</span><span class="shortcut-desc">Open graph view</span>
      <span class="shortcut-key">R</span><span class="shortcut-desc">Refresh memories</span>
      <span class="shortcut-key">Esc</span><span class="shortcut-desc">Close overlays or modal cards</span>
      <span class="shortcut-key">Enter</span><span class="shortcut-desc">Run selected command in command palette</span>
    </div>
  </div>
</div>

<div class="settings-overlay" id="settings-overlay" data-action="close-settings-overlay">
  <div class="settings-box">
    <div class="settings-head">
      <div class="settings-head-main">
        <h3>Viewer Settings</h3>
        <span class="settings-version">v${escapeHtml(SERVER_VERSION)}</span>
      </div>
      <button class="settings-close" data-action="close-settings">Close</button>
    </div>
    <div class="settings-scroll">
      <div class="settings-sections">
        <details class="settings-folder" open>
          <summary>General & Search</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Live Polling</div>
                  <div class="setting-help">Auto-refresh memory stats in background.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-live-poll-enabled">
              </div>
              <div class="setting-row">
                <label for="settings-live-poll-interval">Polling Interval (sec)</label>
                <input type="number" min="5" max="120" step="1" class="setting-input" id="settings-live-poll-interval">
                <div class="setting-help">Lower is faster updates, higher is lighter load.</div>
              </div>
              <div class="setting-row">
                <label for="settings-time-mode">Time Display</label>
                <select class="setting-input" id="settings-time-mode">
                  <option value="utc">UTC</option>
                  <option value="local">Local</option>
                </select>
                <div class="setting-help">Header clock format mode.</div>
              </div>
              <div class="setting-row">
                <label for="settings-default-filter">Default Startup Filter</label>
                <select class="setting-input" id="settings-default-filter">
                  <option value="">All</option>
                  <option value="note">Notes</option>
                  <option value="fact">Facts</option>
                  <option value="journal">Journal</option>
                </select>
                <div class="setting-help">Initial list filter after sign-in.</div>
              </div>
              <div class="setting-row">
                <label for="settings-search-debounce">Search Debounce (ms)</label>
                <input type="number" min="120" max="1500" step="10" class="setting-input" id="settings-search-debounce">
                <div class="setting-help">Delay before list search triggers.</div>
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Compact Cards</div>
                  <div class="setting-help">Fit more memory cards on screen.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-compact-cards">
              </div>
            </div>
          </div>
        </details>

        <details class="settings-folder">
          <summary>Graph Defaults</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Default Inferred Edges</div>
                  <div class="setting-help">Initial graph inferred-edge visibility.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-graph-inferred">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Default Graph Labels</div>
                  <div class="setting-help">Initial graph label visibility.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-graph-labels">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Default Graph Physics</div>
                  <div class="setting-help">Start graph simulation enabled.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-graph-physics">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Open Graph On Sign-in</div>
                  <div class="setting-help">Skip list view and jump to graph first.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-auto-open-graph">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Graph Hover Focus</div>
                  <div class="setting-help">Highlight node neighborhood on hover.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-graph-focus">
              </div>
            </div>
          </div>
        </details>

        <details class="settings-folder">
          <summary>Appearance & Session</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-span-2">
                <label for="settings-theme">Theme</label>
                <div class="setting-help">Choose a color theme for the viewer.</div>
                <div class="theme-picker" id="theme-picker">
                  <button type="button" class="theme-swatch" data-theme-value="cyberpunk" title="Cyberpunk"><span style="background:#080c10;border:2px solid #f0a500"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="light" title="Light"><span style="background:#f5f5f5;border:2px solid #c07800"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="midnight" title="Midnight"><span style="background:#0a0a1a;border:2px solid #7c6aff"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="solarized" title="Solarized"><span style="background:#002b36;border:2px solid #b58900"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="ember" title="Ember"><span style="background:#1a0a08;border:2px solid #ff6b35"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="arctic" title="Arctic"><span style="background:#0c1820;border:2px solid #40c8e0"></span></button>
                </div>
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Show Scanlines</div>
                  <div class="setting-help">Enable CRT-style scanline overlay.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-show-scanlines">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Reduce Motion</div>
                  <div class="setting-help">Disable most transitions and animations.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-reduce-motion">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Confirm Before Lock</div>
                  <div class="setting-help">Prompt before manual logout/lock.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-confirm-logout">
              </div>
            </div>
          </div>
        </details>

        <details class="settings-folder">
          <summary>Notifications</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Toast Notifications</div>
                  <div class="setting-help">In-app feedback for actions and errors.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-toasts-enabled">
              </div>
              <div class="setting-row">
                <label for="settings-toast-duration">Toast Duration (ms)</label>
                <input type="number" min="1200" max="8000" step="100" class="setting-input" id="settings-toast-duration">
                <div class="setting-help">How long toast messages stay visible.</div>
              </div>
            </div>
          </div>
        </details>

        <details class="settings-folder">
          <summary>Semantic Index</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Semantic Reindex Wait</div>
                  <div class="setting-help">Wait for Vectorize index readiness before reindex returns.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-semantic-wait">
              </div>
              <div class="setting-row">
                <label for="settings-semantic-timeout">Semantic Wait Timeout (sec)</label>
                <input type="number" min="1" max="900" step="1" class="setting-input" id="settings-semantic-timeout">
                <div class="setting-help">Used when Semantic Reindex Wait is enabled.</div>
              </div>
              <div class="setting-row">
                <label for="settings-semantic-limit">Semantic Reindex Limit</label>
                <input type="number" min="1" max="2000" step="1" class="setting-input" id="settings-semantic-limit">
                <div class="setting-help">Maximum memories processed per reindex run.</div>
              </div>
              <div class="setting-row setting-span-2">
                <div class="setting-label">Semantic Index Sync</div>
                <div class="setting-help">Run <code>memory_reindex</code> from the viewer and inspect readiness output.</div>
                <div class="semantic-status-box">
                  <div class="semantic-status-line dim" id="semantic-status-line">No semantic reindex run in this session.</div>
                  <div class="semantic-status-meta" id="semantic-status-meta"></div>
                  <button class="refresh-btn utility-btn" id="semantic-reindex-btn" data-action="run-semantic-reindex">RUN SEMANTIC REINDEX</button>
                </div>
              </div>
            </div>
          </div>
        </details>
      </div>
    </div>
    <div class="settings-actions">
      <button class="refresh-btn utility-btn" data-action="open-changelog-overlay">VIEW CHANGELOG</button>
      <button class="refresh-btn utility-btn" data-action="reset-viewer-settings">RESET DEFAULTS</button>
      <button class="refresh-btn" data-action="apply-settings">SAVE SETTINGS</button>
    </div>
  </div>
</div>

<div class="changelog-overlay" id="changelog-overlay" data-action="close-changelog-overlay">
  <div class="changelog-box">
    <div class="changelog-head">
      <div class="changelog-title-group">
        <h3>Release Changelog</h3>
        <div class="changelog-subtitle" id="changelog-subtitle">Recent platform updates</div>
      </div>
      <button class="settings-close" data-action="close-changelog">Close</button>
    </div>
    <div class="changelog-list" id="changelog-list"></div>
    <div class="settings-actions" style="margin-top:0.7rem">
      <button class="refresh-btn utility-btn" data-action="open-full-changelog">OPEN FULL CHANGELOG</button>
    </div>
  </div>
</div>

<div class="toast-wrap" id="toast-wrap"></div>

<script src="/view.js"></script>
</body>
</html>`;
}

function viewerScript(): string {
  return `
  const BASE = location.origin;
  const VIEWER_SERVER_VERSION = '${escapeHtml(SERVER_VERSION)}';
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
  let SESSION_MODE = 'none';
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
  let graphSimulation = null;
  let graphAutoTunedLabels = false;
  let graphSearchQuery = '';
  let graphRelationFilter = new Set(GRAPH_RELATION_TYPES);
  let graphPhysicsEnabled = true;
  let lastStatsSnapshot = { all: null, note: null, fact: null, journal: null };
  let commandPaletteOpen = false;
  let commandQuery = '';
  let commandVisibleActions = [];
  let commandActiveIndex = 0;
  let toastCounter = 0;
  let clockIntervalId = null;
  const VIEWER_SETTINGS_KEY = 'memoryvault.viewer.settings.v1';
  let viewerSettings = null;
  let semanticReindexRunning = false;
  let semanticReindexLastResult = null;
  let semanticReindexLastError = '';

  function hasAuthenticatedSession() {
    return SESSION_MODE === 'user' || (SESSION_MODE === 'legacy' && !!TOKEN);
  }

  function buildDefaultViewerSettings() {
    return {
      theme: 'cyberpunk',
      live_poll_enabled: true,
      live_poll_interval_sec: 10,
      time_mode: 'utc',
      default_memory_filter: '',
      search_debounce_ms: 300,
      compact_cards: false,
      graph_show_inferred: true,
      graph_show_labels: !window.matchMedia('(max-width: 640px)').matches,
      graph_physics_enabled: true,
      graph_focus_highlight: true,
      auto_open_graph: false,
      toasts_enabled: true,
      toast_duration_ms: 2300,
      confirm_logout: false,
      show_scanlines: true,
      reduce_motion: false,
      semantic_reindex_wait_for_index: true,
      semantic_reindex_wait_timeout_seconds: 180,
      semantic_reindex_limit: 500,
    };
  }

  function normalizeViewerSettings(raw) {
    const defaults = buildDefaultViewerSettings();
    const source = raw && typeof raw === 'object' ? raw : {};
    const intervalRaw = Number(source.live_poll_interval_sec);
    const interval = Number.isFinite(intervalRaw) ? intervalRaw : defaults.live_poll_interval_sec;
    const searchDebounceRaw = Number(source.search_debounce_ms);
    const searchDebounce = Number.isFinite(searchDebounceRaw) ? searchDebounceRaw : defaults.search_debounce_ms;
    const toastDurationRaw = Number(source.toast_duration_ms);
    const toastDuration = Number.isFinite(toastDurationRaw) ? toastDurationRaw : defaults.toast_duration_ms;
    const semanticWaitTimeoutRaw = Number(source.semantic_reindex_wait_timeout_seconds);
    const semanticWaitTimeout = Number.isFinite(semanticWaitTimeoutRaw)
      ? semanticWaitTimeoutRaw
      : defaults.semantic_reindex_wait_timeout_seconds;
    const semanticReindexLimitRaw = Number(source.semantic_reindex_limit);
    const semanticReindexLimit = Number.isFinite(semanticReindexLimitRaw)
      ? semanticReindexLimitRaw
      : defaults.semantic_reindex_limit;
    const defaultFilter = ['note', 'fact', 'journal'].includes(source.default_memory_filter)
      ? source.default_memory_filter
      : '';
    const validThemes = ['cyberpunk', 'light', 'midnight', 'solarized', 'ember', 'arctic'];
    const theme = validThemes.includes(source.theme) ? source.theme : defaults.theme;
    return {
      theme,
      live_poll_enabled: source.live_poll_enabled !== false,
      live_poll_interval_sec: Math.min(Math.max(Math.round(interval), 5), 120),
      time_mode: source.time_mode === 'local' ? 'local' : 'utc',
      default_memory_filter: defaultFilter,
      search_debounce_ms: Math.min(Math.max(Math.round(searchDebounce), 120), 1500),
      compact_cards: source.compact_cards === true,
      graph_show_inferred: source.graph_show_inferred !== false,
      graph_show_labels: source.graph_show_labels === undefined ? defaults.graph_show_labels : source.graph_show_labels !== false,
      graph_physics_enabled: source.graph_physics_enabled !== false,
      graph_focus_highlight: source.graph_focus_highlight !== false,
      auto_open_graph: source.auto_open_graph === true,
      toasts_enabled: source.toasts_enabled !== false,
      toast_duration_ms: Math.min(Math.max(Math.round(toastDuration), 1200), 8000),
      confirm_logout: source.confirm_logout === true,
      show_scanlines: source.show_scanlines !== false,
      reduce_motion: source.reduce_motion === true,
      semantic_reindex_wait_for_index: source.semantic_reindex_wait_for_index !== false,
      semantic_reindex_wait_timeout_seconds: Math.min(Math.max(Math.round(semanticWaitTimeout), 1), 900),
      semantic_reindex_limit: Math.min(Math.max(Math.round(semanticReindexLimit), 1), 2000),
    };
  }

  function loadViewerSettings() {
    const defaults = buildDefaultViewerSettings();
    try {
      const raw = localStorage.getItem(VIEWER_SETTINGS_KEY);
      if (!raw) return defaults;
      const parsed = JSON.parse(raw);
      return normalizeViewerSettings(parsed);
    } catch {
      return defaults;
    }
  }

  function persistViewerSettings() {
    if (!viewerSettings) return;
    try {
      localStorage.setItem(VIEWER_SETTINGS_KEY, JSON.stringify(viewerSettings));
    } catch {}
  }

  function applyViewerSettingsToRuntime(options = {}) {
    if (!viewerSettings) return;
    const restartPolling = options.restartPolling !== false;
    const rerenderGraph = options.rerenderGraph === true;
    const rerenderGrid = options.rerenderGrid === true;
    graphShowInferred = viewerSettings.graph_show_inferred;
    graphShowLabels = viewerSettings.graph_show_labels;
    graphPhysicsEnabled = viewerSettings.graph_physics_enabled;
    document.body.classList.toggle('compact-cards', viewerSettings.compact_cards);
    document.body.classList.toggle('scanlines-off', !viewerSettings.show_scanlines);
    document.body.classList.toggle('motion-reduced', viewerSettings.reduce_motion);
    document.documentElement.setAttribute('data-theme', viewerSettings.theme || 'cyberpunk');
    syncThemePicker();
    syncGraphToolbarState();
    if (restartPolling) startLivePolling(true);
    if (rerenderGrid) renderGrid(allMemories);
    if (rerenderGraph && graphVisible) rerenderGraphFromCache();
  }

  function initializeViewerSettings() {
    viewerSettings = loadViewerSettings();
    applyViewerSettingsToRuntime({ restartPolling: false, rerenderGraph: false, rerenderGrid: false });
  }

  initializeViewerSettings();
  fillSettingsForm();
  restoreUserSession();

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

  function isTypingTarget(target) {
    const el = target instanceof HTMLElement ? target : null;
    if (!el) return false;
    if (el.isContentEditable) return true;
    const tag = (el.tagName || '').toUpperCase();
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true;
    return false;
  }

  function showToast(message, tone = 'info', force = false) {
    const text = String(message || '').trim();
    const wrap = document.getElementById('toast-wrap');
    if (!force && viewerSettings && viewerSettings.toasts_enabled === false) return;
    if (!text || !wrap) return;
    const toast = document.createElement('div');
    const safeTone = ['info', 'success', 'error'].includes(tone) ? tone : 'info';
    toast.className = 'toast ' + safeTone;
    toast.dataset.toastId = String(++toastCounter);
    toast.textContent = text;
    wrap.appendChild(toast);
    const durationMs = Math.min(Math.max(Number(viewerSettings?.toast_duration_ms ?? 2300), 1200), 8000);
    setTimeout(() => {
      toast.classList.add('hide');
      setTimeout(() => toast.remove(), 220);
    }, durationMs);
  }

  function enterApp() {
    document.getElementById('login-screen').style.display = 'none';
    document.getElementById('app').style.display = 'flex';
    document.getElementById('app').style.flexDirection = 'column';
    startClock();
    const defaultFilter = viewerSettings?.default_memory_filter || '';
    activeFilter = defaultFilter;
    syncFilterPills(activeFilter);
    loadMemories();
    startLivePolling();
    showToast('Session active. Loading memory stream.', 'success');
    if (viewerSettings && viewerSettings.auto_open_graph) {
      setTimeout(() => { if (hasAuthenticatedSession()) showGraph(); }, 180);
    }
  }

  async function tryRefreshSession() {
    try {
      const r = await fetch(BASE + '/auth/refresh', {
        method: 'POST',
        credentials: 'same-origin',
      });
      if (!r.ok) return false;
      SESSION_MODE = 'user';
      return true;
    } catch {
      return false;
    }
  }

  async function restoreUserSession() {
    if (hasAuthenticatedSession()) return true;
    try {
      let r = await fetch(BASE + '/auth/me', { credentials: 'same-origin' });
      if (r.status === 401) {
        const refreshed = await tryRefreshSession();
        if (!refreshed) return false;
        r = await fetch(BASE + '/auth/me', { credentials: 'same-origin' });
      }
      if (!r.ok) return false;
      SESSION_MODE = 'user';
      enterApp();
      return true;
    } catch {
      return false;
    }
  }

  async function apiFetch(url, options = {}, allowRefresh = true) {
    const mergedHeaders = Object.assign({}, options.headers || {});
    if (SESSION_MODE === 'legacy' && TOKEN) {
      mergedHeaders.Authorization = 'Bearer ' + TOKEN;
    }
    const response = await fetch(url, Object.assign({ credentials: 'same-origin' }, options, { headers: mergedHeaders }));
    if (response.status === 401 && allowRefresh && SESSION_MODE === 'user') {
      const refreshed = await tryRefreshSession();
      if (refreshed) return apiFetch(url, options, false);
    }
    return response;
  }

  async function callMcpTool(name, args = {}, requestId = '') {
    const response = await apiFetch(BASE + '/mcp', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: requestId || ('viewer-' + name + '-' + Date.now()),
        method: 'tools/call',
        params: { name, arguments: args },
      }),
    });
    if (response.status === 401) {
      doLogout(true);
      throw new Error('Session expired.');
    }
    if (!response.ok) {
      throw new Error('MCP request failed (' + response.status + ').');
    }
    const rpc = await response.json();
    if (rpc && rpc.error) {
      const message = typeof rpc.error.message === 'string' && rpc.error.message.trim()
        ? rpc.error.message.trim()
        : 'MCP error.';
      throw new Error(message);
    }
    const text = rpc?.result?.content?.[0]?.text;
    if (typeof text !== 'string') {
      throw new Error('Invalid MCP response.');
    }
    return text;
  }

  function formatDurationMs(ms) {
    const value = Number(ms);
    if (!Number.isFinite(value) || value < 0) return 'n/a';
    if (value < 1000) return Math.round(value) + 'ms';
    return (value / 1000).toFixed(value >= 10000 ? 0 : 1) + 's';
  }

  function getSemanticReindexArgs() {
    const defaultLimit = Number(viewerSettings?.semantic_reindex_limit ?? 500);
    const defaultWait = viewerSettings?.semantic_reindex_wait_for_index !== false;
    const defaultTimeout = Number(viewerSettings?.semantic_reindex_wait_timeout_seconds ?? 180);
    const limitInput = document.getElementById('settings-semantic-limit');
    const waitInput = document.getElementById('settings-semantic-wait');
    const timeoutInput = document.getElementById('settings-semantic-timeout');

    const rawLimit = Number(limitInput?.value);
    const rawTimeout = Number(timeoutInput?.value);
    const limit = Math.min(
      Math.max(
        Number.isFinite(rawLimit) && rawLimit > 0 ? Math.round(rawLimit) : Math.round(defaultLimit),
        1
      ),
      2000
    );
    const waitTimeoutSeconds = Math.min(
      Math.max(
        Number.isFinite(rawTimeout) && rawTimeout > 0 ? Math.round(rawTimeout) : Math.round(defaultTimeout),
        1
      ),
      900
    );
    const waitForIndex = waitInput ? waitInput.checked : defaultWait;

    return {
      limit,
      wait_for_index: waitForIndex,
      wait_timeout_seconds: waitTimeoutSeconds,
    };
  }

  function renderSemanticReindexStatus() {
    const lineEl = document.getElementById('semantic-status-line');
    const metaEl = document.getElementById('semantic-status-meta');
    const buttonEl = document.getElementById('semantic-reindex-btn');
    if (!lineEl || !metaEl || !buttonEl) return;
    buttonEl.disabled = semanticReindexRunning;
    buttonEl.textContent = semanticReindexRunning ? 'RUNNING SEMANTIC REINDEX...' : 'RUN SEMANTIC REINDEX';
    metaEl.innerHTML = '';

    const addPill = (text, cls = '') => {
      const pill = document.createElement('span');
      pill.className = 'semantic-status-pill' + (cls ? (' ' + cls) : '');
      pill.textContent = text;
      metaEl.appendChild(pill);
    };

    if (semanticReindexRunning) {
      lineEl.className = 'semantic-status-line';
      lineEl.textContent = 'Semantic reindex is running. Waiting for MCP response...';
      addPill('RUNNING', 'running');
      return;
    }

    if (semanticReindexLastError) {
      lineEl.className = 'semantic-status-line error';
      lineEl.textContent = 'Last run failed: ' + semanticReindexLastError;
      addPill('FAILED');
      return;
    }

    if (!semanticReindexLastResult || typeof semanticReindexLastResult !== 'object') {
      lineEl.className = 'semantic-status-line dim';
      lineEl.textContent = 'No semantic reindex run in this session.';
      return;
    }

    const result = semanticReindexLastResult;
    const processed = Number.isFinite(Number(result.processed)) ? Number(result.processed) : 0;
    const upserted = Number.isFinite(Number(result.upserted)) ? Number(result.upserted) : 0;
    const deleted = Number.isFinite(Number(result.deleted)) ? Number(result.deleted) : 0;
    const indexReady = result.index_ready;
    const waitElapsedMs = Number(result.wait_elapsed_ms);
    const waitForIndex = result.wait_for_index === true;

    lineEl.className = 'semantic-status-line';
    if (waitForIndex) {
      const readyText = indexReady === true ? 'ready' : (indexReady === false ? 'not ready' : 'pending');
      lineEl.textContent = 'Last run processed ' + processed + ' memories. Index status: ' + readyText + '.';
    } else {
      lineEl.textContent = 'Last run processed ' + processed + ' memories without readiness wait.';
    }

    addPill('UPSERTED ' + upserted);
    addPill('DELETED ' + deleted);
    if (waitForIndex) addPill('WAIT ' + formatDurationMs(waitElapsedMs));
    if (indexReady === true) addPill('INDEX READY', 'ready');
    if (indexReady === false) addPill('INDEX NOT READY', 'not-ready');
  }

  async function runSemanticReindex(source = 'settings') {
    if (!ensureAppReady('Semantic reindex')) return null;
    if (semanticReindexRunning) {
      showToast('Semantic reindex already running.', 'info');
      return null;
    }
    semanticReindexRunning = true;
    semanticReindexLastError = '';
    renderSemanticReindexStatus();

    const args = getSemanticReindexArgs();

    showToast(
      'Semantic reindex started (limit ' + args.limit + ', wait ' + (args.wait_for_index ? 'on' : 'off') + ').',
      'info'
    );
    try {
      const text = await callMcpTool('memory_reindex', args, 'viewer-semantic-reindex');
      const parsed = JSON.parse(text);
      if (!parsed || typeof parsed !== 'object') {
        throw new Error('Unexpected reindex response.');
      }
      semanticReindexLastResult = parsed;
      semanticReindexLastError = '';
      renderSemanticReindexStatus();

      const indexReady = parsed.index_ready;
      if (indexReady === true) {
        showToast('Semantic reindex completed and index is ready.', 'success', true);
      } else if (indexReady === false) {
        showToast('Reindex completed but index is not fully ready yet.', 'info', true);
      } else {
        showToast('Reindex completed.', 'success', true);
      }
      if (source === 'settings') {
        loadMemories(true);
      }
      return parsed;
    } catch (err) {
      semanticReindexLastResult = null;
      semanticReindexLastError = err instanceof Error && err.message ? err.message : 'Semantic reindex failed.';
      renderSemanticReindexStatus();
      showToast(semanticReindexLastError, 'error', true);
      return null;
    } finally {
      semanticReindexRunning = false;
      renderSemanticReindexStatus();
    }
  }

  function runSemanticReindexFromSettings() {
    return runSemanticReindex('settings');
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
      SESSION_MODE = 'legacy';
      enterApp();
      showToast('Legacy token accepted.', 'success');
    } catch {
      setLoginError('⚠ NETWORK ERROR');
      showToast('Network error while validating token.', 'error');
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
        credentials: 'same-origin',
        body: JSON.stringify(payload),
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) {
        setLoginError('⚠ ' + (data.error || 'AUTH FAILED'));
        return;
      }
      TOKEN = '';
      SESSION_MODE = 'user';
      enterApp();
      showToast(mode === 'signup' ? 'Account created and signed in.' : 'Signed in successfully.', 'success');
    } catch {
      setLoginError('⚠ NETWORK ERROR');
      showToast('Network error during authentication.', 'error');
    }
  }

  function doLogin() {
    return doTokenLogin();
  }

  async function doLogout(force = false) {
    if (!force && viewerSettings?.confirm_logout) {
      const ok = window.confirm('Lock and sign out of the current session?');
      if (!ok) return;
    }
    if (SESSION_MODE === 'user') {
      try {
        await tryRefreshSession();
        await fetch(BASE + '/auth/logout', {
          method: 'POST',
          credentials: 'same-origin',
        });
      } catch {}
    }
    if (pollIntervalId) {
      clearInterval(pollIntervalId);
      pollIntervalId = null;
    }
    if (clockIntervalId) {
      clearInterval(clockIntervalId);
      clockIntervalId = null;
    }
    TOKEN = '';
    SESSION_MODE = 'none';
    location.reload();
  }

  function updateTime() {
    const el = document.getElementById('hdr-time');
    if (el) {
      if (viewerSettings && viewerSettings.time_mode === 'local') {
        const local = new Date().toLocaleString();
        el.textContent = local + ' LOCAL';
      } else {
        el.textContent = new Date().toISOString().replace('T',' ').slice(0,19) + ' UTC';
      }
    }
  }

  function startClock() {
    if (clockIntervalId) clearInterval(clockIntervalId);
    updateTime();
    clockIntervalId = setInterval(updateTime, 1000);
  }

  function pulseStatPill(id, changed) {
    if (!changed) return;
    const el = document.getElementById(id);
    if (!el) return;
    el.classList.remove('pulse');
    void el.offsetWidth;
    el.classList.add('pulse');
  }

  async function loadMemories(silent = false) {
    const grid = document.getElementById('grid');
    const refreshBtn = document.querySelector('.refresh-btn');
    const scrollY = window.scrollY;
    if (!silent) {
      grid.innerHTML = '<div class="loading"><div class="loading-dot"></div><div class="loading-dot"></div><div class="loading-dot"></div></div>';
    }
    if (refreshBtn && !silent) refreshBtn.classList.add('syncing');
    const search = document.getElementById('search-input').value;
    let url = BASE + '/api/memories?limit=500';
    if (activeFilter) url += '&type=' + encodeURIComponent(activeFilter);
    if (search) url += '&search=' + encodeURIComponent(search);
    try {
      const r = await apiFetch(url);
      if (r.status === 401) { doLogout(true); return; }
      if (!r.ok) {
        if (!silent) {
          grid.innerHTML = '<div class="empty-state"><div class="empty-icon">⚠</div>ERROR LOADING MEMORIES</div>';
          showToast('Memory load failed (' + r.status + ').', 'error');
        }
        return;
      }
      const data = await r.json();
      allMemories = data.memories || [];
      updateStats(data.stats || [], allMemories);
      renderGrid(allMemories);
      if (silent) window.scrollTo(0, scrollY);
    } catch(e) {
      grid.innerHTML = '<div class="empty-state"><div class="empty-icon">⚠</div>CONNECTION ERROR</div>';
      showToast('Connection error while loading memories.', 'error');
    } finally {
      if (refreshBtn) refreshBtn.classList.remove('syncing');
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
    pulseStatPill('stat-all', lastStatsSnapshot.all !== null && total !== lastStatsSnapshot.all);
    pulseStatPill('stat-note', lastStatsSnapshot.note !== null && counts.note !== lastStatsSnapshot.note);
    pulseStatPill('stat-fact', lastStatsSnapshot.fact !== null && counts.fact !== lastStatsSnapshot.fact);
    pulseStatPill('stat-journal', lastStatsSnapshot.journal !== null && counts.journal !== lastStatsSnapshot.journal);
    lastStatsSnapshot = { all: total, note: counts.note, fact: counts.fact, journal: counts.journal };
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
      return \`<div class="card" data-type="\${m.type}" data-idx="\${i}" data-action="expand-card" data-card-index="\${i}" style="animation-delay:\${Math.min(i*0.04,0.4)}s">
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
        if (r.status === 401) { doLogout(true); return null; }
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

  function appIsVisible() {
    const app = document.getElementById('app');
    if (!app) return false;
    return window.getComputedStyle(app).display !== 'none';
  }

  function ensureAppReady(actionLabel = 'This action') {
    if (hasAuthenticatedSession() && appIsVisible()) return true;
    showToast(actionLabel + ' is available after sign in.', 'info');
    return false;
  }

  function getCommandPaletteActions() {
    return [
      {
        label: 'Refresh memories',
        detail: 'Reload data from API',
        run: () => {
          if (!ensureAppReady('Refresh')) return;
          loadMemories();
          showToast('Refreshing memories...', 'info');
        },
      },
      {
        label: 'Open graph view',
        detail: 'Explore memory network',
        run: async () => {
          if (!ensureAppReady('Graph view')) return;
          await showGraph();
          showToast('Graph view opened.', 'success');
        },
      },
      {
        label: 'Show all memories',
        detail: 'Clear type filter',
        run: () => {
          if (!ensureAppReady('Memory filter')) return;
          setFilter('');
          showToast('Showing all memory types.', 'info');
        },
      },
      {
        label: 'Focus search',
        detail: 'Jump to primary search',
        run: () => {
          if (!ensureAppReady('Search focus')) return;
          const input = document.getElementById('search-input');
          if (!input) return;
          input.focus();
          input.select();
          showToast('Search focused.', 'success');
        },
      },
      {
        label: 'Focus graph search',
        detail: 'Node and edge query',
        run: async () => {
          if (!ensureAppReady('Graph search')) return;
          if (!graphVisible) await showGraph();
          const input = document.getElementById('graph-search-input');
          if (!input) return;
          input.focus();
          input.select();
          showToast('Graph search focused.', 'success');
        },
      },
      {
        label: graphShowInferred ? 'Disable inferred edges' : 'Enable inferred edges',
        detail: graphShowInferred ? 'Currently ON' : 'Currently OFF',
        run: async () => {
          if (!ensureAppReady('Graph controls')) return;
          if (!graphVisible) await showGraph();
          toggleGraphInferred();
        },
      },
      {
        label: graphShowLabels ? 'Hide graph labels' : 'Show graph labels',
        detail: graphShowLabels ? 'Currently ON' : 'Currently OFF',
        run: async () => {
          if (!ensureAppReady('Graph controls')) return;
          if (!graphVisible) await showGraph();
          toggleGraphLabels();
        },
      },
      {
        label: graphPhysicsEnabled ? 'Pause graph physics' : 'Resume graph physics',
        detail: graphPhysicsEnabled ? 'Currently ON' : 'Currently OFF',
        run: async () => {
          if (!ensureAppReady('Graph controls')) return;
          if (!graphVisible) await showGraph();
          toggleGraphPhysics();
        },
      },
      {
        label: 'Open keyboard shortcuts',
        detail: 'Help overlay',
        run: () => toggleShortcutsOverlay(),
      },
      {
        label: 'Reindex semantic memory',
        detail: 'Limit ' + (viewerSettings?.semantic_reindex_limit ?? 500) +
          ' · wait ' + ((viewerSettings?.semantic_reindex_wait_for_index ?? true) ? 'on' : 'off'),
        run: async () => {
          if (!ensureAppReady('Semantic reindex')) return;
          await runSemanticReindex('command');
        },
      },
      {
        label: 'Open settings',
        detail: 'Viewer preferences',
        run: () => openSettingsOverlay(),
      },
      {
        label: 'Open changelog',
        detail: 'Recent release notes',
        run: () => {
          if (!ensureAppReady('Changelog')) return;
          openChangelogOverlay();
        },
      },
      {
        label: 'Lock session',
        detail: 'Sign out',
        run: () => {
          if (!ensureAppReady('Logout')) return;
          doLogout();
        },
      },
    ];
  }

  function updateCommandActiveSelection() {
    const list = document.getElementById('cmd-list');
    if (!list) return;
    list.querySelectorAll('.cmd-item').forEach((el, idx) => {
      el.classList.toggle('active', idx === commandActiveIndex);
    });
  }

  function renderCommandPalette() {
    const list = document.getElementById('cmd-list');
    if (!list) return;
    const query = commandQuery.trim().toLowerCase();
    const allActions = getCommandPaletteActions();
    commandVisibleActions = allActions.filter((action) => {
      if (!query) return true;
      return (action.label + ' ' + action.detail).toLowerCase().includes(query);
    });
    if (commandActiveIndex >= commandVisibleActions.length) {
      commandActiveIndex = Math.max(commandVisibleActions.length - 1, 0);
    }

    if (!commandVisibleActions.length) {
      list.innerHTML = '<div class="cmd-empty">No matching actions</div>';
      return;
    }

    list.innerHTML = commandVisibleActions.map((action, idx) =>
      '<button type="button" class="cmd-item ' + (idx === commandActiveIndex ? 'active' : '') + '" data-command-index="' + idx + '">' +
      '<span class="cmd-item-label">' + esc(action.label) + '</span>' +
      '<span class="cmd-item-detail">' + esc(action.detail) + '</span>' +
      '</button>'
    ).join('');

    list.querySelectorAll('.cmd-item').forEach((el) => {
      const index = Number(el.getAttribute('data-command-index') || '0');
      el.addEventListener('mouseenter', () => {
        commandActiveIndex = index;
        updateCommandActiveSelection();
      });
      el.addEventListener('click', () => runCommandAction(index));
    });
  }

  function onCommandFilter(value) {
    commandQuery = String(value || '');
    commandActiveIndex = 0;
    renderCommandPalette();
  }

  function moveCommandSelection(delta) {
    if (!commandVisibleActions.length) return;
    const next = commandActiveIndex + delta;
    if (next < 0) commandActiveIndex = commandVisibleActions.length - 1;
    else if (next >= commandVisibleActions.length) commandActiveIndex = 0;
    else commandActiveIndex = next;
    updateCommandActiveSelection();
  }

  function runCommandAction(index = commandActiveIndex) {
    const action = commandVisibleActions[index];
    if (!action) return;
    closeCommandPalette();
    Promise.resolve(action.run()).catch(() => showToast('Command failed.', 'error'));
  }

  function openCommandPalette() {
    const overlay = document.getElementById('cmd-overlay');
    const input = document.getElementById('cmd-input');
    if (!overlay || !input) return;
    commandPaletteOpen = true;
    commandQuery = '';
    commandActiveIndex = 0;
    input.value = '';
    overlay.classList.add('open');
    renderCommandPalette();
    setTimeout(() => input.focus(), 0);
  }

  function closeCommandPalette(event) {
    const overlay = document.getElementById('cmd-overlay');
    if (!overlay) return;
    if (event && event.target !== overlay) return;
    commandPaletteOpen = false;
    overlay.classList.remove('open');
  }

  function closeShortcutsOverlay(event) {
    const overlay = document.getElementById('shortcuts-overlay');
    if (!overlay) return;
    if (event && event.target !== overlay) return;
    overlay.classList.remove('open');
  }

  function toggleShortcutsOverlay() {
    const overlay = document.getElementById('shortcuts-overlay');
    if (!overlay) return;
    if (overlay.classList.contains('open')) overlay.classList.remove('open');
    else overlay.classList.add('open');
  }

  function fillSettingsForm() {
    if (!viewerSettings) return;
    const livePollEnabled = document.getElementById('settings-live-poll-enabled');
    const livePollInterval = document.getElementById('settings-live-poll-interval');
    const timeMode = document.getElementById('settings-time-mode');
    const defaultFilter = document.getElementById('settings-default-filter');
    const searchDebounce = document.getElementById('settings-search-debounce');
    const compactCards = document.getElementById('settings-compact-cards');
    const graphInferred = document.getElementById('settings-graph-inferred');
    const graphLabels = document.getElementById('settings-graph-labels');
    const graphPhysics = document.getElementById('settings-graph-physics');
    const graphFocus = document.getElementById('settings-graph-focus');
    const autoOpenGraph = document.getElementById('settings-auto-open-graph');
    const toastsEnabled = document.getElementById('settings-toasts-enabled');
    const toastDuration = document.getElementById('settings-toast-duration');
    const confirmLogout = document.getElementById('settings-confirm-logout');
    const showScanlines = document.getElementById('settings-show-scanlines');
    const reduceMotion = document.getElementById('settings-reduce-motion');
    const semanticWait = document.getElementById('settings-semantic-wait');
    const semanticTimeout = document.getElementById('settings-semantic-timeout');
    const semanticLimit = document.getElementById('settings-semantic-limit');
    if (livePollEnabled) livePollEnabled.checked = viewerSettings.live_poll_enabled;
    if (livePollInterval) livePollInterval.value = String(viewerSettings.live_poll_interval_sec);
    if (timeMode) timeMode.value = viewerSettings.time_mode;
    if (defaultFilter) defaultFilter.value = viewerSettings.default_memory_filter || '';
    if (searchDebounce) searchDebounce.value = String(viewerSettings.search_debounce_ms);
    if (compactCards) compactCards.checked = viewerSettings.compact_cards;
    if (graphInferred) graphInferred.checked = viewerSettings.graph_show_inferred;
    if (graphLabels) graphLabels.checked = viewerSettings.graph_show_labels;
    if (graphPhysics) graphPhysics.checked = viewerSettings.graph_physics_enabled;
    if (graphFocus) graphFocus.checked = viewerSettings.graph_focus_highlight;
    if (autoOpenGraph) autoOpenGraph.checked = viewerSettings.auto_open_graph;
    if (toastsEnabled) toastsEnabled.checked = viewerSettings.toasts_enabled;
    if (toastDuration) toastDuration.value = String(viewerSettings.toast_duration_ms);
    if (confirmLogout) confirmLogout.checked = viewerSettings.confirm_logout;
    if (showScanlines) showScanlines.checked = viewerSettings.show_scanlines;
    if (reduceMotion) reduceMotion.checked = viewerSettings.reduce_motion;
    if (semanticWait) semanticWait.checked = viewerSettings.semantic_reindex_wait_for_index;
    if (semanticTimeout) semanticTimeout.value = String(viewerSettings.semantic_reindex_wait_timeout_seconds);
    if (semanticLimit) semanticLimit.value = String(viewerSettings.semantic_reindex_limit);
    syncThemePicker();
    renderSemanticReindexStatus();
  }

  function readSettingsFromForm() {
    const raw = {
      theme: document.querySelector('.theme-swatch.active')?.dataset?.themeValue || viewerSettings?.theme || 'cyberpunk',
      live_poll_enabled: document.getElementById('settings-live-poll-enabled')?.checked !== false,
      live_poll_interval_sec: Number(document.getElementById('settings-live-poll-interval')?.value ?? 10),
      time_mode: document.getElementById('settings-time-mode')?.value === 'local' ? 'local' : 'utc',
      default_memory_filter: document.getElementById('settings-default-filter')?.value || '',
      search_debounce_ms: Number(document.getElementById('settings-search-debounce')?.value ?? 300),
      compact_cards: document.getElementById('settings-compact-cards')?.checked === true,
      graph_show_inferred: document.getElementById('settings-graph-inferred')?.checked !== false,
      graph_show_labels: document.getElementById('settings-graph-labels')?.checked !== false,
      graph_physics_enabled: document.getElementById('settings-graph-physics')?.checked !== false,
      graph_focus_highlight: document.getElementById('settings-graph-focus')?.checked !== false,
      auto_open_graph: document.getElementById('settings-auto-open-graph')?.checked === true,
      toasts_enabled: document.getElementById('settings-toasts-enabled')?.checked !== false,
      toast_duration_ms: Number(document.getElementById('settings-toast-duration')?.value ?? 2300),
      confirm_logout: document.getElementById('settings-confirm-logout')?.checked === true,
      show_scanlines: document.getElementById('settings-show-scanlines')?.checked !== false,
      reduce_motion: document.getElementById('settings-reduce-motion')?.checked === true,
      semantic_reindex_wait_for_index: document.getElementById('settings-semantic-wait')?.checked !== false,
      semantic_reindex_wait_timeout_seconds: Number(document.getElementById('settings-semantic-timeout')?.value ?? 180),
      semantic_reindex_limit: Number(document.getElementById('settings-semantic-limit')?.value ?? 500),
    };
    return normalizeViewerSettings(raw);
  }

  function closeSettingsOverlay(event) {
    const overlay = document.getElementById('settings-overlay');
    if (!overlay) return;
    if (event && event.target !== overlay) return;
    overlay.classList.remove('open');
  }

  function openSettingsOverlay() {
    const overlay = document.getElementById('settings-overlay');
    if (!overlay) return;
    fillSettingsForm();
    overlay.classList.add('open');
  }

  function closeChangelogOverlay(event) {
    const overlay = document.getElementById('changelog-overlay');
    if (!overlay) return;
    if (event && event.target !== overlay) return;
    overlay.classList.remove('open');
  }

  function formatChangelogDate(unixTs) {
    const ts = Number(unixTs);
    if (!Number.isFinite(ts) || ts <= 0) return 'Unknown date';
    return new Date(ts * 1000).toISOString().slice(0, 10);
  }

  function renderChangelogEntries(entries, latestVersion) {
    const list = document.getElementById('changelog-list');
    const subtitle = document.getElementById('changelog-subtitle');
    if (!list || !subtitle) return;
    const rows = Array.isArray(entries) ? entries : [];
    const latest = typeof latestVersion === 'string' && latestVersion.trim()
      ? latestVersion.trim()
      : VIEWER_SERVER_VERSION;
    subtitle.textContent = 'Latest version: v' + latest + ' - showing ' + rows.length + ' entries';
    if (!rows.length) {
      list.innerHTML = '<div class="setting-help">No changelog entries available.</div>';
      return;
    }

    list.innerHTML = rows.map((entry) => {
      const version = typeof entry.version === 'string' && entry.version.trim() ? entry.version.trim() : 'unknown';
      const summary = typeof entry.summary === 'string' ? entry.summary : '';
      const releaseDate = formatChangelogDate(entry.released_at);
      const changes = Array.isArray(entry.changes) ? entry.changes : [];
      const changesHtml = changes.slice(0, 16).map((change) => {
        const type = typeof change.type === 'string' && change.type.trim() ? change.type.trim() : 'changed';
        const target = typeof change.target === 'string' && change.target.trim() ? change.target.trim() : '';
        const name = typeof change.name === 'string' && change.name.trim() ? change.name.trim() : 'Untitled change';
        const description = typeof change.description === 'string' && change.description.trim() ? change.description.trim() : '';
        const prefix = target ? (target + ': ') : '';
        const detail = prefix + name + (description ? (' - ' + description) : '');
        return '<li class="changelog-change-row">' +
          '<span class="changelog-change-type">' + esc(type) + '</span>' +
          '<span class="changelog-change-text">' + esc(detail) + '</span>' +
        '</li>';
      }).join('');
      return '<article class="changelog-entry">' +
        '<div class="changelog-entry-head">' +
          '<span class="changelog-entry-version">v' + esc(version) + '</span>' +
          '<span class="changelog-entry-date">' + esc(releaseDate) + '</span>' +
        '</div>' +
        '<div class="changelog-entry-summary">' + esc(summary || 'No summary available.') + '</div>' +
        (changesHtml ? ('<ul class="changelog-change-list">' + changesHtml + '</ul>') : '') +
      '</article>';
    }).join('');
  }

  async function loadChangelogEntries() {
    const list = document.getElementById('changelog-list');
    const subtitle = document.getElementById('changelog-subtitle');
    if (!list || !subtitle) return;
    list.innerHTML = '<div class="setting-help">Loading changelog...</div>';
    subtitle.textContent = 'Fetching latest release notes...';
    try {
      const response = await apiFetch(BASE + '/mcp', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: 'viewer-changelog',
          method: 'tools/call',
          params: {
            name: 'tool_changelog',
            arguments: { limit: 12 },
          },
        }),
      });
      if (response.status === 401) {
        doLogout(true);
        return;
      }
      if (!response.ok) throw new Error('Failed to load changelog.');
      const rpc = await response.json();
      if (rpc && rpc.error) throw new Error(typeof rpc.error.message === 'string' ? rpc.error.message : 'Failed to load changelog.');
      const text = rpc?.result?.content?.[0]?.text;
      if (typeof text !== 'string' || !text.trim()) throw new Error('Invalid changelog response.');
      const parsed = JSON.parse(text);
      renderChangelogEntries(parsed?.entries, parsed?.latest_version);
    } catch (err) {
      const message = err instanceof Error && err.message ? err.message : 'Failed to load changelog.';
      subtitle.textContent = 'Unable to load release notes.';
      list.innerHTML = '<div class="setting-help" style="color:var(--red)">' + esc(message) + '</div>';
    }
  }

  async function openChangelogOverlay() {
    closeSettingsOverlay();
    const overlay = document.getElementById('changelog-overlay');
    if (!overlay) return;
    overlay.classList.add('open');
    await loadChangelogEntries();
  }

  function applySettingsFromForm() {
    viewerSettings = readSettingsFromForm();
    persistViewerSettings();
    applyViewerSettingsToRuntime({ restartPolling: true, rerenderGraph: true, rerenderGrid: true });
    updateTime();
    closeSettingsOverlay();
    showToast('Settings saved.', 'success', true);
  }

  function resetViewerSettings() {
    viewerSettings = buildDefaultViewerSettings();
    persistViewerSettings();
    fillSettingsForm();
    applyViewerSettingsToRuntime({ restartPolling: true, rerenderGraph: true, rerenderGrid: true });
    updateTime();
    showToast('Settings reset to defaults.', 'info', true);
  }

  function syncThemePicker() {
    const current = viewerSettings?.theme || 'cyberpunk';
    document.querySelectorAll('.theme-swatch').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.themeValue === current);
    });
  }

  function syncFilterPills(type) {
    ['all','note','fact','journal','graph'].forEach(t => {
      document.getElementById('stat-' + t).classList.toggle('active', (type === '' ? 'all' : type) === t);
    });
  }

  function setFilter(type) {
    graphVisible = false;
    const graphView = document.getElementById('graph-view');
    graphView.classList.remove('visible');
    graphView.style.display = 'none';
    document.querySelector('.grid-wrap').style.display = 'grid';
    activeFilter = type;
    syncFilterPills(type);
    loadMemories();
  }

  function onSearch(val) {
    clearTimeout(searchTimeout);
    const debounceMs = Math.min(Math.max(Number(viewerSettings?.search_debounce_ms ?? 300), 120), 1500);
    searchTimeout = setTimeout(loadMemories, debounceMs);
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

  function startLivePolling(forceRestart = false) {
    if (forceRestart && pollIntervalId) {
      clearInterval(pollIntervalId);
      pollIntervalId = null;
    }
    const liveEl = document.getElementById('live-indicator');
    const pollingEnabled = !viewerSettings || viewerSettings.live_poll_enabled;
    if (!pollingEnabled) {
      if (liveEl) liveEl.style.display = 'none';
      return;
    }
    if (pollIntervalId) return;
    if (liveEl) liveEl.style.display = 'flex';
    const intervalMs = Math.min(Math.max((viewerSettings?.live_poll_interval_sec ?? 10) * 1000, 5000), 120000);
    pollIntervalId = setInterval(async () => {
      if (!hasAuthenticatedSession()) return;
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
    }, intervalMs);
  }

  function syncGraphToolbarState() {
    const inferredBtn = document.getElementById('graph-toggle-inferred');
    const labelsBtn = document.getElementById('graph-toggle-labels');
    const physicsBtn = document.getElementById('graph-toggle-physics');
    if (inferredBtn) {
      inferredBtn.classList.toggle('active', graphShowInferred);
      inferredBtn.classList.toggle('off', !graphShowInferred);
      inferredBtn.textContent = graphShowInferred ? 'INFERRED ON' : 'INFERRED OFF';
    }
    if (labelsBtn) {
      labelsBtn.classList.toggle('active', graphShowLabels);
      labelsBtn.classList.toggle('off', !graphShowLabels);
      labelsBtn.textContent = graphShowLabels ? 'LABELS ON' : 'LABELS OFF';
    }
    if (physicsBtn) {
      physicsBtn.classList.toggle('active', graphPhysicsEnabled);
      physicsBtn.classList.toggle('off', !graphPhysicsEnabled);
      physicsBtn.textContent = graphPhysicsEnabled ? 'PHYSICS ON' : 'PHYSICS OFF';
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
    showToast(graphShowInferred ? 'Inferred edges enabled.' : 'Inferred edges disabled.', 'info');
  }

  function toggleGraphLabels() {
    graphShowLabels = !graphShowLabels;
    syncGraphToolbarState();
    if (graphVisible) rerenderGraphFromCache();
    showToast(graphShowLabels ? 'Graph labels enabled.' : 'Graph labels hidden.', 'info');
  }

  function toggleGraphPhysics() {
    graphPhysicsEnabled = !graphPhysicsEnabled;
    syncGraphToolbarState();
    if (!graphSimulation) return;
    if (graphPhysicsEnabled) {
      graphSimulation.alpha(0.55).restart();
    } else {
      graphSimulation.stop();
    }
    showToast(graphPhysicsEnabled ? 'Graph physics resumed.' : 'Graph physics paused.', 'info');
  }

  function resetGraphView() {
    if (!graphSvgSelection || !graphZoomBehavior) return;
    graphSvgSelection.transition().duration(220).call(graphZoomBehavior.transform, d3.zoomIdentity);
    graphRelationFilter = new Set(GRAPH_RELATION_TYPES);
    graphSearchQuery = '';
    const searchInput = document.getElementById('graph-search-input');
    if (searchInput) searchInput.value = '';
    if (graphPhysicsEnabled && graphSimulation) graphSimulation.alpha(0.45).restart();
    syncGraphToolbarState();
    rerenderGraphFromCache();
    showToast('Graph view reset.', 'success');
  }

  async function showGraph() {
    graphVisible = true;
    syncGraphToolbarState();
    ['all','note','fact','journal'].forEach(t => {
      document.getElementById('stat-' + t).classList.remove('active');
    });
    document.getElementById('stat-graph').classList.add('active');
    document.querySelector('.grid-wrap').style.display = 'none';
    const graphView = document.getElementById('graph-view');
    graphView.classList.remove('visible');
    graphView.style.display = 'block';
    requestAnimationFrame(() => graphView.classList.add('visible'));
    const emptyEl = document.getElementById('graph-empty');
    if (emptyEl) emptyEl.style.display = 'none';
    const legendEl = document.getElementById('graph-legend');
    if (legendEl) legendEl.innerHTML = '';

    const svg = document.getElementById('graph-svg');
    svg.innerHTML = '<text x="50%" y="50%" text-anchor="middle" style="fill:var(--amber);font-family:var(--mono);font-size:0.7rem;letter-spacing:0.15em">LOADING GRAPH...</text>';

    try {
      const r = await apiFetch(BASE + '/api/graph');
      if (r.status === 401) { doLogout(true); return; }
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
      showToast('Graph loaded: ' + lastGraphData.nodes.length + ' nodes.', 'success');
    } catch(e) {
      document.getElementById('graph-svg').innerHTML = '<text x="50%" y="50%" text-anchor="middle" style="fill:var(--red);font-family:var(--mono);font-size:0.7rem;letter-spacing:0.15em">ERROR LOADING GRAPH</text>';
      showToast('Graph load failed.', 'error');
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
    const neighborhoodByNode = new Map();
    links.forEach((l) => {
      const fromId = String(l.source);
      const toId = String(l.target);
      const fromSet = neighborhoodByNode.get(fromId) || new Set();
      fromSet.add(toId);
      neighborhoodByNode.set(fromId, fromSet);
      const toSet = neighborhoodByNode.get(toId) || new Set();
      toSet.add(fromId);
      neighborhoodByNode.set(toId, toSet);
    });
    const baseNodeOpacity = (d) => {
      const confidence = Math.min(Math.max(Number.isFinite(Number(d.dynamic_confidence ?? d.confidence)) ? Number(d.dynamic_confidence ?? d.confidence) : 0.7, 0), 1);
      const visible = isNodeVisible(d.id);
      const baseOpacity = 0.42 + confidence * 0.5;
      return visible ? baseOpacity : Math.max(0.08, baseOpacity * 0.25);
    };
    const baseNodeStrokeOpacity = (d) => isNodeVisible(d.id) ? 1 : 0.2;
    const baseNodeTextOpacity = (d) => isNodeVisible(d.id) ? 1 : 0.2;

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
    graphSimulation = simulation;
    if (!graphPhysicsEnabled) simulation.stop();

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
      .attr('fill-opacity', baseNodeOpacity)
      .attr('stroke', d => typeColor[d.type] || '#888')
      .attr('stroke-opacity', baseNodeStrokeOpacity)
      .attr('stroke-width', (d) => {
        const importance = Math.min(Math.max(Number.isFinite(Number(d.dynamic_importance ?? d.importance)) ? Number(d.dynamic_importance ?? d.importance) : 0.5, 0), 1);
        return 1.4 + importance * 1.6;
      });

    node.append('text')
      .attr('dx', 12).attr('dy', 4)
      .style('opacity', baseNodeTextOpacity)
      .text(d => (d.title || d.key || d.content || '').slice(0, isMobile ? 18 : 24));

    const applyGraphFocus = (focusId) => {
      if (viewerSettings && viewerSettings.graph_focus_highlight === false) {
        focusId = '';
      }
      if (!focusId) {
        link.attr('stroke-opacity', linkOpacity);
        linkLabel.style('opacity', (d) => linkOpacity(d) >= 0.5 ? 1 : 0);
        node.select('circle')
          .attr('fill-opacity', (d) => baseNodeOpacity(d))
          .attr('stroke-opacity', (d) => baseNodeStrokeOpacity(d));
        node.select('text').style('opacity', (d) => baseNodeTextOpacity(d));
        return;
      }

      const neighborSet = neighborhoodByNode.get(focusId) ?? new Set();
      const focusSet = new Set([focusId]);
      neighborSet.forEach((neighborId) => focusSet.add(neighborId));
      const isFocusedNode = (id) => focusSet.has(String(id));
      const isFocusedEdge = (d) => {
        const sId = getEndpointId(d.source);
        const tId = getEndpointId(d.target);
        return isFocusedNode(sId) && isFocusedNode(tId);
      };

      link.attr('stroke-opacity', (d) => {
        if (!isFocusedEdge(d)) return 0.04;
        const base = linkOpacity(d);
        if (d.kind === 'inferred') return Math.max(base, 0.58);
        return Math.max(base, 1);
      });

      linkLabel.style('opacity', (d) => {
        if (!graphShowLabels) return 0;
        return isFocusedEdge(d) ? 1 : 0;
      });

      node.select('circle')
        .attr('fill-opacity', (d) => {
          const id = String(d.id);
          if (id === focusId) return 1;
          if (focusSet.has(id)) return Math.max(baseNodeOpacity(d), 0.78);
          return Math.min(baseNodeOpacity(d), 0.1);
        })
        .attr('stroke-opacity', (d) => {
          const id = String(d.id);
          if (id === focusId) return 1;
          if (focusSet.has(id)) return 0.95;
          return 0.12;
        });

      node.select('text').style('opacity', (d) => {
        const id = String(d.id);
        if (id === focusId) return 1;
        if (focusSet.has(id)) return 0.95;
        return 0.1;
      });
    };

    node
      .on('mouseenter', (event, d) => { applyGraphFocus(String(d.id)); })
      .on('mouseleave', () => { applyGraphFocus(''); });

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

  function bindViewerEventHandlers() {
    const bindInput = (id, handler) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.addEventListener('input', (event) => {
        const target = event.target;
        handler(target && typeof target.value === 'string' ? target.value : '');
      });
    };

    document.addEventListener('click', (event) => {
      const target = event.target instanceof Element ? event.target.closest('[data-action]') : null;
      if (!target) return;
      const action = target.getAttribute('data-action') || '';

      switch (action) {
        case 'login':
          doCredentialAuth('login');
          break;
        case 'signup':
          doCredentialAuth('signup');
          break;
        case 'token-login':
          doTokenLogin();
          break;
        case 'logout':
          doLogout();
          break;
        case 'set-filter':
          setFilter(target.getAttribute('data-filter') || '');
          break;
        case 'show-graph':
          showGraph();
          break;
        case 'refresh-memories':
          loadMemories();
          break;
        case 'open-command-palette':
          openCommandPalette();
          break;
        case 'toggle-shortcuts-overlay':
          toggleShortcutsOverlay();
          break;
        case 'open-settings-overlay':
          openSettingsOverlay();
          break;
        case 'toggle-graph-inferred':
          toggleGraphInferred();
          break;
        case 'toggle-graph-labels':
          toggleGraphLabels();
          break;
        case 'toggle-graph-physics':
          toggleGraphPhysics();
          break;
        case 'reset-graph-view':
          resetGraphView();
          break;
        case 'toggle-graph-relation':
          toggleGraphRelation(target.getAttribute('data-relation') || '');
          break;
        case 'close-expand-overlay':
          closeExpand(event);
          break;
        case 'close-expand':
          closeExpandBtn();
          break;
        case 'close-command-palette-overlay':
          closeCommandPalette(event);
          break;
        case 'close-shortcuts-overlay':
          closeShortcutsOverlay(event);
          break;
        case 'close-shortcuts':
          closeShortcutsOverlay();
          break;
        case 'close-settings-overlay':
          closeSettingsOverlay(event);
          break;
        case 'close-settings':
          closeSettingsOverlay();
          break;
        case 'run-semantic-reindex':
          runSemanticReindexFromSettings();
          break;
        case 'open-changelog-overlay':
          openChangelogOverlay();
          break;
        case 'reset-viewer-settings':
          resetViewerSettings();
          break;
        case 'apply-settings':
          applySettingsFromForm();
          break;
        case 'close-changelog-overlay':
          closeChangelogOverlay(event);
          break;
        case 'close-changelog':
          closeChangelogOverlay();
          break;
        case 'open-full-changelog':
          window.open('https://github.com/guirguispierre/ai-memory-mcp/blob/master/CHANGELOG.md', '_blank', 'noopener');
          break;
        case 'expand-card':
          expandCard(Number(target.getAttribute('data-card-index') || target.getAttribute('data-idx') || '-1'));
          break;
        default:
          break;
      }
    });

    document.addEventListener('click', (event) => {
      const target = event.target instanceof Element ? event.target.closest('.theme-swatch') : null;
      if (!target) return;
      const themeValue = target.getAttribute('data-theme-value');
      if (!themeValue) return;
      viewerSettings = readSettingsFromForm();
      viewerSettings.theme = themeValue;
      persistViewerSettings();
      applyViewerSettingsToRuntime({ restartPolling: false, rerenderGraph: false, rerenderGrid: false });
    });

    bindInput('search-input', onSearch);
    bindInput('graph-search-input', onGraphSearch);
    bindInput('cmd-input', onCommandFilter);
  }

  syncGraphToolbarState();
  bindViewerEventHandlers();

  // Enter key on login
  document.getElementById('token-input').addEventListener('keydown', e => { if (e.key === 'Enter') doTokenLogin(); });
  document.getElementById('email-input').addEventListener('keydown', e => { if (e.key === 'Enter') doCredentialAuth('login'); });
  document.getElementById('password-input').addEventListener('keydown', e => { if (e.key === 'Enter') doCredentialAuth('login'); });
  document.getElementById('brain-name-input').addEventListener('keydown', e => { if (e.key === 'Enter') doCredentialAuth('signup'); });
  document.getElementById('cmd-input').addEventListener('keydown', e => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      moveCommandSelection(1);
      return;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      moveCommandSelection(-1);
      return;
    }
    if (e.key === 'Enter') {
      e.preventDefault();
      runCommandAction();
      return;
    }
    if (e.key === 'Escape') {
      e.preventDefault();
      closeCommandPalette();
    }
  });
  document.addEventListener('keydown', e => {
    const key = String(e.key || '').toLowerCase();
    const shortcutsOpen = document.getElementById('shortcuts-overlay').classList.contains('open');
    const settingsOpen = document.getElementById('settings-overlay').classList.contains('open');
    const changelogOpen = document.getElementById('changelog-overlay').classList.contains('open');
    const expandOpen = document.getElementById('expand-overlay').classList.contains('open');
    const typing = isTypingTarget(e.target);

    if ((e.ctrlKey || e.metaKey) && key === 'k') {
      e.preventDefault();
      if (commandPaletteOpen) closeCommandPalette();
      else openCommandPalette();
      return;
    }

    if (commandPaletteOpen) {
      if (key === 'escape') {
        e.preventDefault();
        closeCommandPalette();
      }
      return;
    }

    if (shortcutsOpen) {
      if (key === 'escape') {
        e.preventDefault();
        closeShortcutsOverlay();
      }
      return;
    }

    if (changelogOpen) {
      if (key === 'escape') {
        e.preventDefault();
        closeChangelogOverlay();
      }
      return;
    }

    if (settingsOpen) {
      if (key === 'escape') {
        e.preventDefault();
        closeSettingsOverlay();
      }
      return;
    }

    if (e.key === '?' && !typing) {
      e.preventDefault();
      toggleShortcutsOverlay();
      return;
    }

    if (key === 'escape' && expandOpen) {
      e.preventDefault();
      closeExpandBtn();
      return;
    }

    if (typing) return;
    if (!hasAuthenticatedSession() || !appIsVisible()) return;

    if (key === '/') {
      e.preventDefault();
      const input = document.getElementById('search-input');
      if (!input) return;
      input.focus();
      input.select();
      return;
    }
    if (key === 'g') {
      e.preventDefault();
      showGraph();
      return;
    }
    if (key === 's') {
      e.preventDefault();
      openSettingsOverlay();
      return;
    }
    if (key === 'r') {
      e.preventDefault();
      loadMemories();
    }
  });
`;
}

function rootLandingHtml(url: URL): string {
  const origin = url.origin;
  const mcpEndpoint = `${origin}/mcp`;
  const viewerEndpoint = `${origin}/view`;
  const authzMetadata = `${origin}/.well-known/oauth-authorization-server`;
  const resourceMetadata = `${origin}/.well-known/oauth-protected-resource`;
  const envLabel = url.hostname.includes('-dev') ? 'Development Environment' : 'Production Environment';
  const devEntries: Array<{ path: string; label: string }> = [
    { path: '/mcp', label: '/mcp' },
    { path: '/view', label: '/view' },
    { path: '/register', label: '/register' },
    { path: '/authorize', label: '/authorize' },
    { path: '/token', label: '/token' },
    { path: '/.well-known/oauth-authorization-server', label: '/.well-known/oauth-authorization-server' },
    { path: '/.well-known/oauth-protected-resource', label: '/.well-known/oauth-protected-resource' },
    { path: '/auth/signup', label: '/auth/signup' },
    { path: '/auth/login', label: '/auth/login' },
    { path: '/auth/refresh', label: '/auth/refresh' },
    { path: '/auth/logout', label: '/auth/logout' },
    { path: '/auth/me', label: '/auth/me' },
    { path: '/auth/sessions', label: '/auth/sessions' },
    { path: '/auth/sessions/revoke', label: '/auth/sessions/revoke' },
    { path: '/api/memories', label: '/api/memories' },
    { path: '/api/tools', label: '/api/tools' },
    { path: '/api/graph', label: '/api/graph' },
    { path: '/api/links/sample-memory-id', label: '/api/links/:memoryId' },
  ];
  const devRows = devEntries.map((entry) => {
    const guide = endpointGuideForPath(entry.path);
    const title = guide?.title
      ?? (entry.path === '/mcp' ? 'MCP Endpoint' : (entry.path === '/view' ? 'Web Viewer' : 'Endpoint'));
    const subtitle = guide?.subtitle
      ?? (entry.path === '/mcp'
        ? 'MCP JSON-RPC and SSE transport'
        : (entry.path === '/view' ? 'Human memory dashboard + graph explorer' : 'Endpoint surface'));
    const methods = guide?.methods ?? 'GET';
    const auth = guide?.auth
      ?? (entry.path === '/view'
        ? 'Browser login available in-page.'
        : (entry.path === '/mcp' ? 'Requires Bearer token/OAuth for tool calls.' : 'See endpoint guide.'));
    const endpointUrl = `${origin}${entry.path}`;
    return `<tr>
      <td><a class="endpoint" href="${endpointUrl}">${escapeHtml(entry.label)}</a></td>
      <td>${escapeHtml(title)}</td>
      <td><code>${escapeHtml(methods)}</code></td>
      <td>${escapeHtml(auth)}</td>
      <td>${escapeHtml(subtitle)}</td>
    </tr>`;
  }).join('');

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MemoryVault Dev Portal</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #060b12;
    --bg2: #0f1927;
    --line: #27466c;
    --line-soft: #1c334c;
    --text: #d6e5f4;
    --dim: #7390aa;
    --amber: #f0a500;
    --teal: #00c8b4;
    --mono: 'Share Tech Mono', monospace;
    --sans: 'Syne', sans-serif;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: var(--mono);
    color: var(--text);
    background:
      radial-gradient(78% 55% at 10% 0%, rgba(0, 200, 180, 0.14), transparent 70%),
      radial-gradient(70% 58% at 100% 100%, rgba(240, 165, 0, 0.1), transparent 72%),
      var(--bg);
    min-height: 100vh;
  }
  .wrap {
    max-width: 1180px;
    margin: 0 auto;
    padding: 2rem 1.1rem 2.6rem;
  }
  .title {
    margin: 0;
    font-family: var(--sans);
    font-size: clamp(1.65rem, 3vw, 2.75rem);
    letter-spacing: -0.02em;
    font-weight: 800;
    line-height: 1.05;
  }
  .title span { color: var(--amber); }
  .sub {
    margin: 0.5rem 0 1.2rem;
    color: var(--dim);
    letter-spacing: 0.11em;
    text-transform: uppercase;
    font-size: 0.72rem;
  }
  .pill {
    display: inline-flex;
    align-items: center;
    border: 1px solid var(--line);
    background: rgba(15, 25, 39, 0.78);
    color: var(--teal);
    font-size: 0.68rem;
    letter-spacing: 0.13em;
    text-transform: uppercase;
    padding: 0.3rem 0.55rem;
    margin-bottom: 1rem;
  }
  .grid {
    display: grid;
    grid-template-columns: 1.05fr 1fr;
    gap: 1rem;
  }
  .card {
    border: 1px solid var(--line);
    background: rgba(15, 25, 39, 0.84);
    padding: 1rem 1rem 0.95rem;
  }
  .card h2 {
    margin: 0 0 0.65rem;
    color: var(--amber);
    font-size: 0.79rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
  }
  p, li {
    margin: 0;
    line-height: 1.58;
    font-size: 0.84rem;
  }
  ul {
    margin: 0;
    padding-left: 1.1rem;
    display: grid;
    gap: 0.45rem;
  }
  .metrics {
    margin-top: 0.8rem;
    display: flex;
    gap: 0.55rem;
    flex-wrap: wrap;
  }
  .metric {
    border: 1px solid var(--line-soft);
    padding: 0.45rem 0.55rem;
    min-width: 150px;
    background: rgba(6, 11, 18, 0.68);
  }
  .metric .k {
    color: var(--dim);
    display: block;
    font-size: 0.66rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
  }
  .metric .v {
    color: var(--teal);
    display: block;
    margin-top: 0.3rem;
    font-size: 0.84rem;
  }
  .actions {
    margin-top: 0.85rem;
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
  }
  .btn {
    border: 1px solid var(--line);
    color: var(--text);
    text-decoration: none;
    font-size: 0.7rem;
    letter-spacing: 0.11em;
    text-transform: uppercase;
    padding: 0.46rem 0.62rem;
    display: inline-block;
  }
  .btn.primary {
    border-color: var(--amber);
    color: var(--amber);
  }
  .dev {
    margin-top: 1rem;
    border: 1px solid var(--line);
    background: rgba(15, 25, 39, 0.84);
    overflow: hidden;
  }
  .dev-head {
    padding: 0.75rem 0.9rem;
    border-bottom: 1px solid var(--line-soft);
    display: flex;
    gap: 0.5rem;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
  }
  .dev-head h2 {
    margin: 0;
    color: var(--amber);
    font-size: 0.8rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
  }
  .dev-head p {
    color: var(--dim);
    font-size: 0.72rem;
  }
  .table-wrap { overflow-x: auto; }
  table {
    width: 100%;
    border-collapse: collapse;
    min-width: 920px;
  }
  th, td {
    text-align: left;
    vertical-align: top;
    border-bottom: 1px solid var(--line-soft);
    padding: 0.62rem 0.72rem;
    font-size: 0.77rem;
    line-height: 1.45;
  }
  th {
    color: var(--dim);
    text-transform: uppercase;
    letter-spacing: 0.12em;
    font-size: 0.66rem;
    position: sticky;
    top: 0;
    background: #0f1927;
    z-index: 2;
  }
  td code {
    color: var(--teal);
    font-family: var(--mono);
    font-size: 0.74rem;
  }
  .endpoint {
    color: var(--teal);
    text-decoration: none;
    display: inline-block;
    max-width: 320px;
    overflow-wrap: anywhere;
  }
  @media (max-width: 930px) {
    .grid { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
  <main class="wrap">
    <div class="pill">${escapeHtml(envLabel)}</div>
    <h1 class="title">MEMORY<span>VAULT</span> Dev Portal</h1>
    <p class="sub">Human-Friendly Landing Page For This MCP Host</p>

    <div class="grid">
      <section class="card">
        <h2>Overview</h2>
        <p>This host serves the MemoryVault MCP, OAuth flow, web viewer, and diagnostic APIs. Use this page as the top-level map for all sub-sites and machine endpoints.</p>
        <div class="metrics">
          <div class="metric"><span class="k">Server</span><span class="v">${escapeHtml(SERVER_NAME)}</span></div>
          <div class="metric"><span class="k">Version</span><span class="v">${escapeHtml(SERVER_VERSION)}</span></div>
          <div class="metric"><span class="k">MCP Tools</span><span class="v">${TOOLS.length}</span></div>
        </div>
        <div class="actions">
          <a class="btn primary" href="${mcpEndpoint}">MCP Guide</a>
          <a class="btn" href="${viewerEndpoint}">Open Viewer</a>
          <a class="btn" href="${authzMetadata}">OAuth Metadata</a>
          <a class="btn" href="${resourceMetadata}">Resource Metadata</a>
        </div>
      </section>
      <section class="card">
        <h2>Quick Dev Notes</h2>
        <ul>
          <li>Browser navigation shows human-readable guides for MCP and API routes.</li>
          <li>Programmatic requests still receive OAuth challenge and normal JSON API behavior.</li>
          <li><code>/mcp</code> is the MCP endpoint for AI clients (JSON-RPC + SSE).</li>
          <li><code>/view</code> is the web UI for login, memory browsing, and graph exploration.</li>
        </ul>
      </section>
    </div>

    <section class="dev">
      <div class="dev-head">
        <h2>Dev Section: All Endpoints</h2>
        <p>Open any path for a friendly guide page or direct endpoint behavior.</p>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Path</th>
              <th>Surface</th>
              <th>Methods</th>
              <th>Auth</th>
              <th>Purpose</th>
            </tr>
          </thead>
          <tbody>${devRows}</tbody>
        </table>
      </div>
    </section>
  </main>
  <script src="https://mcp.figma.com/mcp/html-to-design/capture.js" async></script>
</body>
</html>`;
}

function mcpLandingHtml(url: URL): string {
  const origin = url.origin;
  const mcpEndpoint = `${origin}/mcp`;
  const viewerEndpoint = `${origin}/view`;
  const authzMetadata = `${origin}/.well-known/oauth-authorization-server`;
  const resourceMetadata = `${origin}/.well-known/oauth-protected-resource`;
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MemoryVault MCP</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #070b10;
    --bg2: #101824;
    --line: #234061;
    --text: #d8e8f8;
    --dim: #6f8ea9;
    --amber: #f0a500;
    --teal: #00c8b4;
    --mono: 'Share Tech Mono', monospace;
    --sans: 'Syne', sans-serif;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: var(--mono);
    color: var(--text);
    background:
      radial-gradient(70% 50% at 12% 0%, rgba(0, 200, 180, 0.14), transparent 70%),
      radial-gradient(60% 60% at 100% 100%, rgba(240, 165, 0, 0.12), transparent 70%),
      var(--bg);
    min-height: 100vh;
  }
  .wrap {
    max-width: 980px;
    margin: 0 auto;
    padding: 2rem 1.2rem 2.6rem;
  }
  .title {
    font-family: var(--sans);
    font-size: clamp(1.55rem, 3vw, 2.5rem);
    font-weight: 800;
    letter-spacing: -0.02em;
    margin: 0 0 0.35rem;
  }
  .title span { color: var(--amber); }
  .sub {
    margin: 0 0 1.4rem;
    color: var(--dim);
    letter-spacing: 0.08em;
    font-size: 0.72rem;
    text-transform: uppercase;
  }
  .grid {
    display: grid;
    grid-template-columns: 1.2fr 1fr;
    gap: 1rem;
  }
  .card {
    border: 1px solid var(--line);
    background: rgba(16, 24, 36, 0.88);
    padding: 1rem 1rem 0.95rem;
  }
  .card h2 {
    margin: 0 0 0.65rem;
    color: var(--amber);
    font-size: 0.8rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
  }
  p, li {
    margin: 0;
    color: var(--text);
    line-height: 1.6;
    font-size: 0.86rem;
  }
  ul, ol {
    margin: 0;
    padding-left: 1.1rem;
    display: grid;
    gap: 0.45rem;
  }
  .actions {
    margin-top: 1rem;
    display: flex;
    flex-wrap: wrap;
    gap: 0.55rem;
  }
  .btn {
    border: 1px solid var(--line);
    color: var(--text);
    text-decoration: none;
    font-size: 0.72rem;
    letter-spacing: 0.11em;
    text-transform: uppercase;
    padding: 0.48rem 0.62rem;
    display: inline-block;
  }
  .btn.primary {
    border-color: var(--amber);
    color: var(--amber);
  }
  .endpoint {
    margin-top: 0.5rem;
    display: block;
    color: var(--teal);
    background: rgba(7, 11, 16, 0.85);
    border: 1px solid var(--line);
    padding: 0.45rem 0.5rem;
    font-size: 0.76rem;
    overflow-wrap: anywhere;
  }
  .small { color: var(--dim); font-size: 0.72rem; }
  code {
    font-family: var(--mono);
    color: var(--teal);
    font-size: 0.8rem;
  }
  @media (max-width: 860px) {
    .grid { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
  <main class="wrap">
    <h1 class="title">MEMORY<span>VAULT</span> MCP</h1>
    <p class="sub">Human Guide For The MCP Endpoint</p>

    <div class="grid">
      <section class="card">
        <h2>What This MCP Does</h2>
        <p>This server is a personal memory graph for AI clients. It stores memories (notes, facts, journal entries), links related memories, scores confidence/importance, supports snapshots, and exposes these capabilities as MCP tools.</p>
        <div class="actions">
          <a class="btn primary" href="${viewerEndpoint}">Open Web Viewer</a>
          <a class="btn" href="${authzMetadata}">OAuth Metadata</a>
          <a class="btn" href="${resourceMetadata}">Resource Metadata</a>
        </div>
      </section>

      <section class="card">
        <h2>Connect From AI Tools</h2>
        <ol>
          <li>Set your MCP server URL to <code>${mcpEndpoint}</code>.</li>
          <li>Leave API key blank to use OAuth sign-in.</li>
          <li>Authorize once; your client receives access/refresh tokens.</li>
          <li>Call MCP methods like <code>tools/list</code> and <code>tools/call</code>.</li>
        </ol>
      </section>

      <section class="card">
        <h2>Direct Endpoints</h2>
        <p class="small">MCP endpoint (JSON-RPC / SSE):</p>
        <a class="endpoint" href="${mcpEndpoint}">${mcpEndpoint}</a>
        <p class="small" style="margin-top:0.7rem">Viewer UI:</p>
        <a class="endpoint" href="${viewerEndpoint}">${viewerEndpoint}</a>
      </section>

      <section class="card">
        <h2>Why You See This Page</h2>
        <p>Browser navigation to <code>/mcp</code> now shows this guide. Programmatic MCP requests still receive OAuth challenge/auth-required responses unless authorized.</p>
      </section>
    </div>
  </main>
</body>
</html>`;
}

type EndpointGuide = {
  title: string;
  subtitle: string;
  endpointPath: string;
  methods: string;
  auth: string;
  details: string[];
};

function endpointGuideForPath(pathname: string): EndpointGuide | null {
  if (pathname === '/register') {
    return {
      title: 'OAuth Client Registration',
      subtitle: 'Dynamic client registration endpoint',
      endpointPath: '/register',
      methods: 'POST',
      auth: 'Trusted redirect domains can self-register; all other clients require an admin bearer token.',
      details: [
        'Registers an OAuth client for MCP access.',
        'Expected body includes redirect_uris and token_endpoint_auth_method.',
        'redirect_uris on poke.com or claude.ai can register without Authorization.',
        'All other redirect domains must send Authorization: Bearer ADMIN_TOKEN.',
        'Returns client_id and optional client_secret metadata.',
      ],
    };
  }
  if (pathname === '/authorize') {
    return {
      title: 'OAuth Authorization',
      subtitle: 'Authorization code + PKCE entry point',
      endpointPath: '/authorize',
      methods: 'GET, POST',
      auth: 'User authentication is performed here (signup/login/token mode).',
      details: [
        'Starts or completes the OAuth authorization flow.',
        'Returns an authorization code via redirect_uri.',
        'Used by MCP clients during first-time connection.',
      ],
    };
  }
  if (pathname === '/token') {
    return {
      title: 'OAuth Token Exchange',
      subtitle: 'Authorization code / refresh token exchange',
      endpointPath: '/token',
      methods: 'POST',
      auth: 'Client credentials vary by client type; PKCE is required for authorization_code.',
      details: [
        'Exchanges authorization codes for access and refresh tokens.',
        'Also rotates refresh tokens using grant_type=refresh_token.',
        'Returns OAuth-compliant token responses in JSON.',
      ],
    };
  }
  if (pathname === '/.well-known/oauth-authorization-server' || pathname === '/.well-known/openid-configuration') {
    return {
      title: 'Authorization Server Metadata',
      subtitle: 'OAuth discovery document',
      endpointPath: '/.well-known/oauth-authorization-server',
      methods: 'GET',
      auth: 'Public metadata endpoint.',
      details: [
        'Advertises authorization, token, and registration endpoints.',
        'Used by MCP and OAuth clients for auto-discovery.',
        'Includes supported grants, auth methods, and code challenge methods.',
      ],
    };
  }
  if (pathname === '/.well-known/oauth-protected-resource' || pathname.startsWith('/.well-known/oauth-protected-resource/')) {
    return {
      title: 'Protected Resource Metadata',
      subtitle: 'Resource metadata for MCP protected endpoints',
      endpointPath: '/.well-known/oauth-protected-resource',
      methods: 'GET',
      auth: 'Public metadata endpoint.',
      details: [
        'Describes which authorization server protects this resource.',
        'Used in WWW-Authenticate challenges for MCP endpoints.',
        'The /mcp-specific variant resolves metadata for that resource path.',
      ],
    };
  }
  if (pathname === '/auth/signup') {
    return {
      title: 'User Signup API',
      subtitle: 'Create account + primary brain',
      endpointPath: '/auth/signup',
      methods: 'POST',
      auth: 'No token required.',
      details: [
        'Creates a user account from email/password.',
        'Optionally accepts brain_name for the initial memory brain.',
        'Sets httpOnly auth_token and refresh_token cookies on success using SameSite=Lax.',
        'Returns { success: true, user } on success.',
      ],
    };
  }
  if (pathname === '/auth/login') {
    return {
      title: 'User Login API',
      subtitle: 'Credential login endpoint',
      endpointPath: '/auth/login',
      methods: 'POST',
      auth: 'No token required.',
      details: [
        'Authenticates user email/password credentials.',
        'Sets httpOnly auth_token and refresh_token cookies using SameSite=Lax.',
        'Returns { success: true, user } on success.',
        'Used by the web viewer and OAuth-assisted flows.',
      ],
    };
  }
  if (pathname === '/auth/refresh') {
    return {
      title: 'Token Refresh API',
      subtitle: 'Rotate session using refresh token',
      endpointPath: '/auth/refresh',
      methods: 'POST',
      auth: 'No access token required; requires refresh_token cookie.',
      details: [
        'Reads refresh_token from the Cookie header.',
        'Issues new auth_token and refresh_token cookies using SameSite=Lax.',
        'Revokes/replaces previous refresh token for session safety.',
        'Returns { success: true } on success.',
      ],
    };
  }
  if (pathname === '/auth/logout') {
    return {
      title: 'Logout API',
      subtitle: 'Revoke a refresh token session',
      endpointPath: '/auth/logout',
      methods: 'POST',
      auth: 'Clears auth cookies and revokes the current session when possible.',
      details: [
        'Clears both auth cookies on the server response with Max-Age=0.',
        'Returns { success: true } on success.',
        'Used when user signs out from the web viewer.',
      ],
    };
  }
  if (pathname === '/auth/me') {
    return {
      title: 'Session Check API',
      subtitle: 'Validate current authenticated session',
      endpointPath: '/auth/me',
      methods: 'GET',
      auth: 'Requires Authorization: Bearer <access_token> or auth_token cookie.',
      details: [
        'Validates current access token.',
        'Returns { ok: true } when the session is valid.',
      ],
    };
  }
  if (pathname === '/auth/sessions') {
    return {
      title: 'Session List API',
      subtitle: 'List active sessions for the current user',
      endpointPath: '/auth/sessions',
      methods: 'GET',
      auth: 'Requires Authorization: Bearer <access_token> or auth_token cookie.',
      details: [
        'Returns active sessions bound to the authenticated user.',
        'Used for account/session management and audit.',
      ],
    };
  }
  if (pathname === '/auth/sessions/revoke') {
    return {
      title: 'Session Revoke API',
      subtitle: 'Revoke one or more active sessions',
      endpointPath: '/auth/sessions/revoke',
      methods: 'POST',
      auth: 'Requires Authorization: Bearer <access_token> or auth_token cookie.',
      details: [
        'Revokes target session(s), including all-other-sessions mode.',
        'Used to lock out stale or compromised sessions.',
      ],
    };
  }
  if (pathname === '/api/memories') {
    return {
      title: 'Memories API',
      subtitle: 'List/search/create memory records',
      endpointPath: '/api/memories',
      methods: 'GET, POST',
      auth: 'Requires Authorization: Bearer <access_token>, auth_token cookie, or legacy AUTH_SECRET.',
      details: [
        'Returns memory records scoped to your brain.',
        'Supports type and search filtering via query params.',
        'Backs both web UI and MCP tool operations.',
      ],
    };
  }
  if (pathname === '/api/tools') {
    return {
      title: 'Tool Catalog API',
      subtitle: 'List MCP tools exposed by this server',
      endpointPath: '/api/tools',
      methods: 'GET',
      auth: 'Requires Authorization: Bearer <access_token>, auth_token cookie, or legacy AUTH_SECRET.',
      details: [
        'Returns the tool metadata available to MCP clients.',
        'Primarily useful for diagnostics and integration checks.',
      ],
    };
  }
  if (pathname === '/api/graph') {
    return {
      title: 'Memory Graph API',
      subtitle: 'Graph nodes + explicit/inferred links',
      endpointPath: '/api/graph',
      methods: 'GET',
      auth: 'Requires Authorization: Bearer <access_token>, auth_token cookie, or legacy AUTH_SECRET.',
      details: [
        'Returns graph nodes, explicit edges, and inferred edges.',
        'Used by the graph visualization in /view.',
      ],
    };
  }
  if (pathname.startsWith('/api/links/')) {
    return {
      title: 'Memory Links API',
      subtitle: 'Get links for a specific memory id',
      endpointPath: '/api/links/:memoryId',
      methods: 'GET',
      auth: 'Requires Authorization: Bearer <access_token>, auth_token cookie, or legacy AUTH_SECRET.',
      details: [
        'Returns outbound/inbound links for one memory.',
        'Path parameter is the target memory id.',
      ],
    };
  }
  return null;
}

function endpointGuideHtml(url: URL, guide: EndpointGuide): string {
  const origin = url.origin;
  const mcpEndpoint = `${origin}/mcp`;
  const viewerEndpoint = `${origin}/view`;
  const endpointUrl = guide.endpointPath.includes(':')
    ? `${origin}${guide.endpointPath}`
    : `${origin}${guide.endpointPath}`;
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${guide.title} · MemoryVault</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #070b10;
    --bg2: #101824;
    --line: #234061;
    --text: #d8e8f8;
    --dim: #6f8ea9;
    --amber: #f0a500;
    --teal: #00c8b4;
    --mono: 'Share Tech Mono', monospace;
    --sans: 'Syne', sans-serif;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: var(--mono);
    color: var(--text);
    background:
      radial-gradient(70% 50% at 12% 0%, rgba(0, 200, 180, 0.14), transparent 70%),
      radial-gradient(60% 60% at 100% 100%, rgba(240, 165, 0, 0.12), transparent 70%),
      var(--bg);
    min-height: 100vh;
  }
  .wrap {
    max-width: 920px;
    margin: 0 auto;
    padding: 2rem 1.2rem 2.6rem;
  }
  .title {
    font-family: var(--sans);
    font-size: clamp(1.4rem, 3vw, 2.2rem);
    font-weight: 800;
    letter-spacing: -0.02em;
    margin: 0;
  }
  .title span { color: var(--amber); }
  .sub {
    margin: 0.35rem 0 1.2rem;
    color: var(--dim);
    letter-spacing: 0.08em;
    font-size: 0.72rem;
    text-transform: uppercase;
  }
  .grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.95rem;
  }
  .card {
    border: 1px solid var(--line);
    background: rgba(16, 24, 36, 0.88);
    padding: 0.95rem 1rem;
  }
  .span-2 { grid-column: 1 / -1; }
  .label {
    color: var(--amber);
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    margin: 0 0 0.45rem;
  }
  p, li {
    margin: 0;
    line-height: 1.6;
    font-size: 0.84rem;
  }
  ul {
    margin: 0;
    padding-left: 1.05rem;
    display: grid;
    gap: 0.4rem;
  }
  .endpoint {
    display: block;
    margin-top: 0.35rem;
    color: var(--teal);
    background: rgba(7, 11, 16, 0.85);
    border: 1px solid var(--line);
    padding: 0.45rem 0.5rem;
    font-size: 0.76rem;
    overflow-wrap: anywhere;
    text-decoration: none;
  }
  .actions {
    display: flex;
    flex-wrap: wrap;
    gap: 0.55rem;
    margin-top: 0.8rem;
  }
  .btn {
    border: 1px solid var(--line);
    color: var(--text);
    text-decoration: none;
    font-size: 0.7rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.45rem 0.6rem;
  }
  .btn.primary { border-color: var(--amber); color: var(--amber); }
  code { color: var(--teal); }
  @media (max-width: 800px) {
    .grid { grid-template-columns: 1fr; }
    .span-2 { grid-column: auto; }
  }
</style>
</head>
<body>
  <main class="wrap">
    <h1 class="title">MEMORY<span>VAULT</span> Endpoint Guide</h1>
    <p class="sub">${guide.title}</p>
    <div class="grid">
      <section class="card span-2">
        <p class="label">Purpose</p>
        <p>${guide.subtitle}</p>
      </section>
      <section class="card">
        <p class="label">Endpoint</p>
        <a class="endpoint" href="${endpointUrl}">${endpointUrl}</a>
      </section>
      <section class="card">
        <p class="label">Methods</p>
        <p><code>${guide.methods}</code></p>
        <p class="label" style="margin-top:0.7rem">Auth</p>
        <p>${guide.auth}</p>
      </section>
      <section class="card span-2">
        <p class="label">How To Use</p>
        <ul>
          ${guide.details.map((item) => `<li>${item}</li>`).join('')}
        </ul>
        <div class="actions">
          <a class="btn primary" href="${mcpEndpoint}">MCP Guide</a>
          <a class="btn" href="${viewerEndpoint}">Open Viewer</a>
          <a class="btn" href="${origin}/.well-known/oauth-authorization-server">OAuth Metadata</a>
        </div>
      </section>
    </div>
  </main>
</body>
</html>`;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const response = await (async (): Promise<Response> => {
      const url = new URL(request.url);
      if (url.pathname.startsWith('/mcp/')) {
        url.pathname = url.pathname === '/mcp/' ? '/mcp' : (url.pathname.slice('/mcp'.length) || '/');
      }

      if (request.method === 'OPTIONS') {
        return new Response(null, { headers: CORS_HEADERS });
      }

      await ensureSchema(env);

      if (url.pathname === '/' && isLikelyMcpRootRequest(request)) {
        const mcpUrl = new URL(url.toString());
        mcpUrl.pathname = '/mcp';
        const authCtx = await authenticateRequest(request, env);
        if (!authCtx) return unauthorized(mcpUrl);
        return handleMcp(request, env, mcpUrl, authCtx);
      }

      if (url.pathname === '/') {
        if (isBrowserDocumentRequest(request)) {
          return new Response(rootLandingHtml(url), {
            headers: { 'Content-Type': 'text/html; charset=utf-8' },
          });
        }
        return jsonResponse({ name: SERVER_NAME, version: SERVER_VERSION, status: 'ok', tools: TOOLS.length });
      }

      if (url.pathname === '/view') {
        return new Response(viewerHtml(), {
          headers: { 'Content-Type': 'text/html; charset=utf-8' },
        });
      }

      if (url.pathname === '/view.js') {
        return new Response(viewerScript(), {
          headers: { 'Content-Type': 'application/javascript; charset=utf-8' },
        });
      }

      if (isBrowserDocumentRequest(request)) {
        if (url.pathname === '/mcp') {
          return new Response(mcpLandingHtml(url), {
            headers: { 'Content-Type': 'text/html; charset=utf-8' },
          });
        }
        if (!isOAuthAuthorizeNavigation(url)) {
          const guide = endpointGuideForPath(url.pathname);
          if (guide) {
            return new Response(endpointGuideHtml(url, guide), {
              headers: { 'Content-Type': 'text/html; charset=utf-8' },
            });
          }
        }
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
        return handleAuthSignup(request, env);
      }

      if (url.pathname === '/auth/login') {
        if (request.method !== 'POST') return corsJsonResponse({ error: 'Method not allowed' }, 405);
        return handleAuthLogin(request, env);
      }

      if (url.pathname === '/auth/refresh') {
        if (request.method !== 'POST') return corsJsonResponse({ error: 'Method not allowed' }, 405);
        return handleAuthRefresh(request, env);
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

      if (url.pathname === '/auth/sessions') {
        if (request.method !== 'GET') return corsJsonResponse({ error: 'Method not allowed' }, 405);
        const authCtx = await authenticateRequest(request, env);
        if (!authCtx) return unauthorized(url);
        return handleAuthSessions(authCtx, env);
      }

      if (url.pathname === '/auth/sessions/revoke') {
        if (request.method !== 'POST') return corsJsonResponse({ error: 'Method not allowed' }, 405);
        const authCtx = await authenticateRequest(request, env);
        if (!authCtx) return unauthorized(url);
        return handleAuthSessionRevoke(request, authCtx, env);
      }

      if (url.pathname === '/api/memories') {
        const authCtx = await authenticateRequest(request, env);
        if (!authCtx) return unauthorized(url);
        return handleApiMemories(request, env, authCtx.brainId);
      }

      if (url.pathname === '/api/tools') {
        const authCtx = await authenticateRequest(request, env);
        if (!authCtx) return unauthorized(url);
        return handleApiTools(authCtx);
      }

      if (url.pathname === '/mcp') {
        const authCtx = await authenticateRequest(request, env);
        if (!authCtx) {
          return unauthorized(url);
        }
        return handleMcp(request, env, url, authCtx);
      }

      // GET /api/links/:id
      if (url.pathname.startsWith('/api/links/')) {
        const authCtx = await authenticateRequest(request, env);
        if (!authCtx) return unauthorized(url);
        const memoryId = url.pathname.slice('/api/links/'.length);
        if (!memoryId) return corsJsonResponse({ error: 'Memory ID required' }, 400);
        return handleApiLinks(memoryId, env, authCtx.brainId);
      }

      // GET /api/graph
      if (url.pathname === '/api/graph') {
        const authCtx = await authenticateRequest(request, env);
        if (!authCtx) return unauthorized(url);
        return handleApiGraph(env, authCtx.brainId);
      }

      return jsonResponse({ error: 'Not found' }, 404);
    })();

    const secureResponse = isHtmlResponse(response) ? wrapWithSecurityHeaders(response) : response;
    return applyCors(request, secureResponse);
  },
};
