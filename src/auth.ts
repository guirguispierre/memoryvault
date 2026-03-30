import type {
  Env,
  SessionTokens,
  AuthContext,
  AccessTokenPayload,
  RefreshSessionRow,
  UserRow,
} from './types.js';

import {
  LEGACY_BRAIN_ID,
  LEGACY_USER_ID,
  LEGACY_USER_EMAIL,
  ACCESS_TOKEN_TTL_SECONDS,
  REFRESH_TOKEN_TTL_SECONDS,
  REFRESH_TOKEN_COOKIE_NAME,
  AUTH_RATE_LIMIT_MAX_ATTEMPTS,
  AUTH_RATE_LIMIT_WINDOW_SECONDS,
} from './constants.js';

import {
  generateId,
  now,
  withPrimaryDbEnv,
  readJsonBody,
  getRequestCookie,
  buildSessionCookieHeaders,
  clearSessionCookieHeaders,
  buildRotatedSessionCookieHeaders,
  getAccessTokenFromRequest,
  sanitizeDisplayName,
  sanitizeBrainName,
  userPayload,
  slugify,
} from './utils.js';

import {
  sha256DigestBase64Url,
  hashPassword,
  verifyPassword,
  randomToken,
  normalizeEmail,
  isValidEmail,
  isStrongEnoughPassword,
  signAccessToken,
  verifyAccessToken,
} from './crypto.js';

import { corsJsonResponse } from './cors.js';

import {
  listBrainsForUser,
  findActiveBrain,
} from './db.js';

/* ------------------------------------------------------------------ */
/*  Session token management                                          */
/* ------------------------------------------------------------------ */

export async function createSessionTokens(userId: string, brainId: string, env: Env, clientId: string | null = null): Promise<{
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

export async function getRefreshSessionByToken(refreshToken: string, env: Env): Promise<RefreshSessionRow | null> {
  const refreshHash = await sha256DigestBase64Url(refreshToken);
  return env.DB.prepare(
    `SELECT id, user_id, brain_id, client_id, expires_at, revoked_at
     FROM auth_sessions
     WHERE refresh_hash = ?
     LIMIT 1`
  ).bind(refreshHash).first<RefreshSessionRow>();
}

export async function rotateSession(refreshToken: string, env: Env): Promise<{
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

export async function revokeSession(refreshToken: string, env: Env): Promise<boolean> {
  const refreshHash = await sha256DigestBase64Url(refreshToken);
  const ts = now();
  const result = await env.DB.prepare(
    'UPDATE auth_sessions SET revoked_at = ? WHERE refresh_hash = ? AND revoked_at IS NULL'
  ).bind(ts, refreshHash).run();
  return (result.meta.changes ?? 0) > 0;
}

export async function revokeSessionById(sessionId: string, env: Env): Promise<boolean> {
  const ts = now();
  const result = await env.DB.prepare(
    'UPDATE auth_sessions SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL'
  ).bind(ts, sessionId).run();
  return (result.meta.changes ?? 0) > 0;
}

/* ------------------------------------------------------------------ */
/*  Request authentication                                            */
/* ------------------------------------------------------------------ */

/**
 * Validate the OAuth client for a session. Returns null if the client
 * is invalid/purged, or the client row otherwise.
 * This callback is provided by the OAuth module to avoid circular imports.
 */
export type ValidateOAuthClientFn = (clientId: string, env: Env) => Promise<unknown | null>;

export async function authenticateRequest(
  request: Request,
  env: Env,
  validateOAuthClient?: ValidateOAuthClientFn
): Promise<AuthContext | null> {
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
  if (row.client_id && validateOAuthClient) {
    const client = await validateOAuthClient(row.client_id, authEnv);
    if (!client) return null;
  }
  await authEnv.DB.prepare('UPDATE auth_sessions SET used_at = ? WHERE id = ?').bind(ts, row.id).run();
  return { kind: 'user', brainId: row.brain_id, userId: row.user_id, sessionId: row.id, clientId: row.client_id ?? null };
}

/* ------------------------------------------------------------------ */
/*  Rate limiting                                                     */
/* ------------------------------------------------------------------ */

export function authRateLimitPrefix(ip: string): string {
  return `rl:login:${ip || 'unknown'}`;
}

export function authRateLimitKey(ip: string): string {
  return `${authRateLimitPrefix(ip)}:${Date.now()}`;
}

export async function checkRateLimit(ip: string, kv: KVNamespace): Promise<boolean> {
  await kv.put(authRateLimitKey(ip), '1', { expirationTtl: AUTH_RATE_LIMIT_WINDOW_SECONDS });
  const listed = await kv.list({ prefix: authRateLimitPrefix(ip) });
  return listed.keys.length > AUTH_RATE_LIMIT_MAX_ATTEMPTS;
}

export async function resetAuthRateLimit(ip: string, kv: KVNamespace): Promise<void> {
  const listed = await kv.list({ prefix: authRateLimitPrefix(ip) });
  await Promise.all(listed.keys.map((key) => kv.delete(key.name)));
}

/* ------------------------------------------------------------------ */
/*  Legacy token                                                      */
/* ------------------------------------------------------------------ */

export async function ensureLegacyTokenPrincipal(env: Env): Promise<{ userId: string; brainId: string }> {
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

export function normalizeLegacyToken(raw: unknown): string {
  if (typeof raw !== 'string') return '';
  const trimmed = raw.trim();
  if (!trimmed) return '';
  const match = trimmed.match(/^\s*Bearer\s+(.+?)\s*$/i);
  return (match?.[1] ?? trimmed).trim();
}

/* ------------------------------------------------------------------ */
/*  Auth endpoint handlers                                            */
/* ------------------------------------------------------------------ */

export async function handleAuthSignup(request: Request, env: Env): Promise<Response> {
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

export async function handleAuthLogin(request: Request, env: Env): Promise<Response> {
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

export async function handleAuthRefresh(request: Request, env: Env): Promise<Response> {
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

export async function handleAuthLogout(
  request: Request,
  env: Env,
  authenticateRequestFn: (request: Request, env: Env) => Promise<AuthContext | null>
): Promise<Response> {
  const refreshToken = (getRequestCookie(request, REFRESH_TOKEN_COOKIE_NAME) ?? '').trim();
  if (refreshToken) {
    await revokeSession(refreshToken, env);
  } else {
    const authCtx = await authenticateRequestFn(request, env);
    if (authCtx?.kind === 'user' && authCtx.sessionId) {
      await revokeSessionById(authCtx.sessionId, env);
    }
  }
  return corsJsonResponse({ success: true }, 200, { cookies: clearSessionCookieHeaders() });
}

export async function handleAuthMe(_authCtx: AuthContext, _env: Env): Promise<Response> {
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

export async function handleAuthSessions(authCtx: AuthContext, env: Env): Promise<Response> {
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

export async function handleAuthSessionRevoke(request: Request, authCtx: AuthContext, env: Env): Promise<Response> {
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
