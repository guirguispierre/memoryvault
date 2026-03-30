import type { Env } from './types.js';

import {
  SERVER_VERSION,
  DEFAULT_OAUTH_REDIRECT_DOMAIN_ALLOWLIST,
  TRUSTED_REDIRECT_DOMAINS,
} from './constants.js';

import {
  generateId,
  now,
  withPrimaryDbEnv,
  readJsonBody,
  parseBearerToken,
  escapeHtml,
  sanitizeDisplayName,
  sanitizeBrainName,
  slugify,
  parseJsonStringArray,
} from './utils.js';

import {
  sha256DigestBase64Url,
  hashPassword,
  verifyPassword,
  randomToken,
  normalizeEmail,
  isValidEmail,
  isStrongEnoughPassword,
} from './crypto.js';

import { CORS_HEADERS, corsJsonResponse } from './cors.js';

import {
  listBrainsForUser,
  findActiveBrain,
} from './db.js';

import {
  createSessionTokens,
  getRefreshSessionByToken,
  rotateSession,
  ensureLegacyTokenPrincipal,
  normalizeLegacyToken,
} from './auth.js';
export type OAuthClientRow = {
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

export type OAuthCodeRow = {
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

export const OAUTH_CODE_TTL_SECONDS = 10 * 60;

export function noStoreJsonHeaders(extra: Record<string, string> = {}): Record<string, string> {
  return {
    ...CORS_HEADERS,
    'Content-Type': 'application/json',
    'Cache-Control': 'no-store',
    'Pragma': 'no-cache',
    ...extra,
  };
}

export function oauthError(error: string, errorDescription: string, status = 400): Response {
  return new Response(JSON.stringify({
    error,
    error_description: errorDescription,
  }), {
    status,
    headers: noStoreJsonHeaders(),
  });
}

export function oauthRateLimitedError(): Response {
  return new Response(JSON.stringify({
    error: 'temporarily_unavailable',
    error_description: 'Too many failed attempts. Try again later.',
  }), {
    status: 429,
    headers: noStoreJsonHeaders({ 'Retry-After': '900' }),
  });
}

export function oauthUnauthorized(errorDescription = 'Unauthorized.'): Response {
  return new Response(JSON.stringify({
    error: 'unauthorized',
    error_description: errorDescription,
  }), {
    status: 401,
    headers: noStoreJsonHeaders({ 'WWW-Authenticate': 'Bearer' }),
  });
}

export function timingSafeEqualStrings(a: string, b: string): boolean {
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

export function isWhitelistedRedirectUri(raw: string, env: Env): boolean {
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

export function hasValidAdminBearer(request: Request, env: Env): boolean {
  const provided = parseBearerToken(request)?.trim() ?? '';
  const expected = (env.ADMIN_TOKEN ?? '').trim();
  if (!provided || !expected) return false;
  return timingSafeEqualStrings(provided, expected);
}


export async function readFormBody(request: Request): Promise<URLSearchParams | null> {
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

export async function getOAuthClient(clientId: string, env: Env): Promise<OAuthClientRow | null> {
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

export async function purgeOAuthClientIfNotWhitelisted(client: OAuthClientRow | null, env: Env): Promise<OAuthClientRow | null> {
  if (!client) return null;
  if (hasOnlyWhitelistedRedirectUris(client.redirect_uris, env)) return client;
  await revokeAndDeleteOAuthClient(client.client_id, client.id, env);
  return null;
}

export async function purgeNonWhitelistedOAuthClients(env: Env): Promise<number> {
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

export async function handleOAuthAuthorize(request: Request, url: URL, env: Env): Promise<Response> {
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

export async function handleOAuthToken(request: Request, env: Env): Promise<Response> {
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

export async function handleOAuthRegister(request: Request, env: Env): Promise<Response> {
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

export function handleProtectedResourceMetadata(url: URL): Response {
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

export function handleAuthorizationServerMetadata(url: URL): Response {
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
