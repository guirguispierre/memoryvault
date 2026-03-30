import type {
  Env,
  MemorySearchMode,
  MemoryType,
  RelationType,
  SessionTokens,
  CorsJsonResponseOptions,
  AuthContext,
} from './types.js';

import {
  VALID_TYPES,
  RELATION_TYPES,
} from './types.js';

import {
  AUTH_TOKEN_COOKIE_NAME,
  REFRESH_TOKEN_COOKIE_NAME,
  AUTH_TOKEN_COOKIE_MAX_AGE_SECONDS,
  AUTH_TOKEN_COOKIE_PATH,
  REFRESH_TOKEN_COOKIE_PATH,
  SESSION_COOKIE_SAME_SITE,
  REFRESH_TOKEN_TTL_SECONDS,
} from './constants.js';

export function generateId(): string {
  return crypto.randomUUID();
}

export function now(): number {
  return Math.floor(Date.now() / 1000);
}

export function withPrimaryDbEnv(env: Env): Env {
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

export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

export function mergeHeaders(target: Headers, source?: HeadersInit): Headers {
  if (!source) return target;
  const headers = new Headers(source);
  headers.forEach((value, key) => target.set(key, value));
  return target;
}

export function buildCorsJsonHeaders(
  corsHeaders: Record<string, string>,
  options: CorsJsonResponseOptions = {}
): Headers {
  const headers = mergeHeaders(new Headers(corsHeaders), options.headers);
  headers.set('Content-Type', 'application/json');
  for (const cookie of options.cookies ?? []) {
    headers.append('Set-Cookie', cookie);
  }
  return headers;
}

export function normalizeCorsJsonResponseOptions(
  options: CorsJsonResponseOptions | Record<string, string>
): CorsJsonResponseOptions {
  const candidate = options as CorsJsonResponseOptions;
  if (Array.isArray(candidate.cookies) || Object.prototype.hasOwnProperty.call(candidate, 'headers')) {
    return candidate;
  }
  return { headers: options as Record<string, string> };
}

export function parseRequestCookies(request: Request): Map<string, string> {
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

export function getRequestCookie(request: Request, name: string): string | null {
  return parseRequestCookies(request).get(name) ?? null;
}

export function serializeCookie(
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

export function buildSessionCookieHeaders(tokens: SessionTokens): string[] {
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

export function clearSessionCookieHeaders(): string[] {
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

export function buildRotatedSessionCookieHeaders(tokens: SessionTokens): string[] {
  return [...clearSessionCookieHeaders(), ...buildSessionCookieHeaders(tokens)];
}

export function parseBearerToken(request: Request): string | null {
  const auth = request.headers.get('Authorization');
  if (!auth) return null;
  const match = auth.match(/^\s*Bearer\s+(.+?)\s*$/i);
  if (!match) return null;
  return match[1] || null;
}

export function getAccessTokenFromRequest(request: Request): string | null {
  return parseBearerToken(request) ?? getRequestCookie(request, AUTH_TOKEN_COOKIE_NAME);
}

export function clampToRange(input: unknown, fallback: number, min = 0, max = 1): number {
  const n = typeof input === 'number' ? input : Number(input);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(n, min), max);
}

export function isMemorySearchMode(value: unknown): value is MemorySearchMode {
  return value === 'lexical' || value === 'semantic' || value === 'hybrid';
}

export function hasSemanticSearchBindings(env: Env): env is Env & { AI: Ai; MEMORY_INDEX: Vectorize } {
  return Boolean(env.AI && env.MEMORY_INDEX);
}

export function normalizeSemanticScore(rawScore: number): number {
  if (!Number.isFinite(rawScore)) return 0;
  return clampToRange((rawScore + 1) / 2, 0, 0, 1);
}

export function truncateForMetadata(value: string, max = 120): string {
  const trimmed = value.trim();
  if (!trimmed) return '';
  return trimmed.length <= max ? trimmed : trimmed.slice(0, max);
}

export function parseTags(tags: unknown): string[] {
  if (typeof tags !== 'string' || !tags.trim()) return [];
  return tags
    .split(',')
    .map((tag) => tag.trim())
    .filter(Boolean);
}

export function isValidType(t: unknown): t is MemoryType {
  return typeof t === 'string' && (VALID_TYPES as readonly string[]).includes(t);
}

export function isValidRelationType(t: unknown): t is RelationType {
  return typeof t === 'string' && (RELATION_TYPES as readonly string[]).includes(t);
}

export function canMutateMemories(authCtx: AuthContext): boolean {
  if (authCtx.kind === 'legacy') return true;
  return typeof authCtx.userId === 'string' && authCtx.userId.trim().length > 0;
}

export async function readJsonBody(request: Request): Promise<Record<string, unknown> | null> {
  try {
    const body = await request.json();
    if (!body || typeof body !== 'object' || Array.isArray(body)) return null;
    return body as Record<string, unknown>;
  } catch {
    return null;
  }
}

export function escapeHtml(input: string): string {
  return input
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

export function toFiniteNumber(value: unknown, fallback: number): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export function normalizeSourceKey(raw: string): string {
  return raw.trim().toLowerCase().replace(/\s+/g, ' ').slice(0, 160);
}

export function normalizeTag(raw: string): string {
  return raw.trim().toLowerCase();
}

export function parseTagSet(raw: unknown): Set<string> {
  if (typeof raw !== 'string' || !raw.trim()) return new Set();
  return new Set(
    raw.split(',')
      .map((tag) => normalizeTag(tag))
      .filter(Boolean)
      .slice(0, 64)
  );
}

export function normalizeRelation(raw: unknown): RelationType {
  return isValidRelationType(raw) ? raw : 'related';
}

export function stableJson(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return JSON.stringify({ note: 'unserializable payload' });
  }
}

export function sanitizeDisplayName(raw: unknown, email: string): string | null {
  if (typeof raw === 'string') {
    const trimmed = raw.trim();
    if (trimmed) return trimmed.slice(0, 120);
  }
  const local = email.split('@')[0]?.replace(/[^a-z0-9]+/gi, ' ').trim();
  return local ? local.slice(0, 120) : null;
}

export function sanitizeBrainName(raw: unknown, email: string): string {
  if (typeof raw === 'string') {
    const trimmed = raw.trim();
    if (trimmed) return trimmed.slice(0, 120);
  }
  const local = email.split('@')[0]?.replace(/[^a-z0-9]+/gi, ' ').trim();
  return local ? `${local.slice(0, 64)}'s Second Brain` : 'Second Brain';
}

export function userPayload(row: { id: string; email: string; display_name: string | null; created_at: number }): Record<string, unknown> {
  return {
    id: row.id,
    email: row.email,
    display_name: row.display_name,
    created_at: row.created_at,
  };
}

export function isLikelyMcpRootRequest(request: Request): boolean {
  const accept = (request.headers.get('Accept') ?? '').toLowerCase();
  const contentType = (request.headers.get('Content-Type') ?? '').toLowerCase();
  if (accept.includes('text/event-stream')) return true;
  if (request.headers.has('MCP-Protocol-Version') || request.headers.has('mcp-protocol-version')) return true;
  if (request.method === 'POST' && contentType.includes('application/json')) return true;
  return false;
}

export function isBrowserDocumentRequest(request: Request): boolean {
  if (request.method !== 'GET') return false;
  const accept = (request.headers.get('Accept') ?? '').toLowerCase();
  if (accept.includes('text/event-stream')) return false;
  if (request.headers.has('MCP-Protocol-Version') || request.headers.has('mcp-protocol-version')) return false;
  const fetchDest = (request.headers.get('Sec-Fetch-Dest') ?? '').toLowerCase();
  const fetchMode = (request.headers.get('Sec-Fetch-Mode') ?? '').toLowerCase();
  if (fetchDest === 'document' || fetchMode === 'navigate') return true;
  return accept.includes('text/html');
}

export function isOAuthAuthorizeNavigation(url: URL): boolean {
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

export function normalizeResourcePath(pathname: string): string {
  const input = (pathname || '').trim();
  const withLeadingSlash = input ? (input.startsWith('/') ? input : `/${input}`) : '/';
  const normalized = withLeadingSlash.replace(/\/+$/, '') || '/';
  if (normalized === '/') return '/mcp';
  return normalized;
}

export function protectedResourceMetadataUrl(url: URL, resourcePath = '/mcp'): string {
  const normalized = normalizeResourcePath(resourcePath);
  return `${url.origin}/.well-known/oauth-protected-resource${normalized}`;
}

export function oauthChallengeHeader(url: URL): string {
  return `Bearer realm="mcp", resource_metadata="${protectedResourceMetadataUrl(url, url.pathname)}"`;
}

export function parseJsonStringArray(raw: string, fallback: string[] = []): string[] {
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) return fallback;
    return parsed.filter((v): v is string => typeof v === 'string');
  } catch {
    return fallback;
  }
}
