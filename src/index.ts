import {
  type Env,
  type MemorySearchMode,
  type SemanticMemoryCandidate,
  type VectorSyncStats,
  type SessionTokens,
  type CorsJsonResponseOptions,
  type AuthContext,
  type AccessTokenPayload,
  type RefreshSessionRow,
  VALID_TYPES,
  type MemoryType,
  RELATION_TYPES,
  type RelationType,
  type LinkStats,
  type ScoreComponent,
  type DynamicScoreBreakdown,
  type BrainPolicy,
  type ToolDefinition,
  type ToolReleaseMeta,
  type ToolChangelogChange,
  type ToolChangelogEntry,
  type MemoryGraphNode,
  type MemoryGraphLink,
  type UserRow,
  type BrainSummary,
  type EndpointGuide,
  type ToolArgs,
} from './types.js';

import {
  SERVER_NAME,
  SERVER_VERSION,
  LEGACY_BRAIN_ID,
  LEGACY_USER_ID,
  LEGACY_USER_EMAIL,
  ACCESS_TOKEN_TTL_SECONDS,
  REFRESH_TOKEN_TTL_SECONDS,
  AUTH_TOKEN_COOKIE_NAME,
  REFRESH_TOKEN_COOKIE_NAME,
  AUTH_TOKEN_COOKIE_MAX_AGE_SECONDS,
  AUTH_TOKEN_COOKIE_PATH,
  REFRESH_TOKEN_COOKIE_PATH,
  SESSION_COOKIE_SAME_SITE,
  AUTH_RATE_LIMIT_MAX_ATTEMPTS,
  AUTH_RATE_LIMIT_WINDOW_SECONDS,
  PBKDF2_ITERATIONS,
  EMBEDDING_MODEL,
  VECTORIZE_QUERY_TOP_K_MAX,
  VECTORIZE_UPSERT_BATCH_SIZE,
  VECTORIZE_DELETE_BATCH_SIZE,
  VECTORIZE_SETTLE_POLL_INTERVAL_MS,
  VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS,
  VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS_MAX,
  EMBEDDING_BATCH_SIZE,
  MEMORY_SEARCH_FUSION_K,
  MEMORY_SEARCH_DEFAULT_LIMIT,
  MEMORY_SEARCH_MAX_LIMIT,
  VECTOR_ID_PREFIX,
  VECTOR_ID_MAX_MEMORY_ID_LENGTH,
  DEFAULT_OAUTH_REDIRECT_DOMAIN_ALLOWLIST,
  TRUSTED_REDIRECT_DOMAINS,
  DEFAULT_BRAIN_POLICY,
  EMPTY_LINK_STATS,
} from './constants.js';

import {
  generateId,
  now,
  withPrimaryDbEnv,
  jsonResponse,
  mergeHeaders,
  buildCorsJsonHeaders,
  normalizeCorsJsonResponseOptions,
  parseRequestCookies,
  getRequestCookie,
  serializeCookie,
  buildSessionCookieHeaders,
  clearSessionCookieHeaders,
  buildRotatedSessionCookieHeaders,
  parseBearerToken,
  getAccessTokenFromRequest,
  clampToRange,
  isMemorySearchMode,
  hasSemanticSearchBindings,
  normalizeSemanticScore,
  truncateForMetadata,
  parseTags,
  isValidType,
  isValidRelationType,
  canMutateMemories,
  readJsonBody,
  escapeHtml,
  toFiniteNumber,
  normalizeSourceKey,
  normalizeTag,
  parseTagSet,
  normalizeRelation,
  stableJson,
  sanitizeDisplayName,
  sanitizeBrainName,
  userPayload,
  isLikelyMcpRootRequest,
  isBrowserDocumentRequest,
  isOAuthAuthorizeNavigation,
  normalizeResourcePath,
  protectedResourceMetadataUrl,
  oauthChallengeHeader,
  parseJsonStringArray,
  slugify,
} from './utils.js';

import {
  bytesToBase64Url,
  base64UrlToBytes,
  hmacSha256,
  sha256DigestBase64Url,
  derivePasswordHash,
  hashPassword,
  verifyPassword,
  randomToken,
  normalizeEmail,
  isValidEmail,
  isStrongEnoughPassword,
  signAccessToken,
  verifyAccessToken,
} from './crypto.js';

import {
  ALLOWED_ORIGINS,
  CORS_HEADERS,
  HTML_SECURITY_HEADERS,
  corsJsonResponse,
  getCorsOrigin,
  mergeVaryHeader,
  applyCors,
  isHtmlResponse,
  wrapWithSecurityHeaders,
  unauthorized,
} from './cors.js';

import {
  runMigrationStatement,
  ensureSchema,
  parseJsonObject,
  sanitizePolicyPatch,
  normalizeLinkStats,
  loadMemoryRowsByIds,
  runLexicalMemorySearch,
  loadLinkStatsMap,
  loadSourceTrustMap,
  getBrainPolicy,
  setBrainPolicy,
  loadActiveMemoryNodes,
  loadExplicitMemoryLinks,
  ensureObjectiveRoot,
  logChangelog,
  normalizeWatchEventInput,
  parseWatchEventTypes,
  listBrainsForUser,
  findActiveBrain,
} from './db.js';

import {
  createSessionTokens,
  getRefreshSessionByToken,
  rotateSession,
  revokeSession,
  revokeSessionById,
  authenticateRequest,
  authRateLimitPrefix,
  authRateLimitKey,
  checkRateLimit,
  resetAuthRateLimit,
  ensureLegacyTokenPrincipal,
  normalizeLegacyToken,
  handleAuthSignup,
  handleAuthLogin,
  handleAuthRefresh,
  handleAuthLogout,
  handleAuthMe,
  handleAuthSessions,
  handleAuthSessionRevoke,
} from './auth.js';

import {
  type OAuthClientRow,
  type OAuthCodeRow,
  getOAuthClient,
  purgeOAuthClientIfNotWhitelisted,
  handleOAuthAuthorize,
  handleOAuthToken,
  handleOAuthRegister,
  handleProtectedResourceMetadata,
  handleAuthorizationServerMetadata,
  hasValidAdminBearer,
  readFormBody,
  noStoreJsonHeaders,
} from './oauth.js';

import {
  buildMemoryEmbeddingText,
  syncMemoriesToVectorIndex,
  safeSyncMemoriesToVectorIndex,
  safeDeleteMemoryVectors,
  querySemanticMemoryCandidates,
  fuseSearchRows,
  waitForVectorMutationReady,
  waitForVectorQueryReady,
} from './vectorize.js';

import {
  clamp01,
  round3,
  countKeywordHits,
  computeDynamicScoreBreakdown,
  computeDynamicScores,
  enrichMemoryRowsWithDynamics,
  projectMemoryForClient,
  enrichAndProjectRows,
} from './scoring.js';

import {
  TOOL_RELEASE_META,
  TOOL_CHANGELOG,
  getToolReleaseMeta,
  isToolDeprecated,
  compareSemver,
  parseSemver,
  TOOLS,
  MUTATING_TOOL_NAMES,
  isMutatingTool,
} from './tools-schema.js';

import {
  callTool,
} from './tools.js';

import {
  viewerHtml,
  viewerScript,
} from './viewer.js';

export type { Env };

async function validateOAuthClientForAuth(clientId: string, env: Env): Promise<unknown | null> {
  return purgeOAuthClientIfNotWhitelisted(await getOAuthClient(clientId, env), env);
}

function authRequestWithOAuth(request: Request, env: Env) {
  return authenticateRequest(request, env, validateOAuthClientForAuth);
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
        const authCtx = await authRequestWithOAuth(request, env);
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
        return handleAuthLogout(request, env, authRequestWithOAuth);
      }

      if (url.pathname === '/auth/me') {
        if (request.method !== 'GET') return corsJsonResponse({ error: 'Method not allowed' }, 405);
        const authCtx = await authRequestWithOAuth(request, env);
        if (!authCtx) return unauthorized(url);
        return handleAuthMe(authCtx, env);
      }

      if (url.pathname === '/auth/sessions') {
        if (request.method !== 'GET') return corsJsonResponse({ error: 'Method not allowed' }, 405);
        const authCtx = await authRequestWithOAuth(request, env);
        if (!authCtx) return unauthorized(url);
        return handleAuthSessions(authCtx, env);
      }

      if (url.pathname === '/auth/sessions/revoke') {
        if (request.method !== 'POST') return corsJsonResponse({ error: 'Method not allowed' }, 405);
        const authCtx = await authRequestWithOAuth(request, env);
        if (!authCtx) return unauthorized(url);
        return handleAuthSessionRevoke(request, authCtx, env);
      }

      if (url.pathname === '/api/memories') {
        const authCtx = await authRequestWithOAuth(request, env);
        if (!authCtx) return unauthorized(url);
        return handleApiMemories(request, env, authCtx.brainId);
      }

      if (url.pathname === '/api/tools') {
        const authCtx = await authRequestWithOAuth(request, env);
        if (!authCtx) return unauthorized(url);
        return handleApiTools(authCtx);
      }

      if (url.pathname === '/mcp') {
        const authCtx = await authRequestWithOAuth(request, env);
        if (!authCtx) {
          return unauthorized(url);
        }
        return handleMcp(request, env, url, authCtx);
      }

      // GET /api/links/:id
      if (url.pathname.startsWith('/api/links/')) {
        const authCtx = await authRequestWithOAuth(request, env);
        if (!authCtx) return unauthorized(url);
        const memoryId = url.pathname.slice('/api/links/'.length);
        if (!memoryId) return corsJsonResponse({ error: 'Memory ID required' }, 400);
        return handleApiLinks(memoryId, env, authCtx.brainId);
      }

      // GET /api/graph
      if (url.pathname === '/api/graph') {
        const authCtx = await authRequestWithOAuth(request, env);
        if (!authCtx) return unauthorized(url);
        return handleApiGraph(env, authCtx.brainId);
      }

      return jsonResponse({ error: 'Not found' }, 404);
    })();

    const secureResponse = isHtmlResponse(response) ? wrapWithSecurityHeaders(response) : response;
    return applyCors(request, secureResponse);
  },
};
