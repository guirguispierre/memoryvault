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

import {
  processMcpBody,
  handleMcp,
  handleApiMemories,
  handleApiLinks,
  handleApiGraph,
  handleApiTools,
  rootLandingHtml,
  mcpLandingHtml,
  endpointGuideForPath,
  endpointGuideHtml,
} from './routes.js';

export type { Env };

async function validateOAuthClientForAuth(clientId: string, env: Env): Promise<unknown | null> {
  return purgeOAuthClientIfNotWhitelisted(await getOAuthClient(clientId, env), env);
}

function authRequestWithOAuth(request: Request, env: Env) {
  return authenticateRequest(request, env, validateOAuthClientForAuth);
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
