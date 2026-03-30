import { type Env } from './types.js';
import { SERVER_NAME, SERVER_VERSION } from './constants.js';
import { jsonResponse, isLikelyMcpRootRequest, isBrowserDocumentRequest, isOAuthAuthorizeNavigation } from './utils.js';
import { CORS_HEADERS, corsJsonResponse, applyCors, isHtmlResponse, wrapWithSecurityHeaders, unauthorized } from './cors.js';
import { ensureSchema } from './db.js';
import {
  authenticateRequest,
  handleAuthSignup,
  handleAuthLogin,
  handleAuthRefresh,
  handleAuthLogout,
  handleAuthMe,
  handleAuthSessions,
  handleAuthSessionRevoke,
} from './auth.js';
import {
  getOAuthClient,
  purgeOAuthClientIfNotWhitelisted,
  handleOAuthAuthorize,
  handleOAuthToken,
  handleOAuthRegister,
  handleProtectedResourceMetadata,
  handleAuthorizationServerMetadata,
} from './oauth.js';
import { TOOLS } from './tools-schema.js';
import { viewerHtml, viewerScript } from './viewer.js';
import {
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
