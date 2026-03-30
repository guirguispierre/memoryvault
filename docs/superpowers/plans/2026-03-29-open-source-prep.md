# Open Source Preparation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prepare MemoryVault MCP for public open-source release as a self-hosted Cloudflare Workers template.

**Architecture:** Split the 13,209-line monolith `src/index.ts` into ~15 focused modules, scrub hardcoded infrastructure values, add MIT license, rewrite README for self-hosters, and add CONTRIBUTING.md.

**Tech Stack:** TypeScript, Cloudflare Workers, D1, Vectorize, Workers AI, MCP SDK

---

### Task 1: Extract `src/types.ts`

**Files:**
- Create: `src/types.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/types.ts` with all shared type definitions**

```typescript
// src/types.ts

export interface Env {
  DB: D1Database;
  RATE_LIMIT_KV: KVNamespace;
  AUTH_SECRET: string;
  ADMIN_TOKEN: string;
  OAUTH_REDIRECT_DOMAIN_ALLOWLIST?: string;
  AI?: Ai;
  MEMORY_INDEX?: Vectorize;
}

export type MemorySearchMode = 'lexical' | 'semantic' | 'hybrid';

export type SemanticMemoryCandidate = {
  memory_id: string;
  score: number;
  rank: number;
};

export type VectorSyncStats = {
  upserted: number;
  deleted: number;
  skipped: number;
  mutation_ids: string[];
  probe_vector_id: string | null;
};

export type SessionTokens = {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  refresh_expires_in: number;
  session_id: string;
};

export type CorsJsonResponseOptions = {
  headers?: HeadersInit;
  cookies?: string[];
};

export type AuthContext = {
  kind: 'legacy' | 'user';
  brainId: string;
  userId: string | null;
  sessionId: string | null;
  clientId: string | null;
};

export type AccessTokenPayload = {
  typ: 'access';
  sub: string;
  bid: string;
  sid: string;
  iat: number;
  exp: number;
};

export type RefreshSessionRow = {
  id: string;
  user_id: string;
  brain_id: string;
  client_id: string | null;
  expires_at: number;
  revoked_at: number | null;
};

export type UserRow = {
  id: string;
  email: string;
  password_hash: string;
  display_name: string | null;
  created_at: number;
};

export type BrainSummary = {
  id: string;
  name: string;
  slug: string | null;
  role: string;
  created_at: number;
  updated_at: number;
};

export type ToolDefinition = {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
};

export type ToolReleaseMeta = {
  introduced_in: string;
  deprecated_in?: string;
  replaced_by?: string;
  notes?: string;
};

export type ToolChangelogChange = {
  type: 'added' | 'updated' | 'deprecated' | 'security' | 'fix';
  target: 'tool' | 'endpoint' | 'scoring' | 'auth';
  name: string;
  description: string;
};

export type ToolChangelogEntry = {
  id: string;
  version: string;
  released_at: number;
  summary: string;
  changes: ToolChangelogChange[];
};

export type MemoryGraphNode = {
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

export type MemoryGraphLink = {
  id: string;
  from_id: string;
  to_id: string;
  relation_type: RelationType;
  label: string | null;
  inferred?: boolean;
  score?: number;
};

export type ScoreComponent = {
  factor: string;
  weight: number;
  raw: number;
  weighted: number;
  note: string;
};

export type DynamicScoreBreakdown = {
  dynamic_confidence: number;
  dynamic_importance: number;
  confidence_components: ScoreComponent[];
  importance_components: ScoreComponent[];
};

export type BrainPolicy = {
  max_inferred_edges: number;
  default_confidence: number;
  default_importance: number;
  decay_factor: number;
  consolidation_threshold: number;
};

export type EndpointGuide = {
  title: string;
  subtitle: string;
  endpointPath: string;
  methods: string;
  auth: string;
  details: string[];
};

export type ToolArgs = Record<string, unknown>;

export const VALID_TYPES = ['note', 'fact', 'journal'] as const;
export type MemoryType = typeof VALID_TYPES[number];

export const RELATION_TYPES = ['related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of'] as const;
export type RelationType = typeof RELATION_TYPES[number];
```

- [ ] **Step 2: Update `src/index.ts` to import from `src/types.ts`**

At the top of `src/index.ts`, add:
```typescript
import {
  type Env, type MemorySearchMode, type SemanticMemoryCandidate, type VectorSyncStats,
  type SessionTokens, type CorsJsonResponseOptions, type AuthContext, type AccessTokenPayload,
  type RefreshSessionRow, type UserRow, type BrainSummary, type ToolDefinition, type ToolReleaseMeta,
  type ToolChangelogChange, type ToolChangelogEntry, type MemoryGraphNode, type MemoryGraphLink,
  type ScoreComponent, type DynamicScoreBreakdown, type BrainPolicy, type EndpointGuide, type ToolArgs,
  VALID_TYPES, type MemoryType, RELATION_TYPES, type RelationType,
} from './types.js';
```

Remove the corresponding type/interface/const definitions from `src/index.ts` (the `export interface Env`, all the `type` blocks listed above, and the `VALID_TYPES`/`RELATION_TYPES` const declarations).

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/types.ts src/index.ts
git commit -m "refactor: extract shared types to src/types.ts"
```

---

### Task 2: Extract `src/constants.ts`

**Files:**
- Create: `src/constants.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/constants.ts` with all configuration constants**

Move from `src/index.ts` lines 11-41 (server config, TTLs, rate limit params, embedding config, vectorize params) into `src/constants.ts`. Also move the `DEFAULT_BRAIN_POLICY` object and other standalone constants that don't depend on runtime.

```typescript
// src/constants.ts
import type { BrainPolicy } from './types.js';

export const SERVER_NAME = 'ai-memory-mcp';
export const SERVER_VERSION = '1.10.0';
export const LEGACY_BRAIN_ID = 'legacy-default-brain';
export const LEGACY_USER_ID = 'legacy-token-user';
export const LEGACY_USER_EMAIL = 'legacy-token@memoryvault.local';
export const ACCESS_TOKEN_TTL_SECONDS = 60 * 60;
export const REFRESH_TOKEN_TTL_SECONDS = 60 * 60 * 24 * 30;
export const AUTH_TOKEN_COOKIE_NAME = 'auth_token';
export const REFRESH_TOKEN_COOKIE_NAME = 'refresh_token';
export const AUTH_TOKEN_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24;
export const AUTH_TOKEN_COOKIE_PATH = '/';
export const REFRESH_TOKEN_COOKIE_PATH = '/';
export const SESSION_COOKIE_SAME_SITE = 'Lax' as const;
export const AUTH_RATE_LIMIT_MAX_ATTEMPTS = 10;
export const AUTH_RATE_LIMIT_WINDOW_SECONDS = 60 * 15;
export const PBKDF2_ITERATIONS = 100_000;
export const EMBEDDING_MODEL = '@cf/baai/bge-base-en-v1.5';
export const VECTORIZE_QUERY_TOP_K_MAX = 20;
export const VECTORIZE_UPSERT_BATCH_SIZE = 500;
export const VECTORIZE_DELETE_BATCH_SIZE = 500;
export const VECTORIZE_SETTLE_POLL_INTERVAL_MS = 3000;
export const VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS = 180;
export const VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS_MAX = 900;
export const EMBEDDING_BATCH_SIZE = 16;
export const MEMORY_SEARCH_FUSION_K = 60;
export const MEMORY_SEARCH_DEFAULT_LIMIT = 20;
export const MEMORY_SEARCH_MAX_LIMIT = 20;
export const VECTOR_ID_PREFIX = 'm:';
export const VECTOR_ID_MAX_MEMORY_ID_LENGTH = 62;
export const DEFAULT_OAUTH_REDIRECT_DOMAIN_ALLOWLIST = ['localhost', '127.0.0.1'] as const;
export const TRUSTED_REDIRECT_DOMAINS = ['poke.com', 'claude.ai'] as const;

export const DEFAULT_BRAIN_POLICY: BrainPolicy = {
  max_inferred_edges: 400,
  default_confidence: 0.7,
  default_importance: 0.5,
  decay_factor: 0.05,
  consolidation_threshold: 0.75,
};
```

Note: Copy the actual `DEFAULT_BRAIN_POLICY` values from the source — the values above are from the design doc exploration. Verify against `src/index.ts` around line 1412-1421.

- [ ] **Step 2: Update `src/index.ts` imports**

Add import at top of `src/index.ts`:
```typescript
import {
  SERVER_NAME, SERVER_VERSION, LEGACY_BRAIN_ID, LEGACY_USER_ID, LEGACY_USER_EMAIL,
  ACCESS_TOKEN_TTL_SECONDS, REFRESH_TOKEN_TTL_SECONDS,
  AUTH_TOKEN_COOKIE_NAME, REFRESH_TOKEN_COOKIE_NAME,
  AUTH_TOKEN_COOKIE_MAX_AGE_SECONDS, AUTH_TOKEN_COOKIE_PATH, REFRESH_TOKEN_COOKIE_PATH,
  SESSION_COOKIE_SAME_SITE, AUTH_RATE_LIMIT_MAX_ATTEMPTS, AUTH_RATE_LIMIT_WINDOW_SECONDS,
  PBKDF2_ITERATIONS, EMBEDDING_MODEL, VECTORIZE_QUERY_TOP_K_MAX,
  VECTORIZE_UPSERT_BATCH_SIZE, VECTORIZE_DELETE_BATCH_SIZE,
  VECTORIZE_SETTLE_POLL_INTERVAL_MS, VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS,
  VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS_MAX, EMBEDDING_BATCH_SIZE,
  MEMORY_SEARCH_FUSION_K, MEMORY_SEARCH_DEFAULT_LIMIT, MEMORY_SEARCH_MAX_LIMIT,
  VECTOR_ID_PREFIX, VECTOR_ID_MAX_MEMORY_ID_LENGTH,
  DEFAULT_OAUTH_REDIRECT_DOMAIN_ALLOWLIST, TRUSTED_REDIRECT_DOMAINS,
  DEFAULT_BRAIN_POLICY,
} from './constants.js';
```

Remove corresponding `const` declarations from `src/index.ts`.

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/constants.ts src/index.ts
git commit -m "refactor: extract configuration constants to src/constants.ts"
```

---

### Task 3: Extract `src/utils.ts`

**Files:**
- Create: `src/utils.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/utils.ts` with pure utility functions**

Move these functions from `src/index.ts`:
- `generateId`, `now`, `jsonResponse`, `withPrimaryDbEnv`
- `normalizeResourcePath`, `protectedResourceMetadataUrl`, `oauthChallengeHeader`
- `mergeHeaders`, `buildCorsJsonHeaders`, `normalizeCorsJsonResponseOptions`
- `parseRequestCookies`, `getRequestCookie`, `serializeCookie`
- `buildSessionCookieHeaders`, `clearSessionCookieHeaders`, `buildRotatedSessionCookieHeaders`
- `parseBearerToken`, `getAccessTokenFromRequest`
- `clampToRange`, `isMemorySearchMode`, `hasSemanticSearchBindings`, `normalizeSemanticScore`
- `truncateForMetadata`, `parseTags`, `isValidType`, `isValidRelationType`
- `canMutateMemories`
- `readJsonBody`, `escapeHtml`
- `toFiniteNumber`, `normalizeSourceKey`, `parseTagSet`, `normalizeRelation`
- `textTokens`, `urlSlug`, `stableJson`
- `sanitizeDisplayName`, `sanitizeBrainName`, `userPayload`
- `isLikelyMcpRootRequest`, `isBrowserDocumentRequest`, `isOAuthAuthorizeNavigation`

Each function should be `export`ed. Import types from `./types.js` and constants from `./constants.js` as needed.

- [ ] **Step 2: Update `src/index.ts` to import from `src/utils.ts`**

Replace removed function definitions with imports from `./utils.js`.

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/utils.ts src/index.ts
git commit -m "refactor: extract utility functions to src/utils.ts"
```

---

### Task 4: Extract `src/crypto.ts`

**Files:**
- Create: `src/crypto.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/crypto.ts` with cryptographic functions**

Move from `src/index.ts`:
- `bytesToBase64Url`, `base64UrlToBytes`
- `hmacSha256`, `sha256DigestBase64Url`
- `derivePasswordHash`, `hashPassword`, `verifyPassword`
- `randomToken`
- `normalizeEmail`, `isValidEmail`, `isStrongEnoughPassword`
- `signAccessToken`, `verifyAccessToken`

Import `PBKDF2_ITERATIONS` from `./constants.js` and `AccessTokenPayload` from `./types.js`. Import `now` from `./utils.js`.

- [ ] **Step 2: Update `src/index.ts` to import from `src/crypto.ts`**

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/crypto.ts src/index.ts
git commit -m "refactor: extract crypto utilities to src/crypto.ts"
```

---

### Task 5: Extract `src/cors.ts`

**Files:**
- Create: `src/cors.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/cors.ts`**

Move from `src/index.ts` (around lines 6402-6484):
- `ALLOWED_ORIGINS` — but **replace the hardcoded personal worker URLs** with a comment and env-driven pattern:

```typescript
// src/cors.ts
import type { CorsJsonResponseOptions } from './types.js';
import { buildCorsJsonHeaders, normalizeCorsJsonResponseOptions } from './utils.js';

// Default allowed origins for CORS.
// Add your own Worker URLs here or configure via environment.
export const ALLOWED_ORIGINS = [
  'https://claude.ai',
  'https://poke.com',
];

export const CORS_HEADERS: Record<string, string> = {
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept',
};

export const HTML_SECURITY_HEADERS: Record<string, string> = {
  'X-Frame-Options': 'DENY',
  'X-Content-Type-Options': 'nosniff',
  'Strict-Transport-Security': 'max-age=31536000; includeSubDomains; preload',
  'Referrer-Policy': 'no-referrer',
  'Permissions-Policy': 'geolocation=(), microphone=(), camera=()',
  'Content-Security-Policy': "default-src 'self'; script-src 'self' cdn.jsdelivr.net fonts.googleapis.com fonts.gstatic.com; style-src 'self' 'unsafe-inline' fonts.googleapis.com; font-src fonts.gstatic.com; connect-src 'self'; frame-ancestors 'none';",
};

export function corsJsonResponse(
  body: unknown,
  status = 200,
  options: CorsJsonResponseOptions | Record<string, string> = {}
): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: buildCorsJsonHeaders(normalizeCorsJsonResponseOptions(options)),
  });
}

export function getCorsOrigin(request: Request): string {
  const origin = request.headers.get('Origin')?.trim();
  if (origin && ALLOWED_ORIGINS.includes(origin)) return origin;
  const requestOrigin = new URL(request.url).origin;
  return ALLOWED_ORIGINS.includes(requestOrigin) ? requestOrigin : ALLOWED_ORIGINS[0];
}

export function mergeVaryHeader(existingValue: string | null, value: string): string {
  const varyValues = new Set(
    (existingValue ?? '').split(',').map((item) => item.trim()).filter(Boolean)
  );
  varyValues.add(value);
  return Array.from(varyValues).join(', ');
}

export function applyCors(request: Request, response: Response): Response {
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

export function isHtmlResponse(response: Response): boolean {
  return (response.headers.get('Content-Type') ?? '').toLowerCase().includes('text/html');
}

export function wrapWithSecurityHeaders(response: Response): Response {
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

export function unauthorized(url?: URL): Response {
  const headers: Record<string, string> = { ...CORS_HEADERS, 'Content-Type': 'application/json' };
  if (url) headers['WWW-Authenticate'] = oauthChallengeHeader(url);
  return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers });
}
```

Note: Also move `oauthChallengeHeader` here since `unauthorized` depends on it — or import from utils if it was placed there.

Also remove the `mcp.figma.com` reference from the CSP `script-src` and `connect-src` since that was personal Figma integration, not needed for the open-source template.

- [ ] **Step 2: Update `src/index.ts` to import from `src/cors.ts`**

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/cors.ts src/index.ts
git commit -m "refactor: extract CORS and security headers to src/cors.ts"
```

---

### Task 6: Extract `src/db.ts`

**Files:**
- Create: `src/db.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/db.ts` with database query functions and schema migration**

Move from `src/index.ts`:
- `runMigrationStatement`, `ensureSchema` (the `schemaReady` singleton pattern)
- `loadMemoryRowsByIds`, `runLexicalMemorySearch`
- `loadLinkStatsMap`, `loadSourceTrustMap`
- `getBrainPolicy`
- `loadActiveMemoryNodes`, `loadExplicitMemoryLinks`
- `ensureObjectiveRoot`
- `logChangelog`
- `listBrainsForUser`, `findActiveBrain`

All exported. Import types/constants as needed.

- [ ] **Step 2: Update `src/index.ts` to import from `src/db.ts`**

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/db.ts src/index.ts
git commit -m "refactor: extract database queries and schema migration to src/db.ts"
```

---

### Task 7: Extract `src/auth.ts`

**Files:**
- Create: `src/auth.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/auth.ts` with session management and auth endpoint handlers**

Move from `src/index.ts`:
- `createSessionTokens`, `getRefreshSessionByToken`, `rotateSession`, `revokeSession`, `revokeSessionById`
- `authenticateRequest`
- `authRateLimitPrefix`, `authRateLimitKey`, `checkRateLimit`, `resetAuthRateLimit`
- `ensureLegacyTokenPrincipal`, `normalizeLegacyToken`
- `handleAuthSignup`, `handleAuthLogin`, `handleAuthRefresh`, `handleAuthLogout`
- `handleAuthMe`, `handleAuthSessions`, `handleAuthSessionRevoke`

Import from `./types.js`, `./constants.js`, `./utils.js`, `./crypto.js`, `./cors.js`, `./db.js`.

- [ ] **Step 2: Update `src/index.ts` to import from `src/auth.ts`**

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/auth.ts src/index.ts
git commit -m "refactor: extract auth and session management to src/auth.ts"
```

---

### Task 8: Extract `src/oauth.ts`

**Files:**
- Create: `src/oauth.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/oauth.ts` with OAuth protocol handlers**

Move from `src/index.ts` (around lines 7280-8081):
- `getOAuthClient`, `purgeOAuthClientIfNotWhitelisted`
- `handleAuthorizationServerMetadata`, `handleProtectedResourceMetadata`
- `handleOAuthRegister`, `handleOAuthAuthorize`, `handleOAuthToken`
- OAuth-related helpers (form body parsing, client secret validation, PKCE verification)

Import from `./types.js`, `./constants.js`, `./utils.js`, `./crypto.js`, `./cors.js`, `./auth.js`.

- [ ] **Step 2: Update `src/index.ts` to import from `src/oauth.ts`**

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/oauth.ts src/index.ts
git commit -m "refactor: extract OAuth protocol handlers to src/oauth.ts"
```

---

### Task 9: Extract `src/vectorize.ts`

**Files:**
- Create: `src/vectorize.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/vectorize.ts` with semantic search and embedding functions**

Move from `src/index.ts`:
- `buildMemoryEmbeddingText`, `extractEmbeddingList`
- `vectorIdForMemory`, `memoryIdFromVectorId`
- `generateEmbeddings`, `batchGenerateEmbeddings`
- `syncVectorIndex` (upsert/delete vectors)
- `pollVectorizeMutations`, `waitForVectorizeSettled`
- `runSemanticMemorySearch`, `deduplicateSemanticCandidates`
- `runHybridMemorySearch` (RRF fusion)

Import from `./types.js`, `./constants.js`, `./utils.js`.

- [ ] **Step 2: Update `src/index.ts` to import from `src/vectorize.ts`**

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/vectorize.ts src/index.ts
git commit -m "refactor: extract semantic search and vectorize to src/vectorize.ts"
```

---

### Task 10: Extract `src/scoring.ts`

**Files:**
- Create: `src/scoring.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/scoring.ts` with dynamic scoring model**

Move from `src/index.ts`:
- `computeDynamicScores`
- `enrichMemoryRowsWithDynamics`
- `projectMemoryForClient`
- Any scoring helper functions (link weight computation, decay calculations, etc.)
- `EMPTY_LINK_STATS` template object

Import from `./types.js`, `./constants.js`, `./utils.js`.

- [ ] **Step 2: Update `src/index.ts` to import from `src/scoring.ts`**

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/scoring.ts src/index.ts
git commit -m "refactor: extract dynamic scoring model to src/scoring.ts"
```

---

### Task 11: Extract `src/tools-schema.ts`

**Files:**
- Create: `src/tools-schema.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/tools-schema.ts` with tool definitions and metadata**

Move from `src/index.ts`:
- `TOOLS` array (all 40+ tool definitions with their inputSchema objects)
- `MUTATING_TOOL_NAMES` set
- `isMutatingTool` function
- `TOOL_RELEASE_META` object
- `TOOL_CHANGELOG` array
- Tool metadata lookup functions

Import from `./types.js`.

- [ ] **Step 2: Update `src/index.ts` to import from `src/tools-schema.ts`**

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/tools-schema.ts src/index.ts
git commit -m "refactor: extract MCP tool definitions to src/tools-schema.ts"
```

---

### Task 12: Extract `src/tools.ts`

**Files:**
- Create: `src/tools.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/tools.ts` with the callTool dispatcher**

Move from `src/index.ts` (lines ~3096-6400):
- The entire `callTool` function with its switch statement and all 40+ tool handler implementations
- Graph helper functions used only within callTool: `buildTagInferredLinks`, etc.

Import from `./types.js`, `./constants.js`, `./utils.js`, `./db.js`, `./vectorize.js`, `./scoring.js`, `./tools-schema.js`.

- [ ] **Step 2: Update `src/index.ts` to import from `src/tools.ts`**

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/tools.ts src/index.ts
git commit -m "refactor: extract MCP tool handlers to src/tools.ts"
```

---

### Task 13: Extract `src/viewer.ts`

**Files:**
- Create: `src/viewer.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/viewer.ts` with the viewer HTML and script**

Move from `src/index.ts` (lines ~8083-12167):
- `viewerHtml()` function
- `viewerScript()` function

These are self-contained string-returning functions. Import `SERVER_NAME`, `SERVER_VERSION` from `./constants.js` if used within the HTML.

- [ ] **Step 2: Update `src/index.ts` to import from `src/viewer.ts`**

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/viewer.ts src/index.ts
git commit -m "refactor: extract web viewer to src/viewer.ts"
```

---

### Task 14: Extract `src/routes.ts`

**Files:**
- Create: `src/routes.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create `src/routes.ts` with HTML pages and API route handlers**

Move from `src/index.ts`:
- `rootLandingHtml()`, `mcpLandingHtml()`, `endpointGuideHtml()`
- `endpointGuideForPath()`
- `handleApiMemories`, `handleApiLinks`, `handleApiGraph`, `handleApiTools`
- `handleMcp`, `processMcpBody`

Import from all other modules as needed.

- [ ] **Step 2: Update `src/index.ts` to import from `src/routes.ts`**

- [ ] **Step 3: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/routes.ts src/index.ts
git commit -m "refactor: extract route handlers and HTML pages to src/routes.ts"
```

---

### Task 15: Slim down `src/index.ts` to entry point

**Files:**
- Modify: `src/index.ts`

- [ ] **Step 1: Reduce `src/index.ts` to a thin fetch handler**

After all extractions, `src/index.ts` should contain only:
- Imports from all modules
- The `export default { async fetch() { ... } }` worker entry point
- The top-level routing logic (OPTIONS handling, ensureSchema call, path matching, CORS/security wrapping)

The file should be ~100-150 lines.

- [ ] **Step 2: Verify type-check passes**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 3: Verify the built output works**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npx wrangler dev --local` and verify the health endpoint responds.

- [ ] **Step 4: Commit**

```bash
git add src/index.ts
git commit -m "refactor: slim index.ts to thin entry point (~100 lines)"
```

---

### Task 16: Scrub hardcoded infrastructure values

**Files:**
- Modify: `wrangler.toml`
- Modify: `src/cors.ts` (done in Task 5, verify)
- Modify: `scripts/smoke_oauth_isolation.sh`

- [ ] **Step 1: Update `wrangler.toml` to replace personal IDs with placeholders**

Replace database_id and KV namespace IDs:

```toml
[[d1_databases]]
binding = "DB"
database_name = "ai-memory"
database_id = "YOUR_D1_DATABASE_ID"

[[kv_namespaces]]
binding = "RATE_LIMIT_KV"
id = "YOUR_KV_NAMESPACE_ID"

[env.dev.d1_databases]
# same pattern for dev
database_id = "YOUR_D1_DATABASE_ID"

[[env.dev.kv_namespaces]]
binding = "RATE_LIMIT_KV"
id = "YOUR_DEV_KV_NAMESPACE_ID"
```

- [ ] **Step 2: Update smoke test default URL**

In `scripts/smoke_oauth_isolation.sh`, change the default `BASE_URL` from the personal production URL to a generic local default:

```bash
BASE_URL="${1:-${BASE_URL:-http://127.0.0.1:8787}}"
```

- [ ] **Step 3: Verify no personal URLs remain in source**

Run: `grep -r "guirguispierre" src/ scripts/ wrangler.toml`
Expected: No matches

- [ ] **Step 4: Commit**

```bash
git add wrangler.toml scripts/smoke_oauth_isolation.sh
git commit -m "chore: scrub hardcoded infrastructure IDs and personal URLs"
```

---

### Task 17: Add MIT License

**Files:**
- Create: `LICENSE`
- Modify: `package.json`

- [ ] **Step 1: Create LICENSE file**

```
MIT License

Copyright (c) 2026 Pierre Guirguis

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

- [ ] **Step 2: Update `package.json` metadata**

Change `license` from `ISC` to `MIT`. Update `author` from `MemoryVault` to `Pierre Guirguis`. Ensure `repository` URL points to the public repo.

- [ ] **Step 3: Commit**

```bash
git add LICENSE package.json
git commit -m "chore: add MIT license and update package.json metadata"
```

---

### Task 18: Rewrite README for self-hosters

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Rewrite README.md**

Replace the entire README with a self-hoster focused version. Key sections:

1. **Title + one-line description** — "MemoryVault MCP — A self-hosted, graph-aware memory server for AI assistants"
2. **Features** — bullet list (graph memory, OAuth, semantic search, web viewer, 40+ tools)
3. **Quick Start** — 5-step: clone → install → configure → init DB → deploy
4. **Configuration** — env vars table (AUTH_SECRET, ADMIN_TOKEN, OAUTH_REDIRECT_DOMAIN_ALLOWLIST), wrangler.toml setup
5. **MCP Integration** — how to connect Claude/other clients (generic URLs using `your-worker.your-subdomain.workers.dev`)
6. **Architecture** — tech stack + module overview table
7. **Available MCP Tools** — grouped tool list
8. **Web Viewer** — description of `/view`
9. **Development** — local dev, type-check, smoke tests
10. **Contributing** — link to CONTRIBUTING.md
11. **License** — MIT

Remove all references to `ai-memory-mcp.guirguispierre.workers.dev`. Replace with `https://<your-worker>.<your-subdomain>.workers.dev`.

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: rewrite README for open-source self-hosters"
```

---

### Task 19: Add CONTRIBUTING.md

**Files:**
- Create: `CONTRIBUTING.md`

- [ ] **Step 1: Create CONTRIBUTING.md**

Contents:
- **Prerequisites** — Node 20+, Wrangler CLI, free Cloudflare account
- **Local Setup** — step-by-step (clone, npm install, cp .dev.vars.example, init D1 local, npm run dev)
- **Project Structure** — module map table (same as spec)
- **Adding a New MCP Tool** — brief guide (add schema to tools-schema.ts, add handler to tools.ts, add to MUTATING_TOOL_NAMES if needed)
- **Code Style** — TypeScript strict mode, no external runtime deps beyond @modelcontextprotocol/sdk and zod, prefer pure functions
- **Pull Requests** — fork, create branch, make changes, `npm run type-check`, open PR
- **Reporting Issues** — link to GitHub issues

- [ ] **Step 2: Commit**

```bash
git add CONTRIBUTING.md
git commit -m "docs: add CONTRIBUTING.md for new contributors"
```

---

### Task 20: Update `.dev.vars.example`

**Files:**
- Modify: `.dev.vars.example`

- [ ] **Step 1: Ensure `.dev.vars.example` covers all required secrets**

The current example has:
```
AUTH_SECRET=replace-with-a-long-random-secret
ADMIN_TOKEN=replace-with-a-long-random-admin-token
OAUTH_REDIRECT_DOMAIN_ALLOWLIST=localhost,127.0.0.1
```

This is already good. Verify it matches the current `Env` interface requirements.

- [ ] **Step 2: Verify `.gitignore` is complete**

Current `.gitignore`:
```
node_modules/
.wrangler/
dist/
.DS_Store
.dev.vars
.env
.env.*
```

This is sufficient — secrets files are excluded.

- [ ] **Step 3: Commit (if changes needed)**

```bash
git add .dev.vars.example .gitignore
git commit -m "chore: verify dev vars example and gitignore for open source"
```

---

### Task 21: Final verification and cleanup

**Files:**
- All files in `src/`

- [ ] **Step 1: Run full type-check**

Run: `cd /Users/guirguispierre/ailog/ai-memory-mcp && npm run type-check`
Expected: No errors

- [ ] **Step 2: Verify no secrets or personal info leaked**

Run:
```bash
grep -rn "guirguispierre" src/ README.md CONTRIBUTING.md wrangler.toml scripts/
grep -rn "f881cdf0" src/ wrangler.toml
grep -rn "45fefc1b" src/ wrangler.toml
grep -rn "9c86e1f7" src/ wrangler.toml
```
Expected: No matches for any of these.

- [ ] **Step 3: Verify file count and structure**

Run: `ls src/`
Expected: ~15 .ts files (index, types, constants, utils, crypto, cors, db, auth, oauth, vectorize, scoring, tools-schema, tools, viewer, routes)

- [ ] **Step 4: Final commit for any cleanup**

```bash
git add -A
git commit -m "chore: final cleanup for open-source release"
```
