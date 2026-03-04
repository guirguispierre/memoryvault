# MemoryVault MCP (Cloudflare Workers + D1)

MemoryVault is a secure, graph-aware MCP server for long-term AI memory.

It supports:
- Multi-tenant user accounts with isolated "brains"
- OAuth-first MCP auth (works with clients that leave API key empty)
- Legacy bearer-token auth fallback
- Hybrid lexical + semantic memory retrieval (D1 + Vectorize + Workers AI embeddings)
- Memory graph tools (links, activation, reinforcement, decay, conflicts, objectives)
- Web viewer at `/view`

Production URL:
- `https://ai-memory-mcp.guirguispierre.workers.dev`

Changelog:
- [CHANGELOG.md](./CHANGELOG.md)

## Architecture

- Runtime: Cloudflare Workers
- Database: Cloudflare D1 (SQLite)
- Semantic index: Cloudflare Vectorize (`MEMORY_INDEX`)
- Embeddings: Cloudflare Workers AI (`@cf/baai/bge-base-en-v1.5`)
- Protocol: Model Context Protocol (MCP) over HTTP/SSE
- Auth:
  - OAuth Authorization Code + PKCE (`/authorize`, `/token`, `/register`)
  - JWT access + refresh tokens for user sessions
  - Optional legacy `AUTH_SECRET` bearer mode

## Quick Start (Local)

1. Install dependencies

```bash
npm install
```

2. Configure local secret

```bash
cp .dev.vars.example .dev.vars
```

3. Initialize local D1 schema

```bash
npx wrangler d1 execute ai-memory --local --file=schema.sql
```

4. Run locally

```bash
npm run dev
```

Notes:
- Semantic search + reindex require Workers AI/Vectorize bindings and remote dev (`wrangler dev --remote`).

## Deploy

1. Ensure Wrangler is authenticated

```bash
npx wrangler whoami
```

2. Set production secret (if not set)

```bash
npx wrangler secret put AUTH_SECRET
```

3. Apply schema to remote D1 when needed

```bash
npx wrangler d1 execute ai-memory --remote --file=schema.sql
```

4. Deploy

```bash
npm run deploy
```

Before first semantic deploy, create Vectorize indexes:

```bash
npx wrangler vectorize create ai-memory-semantic-v1 --dimensions=768 --metric=cosine
npx wrangler vectorize create ai-memory-semantic-v1-dev --dimensions=768 --metric=cosine
```

## MCP Integration

Use this server URL:
- `https://ai-memory-mcp.guirguispierre.workers.dev/mcp`

### OAuth mode (recommended)

- Leave API key empty in the MCP client.
- The server responds with OAuth challenge metadata.
- Client should discover:
  - `/.well-known/oauth-protected-resource`
  - `/.well-known/oauth-authorization-server`
- User can sign in, sign up, or use a legacy API token from the authorize screen.
- Legacy token mode maps to the legacy shared brain and upgrades the connection into OAuth session tokens.

### Legacy bearer mode

- Send `Authorization: Bearer <AUTH_SECRET>`.
- This maps to a shared legacy brain (`legacy-default-brain`).

## Key Endpoints

- `GET /` health + version + tool count
- `POST /mcp` MCP methods (`initialize`, `tools/list`, `tools/call`)
- `GET /view` web UI
- `GET /api/memories` memory list/search
- `GET /api/graph` graph nodes + edges + inferred edges
- `GET /api/links/:id` links for a memory
- `POST /auth/signup|login|refresh|logout`
- `GET /auth/me`
- `GET /auth/sessions`
- `POST /auth/sessions/revoke`
- `POST /register` OAuth dynamic client registration
- `GET|POST /authorize` OAuth authorization endpoint
- `POST /token` OAuth token endpoint

## Available MCP Tools

- `memory_save`, `memory_get`, `memory_get_fact`, `memory_search`, `memory_list`
- `memory_reindex` (semantic backfill/repair)
- `memory_update`, `memory_delete`, `memory_stats`
- `memory_link`, `memory_unlink`, `memory_links`
- `memory_consolidate`, `memory_forget`
- `memory_activate`, `memory_reinforce`, `memory_decay`
- `memory_changelog`, `memory_conflicts`
- `memory_link_suggest`, `memory_path_find`, `memory_subgraph`
- `memory_conflict_resolve`, `memory_entity_resolve`
- `memory_source_trust_set`, `memory_source_trust_get`
- `objective_set`, `objective_list`, `objective_next_actions`
- `brain_policy_set`, `brain_policy_get`
- `brain_snapshot_create`, `brain_snapshot_list`, `brain_snapshot_restore`
- `memory_watch`
- `tool_manifest`, `tool_changelog`, `memory_explain_score`

## Scripts

- `npm run dev` start local worker
- `npm run type-check` TypeScript check
- `npm test` alias for type-check
- `npm run deploy` deploy to Cloudflare
- `npm run smoke:oauth-isolation` run OAuth + tenant isolation + session revocation smoke test

Smoke test examples:

```bash
# Against production (default)
npm run smoke:oauth-isolation

# Against local/dev target
BASE_URL=http://127.0.0.1:8787 npm run smoke:oauth-isolation
```

Notes:
- The smoke test creates temporary users/memories in the target environment.
- GitHub Actions includes:
  - `CI` (automatic type-check on push/PR)
  - `Smoke OAuth Isolation` (manual workflow_dispatch against a selected base URL)

## Security Notes

- Per-brain row-level scoping is enforced across memories, links, and changelog.
- Passwords are stored as PBKDF2-SHA256 hashes.
- Refresh tokens are stored hashed.
- Rate limiting is enabled for auth-protected entry points.

## License

ISC
