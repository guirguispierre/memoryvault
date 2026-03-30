# MemoryVault MCP

A self-hosted, graph-aware memory server for AI assistants. Built on Cloudflare Workers + D1.

MemoryVault gives AI clients (Claude, ChatGPT, etc.) persistent memory across sessions via the [Model Context Protocol (MCP)](https://modelcontextprotocol.io). Store notes, facts, and journal entries. Link related memories into a knowledge graph. Search with hybrid lexical + semantic retrieval.

## Features

- **40+ MCP tools** — memory CRUD, graph linking, conflict detection, objectives, snapshots, and more
- **Hybrid search** — lexical + semantic (Vectorize + Workers AI embeddings) with RRF fusion
- **Knowledge graph** — typed relationships, path finding, neighborhood traversal, inferred links
- **Multi-tenant** — user accounts with isolated "brains" and per-brain policies
- **OAuth + PKCE** — standards-based auth for MCP clients, plus legacy bearer token fallback
- **Web viewer** — browse memories, explore the graph, manage settings at `/view`
- **6 themes** — cyberpunk, light, midnight, solarized, ember, arctic
- **Zero external dependencies** at runtime (just @modelcontextprotocol/sdk and zod)

## Quick Start

1. **Clone and install**

```bash
git clone https://github.com/guirguispierre/memoryvault.git
cd memoryvault
npm install
```

2. **Configure secrets**

```bash
cp .dev.vars.example .dev.vars
# Edit .dev.vars with your own secrets
```

3. **Set up Cloudflare resources**

```bash
# Create D1 database
npx wrangler d1 create ai-memory

# Update wrangler.toml with your database_id

# Create KV namespace for rate limiting
npx wrangler kv namespace create RATE_LIMIT_KV
# Update wrangler.toml with the KV namespace id

# Create Vectorize indexes (for semantic search)
npx wrangler vectorize create ai-memory-semantic-v1 --dimensions=768 --metric=cosine
```

4. **Initialize database schema**

```bash
npx wrangler d1 execute ai-memory --local --file=schema.sql
```

5. **Run locally**

```bash
npm run dev
```

## Deploy to Production

```bash
# Set secrets
npx wrangler secret put AUTH_SECRET
npx wrangler secret put ADMIN_TOKEN

# Apply schema to remote D1
npx wrangler d1 execute ai-memory --remote --file=schema.sql

# Deploy
npm run deploy
```

## Configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `AUTH_SECRET` | Yes | Signs JWTs and secures legacy bearer auth |
| `ADMIN_TOKEN` | Yes | Required for `POST /register` (OAuth client registration) |
| `OAUTH_REDIRECT_DOMAIN_ALLOWLIST` | No | Comma-separated hostnames for OAuth redirect URIs. `localhost` and `127.0.0.1` are always allowed |

## MCP Integration

Point your MCP client to:
```
https://<your-worker>.<your-subdomain>.workers.dev/mcp
```

**OAuth mode (recommended):** Leave the API key empty. The server responds with OAuth discovery metadata. Your client handles the flow automatically.

**Legacy bearer mode:** Send `Authorization: Bearer <AUTH_SECRET>` for simple setups.

## Architecture

| Module | Purpose |
|--------|---------|
| `src/index.ts` | Worker entry point and HTTP routing |
| `src/types.ts` | Shared TypeScript types |
| `src/constants.ts` | Configuration constants |
| `src/utils.ts` | Pure utility functions |
| `src/crypto.ts` | PBKDF2, JWT, HMAC utilities |
| `src/cors.ts` | CORS and security headers |
| `src/db.ts` | D1 queries and schema migration |
| `src/auth.ts` | Session management and auth endpoints |
| `src/oauth.ts` | OAuth protocol (authorization, token, registration) |
| `src/vectorize.ts` | Semantic search and Vectorize integration |
| `src/scoring.ts` | Dynamic confidence/importance scoring |
| `src/tools-schema.ts` | MCP tool definitions and metadata |
| `src/tools.ts` | MCP tool handler implementations |
| `src/viewer.ts` | Web viewer UI (`/view`) |
| `src/routes.ts` | API and HTML route handlers |

**Tech stack:** Cloudflare Workers, D1 (SQLite), Vectorize, Workers AI (`@cf/baai/bge-base-en-v1.5`), MCP SDK

## Available MCP Tools

**Memory operations:** `memory_save`, `memory_get`, `memory_get_fact`, `memory_search`, `memory_list`, `memory_update`, `memory_delete`, `memory_reindex`, `memory_stats`

**Graph:** `memory_link`, `memory_unlink`, `memory_links`, `memory_link_suggest`, `memory_path_find`, `memory_subgraph`, `memory_neighbors`, `memory_graph_stats`, `memory_tag_stats`

**Knowledge management:** `memory_consolidate`, `memory_forget`, `memory_activate`, `memory_reinforce`, `memory_decay`, `memory_conflicts`, `memory_conflict_resolve`, `memory_entity_resolve`

**Trust & policy:** `memory_source_trust_set`, `memory_source_trust_get`, `brain_policy_set`, `brain_policy_get`

**Snapshots:** `brain_snapshot_create`, `brain_snapshot_list`, `brain_snapshot_restore`

**Objectives:** `objective_set`, `objective_list`, `objective_next_actions`

**Observability:** `memory_changelog`, `memory_watch`, `memory_explain_score`, `tool_manifest`, `tool_changelog`

## Development

```bash
npm run dev          # Start local worker
npm run type-check   # TypeScript check
npm run deploy       # Deploy to Cloudflare
```

**Smoke test:**
```bash
ADMIN_TOKEN=... npm run smoke:oauth-isolation
```

**Notes:**
- Semantic search requires Workers AI/Vectorize bindings — use `npx wrangler dev --remote` for full functionality
- Local dev uses `--local` D1 by default

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

[MIT](./LICENSE)
