# Open Source Preparation — Design Spec

**Date:** 2026-03-29
**Status:** Approved

## Goal

Prepare MemoryVault MCP for open source release as a self-hosted template. People clone it, deploy their own instance to Cloudflare, and own their data.

## Scope

1. Split `src/index.ts` (13,209 lines) into modules
2. Scrub hardcoded infrastructure values
3. Add MIT license, update metadata
4. Rewrite README for self-hosters
5. Add CONTRIBUTING.md
6. Make repo public-ready

## 1. Module Split

Split `src/index.ts` into these modules under `src/`:

| Module | Contents | Approx Lines |
|--------|----------|-------------|
| `types.ts` | Env, AuthContext, SessionTokens, row types, scoring types, tool metadata types, graph types | ~200 |
| `constants.ts` | Server config, TTLs, rate limit params, embedding config, vectorize params, default policies, valid types/relations | ~150 |
| `utils.ts` | generateId, now, jsonResponse, cookie helpers, tag parsing, text tokenization, slug generation, number helpers | ~200 |
| `crypto.ts` | base64url, HMAC, PBKDF2 password hashing, JWT sign/verify | ~150 |
| `cors.ts` | CORS_HEADERS, allowed origins, applyCors, getCorsOrigin, corsJsonResponse, security headers, mergeVaryHeader | ~100 |
| `db.ts` | ensureSchema (runtime migrations), loadMemoryRowsByIds, runLexicalMemorySearch, loadLinkStatsMap, ensureObjectiveRoot | ~300 |
| `auth.ts` | createSessionTokens, rotateSession, revokeSession, authenticateRequest, checkRateLimit, legacy token support, signup/login/refresh/logout/me/sessions handlers | ~600 |
| `oauth.ts` | OAuth client management, authorization server metadata, dynamic client registration, protected resource metadata, authorize/token endpoints | ~800 |
| `vectorize.ts` | Embedding generation, vector ID helpers, vector sync (upsert/delete), mutation polling, semantic search, reindex logic | ~400 |
| `scoring.ts` | Dynamic confidence/importance model, score components, explainable scoring, source trust weighting | ~200 |
| `tools-schema.ts` | TOOLS array (40+ tool definitions with zod/JSON schemas), MUTATING_TOOL_NAMES set, tool metadata lookups, release metadata, changelog data | ~600 |
| `tools.ts` | callTool dispatcher + all 40+ tool handler implementations (memory CRUD, graph ops, conflicts, objectives, snapshots, watches) | ~3500 |
| `viewer.ts` | viewerHtml() and viewerScript() — the `/view` web UI | ~4000 |
| `routes.ts` | Root landing page HTML, MCP landing page, HTTP route dispatcher, API handlers (/api/memories, /api/graph, /api/links) | ~500 |
| `index.ts` | Worker fetch entry point — imports and wires everything, handles OPTIONS/preflight | ~100 |

**Approach:** Extract modules bottom-up starting with leaf dependencies (types, constants, utils, crypto) then work up to auth, oauth, tools, viewer, routes. Each module exports named functions/constants. `index.ts` becomes a thin orchestrator.

**Import convention:** Relative imports (`./types`, `./auth`). No barrel file beyond index.ts.

## 2. Infrastructure Scrubbing

Remove or parameterize all values specific to the author's deployment:

| Item | Location | Action |
|------|----------|--------|
| D1 database_id `f881cdf0-...` | `wrangler.toml` | Replace with placeholder `YOUR_D1_DATABASE_ID` |
| KV namespace IDs | `wrangler.toml` | Replace with `YOUR_KV_NAMESPACE_ID` |
| Worker name `ai-memory-mcp` | `wrangler.toml` | Keep as default, note it's configurable |
| Production URL `ai-memory-mcp.guirguispierre.workers.dev` | README, smoke script | Replace with generic `your-worker.your-subdomain.workers.dev` |
| `TRUSTED_REDIRECT_DOMAINS` (`poke.com`, `claude.ai`) | `src/index.ts` constants | Keep — these are MCP ecosystem domains, not personal |
| CORS allowed origins with `guirguispierre` | CORS constants | Replace hardcoded origins with env-driven or generic pattern |
| Vectorize index names (`ai-memory-semantic-v1`) | `wrangler.toml` | Keep as sensible defaults |

## 3. License & Metadata

- Add `LICENSE` file (MIT, copyright 2026 Pierre Guirguis)
- Update `package.json`:
  - `author`: your name
  - `license`: `MIT`
  - `repository`: keep pointing to `guirguispierre/ai-memory-mcp`
- Remove `.DS_Store` from repo (add to `.gitignore` if not already — it is)

## 4. README Rewrite

Structure for self-hosters:

1. **What is this** — one-paragraph pitch
2. **Features** — bullet list of capabilities
3. **Quick Start** — clone, install, configure, deploy (5-step)
4. **Configuration** — env vars, secrets, wrangler.toml
5. **MCP Integration** — how to connect Claude/other clients
6. **Architecture** — module overview, tech stack
7. **Available Tools** — tool list (existing content, cleaned up)
8. **Web Viewer** — screenshot placeholder, description
9. **Development** — local dev, type-check, smoke tests
10. **Contributing** — link to CONTRIBUTING.md
11. **License** — MIT

Remove all references to the author's production instance. Frame everything as "your instance."

## 5. CONTRIBUTING.md

Short and practical:
- Prerequisites (Node 20+, Wrangler CLI, Cloudflare account)
- Local setup steps
- Code structure overview (module map)
- How to add a new MCP tool
- PR process (fork, branch, PR)
- Code style (TypeScript strict, no external runtime deps beyond MCP SDK)

## 6. Final Checklist Before Public

- [ ] No secrets in tracked files (`.dev.vars` is gitignored)
- [ ] No hardcoded database/KV IDs in committed code
- [ ] No personal URLs in README or source
- [ ] CORS origins are generic or env-driven
- [ ] LICENSE file present
- [ ] README works for a first-time self-hoster
- [ ] `npm install && npm run type-check` passes
- [ ] `.gitignore` covers node_modules, .wrangler, dist, .dev.vars, .env*

## Out of Scope

- Unit tests (future contribution)
- Docker/local alternative to Cloudflare
- Architecture diagrams
- GitHub issue/PR templates (can add later)
- npm publishing
