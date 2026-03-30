# Contributing to MemoryVault MCP

Thanks for your interest in contributing!

## Prerequisites

- Node.js 20+
- [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/) (`npm install -g wrangler`)
- A free [Cloudflare account](https://dash.cloudflare.com/sign-up)

## Local Setup

1. Fork and clone the repository
2. Install dependencies: `npm install`
3. Copy secrets template: `cp .dev.vars.example .dev.vars`
4. Initialize local D1: `npx wrangler d1 execute ai-memory --local --file=schema.sql`
5. Start dev server: `npm run dev`

## Project Structure

| Module | Purpose |
|--------|---------|
| `src/index.ts` | Worker entry point and HTTP routing (~200 lines) |
| `src/types.ts` | Shared TypeScript types |
| `src/constants.ts` | Configuration constants |
| `src/utils.ts` | Pure utility functions |
| `src/crypto.ts` | PBKDF2, JWT, HMAC utilities |
| `src/cors.ts` | CORS and security headers |
| `src/db.ts` | D1 queries and schema migration |
| `src/auth.ts` | Session management and auth endpoints |
| `src/oauth.ts` | OAuth protocol handlers |
| `src/vectorize.ts` | Semantic search and Vectorize |
| `src/scoring.ts` | Dynamic confidence/importance scoring |
| `src/tools-schema.ts` | MCP tool definitions |
| `src/tools.ts` | MCP tool handler implementations |
| `src/viewer.ts` | Web viewer UI |
| `src/routes.ts` | API and HTML route handlers |

## Adding a New MCP Tool

1. Add the tool definition to the `TOOLS` array in `src/tools-schema.ts`
2. If the tool mutates data, add its name to `MUTATING_TOOL_NAMES` in the same file
3. Add the handler in the `callTool` switch in `src/tools.ts`
4. Run `npm run type-check` to verify

## Code Style

- TypeScript strict mode
- No external runtime dependencies beyond `@modelcontextprotocol/sdk` and `zod`
- ESM imports with `.js` extensions
- Prefer pure functions

## Pull Requests

1. Fork the repo and create a feature branch
2. Make your changes
3. Run `npm run type-check`
4. Open a PR with a clear description of what changed and why

## Reporting Issues

Open an issue at [github.com/guirguispierre/memoryvault/issues](https://github.com/guirguispierre/memoryvault/issues).
