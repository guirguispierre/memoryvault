# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.11.0] - 2026-03-30

### Added
- `memory_merge` MCP tool for merging two or more overlapping memories into a single richer memory, combining content, tags, and graph links while archiving duplicates with supersedes relationships.
- `memory_temporal_cluster` MCP tool for retrieving memories grouped by time windows (hour/day/week) with surrounding graph context — episodic memory recall by time period.
- `memory_spaced_repetition` MCP tool for surfacing important but fading memories due for review, scored by urgency based on importance, confidence gap, staleness, and graph isolation.

## [Unreleased]

### Added
- Viewer settings panel now supports folder-style expandable sections (`General & Search`, `Graph Defaults`, `Appearance & Session`, `Notifications`, `Semantic Index`) to reduce visual overload.
- Viewer settings now include a scrollable settings body so long configurations remain usable on smaller viewports while action buttons stay accessible.
- `memory_reindex` now supports `wait_for_index` (default `true`) and `wait_timeout_seconds` (default `180`) so callers can block until semantic mutations are queryable.
- `memory_reindex` response now includes semantic indexing readiness fields (`index_ready`, `mutation_count`, wait timing, and processed mutation markers).
- `/view` settings include Semantic Index Sync controls (limit, wait toggle, timeout, run button, and readiness status panel).
- Command palette includes a `Reindex semantic memory` action for in-app semantic maintenance.
- Browser auth responses now issue and rotate httpOnly `auth_token` / `refresh_token` cookies for sign-up, login, refresh, and logout flows.
- OAuth redirect handling now includes trusted self-registration domains for hosted MCP clients (`poke.com`, `claude.ai`) alongside explicit redirect-domain configuration.
- CORS allowlisting now explicitly includes `https://poke.com` plus the dev and prod Worker origins used by embedded clients.
- `/view.js` now serves the viewer script from a same-origin asset path so the existing CSP can allow it without `unsafe-inline`.
- Cloudflare Worker auth now binds a dedicated `RATE_LIMIT_KV` namespace in both prod and dev for deployed brute-force protection.

### Changed
- Settings modal layout was refactored from a single long flat list into grouped sections using native expandable containers, while preserving all existing setting field IDs and persistence behavior.
- Settings modal now uses a constrained-height container with internal scrolling (`max-height` + `overflow-y`) for better desktop and mobile usability.
- Mobile settings presentation was tuned to keep section content readable and maintain full-width action buttons.
- Semantic reindex workflow now surfaces readiness signals directly in the viewer status card, including mutation counters and completion state.
- Semantic indexing mutation identifiers now use a parseable/stable format to improve cross-operation consistency between write paths and retrieval.
- OAuth dynamic client registration now requires `ADMIN_TOKEN` unless every `redirect_uri` is on a trusted hosted-client domain, and stale non-whitelisted clients are purged during registration attempts.
- OAuth metadata and authorization validation now enforce `S256` PKCE only, and the smoke test flow now covers the stricter registration/auth requirements.
- Login and signup requests now use a Cloudflare KV-backed rate limiter with 15-minute TTL windows, while authenticated browser/API requests can use cookie-backed auth tokens in addition to bearer headers.
- `/view` UI actions are now bound with `addEventListener` and `data-action` hooks instead of inline `onclick`/`oninput` handlers.

### Fixed
- Semantic retrieval failures caused by non-parseable vector record identifiers were resolved by switching to a parse-safe vector ID strategy.
- Newly created memories now reliably participate in semantic retrieval flows after indexing operations.
- Hybrid retrieval reliability improved by ensuring reindex completion can wait for Vectorize readiness before returning control to callers.
- Mutating MCP tools (including `memory_link`/`memory_unlink`) are now correctly available to authenticated user sessions; write access is no longer incorrectly gated on OAuth `client_id` presence.
- Trusted Poke/Claude redirect URIs can now complete MCP dynamic client registration without manual preregistration, while non-trusted domains still require admin approval.
- CORS responses now reflect only allowlisted origins on both preflight and actual responses instead of falling back to `*`.
- `/view` buttons and interactive controls now work under the strict Content-Security-Policy without relaxing `script-src`.
- Concurrent auth attempts now rate-limit consistently by recording one expiring KV key per request instead of racing on a shared counter key.

## [1.9.0] - 2026-03-04

### Added
- Hybrid semantic retrieval for `memory_search` with `mode` (`lexical`, `semantic`, `hybrid`), `limit`, and `min_score`.
- New `memory_reindex` MCP tool for semantic backfill/repair of existing memories.
- Cloudflare Workers AI + Vectorize bindings in Wrangler config (`AI`, `MEMORY_INDEX`).

### Changed
- Memory write/archival paths now sync vector state (save, update, delete, consolidate, forget, objective upserts, entity archive, snapshot restore).
- Server and package versions bumped to `1.9.0`.

## [1.8.1] - 2026-03-04

### Added
- Human-friendly root landing page at `/` for browser navigation.
- New **Dev Section** on the root page that lists sub-sites and endpoint surfaces with method/auth summaries.
- Viewer command palette (`Ctrl/Cmd+K`) for quick actions (refresh, graph, toggles, focus, logout).
- Viewer keyboard shortcuts overlay (`?`) with inline key reference.
- Viewer toast notifications for auth, graph, and sync actions.
- Viewer settings panel for user preferences (live polling, time mode, compact cards, graph defaults, auto-open graph, toast toggle), persisted in local storage.
- Expanded viewer settings with startup filter, search debounce, toast duration, logout confirmation, graph hover-focus toggle, scanline toggle, and reduced-motion mode.
- Viewer settings now show the running app version and include an in-app changelog viewer backed by `tool_changelog`.

### Changed
- MCP auth now distinguishes read-only human sessions from writable AI-agent sessions using OAuth `client_id` context.
- `tools/list` now hides mutating tools for read-only sessions.

### Fixed
- Human email/password sessions can no longer modify memories via mutating MCP tools; writes are restricted to OAuth agent or legacy-token sessions.

## [1.8.0] - 2026-03-04

### Added
- `memory_tag_stats` MCP tool for tag frequency and co-occurrence analytics.
- `memory_graph_stats` MCP tool for graph topology metrics (density, components, relation counts, hubs, and top tags).
- `memory_neighbors` MCP tool for seeded neighborhood traversal by memory `id` or `query`.
- Graph toolbar physics toggle (`PHYSICS ON/OFF`) in the web viewer.
- Browser-friendly endpoint guides for `/mcp` and machine endpoints (`/auth/*`, `/api/*`, `/register`, `/token`, and `/.well-known/*`) when opened via normal browser navigation.
- Human-readable `/mcp` landing page explaining MCP purpose, connection steps, and key discovery URLs.

### Changed
- Web graph exploration now supports hover neighborhood focus to spotlight a node and its immediate connections.
- Graph node/link opacity behavior was refined for smoother search/focus transitions.
- Server/tool version metadata bumped to `1.8.0`.

### Fixed
- OAuth `/authorize` requests that include authorization flow parameters now correctly render the login/sign-up authorization screen instead of the generic endpoint guide page.

## [1.7.1] - 2026-02-27

### Added
- OAuth authorize page now supports `Use Legacy Token` for pre-account users.
- Legacy token auth now provisions a legacy principal for OAuth session issuance.

### Changed
- Authorization form validation is mode-aware (email/password required only for sign-in/sign-up flows).
- Server version bumped to `1.7.1`.

## [1.7.0] - 2026-02-24

### Added
- `memory_link_suggest` for scored link recommendations.
- `memory_path_find` for path tracing between memory nodes.
- `memory_conflict_resolve` for contradiction lifecycle resolution.
- `memory_entity_resolve` for canonical alias mapping.
- `memory_source_trust_set` and `memory_source_trust_get` for source reliability control.
- `brain_policy_set` and `brain_policy_get` for per-brain behavior defaults.
- `brain_snapshot_create`, `brain_snapshot_list`, and `brain_snapshot_restore` for snapshot lifecycle.
- `objective_next_actions` for objective-to-action prioritization.
- `memory_subgraph` for focused graph extraction.
- `memory_watch` for watch subscriptions with optional webhook delivery.

### Changed
- Dynamic scoring now supports source trust weighting.
- Graph inference cap now follows brain policy (`max_inferred_edges`).
- API/tooling version bumped to `1.7.0`.

## [1.6.0] - 2026-02-24

### Added
- `tool_manifest` MCP tool for canonical tool discovery, schema hashes, and release metadata.
- `tool_changelog` MCP tool for versioned tool/auth/scoring change tracking.
- `memory_explain_score` MCP tool for dynamic confidence/importance breakdowns.

### Changed
- Dynamic scoring now uses a shared explainable model (`memoryvault-dynamic-v1`) behind tool and API responses.
- Smoke coverage now asserts presence of the new MCP tools.

## [1.5.0] - 2026-02-24

### Added
- Multi-tenant auth model with user accounts and per-brain isolation.
- OAuth-first MCP integration (`/.well-known` metadata, `/register`, `/authorize`, `/token`) for keyless setup.
- Session governance endpoints: `GET /auth/sessions` and `POST /auth/sessions/revoke`.
- CI type-check workflow and OAuth/isolation smoke workflow.
- Web viewer and graph memory tooling updates (links, conflicts, objectives, graph APIs).

### Changed
- Release branding and documentation polish for MemoryVault.
- OAuth/session security and verification flows hardened with smoke tests.

## [1.0.0] - 2026-02-23

### Added
- Initial Cloudflare Workers + D1 MemoryVault MCP server scaffold.
- Core memory operations and MCP transport baseline.
