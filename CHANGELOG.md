# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Human-friendly root landing page at `/` for browser navigation.
- New **Dev Section** on the root page that lists sub-sites and endpoint surfaces with method/auth summaries.

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
