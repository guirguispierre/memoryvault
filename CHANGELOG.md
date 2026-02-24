# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
