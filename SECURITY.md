# Security

This document describes what MemoryVault defends against, how, and — just as
importantly — what it does not. If you find a gap between this document and
the code, that gap is itself a bug: please report it.

## Threat model

MemoryVault is a multi-tenant memory store. The primary asset is the content
of each tenant's brain; the primary adversary is an authenticated user of the
same deployment trying to read or modify another tenant's data.

**In scope (defended against):**

- **Cross-tenant access / IDOR.** Memory, link, changelog, snapshot, trust,
  alias, conflict, and watch IDs are not capabilities. Knowing another
  brain's ID gets you nothing: every query is scoped to the brain resolved
  from your verified session.
- **Token forgery.** Access tokens are HS256 JWTs signed with `AUTH_SECRET`.
  Verification pins the algorithm (`alg: HS256`, `typ: JWT` — anything else,
  including `alg: none`, is rejected before any signature work) and compares
  signatures in constant time.
- **SQL injection.** All SQL uses `?` placeholders with bound parameters.
  The only string interpolation in SQL is generated placeholder lists for
  `IN (...)` clauses.
- **Timing attacks on auth.** JWT signature checks and password hash
  comparisons XOR-accumulate over all bytes instead of returning at the
  first mismatch.
- **Credential theft from a database dump.** Passwords are stored as
  PBKDF2-HMAC-SHA256 with 600,000 iterations (OWASP guidance) and a random
  per-user salt. Hashes created before the iteration bump verify at their
  embedded count until the next password change.
- **OAuth redirect and code-interception attacks.** OAuth 2.1 with PKCE
  (S256 only; `plain` is rejected), redirect-URI domain allowlisting, and
  admin-gated client registration.
- **Brute-force login.** Per-IP rate limiting on the auth endpoints
  (10 attempts per 15 minutes).

**Out of scope (not defended against — own these yourself):**

- **Encryption at rest beyond Cloudflare's.** Memory content is stored in
  plaintext in D1. Cloudflare encrypts storage at rest, but anyone with
  access to your Cloudflare account or D1 database reads everything.
  There is no per-tenant or end-to-end encryption.
- **Your infrastructure.** Self-hosters own the security of their Cloudflare
  account, `AUTH_SECRET` / `ADMIN_TOKEN` handling, and any clients they
  authorize. A leaked `AUTH_SECRET` is a full compromise: it signs every
  token and doubles as the legacy bearer credential.
- **Malicious memory content.** MemoryVault stores what clients send. It
  escapes content for its own viewer UI, but it does not sanitize content
  for downstream consumers (e.g. an LLM that retrieves memories can be
  prompt-injected by whatever was stored).
- **Availability.** No DoS protection beyond what Cloudflare provides.
- **Vector index residue.** Deleting a memory schedules deletion of its
  vectors, but Vectorize mutations are asynchronous; embeddings may persist
  briefly after deletion.

## How tenant isolation works

1. A request authenticates via OAuth access token, session cookie, or the
   legacy bearer token.
2. The token's session is resolved to a `brain_id` through the
   `brain_memberships` join — the brain is derived from the verified
   identity, never from request input.
3. Every D1 query that touches `memories`, `memory_links`,
   `memory_changelog`, `brain_source_trust`, `brain_snapshots`,
   `memory_conflict_resolutions`, `memory_entity_aliases`, or
   `memory_watches` carries a `WHERE brain_id = ?` predicate bound to that
   resolved value.
4. Semantic search is namespaced per brain in Vectorize, so vector hits
   cannot cross brains either.

The enforcement mechanism is the black-box isolation suite
(`tests/isolation.mjs`, run in CI by `.github/workflows/isolation.yml`
against a real worker and real D1 — no mocks). Every memory-touching tool
and REST endpoint has a cross-tenant attack case; adding a tool without one
is a release blocker. See [tests/README.md](./tests/README.md).

## Authentication summary

- **Passwords:** PBKDF2-HMAC-SHA256, 600,000 iterations, 16-byte random
  salt, constant-time verification. Minimum length 10 characters.
- **Access tokens:** HS256 JWTs, 1-hour TTL, pinned algorithm,
  constant-time signature check. Sessions are revocable; refresh tokens
  rotate and live server-side.
- **MCP clients:** OAuth 2.1 authorization-code flow with PKCE (S256 only).
  Client registration requires `ADMIN_TOKEN`.
- **Legacy fallback:** `Authorization: Bearer <AUTH_SECRET>` maps to a single
  legacy brain. It is a deliberate convenience for single-user deployments;
  multi-tenant deployments should treat `AUTH_SECRET` like a root credential
  because that is what it is.

## Reporting a vulnerability

Email **p_g1234@hotmail.com** with a description and reproduction steps, or
use [GitHub private vulnerability reporting](https://github.com/guirguispierre/memoryvault/security/advisories/new)
if you prefer. Please do not open a public issue for security reports.

You can expect an acknowledgement within 72 hours and a fix or a concrete
remediation plan within 30 days for confirmed issues. Cross-tenant isolation
failures are treated as the highest severity and prioritized ahead of
everything else.
