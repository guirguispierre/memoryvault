# Tests

## Running everything

```bash
npm test                    # type-check + unit suites; no worker or credentials needed
npm run test:all            # the above, then the live isolation suite
```

`test:all` requires a booted worker first: `npm run db:isolation` once, then
`npm run dev:isolation`, then run `test:all` (or
`BASE_URL=http://127.0.0.1:8787 npm run test:isolation` on its own) in another
terminal.

## Always use the npm scripts for the unit suites

The unit suites import the TypeScript sources directly. The npm scripts
register `tests/ts-loader.mjs` (rewriting the sources' `./x.js` specifiers to
`./x.ts`) and rely on Node's native type stripping, which needs **Node
22.18+**. Bare `node --test tests/crypto.test.mjs` skips the loader and fails
with `ERR_MODULE_NOT_FOUND` / `ERR_UNKNOWN_FILE_EXTENSION`; the suites print a
pointer to the right command when that happens.

## Crypto unit tests (`crypto.test.mjs`)

Exercises `src/crypto.ts` directly (password hashing round-trips, JWT
signature tampering, `alg: none` rejection, expiry, missing claims):

```bash
npm run test:crypto
```

No build step: `register-ts-loader.mjs` rewrites the sources' `./x.js`
specifiers to `./x.ts` and Node's native type stripping does the rest
(requires Node 22.18+).

## Vectorize unit tests (`vectorize.test.mjs`)

Pure-function tests for the semantic-search isolation layers in
`src/vectorize.ts`:

- vector ids are prefixed, 64-byte bounded, and brain-salted in the digest
  fallback;
- `buildSemanticQueryOptions` always namespaces the Vectorize query to the
  caller's `brainId`;
- `filterSemanticMatches` drops any match whose metadata names a different
  brain before the (brain-scoped) D1 re-fetch sees its id.

```bash
npm run test:vectorize
```

## Semantic-path integration test (`vectorize.integration.mjs`)

The local isolation suite proves the **lexical** path only, because the test
worker has no AI/Vectorize bindings. This script proves the **semantic**
path live: Bob saves a memory, waits until his own semantic search returns
it, then Alice runs the same semantic and hybrid queries and must get
nothing of Bob's back.

```bash
VECTORIZE_TEST_URL=https://<worker-with-bindings>.workers.dev npm run test:vectorize:integration
```

Without `VECTORIZE_TEST_URL` it prints a skip message and exits 0, so
credential-less local/CI runs never fail. It creates two throwaway accounts
on the target worker (same pattern as `scripts/smoke_oauth_isolation.sh`).

## Tenant-isolation suite (`isolation.mjs`)

Black-box HTTP suite against a real worker — real auth path, real D1, no
mocks. Every case has one shape: Alice (attacker) aims an operation at Bob's
(victim's) data, then we verify nothing changed on Bob's side and nothing
leaked to Alice's side.

Run locally:

```bash
npm run db:isolation        # apply schema.sql to the local test D1 (once)
npm run dev:isolation       # boot the worker (tests/wrangler.isolation.toml)
# in another terminal:
BASE_URL=http://127.0.0.1:8787 npm run test:isolation
```

`tests/wrangler.isolation.toml` deliberately omits the AI / Vectorize
bindings so the worker runs fully local; search degrades to lexical, which
the suite does not depend on. CI runs this on every push via
`.github/workflows/isolation.yml`.

## Policy

**Any new tool or endpoint that touches memories must add a cross-tenant
case to `isolation.mjs` in the same PR. A missing case is a release
blocker.**
