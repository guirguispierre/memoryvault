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
