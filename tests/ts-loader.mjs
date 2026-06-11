// Source files use ESM-style `./x.js` specifiers (resolved by the Workers
// bundler), but on disk the files are `.ts`. This hook lets `node --test`
// import the real source modules without a build step or new dependencies;
// Node's native type stripping handles the rest.
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

export async function resolve(specifier, context, nextResolve) {
  if (specifier.startsWith('.') && specifier.endsWith('.js') && context.parentURL) {
    const candidate = `${specifier.slice(0, -3)}.ts`;
    const url = new URL(candidate, context.parentURL);
    if (url.protocol === 'file:' && existsSync(fileURLToPath(url))) {
      return nextResolve(candidate, context);
    }
  }
  return nextResolve(specifier, context);
}
