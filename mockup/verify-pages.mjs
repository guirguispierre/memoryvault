// Visual verification for the server-rendered pages now on the vanilla design
// system: the landing page, the /mcp guide, an endpoint guide, and the OAuth
// authorize screen. Boots the worker locally on the isolation wrangler config
// and screenshots each at device_scale_factor=2, waiting for webfonts.
//
// Run from the repo root:  node mockup/verify-pages.mjs
import { spawn, execSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { chromium } from 'playwright';

const PORT = 8798;
const BASE = `http://127.0.0.1:${PORT}`;
const STATE = '.wrangler/verify-pages-state';
const SHOTS = 'mockup/shots';
const FONT_SETTLE_MS = 2800;

function log(msg) {
  process.stdout.write(`[verify-pages] ${msg}\n`);
}

function initDatabase() {
  log('initializing scratch D1 state');
  execSync(
    `npx wrangler d1 execute ai-memory-test --local --file=schema.sql --config tests/wrangler.isolation.toml --persist-to ${STATE}`,
    { stdio: 'pipe' }
  );
}

function startWorker() {
  log(`starting worker on :${PORT}`);
  const child = spawn(
    'npx',
    ['wrangler', 'dev', '--config', 'tests/wrangler.isolation.toml', '--port', String(PORT), '--persist-to', STATE],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );
  child.stdout.on('data', () => {});
  child.stderr.on('data', () => {});
  return child;
}

async function waitForWorker() {
  for (let i = 0; i < 60; i++) {
    try {
      const r = await fetch(`${BASE}/`);
      if (r.ok) return;
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  throw new Error('worker did not become ready');
}

// Register a client with a localhost redirect (no admin token needed) so the
// authorize screen renders its clean form rather than an error.
async function buildAuthorizeUrl() {
  const redirectUri = `${BASE}/callback`;
  const r = await fetch(`${BASE}/register`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      // Admin token from tests/wrangler.isolation.toml so registration is
      // accepted without relying on redirect-domain trust rules.
      Authorization: 'Bearer isolation-test-admin-token',
    },
    body: JSON.stringify({ redirect_uris: [redirectUri], client_name: 'Verify Pages Client' }),
  });
  if (!r.ok) throw new Error(`client registration failed: ${r.status} ${await r.text()}`);
  const { client_id } = await r.json();
  const params = new URLSearchParams({
    response_type: 'code',
    client_id,
    redirect_uri: redirectUri,
    code_challenge: 'a'.repeat(43),
    code_challenge_method: 'S256',
    scope: 'memory.read memory.write',
    state: 'verify-pages',
  });
  return `${BASE}/authorize?${params.toString()}`;
}

async function shoot(browser, name, url, width, height) {
  log(`shooting ${name} (${width}×${height})`);
  const ctx = await browser.newContext({ viewport: { width, height }, deviceScaleFactor: 2, colorScheme: 'dark' });
  const page = await ctx.newPage();
  await page.goto(url, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(FONT_SETTLE_MS);
  await page.screenshot({ path: `${SHOTS}/${name}.png` });
  await ctx.close();
}

// Seed the viewer settings the page bootstrap reads, so the page renders in the
// chosen theme exactly as it would after the user picked it in /view.
async function shootThemed(browser, name, url, settings, width, height) {
  log(`shooting ${name} (${width}×${height})`);
  const ctx = await browser.newContext({ viewport: { width, height }, deviceScaleFactor: 2, colorScheme: 'dark' });
  await ctx.addInitScript((s) => {
    try { localStorage.setItem('memoryvault.viewer.settings.v1', JSON.stringify(s)); } catch (e) {}
  }, settings);
  const page = await ctx.newPage();
  await page.goto(url, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(FONT_SETTLE_MS);
  await page.screenshot({ path: `${SHOTS}/${name}.png` });
  await ctx.close();
}

// The marketing landing is animated, so capture settled states. Light uses the
// paper default (colorScheme light so auto resolves to paper-light); dark forces
// theme_mode. shootLanding keeps the simple full-page/server-page captures.
async function shootLanding(browser, name, opts) {
  log(`shooting ${name} (${opts.width}×${opts.height})`);
  const ctx = await browser.newContext({ viewport: { width: opts.width, height: opts.height }, deviceScaleFactor: 2, colorScheme: opts.dark ? 'dark' : 'light' });
  if (opts.dark) {
    await ctx.addInitScript(() => {
      try { localStorage.setItem('memoryvault.viewer.settings.v1', JSON.stringify({ theme_mode: 'dark' })); } catch (e) {}
    });
  }
  const page = await ctx.newPage();
  await page.goto(opts.url || `${BASE}/`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(FONT_SETTLE_MS);
  await page.screenshot({ path: `${SHOTS}/${name}.png`, fullPage: !!opts.fullPage });
  await ctx.close();
}

// The animated states: hero settled, product-shot overlap, a revealed section,
// the FAQ with one item open, and the dark hero.
async function shootLandingMotion(browser) {
  const newCtx = (dark) =>
    browser.newContext({ viewport: { width: 1280, height: 860 }, deviceScaleFactor: 2, colorScheme: dark ? 'dark' : 'light' }).then(async (ctx) => {
      if (dark) await ctx.addInitScript(() => { try { localStorage.setItem('memoryvault.viewer.settings.v1', JSON.stringify({ theme_mode: 'dark' })); } catch (e) {} });
      return ctx;
    });

  log('shooting landing hero + product shot + section + faq (light)');
  const ctx = await newCtx(false);
  const page = await ctx.newPage();
  await page.goto(`${BASE}/`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(FONT_SETTLE_MS);
  await page.screenshot({ path: `${SHOTS}/landing-hero.png` });
  // Product-shot transition: scroll so the shot overlaps the hero fade.
  await page.evaluate(() => window.scrollTo(0, window.innerHeight * 0.66));
  await page.waitForTimeout(900);
  await page.screenshot({ path: `${SHOTS}/landing-product-shot.png` });
  // A revealed mid section.
  await page.locator('#how').scrollIntoViewIfNeeded();
  await page.waitForTimeout(900);
  await page.screenshot({ path: `${SHOTS}/landing-section.png` });
  // FAQ with the first item open.
  await page.locator('#faq').scrollIntoViewIfNeeded();
  await page.waitForTimeout(600);
  await page.locator('.faq-q').first().click();
  await page.waitForTimeout(600);
  await page.screenshot({ path: `${SHOTS}/landing-faq.png` });
  await ctx.close();

  log('shooting landing hero + section (dark)');
  const dctx = await newCtx(true);
  const dpage = await dctx.newPage();
  await dpage.goto(`${BASE}/`, { waitUntil: 'networkidle' });
  await dpage.waitForTimeout(FONT_SETTLE_MS);
  await dpage.screenshot({ path: `${SHOTS}/landing-hero-dark.png` });
  await dpage.locator('#pricing').scrollIntoViewIfNeeded();
  await dpage.waitForTimeout(900);
  await dpage.screenshot({ path: `${SHOTS}/landing-section-dark.png` });
  await dctx.close();
}

const MIDNIGHT = { theme: 'midnight', theme_mode: 'dark' };

async function screenshotAll() {
  mkdirSync(SHOTS, { recursive: true });
  const authorizeUrl = await buildAuthorizeUrl();
  const browser = await chromium.launch();
  await shoot(browser, 'page-mcp', `${BASE}/mcp`, 1280, 900);
  await shoot(browser, 'page-endpoint-guide', `${BASE}/api/memories`, 1280, 900);
  await shoot(browser, 'page-oauth-authorize', authorizeUrl, 1280, 860);
  // Same pages with the saved theme set to midnight: all should follow.
  await shootThemed(browser, 'page-mcp-midnight', `${BASE}/mcp`, MIDNIGHT, 1280, 900);
  await shootThemed(browser, 'page-endpoint-guide-midnight', `${BASE}/api/memories`, MIDNIGHT, 1280, 900);
  await shootThemed(browser, 'page-oauth-midnight', authorizeUrl, MIDNIGHT, 1280, 860);
  // Marketing landing: full-page light/dark/mobile, then the animated states.
  await shootLanding(browser, 'landing-light', { width: 1280, height: 900, fullPage: true });
  await shootLanding(browser, 'landing-dark', { width: 1280, height: 900, fullPage: true, dark: true });
  await shootLanding(browser, 'landing-mobile', { width: 390, height: 844, fullPage: true });
  await shootLanding(browser, 'page-oauth-paper', { width: 1280, height: 860, url: authorizeUrl });
  await shootLandingMotion(browser);
  await browser.close();
}

let worker = null;
try {
  initDatabase();
  worker = startWorker();
  await waitForWorker();
  await screenshotAll();
  log(`done — screenshots in ${SHOTS}/`);
} finally {
  if (worker) worker.kill('SIGTERM');
}
