// Visual verification for the vanilla viewer reskin.
//
// Boots the worker locally on the isolation wrangler config (no Vectorize —
// the viewer does not need it), seeds one account plus memories of varied
// ages/strengths via signup + /api/import (import preserves created_at /
// updated_at, which memory_save does not), then screenshots:
//   - login            1280×820
//   - main list        1280×980  (all three tiers + the pour populated)
//   - main list mobile  390×844
//
// Run from the repo root:  node mockup/verify-ui.mjs
import { spawn, execSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { chromium } from 'playwright';

const PORT = 8799;
const BASE = `http://127.0.0.1:${PORT}`;
const STATE = '.wrangler/verify-ui-state';
const SHOTS = 'mockup/shots';
const FONT_SETTLE_MS = 2800;

const EMAIL = `verify-ui-${Date.now()}@example.com`;
const PASSWORD = 'verify-ui-password-1';

function log(msg) {
  process.stdout.write(`[verify-ui] ${msg}\n`);
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
      const r = await fetch(`${BASE}/view`);
      if (r.ok) return;
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  throw new Error('worker did not become ready');
}

async function signup(email = EMAIL, brainName = 'Verify UI index') {
  const r = await fetch(`${BASE}/auth/signup`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      // Synthetic IP so repeated local runs do not trip the signup limiter.
      'CF-Connecting-IP': `198.51.100.${(Date.now() % 200) + 1}`,
    },
    body: JSON.stringify({ email, password: PASSWORD, brain_name: brainName }),
  });
  if (!r.ok) throw new Error(`signup failed: ${r.status} ${await r.text()}`);
  const cookie = r.headers.getSetCookie().find((c) => c.startsWith('auth_token='));
  if (!cookie) throw new Error('signup did not set auth_token cookie');
  return cookie.split(';')[0];
}

function buildSeedPayload() {
  const now = Math.floor(Date.now() / 1000);
  // The local D1 state persists across runs and memory ids are globally
  // unique, so ids must be salted per run or re-imports silently no-op.
  const runTag = now.toString(36);
  const mem = (id, ageSec, type, fields) => ({
    id: `${id}-${runTag}`,
    type,
    created_at: now - ageSec,
    updated_at: now - ageSec,
    ...fields,
  });
  // Ages and scores chosen so the derived strength lands in all three tiers:
  // strength = 0.45·imp + 0.20·conf + 0.35·exp(-days/14), tiers at 0.62 / 0.38.
  const memories = [
    mem('verify-a1', 30 * 60, 'journal', {
      title: 'Shipped the isolation suite',
      content: '19 of 19 green against a live worker. Crypto comparisons are constant-time and the JWT algorithm is pinned — the worry is behind us.',
      importance: 0.9, confidence: 0.9, tags: 'release,security',
    }),
    mem('verify-a2', 2 * 3600, 'fact', {
      key: 'project.license',
      content: 'MIT — fully open, graph features not paywalled.',
      importance: 0.8, confidence: 0.95, source: 'repo',
    }),
    mem('verify-s1', 12 * 86400, 'note', {
      title: 'Prefers terse, direct answers',
      content: 'Dislikes filler openers; wants pushback when an idea is weak rather than agreement.',
      importance: 0.4, confidence: 0.55, tags: 'style',
    }),
    mem('verify-s2', 13 * 86400, 'fact', {
      key: 'user.timezone',
      content: 'Europe/Lisbon (WET / WEST)',
      importance: 0.42, confidence: 0.86, source: 'profile',
    }),
    mem('verify-r1', 45 * 86400, 'note', {
      title: 'Launch around the security story',
      content: '"A memory layer you actually own," aimed at developers self-hosting on Cloudflare.',
      importance: 0.3, confidence: 0.4, tags: 'marketing',
    }),
    mem('verify-r2', 60 * 86400, 'note', {
      title: 'Early benchmark idea: LoCoMo',
      content: 'Run LongMemEval + LoCoMo and publish the numbers — required to be visible in comparisons.',
      importance: 0.25, confidence: 0.35,
    }),
    // Extra recent activity so the pour has texture across the last 24h.
    mem('verify-p1', 6 * 3600, 'journal', {
      title: 'Reviewed CORS allowlist',
      content: 'Worker CORS allowlist now restricted to known origins.',
      importance: 0.25, confidence: 0.45,
    }),
  ];
  const link = (id, from, to, relation) => ({
    id: `${id}-${runTag}`,
    from_id: `${from}-${runTag}`,
    to_id: `${to}-${runTag}`,
    relation_type: relation,
    label: null,
    created_at: now - 3600,
  });
  const memory_links = [
    link('verify-l1', 'verify-a1', 'verify-a2', 'related'),
    link('verify-l3', 'verify-a1', 'verify-p1', 'related'),
    link('verify-l7', 'verify-s2', 'verify-s1', 'related'),
  ];
  return { schema: 'memoryvault_export_v1', strategy: 'merge', data: { memories, memory_links } };
}

async function seed(cookie) {
  log('seeding memories via /api/import');
  const r = await fetch(`${BASE}/api/import`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Cookie: cookie },
    body: JSON.stringify(buildSeedPayload()),
  });
  const body = await r.json();
  if (!r.ok) throw new Error(`import failed: ${r.status} ${JSON.stringify(body)}`);
  log(`seeded ${body.imported?.memories ?? 0} memories, ${body.imported?.memory_links ?? 0} links`);
}

async function mcpCall(cookie, name, args) {
  const r = await fetch(`${BASE}/mcp`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Cookie: cookie },
    body: JSON.stringify({ jsonrpc: '2.0', id: name, method: 'tools/call', params: { name, arguments: args } }),
  });
  if (!r.ok) throw new Error(`${name} failed: ${r.status} ${await r.text()}`);
  const rpc = await r.json();
  if (rpc && rpc.error) throw new Error(`${name} error: ${JSON.stringify(rpc.error)}`);
  return rpc;
}

async function listMemoryIds(cookie) {
  const r = await fetch(`${BASE}/api/memories?limit=500`, { headers: { Cookie: cookie } });
  const data = await r.json();
  return (data.memories || []).map((m) => m.id);
}

async function loginThroughUi(page, email = EMAIL) {
  await page.goto(`${BASE}/view`);
  await page.fill('#email-input', email);
  await page.fill('#password-input', PASSWORD);
  await page.click('button[data-action="login"]');
  await page.waitForSelector('.row', { timeout: 15000 });
  const dismiss = page.locator('[data-action="dismiss-update-banner"]');
  if (await page.locator('#update-banner.visible').count()) {
    await dismiss.click();
    await page.waitForTimeout(300);
  }
}

async function openSettings(page) {
  await page.click('[data-action="open-settings-overlay"]');
  await page.waitForSelector('#settings-overlay.open', { timeout: 5000 });
}

async function openFolder(page, text) {
  const folder = page.locator('.settings-folder', { has: page.locator('summary', { hasText: text }) });
  if (!(await folder.evaluate((el) => el.open))) {
    await folder.locator('summary').click();
    await page.waitForTimeout(150);
  }
}

// A custom palette that matches none of the presets, so the reskin is obvious.
const CUSTOM_PALETTE = {
  ground: '#0f1a2b', ground_2: '#16243a', cream: '#eaf2ff',
  cream_dim: '#9fb3d0', butter: '#6fb7ff', rule: '#2a3b55',
};

async function applyCustomPalette(page) {
  await openSettings(page);
  await openFolder(page, 'Appearance');
  await page.click('#theme-picker .theme-swatch[data-theme-value="custom"]');
  await page.waitForSelector('#custom-theme-builder', { state: 'visible' });
  for (const [token, value] of Object.entries(CUSTOM_PALETTE)) {
    await page.fill(`[data-custom-token="${token}"][data-custom-kind="hex"]`, value);
  }
  await page.waitForTimeout(200);
}

async function screenshotFilters(browser) {
  log('shooting filter tabs (ribbon constant, grid filtered)');
  const ctx = await browser.newContext({ viewport: { width: 1280, height: 980 }, deviceScaleFactor: 2, colorScheme: 'dark' });
  const page = await ctx.newPage();
  await loginThroughUi(page);
  await page.waitForTimeout(FONT_SETTLE_MS);
  await page.screenshot({ path: `${SHOTS}/filter-all.png` });
  for (const [type, name] of [['note', 'notes'], ['fact', 'facts'], ['journal', 'journal']]) {
    await page.click(`#stat-${type}`);
    // setFilter -> loadMemories does the grid fetch plus the corpus fetch.
    await page.waitForTimeout(1500);
    await page.screenshot({ path: `${SHOTS}/filter-${name}.png` });
  }
  await ctx.close();
}

async function screenshotReactivity(browser) {
  log('shooting reactivity before/after');
  // A dedicated brain so the earlier theme/compact contexts (which save
  // settings server-side for the shared account) can't override the poll
  // interval this test relies on.
  const reactEmail = `verify-ui-react-${Date.now()}@example.com`;
  const reactCookie = await signup(reactEmail, 'Verify UI reactive');
  await seed(reactCookie);
  const ctx = await browser.newContext({ viewport: { width: 1280, height: 980 }, deviceScaleFactor: 2, colorScheme: 'dark' });
  // Shorten the poll to its 5s floor so the test doesn't wait a full cycle.
  await ctx.addInitScript(() => {
    try { localStorage.setItem('memoryvault.viewer.settings.v1', JSON.stringify({ live_poll_interval_sec: 5 })); } catch (e) {}
  });
  const page = await ctx.newPage();
  await loginThroughUi(page, reactEmail);
  await page.waitForTimeout(FONT_SETTLE_MS);
  await page.screenshot({ path: `${SHOTS}/reactive-before.png` });

  // Let one poll establish the baseline fingerprint before mutating, otherwise
  // the first poll would capture the post-mutation state as its baseline and
  // never see a diff.
  await page.waitForTimeout(6000);

  // Mutate via the API only: add a fresh strong memory and reinforce the
  // oldest. The point is the content fingerprint moves, so the poll refreshes
  // without a click.
  await mcpCall(reactCookie, 'memory_save', {
    type: 'journal',
    title: 'Reinforced live just now',
    content: 'Added through the API to prove the view reacts without a manual refresh.',
    importance: 0.95, confidence: 0.92,
  });
  const ids = await listMemoryIds(reactCookie);
  const oldest = ids[ids.length - 1];
  if (oldest) await mcpCall(reactCookie, 'memory_reinforce', { id: oldest, delta_importance: 0.4, delta_confidence: 0.2 });

  // Wait past the next 5s poll cycle for the silent refresh, then re-shoot.
  await page.waitForTimeout(8000);
  await page.screenshot({ path: `${SHOTS}/reactive-after.png` });
  await ctx.close();
}

async function screenshotAll() {
  mkdirSync(SHOTS, { recursive: true });
  const browser = await chromium.launch();

  log('shooting login (1280×820)');
  const loginCtx = await browser.newContext({ viewport: { width: 1280, height: 820 }, deviceScaleFactor: 2, colorScheme: 'dark' });
  const loginPage = await loginCtx.newPage();
  await loginPage.goto(`${BASE}/view`);
  await loginPage.waitForTimeout(FONT_SETTLE_MS);
  await loginPage.screenshot({ path: `${SHOTS}/login.png` });
  await loginCtx.close();

  log('shooting main list (1280×980)');
  const mainCtx = await browser.newContext({ viewport: { width: 1280, height: 980 }, deviceScaleFactor: 2, colorScheme: 'dark' });
  const mainPage = await mainCtx.newPage();
  await loginThroughUi(mainPage);
  await mainPage.waitForTimeout(FONT_SETTLE_MS);
  await mainPage.screenshot({ path: `${SHOTS}/main.png` });
  await mainCtx.close();

  log('shooting mobile list (390×844)');
  const mobileCtx = await browser.newContext({ viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, isMobile: true, colorScheme: 'dark' });
  const mobilePage = await mobileCtx.newPage();
  await loginThroughUi(mobilePage);
  await mobilePage.waitForTimeout(FONT_SETTLE_MS);
  await mobilePage.screenshot({ path: `${SHOTS}/mobile.png` });
  await mobileCtx.close();

  log('shooting theme picker (presets + Custom)');
  const pickerCtx = await browser.newContext({ viewport: { width: 1280, height: 1000 }, deviceScaleFactor: 2, colorScheme: 'dark' });
  const pickerPage = await pickerCtx.newPage();
  await loginThroughUi(pickerPage);
  await openSettings(pickerPage);
  await openFolder(pickerPage, 'Appearance');
  await pickerPage.locator('#theme-picker').scrollIntoViewIfNeeded();
  await pickerPage.waitForTimeout(FONT_SETTLE_MS);
  await pickerPage.locator('.settings-box').screenshot({ path: `${SHOTS}/settings-theme-picker.png` });
  await pickerCtx.close();

  log('shooting custom builder + custom-theme list');
  const customCtx = await browser.newContext({ viewport: { width: 1280, height: 1000 }, deviceScaleFactor: 2, colorScheme: 'dark' });
  const customPage = await customCtx.newPage();
  await loginThroughUi(customPage);
  await applyCustomPalette(customPage);
  await customPage.locator('#custom-theme-builder').scrollIntoViewIfNeeded();
  await customPage.waitForTimeout(FONT_SETTLE_MS);
  await customPage.locator('.settings-box').screenshot({ path: `${SHOTS}/custom-builder.png` });
  await customPage.click('[data-action="close-settings"]');
  await customPage.waitForTimeout(600);
  await customPage.screenshot({ path: `${SHOTS}/main-custom.png` });
  await customCtx.close();

  log('shooting compact density list');
  const compactCtx = await browser.newContext({ viewport: { width: 1280, height: 980 }, deviceScaleFactor: 2, colorScheme: 'dark' });
  const compactPage = await compactCtx.newPage();
  await loginThroughUi(compactPage);
  await openSettings(compactPage);
  await compactPage.check('#settings-compact-cards');
  await compactPage.click('[data-action="apply-settings"]');
  await compactPage.waitForTimeout(FONT_SETTLE_MS);
  await compactPage.screenshot({ path: `${SHOTS}/main-compact.png` });
  await compactCtx.close();

  await screenshotFilters(browser);
  await screenshotReactivity(browser);

  await browser.close();
}

let worker = null;
try {
  initDatabase();
  worker = startWorker();
  await waitForWorker();
  const cookie = await signup();
  await seed(cookie);
  await screenshotAll();
  log(`done — screenshots in ${SHOTS}/`);
} finally {
  if (worker) worker.kill('SIGTERM');
}
