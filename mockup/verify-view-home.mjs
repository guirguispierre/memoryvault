// Visual verification for the /view control-center redesign.
//
// Targets an already-running worker (defaults to the isolation dev server on
// :8787). Seeds one account with memories of varied ages/strengths + links via
// signup + /api/import (import preserves created_at/updated_at), then shoots the
// redesigned home in Constellation and paper at 1280 and 390:
//   - login, comfortable list, compact list, a selected row (rail reacting),
//     settings overlay (theme picker + custom builder), paper theme.
//
// Run from the repo root:  BASE_URL=http://127.0.0.1:8787 node mockup/verify-view-home.mjs
import { mkdirSync } from 'node:fs';
import { chromium } from 'playwright';

const BASE = (process.env.BASE_URL ?? 'http://127.0.0.1:8787').replace(/\/+$/, '');
const SHOTS = 'mockup/shots';
const FONT_SETTLE_MS = 2600;
const PASSWORD = 'verify-view-password-1';

function log(m) { process.stdout.write(`[verify-view] ${m}\n`); }

async function signup(email, brainName) {
  const r = await fetch(`${BASE}/auth/signup`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
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
  const runTag = now.toString(36) + Math.random().toString(36).slice(2, 5);
  const mem = (id, ageSec, type, fields) => ({
    id: `${id}-${runTag}`, type, created_at: now - ageSec, updated_at: now - ageSec, ...fields,
  });
  const memories = [
    mem('vh-a2', 2 * 3600, 'fact', { key: 'project.license', title: 'MIT, fully open', content: 'The whole graph ships open from the first commit.', importance: 0.85, confidence: 0.95, source: 'repo' }),
    mem('vh-s2', 5 * 3600, 'fact', { key: 'user.timezone', title: 'Europe/Lisbon', content: 'Reinforced this week, used across 6 recalls.', importance: 0.6, confidence: 0.86, source: 'profile' }),
    mem('vh-a1', 26 * 3600, 'journal', { key: 'isolation.suite', title: '19/19 green', content: 'Constant-time crypto comparisons, JWT alg pinned.', importance: 0.55, confidence: 0.6, tags: 'release,security' }),
    mem('vh-r1', 3 * 86400, 'note', { key: 'old.api.note', title: 'Legacy token mode', content: 'Fading. Superseded by the OAuth 2.1 flow.', importance: 0.28, confidence: 0.4 }),
    mem('vh-g1', 6 * 3600, 'fact', { key: 'graph.enabled', title: 'true, no upsell', content: 'Links, traversal, history. All included.', importance: 0.8, confidence: 0.9 }),
    mem('vh-d1', 8 * 3600, 'fact', { key: 'deploy.target', title: 'Cloudflare Workers', content: 'D1 + Vectorize, one command, your account.', importance: 0.66, confidence: 0.8 }),
    mem('vh-p1', 2 * 86400, 'note', { key: 'pricing.model', title: 'Free + $12 hosted', content: 'Self-host free forever, managed tier optional.', importance: 0.4, confidence: 0.5 }),
    mem('vh-m1', 12 * 3600, 'fact', { key: 'mcp.transport', title: 'HTTP + SSE, JSON-RPC', content: 'initialize / tools/list / tools/call, OAuth discovery.', importance: 0.82, confidence: 0.9 }),
    mem('vh-q1', 5 * 86400, 'note', { key: 'draft.idea.q3', title: 'Vertical knowledge APIs', content: 'Half-formed. Revisit after launch.', importance: 0.22, confidence: 0.3 }),
  ];
  const link = (id, from, to, relation) => ({ id: `${id}-${runTag}`, from_id: `${from}-${runTag}`, to_id: `${to}-${runTag}`, relation_type: relation, label: null, created_at: now - 3600 });
  const memory_links = [
    link('vh-l1', 'vh-a2', 'vh-g1', 'related'),
    link('vh-l2', 'vh-a2', 'vh-d1', 'related'),
    link('vh-l3', 'vh-a2', 'vh-p1', 'supports'),
    link('vh-l4', 'vh-g1', 'vh-m1', 'related'),
    link('vh-l5', 'vh-a1', 'vh-m1', 'related'),
    link('vh-l6', 'vh-s2', 'vh-r1', 'supersedes'),
  ];
  return { schema: 'memoryvault_export_v1', strategy: 'merge', data: { memories, memory_links } };
}

async function seed(cookie) {
  const r = await fetch(`${BASE}/api/import`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', Cookie: cookie },
    body: JSON.stringify(buildSeedPayload()),
  });
  const body = await r.json();
  if (!r.ok) throw new Error(`import failed: ${r.status} ${JSON.stringify(body)}`);
  log(`seeded ${body.imported?.memories ?? 0} memories, ${body.imported?.memory_links ?? 0} links`);
}

async function loginUi(page, email) {
  await page.goto(`${BASE}/view`);
  await page.fill('#email-input', email);
  await page.fill('#password-input', PASSWORD);
  await page.click('button[data-action="login"]');
  await page.waitForSelector('#app', { state: 'visible', timeout: 15000 });
  await page.waitForTimeout(800);
  if (await page.locator('#update-banner.visible').count()) {
    await page.click('[data-action="dismiss-update-banner"]').catch(() => {});
    await page.waitForTimeout(250);
  }
}

async function setTheme(page, settings) {
  await page.addInitScript((s) => {
    try { localStorage.setItem('memoryvault.viewer.settings.v1', JSON.stringify(s)); } catch (e) {}
  }, settings);
}

async function main() {
  mkdirSync(SHOTS, { recursive: true });
  const only = process.argv[2] || 'all';
  const browser = await chromium.launch();

  const email = `verify-view-${Date.now()}@example.com`;
  const cookie = await signup(email, 'Verify View index');
  await seed(cookie);

  const shoot = async (name, { width, height, theme = 'constellation', mode = 'dark', compact = false, mobile = false }, fn) => {
    const ctx = await browser.newContext({ viewport: { width, height }, deviceScaleFactor: 2, colorScheme: mode === 'light' ? 'light' : 'dark', isMobile: mobile });
    const page = await ctx.newPage();
    await setTheme(page, { theme, light_theme: 'paper', theme_mode: mode, compact_cards: compact, live_poll_interval_sec: 30 });
    if (fn) await fn(page);
    await ctx.close();
  };

  if (only === 'all' || only === 'login') {
    log('login');
    for (const w of [1280, 390]) {
      const ctx = await browser.newContext({ viewport: { width: w, height: 860 }, deviceScaleFactor: 2, colorScheme: 'dark' });
      const page = await ctx.newPage();
      await setTheme(page, { theme: 'constellation', theme_mode: 'dark' });
      await page.goto(`${BASE}/view`);
      await page.waitForTimeout(FONT_SETTLE_MS);
      await page.screenshot({ path: `${SHOTS}/vh-login-${w}.png` });
      await ctx.close();
    }
  }

  if (only === 'all' || only === 'smoke' || only === 'comfortable') {
    log('comfortable list (constellation)');
    await shoot('comfortable', { width: 1280, height: 940 }, async (page) => {
      await loginUi(page, email);
      await page.waitForTimeout(FONT_SETTLE_MS);
      await page.screenshot({ path: `${SHOTS}/vh-comfortable-1280.png` });
    });
    await shoot('comfortable-m', { width: 390, height: 844, mobile: true }, async (page) => {
      await loginUi(page, email);
      await page.waitForTimeout(FONT_SETTLE_MS);
      await page.screenshot({ path: `${SHOTS}/vh-comfortable-390.png` });
    });
  }

  if (only === 'all' || only === 'compact') {
    log('compact list');
    await shoot('compact', { width: 1280, height: 940, compact: true }, async (page) => {
      await loginUi(page, email);
      await page.waitForTimeout(FONT_SETTLE_MS);
      await page.screenshot({ path: `${SHOTS}/vh-compact-1280.png` });
    });
  }

  if (only === 'all' || only === 'selected') {
    log('selected row + rail');
    await shoot('selected', { width: 1280, height: 940 }, async (page) => {
      await loginUi(page, email);
      await page.waitForTimeout(1600);
      const row = page.locator('#grid .r').first();
      if (await row.count()) { await row.click(); await page.waitForTimeout(1600); }
      await page.screenshot({ path: `${SHOTS}/vh-selected-1280.png` });
    });
  }

  if (only === 'all' || only === 'settings') {
    log('settings overlay (theme picker + custom builder)');
    await shoot('settings', { width: 1280, height: 1000 }, async (page) => {
      await loginUi(page, email);
      await page.click('[data-action="open-settings-overlay"]');
      await page.waitForSelector('#settings-overlay.open');
      const folder = page.locator('.settings-folder', { has: page.locator('summary', { hasText: 'Appearance' }) });
      if (!(await folder.evaluate((el) => el.open))) { await folder.locator('summary').click(); await page.waitForTimeout(150); }
      await page.click('#theme-picker .theme-swatch[data-theme-value="custom"]').catch(() => {});
      await page.waitForSelector('#custom-theme-builder', { state: 'visible' }).catch(() => {});
      await page.waitForTimeout(FONT_SETTLE_MS);
      await page.screenshot({ path: `${SHOTS}/vh-settings-1280.png` });
    });
  }

  if (only === 'all' || only === 'paper') {
    log('paper theme');
    // A dedicated brain so the server-settings reconcile seeds from this run's
    // localStorage (paper/light) instead of an earlier constellation/dark row.
    const paperEmail = `verify-view-paper-${Date.now()}@example.com`;
    const paperCookie = await signup(paperEmail, 'Verify View paper');
    await seed(paperCookie);
    await shoot('paper', { width: 1280, height: 940, theme: 'paper', mode: 'light' }, async (page) => {
      await loginUi(page, paperEmail);
      await page.waitForTimeout(FONT_SETTLE_MS);
      await page.screenshot({ path: `${SHOTS}/vh-paper-1280.png` });
    });
    await shoot('paper-m', { width: 390, height: 844, theme: 'paper', mode: 'light', mobile: true }, async (page) => {
      await loginUi(page, paperEmail);
      await page.waitForTimeout(FONT_SETTLE_MS);
      await page.screenshot({ path: `${SHOTS}/vh-paper-390.png` });
    });
  }

  await browser.close();
  log('done');
}

main().catch((e) => { console.error(e); process.exit(1); });
