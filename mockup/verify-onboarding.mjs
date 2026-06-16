// Visual verification for first-run onboarding. Boots a worker on a scratch D1,
// signs up a brand-new user (zero memories), logs in through the viewer UI, and
// captures the onboarding panel (desktop + mobile), then clicks "Write a test
// memory" and captures the collapsed hint with the populated grid.
//
// Run from the repo root:  node mockup/verify-onboarding.mjs
import { spawn, execSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { chromium } from 'playwright';

const PORT = 8801;
const BASE = `http://127.0.0.1:${PORT}`;
const STATE = '.wrangler/verify-onboarding-state';
const SHOTS = 'mockup/shots';
const PASSWORD = 'onboarding-verify-password-1';
const FONT_SETTLE_MS = 2600;

function log(m) { process.stdout.write(`[verify-onboarding] ${m}\n`); }

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
    try { const r = await fetch(`${BASE}/`); if (r.ok) return; } catch {}
    await new Promise((res) => setTimeout(res, 1000));
  }
  throw new Error('worker did not become ready');
}

async function signup(email) {
  const r = await fetch(`${BASE}/auth/signup`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'CF-Connecting-IP': `198.51.100.${(Date.now() % 200) + 1}` },
    body: JSON.stringify({ email, password: PASSWORD, brain_name: 'Onboarding brain' }),
  });
  if (!r.ok) throw new Error(`signup failed: ${r.status} ${await r.text()}`);
}

async function login(page, email) {
  await page.goto(`${BASE}/view`, { waitUntil: 'networkidle' });
  await page.fill('#email-input', email);
  await page.fill('#password-input', PASSWORD);
  await page.click('button[data-action="login"]');
  // Fresh brain has no rows; wait for the onboarding panel instead.
  await page.waitForSelector('#onboarding .ob-card', { timeout: 15000 });
}

async function run() {
  mkdirSync(SHOTS, { recursive: true });
  const browser = await chromium.launch();

  // Desktop: the full panel, then write a test memory -> collapsed hint.
  const email = `onboarding-${Date.now()}@example.com`;
  await signup(email);
  const ctx = await browser.newContext({ viewport: { width: 1280, height: 980 }, deviceScaleFactor: 2, colorScheme: 'dark' });
  const page = await ctx.newPage();
  await login(page, email);
  await page.waitForTimeout(FONT_SETTLE_MS);
  // Dismiss the version banner if present so it does not cover the panel.
  if (await page.locator('#update-banner.visible').count()) {
    await page.click('[data-action="dismiss-update-banner"]');
    await page.waitForTimeout(300);
  }
  await page.screenshot({ path: `${SHOTS}/onboarding-panel.png` });
  // Switch the connection tab to the generic client snippet for a second shot.
  await page.click('[data-onb="tab"][data-tab="any"]');
  await page.waitForTimeout(300);
  await page.screenshot({ path: `${SHOTS}/onboarding-any-client.png` });
  // Write a real test memory; the panel should collapse to the hint.
  await page.click('[data-onb="tab"][data-tab="claude"]');
  await page.click('[data-onb="test"]');
  await page.waitForSelector('#onboarding .ob-hint', { timeout: 15000 });
  await page.waitForSelector('.row', { timeout: 15000 });
  await page.waitForTimeout(900);
  await page.screenshot({ path: `${SHOTS}/onboarding-after.png` });
  await ctx.close();

  // Mobile: the panel reflow.
  const emailM = `onboarding-m-${Date.now()}@example.com`;
  await signup(emailM);
  const ctxM = await browser.newContext({ viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, isMobile: true, colorScheme: 'dark' });
  const pageM = await ctxM.newPage();
  await login(pageM, emailM);
  await pageM.waitForTimeout(FONT_SETTLE_MS);
  if (await pageM.locator('#update-banner.visible').count()) {
    await pageM.click('[data-action="dismiss-update-banner"]');
    await pageM.waitForTimeout(300);
  }
  await pageM.screenshot({ path: `${SHOTS}/onboarding-mobile.png` });
  await ctxM.close();

  await browser.close();
}

let worker = null;
try {
  initDatabase();
  worker = startWorker();
  await waitForWorker();
  await run();
  log(`done — screenshots in ${SHOTS}/`);
} finally {
  if (worker) worker.kill('SIGTERM');
}
