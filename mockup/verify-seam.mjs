// Probe the hero/section seam on the live worker: screenshot the boundary at
// 1280 and 390, then sample a vertical column in the empty left margin (away
// from text/panels) and report the background-luminance profile and where the
// stars thin out. Run with the worker already up on BASE (default :8787).
import { chromium } from 'playwright';

const BASE = process.env.BASE_URL || 'http://127.0.0.1:8787';
const SHOTS = 'mockup/shots';

function lum(r, g, b) { return 0.2126 * r + 0.7152 * g + 0.0722 * b; }

async function run() {
  const browser = await chromium.launch();

  // Boundary screenshots (boundary at 100vh; scroll so it sits mid-viewport).
  for (const [w, h, tag] of [[1280, 840, '1280'], [390, 844, '390']]) {
    const ctx = await browser.newContext({ viewport: { width: w, height: h }, deviceScaleFactor: 2 });
    const page = await ctx.newPage();
    await page.goto(`${BASE}/`, { waitUntil: 'networkidle' });
    await page.waitForTimeout(2600);
    await page.evaluate((vh) => window.scrollTo(0, vh * 0.55), h);
    await page.waitForTimeout(700);
    await page.screenshot({ path: `${SHOTS}/seam-boundary-${tag}.png` });
    await ctx.close();
  }

  // Pixel probe at DSF 1 so screenshot px == CSS px.
  const ctx = await browser.newContext({ viewport: { width: 1280, height: 840 }, deviceScaleFactor: 1 });
  const page = await ctx.newPage();
  await page.goto(`${BASE}/`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(2600);
  const shot = await page.screenshot({ fullPage: true });
  const dataUrl = 'data:image/png;base64,' + shot.toString('base64');
  await page.close();

  // Decode in a blank page (the landing's CSP forbids data: images). Sample a
  // 60px-wide strip in the left margin (x 60..120), left of the 1080px content
  // container (its left margin starts near x=100 at 1280 wide).
  const decoder = await ctx.newPage();
  await decoder.goto('about:blank');
  const probe = await decoder.evaluate(async (url) => {
    const img = new Image();
    await new Promise((res, rej) => { img.onload = res; img.onerror = rej; img.src = url; });
    const c = document.createElement('canvas');
    c.width = img.width; c.height = img.height;
    const g = c.getContext('2d');
    g.drawImage(img, 0, 0);
    const x0 = 60, x1 = 120;
    const y0 = 760, y1 = 1120; // 0.9vh..1.33vh region around the 840px (100vh) seam
    const L = (r, gg, b) => 0.2126 * r + 0.7152 * gg + 0.0722 * b;
    const rows = [];
    for (let y = y0; y <= y1 && y < img.height; y++) {
      const d = g.getImageData(x0, y, x1 - x0, 1).data;
      let mn = 999, mx = 0;
      for (let i = 0; i < d.length; i += 4) {
        const v = L(d[i], d[i + 1], d[i + 2]);
        if (v < mn) mn = v;
        if (v > mx) mx = v;
      }
      rows.push({ y, bg: mn, star: mx });
    }
    return { rows, h: img.height, w: img.width };
  }, dataUrl);
  await ctx.close();
  await browser.close();

  const rows = probe.rows;
  // Background = per-row min luminance (excludes bright star glints).
  let maxStep = 0, stepAt = 0;
  for (let i = 1; i < rows.length; i++) {
    const step = Math.abs(rows[i].bg - rows[i - 1].bg);
    if (step > maxStep) { maxStep = step; stepAt = rows[i].y; }
  }
  // Where do stars thin out: last row whose (star - bg) headroom exceeds 8.
  let lastStarY = rows[0].y;
  for (const r of rows) if (r.star - r.bg > 8) lastStarY = r.y;
  // Background darkening span: first/last row where bg is within the transition.
  const bgTop = rows[0].bg, bgBot = rows[rows.length - 1].bg;
  const sample = rows.filter((_, i) => i % 30 === 0).map((r) => `y=${r.y} bg=${r.bg.toFixed(1)} star=${r.star.toFixed(1)}`);

  console.log('image', probe.w + 'x' + probe.h);
  console.log('background luminance: top(y760)=' + bgTop.toFixed(1) + ' bottom(y1120)=' + bgBot.toFixed(1));
  console.log('max consecutive background step = ' + maxStep.toFixed(2) + ' at y=' + stepAt + (maxStep >= 6 ? '  <-- FAIL (>=6)' : '  (ok, <6)'));
  console.log('stars last visible (star-bg>8) at y=' + lastStarY + ' (seam=840)');
  console.log('samples:\n  ' + sample.join('\n  '));
}

run();
