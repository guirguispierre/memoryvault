// The dark "constellation" identity shared by every public (non-/view) page:
// deep-space tokens, the background gradient, and the canvas starfield. The
// semantic names (--bg, --ink, --accent, ...) match the design mockup; the
// component aliases below let the shared page chrome (pageChromeCss, the OAuth
// screen) re-theme onto the same values without change.

export const constellationTokensCss = `  :root {
    --bg: #070810;
    --bg2: #0b0d18;
    --surface: #11131f;
    --ink: #f5f4ef;
    --dim: #9aa0b4;
    --faint: #565d72;
    --rule: rgba(255, 255, 255, 0.08);
    --accent: #8ab0ff;
    --good: #86e0b8;
    --warm: #ffcaa0;
    --violet: #b9a3ff;
    --doc: 'Spectral', Georgia, serif;
    --ui: 'Inter', system-ui, sans-serif;
    --mono: 'JetBrains Mono', ui-monospace, monospace;
    /* Component aliases consumed by pageChromeCss and the OAuth screen. */
    --ground: var(--bg);
    --ground-2: var(--bg2);
    --ground-3: var(--surface);
    --cream: var(--ink);
    --cream-dim: var(--dim);
    --cream-faint: var(--faint);
    --rule-soft: rgba(255, 255, 255, 0.05);
    --rule-bright: rgba(255, 255, 255, 0.14);
    --butter: var(--accent);
    --butter-deep: #5e7fd0;
    --butter-glow: rgba(138, 176, 255, 0.14);
    --on-butter: #070810;
    --sage: var(--good);
    --clay: var(--warm);
    --latte: var(--dim);
    --surface-raised: var(--surface);
    --panel-shadow: rgba(0, 0, 0, 0.6);
    --card-glow: rgba(0, 0, 0, 0.5);
    --disp: var(--doc);
    --body: var(--ui);
  }
  /* Deep-space gradient, fixed behind everything. */
  .space {
    position: fixed; inset: 0; z-index: -1;
    background:
      radial-gradient(80% 60% at 50% 8%, rgba(40, 52, 110, 0.38), transparent 60%),
      radial-gradient(60% 50% at 82% 80%, rgba(30, 80, 70, 0.18), transparent 60%),
      radial-gradient(55% 45% at 12% 75%, rgba(70, 40, 110, 0.16), transparent 60%),
      var(--bg);
  }
  .sky { display: block; pointer-events: none; }
  /* Utility pages cover the viewport behind the content (with the gradient); the
     landing positions its own canvas inside the hero so the field ends where the
     content begins. */
  .sky.cover { position: fixed; inset: 0; z-index: -1; }
`;

// The canvas starfield, served from /starfield.js (the page CSP forbids inline
// scripts). The landing's #sky animates with a text dead-zone; a [data-calm]
// canvas (utility pages) renders one sparse static frame. prefers-reduced-motion
// is static too. The loop pauses when the canvas scrolls away or the tab hides.
export const starfieldJs = `(function(){
  var cv = document.getElementById('sky');
  if (!cv || !cv.getContext) return;
  var ctx = cv.getContext('2d');
  var DPR = Math.min(window.devicePixelRatio || 1, 2);
  var calm = cv.hasAttribute('data-calm');
  var reduce = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  var animated = !calm && !reduce;
  var W = 0, H = 0, mx = 0, my = 0, tx = 0, ty = 0;
  var PAL = { a: [134,224,184], s: [255,202,160], f: [86,93,114], r: [138,176,255] };
  var keys = ['a', 's', 'f', 'r'];
  function size() {
    var w = cv.clientWidth || window.innerWidth;
    var h = cv.clientHeight || window.innerHeight;
    W = cv.width = Math.round(w * DPR);
    H = cv.height = Math.round(h * DPR);
  }
  // Density scales with width and is far lower on calm pages; never thousands.
  function starCount() {
    var n = Math.round((cv.clientWidth || window.innerWidth) / (calm ? 90 : 24));
    return Math.max(8, Math.min(n, calm ? 24 : 130));
  }
  var stars = [];
  function build() {
    stars = [];
    var n = starCount();
    for (var i = 0; i < n; i++) {
      var big = Math.random() < 0.12;
      stars.push({
        x: Math.random() * W, y: Math.random() * H,
        vx: (Math.random() - 0.5) * 0.10 * DPR, vy: (Math.random() - 0.5) * 0.10 * DPR,
        r: (big ? Math.random() * 1.8 + 1.6 : Math.random() * 1.1 + 0.6) * DPR,
        big: big, depth: Math.random() * 0.6 + 0.4,
        c: PAL[keys[Math.floor(Math.random() * keys.length)]],
        ph: Math.random() * 6.28, tw: Math.random() * 0.5 + 0.5
      });
    }
  }
  // Fade stars and links to nothing inside an ellipse over the hero text so
  // nothing renders close behind the copy. Calm pages have no centered text.
  // The center/radii are fractions of the canvas height, which now runs to
  // 135vh on the landing, so they are scaled to keep the protected ellipse over
  // the same viewport region as before the canvas was extended.
  function deadZone(px, py) {
    if (calm) return 1;
    var d = Math.hypot((px - W * 0.5) / (W * 0.34), (py - H * 0.34) / (H * 0.22));
    if (d >= 1) return 1;
    return Math.max(0, (d - 0.45) / 0.55);
  }
  function drawStar(sx, sy, s, glow) {
    var r = s.c[0], g = s.c[1], b = s.c[2];
    var R = s.r * (s.big ? 3.2 : 2.6);
    var grd = ctx.createRadialGradient(sx, sy, 0, sx, sy, R);
    grd.addColorStop(0, 'rgba(' + r + ',' + g + ',' + b + ',' + glow + ')');
    grd.addColorStop(0.5, 'rgba(' + r + ',' + g + ',' + b + ',' + (glow * 0.2) + ')');
    grd.addColorStop(1, 'rgba(' + r + ',' + g + ',' + b + ',0)');
    ctx.fillStyle = grd; ctx.beginPath(); ctx.arc(sx, sy, R, 0, 6.2832); ctx.fill();
    // crisp white core
    ctx.fillStyle = 'rgba(255,255,255,' + Math.min(1, glow * 0.9) + ')';
    ctx.beginPath(); ctx.arc(sx, sy, s.r * 0.55, 0, 6.2832); ctx.fill();
  }
  function frame(t) {
    ctx.clearRect(0, 0, W, H);
    ctx.globalCompositeOperation = 'lighter';
    mx += (tx - mx) * 0.05; my += (ty - my) * 0.05;
    var thr = 150 * DPR;
    for (var i = 0; i < stars.length; i++) {
      for (var j = i + 1; j < stars.length; j++) {
        var a = stars[i], b = stars[j];
        var dx = a.x - b.x, dy = a.y - b.y, d = Math.sqrt(dx * dx + dy * dy);
        if (d < thr) {
          var dz = deadZone((a.x + b.x) / 2, (a.y + b.y) / 2);
          var al = 0.13 * (1 - d / thr) * dz;
          if (al > 0.002) {
            ctx.strokeStyle = 'rgba(138,176,255,' + al + ')';
            ctx.lineWidth = DPR * 0.6;
            ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke();
          }
        }
      }
    }
    for (var k = 0; k < stars.length; k++) {
      var s = stars[k];
      if (animated) {
        s.x += s.vx; s.y += s.vy;
        if (s.x < -20) s.x = W + 20; if (s.x > W + 20) s.x = -20;
        if (s.y < -20) s.y = H + 20; if (s.y > H + 20) s.y = -20;
      }
      var px = s.x - mx * 36 * DPR * s.depth, py = s.y - my * 36 * DPR * s.depth;
      var tw = animated ? (0.5 + 0.5 * Math.sin(t / 800 * s.tw + s.ph)) : 0.8;
      var base = (s.c === PAL.f ? 0.36 : 0.8) * tw * deadZone(px, py);
      if (base > 0.01) drawStar(px, py, s, base);
    }
    ctx.globalCompositeOperation = 'source-over';
  }
  size();
  build();
  if (!animated) {
    frame(0);
    window.addEventListener('resize', function () { size(); build(); frame(0); });
    return;
  }
  // Pause when the canvas scrolls out of view (it lives in the hero) or the tab
  // is hidden; throttle to ~45fps.
  var visible = true;
  if ('IntersectionObserver' in window) {
    new IntersectionObserver(function (en) { visible = en[0].isIntersecting; }, { threshold: 0 }).observe(cv);
  }
  window.addEventListener('mousemove', function (e) {
    tx = e.clientX / window.innerWidth - 0.5;
    ty = e.clientY / window.innerHeight - 0.5;
  });
  window.addEventListener('resize', function () { size(); build(); });
  var last = 0;
  function loop(t) {
    if (!document.hidden && visible && t - last >= 22) { frame(t); last = t; }
    requestAnimationFrame(loop);
  }
  requestAnimationFrame(loop);
})();`;

// <head> tags every public page emits for the constellation identity.
export const constellationHeadTags = `<meta name="color-scheme" content="dark">`;

// Gradient + a fixed, sparse, static starfield for the calm utility pages
// (/mcp, guides, /endpoints, OAuth). The landing builds its own animated canvas
// inside the hero instead of calling this.
export const constellationCalmField = `<div class="space"></div>
<canvas id="sky" class="sky cover" aria-hidden="true" data-calm></canvas>
<script src="/starfield.js" defer></script>`;
