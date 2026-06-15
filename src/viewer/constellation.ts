// The dark "constellation" identity shared by every public (non-/view) page:
// deep-space tokens, the background gradient, and the canvas starfield. The
// component variable names match the rest of the design system so the existing
// page chrome (pageChromeCss) re-themes onto these values without change.

export const constellationTokensCss = `  :root {
    --ground: #070810;
    --ground-2: #0C0E1C;
    --ground-3: #12152A;
    --rule: rgba(255, 255, 255, 0.10);
    --rule-soft: rgba(255, 255, 255, 0.055);
    --rule-bright: rgba(255, 255, 255, 0.18);
    --cream: #F5F4EF;
    --cream-dim: #9AA0B4;
    --cream-faint: #565D72;
    --butter: #8AB0FF;
    --butter-deep: #5E7FD0;
    --latte: #9AA0B4;
    --sage: #86E0B8;
    --clay: #FFCAA0;
    --on-butter: #070810;
    --butter-glow: rgba(138, 176, 255, 0.14);
    --violet: #B9A3FF;
    --disp: 'Spectral', Georgia, serif;
    --body: 'Inter', system-ui, sans-serif;
    --mono: 'JetBrains Mono', ui-monospace, monospace;
    --panel-bg: #0C0E1C;
    --surface: rgba(255, 255, 255, 0.035);
    --surface-raised: #0F1224;
    --toast-bg: #0F1224;
    --overlay-bg: rgba(4, 5, 12, 0.82);
    --panel-shadow: rgba(0, 0, 0, 0.6);
    --card-glow: rgba(0, 0, 0, 0.5);
  }
  /* Deep-space gradient, fixed behind everything. */
  .space {
    position: fixed; inset: 0; z-index: -1;
    background:
      radial-gradient(80% 60% at 50% 8%, rgba(40, 52, 110, 0.40), transparent 60%),
      radial-gradient(60% 50% at 82% 80%, rgba(30, 80, 70, 0.22), transparent 60%),
      radial-gradient(55% 45% at 12% 75%, rgba(70, 40, 110, 0.20), transparent 60%),
      var(--ground);
  }
  .sky { display: block; pointer-events: none; }
  /* Utility pages cover the viewport behind the content (with the gradient); the
     landing positions its own canvas inside the hero so the field ends where the
     content begins. */
  .sky.cover { position: fixed; inset: 0; z-index: -1; }
`;

// The canvas starfield, served from /starfield.js (the page CSP forbids inline
// scripts). The landing's #sky animates; a [data-calm] canvas (utility pages)
// renders a single sparse static frame. prefers-reduced-motion is static too.
// The loop pauses when the hero scrolls away or the tab is hidden.
export const starfieldJs = `(function(){
  var cv = document.getElementById('sky');
  if (!cv || !cv.getContext) return;
  var ctx = cv.getContext('2d');
  var DPR = Math.min(window.devicePixelRatio || 1, 2);
  var calm = cv.hasAttribute('data-calm');
  var reduce = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  var animated = !calm && !reduce;
  var W = 0, H = 0, mx = 0, my = 0, tx = 0, ty = 0;
  var PAL = { active: [134,224,184], settling: [255,202,160], fading: [86,93,114], reinforced: [138,176,255] };
  var keys = ['active', 'settling', 'fading', 'reinforced'];
  // Size the backing store to the canvas's own CSS box (set by the page: the
  // hero on the landing, a fixed cover on utility pages); CSS owns display size.
  function size() {
    var w = cv.clientWidth || window.innerWidth;
    var h = cv.clientHeight || window.innerHeight;
    W = cv.width = Math.round(w * DPR);
    H = cv.height = Math.round(h * DPR);
  }
  // Density scales with width and is far lower on calm pages; never thousands.
  function starCount() {
    var n = Math.round((cv.clientWidth || window.innerWidth) / (calm ? 90 : 26));
    return Math.max(8, Math.min(n, calm ? 26 : 130));
  }
  var stars = [];
  function build() {
    stars = [];
    var n = starCount();
    for (var i = 0; i < n; i++) {
      var big = Math.random() < (calm ? 0.10 : 0.16);
      stars.push({
        x: Math.random() * W, y: Math.random() * H,
        vx: (Math.random() - 0.5) * 0.12 * DPR, vy: (Math.random() - 0.5) * 0.12 * DPR,
        r: (big ? Math.random() * 2.4 + 2.2 : Math.random() * 1.3 + 0.7) * DPR,
        big: big, depth: Math.random() * 0.6 + 0.4,
        c: PAL[keys[Math.floor(Math.random() * keys.length)]],
        ph: Math.random() * 6.28, tw: Math.random() * 0.6 + 0.5
      });
    }
  }
  function drawStar(sx, sy, s, glow) {
    var r = s.c[0], g = s.c[1], b = s.c[2];
    var rad = s.r * (s.big ? 6 : 4.5);
    var grd = ctx.createRadialGradient(sx, sy, 0, sx, sy, rad);
    grd.addColorStop(0, 'rgba(' + r + ',' + g + ',' + b + ',' + glow + ')');
    grd.addColorStop(0.4, 'rgba(' + r + ',' + g + ',' + b + ',' + (glow * 0.35) + ')');
    grd.addColorStop(1, 'rgba(' + r + ',' + g + ',' + b + ',0)');
    ctx.fillStyle = grd; ctx.beginPath(); ctx.arc(sx, sy, rad, 0, 6.2832); ctx.fill();
    // lit-from-within white core
    ctx.fillStyle = 'rgba(255,255,255,' + Math.min(1, glow * 1.1) + ')';
    ctx.beginPath(); ctx.arc(sx, sy, s.r * 0.6, 0, 6.2832); ctx.fill();
    if (s.big) {
      ctx.strokeStyle = 'rgba(' + r + ',' + g + ',' + b + ',' + (glow * 0.5) + ')';
      ctx.lineWidth = DPR * 0.8;
      var L = s.r * 5;
      ctx.beginPath();
      ctx.moveTo(sx - L, sy); ctx.lineTo(sx + L, sy);
      ctx.moveTo(sx, sy - L); ctx.lineTo(sx, sy + L);
      ctx.stroke();
    }
  }
  function frame(t) {
    ctx.clearRect(0, 0, W, H);
    ctx.globalCompositeOperation = 'lighter';
    mx += (tx - mx) * 0.05; my += (ty - my) * 0.05;
    var thr = 170 * DPR;
    for (var i = 0; i < stars.length; i++) {
      for (var j = i + 1; j < stars.length; j++) {
        var a = stars[i], b = stars[j];
        var dx = a.x - b.x, dy = a.y - b.y, d = Math.sqrt(dx * dx + dy * dy);
        if (d < thr) {
          var al = (calm ? 0.10 : 0.16) * (1 - d / thr);
          ctx.strokeStyle = 'rgba(138,176,255,' + al + ')';
          ctx.lineWidth = DPR * 0.7;
          ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke();
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
      var px = s.x - mx * 40 * DPR * s.depth, py = s.y - my * 40 * DPR * s.depth;
      var tw = animated ? (0.55 + 0.45 * Math.sin(t / 700 * s.tw + s.ph)) : 0.85;
      var base = (s.c === PAL.fading ? 0.4 : 0.9);
      drawStar(px, py, s, base * tw);
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
  // Pause when the canvas scrolls out of view (it lives in the hero, so the
  // field stops once content takes over) or the tab is hidden; throttle ~45fps.
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
