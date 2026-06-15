// Single source of truth for the vanilla design system shared by every page
// the worker serves: the viewer at /view plus the landing, /mcp, endpoint
// guide, and OAuth screens. Pages import these strings rather than pasting
// hex values, so a palette change here flows everywhere at once.

// One Google Fonts request covering every theme's type roles. The @font-face
// blocks are declared for all families but the browser only fetches the files
// the active theme actually renders, so the paper (Spectral/Inter/JetBrains)
// and warm (Fraunces/Schibsted/IBM Plex) sets don't both download.
export const FONT_LINK_TAGS = `<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,420;0,9..144,560;0,9..144,640;1,9..144,420;1,9..144,560&family=Schibsted+Grotesk:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&family=Spectral:ital,wght@0,400;0,500;0,600;1,400&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">`;

// The default (dark) vanilla tokens. The viewer's theme variants in themes.ts
// build on top of this block; the server-rendered pages emit it verbatim.
export const vanillaTokensCss = `  :root {
    --ground: #181511;
    --ground-2: #201C16;
    --ground-3: #26211A;
    --rule: #332C22;
    --rule-soft: #2A2419;
    --rule-bright: #453B2C;
    --cream: #F0E7D5;
    --cream-dim: #B5AB97;
    --cream-faint: #7E7666;
    --butter: #E3C98F;
    --butter-deep: #A98F5C;
    --latte: #8C8170;
    --sage: #9DB39A;
    --clay: #C9826E;
    --on-butter: #241C0D;
    --butter-glow: rgba(227, 201, 143, 0.07);
    --disp: 'Fraunces', Georgia, serif;
    --body: 'Schibsted Grotesk', system-ui, sans-serif;
    --mono: 'IBM Plex Mono', ui-monospace, monospace;
    --overlay-bg: rgba(16, 13, 10, 0.78);
    --panel-bg: #201C16;
    --panel-shadow: rgba(0, 0, 0, 0.5);
    --surface: rgba(32, 28, 22, 0.6);
    --surface-raised: #241F18;
    --toast-bg: #241F18;
    --card-glow: rgba(0, 0, 0, 0.35);

    /* Legacy aliases: graph client reads these via getComputedStyle and
       relation styling keys off them, so they track the vanilla tokens. */
    --bg: var(--ground);
    --bg2: var(--ground-2);
    --bg3: var(--ground-3);
    --border: var(--rule);
    --border-bright: var(--rule-bright);
    --amber: var(--butter);
    --amber-dim: var(--butter-deep);
    --amber-glow: var(--butter-glow);
    --teal: var(--sage);
    --red: var(--clay);
    --text: var(--cream-dim);
    --text-dim: var(--cream-faint);
    --text-bright: var(--cream);
    --journal: var(--latte);
    --success: var(--sage);
    --info: var(--latte);
    --causes: var(--butter-deep);
  }
`;

// Shared chrome for the document pages (landing, /mcp, endpoint guide). Colour
// and font only ever come from the tokens above — page-specific layout (tables,
// metric tiles, grids) is added by each page on top of this.
export const pageChromeCss = `  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: var(--body);
    color: var(--cream-dim);
    -webkit-font-smoothing: antialiased;
    background:
      radial-gradient(1000px 460px at 50% -12%, var(--butter-glow), transparent 62%),
      var(--ground);
    min-height: 100vh;
  }
  a { color: var(--butter); }
  .wrap { max-width: 1100px; margin: 0 auto; padding: 2.4rem 1.3rem 3rem; }
  .pill {
    display: inline-flex;
    align-items: center;
    border: 1px solid var(--rule);
    border-radius: 7px;
    background: var(--butter-glow);
    color: var(--butter);
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 0.32rem 0.6rem;
    margin-bottom: 1.1rem;
  }
  .title {
    margin: 0;
    font-family: var(--disp);
    font-weight: 560;
    font-size: clamp(1.7rem, 3vw, 2.7rem);
    letter-spacing: 0.005em;
    line-height: 1.06;
    color: var(--cream);
  }
  .title span { font-style: italic; color: var(--butter); }
  .sub {
    margin: 0.55rem 0 1.5rem;
    color: var(--cream-faint);
    font-family: var(--mono);
    letter-spacing: 0.1em;
    text-transform: uppercase;
    font-size: 0.7rem;
  }
  .card {
    border: 1px solid var(--rule);
    border-radius: 12px;
    background: var(--surface-raised);
    padding: 1.2rem 1.2rem 1.1rem;
  }
  .card h2 {
    margin: 0 0 0.7rem;
    color: var(--butter);
    font-family: var(--disp);
    font-weight: 560;
    font-size: 0.95rem;
    letter-spacing: 0.01em;
  }
  p, li { margin: 0; line-height: 1.6; font-size: 0.9rem; color: var(--cream-dim); }
  ul, ol { margin: 0; padding-left: 1.1rem; display: grid; gap: 0.5rem; }
  code { font-family: var(--mono); color: var(--butter); font-size: 0.82rem; }
  .actions { margin-top: 1rem; display: flex; flex-wrap: wrap; gap: 0.55rem; }
  .btn {
    border: 1px solid var(--rule);
    border-radius: 8px;
    color: var(--cream-dim);
    text-decoration: none;
    font-family: var(--mono);
    font-size: 0.68rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 0.5rem 0.72rem;
    display: inline-block;
    transition: border-color 0.15s, color 0.15s;
  }
  .btn:hover { border-color: var(--butter-deep); color: var(--butter); }
  .btn.primary { border-color: var(--butter); background: var(--butter); color: var(--on-butter); }
  .btn.primary:hover { filter: brightness(1.05); color: var(--on-butter); }
  .endpoint {
    color: var(--butter);
    font-family: var(--mono);
    text-decoration: none;
    overflow-wrap: anywhere;
  }
  .endpoint:hover { color: var(--cream); }
  :focus-visible { outline: 2px solid var(--butter); outline-offset: 2px; }
`;

// Pure theme-resolution runtime shared by the viewer client and the inline
// page bootstrap, so the custom-token derivation and theme resolution live in
// exactly one place. No DOM or app state — only settings in, values out.
export const themeRuntimeJs = `  var CUSTOM_FONT_PRESETS = {
    fraunces: { disp: "'Fraunces', Georgia, serif", body: "'Schibsted Grotesk', system-ui, sans-serif" },
    grotesk: { disp: "'Schibsted Grotesk', system-ui, sans-serif", body: "'Schibsted Grotesk', system-ui, sans-serif" },
    system: { disp: "ui-sans-serif, system-ui, -apple-system, 'Segoe UI', sans-serif", body: "ui-sans-serif, system-ui, -apple-system, 'Segoe UI', sans-serif" },
    typewriter: { disp: "'Fraunces', Georgia, serif", body: "'IBM Plex Mono', ui-monospace, monospace" }
  };
  var CUSTOM_FONT_KEYS = Object.keys(CUSTOM_FONT_PRESETS);
  var CUSTOM_COLOR_TOKENS = ['ground', 'ground_2', 'cream', 'cream_dim', 'butter', 'rule'];
  var VALID_THEMES = { slate: 1, paper: 1, vanilla: 1, midnight: 1, solarized: 1, ember: 1, arctic: 1, custom: 1 };

  function buildVanillaCustomTheme() {
    // Mirrors the default :root tokens so 'reset' returns to vanilla dark.
    return { ground: '#181511', ground_2: '#201c16', cream: '#f0e7d5', cream_dim: '#b5ab97', butter: '#e3c98f', rule: '#332c22', font: 'fraunces' };
  }

  var CUSTOM_HEX_RE = /^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/;
  function normalizeHex(value, fallback) {
    var v = String(value || '').trim().toLowerCase();
    if (!CUSTOM_HEX_RE.test(v)) return fallback;
    if (v.length === 4) return '#' + v[1] + v[1] + v[2] + v[2] + v[3] + v[3];
    return v;
  }
  function hexToRgb(hex) {
    var h = normalizeHex(hex, '#000000').slice(1);
    return { r: parseInt(h.slice(0, 2), 16), g: parseInt(h.slice(2, 4), 16), b: parseInt(h.slice(4, 6), 16) };
  }
  function rgbToHex(r, g, b) {
    var c = function (n) { return Math.max(0, Math.min(255, Math.round(n))).toString(16).padStart(2, '0'); };
    return '#' + c(r) + c(g) + c(b);
  }
  function mixHex(a, b, t) {
    var ca = hexToRgb(a), cb = hexToRgb(b);
    return rgbToHex(ca.r + (cb.r - ca.r) * t, ca.g + (cb.g - ca.g) * t, ca.b + (cb.b - ca.b) * t);
  }
  function rgbaOf(hex, alpha) {
    var c = hexToRgb(hex);
    return 'rgba(' + c.r + ', ' + c.g + ', ' + c.b + ', ' + alpha + ')';
  }
  // WCAG relative luminance, used for both contrast checks and dark/light sensing.
  function relLuminance(hex) {
    var c = hexToRgb(hex);
    var lin = function (v) { var s = v / 255; return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4); };
    return 0.2126 * lin(c.r) + 0.7152 * lin(c.g) + 0.0722 * lin(c.b);
  }
  function contrastRatio(a, b) {
    var la = relLuminance(a), lb = relLuminance(b);
    return (Math.max(la, lb) + 0.05) / (Math.min(la, lb) + 0.05);
  }

  function normalizeCustomTheme(raw) {
    var v = buildVanillaCustomTheme();
    var src = raw && typeof raw === 'object' ? raw : {};
    var out = { font: CUSTOM_FONT_KEYS.indexOf(src.font) >= 0 ? src.font : v.font };
    for (var i = 0; i < CUSTOM_COLOR_TOKENS.length; i++) {
      var token = CUSTOM_COLOR_TOKENS[i];
      out[token] = normalizeHex(src[token], v[token]);
    }
    return out;
  }

  // Derive every token the UI keys off from the six the user sets. ground_2/3
  // step from the surface toward the text colour; rules and faint text
  // interpolate between background and foreground, so a light or a dark base
  // both read correctly. Success/error hues aren't user-editable — they only
  // track light vs dark so status colours stay legible against any base.
  function deriveCustomTokens(themeRaw) {
    var ct = normalizeCustomTheme(themeRaw);
    var ground = ct.ground, ground2 = ct.ground_2, cream = ct.cream;
    var creamDim = ct.cream_dim, butter = ct.butter, rule = ct.rule;
    var isDark = relLuminance(ground) < 0.5;
    var font = CUSTOM_FONT_PRESETS[ct.font] || CUSTOM_FONT_PRESETS.fraunces;
    return {
      '--ground': ground,
      '--ground-2': ground2,
      '--ground-3': mixHex(ground2, cream, 0.06),
      '--rule': rule,
      '--rule-soft': mixHex(rule, ground, 0.45),
      '--rule-bright': mixHex(rule, cream, 0.3),
      '--cream': cream,
      '--cream-dim': creamDim,
      '--cream-faint': mixHex(creamDim, ground, 0.45),
      '--butter': butter,
      '--butter-deep': mixHex(butter, ground, 0.35),
      '--latte': mixHex(creamDim, butter, 0.3),
      '--sage': isDark ? '#9DB39A' : '#5F7D5C',
      '--clay': isDark ? '#C9826E' : '#A85B44',
      '--on-butter': relLuminance(butter) > 0.55 ? '#241C0D' : '#FBF4E6',
      '--butter-glow': rgbaOf(butter, 0.08),
      '--panel-bg': ground2,
      '--panel-shadow': isDark ? 'rgba(0, 0, 0, 0.5)' : 'rgba(30, 24, 14, 0.14)',
      '--surface': rgbaOf(ground2, 0.6),
      '--surface-raised': mixHex(ground2, cream, 0.04),
      '--toast-bg': mixHex(ground2, cream, 0.04),
      '--card-glow': isDark ? 'rgba(0, 0, 0, 0.35)' : 'rgba(30, 24, 14, 0.1)',
      '--overlay-bg': isDark ? rgbaOf(mixHex(ground, '#000000', 0.4), 0.78) : rgbaOf(mixHex(ground, '#000000', 0.05), 0.34),
      '--disp': font.disp,
      '--body': font.body
    };
  }

  // Resolve the active data-theme value from a (possibly raw) settings object,
  // honouring theme / light_theme / theme_mode against prefers-color-scheme.
  // The custom palette is a single shared set, so it carries no -light suffix.
  function resolveThemeFromSettings(s) {
    var src = s && typeof s === 'object' ? s : {};
    var migrate = function (v) { return v === 'cyberpunk' ? 'vanilla' : v; };
    var theme = VALID_THEMES[migrate(src.theme)] ? migrate(src.theme) : 'slate';
    var lightBase = VALID_THEMES[migrate(src.light_theme)] ? migrate(src.light_theme) : 'paper';
    var mode = (src.theme_mode === 'light' || src.theme_mode === 'dark') ? src.theme_mode : 'auto';
    var light = lightBase === 'custom' ? 'custom' : lightBase + '-light';
    if (mode === 'light') return light;
    if (mode === 'dark') return theme;
    var prefersDark = !!(window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches);
    return prefersDark ? theme : light;
  }
`;

// The bootstrap body: reads the saved settings the viewer writes, resolves the
// active theme, and sets it on <html> before paint so every page matches the
// user's choice with no flash. It also wires any [data-theme-toggle] control
// (the landing nav) to flip light/dark and persist. Dependency-free and
// failure-tolerant.
export const themeBootstrapJs = `(function(){
${themeRuntimeJs}
  var KEY = 'memoryvault.viewer.settings.v1';
  function readSettings() {
    try { var raw = localStorage.getItem(KEY); return raw ? JSON.parse(raw) : null; } catch (e) { return null; }
  }
  function applyTheme() {
    var s = readSettings();
    var active = resolveThemeFromSettings(s);
    document.documentElement.setAttribute('data-theme', active);
    var el = document.getElementById('mv-custom-theme');
    if (active === 'custom') {
      var tokens = deriveCustomTokens(s && s.custom_theme);
      // html[...] outranks :root so the custom block wins regardless of where
      // this style ends up in the cascade relative to the token <style>.
      var css = 'html[data-theme="custom"]{';
      for (var k in tokens) { css += k + ':' + tokens[k] + ';'; }
      css += '}';
      if (!el) { el = document.createElement('style'); el.id = 'mv-custom-theme'; document.head.appendChild(el); }
      el.textContent = css;
    } else if (el) {
      el.textContent = '';
    }
  }
  try { applyTheme(); } catch (e) {}
  document.addEventListener('DOMContentLoaded', function () {
    var toggles = document.querySelectorAll('[data-theme-toggle]');
    for (var i = 0; i < toggles.length; i++) {
      toggles[i].addEventListener('click', function () {
        try {
          var s = readSettings() || {};
          // Flip to the opposite of whatever is showing now.
          s.theme_mode = /-light$/.test(resolveThemeFromSettings(s)) ? 'dark' : 'light';
          localStorage.setItem(KEY, JSON.stringify(s));
          applyTheme();
        } catch (e) {}
      });
    }
  });
})();`;

// Served from /theme-bootstrap.js because the page CSP forbids inline scripts.
// Parser-blocking in <head> so the theme is set before the body paints.
export const themeBootstrapTag = `<script src="/theme-bootstrap.js"></script>`;
