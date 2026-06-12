export const rootVariables = `  :root {
    --bg: #080c10;
    --bg2: #0d1219;
    --bg3: #111820;
    --border: #1e2d3d;
    --border-bright: #2a4060;
    --amber: #f0a500;
    --amber-dim: #7a5200;
    --amber-glow: rgba(240,165,0,0.12);
    --teal: #00c8b4;
    --red: #e05050;
    --text: #c8d8e8;
    --text-dim: #4a6070;
    --text-bright: #e8f4ff;
    --mono: 'Share Tech Mono', monospace;
    --sans: 'Syne', sans-serif;
    --overlay-bg: rgba(6, 10, 15, 0.84);
    --panel-bg: rgba(13, 18, 25, 0.98);
    --panel-shadow: rgba(0, 0, 0, 0.42);
    --surface: rgba(8, 12, 16, 0.64);
    --surface-raised: rgba(10, 15, 21, 0.8);
    --toast-bg: rgba(13, 18, 25, 0.96);
    --card-glow: rgba(0, 0, 0, 0.28);
    --success: #2eca75;
    --info: #66a9ff;
    --journal: #8888ff;
    --causes: #ff9e4f;
  }

  /* ── LIGHT VARIANTS ── */
`;

export const themeStyles = `  [data-theme="cyberpunk-light"] {
    --bg: #f5f5f5;
    --bg2: #ffffff;
    --bg3: #e8ecf0;
    --border: #d0d5dc;
    --border-bright: #b0b8c4;
    --amber: #c07800;
    --amber-dim: #a06800;
    --amber-glow: rgba(192,120,0,0.10);
    --teal: #008878;
    --red: #c03030;
    --text: #2c3e50;
    --text-dim: #7a8a9a;
    --text-bright: #1a1a2e;
  }
  [data-theme="cyberpunk-light"] body {
    background: linear-gradient(180deg, #f0f2f5 0%, #e4e8ec 100%);
  }
  [data-theme="midnight-light"] {
    --bg: #f2f0fa;
    --bg2: #ffffff;
    --bg3: #eae6f6;
    --border: #d0cce4;
    --border-bright: #b8b2d8;
    --amber: #6050d0;
    --amber-dim: #4a3fb0;
    --amber-glow: rgba(96,80,208,0.10);
    --teal: #4090e0;
    --red: #d04060;
    --text: #302c54;
    --text-dim: #7878a8;
    --text-bright: #1a1640;
  }
  [data-theme="midnight-light"] body {
    background: linear-gradient(180deg, #f0eef8 0%, #e6e2f2 100%);
  }
  [data-theme="solarized-light"] {
    --bg: #fdf6e3;
    --bg2: #eee8d5;
    --bg3: #e6dfc8;
    --border: #d4c8a8;
    --border-bright: #c0b490;
    --amber: #b58900;
    --amber-dim: #7a5c00;
    --amber-glow: rgba(181,137,0,0.10);
    --teal: #2aa198;
    --red: #dc322f;
    --text: #657b83;
    --text-dim: #93a1a1;
    --text-bright: #073642;
  }
  [data-theme="solarized-light"] body {
    background: linear-gradient(180deg, #fdf6e3 0%, #f5edd6 100%);
  }
  [data-theme="ember-light"] {
    --bg: #fdf4ee;
    --bg2: #ffffff;
    --bg3: #f5eae2;
    --border: #e0ccc2;
    --border-bright: #cbb4a6;
    --amber: #d05020;
    --amber-dim: #a84420;
    --amber-glow: rgba(208,80,32,0.10);
    --teal: #d09030;
    --red: #c02020;
    --text: #3e2820;
    --text-dim: #8a7068;
    --text-bright: #1a0a04;
  }
  [data-theme="ember-light"] body {
    background: linear-gradient(180deg, #fdf4ee 0%, #f5ece4 100%);
  }
  [data-theme="arctic-light"] {
    --bg: #f0f7fc;
    --bg2: #ffffff;
    --bg3: #e4eef6;
    --border: #c8d8e8;
    --border-bright: #a8c0d8;
    --amber: #1898b0;
    --amber-dim: #107088;
    --amber-glow: rgba(24,152,176,0.10);
    --teal: #30a898;
    --red: #d04868;
    --text: #1e3850;
    --text-dim: #607888;
    --text-bright: #0c2030;
  }
  [data-theme="arctic-light"] body {
    background: linear-gradient(180deg, #eef5fc 0%, #e4edf6 100%);
  }
  [data-theme$="-light"] {
    --overlay-bg: rgba(0, 0, 0, 0.32);
    --panel-bg: var(--bg2);
    --panel-shadow: rgba(0, 0, 0, 0.12);
    --surface: var(--bg3);
    --surface-raised: var(--bg2);
    --toast-bg: var(--bg2);
    --card-glow: rgba(0, 0, 0, 0.10);
    --success: #1a9a55;
    --info: #3070d0;
    --journal: #6060cc;
    --causes: #c07030;
  }
  [data-theme$="-light"] body::before { display: none; }
  [data-theme$="-light"] body::after { display: none; }
  [data-theme$="-light"] .login-box,
  [data-theme$="-light"] .settings-folder {
    background: var(--bg2);
    box-shadow: 0 1px 4px rgba(0,0,0,0.08);
  }
  [data-theme$="-light"] .card {
    background: var(--bg2);
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  [data-theme$="-light"] .settings-folder[open] {
    background: var(--bg2);
  }
  [data-theme$="-light"] .setting-row {
    background: var(--bg3);
  }
  [data-theme$="-light"] .cmd-box,
  [data-theme$="-light"] .shortcuts-box,
  [data-theme$="-light"] .settings-box,
  [data-theme$="-light"] .changelog-box {
    background: var(--bg2);
    box-shadow: 0 8px 32px rgba(0,0,0,0.12);
  }
  [data-theme$="-light"] .expand-overlay {
    background: rgba(255,255,255,0.8);
  }
  [data-theme$="-light"] .expand-box {
    background: var(--bg2);
    box-shadow: 0 8px 32px rgba(0,0,0,0.10);
  }
  [data-theme$="-light"] .update-banner {
    background: rgba(192,120,0,0.06);
    border-color: var(--amber-dim);
  }
  [data-theme$="-light"] .update-banner-item {
    background: var(--bg3);
  }
  [data-theme$="-light"] .toast {
    background: var(--bg2);
    box-shadow: 0 4px 16px rgba(0,0,0,0.10);
  }
  [data-theme$="-light"] .card::before {
    background: linear-gradient(110deg, transparent 0%, rgba(0,0,0,0.03) 48%, transparent 72%);
  }
  [data-theme$="-light"] .graph-search-input,
  [data-theme$="-light"] .graph-btn,
  [data-theme$="-light"] .graph-legend-item {
    background: var(--bg2);
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }

  /* ── THEME: MIDNIGHT ── */
  [data-theme="midnight"] {
    --bg: #0a0a1a;
    --bg2: #10102a;
    --bg3: #16163a;
    --border: #2a2a5a;
    --border-bright: #3c3c7a;
    --amber: #7c6aff;
    --amber-dim: #4a3fb0;
    --amber-glow: rgba(124,106,255,0.12);
    --teal: #60ddff;
    --red: #ff5a7a;
    --text: #c8ccf0;
    --text-dim: #5a5e8a;
    --text-bright: #e8eaff;
  }
  [data-theme="midnight"] body {
    background:
      radial-gradient(circle at 20% 20%, rgba(124,106,255,0.08), transparent 40%),
      radial-gradient(circle at 80% 80%, rgba(96,221,255,0.06), transparent 40%),
      linear-gradient(180deg, #0a0a1a 0%, #060614 100%);
  }

  /* ── THEME: SOLARIZED ── */
  [data-theme="solarized"] {
    --bg: #002b36;
    --bg2: #073642;
    --bg3: #0a3f4c;
    --border: #1a5a68;
    --border-bright: #2a7a88;
    --amber: #b58900;
    --amber-dim: #7a5c00;
    --amber-glow: rgba(181,137,0,0.12);
    --teal: #2aa198;
    --red: #dc322f;
    --text: #93a1a1;
    --text-dim: #586e75;
    --text-bright: #eee8d5;
  }
  [data-theme="solarized"] body {
    background: linear-gradient(180deg, #002b36 0%, #001f28 100%);
  }

  /* ── THEME: EMBER ── */
  [data-theme="ember"] {
    --bg: #1a0a08;
    --bg2: #241210;
    --bg3: #2e1a16;
    --border: #4a2a22;
    --border-bright: #6a3a30;
    --amber: #ff6b35;
    --amber-dim: #a84420;
    --amber-glow: rgba(255,107,53,0.12);
    --teal: #ffb347;
    --red: #ff4444;
    --text: #e8d0c8;
    --text-dim: #7a5a50;
    --text-bright: #fff0e8;
  }
  [data-theme="ember"] body {
    background:
      radial-gradient(circle at 30% 70%, rgba(255,107,53,0.08), transparent 40%),
      radial-gradient(circle at 70% 20%, rgba(255,179,71,0.06), transparent 40%),
      linear-gradient(180deg, #1a0a08 0%, #120604 100%);
  }

  /* ── THEME: ARCTIC ── */
  [data-theme="arctic"] {
    --bg: #0c1820;
    --bg2: #122430;
    --bg3: #183040;
    --border: #284860;
    --border-bright: #386080;
    --amber: #40c8e0;
    --amber-dim: #2090a8;
    --amber-glow: rgba(64,200,224,0.12);
    --teal: #80e8d0;
    --red: #ff6080;
    --text: #c0dce8;
    --text-dim: #506878;
    --text-bright: #e0f4ff;
  }
  [data-theme="arctic"] body {
    background:
      radial-gradient(circle at 50% 0%, rgba(64,200,224,0.10), transparent 50%),
      radial-gradient(circle at 20% 80%, rgba(128,232,208,0.06), transparent 40%),
      linear-gradient(180deg, #0c1820 0%, #081018 100%);
  }

`;
