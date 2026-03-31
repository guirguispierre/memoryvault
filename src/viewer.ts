import { SERVER_VERSION } from './constants.js';
import { escapeHtml } from './utils.js';

export function viewerHtml(): string {
  return `<!DOCTYPE html>
<html lang="en" data-theme="cyberpunk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MEMORY VAULT</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<style>
  :root {
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
  }

  /* ── THEME: LIGHT ── */
  [data-theme="light"] {
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
  [data-theme="light"] body {
    background: linear-gradient(180deg, #f0f2f5 0%, #e4e8ec 100%);
  }
  [data-theme="light"] body::before { display: none; }
  [data-theme="light"] body::after { display: none; }
  [data-theme="light"] .login-box,
  [data-theme="light"] .settings-folder {
    background: var(--bg2);
    box-shadow: 0 1px 4px rgba(0,0,0,0.08);
  }
  [data-theme="light"] .card {
    background: var(--bg2);
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  [data-theme="light"] .settings-folder[open] {
    background: var(--bg2);
  }
  [data-theme="light"] .setting-row {
    background: var(--bg3);
  }
  [data-theme="light"] .cmd-box,
  [data-theme="light"] .shortcuts-box,
  [data-theme="light"] .settings-box,
  [data-theme="light"] .changelog-box {
    background: var(--bg2);
    box-shadow: 0 8px 32px rgba(0,0,0,0.12);
  }
  [data-theme="light"] .expand-overlay {
    background: rgba(245,245,245,0.92);
  }
  [data-theme="light"] .expand-box {
    background: var(--bg2);
    box-shadow: 0 8px 32px rgba(0,0,0,0.10);
  }
  [data-theme="light"] .update-banner {
    background: rgba(192,120,0,0.06);
    border-color: var(--amber-dim);
  }
  [data-theme="light"] .update-banner-item {
    background: var(--bg3);
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

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background:
      radial-gradient(circle at 16% 18%, rgba(0, 200, 180, 0.08), transparent 36%),
      radial-gradient(circle at 84% 8%, rgba(240, 165, 0, 0.08), transparent 34%),
      linear-gradient(180deg, var(--bg) 0%, #06090d 100%);
    color: var(--text);
    font-family: var(--mono);
    min-height: 100vh;
    overflow-x: hidden;
    position: relative;
  }

  .stat-pill, .refresh-btn, .logout-btn, .login-btn, .card, .connection-chip, .expand-close {
    touch-action: manipulation;
  }

  /* Scanline overlay */
  body::before {
    content: '';
    position: fixed;
    inset: 0;
    background: repeating-linear-gradient(
      0deg,
      transparent,
      transparent 2px,
      rgba(0,0,0,0.08) 2px,
      rgba(0,0,0,0.08) 4px
    );
    pointer-events: none;
    z-index: 9999;
    animation: scanlineDrift 14s linear infinite;
  }

  body::after {
    content: '';
    position: fixed;
    inset: -20%;
    z-index: -1;
    pointer-events: none;
    background:
      radial-gradient(42% 36% at 72% 18%, rgba(0, 200, 180, 0.12), transparent 70%),
      radial-gradient(45% 40% at 20% 72%, rgba(240, 165, 0, 0.1), transparent 70%);
    animation: ambientShift 18s ease-in-out infinite alternate;
  }
  body.scanlines-off::before {
    display: none;
  }
  body.motion-reduced *,
  body.motion-reduced *::before,
  body.motion-reduced *::after {
    animation: none !important;
    transition: none !important;
  }

  /* ── LOGIN SCREEN ── */
  #login-screen {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    min-height: 100vh;
    padding: 2rem;
    animation: fadeIn 0.6s ease;
  }
  .login-box {
    width: 100%;
    max-width: 420px;
    border: 1px solid var(--border-bright);
    background: var(--bg2);
    padding: 3rem 2.5rem;
    position: relative;
    animation: vaultEnter 0.85s cubic-bezier(.18,.79,.26,.99);
  }
  .login-box::before {
    content: 'CLASSIFIED';
    position: absolute;
    top: -1px; left: 2rem;
    background: var(--amber);
    color: var(--bg);
    font-family: var(--mono);
    font-size: 0.6rem;
    font-weight: 700;
    letter-spacing: 0.2em;
    padding: 0.2rem 0.6rem;
  }
  .login-box::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, var(--amber), transparent);
    background-size: 220% 100%;
    animation: lineSweep 2.4s linear infinite;
  }
  .vault-logo {
    display: flex;
    align-items: baseline;
    justify-content: center;
    flex-wrap: wrap;
    gap: 0.1em;
    font-family: var(--sans);
    font-weight: 800;
    font-size: clamp(1.55rem, 7vw, 2.2rem);
    line-height: 1.05;
    letter-spacing: -0.02em;
    color: var(--text-bright);
    margin-bottom: 0.3rem;
    text-align: center;
    animation: logoReveal 0.75s ease-out both;
  }
  .vault-logo .vault-accent { color: var(--amber); }
  .vault-sub {
    font-size: 0.68rem;
    letter-spacing: 0.2em;
    color: var(--text-dim);
    text-transform: uppercase;
    margin-bottom: 2.5rem;
    text-align: center;
  }
  .field-label {
    font-size: 0.65rem;
    letter-spacing: 0.18em;
    color: var(--amber);
    text-transform: uppercase;
    margin-bottom: 0.5rem;
  }
  .token-input {
    width: 100%;
    background: var(--bg3);
    border: 1px solid var(--border);
    color: var(--teal);
    font-family: var(--mono);
    font-size: 0.85rem;
    padding: 0.75rem 1rem;
    outline: none;
    transition: border-color 0.2s;
    letter-spacing: 0.05em;
  }
  .token-input:focus { border-color: var(--amber); }
  .token-input::placeholder { color: var(--text-dim); }
  .login-btn-row {
    display: flex;
    gap: 0.6rem;
    margin-top: 1.1rem;
  }
  .login-btn {
    width: 100%;
    margin-top: 0;
    background: var(--amber);
    color: var(--bg);
    border: none;
    font-family: var(--mono);
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    padding: 0.9rem;
    cursor: pointer;
    transition: background 0.15s, transform 0.1s;
  }
  .login-btn:hover { background: #ffbc20; }
  .login-btn.secondary {
    background: transparent;
    color: var(--text);
    border: 1px solid var(--border-bright);
  }
  .login-btn.secondary:hover {
    background: var(--bg3);
    color: var(--text-bright);
  }
  .token-btn { margin-top: 0.75rem; }
  .login-btn:active { transform: scale(0.99); }
  .login-error {
    margin-top: 1rem;
    font-size: 0.7rem;
    color: var(--red);
    letter-spacing: 0.1em;
    display: none;
  }

  /* ── MAIN APP ── */
  #app { display: none; flex-direction: column; min-height: 100vh; animation: appEnter 0.45s ease; }

  /* Header */
  .hdr {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 1rem 2rem;
    border-bottom: 1px solid var(--border);
    background: var(--bg2);
    position: sticky;
    top: 0;
    z-index: 100;
    backdrop-filter: blur(4px);
  }
  .hdr-brand {
    font-family: var(--sans);
    font-weight: 800;
    font-size: 1.2rem;
    letter-spacing: -0.02em;
    color: var(--text-bright);
    position: absolute;
    left: 50%;
    transform: translateX(-50%);
    pointer-events: none;
    animation: textGlow 5s ease-in-out infinite;
  }
  .hdr-brand span { color: var(--amber); }
  .hdr-right {
    margin-left: auto;
    display: flex;
    align-items: center;
  }
  .hdr-meta {
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    color: var(--text-dim);
    text-align: right;
  }
  .hdr-meta strong { color: var(--amber); }
  .logout-btn {
    background: none;
    border: 1px solid var(--border);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.65rem;
    letter-spacing: 0.12em;
    padding: 0.35rem 0.8rem;
    cursor: pointer;
    transition: border-color 0.2s, color 0.2s, transform 0.2s;
    margin-left: 1.5rem;
    text-transform: uppercase;
  }
  .logout-btn:hover { border-color: var(--red); color: var(--red); transform: translateY(-1px); }

  /* Stats bar */
  .stats-bar {
    display: flex;
    gap: 1px;
    background: var(--border);
    border-bottom: 1px solid var(--border);
  }
  .stat-pill {
    flex: 1;
    padding: 0.6rem 1.5rem;
    background: var(--bg2);
    text-align: center;
    cursor: pointer;
    transition: background 0.15s, transform 0.2s, box-shadow 0.2s;
    position: relative;
    transform: translateY(0);
  }
  .stat-pill:hover, .stat-pill.active { background: var(--bg3); transform: translateY(-2px); box-shadow: inset 0 -1px 0 rgba(255,255,255,0.04); }
  .stat-pill.active::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 2px;
    background: var(--amber);
  }
  .stat-num {
    font-family: var(--sans);
    font-size: 1.6rem;
    font-weight: 800;
    color: var(--amber);
    line-height: 1;
    transition: transform 0.25s ease;
  }
  .stat-pill.pulse .stat-num { animation: countPulse 0.45s ease; }
  .stat-label {
    font-size: 0.6rem;
    letter-spacing: 0.18em;
    color: var(--text-dim);
    text-transform: uppercase;
    margin-top: 0.2rem;
  }

  /* Controls */
  .controls {
    display: flex;
    gap: 0.75rem;
    padding: 1rem 2rem;
    background: var(--bg2);
    border-bottom: 1px solid var(--border);
    flex-wrap: wrap;
    align-items: center;
  }
  .search-wrap {
    flex: 1;
    min-width: 200px;
    position: relative;
  }
  .search-wrap::before {
    content: '//';
    position: absolute;
    left: 0.75rem;
    top: 50%;
    transform: translateY(-50%);
    color: var(--amber);
    font-size: 0.75rem;
    pointer-events: none;
  }
  .search-input {
    width: 100%;
    background: var(--bg3);
    border: 1px solid var(--border);
    color: var(--text);
    font-family: var(--mono);
    font-size: 0.8rem;
    padding: 0.55rem 0.75rem 0.55rem 2.2rem;
    outline: none;
    transition: border-color 0.2s;
  }
  .search-input:focus { border-color: var(--amber); }
  .search-input::placeholder { color: var(--text-dim); }
  .filter-btn {
    background: var(--bg3);
    border: 1px solid var(--border);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.65rem;
    letter-spacing: 0.12em;
    padding: 0.55rem 1rem;
    cursor: pointer;
    text-transform: uppercase;
    transition: all 0.15s;
  }
  .filter-btn:hover { border-color: var(--amber-dim); color: var(--text); }
  .filter-btn.active { border-color: var(--amber); color: var(--amber); background: var(--amber-glow); }
  .refresh-btn {
    background: none;
    border: 1px solid var(--border-bright);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.65rem;
    padding: 0.55rem 0.9rem;
    cursor: pointer;
    letter-spacing: 0.1em;
    transition: all 0.2s;
    text-transform: uppercase;
  }
  .refresh-btn:hover { color: var(--teal); border-color: var(--teal); }
  .refresh-btn.syncing {
    color: var(--teal);
    border-color: var(--teal);
    box-shadow: 0 0 0 1px rgba(0,200,180,0.25), 0 0 18px rgba(0,200,180,0.2);
    animation: syncPulse 0.8s ease-in-out infinite alternate;
  }
  .utility-btn {
    border-color: var(--border);
    color: var(--text-dim);
  }
  .utility-btn:hover {
    border-color: var(--amber);
    color: var(--amber);
  }

  /* Memory grid */
  .grid-wrap {
    flex: 1;
    padding: 1.5rem 2rem;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 1px;
    background: var(--border);
    align-content: start;
  }
  .empty-state {
    grid-column: 1/-1;
    padding: 5rem 2rem;
    text-align: center;
    color: var(--text-dim);
    font-size: 0.75rem;
    letter-spacing: 0.15em;
    text-transform: uppercase;
  }
  .empty-state .empty-icon { font-size: 2.5rem; margin-bottom: 1rem; opacity: 0.3; }

  #graph-view {
    opacity: 0;
    transform: translateY(10px);
    transition: opacity 0.25s ease, transform 0.25s ease;
  }
  #graph-view.visible {
    opacity: 1;
    transform: translateY(0);
  }

  /* Memory card */
  .card {
    background: var(--bg2);
    padding: 1.25rem 1.5rem;
    position: relative;
    transition: background 0.2s, transform 0.2s, box-shadow 0.2s;
    animation: slideUp 0.3s ease backwards;
    cursor: default;
    overflow: hidden;
    transform: translateY(0);
  }
  .card::before {
    content: '';
    position: absolute;
    inset: 0;
    background: linear-gradient(110deg, transparent 0%, rgba(255,255,255,0.08) 48%, transparent 72%);
    transform: translateX(-140%);
    transition: transform 0.5s ease;
    pointer-events: none;
  }
  .card:hover {
    background: var(--bg3);
    transform: translateY(-3px);
    box-shadow: 0 10px 20px rgba(0, 0, 0, 0.28);
  }
  .card:hover::before { transform: translateX(140%); }
  .card-type-stripe {
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 3px;
  }
  .card[data-type="note"] .card-type-stripe { background: var(--teal); }
  .card[data-type="fact"] .card-type-stripe { background: var(--amber); }
  .card[data-type="journal"] .card-type-stripe { background: #8888ff; }

  .card-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
  }
  .card-type-badge {
    font-size: 0.55rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    padding: 0.2rem 0.5rem;
    border: 1px solid;
    flex-shrink: 0;
  }
  .card[data-type="note"] .card-type-badge { border-color: var(--teal); color: var(--teal); }
  .card[data-type="fact"] .card-type-badge { border-color: var(--amber); color: var(--amber); }
  .card[data-type="journal"] .card-type-badge { border-color: #8888ff; color: #8888ff; }

  .card-title {
    font-family: var(--sans);
    font-size: 0.9rem;
    font-weight: 700;
    color: var(--text-bright);
    letter-spacing: -0.01em;
    line-height: 1.3;
    word-break: break-word;
  }
  .card-key {
    font-size: 0.7rem;
    color: var(--amber);
    letter-spacing: 0.08em;
    margin-bottom: 0.5rem;
  }
  .card-key span { color: var(--text-dim); }
  .card-content {
    font-size: 0.78rem;
    color: var(--text);
    line-height: 1.65;
    word-break: break-word;
    white-space: pre-wrap;
    max-height: 120px;
    overflow: hidden;
    position: relative;
  }
  .card-content::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 40px;
    background: linear-gradient(transparent, var(--bg2));
    pointer-events: none;
  }
  .card:hover .card-content::after {
    background: linear-gradient(transparent, var(--bg3));
  }
  .card-footer {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    margin-top: 1rem;
    padding-top: 0.75rem;
    border-top: 1px solid var(--border);
    gap: 0.6rem;
  }
  .card-meta {
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
    min-width: 0;
  }
  .card-tags {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
  }
  .card-quality {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
  }
  .quality-chip {
    font-size: 0.52rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    border: 1px solid var(--border);
    color: var(--text-dim);
    background: rgba(8, 12, 16, 0.7);
    padding: 0.12rem 0.34rem;
  }
  .quality-chip.conf { border-color: #66a9ff; color: #66a9ff; }
  .quality-chip.imp { border-color: var(--amber); color: var(--amber); }
  .quality-chip.src { border-color: var(--teal); color: var(--teal); }
  .tag {
    font-size: 0.55rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    background: var(--bg);
    border: 1px solid var(--border);
    color: var(--text-dim);
    padding: 0.15rem 0.4rem;
  }
  .card-date {
    font-size: 0.6rem;
    color: var(--text-dim);
    letter-spacing: 0.05em;
    white-space: nowrap;
    flex-shrink: 0;
  }
  .card-id {
    font-size: 0.55rem;
    color: var(--text-dim);
    opacity: 0.5;
    letter-spacing: 0.04em;
    margin-top: 0.3rem;
  }

  /* Expand overlay */
  .expand-overlay {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(4,8,14,0.92);
    z-index: 200;
    padding: 2rem;
    overflow-y: auto;
    animation: fadeIn 0.2s ease;
  }
  .expand-overlay.open { display: flex; align-items: flex-start; justify-content: center; }
  .expand-box {
    width: 100%;
    max-width: 680px;
    background: var(--bg2);
    border: 1px solid var(--border-bright);
    padding: 2rem;
    position: relative;
    margin-top: 3rem;
    animation: slideUp 0.25s ease;
  }
  .expand-close {
    position: absolute;
    top: 1rem; right: 1rem;
    background: none;
    border: 1px solid var(--border);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.7rem;
    padding: 0.3rem 0.6rem;
    cursor: pointer;
    transition: all 0.15s;
    letter-spacing: 0.1em;
  }
  .expand-close:hover { border-color: var(--red); color: var(--red); }
  .expand-content {
    font-size: 0.82rem;
    color: var(--text);
    line-height: 1.75;
    white-space: pre-wrap;
    word-break: break-word;
    margin-top: 1rem;
  }

  /* Loading */
  .loading {
    grid-column: 1/-1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 4rem;
    gap: 0.5rem;
    color: var(--amber);
    font-size: 0.7rem;
    letter-spacing: 0.2em;
  }
  .loading-dot {
    width: 4px; height: 4px;
    background: var(--amber);
    border-radius: 50%;
    animation: blink 1s infinite;
  }
  .loading-dot:nth-child(2) { animation-delay: 0.2s; }
  .loading-dot:nth-child(3) { animation-delay: 0.4s; }

  /* Footer */
  .footer {
    padding: 0.75rem 2rem;
    border-top: 1px solid var(--border);
    background: var(--bg2);
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .footer-text { font-size: 0.6rem; color: var(--text-dim); letter-spacing: 0.1em; text-transform: uppercase; }
  .cursor-blink {
    display: inline-block;
    width: 7px; height: 13px;
    background: var(--amber);
    margin-left: 3px;
    vertical-align: middle;
    animation: blink 1s infinite;
  }

  .card-links-badge {
    font-size: 0.58rem;
    letter-spacing: 0.1em;
    color: var(--teal);
    border: 1px solid var(--teal);
    padding: 0.15rem 0.4rem;
    opacity: 0.8;
  }
  .connections-section { margin-top: 1.5rem; padding-top: 1rem; border-top: 1px solid var(--border); }
  .connections-title { font-size: 0.6rem; letter-spacing: 0.2em; color: var(--amber); text-transform: uppercase; margin-bottom: 0.75rem; }
  .connection-chip {
    display: inline-flex; align-items: center; gap: 0.4rem;
    background: var(--bg3); border: 1px solid var(--border);
    padding: 0.35rem 0.7rem; margin: 0.25rem 0.25rem 0.25rem 0;
    cursor: pointer; transition: border-color 0.15s, color 0.15s, transform 0.15s;
    font-size: 0.72rem; color: var(--text);
  }
  .connection-chip:hover { border-color: var(--amber); color: var(--amber); transform: translateX(2px); }

  .live-dot {
    display: inline-block;
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--teal);
    margin-right: 4px;
    box-shadow: 0 0 0 rgba(0, 200, 180, 0.4);
    animation: livePulse 1.9s infinite;
  }
  .connection-chip .chip-type { font-size: 0.55rem; letter-spacing: 0.15em; text-transform: uppercase; opacity: 0.6; }
  .connection-chip .chip-label { font-size: 0.6rem; color: var(--text-dim); font-style: italic; }
  .connection-chip .chip-relation {
    font-size: 0.5rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    border: 1px solid var(--border-bright);
    color: var(--teal);
    padding: 0.12rem 0.3rem;
  }
  .connection-chip .chip-relation.contradicts { border-color: var(--red); color: var(--red); }
  .connection-chip .chip-relation.supersedes { border-color: var(--amber); color: var(--amber); }
  .connection-chip .chip-relation.supports { border-color: #2eca75; color: #2eca75; }
  .graph-node circle { stroke-width: 2px; cursor: pointer; transition: r 0.15s, opacity 0.18s, stroke-opacity 0.18s; }
  .graph-node circle:hover { r: 10; }
  .graph-node text { font-family: var(--mono); font-size: 10px; fill: var(--text); pointer-events: none; transition: opacity 0.18s; }
  .graph-link { stroke-width: 1.5px; transition: stroke-opacity 0.18s; }
  .graph-link.explicit { stroke: var(--border-bright); opacity: 0.9; }
  .graph-link.explicit.relation-related { stroke: var(--border-bright); }
  .graph-link.explicit.relation-supports { stroke: #2eca75; }
  .graph-link.explicit.relation-contradicts { stroke: var(--red); stroke-dasharray: 6 3; }
  .graph-link.explicit.relation-supersedes { stroke: var(--amber); }
  .graph-link.explicit.relation-causes { stroke: #ff9e4f; }
  .graph-link.explicit.relation-example-of { stroke: #66a9ff; }
  .graph-link.inferred { stroke: var(--teal); opacity: 0.4; stroke-dasharray: 4 4; }
  .graph-link-label { font-family: var(--mono); font-size: 9px; fill: var(--text-dim); pointer-events: none; }
  .graph-toolbar-row {
    display: flex;
    gap: 0.35rem;
    flex-wrap: wrap;
    justify-content: flex-end;
    width: 100%;
  }
  .graph-search-input {
    min-width: 150px;
    background: rgba(8, 12, 16, 0.9);
    border: 1px solid var(--border-bright);
    color: var(--text);
    font-family: var(--mono);
    font-size: 0.58rem;
    letter-spacing: 0.1em;
    padding: 0.35rem 0.5rem;
    min-height: 30px;
    outline: none;
  }
  .graph-search-input:focus { border-color: var(--teal); }
  .graph-search-input::placeholder { color: var(--text-dim); }
  .graph-btn.relation { border-color: var(--border); color: var(--text-dim); }
  .graph-btn.relation.active { border-color: var(--amber); color: var(--amber); }
  .graph-btn.relation.off { opacity: 0.55; }
  .graph-toolbar {
    position: absolute;
    top: 0.75rem;
    right: 0.75rem;
    z-index: 8;
    display: flex;
    gap: 0.4rem;
    flex-direction: column;
    align-items: flex-end;
    max-width: calc(100% - 1.5rem);
  }
  .graph-btn {
    border: 1px solid var(--border-bright);
    background: rgba(8, 12, 16, 0.9);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.58rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 0.35rem 0.5rem;
    cursor: pointer;
    min-height: 30px;
  }
  .graph-btn:hover { border-color: var(--amber); color: var(--amber); }
  .graph-btn.active { color: var(--teal); border-color: var(--teal); }
  .graph-btn.off { opacity: 0.6; border-color: var(--border); color: var(--text-dim); }
  .graph-legend {
    position: absolute;
    left: 0.75rem;
    bottom: 0.75rem;
    z-index: 8;
    display: flex;
    gap: 0.5rem;
    flex-wrap: wrap;
    max-width: calc(100% - 1.5rem);
  }
  .graph-legend-item {
    font-size: 0.55rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    border: 1px solid var(--border);
    background: rgba(8, 12, 16, 0.9);
    color: var(--text-dim);
    padding: 0.2rem 0.45rem;
  }
  .toast-wrap {
    position: fixed;
    right: 0.85rem;
    bottom: 0.85rem;
    display: flex;
    flex-direction: column;
    gap: 0.45rem;
    z-index: 320;
    pointer-events: none;
  }
  .toast {
    border: 1px solid var(--border-bright);
    background: rgba(13, 18, 25, 0.96);
    color: var(--text);
    font-size: 0.68rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 0.45rem 0.6rem;
    min-width: 190px;
    max-width: min(80vw, 420px);
    line-height: 1.45;
    box-shadow: 0 10px 22px rgba(0, 0, 0, 0.28);
    animation: toastIn 0.2s ease;
  }
  .toast.info { border-color: var(--border-bright); color: var(--text); }
  .toast.success { border-color: var(--teal); color: var(--teal); }
  .toast.error { border-color: var(--red); color: var(--red); }
  .toast.hide { animation: toastOut 0.2s ease forwards; }

  /* ── UPDATE BANNER ── */
  .update-banner {
    display: none;
    align-items: center;
    gap: 0.6rem;
    padding: 0.55rem 0.8rem;
    margin: 0 0.6rem;
    border: 1px solid var(--amber-dim);
    border-left: 3px solid var(--amber);
    background: var(--amber-glow);
    font-family: var(--mono);
    font-size: 0.68rem;
    letter-spacing: 0.06em;
    color: var(--text);
    line-height: 1.5;
    animation: bannerSlide 0.3s ease;
  }
  .update-banner.visible { display: flex; }
  .update-banner-icon {
    font-size: 1rem;
    flex-shrink: 0;
    color: var(--amber);
  }
  .update-banner-body { flex: 1; min-width: 0; }
  .update-banner-title {
    font-weight: 700;
    color: var(--amber);
    text-transform: uppercase;
    margin-bottom: 0.15rem;
  }
  .update-banner-items {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem 0.6rem;
    margin-top: 0.25rem;
  }
  .update-banner-item {
    display: inline-flex;
    align-items: center;
    gap: 0.25rem;
    padding: 0.12rem 0.35rem;
    border: 1px solid var(--border);
    background: var(--bg2);
    font-size: 0.62rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--text-dim);
  }
  .update-banner-item .badge {
    font-size: 0.55rem;
    padding: 0.05rem 0.2rem;
    background: var(--amber);
    color: var(--bg);
    font-weight: 700;
    letter-spacing: 0.05em;
  }
  .update-banner-actions {
    display: flex;
    gap: 0.4rem;
    flex-shrink: 0;
    align-items: center;
  }
  .update-banner-btn {
    all: unset;
    cursor: pointer;
    font-family: var(--mono);
    font-size: 0.6rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.3rem 0.55rem;
    border: 1px solid var(--border-bright);
    color: var(--text);
    transition: border-color 0.15s, color 0.15s;
  }
  .update-banner-btn:hover {
    border-color: var(--amber);
    color: var(--amber);
  }
  .update-banner-dismiss {
    all: unset;
    cursor: pointer;
    font-size: 0.9rem;
    color: var(--text-dim);
    padding: 0.15rem 0.3rem;
    line-height: 1;
    transition: color 0.15s;
  }
  .update-banner-dismiss:hover { color: var(--text-bright); }
  @keyframes bannerSlide {
    from { opacity: 0; transform: translateY(-8px); }
    to   { opacity: 1; transform: translateY(0); }
  }

  .cmd-overlay {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 300;
    background: rgba(6, 10, 15, 0.86);
    padding: 6vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .cmd-overlay.open { display: flex; }
  .cmd-box {
    width: min(700px, 100%);
    border: 1px solid var(--border-bright);
    background: rgba(13, 18, 25, 0.98);
    box-shadow: 0 26px 50px rgba(0, 0, 0, 0.45);
  }
  .cmd-head {
    padding: 0.8rem;
    border-bottom: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    gap: 0.45rem;
  }
  .cmd-input {
    width: 100%;
    border: 1px solid var(--border);
    background: var(--bg3);
    color: var(--teal);
    font-family: var(--mono);
    font-size: 0.82rem;
    letter-spacing: 0.06em;
    padding: 0.6rem 0.72rem;
    outline: none;
  }
  .cmd-input:focus { border-color: var(--amber); }
  .cmd-input::placeholder { color: var(--text-dim); }
  .cmd-hint {
    color: var(--text-dim);
    font-size: 0.56rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
  }
  .cmd-list {
    max-height: min(62vh, 480px);
    overflow-y: auto;
  }
  .cmd-item {
    width: 100%;
    border: none;
    border-bottom: 1px solid var(--border);
    background: transparent;
    color: var(--text);
    text-align: left;
    cursor: pointer;
    padding: 0.66rem 0.82rem;
    display: flex;
    justify-content: space-between;
    gap: 0.65rem;
    font-family: var(--mono);
  }
  .cmd-item:hover, .cmd-item.active {
    background: rgba(240, 165, 0, 0.1);
  }
  .cmd-item-label {
    font-size: 0.75rem;
    letter-spacing: 0.04em;
    color: var(--text-bright);
  }
  .cmd-item-detail {
    font-size: 0.62rem;
    color: var(--text-dim);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    text-align: right;
  }
  .cmd-empty {
    color: var(--text-dim);
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.85rem;
  }
  .shortcuts-overlay {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 290;
    background: rgba(6, 10, 15, 0.84);
    padding: 8vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .shortcuts-overlay.open { display: flex; }
  .shortcuts-box {
    width: min(620px, 100%);
    border: 1px solid var(--border-bright);
    background: rgba(13, 18, 25, 0.98);
    box-shadow: 0 20px 42px rgba(0, 0, 0, 0.42);
    padding: 0.9rem;
  }
  .shortcuts-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.55rem;
    margin-bottom: 0.8rem;
  }
  .shortcuts-head h3 {
    color: var(--amber);
    font-size: 0.72rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    font-weight: 700;
  }
  .shortcuts-close {
    border: 1px solid var(--border);
    background: transparent;
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.62rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.25rem 0.48rem;
    cursor: pointer;
  }
  .shortcuts-close:hover { border-color: var(--amber); color: var(--amber); }
  .shortcuts-grid {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 0.5rem 0.8rem;
    align-items: center;
  }
  .shortcut-key {
    border: 1px solid var(--border);
    background: var(--bg3);
    color: var(--teal);
    font-size: 0.58rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.2rem 0.36rem;
    min-width: 88px;
    text-align: center;
  }
  .shortcut-desc {
    color: var(--text);
    font-size: 0.72rem;
    line-height: 1.45;
  }
  .settings-overlay {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 295;
    background: rgba(6, 10, 15, 0.84);
    padding: 8vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .settings-overlay.open { display: flex; }
  .settings-box {
    width: min(760px, 100%);
    max-height: min(84vh, 820px);
    border: 1px solid var(--border-bright);
    background: rgba(13, 18, 25, 0.98);
    box-shadow: 0 20px 42px rgba(0, 0, 0, 0.42);
    padding: 0.9rem;
    display: flex;
    flex-direction: column;
  }
  .settings-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.55rem;
    margin-bottom: 0.8rem;
  }
  .settings-head-main {
    display: flex;
    align-items: center;
    gap: 0.55rem;
    flex-wrap: wrap;
  }
  .settings-head h3 {
    color: var(--amber);
    font-size: 0.72rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    font-weight: 700;
  }
  .settings-version {
    border: 1px solid var(--border);
    background: var(--bg3);
    color: var(--teal);
    font-size: 0.56rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 0.18rem 0.4rem;
  }
  .settings-close {
    border: 1px solid var(--border);
    background: transparent;
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.62rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.25rem 0.48rem;
    cursor: pointer;
  }
  .settings-close:hover { border-color: var(--amber); color: var(--amber); }
  .settings-scroll {
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding-right: 0.15rem;
    margin-right: -0.15rem;
  }
  .settings-sections {
    display: flex;
    flex-direction: column;
    gap: 0.55rem;
  }
  .settings-folder {
    border: 1px solid var(--border);
    background: rgba(8, 12, 16, 0.64);
  }
  .settings-folder[open] {
    border-color: var(--border-bright);
    background: rgba(10, 15, 21, 0.8);
  }
  .settings-folder summary {
    list-style: none;
    cursor: pointer;
    padding: 0.5rem 0.62rem;
    color: var(--teal);
    font-size: 0.62rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.6rem;
  }
  .settings-folder summary::-webkit-details-marker { display: none; }
  .settings-folder summary::after {
    content: '+';
    color: var(--amber);
    font-size: 0.82rem;
    line-height: 1;
  }
  .settings-folder[open] summary::after {
    content: '-';
  }
  .settings-folder-body {
    border-top: 1px solid var(--border);
    padding: 0.55rem;
  }
  .settings-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.5rem 0.75rem;
  }
  .setting-row {
    border: 1px solid var(--border);
    background: rgba(8, 12, 16, 0.64);
    padding: 0.55rem 0.62rem;
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
  }
  .setting-row.setting-inline {
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
    gap: 0.6rem;
  }
  .setting-row.setting-span-2 { grid-column: 1 / -1; }
  .setting-row label,
  .setting-row .setting-label {
    color: var(--text);
    font-size: 0.66rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    line-height: 1.35;
  }
  .setting-row .setting-help {
    color: var(--text-dim);
    font-size: 0.58rem;
    letter-spacing: 0.08em;
    line-height: 1.35;
  }
  .setting-input {
    border: 1px solid var(--border);
    background: var(--bg3);
    color: var(--teal);
    font-family: var(--mono);
    font-size: 0.75rem;
    letter-spacing: 0.04em;
    outline: none;
    padding: 0.4rem 0.5rem;
    min-height: 30px;
  }
  .setting-input:focus { border-color: var(--amber); }
  .setting-check {
    width: 18px;
    height: 18px;
    accent-color: var(--teal);
  }
  .semantic-status-box {
    border: 1px solid var(--border);
    background: var(--bg3);
    padding: 0.55rem;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  .semantic-status-line {
    color: var(--text);
    font-size: 0.64rem;
    letter-spacing: 0.08em;
    line-height: 1.45;
    word-break: break-word;
  }
  .semantic-status-line.error { color: var(--red); }
  .semantic-status-line.dim { color: var(--text-dim); }
  .semantic-status-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 0.35rem;
  }
  .semantic-status-pill {
    border: 1px solid var(--border);
    background: rgba(8, 12, 16, 0.78);
    color: var(--teal);
    font-size: 0.54rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.14rem 0.32rem;
  }
  .semantic-status-pill.ready { color: #2eca75; border-color: rgba(46, 202, 117, 0.45); }
  .semantic-status-pill.not-ready { color: var(--amber); border-color: rgba(240, 165, 0, 0.45); }
  .semantic-status-pill.running { color: #66a9ff; border-color: rgba(102, 169, 255, 0.45); }
  .settings-actions {
    display: flex;
    gap: 0.5rem;
    justify-content: flex-end;
    flex-wrap: wrap;
    margin-top: 0.7rem;
  }
  .theme-picker {
    display: flex;
    gap: 0.5rem;
    flex-wrap: wrap;
    margin-top: 0.25rem;
  }
  .theme-swatch {
    width: 40px;
    height: 40px;
    background: transparent;
    border: 2px solid var(--border);
    cursor: pointer;
    padding: 3px;
    transition: border-color 0.15s, transform 0.1s;
    position: relative;
  }
  .theme-swatch:hover {
    border-color: var(--amber);
    transform: scale(1.1);
  }
  .theme-swatch.active {
    border-color: var(--amber);
    box-shadow: 0 0 8px var(--amber-glow);
  }
  .theme-swatch.active::after {
    content: '✓';
    position: absolute;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    color: white;
    font-size: 0.7rem;
    font-weight: 700;
    text-shadow: 0 1px 3px rgba(0,0,0,0.6);
  }
  .theme-swatch span {
    display: block;
    width: 100%;
    height: 100%;
  }
  .changelog-overlay {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 296;
    background: rgba(6, 10, 15, 0.84);
    padding: 8vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .changelog-overlay.open { display: flex; }
  .changelog-box {
    width: min(860px, 100%);
    border: 1px solid var(--border-bright);
    background: rgba(13, 18, 25, 0.98);
    box-shadow: 0 20px 42px rgba(0, 0, 0, 0.42);
    padding: 0.9rem;
  }
  .changelog-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 0.55rem;
    margin-bottom: 0.8rem;
  }
  .changelog-title-group h3 {
    color: var(--amber);
    font-size: 0.72rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    font-weight: 700;
    margin-bottom: 0.25rem;
  }
  .changelog-subtitle {
    color: var(--text-dim);
    font-size: 0.58rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    line-height: 1.4;
  }
  .changelog-list {
    border: 1px solid var(--border);
    background: rgba(8, 12, 16, 0.64);
    padding: 0.7rem;
    max-height: min(62vh, 720px);
    overflow: auto;
    display: flex;
    flex-direction: column;
    gap: 0.65rem;
  }
  .changelog-entry {
    border: 1px solid var(--border);
    background: var(--bg3);
    padding: 0.6rem;
  }
  .changelog-entry-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.45rem;
    margin-bottom: 0.35rem;
    flex-wrap: wrap;
  }
  .changelog-entry-version {
    color: var(--teal);
    font-size: 0.63rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
  }
  .changelog-entry-date {
    color: var(--text-dim);
    font-size: 0.58rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
  }
  .changelog-entry-summary {
    color: var(--text-bright);
    font-size: 0.74rem;
    line-height: 1.45;
    margin-bottom: 0.4rem;
  }
  .changelog-change-list {
    list-style: none;
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }
  .changelog-change-row {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 0.45rem;
    align-items: start;
  }
  .changelog-change-type {
    border: 1px solid var(--border);
    color: var(--amber);
    font-size: 0.54rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 0.08rem 0.28rem;
    white-space: nowrap;
  }
  .changelog-change-text {
    color: var(--text);
    font-size: 0.68rem;
    line-height: 1.45;
  }
  body.compact-cards .grid-wrap {
    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 1px;
  }
  body.compact-cards .card {
    padding: 0.95rem 1rem;
  }
  body.compact-cards .card-content {
    font-size: 0.74rem;
    max-height: 88px;
  }
  body.compact-cards .card-footer {
    margin-top: 0.65rem;
    padding-top: 0.55rem;
  }
  body.compact-cards .card-id {
    font-size: 0.5rem;
  }

  @media (max-width: 900px) {
    .hdr { padding: 0.85rem 1rem; }
    .controls { padding: 0.75rem 1rem; }
    .grid-wrap { padding: 1rem; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); }
    .footer { padding: 0.65rem 1rem; flex-wrap: wrap; gap: 0.45rem; }
  }

  @media (max-width: 640px) {
    body::before { display: none; }
    #login-screen { padding: 1rem; }
    .login-box { padding: 2rem 1rem 1.5rem; }
    .login-box::before { left: 1rem; }
    .login-btn-row { flex-direction: column; gap: 0.45rem; }
    .vault-logo { font-size: 1.65rem; }
    .vault-sub { margin-bottom: 1.5rem; font-size: 0.62rem; }
    .token-input, .search-input { font-size: 16px; }

    .hdr {
      position: static;
      flex-direction: column;
      align-items: flex-start;
      gap: 0.65rem;
      padding: 0.75rem 0.75rem 0.6rem;
    }
    .hdr-brand { font-size: 1.05rem; }
    .hdr-brand {
      position: static;
      transform: none;
      pointer-events: auto;
    }
    .hdr-right {
      width: 100%;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.6rem;
    }
    .hdr-meta { text-align: left; font-size: 0.58rem; letter-spacing: 0.08em; }
    #live-indicator { font-size: 0.54rem !important; letter-spacing: 0.12em !important; }
    .logout-btn {
      margin-left: 0;
      min-height: 38px;
      padding: 0.45rem 0.72rem;
      font-size: 0.62rem;
    }

    .stats-bar {
      overflow-x: auto;
      overflow-y: hidden;
      -webkit-overflow-scrolling: touch;
      scrollbar-width: none;
    }
    .stats-bar::-webkit-scrollbar { display: none; }
    .stat-pill {
      flex: 0 0 88px;
      padding: 0.55rem 0.4rem;
    }
    .stat-num { font-size: 1.1rem; }
    .stat-label { font-size: 0.55rem; letter-spacing: 0.14em; }

    .controls {
      flex-direction: column;
      align-items: stretch;
      padding: 0.65rem 0.75rem;
      gap: 0.55rem;
    }
    .search-wrap { min-width: 0; width: 100%; }
    .refresh-btn {
      width: 100%;
      min-height: 42px;
      font-size: 0.62rem;
    }
    .utility-btn { width: 100%; }

    #graph-view { min-height: 54vh !important; }
    #graph-svg { min-height: 54vh !important; height: 54vh !important; }
    .graph-link-label { display: none; }
    .graph-toolbar {
      top: 0.45rem;
      left: 0.45rem;
      right: 0.45rem;
      max-width: none;
      gap: 0.25rem;
      align-items: stretch;
    }
    .graph-toolbar-row { justify-content: flex-start; }
    .graph-search-input { width: 100%; min-height: 28px; }
    .graph-btn { font-size: 0.52rem; letter-spacing: 0.08em; padding: 0.3rem 0.42rem; min-height: 28px; }
    .graph-legend {
      left: 0.45rem;
      right: 0.45rem;
      bottom: 0.45rem;
      max-width: none;
      gap: 0.35rem;
    }
    .graph-legend-item { font-size: 0.5rem; letter-spacing: 0.08em; padding: 0.2rem 0.36rem; }

    .grid-wrap {
      padding: 0.5rem;
      grid-template-columns: 1fr;
      gap: 1px;
    }
    .card { padding: 1rem 1rem 0.95rem; }
    .card-content { max-height: 96px; }
    .card-footer {
      flex-direction: column;
      align-items: flex-start;
      gap: 0.45rem;
    }
    .card-date { align-self: flex-end; font-size: 0.58rem; }

    .expand-overlay {
      padding: 0;
      align-items: stretch;
    }
    .expand-box {
      margin-top: 0;
      max-width: none;
      min-height: 100vh;
      border: none;
      border-top: 1px solid var(--border-bright);
      padding: 3.25rem 1rem 1.25rem;
    }
    .expand-close {
      top: 0.65rem;
      right: 0.65rem;
      padding: 0.45rem 0.7rem;
      font-size: 0.62rem;
    }
    .expand-content { font-size: 0.8rem; line-height: 1.7; }
    .connection-chip {
      display: flex;
      width: 100%;
      margin-right: 0;
    }

    .footer { padding: 0.55rem 0.75rem; }
    .footer-text { font-size: 0.52rem; letter-spacing: 0.08em; }
    .footer .footer-text:last-child { display: none; }
    .toast-wrap { left: 0.65rem; right: 0.65rem; bottom: 0.65rem; }
    .toast { max-width: none; }
    .cmd-overlay { padding-top: 3vh; }
    .cmd-head { padding: 0.62rem; }
    .cmd-item { padding: 0.54rem 0.62rem; }
    .cmd-item-label { font-size: 0.68rem; }
    .cmd-item-detail { font-size: 0.56rem; }
    .shortcuts-overlay { padding-top: 5vh; }
    .shortcuts-box { padding: 0.62rem; }
    .shortcut-key { min-width: 74px; }
    .shortcut-desc { font-size: 0.68rem; }
    .settings-overlay { padding-top: 5vh; }
    .settings-box { padding: 0.62rem; max-height: 90vh; }
    .settings-scroll { padding-right: 0; margin-right: 0; }
    .settings-grid { grid-template-columns: 1fr; }
    .settings-actions { justify-content: stretch; }
    .settings-actions .refresh-btn { width: 100%; }
    .changelog-overlay { padding-top: 5vh; }
    .changelog-box { padding: 0.62rem; }
    .changelog-list { max-height: min(60vh, 560px); }
    .changelog-change-row { grid-template-columns: 1fr; gap: 0.25rem; }
  }
  @media (prefers-reduced-motion: reduce) {
    *, *::before, *::after {
      animation: none !important;
      transition: none !important;
    }
    #graph-view {
      opacity: 1 !important;
      transform: none !important;
    }
  }
  @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
  @keyframes appEnter { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; transform: translateY(0); } }
  @keyframes slideUp { from { opacity: 0; transform: translateY(16px); } to { opacity: 1; transform: translateY(0); } }
  @keyframes blink { 0%,100% { opacity: 1; } 50% { opacity: 0; } }
  @keyframes scanlineDrift { from { transform: translateY(0); } to { transform: translateY(12px); } }
  @keyframes ambientShift {
    0% { transform: translate3d(-2%, -1%, 0) scale(1); opacity: 0.65; }
    100% { transform: translate3d(2%, 1%, 0) scale(1.06); opacity: 1; }
  }
  @keyframes vaultEnter {
    0% { opacity: 0; transform: translateY(18px) scale(0.98); }
    100% { opacity: 1; transform: translateY(0) scale(1); }
  }
  @keyframes logoReveal {
    0% { opacity: 0; transform: translateY(8px); letter-spacing: 0.02em; }
    100% { opacity: 1; transform: translateY(0); letter-spacing: -0.02em; }
  }
  @keyframes lineSweep {
    0% { background-position: 0% 50%; }
    100% { background-position: 200% 50%; }
  }
  @keyframes textGlow {
    0%, 100% { text-shadow: 0 0 0 rgba(0, 200, 180, 0); }
    50% { text-shadow: 0 0 12px rgba(0, 200, 180, 0.2); }
  }
  @keyframes countPulse {
    0% { transform: scale(1); }
    40% { transform: scale(1.12); }
    100% { transform: scale(1); }
  }
  @keyframes syncPulse {
    0% { box-shadow: 0 0 0 1px rgba(0,200,180,0.2), 0 0 8px rgba(0,200,180,0.12); }
    100% { box-shadow: 0 0 0 1px rgba(0,200,180,0.45), 0 0 18px rgba(0,200,180,0.24); }
  }
  @keyframes livePulse {
    0% { transform: scale(1); box-shadow: 0 0 0 0 rgba(0, 200, 180, 0.35); opacity: 1; }
    70% { transform: scale(1.15); box-shadow: 0 0 0 8px rgba(0, 200, 180, 0); opacity: 0.9; }
    100% { transform: scale(1); box-shadow: 0 0 0 0 rgba(0, 200, 180, 0); opacity: 1; }
  }
  @keyframes toastIn {
    0% { opacity: 0; transform: translateY(8px); }
    100% { opacity: 1; transform: translateY(0); }
  }
  @keyframes toastOut {
    0% { opacity: 1; transform: translateY(0); }
    100% { opacity: 0; transform: translateY(8px); }
  }
</style>
</head>
<body>

<!-- LOGIN -->
<div id="login-screen">
  <div class="login-box">
    <div class="vault-logo"><span>MEMORY</span><span class="vault-accent">VAULT</span></div>
    <div class="vault-sub">Secure Access Required</div>
    <div class="field-label">Email</div>
    <input type="email" class="token-input" id="email-input" placeholder="you@example.com" autocomplete="username" autocapitalize="off" autocorrect="off" spellcheck="false">
    <div class="field-label" style="margin-top:0.75rem">Password</div>
    <input type="password" class="token-input" id="password-input" placeholder="Enter password" autocomplete="current-password">
    <div class="field-label" style="margin-top:0.75rem">Brain Name (for signup)</div>
    <input type="text" class="token-input" id="brain-name-input" placeholder="Second Brain name (optional)" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false">
    <div class="login-btn-row">
      <button class="login-btn" data-action="login">SIGN IN →</button>
      <button class="login-btn secondary" data-action="signup">SIGN UP →</button>
    </div>
    <div class="field-label" style="margin-top:1rem">Legacy Access Token</div>
    <input type="password" class="token-input" id="token-input" placeholder="Bearer token (legacy mode)" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false">
    <button class="login-btn secondary token-btn" data-action="token-login">TOKEN LOGIN →</button>
    <div class="login-error" id="login-error">⚠ ACCESS DENIED</div>
  </div>
</div>

<!-- APP -->
<div id="app">
  <header class="hdr">
    <div class="hdr-brand">MEMORY<span>VAULT</span></div>
    <div class="hdr-right">
      <div class="hdr-meta">
        <div id="hdr-count">— entries</div>
        <div id="hdr-time"></div>
      </div>
      <div id="live-indicator" style="font-size:0.6rem;letter-spacing:0.15em;color:var(--text-dim);display:none;align-items:center">
        <span class="live-dot"></span>LIVE
      </div>
      <button class="logout-btn" data-action="logout">LOCK</button>
    </div>
  </header>

  <div class="update-banner" id="update-banner">
    <div class="update-banner-icon">&#9670;</div>
    <div class="update-banner-body">
      <div class="update-banner-title" id="update-banner-title">New in v${escapeHtml(SERVER_VERSION)}</div>
      <div class="update-banner-items" id="update-banner-items"></div>
    </div>
    <div class="update-banner-actions">
      <button class="update-banner-btn" data-action="open-changelog-overlay">Details</button>
      <button class="update-banner-dismiss" data-action="dismiss-update-banner" title="Dismiss">&times;</button>
    </div>
  </div>

  <div class="stats-bar">
    <div class="stat-pill active" id="stat-all" data-action="set-filter" data-filter="">
      <div class="stat-num" id="count-all">0</div>
      <div class="stat-label">All</div>
    </div>
    <div class="stat-pill" id="stat-note" data-action="set-filter" data-filter="note">
      <div class="stat-num" id="count-note">0</div>
      <div class="stat-label">Notes</div>
    </div>
    <div class="stat-pill" id="stat-fact" data-action="set-filter" data-filter="fact">
      <div class="stat-num" id="count-fact">0</div>
      <div class="stat-label">Facts</div>
    </div>
    <div class="stat-pill" id="stat-journal" data-action="set-filter" data-filter="journal">
      <div class="stat-num" id="count-journal">0</div>
      <div class="stat-label">Journal</div>
    </div>
    <div class="stat-pill" id="stat-graph" data-action="show-graph">
      <div class="stat-num">⬡</div>
      <div class="stat-label">Graph</div>
    </div>
  </div>

  <div class="controls">
    <div class="search-wrap">
      <input type="text" class="search-input" id="search-input" placeholder="Search by name, id, key, or text..." inputmode="search">
    </div>
    <button class="refresh-btn" data-action="refresh-memories">↻ REFRESH</button>
    <button class="refresh-btn utility-btn" data-action="open-command-palette">COMMAND</button>
    <button class="refresh-btn utility-btn" data-action="toggle-shortcuts-overlay">SHORTCUTS</button>
    <button class="refresh-btn utility-btn" data-action="open-settings-overlay">SETTINGS</button>
  </div>

  <div id="graph-view" style="display:none;flex:1;position:relative;background:var(--bg);min-height:600px">
    <div class="graph-toolbar">
      <div class="graph-toolbar-row">
        <input type="text" class="graph-search-input" id="graph-search-input" placeholder="Search graph..." inputmode="search">
      </div>
      <div class="graph-toolbar-row">
        <button class="graph-btn active" id="graph-toggle-inferred" data-action="toggle-graph-inferred">INFERRED ON</button>
        <button class="graph-btn active" id="graph-toggle-labels" data-action="toggle-graph-labels">LABELS ON</button>
        <button class="graph-btn active" id="graph-toggle-physics" data-action="toggle-graph-physics">PHYSICS ON</button>
        <button class="graph-btn" data-action="reset-graph-view">RESET VIEW</button>
      </div>
      <div class="graph-toolbar-row">
        <button class="graph-btn relation active" id="graph-rel-related" data-action="toggle-graph-relation" data-relation="related">RELATED</button>
        <button class="graph-btn relation active" id="graph-rel-supports" data-action="toggle-graph-relation" data-relation="supports">SUPPORTS</button>
        <button class="graph-btn relation active" id="graph-rel-contradicts" data-action="toggle-graph-relation" data-relation="contradicts">CONTRA</button>
        <button class="graph-btn relation active" id="graph-rel-supersedes" data-action="toggle-graph-relation" data-relation="supersedes">SUPER</button>
        <button class="graph-btn relation active" id="graph-rel-causes" data-action="toggle-graph-relation" data-relation="causes">CAUSES</button>
        <button class="graph-btn relation active" id="graph-rel-example_of" data-action="toggle-graph-relation" data-relation="example_of">EXAMPLE</button>
      </div>
    </div>
    <div class="graph-legend" id="graph-legend"></div>
    <svg id="graph-svg" style="width:100%;height:100%;min-height:600px"></svg>
    <div id="graph-empty" style="display:none;position:absolute;inset:0;align-items:center;justify-content:center;text-align:center;color:var(--text-dim);font-size:0.72rem;letter-spacing:0.12em;padding:1rem">NO MEMORIES YET</div>
  </div>
  <div class="grid-wrap" id="grid">
    <div class="loading"><div class="loading-dot"></div><div class="loading-dot"></div><div class="loading-dot"></div></div>
  </div>

  <footer class="footer">
    <div class="footer-text">AI MEMORY MCP · CLOUDFLARE D1</div>
    <div class="footer-text">SECURE SESSION<span class="cursor-blink"></span></div>
  </footer>
</div>

<!-- EXPAND OVERLAY -->
<div class="expand-overlay" id="expand-overlay" data-action="close-expand-overlay">
  <div class="expand-box">
    <button class="expand-close" data-action="close-expand">✕ CLOSE</button>
    <div id="expand-header"></div>
    <div class="expand-content" id="expand-content"></div>
    <div style="margin-top:1.5rem;padding-top:1rem;border-top:1px solid var(--border);font-size:0.6rem;color:var(--text-dim);letter-spacing:0.08em" id="expand-meta"></div>
    <div id="expand-connections"></div>
  </div>
</div>

<div class="cmd-overlay" id="cmd-overlay" data-action="close-command-palette-overlay">
  <div class="cmd-box">
    <div class="cmd-head">
      <input type="text" class="cmd-input" id="cmd-input" placeholder="Run an action..." autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false">
      <div class="cmd-hint">enter run - esc close - arrows move</div>
    </div>
    <div class="cmd-list" id="cmd-list"></div>
  </div>
</div>

<div class="shortcuts-overlay" id="shortcuts-overlay" data-action="close-shortcuts-overlay">
  <div class="shortcuts-box">
    <div class="shortcuts-head">
      <h3>Keyboard Shortcuts</h3>
      <button class="shortcuts-close" data-action="close-shortcuts">Close</button>
    </div>
    <div class="shortcuts-grid">
      <span class="shortcut-key">Ctrl/Cmd+K</span><span class="shortcut-desc">Open command palette</span>
      <span class="shortcut-key">?</span><span class="shortcut-desc">Open this shortcuts panel</span>
      <span class="shortcut-key">S</span><span class="shortcut-desc">Open settings panel</span>
      <span class="shortcut-key">/</span><span class="shortcut-desc">Focus search input</span>
      <span class="shortcut-key">G</span><span class="shortcut-desc">Open graph view</span>
      <span class="shortcut-key">R</span><span class="shortcut-desc">Refresh memories</span>
      <span class="shortcut-key">Esc</span><span class="shortcut-desc">Close overlays or modal cards</span>
      <span class="shortcut-key">Enter</span><span class="shortcut-desc">Run selected command in command palette</span>
    </div>
  </div>
</div>

<div class="settings-overlay" id="settings-overlay" data-action="close-settings-overlay">
  <div class="settings-box">
    <div class="settings-head">
      <div class="settings-head-main">
        <h3>Viewer Settings</h3>
        <span class="settings-version">v${escapeHtml(SERVER_VERSION)}</span>
      </div>
      <button class="settings-close" data-action="close-settings">Close</button>
    </div>
    <div class="settings-scroll">
      <div class="settings-sections">
        <details class="settings-folder" open>
          <summary>General & Search</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Live Polling</div>
                  <div class="setting-help">Auto-refresh memory stats in background.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-live-poll-enabled">
              </div>
              <div class="setting-row">
                <label for="settings-live-poll-interval">Polling Interval (sec)</label>
                <input type="number" min="5" max="120" step="1" class="setting-input" id="settings-live-poll-interval">
                <div class="setting-help">Lower is faster updates, higher is lighter load.</div>
              </div>
              <div class="setting-row">
                <label for="settings-time-mode">Time Display</label>
                <select class="setting-input" id="settings-time-mode">
                  <option value="utc">UTC</option>
                  <option value="local">Local</option>
                </select>
                <div class="setting-help">Header clock format mode.</div>
              </div>
              <div class="setting-row">
                <label for="settings-default-filter">Default Startup Filter</label>
                <select class="setting-input" id="settings-default-filter">
                  <option value="">All</option>
                  <option value="note">Notes</option>
                  <option value="fact">Facts</option>
                  <option value="journal">Journal</option>
                </select>
                <div class="setting-help">Initial list filter after sign-in.</div>
              </div>
              <div class="setting-row">
                <label for="settings-search-debounce">Search Debounce (ms)</label>
                <input type="number" min="120" max="1500" step="10" class="setting-input" id="settings-search-debounce">
                <div class="setting-help">Delay before list search triggers.</div>
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Compact Cards</div>
                  <div class="setting-help">Fit more memory cards on screen.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-compact-cards">
              </div>
            </div>
          </div>
        </details>

        <details class="settings-folder">
          <summary>Graph Defaults</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Default Inferred Edges</div>
                  <div class="setting-help">Initial graph inferred-edge visibility.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-graph-inferred">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Default Graph Labels</div>
                  <div class="setting-help">Initial graph label visibility.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-graph-labels">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Default Graph Physics</div>
                  <div class="setting-help">Start graph simulation enabled.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-graph-physics">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Open Graph On Sign-in</div>
                  <div class="setting-help">Skip list view and jump to graph first.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-auto-open-graph">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Graph Hover Focus</div>
                  <div class="setting-help">Highlight node neighborhood on hover.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-graph-focus">
              </div>
            </div>
          </div>
        </details>

        <details class="settings-folder">
          <summary>Appearance & Session</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-span-2">
                <label for="settings-theme">Theme</label>
                <div class="setting-help">Choose a color theme for the viewer.</div>
                <div class="theme-picker" id="theme-picker">
                  <button type="button" class="theme-swatch" data-theme-value="cyberpunk" title="Cyberpunk"><span style="background:#080c10;border:2px solid #f0a500"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="light" title="Light"><span style="background:#f5f5f5;border:2px solid #c07800"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="midnight" title="Midnight"><span style="background:#0a0a1a;border:2px solid #7c6aff"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="solarized" title="Solarized"><span style="background:#002b36;border:2px solid #b58900"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="ember" title="Ember"><span style="background:#1a0a08;border:2px solid #ff6b35"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="arctic" title="Arctic"><span style="background:#0c1820;border:2px solid #40c8e0"></span></button>
                </div>
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Show Scanlines</div>
                  <div class="setting-help">Enable CRT-style scanline overlay.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-show-scanlines">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Reduce Motion</div>
                  <div class="setting-help">Disable most transitions and animations.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-reduce-motion">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Confirm Before Lock</div>
                  <div class="setting-help">Prompt before manual logout/lock.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-confirm-logout">
              </div>
            </div>
          </div>
        </details>

        <details class="settings-folder">
          <summary>Notifications</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Toast Notifications</div>
                  <div class="setting-help">In-app feedback for actions and errors.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-toasts-enabled">
              </div>
              <div class="setting-row">
                <label for="settings-toast-duration">Toast Duration (ms)</label>
                <input type="number" min="1200" max="8000" step="100" class="setting-input" id="settings-toast-duration">
                <div class="setting-help">How long toast messages stay visible.</div>
              </div>
            </div>
          </div>
        </details>

        <details class="settings-folder">
          <summary>Semantic Index</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Semantic Reindex Wait</div>
                  <div class="setting-help">Wait for Vectorize index readiness before reindex returns.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-semantic-wait">
              </div>
              <div class="setting-row">
                <label for="settings-semantic-timeout">Semantic Wait Timeout (sec)</label>
                <input type="number" min="1" max="900" step="1" class="setting-input" id="settings-semantic-timeout">
                <div class="setting-help">Used when Semantic Reindex Wait is enabled.</div>
              </div>
              <div class="setting-row">
                <label for="settings-semantic-limit">Semantic Reindex Limit</label>
                <input type="number" min="1" max="2000" step="1" class="setting-input" id="settings-semantic-limit">
                <div class="setting-help">Maximum memories processed per reindex run.</div>
              </div>
              <div class="setting-row setting-span-2">
                <div class="setting-label">Semantic Index Sync</div>
                <div class="setting-help">Run <code>memory_reindex</code> from the viewer and inspect readiness output.</div>
                <div class="semantic-status-box">
                  <div class="semantic-status-line dim" id="semantic-status-line">No semantic reindex run in this session.</div>
                  <div class="semantic-status-meta" id="semantic-status-meta"></div>
                  <button class="refresh-btn utility-btn" id="semantic-reindex-btn" data-action="run-semantic-reindex">RUN SEMANTIC REINDEX</button>
                </div>
              </div>
            </div>
          </div>
        </details>
      </div>
    </div>
    <div class="settings-actions">
      <button class="refresh-btn utility-btn" data-action="open-changelog-overlay">VIEW CHANGELOG</button>
      <button class="refresh-btn utility-btn" data-action="reset-viewer-settings">RESET DEFAULTS</button>
      <button class="refresh-btn" data-action="apply-settings">SAVE SETTINGS</button>
    </div>
  </div>
</div>

<div class="changelog-overlay" id="changelog-overlay" data-action="close-changelog-overlay">
  <div class="changelog-box">
    <div class="changelog-head">
      <div class="changelog-title-group">
        <h3>Release Changelog</h3>
        <div class="changelog-subtitle" id="changelog-subtitle">Recent platform updates</div>
      </div>
      <button class="settings-close" data-action="close-changelog">Close</button>
    </div>
    <div class="changelog-list" id="changelog-list"></div>
    <div class="settings-actions" style="margin-top:0.7rem">
      <button class="refresh-btn utility-btn" data-action="open-full-changelog">OPEN FULL CHANGELOG</button>
    </div>
  </div>
</div>

<div class="toast-wrap" id="toast-wrap"></div>

<script src="/view.js"></script>
</body>
</html>`;
}

export function viewerScript(): string {
  return `
  const BASE = location.origin;
  const VIEWER_SERVER_VERSION = '${escapeHtml(SERVER_VERSION)}';
  const GRAPH_RELATION_TYPES = ['related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of'];
  const GRAPH_RELATION_COLOR = {
    related: '#2a4060',
    supports: '#2eca75',
    contradicts: '#e05050',
    supersedes: '#f0a500',
    causes: '#ff9e4f',
    example_of: '#66a9ff',
  };
  let TOKEN = '';
  let SESSION_MODE = 'none';
  let activeFilter = '';
  let searchTimeout = null;
  let allMemories = [];
  let expandGen = 0;
  let graphVisible = false;
  let lastGraphData = { nodes: [], edges: [], inferred_edges: [] };
  let graphResizeTimer = null;
  let graphShowInferred = true;
  let graphShowLabels = !window.matchMedia('(max-width: 640px)').matches;
  let graphSvgSelection = null;
  let graphZoomBehavior = null;
  let graphSimulation = null;
  let graphAutoTunedLabels = false;
  let graphSearchQuery = '';
  let graphRelationFilter = new Set(GRAPH_RELATION_TYPES);
  let graphPhysicsEnabled = true;
  let lastStatsSnapshot = { all: null, note: null, fact: null, journal: null };
  let commandPaletteOpen = false;
  let commandQuery = '';
  let commandVisibleActions = [];
  let commandActiveIndex = 0;
  let toastCounter = 0;
  let clockIntervalId = null;
  const VIEWER_SETTINGS_KEY = 'memoryvault.viewer.settings.v1';
  let viewerSettings = null;
  let semanticReindexRunning = false;
  let semanticReindexLastResult = null;
  let semanticReindexLastError = '';

  function hasAuthenticatedSession() {
    return SESSION_MODE === 'user' || (SESSION_MODE === 'legacy' && !!TOKEN);
  }

  function buildDefaultViewerSettings() {
    return {
      theme: 'cyberpunk',
      live_poll_enabled: true,
      live_poll_interval_sec: 10,
      time_mode: 'utc',
      default_memory_filter: '',
      search_debounce_ms: 300,
      compact_cards: false,
      graph_show_inferred: true,
      graph_show_labels: !window.matchMedia('(max-width: 640px)').matches,
      graph_physics_enabled: true,
      graph_focus_highlight: true,
      auto_open_graph: false,
      toasts_enabled: true,
      toast_duration_ms: 2300,
      confirm_logout: false,
      show_scanlines: true,
      reduce_motion: false,
      semantic_reindex_wait_for_index: true,
      semantic_reindex_wait_timeout_seconds: 180,
      semantic_reindex_limit: 500,
    };
  }

  function normalizeViewerSettings(raw) {
    const defaults = buildDefaultViewerSettings();
    const source = raw && typeof raw === 'object' ? raw : {};
    const intervalRaw = Number(source.live_poll_interval_sec);
    const interval = Number.isFinite(intervalRaw) ? intervalRaw : defaults.live_poll_interval_sec;
    const searchDebounceRaw = Number(source.search_debounce_ms);
    const searchDebounce = Number.isFinite(searchDebounceRaw) ? searchDebounceRaw : defaults.search_debounce_ms;
    const toastDurationRaw = Number(source.toast_duration_ms);
    const toastDuration = Number.isFinite(toastDurationRaw) ? toastDurationRaw : defaults.toast_duration_ms;
    const semanticWaitTimeoutRaw = Number(source.semantic_reindex_wait_timeout_seconds);
    const semanticWaitTimeout = Number.isFinite(semanticWaitTimeoutRaw)
      ? semanticWaitTimeoutRaw
      : defaults.semantic_reindex_wait_timeout_seconds;
    const semanticReindexLimitRaw = Number(source.semantic_reindex_limit);
    const semanticReindexLimit = Number.isFinite(semanticReindexLimitRaw)
      ? semanticReindexLimitRaw
      : defaults.semantic_reindex_limit;
    const defaultFilter = ['note', 'fact', 'journal'].includes(source.default_memory_filter)
      ? source.default_memory_filter
      : '';
    const validThemes = ['cyberpunk', 'light', 'midnight', 'solarized', 'ember', 'arctic'];
    const theme = validThemes.includes(source.theme) ? source.theme : defaults.theme;
    return {
      theme,
      live_poll_enabled: source.live_poll_enabled !== false,
      live_poll_interval_sec: Math.min(Math.max(Math.round(interval), 5), 120),
      time_mode: source.time_mode === 'local' ? 'local' : 'utc',
      default_memory_filter: defaultFilter,
      search_debounce_ms: Math.min(Math.max(Math.round(searchDebounce), 120), 1500),
      compact_cards: source.compact_cards === true,
      graph_show_inferred: source.graph_show_inferred !== false,
      graph_show_labels: source.graph_show_labels === undefined ? defaults.graph_show_labels : source.graph_show_labels !== false,
      graph_physics_enabled: source.graph_physics_enabled !== false,
      graph_focus_highlight: source.graph_focus_highlight !== false,
      auto_open_graph: source.auto_open_graph === true,
      toasts_enabled: source.toasts_enabled !== false,
      toast_duration_ms: Math.min(Math.max(Math.round(toastDuration), 1200), 8000),
      confirm_logout: source.confirm_logout === true,
      show_scanlines: source.show_scanlines !== false,
      reduce_motion: source.reduce_motion === true,
      semantic_reindex_wait_for_index: source.semantic_reindex_wait_for_index !== false,
      semantic_reindex_wait_timeout_seconds: Math.min(Math.max(Math.round(semanticWaitTimeout), 1), 900),
      semantic_reindex_limit: Math.min(Math.max(Math.round(semanticReindexLimit), 1), 2000),
    };
  }

  function loadViewerSettings() {
    const defaults = buildDefaultViewerSettings();
    try {
      const raw = localStorage.getItem(VIEWER_SETTINGS_KEY);
      if (!raw) return defaults;
      const parsed = JSON.parse(raw);
      return normalizeViewerSettings(parsed);
    } catch {
      return defaults;
    }
  }

  function persistViewerSettings() {
    if (!viewerSettings) return;
    try {
      localStorage.setItem(VIEWER_SETTINGS_KEY, JSON.stringify(viewerSettings));
    } catch {}
  }

  function applyViewerSettingsToRuntime(options = {}) {
    if (!viewerSettings) return;
    const restartPolling = options.restartPolling !== false;
    const rerenderGraph = options.rerenderGraph === true;
    const rerenderGrid = options.rerenderGrid === true;
    graphShowInferred = viewerSettings.graph_show_inferred;
    graphShowLabels = viewerSettings.graph_show_labels;
    graphPhysicsEnabled = viewerSettings.graph_physics_enabled;
    document.body.classList.toggle('compact-cards', viewerSettings.compact_cards);
    document.body.classList.toggle('scanlines-off', !viewerSettings.show_scanlines);
    document.body.classList.toggle('motion-reduced', viewerSettings.reduce_motion);
    document.documentElement.setAttribute('data-theme', viewerSettings.theme || 'cyberpunk');
    syncThemePicker();
    syncGraphToolbarState();
    if (restartPolling) startLivePolling(true);
    if (rerenderGrid) renderGrid(allMemories);
    if (rerenderGraph && graphVisible) rerenderGraphFromCache();
  }

  function initializeViewerSettings() {
    viewerSettings = loadViewerSettings();
    applyViewerSettingsToRuntime({ restartPolling: false, rerenderGraph: false, rerenderGrid: false });
  }

  initializeViewerSettings();
  fillSettingsForm();
  restoreUserSession();

  function setLoginError(message) {
    const el = document.getElementById('login-error');
    if (!el) return;
    el.textContent = message || '⚠ ACCESS DENIED';
    el.style.display = 'block';
  }

  function clearLoginError() {
    const el = document.getElementById('login-error');
    if (!el) return;
    el.style.display = 'none';
  }

  function isTypingTarget(target) {
    const el = target instanceof HTMLElement ? target : null;
    if (!el) return false;
    if (el.isContentEditable) return true;
    const tag = (el.tagName || '').toUpperCase();
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true;
    return false;
  }

  function showToast(message, tone = 'info', force = false) {
    const text = String(message || '').trim();
    const wrap = document.getElementById('toast-wrap');
    if (!force && viewerSettings && viewerSettings.toasts_enabled === false) return;
    if (!text || !wrap) return;
    const toast = document.createElement('div');
    const safeTone = ['info', 'success', 'error'].includes(tone) ? tone : 'info';
    toast.className = 'toast ' + safeTone;
    toast.dataset.toastId = String(++toastCounter);
    toast.textContent = text;
    wrap.appendChild(toast);
    const durationMs = Math.min(Math.max(Number(viewerSettings?.toast_duration_ms ?? 2300), 1200), 8000);
    setTimeout(() => {
      toast.classList.add('hide');
      setTimeout(() => toast.remove(), 220);
    }, durationMs);
  }

  function enterApp() {
    document.getElementById('login-screen').style.display = 'none';
    document.getElementById('app').style.display = 'flex';
    document.getElementById('app').style.flexDirection = 'column';
    startClock();
    const defaultFilter = viewerSettings?.default_memory_filter || '';
    activeFilter = defaultFilter;
    syncFilterPills(activeFilter);
    loadMemories();
    startLivePolling();
    showToast('Session active. Loading memory stream.', 'success');
    if (viewerSettings && viewerSettings.auto_open_graph) {
      setTimeout(() => { if (hasAuthenticatedSession()) showGraph(); }, 180);
    }
    checkUpdateBanner();
  }

  const UPDATE_BANNER_DISMISSED_KEY = 'memoryvault.update_banner.dismissed_version';

  async function checkUpdateBanner() {
    try {
      const dismissedVersion = localStorage.getItem(UPDATE_BANNER_DISMISSED_KEY) || '';
      if (dismissedVersion === VIEWER_SERVER_VERSION) return;
      const headers = SESSION_MODE === 'legacy' && TOKEN
        ? { 'Authorization': 'Bearer ' + TOKEN }
        : {};
      const r = await fetch(BASE + '/mcp', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...headers },
        credentials: 'same-origin',
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: 'update-banner',
          method: 'tools/call',
          params: {
            name: 'tool_changelog',
            arguments: dismissedVersion ? { since_version: dismissedVersion, limit: 5 } : { limit: 3 },
          },
        }),
      });
      if (!r.ok) return;
      const data = await r.json();
      const text = data?.result?.content?.[0]?.text;
      if (!text) return;
      const parsed = JSON.parse(text);
      const entries = parsed?.entries || parsed;
      if (!Array.isArray(entries) || entries.length === 0) return;
      const items = [];
      for (const entry of entries) {
        if (!entry.changes) continue;
        for (const change of entry.changes) {
          items.push({
            type: change.type || 'added',
            name: change.name || '',
            version: entry.version || '',
          });
        }
      }
      if (items.length === 0) return;
      const container = document.getElementById('update-banner-items');
      if (!container) return;
      container.innerHTML = '';
      const shown = items.slice(0, 6);
      for (const item of shown) {
        const el = document.createElement('span');
        el.className = 'update-banner-item';
        const badge = document.createElement('span');
        badge.className = 'badge';
        badge.textContent = item.type.toUpperCase();
        el.appendChild(badge);
        const label = document.createTextNode(' ' + item.name);
        el.appendChild(label);
        container.appendChild(el);
      }
      if (items.length > 6) {
        const more = document.createElement('span');
        more.className = 'update-banner-item';
        more.textContent = '+' + (items.length - 6) + ' more';
        container.appendChild(more);
      }
      const titleEl = document.getElementById('update-banner-title');
      if (titleEl) {
        const latestVersion = entries[0]?.version || VIEWER_SERVER_VERSION;
        titleEl.textContent = 'New in v' + latestVersion;
      }
      document.getElementById('update-banner').classList.add('visible');
    } catch {}
  }

  function dismissUpdateBanner() {
    localStorage.setItem(UPDATE_BANNER_DISMISSED_KEY, VIEWER_SERVER_VERSION);
    const banner = document.getElementById('update-banner');
    if (banner) {
      banner.style.animation = 'bannerSlide 0.2s ease reverse forwards';
      setTimeout(() => {
        banner.classList.remove('visible');
        banner.style.animation = '';
      }, 200);
    }
  }

  async function tryRefreshSession() {
    try {
      const r = await fetch(BASE + '/auth/refresh', {
        method: 'POST',
        credentials: 'same-origin',
      });
      if (!r.ok) return false;
      SESSION_MODE = 'user';
      return true;
    } catch {
      return false;
    }
  }

  async function restoreUserSession() {
    if (hasAuthenticatedSession()) return true;
    try {
      let r = await fetch(BASE + '/auth/me', { credentials: 'same-origin' });
      if (r.status === 401) {
        const refreshed = await tryRefreshSession();
        if (!refreshed) return false;
        r = await fetch(BASE + '/auth/me', { credentials: 'same-origin' });
      }
      if (!r.ok) return false;
      SESSION_MODE = 'user';
      enterApp();
      return true;
    } catch {
      return false;
    }
  }

  async function apiFetch(url, options = {}, allowRefresh = true) {
    const mergedHeaders = Object.assign({}, options.headers || {});
    if (SESSION_MODE === 'legacy' && TOKEN) {
      mergedHeaders.Authorization = 'Bearer ' + TOKEN;
    }
    const response = await fetch(url, Object.assign({ credentials: 'same-origin' }, options, { headers: mergedHeaders }));
    if (response.status === 401 && allowRefresh && SESSION_MODE === 'user') {
      const refreshed = await tryRefreshSession();
      if (refreshed) return apiFetch(url, options, false);
    }
    return response;
  }

  async function callMcpTool(name, args = {}, requestId = '') {
    const response = await apiFetch(BASE + '/mcp', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: requestId || ('viewer-' + name + '-' + Date.now()),
        method: 'tools/call',
        params: { name, arguments: args },
      }),
    });
    if (response.status === 401) {
      doLogout(true);
      throw new Error('Session expired.');
    }
    if (!response.ok) {
      throw new Error('MCP request failed (' + response.status + ').');
    }
    const rpc = await response.json();
    if (rpc && rpc.error) {
      const message = typeof rpc.error.message === 'string' && rpc.error.message.trim()
        ? rpc.error.message.trim()
        : 'MCP error.';
      throw new Error(message);
    }
    const text = rpc?.result?.content?.[0]?.text;
    if (typeof text !== 'string') {
      throw new Error('Invalid MCP response.');
    }
    return text;
  }

  function formatDurationMs(ms) {
    const value = Number(ms);
    if (!Number.isFinite(value) || value < 0) return 'n/a';
    if (value < 1000) return Math.round(value) + 'ms';
    return (value / 1000).toFixed(value >= 10000 ? 0 : 1) + 's';
  }

  function getSemanticReindexArgs() {
    const defaultLimit = Number(viewerSettings?.semantic_reindex_limit ?? 500);
    const defaultWait = viewerSettings?.semantic_reindex_wait_for_index !== false;
    const defaultTimeout = Number(viewerSettings?.semantic_reindex_wait_timeout_seconds ?? 180);
    const limitInput = document.getElementById('settings-semantic-limit');
    const waitInput = document.getElementById('settings-semantic-wait');
    const timeoutInput = document.getElementById('settings-semantic-timeout');

    const rawLimit = Number(limitInput?.value);
    const rawTimeout = Number(timeoutInput?.value);
    const limit = Math.min(
      Math.max(
        Number.isFinite(rawLimit) && rawLimit > 0 ? Math.round(rawLimit) : Math.round(defaultLimit),
        1
      ),
      2000
    );
    const waitTimeoutSeconds = Math.min(
      Math.max(
        Number.isFinite(rawTimeout) && rawTimeout > 0 ? Math.round(rawTimeout) : Math.round(defaultTimeout),
        1
      ),
      900
    );
    const waitForIndex = waitInput ? waitInput.checked : defaultWait;

    return {
      limit,
      wait_for_index: waitForIndex,
      wait_timeout_seconds: waitTimeoutSeconds,
    };
  }

  function renderSemanticReindexStatus() {
    const lineEl = document.getElementById('semantic-status-line');
    const metaEl = document.getElementById('semantic-status-meta');
    const buttonEl = document.getElementById('semantic-reindex-btn');
    if (!lineEl || !metaEl || !buttonEl) return;
    buttonEl.disabled = semanticReindexRunning;
    buttonEl.textContent = semanticReindexRunning ? 'RUNNING SEMANTIC REINDEX...' : 'RUN SEMANTIC REINDEX';
    metaEl.innerHTML = '';

    const addPill = (text, cls = '') => {
      const pill = document.createElement('span');
      pill.className = 'semantic-status-pill' + (cls ? (' ' + cls) : '');
      pill.textContent = text;
      metaEl.appendChild(pill);
    };

    if (semanticReindexRunning) {
      lineEl.className = 'semantic-status-line';
      lineEl.textContent = 'Semantic reindex is running. Waiting for MCP response...';
      addPill('RUNNING', 'running');
      return;
    }

    if (semanticReindexLastError) {
      lineEl.className = 'semantic-status-line error';
      lineEl.textContent = 'Last run failed: ' + semanticReindexLastError;
      addPill('FAILED');
      return;
    }

    if (!semanticReindexLastResult || typeof semanticReindexLastResult !== 'object') {
      lineEl.className = 'semantic-status-line dim';
      lineEl.textContent = 'No semantic reindex run in this session.';
      return;
    }

    const result = semanticReindexLastResult;
    const processed = Number.isFinite(Number(result.processed)) ? Number(result.processed) : 0;
    const upserted = Number.isFinite(Number(result.upserted)) ? Number(result.upserted) : 0;
    const deleted = Number.isFinite(Number(result.deleted)) ? Number(result.deleted) : 0;
    const indexReady = result.index_ready;
    const waitElapsedMs = Number(result.wait_elapsed_ms);
    const waitForIndex = result.wait_for_index === true;

    lineEl.className = 'semantic-status-line';
    if (waitForIndex) {
      const readyText = indexReady === true ? 'ready' : (indexReady === false ? 'not ready' : 'pending');
      lineEl.textContent = 'Last run processed ' + processed + ' memories. Index status: ' + readyText + '.';
    } else {
      lineEl.textContent = 'Last run processed ' + processed + ' memories without readiness wait.';
    }

    addPill('UPSERTED ' + upserted);
    addPill('DELETED ' + deleted);
    if (waitForIndex) addPill('WAIT ' + formatDurationMs(waitElapsedMs));
    if (indexReady === true) addPill('INDEX READY', 'ready');
    if (indexReady === false) addPill('INDEX NOT READY', 'not-ready');
  }

  async function runSemanticReindex(source = 'settings') {
    if (!ensureAppReady('Semantic reindex')) return null;
    if (semanticReindexRunning) {
      showToast('Semantic reindex already running.', 'info');
      return null;
    }
    semanticReindexRunning = true;
    semanticReindexLastError = '';
    renderSemanticReindexStatus();

    const args = getSemanticReindexArgs();

    showToast(
      'Semantic reindex started (limit ' + args.limit + ', wait ' + (args.wait_for_index ? 'on' : 'off') + ').',
      'info'
    );
    try {
      const text = await callMcpTool('memory_reindex', args, 'viewer-semantic-reindex');
      const parsed = JSON.parse(text);
      if (!parsed || typeof parsed !== 'object') {
        throw new Error('Unexpected reindex response.');
      }
      semanticReindexLastResult = parsed;
      semanticReindexLastError = '';
      renderSemanticReindexStatus();

      const indexReady = parsed.index_ready;
      if (indexReady === true) {
        showToast('Semantic reindex completed and index is ready.', 'success', true);
      } else if (indexReady === false) {
        showToast('Reindex completed but index is not fully ready yet.', 'info', true);
      } else {
        showToast('Reindex completed.', 'success', true);
      }
      if (source === 'settings') {
        loadMemories(true);
      }
      return parsed;
    } catch (err) {
      semanticReindexLastResult = null;
      semanticReindexLastError = err instanceof Error && err.message ? err.message : 'Semantic reindex failed.';
      renderSemanticReindexStatus();
      showToast(semanticReindexLastError, 'error', true);
      return null;
    } finally {
      semanticReindexRunning = false;
      renderSemanticReindexStatus();
    }
  }

  function runSemanticReindexFromSettings() {
    return runSemanticReindex('settings');
  }

  async function doTokenLogin() {
    clearLoginError();
    const val = document.getElementById('token-input').value.trim();
    if (!val) {
      setLoginError('⚠ ENTER A TOKEN');
      return;
    }
    try {
      const r = await fetch(BASE + '/api/memories?limit=1', {
        headers: { 'Authorization': 'Bearer ' + val },
      });
      if (!r.ok) {
        setLoginError('⚠ ACCESS DENIED — invalid token');
        return;
      }
      TOKEN = val;
      SESSION_MODE = 'legacy';
      enterApp();
      showToast('Legacy token accepted.', 'success');
    } catch {
      setLoginError('⚠ NETWORK ERROR');
      showToast('Network error while validating token.', 'error');
    }
  }

  async function doCredentialAuth(mode) {
    clearLoginError();
    const email = document.getElementById('email-input').value.trim();
    const password = document.getElementById('password-input').value;
    const brainName = document.getElementById('brain-name-input').value.trim();
    if (!email || !password) {
      setLoginError('⚠ EMAIL + PASSWORD REQUIRED');
      return;
    }

    const payload = { email, password };
    if (mode === 'signup' && brainName) payload.brain_name = brainName;

    try {
      const endpoint = mode === 'signup' ? '/auth/signup' : '/auth/login';
      const r = await fetch(BASE + endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify(payload),
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) {
        setLoginError('⚠ ' + (data.error || 'AUTH FAILED'));
        return;
      }
      TOKEN = '';
      SESSION_MODE = 'user';
      enterApp();
      showToast(mode === 'signup' ? 'Account created and signed in.' : 'Signed in successfully.', 'success');
    } catch {
      setLoginError('⚠ NETWORK ERROR');
      showToast('Network error during authentication.', 'error');
    }
  }

  function doLogin() {
    return doTokenLogin();
  }

  async function doLogout(force = false) {
    if (!force && viewerSettings?.confirm_logout) {
      const ok = window.confirm('Lock and sign out of the current session?');
      if (!ok) return;
    }
    if (SESSION_MODE === 'user') {
      try {
        await tryRefreshSession();
        await fetch(BASE + '/auth/logout', {
          method: 'POST',
          credentials: 'same-origin',
        });
      } catch {}
    }
    if (pollIntervalId) {
      clearInterval(pollIntervalId);
      pollIntervalId = null;
    }
    if (clockIntervalId) {
      clearInterval(clockIntervalId);
      clockIntervalId = null;
    }
    TOKEN = '';
    SESSION_MODE = 'none';
    location.reload();
  }

  function updateTime() {
    const el = document.getElementById('hdr-time');
    if (el) {
      if (viewerSettings && viewerSettings.time_mode === 'local') {
        const local = new Date().toLocaleString();
        el.textContent = local + ' LOCAL';
      } else {
        el.textContent = new Date().toISOString().replace('T',' ').slice(0,19) + ' UTC';
      }
    }
  }

  function startClock() {
    if (clockIntervalId) clearInterval(clockIntervalId);
    updateTime();
    clockIntervalId = setInterval(updateTime, 1000);
  }

  function pulseStatPill(id, changed) {
    if (!changed) return;
    const el = document.getElementById(id);
    if (!el) return;
    el.classList.remove('pulse');
    void el.offsetWidth;
    el.classList.add('pulse');
  }

  async function loadMemories(silent = false) {
    const grid = document.getElementById('grid');
    const refreshBtn = document.querySelector('.refresh-btn');
    const scrollY = window.scrollY;
    if (!silent) {
      grid.innerHTML = '<div class="loading"><div class="loading-dot"></div><div class="loading-dot"></div><div class="loading-dot"></div></div>';
    }
    if (refreshBtn && !silent) refreshBtn.classList.add('syncing');
    const search = document.getElementById('search-input').value;
    let url = BASE + '/api/memories?limit=500';
    if (activeFilter) url += '&type=' + encodeURIComponent(activeFilter);
    if (search) url += '&search=' + encodeURIComponent(search);
    try {
      const r = await apiFetch(url);
      if (r.status === 401) { doLogout(true); return; }
      if (!r.ok) {
        if (!silent) {
          grid.innerHTML = '<div class="empty-state"><div class="empty-icon">⚠</div>ERROR LOADING MEMORIES</div>';
          showToast('Memory load failed (' + r.status + ').', 'error');
        }
        return;
      }
      const data = await r.json();
      allMemories = data.memories || [];
      updateStats(data.stats || [], allMemories);
      renderGrid(allMemories);
      if (silent) window.scrollTo(0, scrollY);
    } catch(e) {
      grid.innerHTML = '<div class="empty-state"><div class="empty-icon">⚠</div>CONNECTION ERROR</div>';
      showToast('Connection error while loading memories.', 'error');
    } finally {
      if (refreshBtn) refreshBtn.classList.remove('syncing');
    }
  }

  function updateStats(stats, memories = []) {
    const counts = { note: 0, fact: 0, journal: 0 };
    let total = 0;
    stats.forEach(s => { counts[s.type] = s.count; total += s.count; });
    document.getElementById('count-all').textContent = total;
    document.getElementById('count-note').textContent = counts.note;
    document.getElementById('count-fact').textContent = counts.fact;
    document.getElementById('count-journal').textContent = counts.journal;
    pulseStatPill('stat-all', lastStatsSnapshot.all !== null && total !== lastStatsSnapshot.all);
    pulseStatPill('stat-note', lastStatsSnapshot.note !== null && counts.note !== lastStatsSnapshot.note);
    pulseStatPill('stat-fact', lastStatsSnapshot.fact !== null && counts.fact !== lastStatsSnapshot.fact);
    pulseStatPill('stat-journal', lastStatsSnapshot.journal !== null && counts.journal !== lastStatsSnapshot.journal);
    lastStatsSnapshot = { all: total, note: counts.note, fact: counts.fact, journal: counts.journal };
    const confidenceValues = memories
      .map((m) => Number(m.dynamic_confidence ?? m.confidence))
      .filter((v) => Number.isFinite(v));
    const avgConfidence = confidenceValues.length
      ? Math.round((confidenceValues.reduce((a, b) => a + b, 0) / confidenceValues.length) * 100)
      : null;
    document.getElementById('hdr-count').textContent = avgConfidence === null
      ? (total + ' entries')
      : (total + ' entries · avg conf ' + avgConfidence + '%');
  }

  function renderGrid(memories) {
    const grid = document.getElementById('grid');
    if (!memories.length) {
      grid.innerHTML = '<div class="empty-state"><div class="empty-icon">◈</div>NO MEMORIES FOUND</div>';
      return;
    }
    grid.innerHTML = memories.map((m, i) => {
      const date = new Date(m.created_at * 1000).toISOString().slice(0,10);
      const tags = m.tags ? m.tags.split(',').map(t => \`<span class="tag">\${esc(t.trim())}</span>\`).join('') : '';
      const linkBadge = m.link_count > 0 ? \`<span class="card-links-badge">⬡ \${m.link_count} connections</span>\` : '';
      const titleHtml = m.title ? \`<div class="card-title">\${esc(m.title)}</div>\` : '';
      const keyHtml = m.key ? \`<div class="card-key"><span>KEY /</span> \${esc(m.key)}</div>\` : '';
      const confidenceNum = Number(m.dynamic_confidence ?? m.confidence);
      const importanceNum = Number(m.dynamic_importance ?? m.importance);
      const confidencePct = Number.isFinite(confidenceNum) ? Math.round(Math.min(Math.max(confidenceNum, 0), 1) * 100) : null;
      const importancePct = Number.isFinite(importanceNum) ? Math.round(Math.min(Math.max(importanceNum, 0), 1) * 100) : null;
      const sourceLabel = m.source ? String(m.source).trim() : '';
      const sourceDisplay = sourceLabel.length > 18 ? (sourceLabel.slice(0, 17) + '…') : sourceLabel;
      const sourceChip = sourceDisplay ? \`<span class="quality-chip src">SRC \${esc(sourceDisplay)}</span>\` : '';
      const confChip = confidencePct === null ? '' : \`<span class="quality-chip conf">CONF \${confidencePct}%</span>\`;
      const impChip = importancePct === null ? '' : \`<span class="quality-chip imp">IMP \${importancePct}%</span>\`;
      const qualityChips = sourceChip || confChip || impChip
        ? \`<div class="card-quality">\${sourceChip}\${confChip}\${impChip}</div>\`
        : '';
      return \`<div class="card" data-type="\${m.type}" data-idx="\${i}" data-action="expand-card" data-card-index="\${i}" style="animation-delay:\${Math.min(i*0.04,0.4)}s">
        <div class="card-type-stripe"></div>
        <div class="card-header">
          <div>\${titleHtml}\${keyHtml}\${!m.title && !m.key ? '<div class="card-title" style="opacity:0.4">untitled</div>' : ''}</div>
          <span class="card-type-badge">\${m.type}</span>
        </div>
        <div class="card-content">\${esc(m.content)}</div>
        <div class="card-footer">
          <div class="card-meta">
            <div class="card-tags">\${tags}\${linkBadge}</div>
            \${qualityChips}
          </div>
          <div class="card-date">\${date}</div>
        </div>
        <div class="card-id">\${m.id}</div>
      </div>\`;
    }).join('');
  }

  function expandCard(idx) {
    const m = allMemories[idx];
    if (!m) return;
    const date = new Date(m.created_at * 1000).toLocaleString();
    const updated = m.updated_at !== m.created_at ? '  ·  Updated ' + new Date(m.updated_at * 1000).toLocaleString() : '';
    const typeColors = { note: 'var(--teal)', fact: 'var(--amber)', journal: '#8888ff' };
    const qualityChips = [
      m.source ? \`<span class="tag">src:\${esc(m.source)}</span>\` : '',
      Number.isFinite(Number(m.dynamic_confidence ?? m.confidence)) ? \`<span class="tag">conf:\${Math.round(Number(m.dynamic_confidence ?? m.confidence) * 100)}%</span>\` : '',
      Number.isFinite(Number(m.dynamic_importance ?? m.importance)) ? \`<span class="tag">imp:\${Math.round(Number(m.dynamic_importance ?? m.importance) * 100)}%</span>\` : '',
    ].filter(Boolean).join('');
    document.getElementById('expand-header').innerHTML =
      \`<div style="display:flex;align-items:center;gap:0.75rem;margin-bottom:0.5rem;flex-wrap:wrap">
        <span style="font-size:0.6rem;letter-spacing:0.2em;text-transform:uppercase;border:1px solid \${typeColors[m.type]||'#fff'};color:\${typeColors[m.type]||'#fff'};padding:0.2rem 0.5rem">\${m.type}</span>
        \${m.title ? \`<span style="font-family:var(--sans);font-weight:700;font-size:1.1rem;color:var(--text-bright)">\${esc(m.title)}</span>\` : ''}
        \${m.key ? \`<span style="font-size:0.75rem;color:var(--amber)">KEY: \${esc(m.key)}</span>\` : ''}
      </div>
      \${m.tags ? \`<div style="display:flex;flex-wrap:wrap;gap:0.3rem;margin-bottom:0.25rem">\${m.tags.split(',').map(t => \`<span class="tag">\${esc(t.trim())}</span>\`).join('')}</div>\` : ''}
      \${qualityChips ? \`<div style="display:flex;flex-wrap:wrap;gap:0.3rem;margin-bottom:0.25rem">\${qualityChips}</div>\` : ''}\`;
    document.getElementById('expand-content').textContent = m.content;
    document.getElementById('expand-meta').textContent = 'ID: ' + m.id + '  ·  Created ' + date + updated;
    document.getElementById('expand-overlay').classList.add('open');
    document.body.style.overflow = 'hidden';

    // Lazy-load connections
    const connEl = document.getElementById('expand-connections');
    connEl.innerHTML = '<div style="font-size:0.65rem;color:var(--text-dim);letter-spacing:0.1em;margin-top:1rem">LOADING CONNECTIONS...</div>';
    const myGen = ++expandGen;
    apiFetch(BASE + '/api/links/' + m.id)
      .then(r => {
        if (r.status === 401) { doLogout(true); return null; }
        if (!r.ok) throw new Error('fetch failed');
        return r.json();
      })
      .then(links => {
        if (!links) return;
        if (myGen !== expandGen) return; // card changed, discard stale result
        if (!links || !links.length) { connEl.innerHTML = ''; return; }
        connEl.innerHTML = \`<div class="connections-section">
          <div class="connections-title">⬡ Connections (\${links.length})</div>
          \${links.map(l => {
            const cm = l.memory;
            const relationRaw = String(l.relation_type || 'related').toLowerCase();
            const relationLabel = relationRaw.replace(/_/g, ' ');
            const relationClass = relationRaw.replace(/_/g, '-').replace(/[^a-z-]/g, '');
            const label = l.label ? \`<span class="chip-label">"\${esc(l.label)}"</span>\` : '';
            const name = cm.title || cm.key || (cm.content || '').slice(0, 40) + '…';
            const arrow = l.direction === 'from' ? '→' : '←';
            return \`<span class="connection-chip" data-conn-id="\${esc(cm.id)}">
              <span class="chip-type">[\${esc(cm.type)}]</span>
              \${esc(name)}
              <span class="chip-relation \${esc(relationClass)}">\${esc(relationLabel)}</span>
              \${label}
              <span style="opacity:0.4">\${arrow}</span>
            </span>\`;
          }).join('')}
        </div>\`;
        connEl.querySelectorAll('.connection-chip').forEach(chip => {
          chip.addEventListener('click', () => expandById(chip.dataset.connId));
        });
      })
      .catch(() => { if (myGen === expandGen) connEl.innerHTML = ''; });
  }

  function closeExpand(e) {
    if (e.target === document.getElementById('expand-overlay')) closeExpandBtn();
  }
  function closeExpandBtn() {
    document.getElementById('expand-overlay').classList.remove('open');
    document.body.style.overflow = '';
  }

  function appIsVisible() {
    const app = document.getElementById('app');
    if (!app) return false;
    return window.getComputedStyle(app).display !== 'none';
  }

  function ensureAppReady(actionLabel = 'This action') {
    if (hasAuthenticatedSession() && appIsVisible()) return true;
    showToast(actionLabel + ' is available after sign in.', 'info');
    return false;
  }

  function getCommandPaletteActions() {
    return [
      {
        label: 'Refresh memories',
        detail: 'Reload data from API',
        run: () => {
          if (!ensureAppReady('Refresh')) return;
          loadMemories();
          showToast('Refreshing memories...', 'info');
        },
      },
      {
        label: 'Open graph view',
        detail: 'Explore memory network',
        run: async () => {
          if (!ensureAppReady('Graph view')) return;
          await showGraph();
          showToast('Graph view opened.', 'success');
        },
      },
      {
        label: 'Show all memories',
        detail: 'Clear type filter',
        run: () => {
          if (!ensureAppReady('Memory filter')) return;
          setFilter('');
          showToast('Showing all memory types.', 'info');
        },
      },
      {
        label: 'Focus search',
        detail: 'Jump to primary search',
        run: () => {
          if (!ensureAppReady('Search focus')) return;
          const input = document.getElementById('search-input');
          if (!input) return;
          input.focus();
          input.select();
          showToast('Search focused.', 'success');
        },
      },
      {
        label: 'Focus graph search',
        detail: 'Node and edge query',
        run: async () => {
          if (!ensureAppReady('Graph search')) return;
          if (!graphVisible) await showGraph();
          const input = document.getElementById('graph-search-input');
          if (!input) return;
          input.focus();
          input.select();
          showToast('Graph search focused.', 'success');
        },
      },
      {
        label: graphShowInferred ? 'Disable inferred edges' : 'Enable inferred edges',
        detail: graphShowInferred ? 'Currently ON' : 'Currently OFF',
        run: async () => {
          if (!ensureAppReady('Graph controls')) return;
          if (!graphVisible) await showGraph();
          toggleGraphInferred();
        },
      },
      {
        label: graphShowLabels ? 'Hide graph labels' : 'Show graph labels',
        detail: graphShowLabels ? 'Currently ON' : 'Currently OFF',
        run: async () => {
          if (!ensureAppReady('Graph controls')) return;
          if (!graphVisible) await showGraph();
          toggleGraphLabels();
        },
      },
      {
        label: graphPhysicsEnabled ? 'Pause graph physics' : 'Resume graph physics',
        detail: graphPhysicsEnabled ? 'Currently ON' : 'Currently OFF',
        run: async () => {
          if (!ensureAppReady('Graph controls')) return;
          if (!graphVisible) await showGraph();
          toggleGraphPhysics();
        },
      },
      {
        label: 'Open keyboard shortcuts',
        detail: 'Help overlay',
        run: () => toggleShortcutsOverlay(),
      },
      {
        label: 'Reindex semantic memory',
        detail: 'Limit ' + (viewerSettings?.semantic_reindex_limit ?? 500) +
          ' · wait ' + ((viewerSettings?.semantic_reindex_wait_for_index ?? true) ? 'on' : 'off'),
        run: async () => {
          if (!ensureAppReady('Semantic reindex')) return;
          await runSemanticReindex('command');
        },
      },
      {
        label: 'Open settings',
        detail: 'Viewer preferences',
        run: () => openSettingsOverlay(),
      },
      {
        label: 'Open changelog',
        detail: 'Recent release notes',
        run: () => {
          if (!ensureAppReady('Changelog')) return;
          openChangelogOverlay();
        },
      },
      {
        label: 'Lock session',
        detail: 'Sign out',
        run: () => {
          if (!ensureAppReady('Logout')) return;
          doLogout();
        },
      },
    ];
  }

  function updateCommandActiveSelection() {
    const list = document.getElementById('cmd-list');
    if (!list) return;
    list.querySelectorAll('.cmd-item').forEach((el, idx) => {
      el.classList.toggle('active', idx === commandActiveIndex);
    });
  }

  function renderCommandPalette() {
    const list = document.getElementById('cmd-list');
    if (!list) return;
    const query = commandQuery.trim().toLowerCase();
    const allActions = getCommandPaletteActions();
    commandVisibleActions = allActions.filter((action) => {
      if (!query) return true;
      return (action.label + ' ' + action.detail).toLowerCase().includes(query);
    });
    if (commandActiveIndex >= commandVisibleActions.length) {
      commandActiveIndex = Math.max(commandVisibleActions.length - 1, 0);
    }

    if (!commandVisibleActions.length) {
      list.innerHTML = '<div class="cmd-empty">No matching actions</div>';
      return;
    }

    list.innerHTML = commandVisibleActions.map((action, idx) =>
      '<button type="button" class="cmd-item ' + (idx === commandActiveIndex ? 'active' : '') + '" data-command-index="' + idx + '">' +
      '<span class="cmd-item-label">' + esc(action.label) + '</span>' +
      '<span class="cmd-item-detail">' + esc(action.detail) + '</span>' +
      '</button>'
    ).join('');

    list.querySelectorAll('.cmd-item').forEach((el) => {
      const index = Number(el.getAttribute('data-command-index') || '0');
      el.addEventListener('mouseenter', () => {
        commandActiveIndex = index;
        updateCommandActiveSelection();
      });
      el.addEventListener('click', () => runCommandAction(index));
    });
  }

  function onCommandFilter(value) {
    commandQuery = String(value || '');
    commandActiveIndex = 0;
    renderCommandPalette();
  }

  function moveCommandSelection(delta) {
    if (!commandVisibleActions.length) return;
    const next = commandActiveIndex + delta;
    if (next < 0) commandActiveIndex = commandVisibleActions.length - 1;
    else if (next >= commandVisibleActions.length) commandActiveIndex = 0;
    else commandActiveIndex = next;
    updateCommandActiveSelection();
  }

  function runCommandAction(index = commandActiveIndex) {
    const action = commandVisibleActions[index];
    if (!action) return;
    closeCommandPalette();
    Promise.resolve(action.run()).catch(() => showToast('Command failed.', 'error'));
  }

  function openCommandPalette() {
    const overlay = document.getElementById('cmd-overlay');
    const input = document.getElementById('cmd-input');
    if (!overlay || !input) return;
    commandPaletteOpen = true;
    commandQuery = '';
    commandActiveIndex = 0;
    input.value = '';
    overlay.classList.add('open');
    renderCommandPalette();
    setTimeout(() => input.focus(), 0);
  }

  function closeCommandPalette(event) {
    const overlay = document.getElementById('cmd-overlay');
    if (!overlay) return;
    if (event && event.target !== overlay) return;
    commandPaletteOpen = false;
    overlay.classList.remove('open');
  }

  function closeShortcutsOverlay(event) {
    const overlay = document.getElementById('shortcuts-overlay');
    if (!overlay) return;
    if (event && event.target !== overlay) return;
    overlay.classList.remove('open');
  }

  function toggleShortcutsOverlay() {
    const overlay = document.getElementById('shortcuts-overlay');
    if (!overlay) return;
    if (overlay.classList.contains('open')) overlay.classList.remove('open');
    else overlay.classList.add('open');
  }

  function fillSettingsForm() {
    if (!viewerSettings) return;
    const livePollEnabled = document.getElementById('settings-live-poll-enabled');
    const livePollInterval = document.getElementById('settings-live-poll-interval');
    const timeMode = document.getElementById('settings-time-mode');
    const defaultFilter = document.getElementById('settings-default-filter');
    const searchDebounce = document.getElementById('settings-search-debounce');
    const compactCards = document.getElementById('settings-compact-cards');
    const graphInferred = document.getElementById('settings-graph-inferred');
    const graphLabels = document.getElementById('settings-graph-labels');
    const graphPhysics = document.getElementById('settings-graph-physics');
    const graphFocus = document.getElementById('settings-graph-focus');
    const autoOpenGraph = document.getElementById('settings-auto-open-graph');
    const toastsEnabled = document.getElementById('settings-toasts-enabled');
    const toastDuration = document.getElementById('settings-toast-duration');
    const confirmLogout = document.getElementById('settings-confirm-logout');
    const showScanlines = document.getElementById('settings-show-scanlines');
    const reduceMotion = document.getElementById('settings-reduce-motion');
    const semanticWait = document.getElementById('settings-semantic-wait');
    const semanticTimeout = document.getElementById('settings-semantic-timeout');
    const semanticLimit = document.getElementById('settings-semantic-limit');
    if (livePollEnabled) livePollEnabled.checked = viewerSettings.live_poll_enabled;
    if (livePollInterval) livePollInterval.value = String(viewerSettings.live_poll_interval_sec);
    if (timeMode) timeMode.value = viewerSettings.time_mode;
    if (defaultFilter) defaultFilter.value = viewerSettings.default_memory_filter || '';
    if (searchDebounce) searchDebounce.value = String(viewerSettings.search_debounce_ms);
    if (compactCards) compactCards.checked = viewerSettings.compact_cards;
    if (graphInferred) graphInferred.checked = viewerSettings.graph_show_inferred;
    if (graphLabels) graphLabels.checked = viewerSettings.graph_show_labels;
    if (graphPhysics) graphPhysics.checked = viewerSettings.graph_physics_enabled;
    if (graphFocus) graphFocus.checked = viewerSettings.graph_focus_highlight;
    if (autoOpenGraph) autoOpenGraph.checked = viewerSettings.auto_open_graph;
    if (toastsEnabled) toastsEnabled.checked = viewerSettings.toasts_enabled;
    if (toastDuration) toastDuration.value = String(viewerSettings.toast_duration_ms);
    if (confirmLogout) confirmLogout.checked = viewerSettings.confirm_logout;
    if (showScanlines) showScanlines.checked = viewerSettings.show_scanlines;
    if (reduceMotion) reduceMotion.checked = viewerSettings.reduce_motion;
    if (semanticWait) semanticWait.checked = viewerSettings.semantic_reindex_wait_for_index;
    if (semanticTimeout) semanticTimeout.value = String(viewerSettings.semantic_reindex_wait_timeout_seconds);
    if (semanticLimit) semanticLimit.value = String(viewerSettings.semantic_reindex_limit);
    syncThemePicker();
    renderSemanticReindexStatus();
  }

  function readSettingsFromForm() {
    const raw = {
      theme: document.querySelector('.theme-swatch.active')?.dataset?.themeValue || viewerSettings?.theme || 'cyberpunk',
      live_poll_enabled: document.getElementById('settings-live-poll-enabled')?.checked !== false,
      live_poll_interval_sec: Number(document.getElementById('settings-live-poll-interval')?.value ?? 10),
      time_mode: document.getElementById('settings-time-mode')?.value === 'local' ? 'local' : 'utc',
      default_memory_filter: document.getElementById('settings-default-filter')?.value || '',
      search_debounce_ms: Number(document.getElementById('settings-search-debounce')?.value ?? 300),
      compact_cards: document.getElementById('settings-compact-cards')?.checked === true,
      graph_show_inferred: document.getElementById('settings-graph-inferred')?.checked !== false,
      graph_show_labels: document.getElementById('settings-graph-labels')?.checked !== false,
      graph_physics_enabled: document.getElementById('settings-graph-physics')?.checked !== false,
      graph_focus_highlight: document.getElementById('settings-graph-focus')?.checked !== false,
      auto_open_graph: document.getElementById('settings-auto-open-graph')?.checked === true,
      toasts_enabled: document.getElementById('settings-toasts-enabled')?.checked !== false,
      toast_duration_ms: Number(document.getElementById('settings-toast-duration')?.value ?? 2300),
      confirm_logout: document.getElementById('settings-confirm-logout')?.checked === true,
      show_scanlines: document.getElementById('settings-show-scanlines')?.checked !== false,
      reduce_motion: document.getElementById('settings-reduce-motion')?.checked === true,
      semantic_reindex_wait_for_index: document.getElementById('settings-semantic-wait')?.checked !== false,
      semantic_reindex_wait_timeout_seconds: Number(document.getElementById('settings-semantic-timeout')?.value ?? 180),
      semantic_reindex_limit: Number(document.getElementById('settings-semantic-limit')?.value ?? 500),
    };
    return normalizeViewerSettings(raw);
  }

  function closeSettingsOverlay(event) {
    const overlay = document.getElementById('settings-overlay');
    if (!overlay) return;
    if (event && event.target !== overlay) return;
    overlay.classList.remove('open');
  }

  function openSettingsOverlay() {
    const overlay = document.getElementById('settings-overlay');
    if (!overlay) return;
    fillSettingsForm();
    overlay.classList.add('open');
  }

  function closeChangelogOverlay(event) {
    const overlay = document.getElementById('changelog-overlay');
    if (!overlay) return;
    if (event && event.target !== overlay) return;
    overlay.classList.remove('open');
  }

  function formatChangelogDate(unixTs) {
    const ts = Number(unixTs);
    if (!Number.isFinite(ts) || ts <= 0) return 'Unknown date';
    return new Date(ts * 1000).toISOString().slice(0, 10);
  }

  function renderChangelogEntries(entries, latestVersion) {
    const list = document.getElementById('changelog-list');
    const subtitle = document.getElementById('changelog-subtitle');
    if (!list || !subtitle) return;
    const rows = Array.isArray(entries) ? entries : [];
    const latest = typeof latestVersion === 'string' && latestVersion.trim()
      ? latestVersion.trim()
      : VIEWER_SERVER_VERSION;
    subtitle.textContent = 'Latest version: v' + latest + ' - showing ' + rows.length + ' entries';
    if (!rows.length) {
      list.innerHTML = '<div class="setting-help">No changelog entries available.</div>';
      return;
    }

    list.innerHTML = rows.map((entry) => {
      const version = typeof entry.version === 'string' && entry.version.trim() ? entry.version.trim() : 'unknown';
      const summary = typeof entry.summary === 'string' ? entry.summary : '';
      const releaseDate = formatChangelogDate(entry.released_at);
      const changes = Array.isArray(entry.changes) ? entry.changes : [];
      const changesHtml = changes.slice(0, 16).map((change) => {
        const type = typeof change.type === 'string' && change.type.trim() ? change.type.trim() : 'changed';
        const target = typeof change.target === 'string' && change.target.trim() ? change.target.trim() : '';
        const name = typeof change.name === 'string' && change.name.trim() ? change.name.trim() : 'Untitled change';
        const description = typeof change.description === 'string' && change.description.trim() ? change.description.trim() : '';
        const prefix = target ? (target + ': ') : '';
        const detail = prefix + name + (description ? (' - ' + description) : '');
        return '<li class="changelog-change-row">' +
          '<span class="changelog-change-type">' + esc(type) + '</span>' +
          '<span class="changelog-change-text">' + esc(detail) + '</span>' +
        '</li>';
      }).join('');
      return '<article class="changelog-entry">' +
        '<div class="changelog-entry-head">' +
          '<span class="changelog-entry-version">v' + esc(version) + '</span>' +
          '<span class="changelog-entry-date">' + esc(releaseDate) + '</span>' +
        '</div>' +
        '<div class="changelog-entry-summary">' + esc(summary || 'No summary available.') + '</div>' +
        (changesHtml ? ('<ul class="changelog-change-list">' + changesHtml + '</ul>') : '') +
      '</article>';
    }).join('');
  }

  async function loadChangelogEntries() {
    const list = document.getElementById('changelog-list');
    const subtitle = document.getElementById('changelog-subtitle');
    if (!list || !subtitle) return;
    list.innerHTML = '<div class="setting-help">Loading changelog...</div>';
    subtitle.textContent = 'Fetching latest release notes...';
    try {
      const response = await apiFetch(BASE + '/mcp', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: 'viewer-changelog',
          method: 'tools/call',
          params: {
            name: 'tool_changelog',
            arguments: { limit: 12 },
          },
        }),
      });
      if (response.status === 401) {
        doLogout(true);
        return;
      }
      if (!response.ok) throw new Error('Failed to load changelog.');
      const rpc = await response.json();
      if (rpc && rpc.error) throw new Error(typeof rpc.error.message === 'string' ? rpc.error.message : 'Failed to load changelog.');
      const text = rpc?.result?.content?.[0]?.text;
      if (typeof text !== 'string' || !text.trim()) throw new Error('Invalid changelog response.');
      const parsed = JSON.parse(text);
      renderChangelogEntries(parsed?.entries, parsed?.latest_version);
    } catch (err) {
      const message = err instanceof Error && err.message ? err.message : 'Failed to load changelog.';
      subtitle.textContent = 'Unable to load release notes.';
      list.innerHTML = '<div class="setting-help" style="color:var(--red)">' + esc(message) + '</div>';
    }
  }

  async function openChangelogOverlay() {
    closeSettingsOverlay();
    const overlay = document.getElementById('changelog-overlay');
    if (!overlay) return;
    overlay.classList.add('open');
    await loadChangelogEntries();
  }

  function applySettingsFromForm() {
    viewerSettings = readSettingsFromForm();
    persistViewerSettings();
    applyViewerSettingsToRuntime({ restartPolling: true, rerenderGraph: true, rerenderGrid: true });
    updateTime();
    closeSettingsOverlay();
    showToast('Settings saved.', 'success', true);
  }

  function resetViewerSettings() {
    viewerSettings = buildDefaultViewerSettings();
    persistViewerSettings();
    fillSettingsForm();
    applyViewerSettingsToRuntime({ restartPolling: true, rerenderGraph: true, rerenderGrid: true });
    updateTime();
    showToast('Settings reset to defaults.', 'info', true);
  }

  function syncThemePicker() {
    const current = viewerSettings?.theme || 'cyberpunk';
    document.querySelectorAll('.theme-swatch').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.themeValue === current);
    });
  }

  function syncFilterPills(type) {
    ['all','note','fact','journal','graph'].forEach(t => {
      document.getElementById('stat-' + t).classList.toggle('active', (type === '' ? 'all' : type) === t);
    });
  }

  function setFilter(type) {
    graphVisible = false;
    const graphView = document.getElementById('graph-view');
    graphView.classList.remove('visible');
    graphView.style.display = 'none';
    document.querySelector('.grid-wrap').style.display = 'grid';
    activeFilter = type;
    syncFilterPills(type);
    loadMemories();
  }

  function onSearch(val) {
    clearTimeout(searchTimeout);
    const debounceMs = Math.min(Math.max(Number(viewerSettings?.search_debounce_ms ?? 300), 120), 1500);
    searchTimeout = setTimeout(loadMemories, debounceMs);
  }

  function esc(str) {
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  function expandById(id) {
    const idx = allMemories.findIndex(m => m.id === id);
    if (idx !== -1) {
      expandCard(idx);
    } else {
      // Memory not found in current view (may be filtered out or not yet loaded)
      const connEl = document.getElementById('expand-connections');
      if (connEl) {
        const note = document.createElement('div');
        note.style.cssText = 'font-size:0.65rem;color:var(--text-dim);letter-spacing:0.1em;margin-top:0.5rem';
        note.textContent = '⚠ Linked memory not visible in current filter.';
        const existing = connEl.querySelector('.connections-section');
        if (existing) {
          existing.appendChild(note);
        } else {
          connEl.appendChild(note);
        }
      }
    }
  }

  let lastPollSig = '';
  let pollIntervalId = null;

  function startLivePolling(forceRestart = false) {
    if (forceRestart && pollIntervalId) {
      clearInterval(pollIntervalId);
      pollIntervalId = null;
    }
    const liveEl = document.getElementById('live-indicator');
    const pollingEnabled = !viewerSettings || viewerSettings.live_poll_enabled;
    if (!pollingEnabled) {
      if (liveEl) liveEl.style.display = 'none';
      return;
    }
    if (pollIntervalId) return;
    if (liveEl) liveEl.style.display = 'flex';
    const intervalMs = Math.min(Math.max((viewerSettings?.live_poll_interval_sec ?? 10) * 1000, 5000), 120000);
    pollIntervalId = setInterval(async () => {
      if (!hasAuthenticatedSession()) return;
      try {
        const r = await apiFetch(BASE + '/api/memories?limit=1');
        if (!r.ok) return;
        const data = await r.json();
        const sig = (data.stats || []).map(s => s.type + ':' + s.count).join('|');
        if (lastPollSig && sig !== lastPollSig) {
          loadMemories(true); // silent refresh
        }
        lastPollSig = sig;
      } catch {}
    }, intervalMs);
  }

  function syncGraphToolbarState() {
    const inferredBtn = document.getElementById('graph-toggle-inferred');
    const labelsBtn = document.getElementById('graph-toggle-labels');
    const physicsBtn = document.getElementById('graph-toggle-physics');
    if (inferredBtn) {
      inferredBtn.classList.toggle('active', graphShowInferred);
      inferredBtn.classList.toggle('off', !graphShowInferred);
      inferredBtn.textContent = graphShowInferred ? 'INFERRED ON' : 'INFERRED OFF';
    }
    if (labelsBtn) {
      labelsBtn.classList.toggle('active', graphShowLabels);
      labelsBtn.classList.toggle('off', !graphShowLabels);
      labelsBtn.textContent = graphShowLabels ? 'LABELS ON' : 'LABELS OFF';
    }
    if (physicsBtn) {
      physicsBtn.classList.toggle('active', graphPhysicsEnabled);
      physicsBtn.classList.toggle('off', !graphPhysicsEnabled);
      physicsBtn.textContent = graphPhysicsEnabled ? 'PHYSICS ON' : 'PHYSICS OFF';
    }
    GRAPH_RELATION_TYPES.forEach((relation) => {
      const btn = document.getElementById('graph-rel-' + relation);
      if (!btn) return;
      const active = graphRelationFilter.has(relation);
      btn.classList.toggle('active', active);
      btn.classList.toggle('off', !active);
    });
  }

  function onGraphSearch(value) {
    graphSearchQuery = String(value || '').trim().toLowerCase();
    if (graphVisible) rerenderGraphFromCache();
  }

  function toggleGraphRelation(relation) {
    if (!GRAPH_RELATION_TYPES.includes(relation)) return;
    if (graphRelationFilter.has(relation)) {
      if (graphRelationFilter.size === 1) return;
      graphRelationFilter.delete(relation);
    } else {
      graphRelationFilter.add(relation);
    }
    syncGraphToolbarState();
    if (graphVisible) rerenderGraphFromCache();
  }

  function updateGraphLegend(nodesCount, explicitCount, inferredVisibleCount, inferredTotal, relationCounts = {}, avgConfidence = null, avgImportance = null, matchCount = null) {
    const legend = document.getElementById('graph-legend');
    if (!legend) return;
    const inferredText = graphShowInferred
      ? \`INFERRED \${inferredVisibleCount}/\${inferredTotal}\`
      : \`INFERRED OFF (\${inferredTotal} AVAIL)\`;
    const relationPriority = ['contradicts', 'supports', 'supersedes', 'causes', 'example_of'];
    const relationText = relationPriority
      .filter((key) => relationCounts[key] > 0)
      .slice(0, 2)
      .map((key) => \`\${key.toUpperCase().replace('_', ' ')} \${relationCounts[key]}\`)
      .join(' · ');
    const avgConfText = avgConfidence === null ? '' : \`<span class="graph-legend-item">AVG CONF \${Math.round(avgConfidence * 100)}%</span>\`;
    const avgImpText = avgImportance === null ? '' : \`<span class="graph-legend-item">AVG IMP \${Math.round(avgImportance * 100)}%</span>\`;
    const matchText = matchCount === null ? '' : \`<span class="graph-legend-item">MATCH \${matchCount}</span>\`;
    legend.innerHTML = \`
      <span class="graph-legend-item">NODES \${nodesCount}</span>
      <span class="graph-legend-item">LINKS \${explicitCount}</span>
      <span class="graph-legend-item">\${inferredText}</span>
      \${relationText ? \`<span class="graph-legend-item">\${relationText}</span>\` : ''}
      \${avgConfText}
      \${avgImpText}
      \${matchText}
    \`;
  }

  function cloneGraphData() {
    return {
      nodes: (lastGraphData.nodes || []).map(n => ({ ...n })),
      edges: (lastGraphData.edges || []).map(e => ({ ...e })),
      inferred_edges: (lastGraphData.inferred_edges || []).map(e => ({ ...e })),
    };
  }

  function rerenderGraphFromCache() {
    const data = cloneGraphData();
    renderGraph(data.nodes, data.edges, data.inferred_edges);
  }

  function toggleGraphInferred() {
    graphShowInferred = !graphShowInferred;
    syncGraphToolbarState();
    if (graphVisible) rerenderGraphFromCache();
    showToast(graphShowInferred ? 'Inferred edges enabled.' : 'Inferred edges disabled.', 'info');
  }

  function toggleGraphLabels() {
    graphShowLabels = !graphShowLabels;
    syncGraphToolbarState();
    if (graphVisible) rerenderGraphFromCache();
    showToast(graphShowLabels ? 'Graph labels enabled.' : 'Graph labels hidden.', 'info');
  }

  function toggleGraphPhysics() {
    graphPhysicsEnabled = !graphPhysicsEnabled;
    syncGraphToolbarState();
    if (!graphSimulation) return;
    if (graphPhysicsEnabled) {
      graphSimulation.alpha(0.55).restart();
    } else {
      graphSimulation.stop();
    }
    showToast(graphPhysicsEnabled ? 'Graph physics resumed.' : 'Graph physics paused.', 'info');
  }

  function resetGraphView() {
    if (!graphSvgSelection || !graphZoomBehavior) return;
    graphSvgSelection.transition().duration(220).call(graphZoomBehavior.transform, d3.zoomIdentity);
    graphRelationFilter = new Set(GRAPH_RELATION_TYPES);
    graphSearchQuery = '';
    const searchInput = document.getElementById('graph-search-input');
    if (searchInput) searchInput.value = '';
    if (graphPhysicsEnabled && graphSimulation) graphSimulation.alpha(0.45).restart();
    syncGraphToolbarState();
    rerenderGraphFromCache();
    showToast('Graph view reset.', 'success');
  }

  async function showGraph() {
    graphVisible = true;
    syncGraphToolbarState();
    ['all','note','fact','journal'].forEach(t => {
      document.getElementById('stat-' + t).classList.remove('active');
    });
    document.getElementById('stat-graph').classList.add('active');
    document.querySelector('.grid-wrap').style.display = 'none';
    const graphView = document.getElementById('graph-view');
    graphView.classList.remove('visible');
    graphView.style.display = 'block';
    requestAnimationFrame(() => graphView.classList.add('visible'));
    const emptyEl = document.getElementById('graph-empty');
    if (emptyEl) emptyEl.style.display = 'none';
    const legendEl = document.getElementById('graph-legend');
    if (legendEl) legendEl.innerHTML = '';

    const svg = document.getElementById('graph-svg');
    svg.innerHTML = '<text x="50%" y="50%" text-anchor="middle" style="fill:var(--amber);font-family:var(--mono);font-size:0.7rem;letter-spacing:0.15em">LOADING GRAPH...</text>';

    try {
      const r = await apiFetch(BASE + '/api/graph');
      if (r.status === 401) { doLogout(true); return; }
      if (!r.ok) throw new Error('failed');
      const data = await r.json();
      lastGraphData = {
        nodes: (data.nodes || []).map(n => ({ ...n })),
        edges: (data.edges || []).map(e => ({ ...e })),
        inferred_edges: (data.inferred_edges || []).map(e => ({ ...e })),
      };
      if (!graphAutoTunedLabels && (lastGraphData.edges.length + lastGraphData.inferred_edges.length) > 80) {
        graphShowLabels = false;
        graphAutoTunedLabels = true;
      }
      syncGraphToolbarState();
      rerenderGraphFromCache();
      showToast('Graph loaded: ' + lastGraphData.nodes.length + ' nodes.', 'success');
    } catch(e) {
      document.getElementById('graph-svg').innerHTML = '<text x="50%" y="50%" text-anchor="middle" style="fill:var(--red);font-family:var(--mono);font-size:0.7rem;letter-spacing:0.15em">ERROR LOADING GRAPH</text>';
      showToast('Graph load failed.', 'error');
    }
  }

  function renderGraph(nodes, edges, inferredEdges = []) {
    const svgEl = document.getElementById('graph-svg');
    const emptyEl = document.getElementById('graph-empty');
    svgEl.innerHTML = '';
    if (emptyEl) emptyEl.style.display = 'none';

    if (!nodes.length) {
      const legendEl = document.getElementById('graph-legend');
      if (legendEl) legendEl.innerHTML = '';
      if (emptyEl) { emptyEl.style.display = 'flex'; }
      return;
    }

    const width = svgEl.clientWidth || 800;
    const height = svgEl.clientHeight || 600;
    const isMobile = window.matchMedia('(max-width: 640px)').matches;
    const typeColor = { note: '#00c8b4', fact: '#f0a500', journal: '#8888ff' };
    const relationDistance = {
      related: isMobile ? 88 : 112,
      supports: isMobile ? 94 : 118,
      contradicts: isMobile ? 106 : 132,
      supersedes: isMobile ? 96 : 120,
      causes: isMobile ? 100 : 126,
      example_of: isMobile ? 90 : 114,
    };

    const nodeMap = new Map(nodes.map(n => [n.id, n]));
    const explicitLinks = edges
      .map((e) => {
        const relation = String(e.relation_type || 'related').toLowerCase();
        return { ...e, source: e.from_id, target: e.to_id, kind: 'explicit', relation_type: relation };
      })
      .filter((e) => nodeMap.has(e.source) && nodeMap.has(e.target))
      .filter((e) => graphRelationFilter.has(e.relation_type));
    const inferredCandidates = graphShowInferred
      ? inferredEdges
        .map((e) => ({
          ...e,
          source: e.from_id,
          target: e.to_id,
          kind: 'inferred',
          score: Number.isFinite(Number(e.score)) ? Number(e.score) : 0,
          strength: Number.isFinite(Number(e.strength)) ? Number(e.strength) : 1,
        }))
        .filter((e) => nodeMap.has(e.source) && nodeMap.has(e.target))
      : [];

    inferredCandidates.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return b.strength - a.strength;
    });
    const inferredPerNodeLimit = isMobile ? 3 : 5;
    const inferredMaxVisible = isMobile ? 120 : 220;
    const inferredNodeDegree = new Map();
    const inferredLinks = [];
    for (const edge of inferredCandidates) {
      if (inferredLinks.length >= inferredMaxVisible) break;
      if (edge.strength < 2 && edge.score < 0.85) continue;
      const fromDeg = inferredNodeDegree.get(edge.source) || 0;
      const toDeg = inferredNodeDegree.get(edge.target) || 0;
      if (fromDeg >= inferredPerNodeLimit || toDeg >= inferredPerNodeLimit) continue;
      inferredLinks.push(edge);
      inferredNodeDegree.set(edge.source, fromDeg + 1);
      inferredNodeDegree.set(edge.target, toDeg + 1);
    }
    const links = [...explicitLinks, ...inferredLinks];

    const normalizedSearch = graphSearchQuery.trim().toLowerCase();
    const matchingNodeIds = new Set();
    if (normalizedSearch) {
      nodes.forEach((n) => {
        const haystack = [
          n.title || '',
          n.key || '',
          n.content || '',
          n.tags || '',
          n.source || '',
        ].join(' ').toLowerCase();
        if (haystack.includes(normalizedSearch)) matchingNodeIds.add(n.id);
      });
    }
    const hasSearch = normalizedSearch.length > 0;
    const isNodeVisible = (id) => !hasSearch || matchingNodeIds.has(id);

    const degreeById = new Map();
    links.forEach((l) => {
      degreeById.set(l.source, (degreeById.get(l.source) || 0) + 1);
      degreeById.set(l.target, (degreeById.get(l.target) || 0) + 1);
    });
    const neighborhoodByNode = new Map();
    links.forEach((l) => {
      const fromId = String(l.source);
      const toId = String(l.target);
      const fromSet = neighborhoodByNode.get(fromId) || new Set();
      fromSet.add(toId);
      neighborhoodByNode.set(fromId, fromSet);
      const toSet = neighborhoodByNode.get(toId) || new Set();
      toSet.add(fromId);
      neighborhoodByNode.set(toId, toSet);
    });
    const baseNodeOpacity = (d) => {
      const confidence = Math.min(Math.max(Number.isFinite(Number(d.dynamic_confidence ?? d.confidence)) ? Number(d.dynamic_confidence ?? d.confidence) : 0.7, 0), 1);
      const visible = isNodeVisible(d.id);
      const baseOpacity = 0.42 + confidence * 0.5;
      return visible ? baseOpacity : Math.max(0.08, baseOpacity * 0.25);
    };
    const baseNodeStrokeOpacity = (d) => isNodeVisible(d.id) ? 1 : 0.2;
    const baseNodeTextOpacity = (d) => isNodeVisible(d.id) ? 1 : 0.2;

    const inferredHeavy = inferredLinks.length > explicitLinks.length;
    const simulation = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links).id(d => d.id).distance((d) => {
        if (d.kind === 'inferred') {
          const score = Math.min(Math.max(Number(d.score) || 0, 0), 1);
          const minDist = isMobile ? 92 : 116;
          const maxDist = isMobile ? 130 : 168;
          return maxDist - score * (maxDist - minDist);
        }
        return relationDistance[d.relation_type] ?? (isMobile ? 96 : 120);
      }).strength((d) => {
        if (d.kind === 'inferred') {
          const score = Math.min(Math.max(Number(d.score) || 0, 0), 1);
          return 0.018 + (score * 0.03);
        }
        if (d.relation_type === 'supports') return 0.5;
        if (d.relation_type === 'contradicts') return 0.35;
        if (d.relation_type === 'supersedes') return 0.55;
        if (d.relation_type === 'causes') return 0.45;
        if (d.relation_type === 'example_of') return 0.42;
        return 0.4;
      }))
      .force('charge', d3.forceManyBody().strength(isMobile ? (inferredHeavy ? -300 : -220) : (inferredHeavy ? -420 : -300)))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('x', d3.forceX((d) => {
        if (isMobile) return width / 2;
        const lane = d.type === 'note' ? 1 : (d.type === 'fact' ? 2 : 3);
        return (width / 4) * lane;
      }).strength(isMobile ? 0.01 : 0.035))
      .force('y', d3.forceY(height / 2).strength(isMobile ? 0.01 : 0.03))
      .force('collision', d3.forceCollide(isMobile ? (inferredHeavy ? 27 : 24) : (inferredHeavy ? 34 : 30)));
    graphSimulation = simulation;
    if (!graphPhysicsEnabled) simulation.stop();

    const svg = d3.select('#graph-svg');
    graphSvgSelection = svg;
    const defs = svg.append('defs');
    Object.entries(GRAPH_RELATION_COLOR).forEach(([relation, color]) => {
      const markerId = 'arrow-' + relation.replace(/_/g, '-');
      defs.append('marker')
        .attr('id', markerId)
        .attr('viewBox', '0 0 10 10')
        .attr('refX', 13)
        .attr('refY', 5)
        .attr('markerWidth', 7)
        .attr('markerHeight', 7)
        .attr('orient', 'auto-start-reverse')
        .append('path')
        .attr('d', 'M 0 0 L 10 5 L 0 10 z')
        .attr('fill', color);
    });

    const relationCounts = {};
    explicitLinks.forEach((edge) => {
      const key = String(edge.relation_type || 'related');
      relationCounts[key] = (relationCounts[key] || 0) + 1;
    });
    const confidenceVals = nodes.map((n) => Number(n.dynamic_confidence ?? n.confidence)).filter((n) => Number.isFinite(n));
    const importanceVals = nodes.map((n) => Number(n.dynamic_importance ?? n.importance)).filter((n) => Number.isFinite(n));
    const avgConfidence = confidenceVals.length ? confidenceVals.reduce((a, b) => a + b, 0) / confidenceVals.length : null;
    const avgImportance = importanceVals.length ? importanceVals.reduce((a, b) => a + b, 0) / importanceVals.length : null;
    updateGraphLegend(
      nodes.length,
      explicitLinks.length,
      inferredLinks.length,
      inferredEdges.length,
      relationCounts,
      avgConfidence,
      avgImportance,
      hasSearch ? matchingNodeIds.size : null
    );
    const g = svg.append('g');

    const zoom = d3.zoom().scaleExtent([0.2, 4]).on('zoom', (event) => {
      g.attr('transform', event.transform);
    });
    graphZoomBehavior = zoom;
    svg.call(zoom);

    const getEndpointId = (endpoint) => (typeof endpoint === 'string' ? endpoint : (endpoint && endpoint.id ? endpoint.id : ''));
    const linkOpacity = (d) => {
      if (!hasSearch) return d.kind === 'inferred' ? 0.4 : 0.9;
      const sId = getEndpointId(d.source);
      const tId = getEndpointId(d.target);
      const match = matchingNodeIds.has(sId) || matchingNodeIds.has(tId);
      return match ? (d.kind === 'inferred' ? 0.55 : 1) : 0.06;
    };

    const link = g.append('g').selectAll('line')
      .data(links).join('line').attr('class', d => {
        if (d.kind !== 'explicit') return 'graph-link inferred';
        const relationClass = String(d.relation_type || 'related').replace(/_/g, '-').replace(/[^a-z-]/g, '').toLowerCase();
        return \`graph-link explicit relation-\${relationClass}\`;
      })
      .attr('marker-end', (d) => {
        if (d.kind !== 'explicit') return null;
        const relationClass = String(d.relation_type || 'related').replace(/_/g, '-').replace(/[^a-z-]/g, '').toLowerCase();
        return \`url(#arrow-\${relationClass})\`;
      })
      .attr('stroke-width', (d) => {
        if (d.kind !== 'inferred') return 1.5;
        const score = Math.min(Math.max(Number(d.score) || 0, 0), 1);
        return 0.8 + score * 0.7;
      })
      .attr('stroke-opacity', linkOpacity);

    const linkLabel = g.append('g').selectAll('text')
      .data(links).join('text').attr('class', 'graph-link-label')
      .style('display', graphShowLabels ? null : 'none')
      .style('opacity', (d) => linkOpacity(d) >= 0.5 ? 1 : 0)
      .text(d => {
        if (d.kind !== 'explicit') return '';
        if (d.label) return d.label;
        if (d.relation_type && d.relation_type !== 'related') return String(d.relation_type).replace('_', ' ');
        return '';
      });

    const node = g.append('g').selectAll('g')
      .data(nodes).join('g').attr('class', 'graph-node')
      .call(d3.drag()
        .on('start', (event, d) => { if (!event.active) simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
        .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
        .on('end', (event, d) => { if (!event.active) simulation.alphaTarget(0); d.fx = null; d.fy = null; })
      )
      .on('click', (event, d) => { expandById(d.id); });

    node.append('circle')
      .attr('r', d => {
        const degree = degreeById.get(d.id) || 0;
        const base = isMobile ? 8 : 6;
        const maxR = isMobile ? 17 : 15;
        const importance = Math.min(Math.max(Number.isFinite(Number(d.dynamic_importance ?? d.importance)) ? Number(d.dynamic_importance ?? d.importance) : 0.5, 0), 1);
        return Math.min(maxR, base + degree * 0.4 + importance * (isMobile ? 4.2 : 3.6));
      })
      .attr('fill', d => typeColor[d.type] || '#888')
      .attr('fill-opacity', baseNodeOpacity)
      .attr('stroke', d => typeColor[d.type] || '#888')
      .attr('stroke-opacity', baseNodeStrokeOpacity)
      .attr('stroke-width', (d) => {
        const importance = Math.min(Math.max(Number.isFinite(Number(d.dynamic_importance ?? d.importance)) ? Number(d.dynamic_importance ?? d.importance) : 0.5, 0), 1);
        return 1.4 + importance * 1.6;
      });

    node.append('text')
      .attr('dx', 12).attr('dy', 4)
      .style('opacity', baseNodeTextOpacity)
      .text(d => (d.title || d.key || d.content || '').slice(0, isMobile ? 18 : 24));

    const applyGraphFocus = (focusId) => {
      if (viewerSettings && viewerSettings.graph_focus_highlight === false) {
        focusId = '';
      }
      if (!focusId) {
        link.attr('stroke-opacity', linkOpacity);
        linkLabel.style('opacity', (d) => linkOpacity(d) >= 0.5 ? 1 : 0);
        node.select('circle')
          .attr('fill-opacity', (d) => baseNodeOpacity(d))
          .attr('stroke-opacity', (d) => baseNodeStrokeOpacity(d));
        node.select('text').style('opacity', (d) => baseNodeTextOpacity(d));
        return;
      }

      const neighborSet = neighborhoodByNode.get(focusId) ?? new Set();
      const focusSet = new Set([focusId]);
      neighborSet.forEach((neighborId) => focusSet.add(neighborId));
      const isFocusedNode = (id) => focusSet.has(String(id));
      const isFocusedEdge = (d) => {
        const sId = getEndpointId(d.source);
        const tId = getEndpointId(d.target);
        return isFocusedNode(sId) && isFocusedNode(tId);
      };

      link.attr('stroke-opacity', (d) => {
        if (!isFocusedEdge(d)) return 0.04;
        const base = linkOpacity(d);
        if (d.kind === 'inferred') return Math.max(base, 0.58);
        return Math.max(base, 1);
      });

      linkLabel.style('opacity', (d) => {
        if (!graphShowLabels) return 0;
        return isFocusedEdge(d) ? 1 : 0;
      });

      node.select('circle')
        .attr('fill-opacity', (d) => {
          const id = String(d.id);
          if (id === focusId) return 1;
          if (focusSet.has(id)) return Math.max(baseNodeOpacity(d), 0.78);
          return Math.min(baseNodeOpacity(d), 0.1);
        })
        .attr('stroke-opacity', (d) => {
          const id = String(d.id);
          if (id === focusId) return 1;
          if (focusSet.has(id)) return 0.95;
          return 0.12;
        });

      node.select('text').style('opacity', (d) => {
        const id = String(d.id);
        if (id === focusId) return 1;
        if (focusSet.has(id)) return 0.95;
        return 0.1;
      });
    };

    node
      .on('mouseenter', (event, d) => { applyGraphFocus(String(d.id)); })
      .on('mouseleave', () => { applyGraphFocus(''); });

    node.append('title').text((d) => {
      const label = d.title || d.key || (d.content || '').slice(0, 70) || d.id;
      const confidence = Math.round(Math.min(Math.max(Number(d.dynamic_confidence ?? d.confidence) || 0.7, 0), 1) * 100);
      const importance = Math.round(Math.min(Math.max(Number(d.dynamic_importance ?? d.importance) || 0.5, 0), 1) * 100);
      const source = d.source ? \`\\nsource: \${d.source}\` : '';
      return \`\${label}\\nconfidence: \${confidence}%\\nimportance: \${importance}%\${source}\`;
    });

    simulation.on('tick', () => {
      link
        .attr('x1', d => d.source.x).attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
      linkLabel
        .attr('x', d => (d.source.x + d.target.x) / 2)
        .attr('y', d => (d.source.y + d.target.y) / 2);
      node.attr('transform', d => \`translate(\${d.x},\${d.y})\`);
    });
  }

  window.addEventListener('resize', () => {
    clearTimeout(graphResizeTimer);
    graphResizeTimer = setTimeout(() => {
      if (!graphVisible) return;
      rerenderGraphFromCache();
    }, 120);
  });

  function bindViewerEventHandlers() {
    const bindInput = (id, handler) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.addEventListener('input', (event) => {
        const target = event.target;
        handler(target && typeof target.value === 'string' ? target.value : '');
      });
    };

    document.addEventListener('click', (event) => {
      const target = event.target instanceof Element ? event.target.closest('[data-action]') : null;
      if (!target) return;
      const action = target.getAttribute('data-action') || '';

      switch (action) {
        case 'login':
          doCredentialAuth('login');
          break;
        case 'signup':
          doCredentialAuth('signup');
          break;
        case 'token-login':
          doTokenLogin();
          break;
        case 'logout':
          doLogout();
          break;
        case 'set-filter':
          setFilter(target.getAttribute('data-filter') || '');
          break;
        case 'show-graph':
          showGraph();
          break;
        case 'refresh-memories':
          loadMemories();
          break;
        case 'open-command-palette':
          openCommandPalette();
          break;
        case 'toggle-shortcuts-overlay':
          toggleShortcutsOverlay();
          break;
        case 'open-settings-overlay':
          openSettingsOverlay();
          break;
        case 'toggle-graph-inferred':
          toggleGraphInferred();
          break;
        case 'toggle-graph-labels':
          toggleGraphLabels();
          break;
        case 'toggle-graph-physics':
          toggleGraphPhysics();
          break;
        case 'reset-graph-view':
          resetGraphView();
          break;
        case 'toggle-graph-relation':
          toggleGraphRelation(target.getAttribute('data-relation') || '');
          break;
        case 'close-expand-overlay':
          closeExpand(event);
          break;
        case 'close-expand':
          closeExpandBtn();
          break;
        case 'close-command-palette-overlay':
          closeCommandPalette(event);
          break;
        case 'close-shortcuts-overlay':
          closeShortcutsOverlay(event);
          break;
        case 'close-shortcuts':
          closeShortcutsOverlay();
          break;
        case 'close-settings-overlay':
          closeSettingsOverlay(event);
          break;
        case 'close-settings':
          closeSettingsOverlay();
          break;
        case 'run-semantic-reindex':
          runSemanticReindexFromSettings();
          break;
        case 'open-changelog-overlay':
          openChangelogOverlay();
          break;
        case 'reset-viewer-settings':
          resetViewerSettings();
          break;
        case 'apply-settings':
          applySettingsFromForm();
          break;
        case 'close-changelog-overlay':
          closeChangelogOverlay(event);
          break;
        case 'close-changelog':
          closeChangelogOverlay();
          break;
        case 'dismiss-update-banner':
          dismissUpdateBanner();
          break;
        case 'open-full-changelog':
          window.open('https://github.com/guirguispierre/memoryvault/blob/main/CHANGELOG.md', '_blank', 'noopener');
          break;
        case 'expand-card':
          expandCard(Number(target.getAttribute('data-card-index') || target.getAttribute('data-idx') || '-1'));
          break;
        default:
          break;
      }
    });

    document.addEventListener('click', (event) => {
      const target = event.target instanceof Element ? event.target.closest('.theme-swatch') : null;
      if (!target) return;
      const themeValue = target.getAttribute('data-theme-value');
      if (!themeValue) return;
      viewerSettings = readSettingsFromForm();
      viewerSettings.theme = themeValue;
      persistViewerSettings();
      applyViewerSettingsToRuntime({ restartPolling: false, rerenderGraph: false, rerenderGrid: false });
    });

    bindInput('search-input', onSearch);
    bindInput('graph-search-input', onGraphSearch);
    bindInput('cmd-input', onCommandFilter);
  }

  syncGraphToolbarState();
  bindViewerEventHandlers();

  // Enter key on login
  document.getElementById('token-input').addEventListener('keydown', e => { if (e.key === 'Enter') doTokenLogin(); });
  document.getElementById('email-input').addEventListener('keydown', e => { if (e.key === 'Enter') doCredentialAuth('login'); });
  document.getElementById('password-input').addEventListener('keydown', e => { if (e.key === 'Enter') doCredentialAuth('login'); });
  document.getElementById('brain-name-input').addEventListener('keydown', e => { if (e.key === 'Enter') doCredentialAuth('signup'); });
  document.getElementById('cmd-input').addEventListener('keydown', e => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      moveCommandSelection(1);
      return;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      moveCommandSelection(-1);
      return;
    }
    if (e.key === 'Enter') {
      e.preventDefault();
      runCommandAction();
      return;
    }
    if (e.key === 'Escape') {
      e.preventDefault();
      closeCommandPalette();
    }
  });
  document.addEventListener('keydown', e => {
    const key = String(e.key || '').toLowerCase();
    const shortcutsOpen = document.getElementById('shortcuts-overlay').classList.contains('open');
    const settingsOpen = document.getElementById('settings-overlay').classList.contains('open');
    const changelogOpen = document.getElementById('changelog-overlay').classList.contains('open');
    const expandOpen = document.getElementById('expand-overlay').classList.contains('open');
    const typing = isTypingTarget(e.target);

    if ((e.ctrlKey || e.metaKey) && key === 'k') {
      e.preventDefault();
      if (commandPaletteOpen) closeCommandPalette();
      else openCommandPalette();
      return;
    }

    if (commandPaletteOpen) {
      if (key === 'escape') {
        e.preventDefault();
        closeCommandPalette();
      }
      return;
    }

    if (shortcutsOpen) {
      if (key === 'escape') {
        e.preventDefault();
        closeShortcutsOverlay();
      }
      return;
    }

    if (changelogOpen) {
      if (key === 'escape') {
        e.preventDefault();
        closeChangelogOverlay();
      }
      return;
    }

    if (settingsOpen) {
      if (key === 'escape') {
        e.preventDefault();
        closeSettingsOverlay();
      }
      return;
    }

    if (e.key === '?' && !typing) {
      e.preventDefault();
      toggleShortcutsOverlay();
      return;
    }

    if (key === 'escape' && expandOpen) {
      e.preventDefault();
      closeExpandBtn();
      return;
    }

    if (typing) return;
    if (!hasAuthenticatedSession() || !appIsVisible()) return;

    if (key === '/') {
      e.preventDefault();
      const input = document.getElementById('search-input');
      if (!input) return;
      input.focus();
      input.select();
      return;
    }
    if (key === 'g') {
      e.preventDefault();
      showGraph();
      return;
    }
    if (key === 's') {
      e.preventDefault();
      openSettingsOverlay();
      return;
    }
    if (key === 'r') {
      e.preventDefault();
      loadMemories();
    }
  });
`;
}
