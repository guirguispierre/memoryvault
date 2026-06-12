export const baseStyles = `  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

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
  .login-btn:hover { filter: brightness(1.15); }
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
  .stat-pill:hover, .stat-pill.active { background: var(--bg3); transform: translateY(-2px); box-shadow: inset 0 -1px 0 var(--amber-glow); }
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
    box-shadow: 0 10px 20px var(--card-glow);
  }
  .card:hover::before { transform: translateX(140%); }
  .card-type-stripe {
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 3px;
  }
  .card[data-type="note"] .card-type-stripe { background: var(--teal); }
  .card[data-type="fact"] .card-type-stripe { background: var(--amber); }
  .card[data-type="journal"] .card-type-stripe { background: var(--journal); }

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
  .card[data-type="journal"] .card-type-badge { border-color: var(--journal); color: var(--journal); }

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
    background: var(--surface);
    padding: 0.12rem 0.34rem;
  }
  .quality-chip.conf { border-color: var(--info); color: var(--info); }
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
    background: var(--overlay-bg);
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
    background: #22c55e;
    margin-right: 4px;
    box-shadow: 0 0 0 rgba(34, 197, 94, 0.4);
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
  .connection-chip .chip-relation.supports { border-color: var(--success); color: var(--success); }
  .graph-node circle { stroke-width: 2px; cursor: pointer; transition: r 0.15s, opacity 0.18s, stroke-opacity 0.18s; }
  .graph-node circle:hover { r: 10; }
  .graph-node text { font-family: var(--mono); font-size: 10px; fill: var(--text); pointer-events: none; transition: opacity 0.18s; }
  .graph-link { stroke-width: 1.5px; transition: stroke-opacity 0.18s; }
  .graph-link.explicit { stroke: var(--border-bright); opacity: 0.9; }
  .graph-link.explicit.relation-related { stroke: var(--border-bright); }
  .graph-link.explicit.relation-supports { stroke: var(--success); }
  .graph-link.explicit.relation-contradicts { stroke: var(--red); stroke-dasharray: 6 3; }
  .graph-link.explicit.relation-supersedes { stroke: var(--amber); }
  .graph-link.explicit.relation-causes { stroke: var(--causes); }
  .graph-link.explicit.relation-example-of { stroke: var(--info); }
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
    background: var(--surface-raised);
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
    background: var(--surface-raised);
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
    background: var(--surface-raised);
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
`;
