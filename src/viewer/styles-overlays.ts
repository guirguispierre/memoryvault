export const overlayStyles = `  .semantic-status-box {
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
    background: var(--surface);
    color: var(--teal);
    font-size: 0.54rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.14rem 0.32rem;
  }
  .semantic-status-pill.ready { color: var(--success); border-color: var(--success); }
  .semantic-status-pill.not-ready { color: var(--amber); border-color: var(--amber); }
  .semantic-status-pill.running { color: var(--info); border-color: var(--info); }
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
  .theme-mode-picker {
    display: flex;
    border: 1px solid var(--border);
    border-radius: 6px;
    overflow: hidden;
    width: fit-content;
    margin-top: 0.25rem;
  }
  .theme-mode-btn {
    background: transparent;
    color: var(--text-dim);
    border: none;
    padding: 0.4rem 1rem;
    font-size: 0.62rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    cursor: pointer;
    transition: background 0.15s, color 0.15s;
    font-family: inherit;
  }
  .theme-mode-btn + .theme-mode-btn {
    border-left: 1px solid var(--border);
  }
  .theme-mode-btn:hover {
    background: var(--amber-glow);
    color: var(--text);
  }
  .theme-mode-btn.active {
    background: var(--amber);
    color: var(--bg);
    font-weight: 600;
  }
  
  .changelog-overlay {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 296;
    background: var(--overlay-bg);
    padding: 8vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .changelog-overlay.open { display: flex; }
  .changelog-box {
    width: min(860px, 100%);
    border: 1px solid var(--border-bright);
    background: var(--panel-bg);
    box-shadow: 0 20px 42px var(--panel-shadow);
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
    background: var(--surface);
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
    0% { transform: scale(1); box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.35); opacity: 1; }
    70% { transform: scale(1.15); box-shadow: 0 0 0 8px rgba(34, 197, 94, 0); opacity: 0.9; }
    100% { transform: scale(1); box-shadow: 0 0 0 0 rgba(34, 197, 94, 0); opacity: 1; }
  }
  @keyframes toastIn {
    0% { opacity: 0; transform: translateY(8px); }
    100% { opacity: 1; transform: translateY(0); }
  }
  @keyframes toastOut {
    0% { opacity: 1; transform: translateY(0); }
    100% { opacity: 0; transform: translateY(8px); }
  }
`;
