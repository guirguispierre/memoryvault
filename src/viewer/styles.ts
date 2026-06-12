export const baseStyles = `  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background:
      radial-gradient(1000px 460px at 50% -12%, var(--butter-glow), transparent 62%),
      var(--ground);
    color: var(--cream-dim);
    font-family: var(--body);
    -webkit-font-smoothing: antialiased;
    min-height: 100vh;
    overflow-x: hidden;
    position: relative;
  }

  .stat-pill, .refresh-btn, .logout-btn, .login-btn, .row, .connection-chip, .expand-close {
    touch-action: manipulation;
  }

  /* Faint warm grain, controlled by the "Surface texture" setting */
  body::before {
    content: '';
    position: fixed;
    inset: 0;
    background: repeating-linear-gradient(
      45deg,
      transparent,
      transparent 3px,
      rgba(240, 231, 213, 0.012) 3px,
      rgba(240, 231, 213, 0.012) 4px
    );
    pointer-events: none;
    z-index: 9999;
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

  :focus-visible {
    outline: 2px solid var(--butter);
    outline-offset: 2px;
  }

  /* ── LOGIN ── */
  #login-screen {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    min-height: 100vh;
    padding: 2rem;
    animation: fadeIn 0.5s ease;
  }
  .login-box {
    width: 100%;
    max-width: 392px;
    background: var(--ground-2);
    border: 1px solid var(--rule);
    border-radius: 14px;
    padding: 36px 34px 28px;
    box-shadow: 0 32px 80px rgba(0, 0, 0, 0.5);
    animation: vaultEnter 0.6s cubic-bezier(.18,.79,.26,.99);
  }
  .login-pour {
    display: flex;
    align-items: flex-end;
    gap: 3px;
    height: 20px;
    margin-bottom: 22px;
  }
  .login-pour i {
    width: 4px;
    border-radius: 2px;
    background: var(--cream);
  }
  .vault-logo {
    font-family: var(--disp);
    font-weight: 560;
    font-size: 26px;
    letter-spacing: 0.005em;
    color: var(--cream);
    margin-bottom: 6px;
  }
  .vault-logo em { font-style: italic; color: var(--butter); }
  .vault-sub {
    font-size: 14px;
    line-height: 1.5;
    color: var(--cream-dim);
    margin-bottom: 24px;
  }
  .field-label {
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.09em;
    text-transform: uppercase;
    color: var(--cream-faint);
    margin: 15px 1px 6px;
  }
  .field-label:first-of-type { margin-top: 0; }
  .token-input {
    width: 100%;
    background: var(--ground-3);
    border: 1px solid var(--rule);
    border-radius: 8px;
    color: var(--cream);
    font-family: var(--body);
    font-size: 14px;
    padding: 11px 13px;
    outline: none;
    transition: border-color 0.16s, background 0.16s;
  }
  .token-input::placeholder { color: var(--cream-faint); }
  .token-input:focus { border-color: var(--butter-deep); background: var(--surface-raised); }
  .login-btn-row {
    display: flex;
    gap: 10px;
    margin-top: 22px;
  }
  .login-btn {
    flex: 1;
    width: 100%;
    font-family: var(--body);
    font-weight: 600;
    font-size: 13.5px;
    padding: 12px;
    border-radius: 8px;
    cursor: pointer;
    transition: filter 0.16s, border-color 0.16s, color 0.16s;
    border: 1px solid var(--butter);
    background: var(--butter);
    color: var(--on-butter);
  }
  .login-btn:hover { filter: brightness(1.05); }
  .login-btn.secondary {
    background: transparent;
    border-color: var(--rule);
    color: var(--cream-dim);
  }
  .login-btn.secondary:hover {
    filter: none;
    border-color: var(--butter-deep);
    color: var(--cream);
  }
  .login-agent {
    margin-top: 22px;
    padding-top: 16px;
    border-top: 1px solid var(--rule-soft);
    font-size: 13px;
    color: var(--cream-faint);
    text-align: center;
  }
  .login-agent summary {
    list-style: none;
    cursor: pointer;
  }
  .login-agent summary::-webkit-details-marker { display: none; }
  .login-agent summary em {
    font-family: var(--disp);
    font-style: italic;
    color: var(--cream-dim);
  }
  .login-agent summary:hover em { color: var(--butter); }
  .login-agent .token-form {
    margin-top: 14px;
    text-align: left;
  }
  .token-btn { margin-top: 10px; flex: none; }
  .login-error {
    margin-top: 14px;
    font-size: 13px;
    line-height: 1.5;
    color: var(--clay);
    display: none;
  }

  /* ── MAIN APP ── */
  #app { display: none; flex-direction: column; min-height: 100vh; animation: appEnter 0.4s ease; }

  /* Header */
  .hdr {
    display: flex;
    align-items: center;
    gap: 26px;
    padding: 20px 40px;
    border-bottom: 1px solid var(--rule);
    background: transparent;
    position: sticky;
    top: 0;
    z-index: 100;
    background: var(--ground);
  }
  .hdr-brand {
    font-family: var(--disp);
    font-weight: 560;
    font-size: 21px;
    letter-spacing: 0.005em;
    color: var(--cream);
    white-space: nowrap;
  }
  .hdr-brand em { font-style: italic; color: var(--butter); }
  .hdr-brand .sub {
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.14em;
    color: var(--cream-faint);
    margin-left: 12px;
    text-transform: uppercase;
  }
  .search-wrap {
    flex: 1;
    max-width: 500px;
    min-width: 160px;
    position: relative;
  }
  .search-wrap .mag {
    position: absolute;
    left: 12px;
    top: 50%;
    transform: translateY(-50%);
    color: var(--cream-faint);
    pointer-events: none;
  }
  .search-input {
    width: 100%;
    background: var(--ground-3);
    border: 1px solid var(--rule);
    border-radius: 9px;
    color: var(--cream);
    font-family: var(--body);
    font-size: 13.5px;
    padding: 11px 13px 11px 36px;
    outline: none;
    transition: border-color 0.18s, background 0.18s;
  }
  .search-input::placeholder { color: var(--cream-faint); }
  .search-input:focus { border-color: var(--butter-deep); background: var(--surface-raised); }
  .hdr-right {
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 18px;
  }
  .logout-btn {
    font-family: var(--mono);
    font-size: 11px;
    letter-spacing: 0.06em;
    color: var(--cream-faint);
    background: none;
    border: none;
    cursor: pointer;
    padding: 4px 2px;
    transition: color 0.16s;
  }
  .logout-btn:hover { color: var(--butter); }

  /* Segmented type filter */
  .stats-bar {
    display: flex;
    border: 1px solid var(--rule);
    border-radius: 9px;
    overflow: hidden;
    background: var(--ground-2);
    flex-shrink: 0;
  }
  .stat-pill {
    display: flex;
    align-items: baseline;
    gap: 6px;
    font-family: var(--body);
    font-weight: 500;
    font-size: 12.5px;
    color: var(--cream-dim);
    background: transparent;
    border: none;
    padding: 8px 14px;
    cursor: pointer;
    transition: background 0.14s, color 0.14s;
    white-space: nowrap;
  }
  .stat-pill + .stat-pill { border-left: 1px solid var(--rule-soft); }
  .stat-pill:hover { color: var(--cream); }
  .stat-pill.active {
    background: var(--butter);
    color: var(--on-butter);
    font-weight: 600;
  }
  .stat-label { order: 1; }
  .stat-num {
    order: 2;
    font-family: var(--mono);
    font-size: 10px;
    color: var(--cream-faint);
  }
  .stat-pill.active .stat-num { color: var(--on-butter); opacity: 0.7; }
  .stat-pill.pulse .stat-num { animation: countPulse 0.45s ease; }

  /* The pour — activity seam under the header */
  .pour {
    position: relative;
    padding: 16px 40px 14px;
    border-bottom: 1px solid var(--rule);
    background: linear-gradient(180deg, var(--butter-glow), transparent 80%);
  }
  .pour-cap {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 14px;
    margin-bottom: 10px;
  }
  .pour-label {
    font-family: var(--disp);
    font-style: italic;
    font-weight: 420;
    font-size: 14px;
    color: var(--cream-dim);
    white-space: nowrap;
  }
  .pour-label b { font-style: normal; font-weight: 560; color: var(--cream); }
  .pour-meta {
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.05em;
    color: var(--cream-faint);
    display: flex;
    align-items: baseline;
    gap: 6px;
    white-space: nowrap;
    overflow: hidden;
  }
  .pour-ticks {
    display: flex;
    align-items: flex-end;
    gap: 3px;
    height: 40px;
  }
  .pour-ticks i {
    flex: 1;
    min-width: 2px;
    border-radius: 2px 2px 0 0;
    background: var(--cream);
    animation: tickRise 0.5s ease backwards;
  }
  .live-dot {
    display: inline-block;
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--sage);
    margin-right: 4px;
    animation: livePulse 1.9s infinite;
  }

  /* Utility strip */
  .controls {
    display: flex;
    gap: 18px;
    padding: 8px 40px;
    border-bottom: 1px solid var(--rule-soft);
    justify-content: flex-end;
    flex-wrap: wrap;
    align-items: center;
  }
  .refresh-btn {
    background: none;
    border: none;
    color: var(--cream-faint);
    font-family: var(--mono);
    font-size: 10.5px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 6px 2px;
    cursor: pointer;
    transition: color 0.16s;
  }
  .refresh-btn:hover { color: var(--butter); }
  .refresh-btn.syncing { color: var(--butter); animation: syncFade 0.8s ease-in-out infinite alternate; }
  .utility-btn { color: var(--cream-faint); }
  .utility-btn:hover { color: var(--butter); }

  /* ── MEMORY LIST ── */
  .grid-wrap {
    flex: 1;
    padding: 6px 40px 60px;
    max-width: 980px;
    width: 100%;
  }
  .empty-state {
    padding: 5rem 2rem;
    text-align: center;
    color: var(--cream-dim);
    font-size: 14px;
    line-height: 1.6;
  }
  .empty-state .empty-icon {
    font-family: var(--disp);
    font-style: italic;
    font-size: 18px;
    color: var(--cream-faint);
    margin-bottom: 0.6rem;
  }

  #graph-view {
    opacity: 0;
    transform: translateY(10px);
    transition: opacity 0.25s ease, transform 0.25s ease;
  }
  #graph-view.visible {
    opacity: 1;
    transform: translateY(0);
  }

  /* Tier group header */
  .group {
    display: flex;
    align-items: baseline;
    gap: 14px;
    margin: 26px 2px 4px;
  }
  .group .t {
    font-family: var(--disp);
    font-style: italic;
    font-weight: 420;
    font-size: 16px;
    color: var(--butter);
  }
  .group .ln { flex: 1; height: 1px; background: var(--rule-soft); transform: translateY(-4px); }
  .group .n {
    font-family: var(--mono);
    font-size: 10px;
    color: var(--cream-faint);
  }

  /* Memory row */
  .row {
    display: grid;
    grid-template-columns: 16px 1fr auto;
    gap: 15px;
    align-items: start;
    padding: 16px;
    border: 1px solid transparent;
    border-radius: 11px;
    transition: background 0.15s, border-color 0.15s;
    cursor: pointer;
    animation: slideUp 0.3s ease backwards;
  }
  .row:hover { background: var(--ground-2); border-color: var(--rule); }
  .bead { width: 9px; height: 9px; border-radius: 50%; margin-top: 6px; }
  .bead.full { background: var(--butter); }
  .bead.half { background: var(--butter-deep); }
  .bead.ring { background: transparent; border: 1.5px solid var(--latte); }
  .ttl {
    font-family: var(--disp);
    font-weight: 560;
    font-size: 16.5px;
    letter-spacing: 0.002em;
    margin-bottom: 4px;
    display: flex;
    align-items: baseline;
    gap: 10px;
    flex-wrap: wrap;
    color: var(--cream);
    word-break: break-word;
  }
  .kind {
    font-family: var(--mono);
    font-size: 9.5px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--cream-faint);
  }
  .ver {
    font-family: var(--mono);
    font-size: 9px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--sage);
    border: 1px solid rgba(157, 179, 154, 0.35);
    border-radius: 4px;
    padding: 1px 6px;
    transform: translateY(-1px);
  }
  .txt {
    font-size: 14px;
    line-height: 1.55;
    color: var(--cream-dim);
    max-width: 64ch;
    word-break: break-word;
  }
  .ledger .k { font-family: var(--mono); font-size: 12.5px; color: var(--butter-deep); }
  .ledger .a { color: var(--cream-faint); margin: 0 7px; }
  .ledger .v { font-size: 14.5px; color: var(--cream-dim); }
  .meta {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: 7px;
    white-space: nowrap;
    padding-top: 2px;
  }
  .acc { font-family: var(--mono); font-size: 10.5px; color: var(--cream-faint); }
  .strength { display: flex; align-items: center; gap: 6px; }
  .strength .lab {
    font-family: var(--mono);
    font-size: 9px;
    letter-spacing: 0.07em;
    text-transform: uppercase;
    color: var(--cream-faint);
  }
  .strength .bar {
    width: 54px;
    height: 4px;
    border-radius: 2px;
    background: var(--ground-3);
    border: 1px solid var(--rule);
    overflow: hidden;
  }
  .strength .bar i { display: block; height: 100%; background: var(--butter); }
  .links { font-family: var(--mono); font-size: 10.5px; color: var(--sage); }
  .row.dim .strength .bar i { background: var(--latte); }
  .row.dim .ttl { color: var(--cream-dim); }

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
    background: var(--ground-2);
    border: 1px solid var(--rule);
    border-radius: 14px;
    padding: 2rem;
    position: relative;
    margin-top: 3rem;
    box-shadow: 0 32px 80px rgba(0, 0, 0, 0.45);
    animation: slideUp 0.25s ease;
  }
  .expand-close {
    position: absolute;
    top: 1rem; right: 1rem;
    background: none;
    border: 1px solid var(--rule);
    border-radius: 7px;
    color: var(--cream-faint);
    font-family: var(--mono);
    font-size: 10.5px;
    letter-spacing: 0.06em;
    padding: 5px 10px;
    cursor: pointer;
    transition: border-color 0.15s, color 0.15s;
  }
  .expand-close:hover { border-color: var(--butter-deep); color: var(--butter); }
  .expand-title {
    font-family: var(--disp);
    font-weight: 560;
    font-size: 19px;
    color: var(--cream);
  }
  .expand-key { font-family: var(--mono); font-size: 12px; color: var(--butter-deep); }
  .expand-content {
    font-size: 14.5px;
    color: var(--cream-dim);
    line-height: 1.7;
    white-space: pre-wrap;
    word-break: break-word;
    margin-top: 1rem;
  }

  /* Loading */
  .loading {
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 4rem;
    gap: 0.5rem;
    color: var(--butter);
  }
  .loading-dot {
    width: 4px; height: 4px;
    background: var(--butter);
    border-radius: 50%;
    animation: blink 1s infinite;
  }
  .loading-dot:nth-child(2) { animation-delay: 0.2s; }
  .loading-dot:nth-child(3) { animation-delay: 0.4s; }

  /* Footer */
  .footer {
    padding: 14px 40px;
    border-top: 1px solid var(--rule);
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.5rem;
  }
  .footer-text {
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.06em;
    color: var(--cream-faint);
  }

  .card-links-badge {
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.05em;
    color: var(--sage);
    border: 1px solid rgba(157, 179, 154, 0.35);
    border-radius: 4px;
    padding: 2px 7px;
  }
  .tag {
    font-family: var(--mono);
    font-size: 9.5px;
    letter-spacing: 0.06em;
    color: var(--cream-faint);
    border: 1px solid var(--rule);
    border-radius: 4px;
    background: var(--ground-3);
    padding: 2px 7px;
  }
  .connections-section { margin-top: 1.5rem; padding-top: 1rem; border-top: 1px solid var(--rule); }
  .connections-title {
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.12em;
    color: var(--butter-deep);
    text-transform: uppercase;
    margin-bottom: 0.75rem;
  }
  .connection-chip {
    display: inline-flex; align-items: center; gap: 0.4rem;
    background: var(--ground-3); border: 1px solid var(--rule);
    border-radius: 8px;
    padding: 0.35rem 0.7rem; margin: 0.25rem 0.25rem 0.25rem 0;
    cursor: pointer; transition: border-color 0.15s, color 0.15s;
    font-size: 12.5px; color: var(--cream-dim);
  }
  .connection-chip:hover { border-color: var(--butter-deep); color: var(--cream); }
  .connection-chip .chip-type {
    font-family: var(--mono);
    font-size: 9px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--cream-faint);
  }
  .connection-chip .chip-label { font-size: 11px; color: var(--cream-faint); font-style: italic; }
  .connection-chip .chip-relation {
    font-family: var(--mono);
    font-size: 9px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    border: 1px solid var(--rule-bright);
    border-radius: 4px;
    color: var(--sage);
    padding: 1px 5px;
  }
  .connection-chip .chip-relation.contradicts { border-color: var(--clay); color: var(--clay); }
  .connection-chip .chip-relation.supersedes { border-color: var(--butter-deep); color: var(--butter); }
  .connection-chip .chip-relation.supports { border-color: var(--sage); color: var(--sage); }

  /* ── GRAPH ── */
  .graph-node circle { stroke-width: 2px; cursor: pointer; transition: r 0.15s, opacity 0.18s, stroke-opacity 0.18s; }
  .graph-node circle:hover { r: 10; }
  .graph-node text { font-family: var(--mono); font-size: 10px; fill: var(--cream-dim); pointer-events: none; transition: opacity 0.18s; }
  .graph-link { stroke-width: 1.5px; transition: stroke-opacity 0.18s; }
  .graph-link.explicit { stroke: var(--rule-bright); opacity: 0.9; }
  .graph-link.explicit.relation-related { stroke: var(--rule-bright); }
  .graph-link.explicit.relation-supports { stroke: var(--sage); }
  .graph-link.explicit.relation-contradicts { stroke: var(--clay); stroke-dasharray: 6 3; }
  .graph-link.explicit.relation-supersedes { stroke: var(--butter); }
  .graph-link.explicit.relation-causes { stroke: var(--butter-deep); }
  .graph-link.explicit.relation-example-of { stroke: var(--latte); }
  .graph-link.inferred { stroke: var(--sage); opacity: 0.4; stroke-dasharray: 4 4; }
  .graph-link-label { font-family: var(--mono); font-size: 9px; fill: var(--cream-faint); pointer-events: none; }
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
  .graph-toolbar-row {
    display: flex;
    gap: 0.35rem;
    flex-wrap: wrap;
    justify-content: flex-end;
    width: 100%;
  }
  .graph-search-input {
    min-width: 150px;
    background: var(--ground-2);
    border: 1px solid var(--rule);
    border-radius: 7px;
    color: var(--cream);
    font-family: var(--body);
    font-size: 12px;
    padding: 0.35rem 0.55rem;
    min-height: 30px;
    outline: none;
    transition: border-color 0.16s;
  }
  .graph-search-input:focus { border-color: var(--butter-deep); }
  .graph-search-input::placeholder { color: var(--cream-faint); }
  .graph-btn {
    border: 1px solid var(--rule);
    background: var(--ground-2);
    border-radius: 7px;
    color: var(--cream-faint);
    font-family: var(--mono);
    font-size: 9.5px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 0.35rem 0.5rem;
    cursor: pointer;
    min-height: 30px;
    transition: border-color 0.15s, color 0.15s;
  }
  .graph-btn:hover { border-color: var(--butter-deep); color: var(--butter); }
  .graph-btn.active { color: var(--cream); border-color: var(--rule-bright); background: var(--ground-3); }
  .graph-btn.off { opacity: 0.6; border-color: var(--rule-soft); color: var(--cream-faint); }
  .graph-btn.relation.active { border-color: var(--butter-deep); color: var(--butter); background: var(--ground-2); }
  .graph-btn.relation.off { opacity: 0.55; }
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
    font-family: var(--mono);
    font-size: 9.5px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    border: 1px solid var(--rule);
    border-radius: 6px;
    background: var(--ground-2);
    color: var(--cream-faint);
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
