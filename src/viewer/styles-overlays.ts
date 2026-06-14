export const overlayStyles = `  .semantic-status-box {
    border: 1px solid var(--rule-soft);
    border-radius: 8px;
    background: var(--ground-3);
    padding: 0.6rem;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  .semantic-status-line {
    color: var(--cream-dim);
    font-size: 12px;
    line-height: 1.45;
    word-break: break-word;
  }
  .semantic-status-line.error { color: var(--clay); }
  .semantic-status-line.dim { color: var(--cream-faint); }
  .semantic-status-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 0.35rem;
  }
  .semantic-status-pill {
    border: 1px solid var(--rule);
    border-radius: 5px;
    background: var(--surface);
    color: var(--cream-faint);
    font-family: var(--mono);
    font-size: 9.5px;
    letter-spacing: 0.07em;
    text-transform: uppercase;
    padding: 0.14rem 0.35rem;
  }
  .semantic-status-pill.ready { color: var(--sage); border-color: rgba(157, 179, 154, 0.5); }
  .semantic-status-pill.not-ready { color: var(--butter); border-color: var(--butter-deep); }
  .semantic-status-pill.running { color: var(--cream-dim); border-color: var(--rule-bright); }
  .settings-actions {
    display: flex;
    gap: 0.5rem;
    justify-content: flex-end;
    flex-wrap: wrap;
    margin-top: 0.7rem;
  }
  .settings-actions .refresh-btn,
  .settings-folder .refresh-btn {
    border: 1px solid var(--rule);
    border-radius: 7px;
    padding: 0.45rem 0.8rem;
  }
  .settings-actions .refresh-btn:hover,
  .settings-folder .refresh-btn:hover { border-color: var(--butter-deep); }
  .settings-actions > .refresh-btn:last-child {
    background: var(--butter);
    border-color: var(--butter);
    color: var(--on-butter);
  }
  .settings-actions > .refresh-btn:last-child:hover {
    color: var(--on-butter);
    filter: brightness(1.05);
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
    border: 2px solid var(--rule);
    border-radius: 9px;
    cursor: pointer;
    padding: 3px;
    transition: border-color 0.15s, transform 0.1s;
    position: relative;
  }
  .theme-swatch:hover {
    border-color: var(--butter-deep);
    transform: scale(1.08);
  }
  .theme-swatch.active {
    border-color: var(--butter);
  }
  .theme-swatch.active::after {
    content: '\\2713';
    position: absolute;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    color: white;
    font-size: 0.7rem;
    font-weight: 700;
    text-shadow: 0 1px 3px rgba(0, 0, 0, 0.6);
  }
  .theme-swatch span {
    display: block;
    width: 100%;
    height: 100%;
    border-radius: 5px;
  }
  .theme-mode-picker {
    display: flex;
    border: 1px solid var(--rule);
    border-radius: 8px;
    overflow: hidden;
    width: fit-content;
    margin-top: 0.25rem;
    background: var(--ground-2);
  }
  .theme-mode-btn {
    background: transparent;
    color: var(--cream-dim);
    border: none;
    padding: 0.4rem 1rem;
    font-size: 12px;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s, color 0.15s;
    font-family: var(--body);
  }
  .theme-mode-btn + .theme-mode-btn {
    border-left: 1px solid var(--rule-soft);
  }
  .theme-mode-btn:hover {
    color: var(--cream);
  }
  .theme-mode-btn.active {
    background: var(--butter);
    color: var(--on-butter);
    font-weight: 600;
  }
  /* The custom swatch advertises "your colours" with a small spectrum. */
  .theme-swatch-custom span {
    background: conic-gradient(from 210deg, #e3c98f, #c9826e, #9db39a, #8fc7d8, #a99be8, #e3c98f);
  }

  /* ── CUSTOM THEME BUILDER ── */
  .custom-builder { gap: 0.5rem; }
  .custom-builder-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.4rem 0.7rem;
    margin-top: 0.2rem;
  }
  .custom-color-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.5rem;
  }
  .custom-color-row label {
    color: var(--cream-dim);
    font-size: 12px;
  }
  .custom-color-field {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
  }
  .custom-color-field input[type="color"] {
    width: 30px;
    height: 26px;
    padding: 0;
    border: 1px solid var(--rule);
    border-radius: 6px;
    background: var(--ground-3);
    cursor: pointer;
  }
  .custom-color-field input[type="color"]::-webkit-color-swatch-wrapper { padding: 2px; }
  .custom-color-field input[type="color"]::-webkit-color-swatch { border: none; border-radius: 4px; }
  .custom-hex {
    width: 78px;
    border: 1px solid var(--rule);
    border-radius: 6px;
    background: var(--ground-3);
    color: var(--cream);
    font-family: var(--mono);
    font-size: 11px;
    padding: 0.3rem 0.4rem;
    outline: none;
    transition: border-color 0.16s;
  }
  .custom-hex:focus { border-color: var(--butter-deep); }
  .custom-builder-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.6rem;
    margin-top: 0.2rem;
  }
  .custom-builder-row label { color: var(--cream-dim); font-size: 12px; }
  .custom-builder-row .setting-input { max-width: 230px; }
  .custom-contrast {
    border: 1px solid var(--clay);
    border-radius: 7px;
    background: var(--ground-3);
    color: var(--clay);
    font-size: 11.5px;
    line-height: 1.4;
    padding: 0.4rem 0.5rem;
  }
  .custom-builder-actions {
    display: flex;
    justify-content: flex-end;
    margin-top: 0.1rem;
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
    border: 1px solid var(--rule);
    border-radius: 12px;
    background: var(--panel-bg);
    box-shadow: 0 20px 42px var(--panel-shadow);
    padding: 1.1rem;
  }
  .changelog-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 0.55rem;
    margin-bottom: 0.9rem;
  }
  .changelog-title-group h3 {
    font-family: var(--disp);
    font-weight: 560;
    color: var(--cream);
    font-size: 16px;
    margin-bottom: 0.25rem;
  }
  .changelog-subtitle {
    color: var(--cream-faint);
    font-size: 12px;
    line-height: 1.4;
  }
  .changelog-list {
    border: 1px solid var(--rule-soft);
    border-radius: 9px;
    background: var(--surface);
    padding: 0.7rem;
    max-height: min(62vh, 720px);
    overflow: auto;
    display: flex;
    flex-direction: column;
    gap: 0.65rem;
  }
  .changelog-entry {
    border: 1px solid var(--rule-soft);
    border-radius: 8px;
    background: var(--ground-3);
    padding: 0.65rem;
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
    color: var(--butter);
    font-family: var(--mono);
    font-size: 11px;
    letter-spacing: 0.08em;
  }
  .changelog-entry-date {
    color: var(--cream-faint);
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.06em;
  }
  .changelog-entry-summary {
    color: var(--cream);
    font-size: 13.5px;
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
    border: 1px solid var(--rule);
    border-radius: 4px;
    color: var(--butter-deep);
    font-family: var(--mono);
    font-size: 9px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.1rem 0.3rem;
    white-space: nowrap;
  }
  .changelog-change-text {
    color: var(--cream-dim);
    font-size: 12.5px;
    line-height: 1.45;
  }

  /* Compact density, driven by data-density on the list container. */
  .grid-wrap[data-density="compact"] .row {
    padding: 9px 16px;
    gap: 12px;
  }
  .grid-wrap[data-density="compact"] .ttl { font-size: 15px; margin-bottom: 1px; }
  .grid-wrap[data-density="compact"] .group { margin-top: 16px; }
  /* Clamp the body preview to a single line so rows stay tight. */
  .grid-wrap[data-density="compact"] .txt {
    font-size: 12.5px;
    line-height: 1.4;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  @media (max-width: 900px) {
    .hdr { padding: 14px 16px; gap: 14px; flex-wrap: wrap; }
    .pour { padding: 12px 16px 10px; }
    .controls { padding: 8px 16px; }
    .grid-wrap { padding: 4px 16px 48px; }
    .update-banner { margin: 0.6rem 16px 0; }
    .footer { padding: 12px 16px; flex-wrap: wrap; gap: 0.45rem; }
  }

  @media (max-width: 640px) {
    body::before { display: none; }
    #login-screen { padding: 1rem; }
    .login-box { padding: 26px 22px 22px; }
    .login-btn-row { flex-direction: column; gap: 0.5rem; }
    .token-input, .search-input { font-size: 16px; }

    .hdr {
      position: static;
      gap: 10px 14px;
    }
    .hdr-brand { font-size: 19px; }
    .hdr-brand .sub { display: none; }
    .hdr-right { gap: 12px; }
    .search-wrap {
      order: 4;
      flex-basis: 100%;
      max-width: none;
    }

    .stats-bar {
      order: 5;
      flex-basis: 100%;
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
      scrollbar-width: none;
    }
    .stats-bar::-webkit-scrollbar { display: none; }
    .stat-pill { flex: 1 0 auto; justify-content: center; padding: 8px 12px; }

    .pour-ticks { gap: 2px; }
    .pour-ticks i:nth-child(odd) { display: none; }
    .pour-cap { flex-wrap: wrap; gap: 4px 14px; }

    .controls {
      justify-content: space-between;
      gap: 10px;
    }

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
    .graph-btn { font-size: 9px; letter-spacing: 0.06em; padding: 0.3rem 0.42rem; min-height: 28px; }
    .graph-legend {
      left: 0.45rem;
      right: 0.45rem;
      bottom: 0.45rem;
      max-width: none;
      gap: 0.35rem;
    }
    .graph-legend-item { font-size: 9px; letter-spacing: 0.06em; padding: 0.2rem 0.36rem; }

    /* Rows: meta column stacks under the content */
    .row {
      grid-template-columns: 16px 1fr;
      padding: 14px 10px;
    }
    .row .meta {
      grid-column: 2;
      flex-direction: row;
      flex-wrap: wrap;
      align-items: center;
      justify-content: flex-start;
      gap: 12px;
      padding-top: 8px;
    }
    .txt { max-width: none; }

    .expand-overlay {
      padding: 0;
      align-items: stretch;
    }
    .expand-box {
      margin-top: 0;
      max-width: none;
      min-height: 100vh;
      border: none;
      border-radius: 0;
      border-top: 1px solid var(--rule);
      padding: 3.25rem 1rem 1.25rem;
    }
    .expand-close {
      top: 0.65rem;
      right: 0.65rem;
      padding: 0.45rem 0.7rem;
    }
    .expand-content { font-size: 14px; line-height: 1.65; }
    .connection-chip {
      display: flex;
      width: 100%;
      margin-right: 0;
    }

    .footer .footer-text:last-child { display: none; }
    .toast-wrap { left: 0.65rem; right: 0.65rem; bottom: 0.65rem; }
    .toast { max-width: none; }
    .cmd-overlay { padding-top: 3vh; }
    .cmd-head { padding: 0.62rem; }
    .cmd-item { padding: 0.54rem 0.62rem; }
    .cmd-item-label { font-size: 13px; }
    .shortcuts-overlay { padding-top: 5vh; }
    .shortcuts-box { padding: 0.8rem; }
    .shortcut-key { min-width: 74px; }
    .shortcut-desc { font-size: 12.5px; }
    .settings-overlay { padding-top: 5vh; }
    .settings-box { padding: 0.8rem; max-height: 90vh; }
    .settings-scroll { padding-right: 0; margin-right: 0; }
    .settings-grid { grid-template-columns: 1fr; }
    .settings-actions { justify-content: stretch; }
    .settings-actions .refresh-btn { width: 100%; }
    .changelog-overlay { padding-top: 5vh; }
    .changelog-box { padding: 0.8rem; }
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
  @keyframes vaultEnter {
    0% { opacity: 0; transform: translateY(18px) scale(0.98); }
    100% { opacity: 1; transform: translateY(0) scale(1); }
  }
  @keyframes countPulse {
    0% { transform: scale(1); }
    40% { transform: scale(1.12); }
    100% { transform: scale(1); }
  }
  @keyframes syncFade {
    0% { opacity: 0.55; }
    100% { opacity: 1; }
  }
  @keyframes livePulse {
    0% { transform: scale(1); opacity: 1; }
    70% { transform: scale(1.15); opacity: 0.75; }
    100% { transform: scale(1); opacity: 1; }
  }
  @keyframes tickRise {
    0% { transform: scaleY(0.4); transform-origin: bottom; }
    100% { transform: scaleY(1); transform-origin: bottom; }
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
