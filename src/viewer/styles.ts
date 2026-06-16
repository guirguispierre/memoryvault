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

  /* ── TOP BAR (control center) ── */
  .topbar {
    display: flex;
    align-items: center;
    gap: 16px;
    padding: 13px 26px;
    border-bottom: 1px solid var(--rule);
    position: sticky;
    top: 0;
    z-index: 100;
    background: var(--ground);
  }
  .tb-brand {
    font-family: var(--disp);
    font-weight: 600;
    font-size: 18px;
    letter-spacing: 0.005em;
    color: var(--cream);
    white-space: nowrap;
  }
  .tb-brand em { font-style: italic; color: var(--butter); }

  /* Segmented mode switch: Memories / Graph */
  .modeswitch {
    display: flex;
    gap: 2px;
    background: var(--surface-raised);
    border: 1px solid var(--rule);
    border-radius: 10px;
    padding: 3px;
    flex-shrink: 0;
  }
  .modeswitch .mode {
    background: none;
    border: none;
    color: var(--cream-dim);
    font-family: var(--body);
    font-size: 13px;
    font-weight: 500;
    padding: 7px 15px;
    border-radius: 7px;
    cursor: pointer;
    transition: background 0.14s, color 0.14s;
    white-space: nowrap;
  }
  .modeswitch .mode:hover { color: var(--cream); }
  .modeswitch .mode.active { background: var(--ground-3); color: var(--cream); }

  /* Centered command search */
  .cmdsearch {
    flex: 1;
    max-width: 560px;
    margin: 0 auto;
    position: relative;
    display: flex;
    align-items: center;
    min-width: 140px;
  }
  .cmdsearch .cs-mag {
    position: absolute;
    left: 13px;
    top: 50%;
    transform: translateY(-50%);
    color: var(--cream-faint);
    pointer-events: none;
  }
  .search-input {
    width: 100%;
    background: var(--surface-raised);
    border: 1px solid var(--rule);
    border-radius: 10px;
    color: var(--cream);
    font-family: var(--body);
    font-size: 13.5px;
    padding: 11px 70px 11px 38px;
    outline: none;
    transition: border-color 0.18s, background 0.18s;
  }
  .search-input::placeholder { color: var(--cream-faint); }
  .search-input:focus { border-color: var(--butter-deep); }
  .cmdsearch .cs-kbd {
    position: absolute;
    right: 9px;
    top: 50%;
    transform: translateY(-50%);
    font-family: var(--mono);
    font-size: 10px;
    color: var(--cream-faint);
    background: var(--ground-2);
    border: 1px solid var(--rule);
    border-radius: 5px;
    padding: 3px 7px;
    cursor: pointer;
    transition: border-color 0.15s, color 0.15s;
  }
  .cmdsearch .cs-kbd:hover { border-color: var(--butter-deep); color: var(--butter); }

  /* Right-side action icons */
  .topacts {
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 7px;
  }
  .ico {
    width: 34px;
    height: 34px;
    border-radius: 8px;
    background: var(--surface-raised);
    border: 1px solid var(--rule);
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    color: var(--cream-dim);
    position: relative;
    transition: color 0.15s, border-color 0.15s;
  }
  .ico:hover { color: var(--cream); border-color: var(--butter-deep); }
  .ico svg { width: 15px; height: 15px; stroke: currentColor; fill: none; stroke-width: 1.7; stroke-linecap: round; stroke-linejoin: round; }
  .ico .tip {
    position: absolute;
    top: 40px;
    right: 0;
    background: var(--surface-raised);
    border: 1px solid var(--rule);
    color: var(--cream-dim);
    font-family: var(--mono);
    font-size: 10px;
    padding: 3px 7px;
    border-radius: 5px;
    white-space: nowrap;
    opacity: 0;
    pointer-events: none;
    transition: opacity 0.12s;
    z-index: 20;
  }
  .ico:hover .tip { opacity: 1; }
  .nbtn {
    background: var(--butter);
    color: var(--on-butter);
    border: none;
    border-radius: 9px;
    padding: 9px 15px;
    font-family: var(--body);
    font-weight: 600;
    font-size: 13px;
    cursor: pointer;
    white-space: nowrap;
    transition: filter 0.15s;
  }
  .nbtn:hover { filter: brightness(1.06); }

  /* ── FILTER BAR ── */
  .feed { min-width: 0; }
  .filterbar {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 12px;
    flex-wrap: wrap;
  }
  .chips { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
  .filter-sp { flex: 1; }
  .chip {
    background: var(--surface-raised);
    border: 1px solid var(--rule);
    color: var(--cream-dim);
    font-family: var(--body);
    font-size: 12.5px;
    border-radius: 20px;
    padding: 6px 13px;
    cursor: pointer;
    transition: background 0.14s, border-color 0.14s, color 0.14s;
    white-space: nowrap;
  }
  .chip:hover { color: var(--cream); }
  .chip.active {
    background: var(--butter-glow);
    border-color: var(--butter-deep);
    color: var(--cream);
  }
  .chip .chip-n {
    font-family: var(--mono);
    font-size: 10px;
    color: var(--cream-faint);
    margin-left: 3px;
  }
  .chip.active .chip-n { color: var(--butter); }
  .chip .c {
    display: inline-block;
    width: 6px;
    height: 6px;
    border-radius: 50%;
    margin-right: 6px;
    vertical-align: middle;
  }
  /* A state chip that is toggled off reads as muted, not selected. */
  .chips-state .chip:not(.active) { opacity: 0.5; }
  .dens {
    font-family: var(--mono);
    font-size: 11px;
    color: var(--cream-faint);
    cursor: pointer;
    background: var(--surface-raised);
    border: 1px solid var(--rule);
    border-radius: 7px;
    padding: 5px 9px;
    white-space: nowrap;
    transition: border-color 0.14s, color 0.14s;
  }
  .dens:hover { border-color: var(--butter-deep); color: var(--cream); }
  .dens.active { border-color: var(--butter-deep); color: var(--butter); }

  /* ── STAT STRIP ── */
  .statstrip {
    display: flex;
    align-items: center;
    gap: 0;
    padding: 10px 26px;
    border-bottom: 1px solid var(--rule);
    font-size: 12.5px;
    color: var(--cream-dim);
  }
  .stat-s {
    display: flex;
    align-items: center;
    gap: 7px;
    padding-right: 18px;
    margin-right: 18px;
    border-right: 1px solid var(--rule);
    white-space: nowrap;
  }
  .stat-s.stat-s-end { border-right: none; }
  .stat-s b { color: var(--cream); font-weight: 600; font-family: var(--mono); }
  .stat-dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
  .stat-sp { flex: 1; }
  .stat-recall {
    display: flex;
    align-items: center;
    gap: 9px;
    min-width: 0;
  }
  .stat-recall-label, .stat-recall-time {
    font-family: var(--mono);
    font-size: 9.5px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--cream-faint);
    white-space: nowrap;
  }
  .stat-recall .pour-ticks {
    width: 120px;
    height: 22px;
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

  /* Utility buttons (reused by settings / overlays) */
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

  /* ── HOME: list feed + graph rail ── */
  .grid-wrap {
    flex: 1;
    display: grid;
    grid-template-columns: minmax(0, 1fr) 340px;
    gap: 0;
    align-items: start;
    padding: 16px 26px 60px;
    width: 100%;
    max-width: none;
    margin: 0 auto;
  }
  .feed { padding-right: 22px; }

  /* Graph rail */
  .rail {
    border-left: 1px solid var(--rule);
    padding: 4px 0 0 18px;
    position: sticky;
    top: 78px;
  }
  .rail h3 {
    font-family: var(--mono);
    font-size: 11px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--cream-faint);
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .rail h3 .exp {
    color: var(--butter);
    cursor: pointer;
    text-transform: none;
    letter-spacing: 0;
    font-family: var(--body);
    font-size: 12px;
    background: none;
    border: none;
    padding: 0;
  }
  .rail h3 .exp:hover { filter: brightness(1.1); text-decoration: underline; }
  .mini {
    height: 280px;
    border-radius: 14px;
    background:
      radial-gradient(circle at 50% 45%, var(--butter-glow), transparent 70%),
      var(--surface-raised);
    border: 1px solid var(--rule);
    position: relative;
    overflow: hidden;
  }
  .mini canvas { position: absolute; inset: 0; width: 100%; height: 100%; display: block; }
  .railcard {
    margin-top: 14px;
    background: var(--surface-raised);
    border: 1px solid var(--rule);
    border-radius: 12px;
    padding: 14px;
  }
  .railcard .k2 { font-family: var(--mono); font-size: 11px; color: var(--butter); margin-bottom: 4px; word-break: break-word; }
  .railcard .t2 { font-family: var(--disp); font-size: 15px; color: var(--cream); margin-bottom: 8px; line-height: 1.3; }
  .railcard .links { font-size: 12px; color: var(--cream-dim); line-height: 1.7; word-break: break-word; }
  .railcard .links a { color: var(--mem-link); text-decoration: none; cursor: pointer; }
  .railcard .links a:hover { text-decoration: underline; }
  .railcard .rail-dim { color: var(--cream-faint); }
  .railcard .rail-empty { color: var(--cream-faint); font-size: 12.5px; line-height: 1.55; }
  .rail-detail {
    margin-top: 12px;
    background: none;
    border: 1px solid var(--rule);
    border-radius: 8px;
    color: var(--cream-dim);
    font-family: var(--body);
    font-size: 12px;
    padding: 6px 11px;
    cursor: pointer;
    transition: border-color 0.15s, color 0.15s;
  }
  .rail-detail:hover { border-color: var(--butter-deep); color: var(--butter); }
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

  /* ── MEMORY TABLE ── */
  /* Column header + rows share one grid so the columns line up exactly:
     state | key | memory | type | links | seen */
  .lh, .r {
    display: grid;
    grid-template-columns: 16px 158px minmax(0, 1fr) 70px 58px 46px;
    gap: 14px;
    align-items: center;
  }
  .lh {
    padding: 0 12px 8px;
    font-family: var(--mono);
    font-size: 9.5px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--cream-faint);
    border-bottom: 1px solid var(--rule);
  }
  .lh span:nth-child(4), .lh span:nth-child(5), .lh span:nth-child(6) { text-align: right; }
  .r {
    padding: 10px 12px;
    border-bottom: 1px solid var(--rule-soft);
    cursor: pointer;
    position: relative;
    transition: background 0.1s;
    animation: slideUp 0.3s ease backwards;
  }
  .r:hover { background: var(--surface-raised); }
  .r.sel { background: var(--surface-raised); }
  .r.sel::before {
    content: '';
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 2px;
    background: var(--butter);
  }
  /* State cell: glowing dot (memory state) over a vertical strength meter. */
  .stcell { display: flex; flex-direction: column; align-items: center; gap: 3px; }
  .stcell .dot { width: 8px; height: 8px; border-radius: 50%; box-shadow: 0 0 7px currentColor; }
  .stcell .mtr {
    width: 8px;
    height: 22px;
    border-radius: 2px;
    background: var(--ground-3);
    overflow: hidden;
    display: flex;
    flex-direction: column-reverse;
  }
  .stcell .mtr i { display: block; width: 100%; }
  .r .k {
    font-family: var(--mono);
    font-size: 11px;
    color: var(--butter);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .r .ti { min-width: 0; }
  .r .ti .t {
    font-family: var(--disp);
    font-size: 14.5px;
    color: var(--cream);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .r .ti .x {
    font-size: 11.5px;
    color: var(--cream-faint);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    margin-top: 1px;
  }
  .r .type {
    font-family: var(--mono);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--cream-dim);
    text-align: right;
  }
  .r .lk { font-family: var(--mono); font-size: 11px; color: var(--mem-link); text-align: right; white-space: nowrap; }
  .r .when { font-family: var(--mono); font-size: 10.5px; color: var(--cream-faint); text-align: right; }
  /* .kind retained: used by the expand/detail overlay header. */
  .kind {
    font-family: var(--mono);
    font-size: 9.5px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--cream-faint);
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

  /* Opt-in 3D graph layer: fills the view, sits under the toolbar/legend. */
  #graph-3d { position: absolute; inset: 0; display: none; z-index: 5; }
  #graph-3d canvas { display: block; }

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
