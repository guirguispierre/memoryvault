export const componentStyles = `  .toast {
    border: 1px solid var(--rule);
    border-radius: 9px;
    background: var(--toast-bg);
    color: var(--cream-dim);
    font-size: 12.5px;
    padding: 0.5rem 0.7rem;
    min-width: 190px;
    max-width: min(80vw, 420px);
    line-height: 1.45;
    box-shadow: 0 10px 22px var(--card-glow);
    animation: toastIn 0.2s ease;
  }
  .toast.info { border-color: var(--rule); color: var(--cream-dim); }
  .toast.success { border-color: rgba(157, 179, 154, 0.5); color: var(--sage); }
  .toast.error { border-color: var(--clay); color: var(--clay); }
  .toast.hide { animation: toastOut 0.2s ease forwards; }

  /* ── UPDATE BANNER ── */
  .update-banner {
    display: none;
    align-items: center;
    gap: 0.7rem;
    padding: 0.6rem 0.9rem;
    /* Clear gap below the sticky header; z-index stays under the header (100)
       so the opaque header occludes the banner cleanly as it scrolls away
       rather than colliding with it. The bottom margin keeps the content below
       from crowding the banner. */
    margin: 1rem 40px;
    position: relative;
    z-index: 1;
    border: 1px solid var(--rule);
    border-radius: 10px;
    background: var(--surface-raised);
    font-size: 12.5px;
    color: var(--cream-dim);
    line-height: 1.5;
    animation: bannerSlide 0.3s ease;
  }
  .update-banner.visible { display: flex; }
  .update-banner-icon {
    font-size: 0.85rem;
    flex-shrink: 0;
    color: var(--butter);
  }
  .update-banner-body { flex: 1; min-width: 0; }
  .update-banner-title {
    font-family: var(--disp);
    font-weight: 560;
    font-size: 13.5px;
    color: var(--cream);
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
    gap: 0.3rem;
    padding: 0.12rem 0.4rem;
    border: 1px solid var(--rule);
    border-radius: 5px;
    background: var(--ground-2);
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.04em;
    color: var(--cream-faint);
  }
  .update-banner-item .badge {
    font-family: var(--mono);
    font-size: 9px;
    letter-spacing: 0.05em;
    padding: 0.05rem 0.25rem;
    border-radius: 3px;
    background: var(--butter);
    color: var(--on-butter);
    font-weight: 500;
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
    font-size: 10.5px;
    letter-spacing: 0.06em;
    padding: 0.3rem 0.6rem;
    border: 1px solid var(--rule);
    border-radius: 6px;
    color: var(--cream-dim);
    transition: border-color 0.15s, color 0.15s;
  }
  .update-banner-btn:hover {
    border-color: var(--butter-deep);
    color: var(--butter);
  }
  .update-banner-btn:focus-visible {
    outline: 2px solid var(--butter);
    outline-offset: 2px;
  }
  .update-banner-dismiss {
    all: unset;
    cursor: pointer;
    font-size: 0.95rem;
    color: var(--cream-faint);
    padding: 0.15rem 0.35rem;
    line-height: 1;
    transition: color 0.15s;
  }
  .update-banner-dismiss:hover { color: var(--cream); }
  .update-banner-dismiss:focus-visible {
    outline: 2px solid var(--butter);
    outline-offset: 2px;
  }
  @keyframes bannerSlide {
    from { opacity: 0; transform: translateY(-8px); }
    to   { opacity: 1; transform: translateY(0); }
  }

  /* ── COMMAND PALETTE ── */
  .cmd-overlay {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 300;
    background: var(--overlay-bg);
    padding: 6vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .cmd-overlay.open { display: flex; }
  .cmd-box {
    width: min(700px, 100%);
    border: 1px solid var(--rule);
    border-radius: 12px;
    overflow: hidden;
    background: var(--panel-bg);
    box-shadow: 0 26px 50px var(--panel-shadow);
  }
  .cmd-head {
    padding: 0.8rem;
    border-bottom: 1px solid var(--rule-soft);
    display: flex;
    flex-direction: column;
    gap: 0.45rem;
  }
  .cmd-input {
    width: 100%;
    border: 1px solid var(--rule);
    border-radius: 8px;
    background: var(--ground-3);
    color: var(--cream);
    font-family: var(--body);
    font-size: 14px;
    padding: 0.6rem 0.75rem;
    outline: none;
    transition: border-color 0.16s;
  }
  .cmd-input:focus { border-color: var(--butter-deep); }
  .cmd-input::placeholder { color: var(--cream-faint); }
  .cmd-hint {
    color: var(--cream-faint);
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.06em;
  }
  .cmd-list {
    max-height: min(62vh, 480px);
    overflow-y: auto;
  }
  .cmd-item {
    width: 100%;
    border: none;
    border-bottom: 1px solid var(--rule-soft);
    background: transparent;
    color: var(--cream-dim);
    text-align: left;
    cursor: pointer;
    padding: 0.66rem 0.85rem;
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 0.65rem;
    font-family: var(--body);
  }
  .cmd-item:hover, .cmd-item.active {
    background: var(--butter-glow);
  }
  .cmd-item-label {
    font-size: 13.5px;
    color: var(--cream);
  }
  .cmd-item-detail {
    font-family: var(--mono);
    font-size: 10px;
    color: var(--cream-faint);
    letter-spacing: 0.04em;
    text-align: right;
  }
  .cmd-empty {
    color: var(--cream-faint);
    font-size: 13px;
    padding: 0.85rem;
  }

  /* ── SHORTCUTS ── */
  .shortcuts-overlay {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 290;
    background: var(--overlay-bg);
    padding: 8vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .shortcuts-overlay.open { display: flex; }
  .shortcuts-box {
    width: min(620px, 100%);
    border: 1px solid var(--rule);
    border-radius: 12px;
    background: var(--panel-bg);
    box-shadow: 0 20px 42px var(--panel-shadow);
    padding: 1.1rem;
  }
  .shortcuts-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.55rem;
    margin-bottom: 0.9rem;
  }
  .shortcuts-head h3 {
    font-family: var(--disp);
    font-weight: 560;
    color: var(--cream);
    font-size: 16px;
  }
  .shortcuts-close {
    border: 1px solid var(--rule);
    border-radius: 7px;
    background: transparent;
    color: var(--cream-faint);
    font-family: var(--body);
    font-size: 12px;
    padding: 0.28rem 0.55rem;
    cursor: pointer;
    transition: border-color 0.15s, color 0.15s;
  }
  .shortcuts-close:hover { border-color: var(--butter-deep); color: var(--butter); }
  .shortcuts-grid {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 0.5rem 0.8rem;
    align-items: center;
  }
  .shortcut-key {
    border: 1px solid var(--rule);
    border-radius: 6px;
    background: var(--ground-3);
    color: var(--butter);
    font-family: var(--mono);
    font-size: 10.5px;
    letter-spacing: 0.05em;
    padding: 0.22rem 0.4rem;
    min-width: 88px;
    text-align: center;
  }
  .shortcut-desc {
    color: var(--cream-dim);
    font-size: 13px;
    line-height: 1.45;
  }

  /* ── SETTINGS ── */
  .settings-overlay {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 295;
    background: var(--overlay-bg);
    padding: 8vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .settings-overlay.open { display: flex; }
  .settings-box {
    width: min(760px, 100%);
    max-height: min(84vh, 820px);
    border: 1px solid var(--rule);
    border-radius: 12px;
    background: var(--panel-bg);
    box-shadow: 0 20px 42px var(--panel-shadow);
    padding: 1.1rem;
    display: flex;
    flex-direction: column;
  }
  .settings-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.55rem;
    margin-bottom: 0.9rem;
  }
  .settings-head-main {
    display: flex;
    align-items: baseline;
    gap: 0.55rem;
    flex-wrap: wrap;
  }
  .settings-head h3 {
    font-family: var(--disp);
    font-weight: 560;
    color: var(--cream);
    font-size: 16px;
  }
  .settings-version {
    border: 1px solid var(--rule);
    border-radius: 5px;
    background: var(--ground-3);
    color: var(--cream-faint);
    font-family: var(--mono);
    font-size: 9.5px;
    letter-spacing: 0.08em;
    padding: 0.16rem 0.4rem;
  }
  .settings-close {
    border: 1px solid var(--rule);
    border-radius: 7px;
    background: transparent;
    color: var(--cream-faint);
    font-family: var(--body);
    font-size: 12px;
    padding: 0.28rem 0.55rem;
    cursor: pointer;
    transition: border-color 0.15s, color 0.15s;
  }
  .settings-close:hover { border-color: var(--butter-deep); color: var(--butter); }
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
    border: 1px solid var(--rule-soft);
    border-radius: 10px;
    background: var(--surface);
  }
  .settings-folder[open] {
    border-color: var(--rule);
    background: var(--surface-raised);
  }
  .settings-folder summary {
    list-style: none;
    cursor: pointer;
    padding: 0.55rem 0.7rem;
    color: var(--cream);
    font-family: var(--disp);
    font-weight: 560;
    font-size: 13.5px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.6rem;
  }
  .settings-folder summary::-webkit-details-marker { display: none; }
  .settings-folder summary::after {
    content: '+';
    color: var(--butter);
    font-size: 0.9rem;
    line-height: 1;
  }
  .settings-folder[open] summary::after {
    content: '\\2212';
  }
  .settings-folder-body {
    border-top: 1px solid var(--rule-soft);
    padding: 0.6rem;
  }
  .settings-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.5rem 0.75rem;
  }
  .setting-row {
    border: 1px solid var(--rule-soft);
    border-radius: 8px;
    background: var(--surface);
    padding: 0.55rem 0.65rem;
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
    color: var(--cream-dim);
    font-size: 12.5px;
    font-weight: 500;
    line-height: 1.35;
  }
  .setting-row .setting-help {
    color: var(--cream-faint);
    font-size: 11.5px;
    line-height: 1.4;
  }
  .setting-input {
    border: 1px solid var(--rule);
    border-radius: 7px;
    background: var(--ground-3);
    color: var(--cream);
    font-family: var(--body);
    font-size: 13px;
    outline: none;
    padding: 0.4rem 0.5rem;
    min-height: 30px;
    transition: border-color 0.16s;
  }
  .setting-input:focus { border-color: var(--butter-deep); }
  .setting-check {
    width: 18px;
    height: 18px;
    accent-color: var(--butter);
  }

  /* ── FIRST-RUN ONBOARDING ── */
  .onboarding { margin: 0 0 1.4rem; }
  .ob-card { background: var(--surface-raised); border: 1px solid var(--rule); border-radius: 16px; padding: 1.6rem 1.6rem 1.3rem; }
  .ob-head { margin-bottom: 1.3rem; }
  .ob-eyebrow { font-family: var(--mono); font-size: 0.62rem; letter-spacing: 0.14em; text-transform: uppercase; color: var(--butter); margin-bottom: 0.5rem; }
  .ob-title { font-family: var(--disp); font-weight: 560; font-size: 1.5rem; color: var(--cream); margin: 0 0 0.4rem; }
  .ob-lede { color: var(--cream-dim); font-size: 0.92rem; line-height: 1.55; max-width: 60ch; }
  .ob-steps { list-style: none; margin: 0; padding: 0; display: grid; gap: 1.2rem; }
  .ob-step { display: grid; grid-template-columns: auto 1fr; gap: 0.9rem; align-items: start; }
  .ob-num { width: 26px; height: 26px; border-radius: 50%; background: var(--butter-glow); color: var(--butter); font-family: var(--mono); font-size: 0.8rem; display: flex; align-items: center; justify-content: center; border: 1px solid var(--rule); }
  .ob-bd h3 { font-family: var(--disp); font-weight: 560; font-size: 1.05rem; color: var(--cream); margin: 0 0 0.3rem; }
  .ob-bd p { color: var(--cream-dim); font-size: 0.88rem; line-height: 1.5; margin: 0 0 0.7rem; max-width: 62ch; }
  .ob-row { display: flex; gap: 0.5rem; align-items: stretch; flex-wrap: wrap; }
  .ob-mono { font-family: var(--mono); font-size: 0.78rem; color: var(--cream); background: var(--ground); border: 1px solid var(--rule); border-radius: 8px; padding: 0.6rem 0.7rem; flex: 1; min-width: 220px; overflow-x: auto; white-space: nowrap; }
  .ob-mono.ob-pre { white-space: pre; line-height: 1.7; }
  .ob-copy { font-family: var(--body); font-weight: 600; font-size: 0.74rem; color: var(--on-butter); background: var(--butter); border: 1px solid var(--butter); border-radius: 8px; padding: 0.4rem 0.85rem; cursor: pointer; white-space: nowrap; }
  .ob-copy:hover { filter: brightness(1.05); }
  .ob-tabs { display: flex; gap: 0.4rem; margin-bottom: 0.7rem; }
  .ob-tab { font-family: var(--body); font-size: 0.78rem; color: var(--cream-dim); background: transparent; border: 1px solid var(--rule); border-radius: 7px; padding: 0.35rem 0.7rem; cursor: pointer; }
  .ob-tab.on { color: var(--cream); border-color: var(--butter-deep); background: var(--butter-glow); }
  .ob-note { color: var(--cream-faint); font-size: 0.8rem; margin: 0.6rem 0 0; }
  .ob-kv { display: flex; gap: 0.6rem; align-items: baseline; padding: 0.3rem 0; }
  .ob-kv span { color: var(--cream-faint); min-width: 96px; font-family: var(--mono); font-size: 0.66rem; text-transform: uppercase; letter-spacing: 0.06em; }
  .ob-kv code { font-family: var(--mono); color: var(--cream); font-size: 0.78rem; }
  .ob-primary { font-family: var(--body); font-weight: 600; font-size: 0.85rem; color: var(--on-butter); background: var(--butter); border: 1px solid var(--butter); border-radius: 9px; padding: 0.6rem 1.1rem; cursor: pointer; }
  .ob-primary:hover { filter: brightness(1.05); }
  .ob-primary:disabled { opacity: 0.6; cursor: default; }
  .ob-skip { margin-top: 1.2rem; font-family: var(--body); font-size: 0.78rem; color: var(--cream-faint); background: none; border: none; cursor: pointer; text-decoration: underline; }
  .ob-skip:hover { color: var(--cream-dim); }
  .ob-hint { display: flex; align-items: center; gap: 0.6rem; background: var(--surface-raised); border: 1px solid var(--rule); border-radius: 11px; padding: 0.6rem 0.8rem; font-size: 0.82rem; color: var(--cream-dim); }
  .ob-hint-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--sage); flex-shrink: 0; }
  .ob-hint-text { flex: 1; }
  .ob-hint-link { font-family: var(--body); font-weight: 600; font-size: 0.78rem; color: var(--butter); background: none; border: none; cursor: pointer; }
  .ob-hint-x { font-size: 1.1rem; line-height: 1; color: var(--cream-faint); background: none; border: none; cursor: pointer; padding: 0 0.2rem; }
  .ob-hint-x:hover { color: var(--cream); }
  @media (max-width: 640px) {
    .ob-step { grid-template-columns: 1fr; }
    .ob-num { display: none; }
    .ob-mono { white-space: pre-wrap; word-break: break-all; }
    .ob-mono.ob-pre { white-space: pre-wrap; }
  }
`;
