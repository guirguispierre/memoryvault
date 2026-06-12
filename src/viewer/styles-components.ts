export const componentStyles = `  .toast {
    border: 1px solid var(--border-bright);
    background: var(--toast-bg);
    color: var(--text);
    font-size: 0.68rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 0.45rem 0.6rem;
    min-width: 190px;
    max-width: min(80vw, 420px);
    line-height: 1.45;
    box-shadow: 0 10px 22px var(--card-glow);
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
    background: var(--overlay-bg);
    padding: 6vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .cmd-overlay.open { display: flex; }
  .cmd-box {
    width: min(700px, 100%);
    border: 1px solid var(--border-bright);
    background: var(--panel-bg);
    box-shadow: 0 26px 50px var(--panel-shadow);
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
    background: var(--amber-glow);
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
    background: var(--overlay-bg);
    padding: 8vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .shortcuts-overlay.open { display: flex; }
  .shortcuts-box {
    width: min(620px, 100%);
    border: 1px solid var(--border-bright);
    background: var(--panel-bg);
    box-shadow: 0 20px 42px var(--panel-shadow);
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
    background: var(--overlay-bg);
    padding: 8vh 1rem 1rem;
    align-items: flex-start;
    justify-content: center;
  }
  .settings-overlay.open { display: flex; }
  .settings-box {
    width: min(760px, 100%);
    max-height: min(84vh, 820px);
    border: 1px solid var(--border-bright);
    background: var(--panel-bg);
    box-shadow: 0 20px 42px var(--panel-shadow);
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
    background: var(--surface);
  }
  .settings-folder[open] {
    border-color: var(--border-bright);
    background: var(--surface-raised);
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
    background: var(--surface);
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
`;
