import { SERVER_VERSION } from '../constants.js';
import { escapeHtml } from '../utils.js';

export const documentOpen = `<!DOCTYPE html>
<html lang="en" data-theme="cyberpunk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MEMORY VAULT</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<style>
`;

export const bodyMarkup = `</style>
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
      <div id="live-indicator" style="font-size:0.6rem;letter-spacing:0.15em;color:var(--text-dim);display:none;align-items:center;margin:0 0.6rem">
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
                <label>Theme Mode</label>
                <div class="setting-help">Auto follows your device's light or dark preference.</div>
                <div class="theme-mode-picker" id="theme-mode-picker">
                  <button type="button" class="theme-mode-btn" data-mode="auto">Auto</button>
                  <button type="button" class="theme-mode-btn" data-mode="light">Light</button>
                  <button type="button" class="theme-mode-btn" data-mode="dark">Dark</button>
                </div>
              </div>
              <div class="setting-row setting-span-2">
                <label>Light Theme</label>
                <div class="setting-help">Color palette used in light mode.</div>
                <div class="theme-picker" id="light-theme-picker">
                  <button type="button" class="theme-swatch" data-theme-value="cyberpunk" title="Cyberpunk"><span style="background:#f5f5f5;border:2px solid #c07800"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="midnight" title="Midnight"><span style="background:#f2f0fa;border:2px solid #6050d0"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="solarized" title="Solarized"><span style="background:#fdf6e3;border:2px solid #b58900"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="ember" title="Ember"><span style="background:#fdf4ee;border:2px solid #d05020"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="arctic" title="Arctic"><span style="background:#f0f7fc;border:2px solid #1898b0"></span></button>
                </div>
              </div>
              <div class="setting-row setting-span-2">
                <label>Dark Theme</label>
                <div class="setting-help">Color palette used in dark mode.</div>
                <div class="theme-picker" id="theme-picker">
                  <button type="button" class="theme-swatch" data-theme-value="cyberpunk" title="Cyberpunk"><span style="background:#080c10;border:2px solid #f0a500"></span></button>
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

        <details class="settings-folder" id="settings-data-management">
          <summary>Data Management</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">

              <div class="setting-row setting-span-2">
                <div class="setting-label" style="font-size:0.75rem;letter-spacing:0.12em;margin-bottom:0.15rem">EXPORT</div>
                <div class="setting-help">Save a backup of all your memories, links, and settings to a <code>.json</code> file. The export does not contain any account identifiers — only your content.</div>
                <div style="margin-top:0.4rem">
                  <button class="refresh-btn utility-btn" id="export-btn" data-action="run-export">EXPORT DATA</button>
                  <div class="semantic-status-line dim" id="export-status-line" style="margin-top:0.3rem"></div>
                </div>
              </div>

              <div class="setting-row setting-span-2" style="border-top:1px solid var(--border);padding-top:0.8rem;margin-top:0.3rem">
                <div class="setting-label" style="font-size:0.75rem;letter-spacing:0.12em;margin-bottom:0.15rem">IMPORT</div>
                <div class="setting-help">Restore data from a previously exported <code>.json</code> backup file.</div>
                <div style="margin-top:0.4rem">
                  <input type="file" accept=".json,application/json" id="import-file-input" style="display:none">
                  <button class="refresh-btn utility-btn" id="import-choose-btn" data-action="choose-import-file">SELECT .JSON FILE</button>
                  <div class="semantic-status-line dim" id="import-file-name" style="margin-top:0.3rem"></div>

                  <div id="import-step-strategy" style="display:none;margin-top:0.6rem;padding-top:0.5rem;border-top:1px solid var(--border)">
                    <div class="setting-help" style="margin-bottom:0.3rem">How should existing data be handled?</div>
                    <select class="setting-input" id="import-strategy">
                      <option value="merge">Merge — add new entries, update existing</option>
                      <option value="skip_existing">Skip existing — only add new entries</option>
                      <option value="overwrite">Overwrite — erase everything, then import (destructive!)</option>
                    </select>
                    <div class="setting-help" id="import-strategy-help" style="margin-top:0.2rem">Safest option. New entries are added, existing ones are updated.</div>
                  </div>

                  <div id="import-step-run" style="display:none;margin-top:0.6rem;padding-top:0.5rem;border-top:1px solid var(--border)">
                    <button class="refresh-btn utility-btn" id="import-btn" data-action="run-import">IMPORT DATA</button>
                  </div>

                  <div class="semantic-status-line dim" id="import-status-line" style="margin-top:0.4rem"></div>
                  <div class="semantic-status-meta" id="import-status-meta"></div>
                </div>
              </div>

              <div class="setting-row setting-span-2" style="border-top:1px solid var(--border);padding-top:0.8rem;margin-top:0.3rem">
                <div class="setting-label" style="font-size:0.75rem;letter-spacing:0.12em;margin-bottom:0.15rem;color:var(--red,#e05050)">DANGER ZONE</div>
                <div class="setting-help">Permanently delete <strong>all</strong> memories, links, changelog, snapshots, and settings from this brain. This cannot be undone. Consider exporting a backup first.</div>
                <div style="margin-top:0.4rem">
                  <button class="refresh-btn utility-btn" id="purge-btn" data-action="run-purge" style="border-color:var(--red,#e05050);color:var(--red,#e05050)">PURGE ALL DATA</button>
                  <div class="semantic-status-line dim" id="purge-status-line" style="margin-top:0.3rem"></div>
                </div>
              </div>

              <div class="setting-row setting-span-2" style="border-top:1px solid var(--border);padding-top:0.6rem;margin-top:0.3rem">
                <div class="setting-help" style="font-size:0.55rem;opacity:0.6;line-height:1.5">
                  Privacy note: Exported files contain the full text of your memories and metadata. Review the file contents before sharing. Do not share exports that contain passwords, API keys, or other sensitive information.
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
