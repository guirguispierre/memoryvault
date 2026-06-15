import { SERVER_VERSION } from '../constants.js';
import { escapeHtml } from '../utils.js';
import { FONT_LINK_TAGS } from './tokens.js';

export const documentOpen = `<!DOCTYPE html>
<html lang="en" data-theme="slate">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MemoryVault — your index</title>
${FONT_LINK_TAGS}
<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<style>
`;

export const bodyMarkup = `</style>
</head>
<body>

<!-- LOGIN -->
<div id="login-screen">
  <div class="login-box">
    <div class="login-pour" aria-hidden="true"><i style="height:4px;opacity:.14"></i><i style="height:6px;opacity:.18"></i><i style="height:5px;opacity:.22"></i><i style="height:9px;opacity:.3"></i><i style="height:7px;opacity:.38"></i><i style="height:12px;opacity:.5"></i><i style="height:10px;opacity:.6"></i><i style="height:15px;opacity:.74"></i><i style="height:18px;opacity:.88"></i><i style="height:20px;opacity:1"></i></div>
    <div class="vault-logo">Memory<em>Vault</em></div>
    <div class="vault-sub">Open your index — a second brain you host yourself.</div>

    <div class="field-label">Email</div>
    <input type="email" class="token-input" id="email-input" placeholder="you@example.com" autocomplete="username" autocapitalize="off" autocorrect="off" spellcheck="false">
    <div class="field-label">Password</div>
    <input type="password" class="token-input" id="password-input" placeholder="••••••••••••" autocomplete="current-password">
    <div class="field-label">Brain name — for a new account</div>
    <input type="text" class="token-input" id="brain-name-input" placeholder="e.g. Pierre&rsquo;s index" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false">

    <div class="login-btn-row">
      <button class="login-btn" data-action="login">Open index</button>
      <button class="login-btn secondary" data-action="signup">Create account</button>
    </div>
    <div class="login-error" id="login-error"></div>

    <details class="login-agent">
      <summary>Connecting an agent? <em>Use a bearer token instead.</em></summary>
      <div class="token-form">
        <div class="field-label">Bearer token</div>
        <input type="password" class="token-input" id="token-input" placeholder="Paste an access token" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false">
        <button class="login-btn secondary token-btn" data-action="token-login">Use token</button>
      </div>
    </details>
  </div>
</div>

<!-- APP -->
<div id="app">
  <header class="hdr">
    <div class="hdr-brand">Memory<em>Vault</em><span class="sub">your index</span></div>
    <div class="search-wrap">
      <svg class="mag" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><circle cx="11" cy="11" r="7"/><line x1="21" y1="21" x2="16.5" y2="16.5"/></svg>
      <input type="text" class="search-input" id="search-input" placeholder="recall a memory — text, key, or meaning…" inputmode="search">
    </div>
    <div class="stats-bar">
      <button class="stat-pill active" id="stat-all" data-action="set-filter" data-filter="">
        <span class="stat-label">All</span><span class="stat-num" id="count-all">0</span>
      </button>
      <button class="stat-pill" id="stat-note" data-action="set-filter" data-filter="note">
        <span class="stat-label">Notes</span><span class="stat-num" id="count-note">0</span>
      </button>
      <button class="stat-pill" id="stat-fact" data-action="set-filter" data-filter="fact">
        <span class="stat-label">Facts</span><span class="stat-num" id="count-fact">0</span>
      </button>
      <button class="stat-pill" id="stat-journal" data-action="set-filter" data-filter="journal">
        <span class="stat-label">Journal</span><span class="stat-num" id="count-journal">0</span>
      </button>
      <button class="stat-pill" id="stat-graph" data-action="show-graph">
        <span class="stat-label">Graph</span>
      </button>
    </div>
    <div class="hdr-right">
      <div id="live-indicator" style="display:none;align-items:center;font-family:var(--mono);font-size:10px;letter-spacing:0.08em;color:var(--cream-faint)">
        <span class="live-dot"></span>live
      </div>
      <button class="logout-btn" data-action="logout">lock</button>
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

  <div class="pour">
    <div class="pour-cap">
      <span class="pour-label">Recall, <b>last 24 hours</b></span>
      <span class="pour-meta"><span id="hdr-count">— entries</span><span>&middot;</span><span>synced <span id="hdr-time"></span></span></span>
    </div>
    <div class="pour-ticks" id="pour-ticks" aria-hidden="true"></div>
  </div>

  <div class="controls">
    <button class="refresh-btn" data-action="refresh-memories">Refresh</button>
    <button class="refresh-btn utility-btn" data-action="open-command-palette">Command</button>
    <button class="refresh-btn utility-btn" data-action="toggle-shortcuts-overlay">Shortcuts</button>
    <button class="refresh-btn utility-btn" data-action="open-settings-overlay">Settings</button>
  </div>

  <div id="graph-view" style="display:none;flex:1;position:relative;background:var(--ground);min-height:600px">
    <div class="graph-toolbar">
      <div class="graph-toolbar-row">
        <input type="text" class="graph-search-input" id="graph-search-input" placeholder="Search graph…" inputmode="search">
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
    <div id="graph-empty" style="display:none;position:absolute;inset:0;align-items:center;justify-content:center;text-align:center;color:var(--cream-dim);font-size:14px;padding:1rem">Nothing here yet — save your first memory.</div>
  </div>
  <div class="grid-wrap" id="grid">
    <div class="loading"><div class="loading-dot"></div><div class="loading-dot"></div><div class="loading-dot"></div></div>
  </div>

  <footer class="footer">
    <div class="footer-text">MemoryVault &middot; self-hosted on Cloudflare</div>
    <div class="footer-text">Private session</div>
  </footer>
</div>

<!-- EXPAND OVERLAY -->
<div class="expand-overlay" id="expand-overlay" data-action="close-expand-overlay">
  <div class="expand-box">
    <button class="expand-close" data-action="close-expand">Close</button>
    <div id="expand-header"></div>
    <div class="expand-content" id="expand-content"></div>
    <div style="margin-top:1.5rem;padding-top:1rem;border-top:1px solid var(--rule);font-family:var(--mono);font-size:10.5px;color:var(--cream-faint);letter-spacing:0.04em" id="expand-meta"></div>
    <div id="expand-connections"></div>
  </div>
</div>

<div class="cmd-overlay" id="cmd-overlay" data-action="close-command-palette-overlay">
  <div class="cmd-box">
    <div class="cmd-head">
      <input type="text" class="cmd-input" id="cmd-input" placeholder="Run an action…" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false">
      <div class="cmd-hint">enter to run &middot; esc to close &middot; arrows to move</div>
    </div>
    <div class="cmd-list" id="cmd-list"></div>
  </div>
</div>

<div class="shortcuts-overlay" id="shortcuts-overlay" data-action="close-shortcuts-overlay">
  <div class="shortcuts-box">
    <div class="shortcuts-head">
      <h3>Keyboard shortcuts</h3>
      <button class="shortcuts-close" data-action="close-shortcuts">Close</button>
    </div>
    <div class="shortcuts-grid">
      <span class="shortcut-key">Ctrl/Cmd+K</span><span class="shortcut-desc">Open the command palette</span>
      <span class="shortcut-key">?</span><span class="shortcut-desc">Open this shortcuts panel</span>
      <span class="shortcut-key">S</span><span class="shortcut-desc">Open settings</span>
      <span class="shortcut-key">/</span><span class="shortcut-desc">Focus search</span>
      <span class="shortcut-key">G</span><span class="shortcut-desc">Open the graph view</span>
      <span class="shortcut-key">R</span><span class="shortcut-desc">Refresh memories</span>
      <span class="shortcut-key">Esc</span><span class="shortcut-desc">Close overlays and cards</span>
      <span class="shortcut-key">Enter</span><span class="shortcut-desc">Run the selected command</span>
    </div>
  </div>
</div>

<div class="settings-overlay" id="settings-overlay" data-action="close-settings-overlay">
  <div class="settings-box">
    <div class="settings-head">
      <div class="settings-head-main">
        <h3>Viewer settings</h3>
        <span class="settings-version">v${escapeHtml(SERVER_VERSION)}</span>
      </div>
      <button class="settings-close" data-action="close-settings">Close</button>
    </div>
    <div class="settings-scroll">
      <div class="settings-sections">
        <details class="settings-folder" open>
          <summary>General &amp; search</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Live polling</div>
                  <div class="setting-help">Refresh memory stats quietly in the background.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-live-poll-enabled">
              </div>
              <div class="setting-row">
                <label for="settings-live-poll-interval">Polling interval (sec)</label>
                <input type="number" min="5" max="120" step="1" class="setting-input" id="settings-live-poll-interval">
                <div class="setting-help">Lower means faster updates; higher is a lighter load.</div>
              </div>
              <div class="setting-row">
                <label for="settings-time-mode">Time display</label>
                <select class="setting-input" id="settings-time-mode">
                  <option value="utc">UTC</option>
                  <option value="local">Local</option>
                </select>
                <div class="setting-help">Format for the synced-at clock.</div>
              </div>
              <div class="setting-row">
                <label for="settings-default-filter">Default startup filter</label>
                <select class="setting-input" id="settings-default-filter">
                  <option value="">All</option>
                  <option value="note">Notes</option>
                  <option value="fact">Facts</option>
                  <option value="journal">Journal</option>
                </select>
                <div class="setting-help">Initial list filter after you sign in.</div>
              </div>
              <div class="setting-row">
                <label for="settings-search-debounce">Search debounce (ms)</label>
                <input type="number" min="120" max="1500" step="10" class="setting-input" id="settings-search-debounce">
                <div class="setting-help">Delay before the list search runs.</div>
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Compact rows</div>
                  <div class="setting-help">Fit more memories on screen.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-compact-cards">
              </div>
            </div>
          </div>
        </details>

        <details class="settings-folder">
          <summary>Graph defaults</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Inferred edges</div>
                  <div class="setting-help">Show inferred connections by default.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-graph-inferred">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Graph labels</div>
                  <div class="setting-help">Show node labels by default.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-graph-labels">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Graph physics</div>
                  <div class="setting-help">Start the simulation enabled.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-graph-physics">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Open graph on sign-in</div>
                  <div class="setting-help">Skip the list and jump straight to the graph.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-auto-open-graph">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Hover focus</div>
                  <div class="setting-help">Highlight a node's neighborhood on hover.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-graph-focus">
              </div>
            </div>
          </div>
        </details>

        <details class="settings-folder">
          <summary>Appearance &amp; session</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-span-2">
                <label>Theme mode</label>
                <div class="setting-help">Auto follows your device's light or dark preference.</div>
                <div class="theme-mode-picker" id="theme-mode-picker">
                  <button type="button" class="theme-mode-btn" data-mode="auto">Auto</button>
                  <button type="button" class="theme-mode-btn" data-mode="light">Light</button>
                  <button type="button" class="theme-mode-btn" data-mode="dark">Dark</button>
                </div>
              </div>
              <div class="setting-row setting-span-2">
                <label>Light theme</label>
                <div class="setting-help">Palette used in light mode.</div>
                <div class="theme-picker" id="light-theme-picker">
                  <button type="button" class="theme-swatch" data-theme-value="paper" title="Paper"><span style="background:#FBFAF7;border:2px solid #3B5BDB"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="slate" title="Slate"><span style="background:#F4F5F6;border:2px solid #3E6E99"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="vanilla" title="Vanilla"><span style="background:#F4EDDE;border:2px solid #8F713B"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="midnight" title="Midnight"><span style="background:#F0EEF8;border:2px solid #7C6AD0"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="solarized" title="Solarized"><span style="background:#FDF6E3;border:2px solid #A37B00"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="ember" title="Ember"><span style="background:#FAF0E8;border:2px solid #C0622E"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="arctic" title="Arctic"><span style="background:#EEF5FA;border:2px solid #1F7E9A"></span></button>
                  <button type="button" class="theme-swatch theme-swatch-custom" data-theme-value="custom" title="Custom"><span></span></button>
                </div>
              </div>
              <div class="setting-row setting-span-2">
                <label>Dark theme</label>
                <div class="setting-help">Palette used in dark mode.</div>
                <div class="theme-picker" id="theme-picker">
                  <button type="button" class="theme-swatch" data-theme-value="slate" title="Slate"><span style="background:#0F1011;border:2px solid #7FA6C9"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="paper" title="Paper"><span style="background:#16161A;border:2px solid #8AA0F0"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="vanilla" title="Vanilla"><span style="background:#181511;border:2px solid #E3C98F"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="midnight" title="Midnight"><span style="background:#14141F;border:2px solid #A99BE8"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="solarized" title="Solarized"><span style="background:#002B36;border:2px solid #B58900"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="ember" title="Ember"><span style="background:#1A0E0A;border:2px solid #E8956A"></span></button>
                  <button type="button" class="theme-swatch" data-theme-value="arctic" title="Arctic"><span style="background:#0F1B22;border:2px solid #8FC7D8"></span></button>
                  <button type="button" class="theme-swatch theme-swatch-custom" data-theme-value="custom" title="Custom"><span></span></button>
                </div>
              </div>
              <div class="setting-row setting-span-2 custom-builder" id="custom-theme-builder" style="display:none">
                <div class="setting-label">Custom palette</div>
                <div class="setting-help">Six colours theme the whole interface; the rest are derived. The shared palette is used wherever Custom is selected, light or dark.</div>
                <div class="custom-builder-grid">
                  <div class="custom-color-row">
                    <label for="custom-ground">Background</label>
                    <span class="custom-color-field">
                      <input type="color" id="custom-ground" data-custom-token="ground" data-custom-kind="color">
                      <input type="text" class="custom-hex" data-custom-token="ground" data-custom-kind="hex" maxlength="7" spellcheck="false" autocapitalize="off" autocorrect="off">
                    </span>
                  </div>
                  <div class="custom-color-row">
                    <label for="custom-ground2">Raised surface</label>
                    <span class="custom-color-field">
                      <input type="color" id="custom-ground2" data-custom-token="ground_2" data-custom-kind="color">
                      <input type="text" class="custom-hex" data-custom-token="ground_2" data-custom-kind="hex" maxlength="7" spellcheck="false" autocapitalize="off" autocorrect="off">
                    </span>
                  </div>
                  <div class="custom-color-row">
                    <label for="custom-cream">Primary text</label>
                    <span class="custom-color-field">
                      <input type="color" id="custom-cream" data-custom-token="cream" data-custom-kind="color">
                      <input type="text" class="custom-hex" data-custom-token="cream" data-custom-kind="hex" maxlength="7" spellcheck="false" autocapitalize="off" autocorrect="off">
                    </span>
                  </div>
                  <div class="custom-color-row">
                    <label for="custom-creamdim">Secondary text</label>
                    <span class="custom-color-field">
                      <input type="color" id="custom-creamdim" data-custom-token="cream_dim" data-custom-kind="color">
                      <input type="text" class="custom-hex" data-custom-token="cream_dim" data-custom-kind="hex" maxlength="7" spellcheck="false" autocapitalize="off" autocorrect="off">
                    </span>
                  </div>
                  <div class="custom-color-row">
                    <label for="custom-butter">Accent</label>
                    <span class="custom-color-field">
                      <input type="color" id="custom-butter" data-custom-token="butter" data-custom-kind="color">
                      <input type="text" class="custom-hex" data-custom-token="butter" data-custom-kind="hex" maxlength="7" spellcheck="false" autocapitalize="off" autocorrect="off">
                    </span>
                  </div>
                  <div class="custom-color-row">
                    <label for="custom-rule">Borders</label>
                    <span class="custom-color-field">
                      <input type="color" id="custom-rule" data-custom-token="rule" data-custom-kind="color">
                      <input type="text" class="custom-hex" data-custom-token="rule" data-custom-kind="hex" maxlength="7" spellcheck="false" autocapitalize="off" autocorrect="off">
                    </span>
                  </div>
                </div>
                <div class="custom-builder-row">
                  <label for="custom-font">Font</label>
                  <select class="setting-input" id="custom-font" data-custom-token="font" data-custom-kind="font">
                    <option value="fraunces">Fraunces + Grotesk (default)</option>
                    <option value="grotesk">Schibsted Grotesk</option>
                    <option value="system">System sans</option>
                    <option value="typewriter">Fraunces + Plex Mono</option>
                  </select>
                </div>
                <div class="custom-contrast" id="custom-contrast-warning" style="display:none"></div>
                <div class="custom-builder-actions">
                  <button type="button" class="refresh-btn utility-btn" data-action="reset-custom-theme">Reset to vanilla</button>
                </div>
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Surface texture</div>
                  <div class="setting-help">A faint warm grain over the whole surface.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-show-scanlines">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Reduce motion</div>
                  <div class="setting-help">Disable most transitions and animations.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-reduce-motion">
              </div>
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Confirm before lock</div>
                  <div class="setting-help">Ask before signing out.</div>
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
                  <div class="setting-label">Toast notifications</div>
                  <div class="setting-help">In-app feedback for actions and errors.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-toasts-enabled">
              </div>
              <div class="setting-row">
                <label for="settings-toast-duration">Toast duration (ms)</label>
                <input type="number" min="1200" max="8000" step="100" class="setting-input" id="settings-toast-duration">
                <div class="setting-help">How long toast messages stay visible.</div>
              </div>
            </div>
          </div>
        </details>

        <details class="settings-folder">
          <summary>Semantic index</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">
              <div class="setting-row setting-inline">
                <div>
                  <div class="setting-label">Wait for readiness</div>
                  <div class="setting-help">Wait for the Vectorize index before reindex returns.</div>
                </div>
                <input type="checkbox" class="setting-check" id="settings-semantic-wait">
              </div>
              <div class="setting-row">
                <label for="settings-semantic-timeout">Wait timeout (sec)</label>
                <input type="number" min="1" max="900" step="1" class="setting-input" id="settings-semantic-timeout">
                <div class="setting-help">Used when readiness wait is enabled.</div>
              </div>
              <div class="setting-row">
                <label for="settings-semantic-limit">Reindex limit</label>
                <input type="number" min="1" max="2000" step="1" class="setting-input" id="settings-semantic-limit">
                <div class="setting-help">Maximum memories processed per run.</div>
              </div>
              <div class="setting-row setting-span-2">
                <div class="setting-label">Semantic index sync</div>
                <div class="setting-help">Run <code>memory_reindex</code> from the viewer and inspect readiness output.</div>
                <div class="semantic-status-box">
                  <div class="semantic-status-line dim" id="semantic-status-line">No semantic reindex run in this session.</div>
                  <div class="semantic-status-meta" id="semantic-status-meta"></div>
                  <button class="refresh-btn utility-btn" id="semantic-reindex-btn" data-action="run-semantic-reindex">Run semantic reindex</button>
                </div>
              </div>
            </div>
          </div>
        </details>

        <details class="settings-folder" id="settings-data-management">
          <summary>Data management</summary>
          <div class="settings-folder-body">
            <div class="settings-grid">

              <div class="setting-row setting-span-2">
                <div class="setting-label">Export</div>
                <div class="setting-help">Save a backup of all your memories, links, and settings to a <code>.json</code> file. The export carries no account identifiers — only your content.</div>
                <div style="margin-top:0.4rem">
                  <button class="refresh-btn utility-btn" id="export-btn" data-action="run-export">Export data</button>
                  <div class="semantic-status-line dim" id="export-status-line" style="margin-top:0.3rem"></div>
                </div>
              </div>

              <div class="setting-row setting-span-2" style="border-top:1px solid var(--rule-soft);padding-top:0.8rem;margin-top:0.3rem">
                <div class="setting-label">Import</div>
                <div class="setting-help">Restore data from a previously exported <code>.json</code> backup file.</div>
                <div style="margin-top:0.4rem">
                  <input type="file" accept=".json,application/json" id="import-file-input" style="display:none">
                  <button class="refresh-btn utility-btn" id="import-choose-btn" data-action="choose-import-file">Select a .json file</button>
                  <div class="semantic-status-line dim" id="import-file-name" style="margin-top:0.3rem"></div>

                  <div id="import-step-strategy" style="display:none;margin-top:0.6rem;padding-top:0.5rem;border-top:1px solid var(--rule-soft)">
                    <div class="setting-help" style="margin-bottom:0.3rem">How should existing data be handled?</div>
                    <select class="setting-input" id="import-strategy">
                      <option value="merge">Merge — add new entries, update existing</option>
                      <option value="skip_existing">Skip existing — only add new entries</option>
                      <option value="overwrite">Overwrite — erase everything, then import (destructive!)</option>
                    </select>
                    <div class="setting-help" id="import-strategy-help" style="margin-top:0.2rem">Safest option. New entries are added, existing ones are updated.</div>
                  </div>

                  <div id="import-step-run" style="display:none;margin-top:0.6rem;padding-top:0.5rem;border-top:1px solid var(--rule-soft)">
                    <button class="refresh-btn utility-btn" id="import-btn" data-action="run-import">Import data</button>
                  </div>

                  <div class="semantic-status-line dim" id="import-status-line" style="margin-top:0.4rem"></div>
                  <div class="semantic-status-meta" id="import-status-meta"></div>
                </div>
              </div>

              <div class="setting-row setting-span-2" style="border-top:1px solid var(--rule-soft);padding-top:0.8rem;margin-top:0.3rem">
                <div class="setting-label" style="color:var(--clay)">Danger zone</div>
                <div class="setting-help">Permanently delete <strong>all</strong> memories, links, changelog, snapshots, and settings from this brain. This cannot be undone — consider exporting a backup first.</div>
                <div style="margin-top:0.4rem">
                  <button class="refresh-btn utility-btn" id="purge-btn" data-action="run-purge" style="border-color:var(--clay);color:var(--clay)">Purge all data</button>
                  <div class="semantic-status-line dim" id="purge-status-line" style="margin-top:0.3rem"></div>
                </div>
              </div>

              <div class="setting-row setting-span-2" style="border-top:1px solid var(--rule-soft);padding-top:0.6rem;margin-top:0.3rem">
                <div class="setting-help" style="opacity:0.75;line-height:1.5">
                  Privacy note: exported files contain the full text of your memories and metadata. Review the file before sharing it, and never share exports that hold passwords, API keys, or other sensitive information.
                </div>
              </div>

            </div>
          </div>
        </details>
      </div>
    </div>
    <div class="settings-actions">
      <button class="refresh-btn utility-btn" data-action="open-changelog-overlay">View changelog</button>
      <button class="refresh-btn utility-btn" data-action="reset-viewer-settings">Reset defaults</button>
      <button class="refresh-btn" data-action="apply-settings">Save settings</button>
    </div>
  </div>
</div>

<div class="changelog-overlay" id="changelog-overlay" data-action="close-changelog-overlay">
  <div class="changelog-box">
    <div class="changelog-head">
      <div class="changelog-title-group">
        <h3>Release changelog</h3>
        <div class="changelog-subtitle" id="changelog-subtitle">Recent platform updates</div>
      </div>
      <button class="settings-close" data-action="close-changelog">Close</button>
    </div>
    <div class="changelog-list" id="changelog-list"></div>
    <div class="settings-actions" style="margin-top:0.7rem">
      <button class="refresh-btn utility-btn" data-action="open-full-changelog">Open full changelog</button>
    </div>
  </div>
</div>

<div class="toast-wrap" id="toast-wrap"></div>

<script src="/view.js"></script>
</body>
</html>`;
