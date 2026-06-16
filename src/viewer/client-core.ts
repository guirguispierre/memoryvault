import { SERVER_VERSION } from '../constants.js';
import { escapeHtml } from '../utils.js';

export const clientCore = `
  const BASE = location.origin;
  const VIEWER_SERVER_VERSION = '${escapeHtml(SERVER_VERSION)}';
  const GRAPH_RELATION_TYPES = ['related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of'];
  function getGraphRelationColors() {
    const s = getComputedStyle(document.documentElement);
    const v = (name) => s.getPropertyValue(name).trim();
    return {
      related: v('--border-bright') || '#453B2C',
      supports: v('--success') || '#9DB39A',
      contradicts: v('--red') || '#C9826E',
      supersedes: v('--amber') || '#E3C98F',
      causes: v('--causes') || '#A98F5C',
      example_of: v('--info') || '#8C8170',
    };
  }
  let TOKEN = '';
  let SESSION_MODE = 'none';
  let activeFilter = '';
  // Client-side state-tier filter (drawn from the same strength tiers the list
  // groups by). All tiers shown by default; toggling re-renders the grid only.
  let memoryStateFilter = new Set(['active', 'settling', 'resting']);
  // The row the user has selected; drives the graph rail and its card.
  let selectedMemoryIndex = -1;
  let selectedMemoryId = null;
  let searchTimeout = null;
  // corpusMemories is the whole brain (no type/search filter) and drives the
  // recall ribbon and header summary; displayedMemories is the filtered set
  // shown in the grid.
  let corpusMemories = [];
  let displayedMemories = [];
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
  let pourTickId = null;
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
      // Constellation is the default dark base, paired with paper for day, so an
      // unset/new brain gets the brand dark theme at night and paper by day.
      theme: 'constellation',
      light_theme: 'paper',
      theme_mode: 'auto',
      live_poll_enabled: true,
      live_poll_interval_sec: 10,
      time_mode: 'utc',
      default_memory_filter: '',
      search_debounce_ms: 300,
      compact_cards: false,
      custom_theme: buildVanillaCustomTheme(),
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
    const validThemes = ['constellation', 'slate', 'paper', 'vanilla', 'midnight', 'solarized', 'ember', 'arctic', 'custom'];
    // 'cyberpunk' was the pre-vanilla default; migrate stored settings to the new default.
    const migrateTheme = (value) => (value === 'cyberpunk' ? 'vanilla' : value);
    const theme = validThemes.includes(migrateTheme(source.theme)) ? migrateTheme(source.theme) : defaults.theme;
    const light_theme = validThemes.includes(migrateTheme(source.light_theme)) ? migrateTheme(source.light_theme) : defaults.light_theme;
    const validModes = ['auto', 'light', 'dark'];
    const theme_mode = validModes.includes(source.theme_mode) ? source.theme_mode : defaults.theme_mode;
    return {
      theme,
      light_theme,
      theme_mode,
      live_poll_enabled: source.live_poll_enabled !== false,
      live_poll_interval_sec: Math.min(Math.max(Math.round(interval), 5), 120),
      time_mode: source.time_mode === 'local' ? 'local' : 'utc',
      default_memory_filter: defaultFilter,
      search_debounce_ms: Math.min(Math.max(Math.round(searchDebounce), 120), 1500),
      compact_cards: source.compact_cards === true,
      custom_theme: normalizeCustomTheme(source.custom_theme),
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

  // Custom-theme derivation (deriveCustomTokens, normalizeCustomTheme, the
  // colour math, font presets, contrastRatio) and resolveThemeFromSettings
  // live in themeRuntimeJs, concatenated ahead of this script and shared with
  // the page bootstrap so both resolve a theme identically.

  function ensureCustomThemeStyleEl() {
    let el = document.getElementById('mv-custom-theme');
    if (!el) {
      el = document.createElement('style');
      el.id = 'mv-custom-theme';
      document.head.appendChild(el);
    }
    return el;
  }

  // Scoped to [data-theme="custom"] so switching back to a preset deactivates
  // it with no cleanup — the preset's own block simply wins again.
  function applyCustomTheme() {
    const tokens = deriveCustomTokens(viewerSettings && viewerSettings.custom_theme);
    const decls = Object.keys(tokens).map((k) => '    ' + k + ': ' + tokens[k] + ';').join('\\n');
    // html[...] outranks :root, so the custom block wins regardless of order.
    ensureCustomThemeStyleEl().textContent = 'html[data-theme="custom"] {\\n' + decls + '\\n  }';
  }

  // ── Server-side persistence ─────────────────────────────────────────
  // localStorage paints instantly and survives offline; the server copy is
  // the cross-device source of truth. On load the server wins when reachable;
  // on change we write through to both, server in the background.
  const VIEWER_SETTINGS_ENDPOINT = BASE + '/api/viewer-settings';
  let serverSettingsSaveTimer = null;

  async function saveServerViewerSettings() {
    if (!viewerSettings || !hasAuthenticatedSession()) return;
    try {
      await apiFetch(VIEWER_SETTINGS_ENDPOINT, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ settings: viewerSettings }),
      });
    } catch {}
  }

  function scheduleServerSettingsSave() {
    if (!hasAuthenticatedSession()) return;
    clearTimeout(serverSettingsSaveTimer);
    serverSettingsSaveTimer = setTimeout(saveServerViewerSettings, 500);
  }

  async function reconcileServerViewerSettings() {
    if (!hasAuthenticatedSession()) return;
    try {
      const r = await apiFetch(VIEWER_SETTINGS_ENDPOINT);
      if (!r.ok) return; // offline or error: keep the local values already applied
      const data = await r.json();
      if (data && data.settings && typeof data.settings === 'object') {
        viewerSettings = normalizeViewerSettings(data.settings);
        persistViewerSettings();
        applyViewerSettingsToRuntime({ restartPolling: true, rerenderGraph: true, rerenderGrid: true });
        fillSettingsForm();
        updateTime();
      } else {
        // No server row yet: seed it from whatever the client is holding.
        saveServerViewerSettings();
      }
    } catch {}
  }

  const darkModeMediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

  function resolveActiveTheme() {
    if (!viewerSettings) return 'slate';
    return resolveThemeFromSettings(viewerSettings);
  }

  function applyViewerSettingsToRuntime(options = {}) {
    if (!viewerSettings) return;
    const restartPolling = options.restartPolling !== false;
    const rerenderGraph = options.rerenderGraph === true;
    const rerenderGrid = options.rerenderGrid === true;
    graphShowInferred = viewerSettings.graph_show_inferred;
    graphShowLabels = viewerSettings.graph_show_labels;
    graphPhysicsEnabled = viewerSettings.graph_physics_enabled;
    const grid = document.getElementById('grid');
    if (grid) grid.setAttribute('data-density', viewerSettings.compact_cards ? 'compact' : 'comfortable');
    syncDensityToggle();
    document.body.classList.toggle('scanlines-off', !viewerSettings.show_scanlines);
    document.body.classList.toggle('motion-reduced', viewerSettings.reduce_motion);
    applyCustomTheme();
    document.documentElement.setAttribute('data-theme', resolveActiveTheme());
    syncThemePicker();
    syncGraphToolbarState();
    if (restartPolling) startLivePolling(true);
    if (rerenderGrid) renderGrid(displayedMemories);
    if (rerenderGraph && graphVisible) rerenderGraphFromCache();
    // Recolour the rail constellation for the active theme and honour a
    // reduce-motion change.
    if (typeof railBuildNodes === 'function') { railBuildNodes(); railSync(); }
  }

  function initializeViewerSettings() {
    viewerSettings = loadViewerSettings();
    applyViewerSettingsToRuntime({ restartPolling: false, rerenderGraph: false, rerenderGrid: false });
  }

  initializeViewerSettings();
  fillSettingsForm();

  darkModeMediaQuery.addEventListener('change', () => {
    if (viewerSettings?.theme_mode === 'auto') {
      document.documentElement.setAttribute('data-theme', resolveActiveTheme());
    }
  });

  restoreUserSession();

  function setLoginError(message) {
    const el = document.getElementById('login-error');
    if (!el) return;
    el.textContent = message || 'Something went wrong — try again.';
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
    startPourTick();
    const defaultFilter = viewerSettings?.default_memory_filter || '';
    activeFilter = defaultFilter;
    syncFilterPills(activeFilter);
    syncStateChips();
    syncDensityToggle();
    railInit();
    loadMemories();
    startLivePolling();
    reconcileServerViewerSettings();
    showToast('Signed in — loading your index.', 'success');
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
    buttonEl.textContent = semanticReindexRunning ? 'Running semantic reindex…' : 'Run semantic reindex';
    metaEl.innerHTML = '';

    const addPill = (text, cls = '') => {
      const pill = document.createElement('span');
      pill.className = 'semantic-status-pill' + (cls ? (' ' + cls) : '');
      pill.textContent = text;
      metaEl.appendChild(pill);
    };

    if (semanticReindexRunning) {
      lineEl.className = 'semantic-status-line';
      lineEl.textContent = 'Semantic reindex is running — waiting for the MCP response.';
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

  let importSelectedFile = null;
  let importRunning = false;
  let exportRunning = false;

  function updateImportStrategyHelp() {
    const select = document.getElementById('import-strategy');
    const help = document.getElementById('import-strategy-help');
    if (!select || !help) return;
    const v = select.value;
    if (v === 'merge') help.textContent = 'Safest option. New entries are added, existing ones are updated.';
    else if (v === 'skip_existing') help.textContent = 'Conservative. Only adds new entries. Your current data is never modified.';
    else if (v === 'overwrite') help.textContent = 'Destructive! All existing data is permanently deleted before import. You will be asked to confirm.';
  }

  async function runExport() {
    if (!ensureAppReady('Export')) return;
    if (exportRunning) { showToast('Export already in progress.', 'info'); return; }
    exportRunning = true;
    const statusEl = document.getElementById('export-status-line');
    const btn = document.getElementById('export-btn');
    if (btn) btn.disabled = true;
    if (statusEl) { statusEl.className = 'semantic-status-line'; statusEl.textContent = 'Preparing backup file...'; }
    try {
      const r = await apiFetch(BASE + '/api/export');
      if (r.status === 401) { doLogout(true); return; }
      if (!r.ok) throw new Error('Export failed (' + r.status + ')');
      const disposition = r.headers.get('Content-Disposition') || '';
      const match = disposition.match(/filename="(.+?)"/);
      const filename = match ? match[1] : 'memoryvault-export.json';
      const text = await r.text();
      const parsed = JSON.parse(text);
      const stats = parsed.stats || {};
      const blob = new Blob([text], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      setTimeout(() => { URL.revokeObjectURL(url); a.remove(); }, 200);
      const summary = (stats.memories || 0) + ' memories, ' + (stats.memory_links || 0) + ' links';
      if (statusEl) { statusEl.className = 'semantic-status-line'; statusEl.textContent = 'Backup saved: ' + filename + ' (' + summary + ')'; }
      showToast('Backup downloaded — ' + summary + '.', 'success');
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Export failed.';
      if (statusEl) { statusEl.className = 'semantic-status-line error'; statusEl.textContent = msg; }
      showToast(msg, 'error');
    } finally {
      exportRunning = false;
      if (btn) btn.disabled = false;
    }
  }

  function chooseImportFile() {
    const input = document.getElementById('import-file-input');
    if (input) input.click();
  }

  function resetImportSteps() {
    const nameEl = document.getElementById('import-file-name');
    const stepStrategy = document.getElementById('import-step-strategy');
    const stepRun = document.getElementById('import-step-run');
    const statusEl = document.getElementById('import-status-line');
    const metaEl = document.getElementById('import-status-meta');
    importSelectedFile = null;
    const fileInput = document.getElementById('import-file-input');
    if (fileInput) fileInput.value = '';
    if (nameEl) nameEl.textContent = '';
    if (stepStrategy) stepStrategy.style.display = 'none';
    if (stepRun) stepRun.style.display = 'none';
    if (statusEl) { statusEl.className = 'semantic-status-line dim'; statusEl.textContent = ''; }
    if (metaEl) metaEl.innerHTML = '';
  }

  function showImportStep(step) {
    const stepStrategy = document.getElementById('import-step-strategy');
    const stepRun = document.getElementById('import-step-run');
    if (step >= 2 && stepStrategy) stepStrategy.style.display = 'block';
    if (step >= 3 && stepRun) stepRun.style.display = 'block';
    if (step < 3 && stepRun) stepRun.style.display = 'none';
    if (step < 2 && stepStrategy) stepStrategy.style.display = 'none';
  }

  function onImportFileSelected(event) {
    const input = event.target;
    const file = input && input.files && input.files[0];
    const nameEl = document.getElementById('import-file-name');
    const statusEl = document.getElementById('import-status-line');
    const metaEl = document.getElementById('import-status-meta');
    if (!file) {
      resetImportSteps();
      return;
    }
    if (!file.name.toLowerCase().endsWith('.json')) {
      showToast('Please select a .json file.', 'error');
      resetImportSteps();
      return;
    }
    importSelectedFile = file;
    if (nameEl) nameEl.textContent = file.name + ' (' + (file.size / 1024).toFixed(1) + ' KB)';
    if (statusEl) { statusEl.className = 'semantic-status-line dim'; statusEl.textContent = ''; }
    if (metaEl) metaEl.innerHTML = '';
    showImportStep(3);
  }

  function onImportStrategyChanged() {
    updateImportStrategyHelp();
  }

  async function runImport(source) {
    if (!ensureAppReady('Import')) return;
    if (importRunning) { showToast('Import already in progress.', 'info'); return; }
    if (!importSelectedFile) { showToast('Select a file first.', 'info'); return; }

    const strategySelect = document.getElementById('import-strategy');
    const strategy = strategySelect ? strategySelect.value : 'merge';

    if (strategy === 'overwrite') {
      const confirmed = window.confirm(
        'OVERWRITE will permanently delete ALL existing memories, links, changelog, and settings in this brain before importing. This cannot be undone.\\n\\nContinue?'
      );
      if (!confirmed) return;
    }

    importRunning = true;
    const statusEl = document.getElementById('import-status-line');
    const metaEl = document.getElementById('import-status-meta');
    const importBtn = document.getElementById('import-btn');
    if (importBtn) importBtn.disabled = true;
    if (statusEl) { statusEl.className = 'semantic-status-line'; statusEl.textContent = 'Importing (' + strategy + ')...'; }
    if (metaEl) metaEl.innerHTML = '';

    try {
      const text = await importSelectedFile.text();
      let parsed;
      try { parsed = JSON.parse(text); } catch { throw new Error('File is not valid JSON.'); }
      if (!parsed || typeof parsed !== 'object') throw new Error('File content is not a valid object.');
      if (parsed.schema !== 'memoryvault_export_v1') throw new Error('Unsupported file format. Expected memoryvault_export_v1 schema.');

      parsed.strategy = strategy;

      const r = await apiFetch(BASE + '/api/import', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(parsed),
      });
      if (r.status === 401) { doLogout(true); return; }
      const result = await r.json();
      if (!r.ok) throw new Error(result.error || 'Import failed (' + r.status + ')');

      const imported = result.imported || {};

      showImportStep(1);
      importSelectedFile = null;
      const nameEl = document.getElementById('import-file-name');
      if (nameEl) nameEl.textContent = '';
      const fileInput = document.getElementById('import-file-input');
      if (fileInput) fileInput.value = '';

      if (statusEl) {
        statusEl.className = 'semantic-status-line';
        statusEl.textContent = 'Import completed (' + strategy + ').';
      }
      if (metaEl) {
        metaEl.innerHTML = '';
        const addPill = (text, cls) => {
          const pill = document.createElement('span');
          pill.className = 'semantic-status-pill' + (cls ? (' ' + cls) : '');
          pill.textContent = text;
          metaEl.appendChild(pill);
        };
        if (imported.memories > 0) addPill(imported.memories + ' memories');
        if (imported.memory_links > 0) addPill(imported.memory_links + ' links');
        if (imported.memory_changelog > 0) addPill(imported.memory_changelog + ' changelog');
        if (imported.brain_source_trust > 0) addPill(imported.brain_source_trust + ' trust rules');
        if (imported.memory_watches > 0) addPill(imported.memory_watches + ' watches');
        if (imported.skipped > 0) addPill(imported.skipped + ' skipped');
        if (imported.memory_conflict_resolutions > 0) addPill(imported.memory_conflict_resolutions + ' resolutions');
        if (imported.memory_entity_aliases > 0) addPill(imported.memory_entity_aliases + ' aliases');
      }
      showToast('Import completed: ' + (imported.memories || 0) + ' memories, ' + (imported.memory_links || 0) + ' links.', 'success');
      loadMemories(true);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Import failed.';
      if (statusEl) { statusEl.className = 'semantic-status-line error'; statusEl.textContent = msg; }
      showToast(msg, 'error');
    } finally {
      importRunning = false;
      if (importBtn) importBtn.disabled = false;
    }
  }

`;
