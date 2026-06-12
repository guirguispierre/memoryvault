export const clientSettings = `  function fillSettingsForm() {
    if (!viewerSettings) return;
    const livePollEnabled = document.getElementById('settings-live-poll-enabled');
    const livePollInterval = document.getElementById('settings-live-poll-interval');
    const timeMode = document.getElementById('settings-time-mode');
    const defaultFilter = document.getElementById('settings-default-filter');
    const searchDebounce = document.getElementById('settings-search-debounce');
    const compactCards = document.getElementById('settings-compact-cards');
    const graphInferred = document.getElementById('settings-graph-inferred');
    const graphLabels = document.getElementById('settings-graph-labels');
    const graphPhysics = document.getElementById('settings-graph-physics');
    const graphFocus = document.getElementById('settings-graph-focus');
    const autoOpenGraph = document.getElementById('settings-auto-open-graph');
    const toastsEnabled = document.getElementById('settings-toasts-enabled');
    const toastDuration = document.getElementById('settings-toast-duration');
    const confirmLogout = document.getElementById('settings-confirm-logout');
    const showScanlines = document.getElementById('settings-show-scanlines');
    const reduceMotion = document.getElementById('settings-reduce-motion');
    const semanticWait = document.getElementById('settings-semantic-wait');
    const semanticTimeout = document.getElementById('settings-semantic-timeout');
    const semanticLimit = document.getElementById('settings-semantic-limit');
    if (livePollEnabled) livePollEnabled.checked = viewerSettings.live_poll_enabled;
    if (livePollInterval) livePollInterval.value = String(viewerSettings.live_poll_interval_sec);
    if (timeMode) timeMode.value = viewerSettings.time_mode;
    if (defaultFilter) defaultFilter.value = viewerSettings.default_memory_filter || '';
    if (searchDebounce) searchDebounce.value = String(viewerSettings.search_debounce_ms);
    if (compactCards) compactCards.checked = viewerSettings.compact_cards;
    if (graphInferred) graphInferred.checked = viewerSettings.graph_show_inferred;
    if (graphLabels) graphLabels.checked = viewerSettings.graph_show_labels;
    if (graphPhysics) graphPhysics.checked = viewerSettings.graph_physics_enabled;
    if (graphFocus) graphFocus.checked = viewerSettings.graph_focus_highlight;
    if (autoOpenGraph) autoOpenGraph.checked = viewerSettings.auto_open_graph;
    if (toastsEnabled) toastsEnabled.checked = viewerSettings.toasts_enabled;
    if (toastDuration) toastDuration.value = String(viewerSettings.toast_duration_ms);
    if (confirmLogout) confirmLogout.checked = viewerSettings.confirm_logout;
    if (showScanlines) showScanlines.checked = viewerSettings.show_scanlines;
    if (reduceMotion) reduceMotion.checked = viewerSettings.reduce_motion;
    if (semanticWait) semanticWait.checked = viewerSettings.semantic_reindex_wait_for_index;
    if (semanticTimeout) semanticTimeout.value = String(viewerSettings.semantic_reindex_wait_timeout_seconds);
    if (semanticLimit) semanticLimit.value = String(viewerSettings.semantic_reindex_limit);
    syncThemePicker();
    renderSemanticReindexStatus();
  }

  function readSettingsFromForm() {
    const raw = {
      theme: document.querySelector('#theme-picker .theme-swatch.active')?.dataset?.themeValue || viewerSettings?.theme || 'cyberpunk',
      light_theme: document.querySelector('#light-theme-picker .theme-swatch.active')?.dataset?.themeValue || viewerSettings?.light_theme || 'cyberpunk',
      theme_mode: document.querySelector('.theme-mode-btn.active')?.dataset?.mode || viewerSettings?.theme_mode || 'auto',
      live_poll_enabled: document.getElementById('settings-live-poll-enabled')?.checked !== false,
      live_poll_interval_sec: Number(document.getElementById('settings-live-poll-interval')?.value ?? 10),
      time_mode: document.getElementById('settings-time-mode')?.value === 'local' ? 'local' : 'utc',
      default_memory_filter: document.getElementById('settings-default-filter')?.value || '',
      search_debounce_ms: Number(document.getElementById('settings-search-debounce')?.value ?? 300),
      compact_cards: document.getElementById('settings-compact-cards')?.checked === true,
      graph_show_inferred: document.getElementById('settings-graph-inferred')?.checked !== false,
      graph_show_labels: document.getElementById('settings-graph-labels')?.checked !== false,
      graph_physics_enabled: document.getElementById('settings-graph-physics')?.checked !== false,
      graph_focus_highlight: document.getElementById('settings-graph-focus')?.checked !== false,
      auto_open_graph: document.getElementById('settings-auto-open-graph')?.checked === true,
      toasts_enabled: document.getElementById('settings-toasts-enabled')?.checked !== false,
      toast_duration_ms: Number(document.getElementById('settings-toast-duration')?.value ?? 2300),
      confirm_logout: document.getElementById('settings-confirm-logout')?.checked === true,
      show_scanlines: document.getElementById('settings-show-scanlines')?.checked !== false,
      reduce_motion: document.getElementById('settings-reduce-motion')?.checked === true,
      semantic_reindex_wait_for_index: document.getElementById('settings-semantic-wait')?.checked !== false,
      semantic_reindex_wait_timeout_seconds: Number(document.getElementById('settings-semantic-timeout')?.value ?? 180),
      semantic_reindex_limit: Number(document.getElementById('settings-semantic-limit')?.value ?? 500),
    };
    return normalizeViewerSettings(raw);
  }

  function closeSettingsOverlay(event) {
    const overlay = document.getElementById('settings-overlay');
    if (!overlay) return;
    if (event && event.target !== overlay) return;
    overlay.classList.remove('open');
  }

  function openSettingsOverlay() {
    const overlay = document.getElementById('settings-overlay');
    if (!overlay) return;
    fillSettingsForm();
    overlay.classList.add('open');
  }

  function closeChangelogOverlay(event) {
    const overlay = document.getElementById('changelog-overlay');
    if (!overlay) return;
    if (event && event.target !== overlay) return;
    overlay.classList.remove('open');
  }

  function formatChangelogDate(unixTs) {
    const ts = Number(unixTs);
    if (!Number.isFinite(ts) || ts <= 0) return 'Unknown date';
    return new Date(ts * 1000).toISOString().slice(0, 10);
  }

  function renderChangelogEntries(entries, latestVersion) {
    const list = document.getElementById('changelog-list');
    const subtitle = document.getElementById('changelog-subtitle');
    if (!list || !subtitle) return;
    const rows = Array.isArray(entries) ? entries : [];
    const latest = typeof latestVersion === 'string' && latestVersion.trim()
      ? latestVersion.trim()
      : VIEWER_SERVER_VERSION;
    subtitle.textContent = 'Latest version: v' + latest + ' - showing ' + rows.length + ' entries';
    if (!rows.length) {
      list.innerHTML = '<div class="setting-help">No changelog entries available.</div>';
      return;
    }

    list.innerHTML = rows.map((entry) => {
      const version = typeof entry.version === 'string' && entry.version.trim() ? entry.version.trim() : 'unknown';
      const summary = typeof entry.summary === 'string' ? entry.summary : '';
      const releaseDate = formatChangelogDate(entry.released_at);
      const changes = Array.isArray(entry.changes) ? entry.changes : [];
      const changesHtml = changes.slice(0, 16).map((change) => {
        const type = typeof change.type === 'string' && change.type.trim() ? change.type.trim() : 'changed';
        const target = typeof change.target === 'string' && change.target.trim() ? change.target.trim() : '';
        const name = typeof change.name === 'string' && change.name.trim() ? change.name.trim() : 'Untitled change';
        const description = typeof change.description === 'string' && change.description.trim() ? change.description.trim() : '';
        const prefix = target ? (target + ': ') : '';
        const detail = prefix + name + (description ? (' - ' + description) : '');
        return '<li class="changelog-change-row">' +
          '<span class="changelog-change-type">' + esc(type) + '</span>' +
          '<span class="changelog-change-text">' + esc(detail) + '</span>' +
        '</li>';
      }).join('');
      return '<article class="changelog-entry">' +
        '<div class="changelog-entry-head">' +
          '<span class="changelog-entry-version">v' + esc(version) + '</span>' +
          '<span class="changelog-entry-date">' + esc(releaseDate) + '</span>' +
        '</div>' +
        '<div class="changelog-entry-summary">' + esc(summary || 'No summary available.') + '</div>' +
        (changesHtml ? ('<ul class="changelog-change-list">' + changesHtml + '</ul>') : '') +
      '</article>';
    }).join('');
  }

  async function loadChangelogEntries() {
    const list = document.getElementById('changelog-list');
    const subtitle = document.getElementById('changelog-subtitle');
    if (!list || !subtitle) return;
    list.innerHTML = '<div class="setting-help">Loading changelog...</div>';
    subtitle.textContent = 'Fetching latest release notes...';
    try {
      const response = await apiFetch(BASE + '/mcp', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: 'viewer-changelog',
          method: 'tools/call',
          params: {
            name: 'tool_changelog',
            arguments: { limit: 12 },
          },
        }),
      });
      if (response.status === 401) {
        doLogout(true);
        return;
      }
      if (!response.ok) throw new Error('Failed to load changelog.');
      const rpc = await response.json();
      if (rpc && rpc.error) throw new Error(typeof rpc.error.message === 'string' ? rpc.error.message : 'Failed to load changelog.');
      const text = rpc?.result?.content?.[0]?.text;
      if (typeof text !== 'string' || !text.trim()) throw new Error('Invalid changelog response.');
      const parsed = JSON.parse(text);
      renderChangelogEntries(parsed?.entries, parsed?.latest_version);
    } catch (err) {
      const message = err instanceof Error && err.message ? err.message : 'Failed to load changelog.';
      subtitle.textContent = 'Unable to load release notes.';
      list.innerHTML = '<div class="setting-help" style="color:var(--red)">' + esc(message) + '</div>';
    }
  }

  async function openChangelogOverlay() {
    closeSettingsOverlay();
    const overlay = document.getElementById('changelog-overlay');
    if (!overlay) return;
    overlay.classList.add('open');
    await loadChangelogEntries();
  }

  function applySettingsFromForm() {
    viewerSettings = readSettingsFromForm();
    persistViewerSettings();
    applyViewerSettingsToRuntime({ restartPolling: true, rerenderGraph: true, rerenderGrid: true });
    updateTime();
    closeSettingsOverlay();
    showToast('Settings saved.', 'success', true);
  }

  function resetViewerSettings() {
    viewerSettings = buildDefaultViewerSettings();
    persistViewerSettings();
    fillSettingsForm();
    applyViewerSettingsToRuntime({ restartPolling: true, rerenderGraph: true, rerenderGrid: true });
    updateTime();
    showToast('Settings reset to defaults.', 'info', true);
  }

  function syncThemePicker() {
    const darkTheme = viewerSettings?.theme || 'cyberpunk';
    document.querySelectorAll('#theme-picker .theme-swatch').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.themeValue === darkTheme);
    });
    const lightTheme = viewerSettings?.light_theme || 'cyberpunk';
    document.querySelectorAll('#light-theme-picker .theme-swatch').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.themeValue === lightTheme);
    });
    const currentMode = viewerSettings?.theme_mode || 'auto';
    document.querySelectorAll('.theme-mode-btn').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.mode === currentMode);
    });
  }

  function syncFilterPills(type) {
    ['all','note','fact','journal','graph'].forEach(t => {
      document.getElementById('stat-' + t).classList.toggle('active', (type === '' ? 'all' : type) === t);
    });
  }

  function setFilter(type) {
    graphVisible = false;
    const graphView = document.getElementById('graph-view');
    graphView.classList.remove('visible');
    graphView.style.display = 'none';
    document.querySelector('.grid-wrap').style.display = 'grid';
    activeFilter = type;
    syncFilterPills(type);
    loadMemories();
  }

  function onSearch(val) {
    clearTimeout(searchTimeout);
    const debounceMs = Math.min(Math.max(Number(viewerSettings?.search_debounce_ms ?? 300), 120), 1500);
    searchTimeout = setTimeout(loadMemories, debounceMs);
  }

  function esc(str) {
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  function expandById(id) {
    const idx = allMemories.findIndex(m => m.id === id);
    if (idx !== -1) {
      expandCard(idx);
    } else {
      // Memory not found in current view (may be filtered out or not yet loaded)
      const connEl = document.getElementById('expand-connections');
      if (connEl) {
        const note = document.createElement('div');
        note.style.cssText = 'font-size:0.65rem;color:var(--text-dim);letter-spacing:0.1em;margin-top:0.5rem';
        note.textContent = '⚠ Linked memory not visible in current filter.';
        const existing = connEl.querySelector('.connections-section');
        if (existing) {
          existing.appendChild(note);
        } else {
          connEl.appendChild(note);
        }
      }
    }
  }

  let lastPollSig = '';
  let pollIntervalId = null;

  function startLivePolling(forceRestart = false) {
    if (forceRestart && pollIntervalId) {
      clearInterval(pollIntervalId);
      pollIntervalId = null;
    }
    const liveEl = document.getElementById('live-indicator');
    const pollingEnabled = !viewerSettings || viewerSettings.live_poll_enabled;
    if (!pollingEnabled) {
      if (liveEl) liveEl.style.display = 'none';
      return;
    }
    if (pollIntervalId) return;
    if (liveEl) liveEl.style.display = 'flex';
    const intervalMs = Math.min(Math.max((viewerSettings?.live_poll_interval_sec ?? 10) * 1000, 5000), 120000);
    pollIntervalId = setInterval(async () => {
      if (!hasAuthenticatedSession()) return;
      try {
        const r = await apiFetch(BASE + '/api/memories?limit=1');
        if (!r.ok) return;
        const data = await r.json();
        const sig = (data.stats || []).map(s => s.type + ':' + s.count).join('|');
        if (lastPollSig && sig !== lastPollSig) {
          loadMemories(true); // silent refresh
        }
        lastPollSig = sig;
      } catch {}
    }, intervalMs);
  }

  function syncGraphToolbarState() {
    const inferredBtn = document.getElementById('graph-toggle-inferred');
    const labelsBtn = document.getElementById('graph-toggle-labels');
    const physicsBtn = document.getElementById('graph-toggle-physics');
    if (inferredBtn) {
      inferredBtn.classList.toggle('active', graphShowInferred);
      inferredBtn.classList.toggle('off', !graphShowInferred);
      inferredBtn.textContent = graphShowInferred ? 'INFERRED ON' : 'INFERRED OFF';
    }
    if (labelsBtn) {
      labelsBtn.classList.toggle('active', graphShowLabels);
      labelsBtn.classList.toggle('off', !graphShowLabels);
      labelsBtn.textContent = graphShowLabels ? 'LABELS ON' : 'LABELS OFF';
    }
    if (physicsBtn) {
      physicsBtn.classList.toggle('active', graphPhysicsEnabled);
      physicsBtn.classList.toggle('off', !graphPhysicsEnabled);
      physicsBtn.textContent = graphPhysicsEnabled ? 'PHYSICS ON' : 'PHYSICS OFF';
    }
    GRAPH_RELATION_TYPES.forEach((relation) => {
      const btn = document.getElementById('graph-rel-' + relation);
      if (!btn) return;
      const active = graphRelationFilter.has(relation);
      btn.classList.toggle('active', active);
      btn.classList.toggle('off', !active);
    });
  }

  function onGraphSearch(value) {
    graphSearchQuery = String(value || '').trim().toLowerCase();
    if (graphVisible) rerenderGraphFromCache();
  }

  function toggleGraphRelation(relation) {
    if (!GRAPH_RELATION_TYPES.includes(relation)) return;
    if (graphRelationFilter.has(relation)) {
      if (graphRelationFilter.size === 1) return;
      graphRelationFilter.delete(relation);
    } else {
      graphRelationFilter.add(relation);
    }
    syncGraphToolbarState();
    if (graphVisible) rerenderGraphFromCache();
  }

  function updateGraphLegend(nodesCount, explicitCount, inferredVisibleCount, inferredTotal, relationCounts = {}, avgConfidence = null, avgImportance = null, matchCount = null) {
    const legend = document.getElementById('graph-legend');
    if (!legend) return;
    const inferredText = graphShowInferred
      ? \`INFERRED \${inferredVisibleCount}/\${inferredTotal}\`
      : \`INFERRED OFF (\${inferredTotal} AVAIL)\`;
    const relationPriority = ['contradicts', 'supports', 'supersedes', 'causes', 'example_of'];
    const relationText = relationPriority
      .filter((key) => relationCounts[key] > 0)
      .slice(0, 2)
      .map((key) => \`\${key.toUpperCase().replace('_', ' ')} \${relationCounts[key]}\`)
      .join(' · ');
    const avgConfText = avgConfidence === null ? '' : \`<span class="graph-legend-item">AVG CONF \${Math.round(avgConfidence * 100)}%</span>\`;
    const avgImpText = avgImportance === null ? '' : \`<span class="graph-legend-item">AVG IMP \${Math.round(avgImportance * 100)}%</span>\`;
    const matchText = matchCount === null ? '' : \`<span class="graph-legend-item">MATCH \${matchCount}</span>\`;
    legend.innerHTML = \`
      <span class="graph-legend-item">NODES \${nodesCount}</span>
      <span class="graph-legend-item">LINKS \${explicitCount}</span>
      <span class="graph-legend-item">\${inferredText}</span>
      \${relationText ? \`<span class="graph-legend-item">\${relationText}</span>\` : ''}
      \${avgConfText}
      \${avgImpText}
      \${matchText}
    \`;
  }

  function cloneGraphData() {
    return {
      nodes: (lastGraphData.nodes || []).map(n => ({ ...n })),
      edges: (lastGraphData.edges || []).map(e => ({ ...e })),
      inferred_edges: (lastGraphData.inferred_edges || []).map(e => ({ ...e })),
    };
  }

  function rerenderGraphFromCache() {
    const data = cloneGraphData();
    renderGraph(data.nodes, data.edges, data.inferred_edges);
  }

  function toggleGraphInferred() {
    graphShowInferred = !graphShowInferred;
    syncGraphToolbarState();
    if (graphVisible) rerenderGraphFromCache();
    showToast(graphShowInferred ? 'Inferred edges enabled.' : 'Inferred edges disabled.', 'info');
  }

  function toggleGraphLabels() {
    graphShowLabels = !graphShowLabels;
    syncGraphToolbarState();
    if (graphVisible) rerenderGraphFromCache();
    showToast(graphShowLabels ? 'Graph labels enabled.' : 'Graph labels hidden.', 'info');
  }

  function toggleGraphPhysics() {
    graphPhysicsEnabled = !graphPhysicsEnabled;
    syncGraphToolbarState();
    if (!graphSimulation) return;
    if (graphPhysicsEnabled) {
      graphSimulation.alpha(0.55).restart();
    } else {
      graphSimulation.stop();
    }
    showToast(graphPhysicsEnabled ? 'Graph physics resumed.' : 'Graph physics paused.', 'info');
  }

  function resetGraphView() {
    if (!graphSvgSelection || !graphZoomBehavior) return;
    graphSvgSelection.transition().duration(220).call(graphZoomBehavior.transform, d3.zoomIdentity);
    graphRelationFilter = new Set(GRAPH_RELATION_TYPES);
    graphSearchQuery = '';
    const searchInput = document.getElementById('graph-search-input');
    if (searchInput) searchInput.value = '';
    if (graphPhysicsEnabled && graphSimulation) graphSimulation.alpha(0.45).restart();
    syncGraphToolbarState();
    rerenderGraphFromCache();
    showToast('Graph view reset.', 'success');
  }

  async function showGraph() {
    graphVisible = true;
    syncGraphToolbarState();
    ['all','note','fact','journal'].forEach(t => {
      document.getElementById('stat-' + t).classList.remove('active');
    });
    document.getElementById('stat-graph').classList.add('active');
    document.querySelector('.grid-wrap').style.display = 'none';
    const graphView = document.getElementById('graph-view');
    graphView.classList.remove('visible');
    graphView.style.display = 'block';
    requestAnimationFrame(() => graphView.classList.add('visible'));
    const emptyEl = document.getElementById('graph-empty');
    if (emptyEl) emptyEl.style.display = 'none';
    const legendEl = document.getElementById('graph-legend');
    if (legendEl) legendEl.innerHTML = '';

    const svg = document.getElementById('graph-svg');
    svg.innerHTML = '<text x="50%" y="50%" text-anchor="middle" style="fill:var(--amber);font-family:var(--mono);font-size:0.7rem;letter-spacing:0.15em">LOADING GRAPH...</text>';

    try {
      const r = await apiFetch(BASE + '/api/graph');
      if (r.status === 401) { doLogout(true); return; }
      if (!r.ok) throw new Error('failed');
      const data = await r.json();
      lastGraphData = {
        nodes: (data.nodes || []).map(n => ({ ...n })),
        edges: (data.edges || []).map(e => ({ ...e })),
        inferred_edges: (data.inferred_edges || []).map(e => ({ ...e })),
      };
      if (!graphAutoTunedLabels && (lastGraphData.edges.length + lastGraphData.inferred_edges.length) > 80) {
        graphShowLabels = false;
        graphAutoTunedLabels = true;
      }
      syncGraphToolbarState();
      rerenderGraphFromCache();
      showToast('Graph loaded: ' + lastGraphData.nodes.length + ' nodes.', 'success');
    } catch(e) {
      document.getElementById('graph-svg').innerHTML = '<text x="50%" y="50%" text-anchor="middle" style="fill:var(--red);font-family:var(--mono);font-size:0.7rem;letter-spacing:0.15em">ERROR LOADING GRAPH</text>';
      showToast('Graph load failed.', 'error');
    }
  }

`;
