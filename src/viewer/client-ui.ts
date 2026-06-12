export const clientUi = `  function runImportFromSettings() { return runImport('settings'); }

  async function runPurge() {
    if (!ensureAppReady('Purge')) return;
    const statusEl = document.getElementById('purge-status-line');
    const btn = document.getElementById('purge-btn');

    const first = window.confirm(
      'This will permanently delete ALL memories, links, changelog, snapshots, and settings from this brain.\\n\\nThis cannot be undone. Are you sure?'
    );
    if (!first) return;

    const second = window.prompt(
      'To confirm, type PURGE below:'
    );
    if (second !== 'PURGE') {
      showToast('Purge cancelled.', 'info');
      return;
    }

    if (btn) btn.disabled = true;
    if (statusEl) { statusEl.className = 'semantic-status-line'; statusEl.textContent = 'Purging all data...'; }

    try {
      const r = await apiFetch(BASE + '/api/purge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ confirm: 'PURGE ALL DATA' }),
      });
      if (r.status === 401) { doLogout(true); return; }
      const result = await r.json();
      if (!r.ok) throw new Error(result.error || 'Purge failed.');
      const purged = result.purged || {};
      if (statusEl) { statusEl.className = 'semantic-status-line'; statusEl.textContent = 'Purged ' + (purged.memories || 0) + ' memories and ' + (purged.links || 0) + ' links.'; }
      showToast('All data has been purged.', 'success', true);
      loadMemories(false);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Purge failed.';
      if (statusEl) { statusEl.className = 'semantic-status-line error'; statusEl.textContent = msg; }
      showToast(msg, 'error');
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  async function doTokenLogin() {
    clearLoginError();
    const val = document.getElementById('token-input').value.trim();
    if (!val) {
      setLoginError('⚠ ENTER A TOKEN');
      return;
    }
    try {
      const r = await fetch(BASE + '/api/memories?limit=1', {
        headers: { 'Authorization': 'Bearer ' + val },
      });
      if (!r.ok) {
        setLoginError('⚠ ACCESS DENIED — invalid token');
        return;
      }
      TOKEN = val;
      SESSION_MODE = 'legacy';
      enterApp();
      showToast('Legacy token accepted.', 'success');
    } catch {
      setLoginError('⚠ NETWORK ERROR');
      showToast('Network error while validating token.', 'error');
    }
  }

  async function doCredentialAuth(mode) {
    clearLoginError();
    const email = document.getElementById('email-input').value.trim();
    const password = document.getElementById('password-input').value;
    const brainName = document.getElementById('brain-name-input').value.trim();
    if (!email || !password) {
      setLoginError('⚠ EMAIL + PASSWORD REQUIRED');
      return;
    }

    const payload = { email, password };
    if (mode === 'signup' && brainName) payload.brain_name = brainName;

    try {
      const endpoint = mode === 'signup' ? '/auth/signup' : '/auth/login';
      const r = await fetch(BASE + endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify(payload),
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) {
        setLoginError('⚠ ' + (data.error || 'AUTH FAILED'));
        return;
      }
      TOKEN = '';
      SESSION_MODE = 'user';
      enterApp();
      showToast(mode === 'signup' ? 'Account created and signed in.' : 'Signed in successfully.', 'success');
    } catch {
      setLoginError('⚠ NETWORK ERROR');
      showToast('Network error during authentication.', 'error');
    }
  }

  function doLogin() {
    return doTokenLogin();
  }

  async function doLogout(force = false) {
    if (!force && viewerSettings?.confirm_logout) {
      const ok = window.confirm('Lock and sign out of the current session?');
      if (!ok) return;
    }
    if (SESSION_MODE === 'user') {
      try {
        await tryRefreshSession();
        await fetch(BASE + '/auth/logout', {
          method: 'POST',
          credentials: 'same-origin',
        });
      } catch {}
    }
    if (pollIntervalId) {
      clearInterval(pollIntervalId);
      pollIntervalId = null;
    }
    if (clockIntervalId) {
      clearInterval(clockIntervalId);
      clockIntervalId = null;
    }
    TOKEN = '';
    SESSION_MODE = 'none';
    location.reload();
  }

  function updateTime() {
    const el = document.getElementById('hdr-time');
    if (el) {
      if (viewerSettings && viewerSettings.time_mode === 'local') {
        const local = new Date().toLocaleString();
        el.textContent = local + ' LOCAL';
      } else {
        el.textContent = new Date().toISOString().replace('T',' ').slice(0,19) + ' UTC';
      }
    }
  }

  function startClock() {
    if (clockIntervalId) clearInterval(clockIntervalId);
    updateTime();
    clockIntervalId = setInterval(updateTime, 1000);
  }

  function pulseStatPill(id, changed) {
    if (!changed) return;
    const el = document.getElementById(id);
    if (!el) return;
    el.classList.remove('pulse');
    void el.offsetWidth;
    el.classList.add('pulse');
  }

  async function loadMemories(silent = false) {
    const grid = document.getElementById('grid');
    const refreshBtn = document.querySelector('.refresh-btn');
    const scrollY = window.scrollY;
    if (!silent) {
      grid.innerHTML = '<div class="loading"><div class="loading-dot"></div><div class="loading-dot"></div><div class="loading-dot"></div></div>';
    }
    if (refreshBtn && !silent) refreshBtn.classList.add('syncing');
    const search = document.getElementById('search-input').value;
    let url = BASE + '/api/memories?limit=500';
    if (activeFilter) url += '&type=' + encodeURIComponent(activeFilter);
    if (search) url += '&search=' + encodeURIComponent(search);
    try {
      const r = await apiFetch(url);
      if (r.status === 401) { doLogout(true); return; }
      if (!r.ok) {
        if (!silent) {
          grid.innerHTML = '<div class="empty-state"><div class="empty-icon">⚠</div>ERROR LOADING MEMORIES</div>';
          showToast('Memory load failed (' + r.status + ').', 'error');
        }
        return;
      }
      const data = await r.json();
      allMemories = data.memories || [];
      updateStats(data.stats || [], allMemories);
      renderGrid(allMemories);
      if (silent) window.scrollTo(0, scrollY);
    } catch(e) {
      grid.innerHTML = '<div class="empty-state"><div class="empty-icon">⚠</div>CONNECTION ERROR</div>';
      showToast('Connection error while loading memories.', 'error');
    } finally {
      if (refreshBtn) refreshBtn.classList.remove('syncing');
    }
  }

  function updateStats(stats, memories = []) {
    const counts = { note: 0, fact: 0, journal: 0 };
    let total = 0;
    stats.forEach(s => { counts[s.type] = s.count; total += s.count; });
    document.getElementById('count-all').textContent = total;
    document.getElementById('count-note').textContent = counts.note;
    document.getElementById('count-fact').textContent = counts.fact;
    document.getElementById('count-journal').textContent = counts.journal;
    pulseStatPill('stat-all', lastStatsSnapshot.all !== null && total !== lastStatsSnapshot.all);
    pulseStatPill('stat-note', lastStatsSnapshot.note !== null && counts.note !== lastStatsSnapshot.note);
    pulseStatPill('stat-fact', lastStatsSnapshot.fact !== null && counts.fact !== lastStatsSnapshot.fact);
    pulseStatPill('stat-journal', lastStatsSnapshot.journal !== null && counts.journal !== lastStatsSnapshot.journal);
    lastStatsSnapshot = { all: total, note: counts.note, fact: counts.fact, journal: counts.journal };
    const confidenceValues = memories
      .map((m) => Number(m.dynamic_confidence ?? m.confidence))
      .filter((v) => Number.isFinite(v));
    const avgConfidence = confidenceValues.length
      ? Math.round((confidenceValues.reduce((a, b) => a + b, 0) / confidenceValues.length) * 100)
      : null;
    document.getElementById('hdr-count').textContent = avgConfidence === null
      ? (total + ' entries')
      : (total + ' entries · avg conf ' + avgConfidence + '%');
  }

  function renderGrid(memories) {
    const grid = document.getElementById('grid');
    if (!memories.length) {
      grid.innerHTML = '<div class="empty-state"><div class="empty-icon">◈</div>NO MEMORIES FOUND</div>';
      return;
    }
    grid.innerHTML = memories.map((m, i) => {
      const date = new Date(m.created_at * 1000).toISOString().slice(0,10);
      const tags = m.tags ? m.tags.split(',').map(t => \`<span class="tag">\${esc(t.trim())}</span>\`).join('') : '';
      const linkBadge = m.link_count > 0 ? \`<span class="card-links-badge">⬡ \${m.link_count} connections</span>\` : '';
      const titleHtml = m.title ? \`<div class="card-title">\${esc(m.title)}</div>\` : '';
      const keyHtml = m.key ? \`<div class="card-key"><span>KEY /</span> \${esc(m.key)}</div>\` : '';
      const confidenceNum = Number(m.dynamic_confidence ?? m.confidence);
      const importanceNum = Number(m.dynamic_importance ?? m.importance);
      const confidencePct = Number.isFinite(confidenceNum) ? Math.round(Math.min(Math.max(confidenceNum, 0), 1) * 100) : null;
      const importancePct = Number.isFinite(importanceNum) ? Math.round(Math.min(Math.max(importanceNum, 0), 1) * 100) : null;
      const sourceLabel = m.source ? String(m.source).trim() : '';
      const sourceDisplay = sourceLabel.length > 18 ? (sourceLabel.slice(0, 17) + '…') : sourceLabel;
      const sourceChip = sourceDisplay ? \`<span class="quality-chip src">SRC \${esc(sourceDisplay)}</span>\` : '';
      const confChip = confidencePct === null ? '' : \`<span class="quality-chip conf">CONF \${confidencePct}%</span>\`;
      const impChip = importancePct === null ? '' : \`<span class="quality-chip imp">IMP \${importancePct}%</span>\`;
      const qualityChips = sourceChip || confChip || impChip
        ? \`<div class="card-quality">\${sourceChip}\${confChip}\${impChip}</div>\`
        : '';
      return \`<div class="card" data-type="\${m.type}" data-idx="\${i}" data-action="expand-card" data-card-index="\${i}" style="animation-delay:\${Math.min(i*0.04,0.4)}s">
        <div class="card-type-stripe"></div>
        <div class="card-header">
          <div>\${titleHtml}\${keyHtml}\${!m.title && !m.key ? '<div class="card-title" style="opacity:0.4">untitled</div>' : ''}</div>
          <span class="card-type-badge">\${m.type}</span>
        </div>
        <div class="card-content">\${esc(m.content)}</div>
        <div class="card-footer">
          <div class="card-meta">
            <div class="card-tags">\${tags}\${linkBadge}</div>
            \${qualityChips}
          </div>
          <div class="card-date">\${date}</div>
        </div>
        <div class="card-id">\${m.id}</div>
      </div>\`;
    }).join('');
  }

  function expandCard(idx) {
    const m = allMemories[idx];
    if (!m) return;
    const date = new Date(m.created_at * 1000).toLocaleString();
    const updated = m.updated_at !== m.created_at ? '  ·  Updated ' + new Date(m.updated_at * 1000).toLocaleString() : '';
    const typeColors = { note: 'var(--teal)', fact: 'var(--amber)', journal: 'var(--journal)' };
    const qualityChips = [
      m.source ? \`<span class="tag">src:\${esc(m.source)}</span>\` : '',
      Number.isFinite(Number(m.dynamic_confidence ?? m.confidence)) ? \`<span class="tag">conf:\${Math.round(Number(m.dynamic_confidence ?? m.confidence) * 100)}%</span>\` : '',
      Number.isFinite(Number(m.dynamic_importance ?? m.importance)) ? \`<span class="tag">imp:\${Math.round(Number(m.dynamic_importance ?? m.importance) * 100)}%</span>\` : '',
    ].filter(Boolean).join('');
    document.getElementById('expand-header').innerHTML =
      \`<div style="display:flex;align-items:center;gap:0.75rem;margin-bottom:0.5rem;flex-wrap:wrap">
        <span style="font-size:0.6rem;letter-spacing:0.2em;text-transform:uppercase;border:1px solid \${typeColors[m.type]||'#fff'};color:\${typeColors[m.type]||'#fff'};padding:0.2rem 0.5rem">\${m.type}</span>
        \${m.title ? \`<span style="font-family:var(--sans);font-weight:700;font-size:1.1rem;color:var(--text-bright)">\${esc(m.title)}</span>\` : ''}
        \${m.key ? \`<span style="font-size:0.75rem;color:var(--amber)">KEY: \${esc(m.key)}</span>\` : ''}
      </div>
      \${m.tags ? \`<div style="display:flex;flex-wrap:wrap;gap:0.3rem;margin-bottom:0.25rem">\${m.tags.split(',').map(t => \`<span class="tag">\${esc(t.trim())}</span>\`).join('')}</div>\` : ''}
      \${qualityChips ? \`<div style="display:flex;flex-wrap:wrap;gap:0.3rem;margin-bottom:0.25rem">\${qualityChips}</div>\` : ''}\`;
    document.getElementById('expand-content').textContent = m.content;
    document.getElementById('expand-meta').textContent = 'ID: ' + m.id + '  ·  Created ' + date + updated;
    document.getElementById('expand-overlay').classList.add('open');
    document.body.style.overflow = 'hidden';

    // Lazy-load connections
    const connEl = document.getElementById('expand-connections');
    connEl.innerHTML = '<div style="font-size:0.65rem;color:var(--text-dim);letter-spacing:0.1em;margin-top:1rem">LOADING CONNECTIONS...</div>';
    const myGen = ++expandGen;
    apiFetch(BASE + '/api/links/' + m.id)
      .then(r => {
        if (r.status === 401) { doLogout(true); return null; }
        if (!r.ok) throw new Error('fetch failed');
        return r.json();
      })
      .then(links => {
        if (!links) return;
        if (myGen !== expandGen) return; // card changed, discard stale result
        if (!links || !links.length) { connEl.innerHTML = ''; return; }
        connEl.innerHTML = \`<div class="connections-section">
          <div class="connections-title">⬡ Connections (\${links.length})</div>
          \${links.map(l => {
            const cm = l.memory;
            const relationRaw = String(l.relation_type || 'related').toLowerCase();
            const relationLabel = relationRaw.replace(/_/g, ' ');
            const relationClass = relationRaw.replace(/_/g, '-').replace(/[^a-z-]/g, '');
            const label = l.label ? \`<span class="chip-label">"\${esc(l.label)}"</span>\` : '';
            const name = cm.title || cm.key || (cm.content || '').slice(0, 40) + '…';
            const arrow = l.direction === 'from' ? '→' : '←';
            return \`<span class="connection-chip" data-conn-id="\${esc(cm.id)}">
              <span class="chip-type">[\${esc(cm.type)}]</span>
              \${esc(name)}
              <span class="chip-relation \${esc(relationClass)}">\${esc(relationLabel)}</span>
              \${label}
              <span style="opacity:0.4">\${arrow}</span>
            </span>\`;
          }).join('')}
        </div>\`;
        connEl.querySelectorAll('.connection-chip').forEach(chip => {
          chip.addEventListener('click', () => expandById(chip.dataset.connId));
        });
      })
      .catch(() => { if (myGen === expandGen) connEl.innerHTML = ''; });
  }

  function closeExpand(e) {
    if (e.target === document.getElementById('expand-overlay')) closeExpandBtn();
  }
  function closeExpandBtn() {
    document.getElementById('expand-overlay').classList.remove('open');
    document.body.style.overflow = '';
  }

  function appIsVisible() {
    const app = document.getElementById('app');
    if (!app) return false;
    return window.getComputedStyle(app).display !== 'none';
  }

  function ensureAppReady(actionLabel = 'This action') {
    if (hasAuthenticatedSession() && appIsVisible()) return true;
    showToast(actionLabel + ' is available after sign in.', 'info');
    return false;
  }

  function getCommandPaletteActions() {
    return [
      {
        label: 'Refresh memories',
        detail: 'Reload data from API',
        run: () => {
          if (!ensureAppReady('Refresh')) return;
          loadMemories();
          showToast('Refreshing memories...', 'info');
        },
      },
      {
        label: 'Open graph view',
        detail: 'Explore memory network',
        run: async () => {
          if (!ensureAppReady('Graph view')) return;
          await showGraph();
          showToast('Graph view opened.', 'success');
        },
      },
      {
        label: 'Show all memories',
        detail: 'Clear type filter',
        run: () => {
          if (!ensureAppReady('Memory filter')) return;
          setFilter('');
          showToast('Showing all memory types.', 'info');
        },
      },
      {
        label: 'Focus search',
        detail: 'Jump to primary search',
        run: () => {
          if (!ensureAppReady('Search focus')) return;
          const input = document.getElementById('search-input');
          if (!input) return;
          input.focus();
          input.select();
          showToast('Search focused.', 'success');
        },
      },
      {
        label: 'Focus graph search',
        detail: 'Node and edge query',
        run: async () => {
          if (!ensureAppReady('Graph search')) return;
          if (!graphVisible) await showGraph();
          const input = document.getElementById('graph-search-input');
          if (!input) return;
          input.focus();
          input.select();
          showToast('Graph search focused.', 'success');
        },
      },
      {
        label: graphShowInferred ? 'Disable inferred edges' : 'Enable inferred edges',
        detail: graphShowInferred ? 'Currently ON' : 'Currently OFF',
        run: async () => {
          if (!ensureAppReady('Graph controls')) return;
          if (!graphVisible) await showGraph();
          toggleGraphInferred();
        },
      },
      {
        label: graphShowLabels ? 'Hide graph labels' : 'Show graph labels',
        detail: graphShowLabels ? 'Currently ON' : 'Currently OFF',
        run: async () => {
          if (!ensureAppReady('Graph controls')) return;
          if (!graphVisible) await showGraph();
          toggleGraphLabels();
        },
      },
      {
        label: graphPhysicsEnabled ? 'Pause graph physics' : 'Resume graph physics',
        detail: graphPhysicsEnabled ? 'Currently ON' : 'Currently OFF',
        run: async () => {
          if (!ensureAppReady('Graph controls')) return;
          if (!graphVisible) await showGraph();
          toggleGraphPhysics();
        },
      },
      {
        label: 'Open keyboard shortcuts',
        detail: 'Help overlay',
        run: () => toggleShortcutsOverlay(),
      },
      {
        label: 'Reindex semantic memory',
        detail: 'Limit ' + (viewerSettings?.semantic_reindex_limit ?? 500) +
          ' · wait ' + ((viewerSettings?.semantic_reindex_wait_for_index ?? true) ? 'on' : 'off'),
        run: async () => {
          if (!ensureAppReady('Semantic reindex')) return;
          await runSemanticReindex('command');
        },
      },
      {
        label: 'Export brain data',
        detail: 'Download all data as JSON file',
        run: async () => {
          if (!ensureAppReady('Export')) return;
          await runExport();
        },
      },
      {
        label: 'Import brain data',
        detail: 'Restore from a backup file',
        run: () => {
          if (!ensureAppReady('Import')) return;
          openSettingsOverlay();
          setTimeout(() => {
            const section = document.getElementById('settings-data-management');
            if (section) { section.open = true; section.scrollIntoView({ behavior: 'smooth', block: 'center' }); }
          }, 120);
        },
      },
      {
        label: 'Purge all data',
        detail: 'Permanently delete everything (danger)',
        run: async () => {
          if (!ensureAppReady('Purge')) return;
          await runPurge();
        },
      },
      {
        label: 'Open settings',
        detail: 'Viewer preferences',
        run: () => openSettingsOverlay(),
      },
      {
        label: 'Open changelog',
        detail: 'Recent release notes',
        run: () => {
          if (!ensureAppReady('Changelog')) return;
          openChangelogOverlay();
        },
      },
      {
        label: 'Lock session',
        detail: 'Sign out',
        run: () => {
          if (!ensureAppReady('Logout')) return;
          doLogout();
        },
      },
    ];
  }

  function updateCommandActiveSelection() {
    const list = document.getElementById('cmd-list');
    if (!list) return;
    list.querySelectorAll('.cmd-item').forEach((el, idx) => {
      el.classList.toggle('active', idx === commandActiveIndex);
    });
  }

  function renderCommandPalette() {
    const list = document.getElementById('cmd-list');
    if (!list) return;
    const query = commandQuery.trim().toLowerCase();
    const allActions = getCommandPaletteActions();
    commandVisibleActions = allActions.filter((action) => {
      if (!query) return true;
      return (action.label + ' ' + action.detail).toLowerCase().includes(query);
    });
    if (commandActiveIndex >= commandVisibleActions.length) {
      commandActiveIndex = Math.max(commandVisibleActions.length - 1, 0);
    }

    if (!commandVisibleActions.length) {
      list.innerHTML = '<div class="cmd-empty">No matching actions</div>';
      return;
    }

    list.innerHTML = commandVisibleActions.map((action, idx) =>
      '<button type="button" class="cmd-item ' + (idx === commandActiveIndex ? 'active' : '') + '" data-command-index="' + idx + '">' +
      '<span class="cmd-item-label">' + esc(action.label) + '</span>' +
      '<span class="cmd-item-detail">' + esc(action.detail) + '</span>' +
      '</button>'
    ).join('');

    list.querySelectorAll('.cmd-item').forEach((el) => {
      const index = Number(el.getAttribute('data-command-index') || '0');
      el.addEventListener('mouseenter', () => {
        commandActiveIndex = index;
        updateCommandActiveSelection();
      });
      el.addEventListener('click', () => runCommandAction(index));
    });
  }

  function onCommandFilter(value) {
    commandQuery = String(value || '');
    commandActiveIndex = 0;
    renderCommandPalette();
  }

  function moveCommandSelection(delta) {
    if (!commandVisibleActions.length) return;
    const next = commandActiveIndex + delta;
    if (next < 0) commandActiveIndex = commandVisibleActions.length - 1;
    else if (next >= commandVisibleActions.length) commandActiveIndex = 0;
    else commandActiveIndex = next;
    updateCommandActiveSelection();
  }

  function runCommandAction(index = commandActiveIndex) {
    const action = commandVisibleActions[index];
    if (!action) return;
    closeCommandPalette();
    Promise.resolve(action.run()).catch(() => showToast('Command failed.', 'error'));
  }

  function openCommandPalette() {
    const overlay = document.getElementById('cmd-overlay');
    const input = document.getElementById('cmd-input');
    if (!overlay || !input) return;
    commandPaletteOpen = true;
    commandQuery = '';
    commandActiveIndex = 0;
    input.value = '';
    overlay.classList.add('open');
    renderCommandPalette();
    setTimeout(() => input.focus(), 0);
  }

  function closeCommandPalette(event) {
    const overlay = document.getElementById('cmd-overlay');
    if (!overlay) return;
    if (event && event.target !== overlay) return;
    commandPaletteOpen = false;
    overlay.classList.remove('open');
  }

  function closeShortcutsOverlay(event) {
    const overlay = document.getElementById('shortcuts-overlay');
    if (!overlay) return;
    if (event && event.target !== overlay) return;
    overlay.classList.remove('open');
  }

  function toggleShortcutsOverlay() {
    const overlay = document.getElementById('shortcuts-overlay');
    if (!overlay) return;
    if (overlay.classList.contains('open')) overlay.classList.remove('open');
    else overlay.classList.add('open');
  }

`;
