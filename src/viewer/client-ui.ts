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
      setLoginError('Paste a bearer token first.');
      return;
    }
    try {
      const r = await fetch(BASE + '/api/memories?limit=1', {
        headers: { 'Authorization': 'Bearer ' + val },
      });
      if (!r.ok) {
        setLoginError('That token was not accepted — check it and try again.');
        return;
      }
      TOKEN = val;
      SESSION_MODE = 'legacy';
      enterApp();
      showToast('Token accepted.', 'success');
    } catch {
      setLoginError('Network error — check your connection and try again.');
      showToast('Network error while validating token.', 'error');
    }
  }

  async function doCredentialAuth(mode) {
    clearLoginError();
    const email = document.getElementById('email-input').value.trim();
    const password = document.getElementById('password-input').value;
    const brainName = document.getElementById('brain-name-input').value.trim();
    if (!email || !password) {
      setLoginError('Enter your email and password.');
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
        setLoginError(data.error || 'Sign-in failed — check your details and try again.');
        return;
      }
      TOKEN = '';
      SESSION_MODE = 'user';
      enterApp();
      showToast(mode === 'signup' ? 'Account created and signed in.' : 'Signed in successfully.', 'success');
    } catch {
      setLoginError('Network error — check your connection and try again.');
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
    if (pourTickId) {
      clearInterval(pourTickId);
      pourTickId = null;
    }
    TOKEN = '';
    SESSION_MODE = 'none';
    location.reload();
  }

  function updateTime() {
    const el = document.getElementById('hdr-time');
    if (el) {
      if (viewerSettings && viewerSettings.time_mode === 'local') {
        el.textContent = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
      } else {
        el.textContent = new Date().toISOString().slice(11, 16) + ' UTC';
      }
    }
  }

  function startClock() {
    if (clockIntervalId) clearInterval(clockIntervalId);
    updateTime();
    clockIntervalId = setInterval(updateTime, 1000);
  }

  // The recall ribbon is a 24h time window, so it drifts even when no data
  // changes. Re-render it once a minute so the seam breathes as memories age
  // out; the data recompute is unconditional, the CSS animation is what the
  // reduce-motion setting suppresses.
  function startPourTick() {
    if (pourTickId) clearInterval(pourTickId);
    pourTickId = setInterval(() => {
      if (hasAuthenticatedSession() && corpusMemories.length) renderPour(corpusMemories);
    }, 60000);
  }

  function pulseStatPill(id, changed) {
    if (!changed) return;
    const el = document.getElementById(id);
    if (!el) return;
    el.classList.remove('pulse');
    void el.offsetWidth;
    el.classList.add('pulse');
  }

  // Unfiltered fetch used to keep the ribbon/header whole-brain while a filter
  // or search narrows the grid. Returns null on failure so the caller keeps the
  // last known corpus.
  async function fetchCorpusMemories() {
    try {
      const r = await apiFetch(BASE + '/api/memories?limit=500');
      if (!r.ok) return null;
      const data = await r.json();
      return data.memories || [];
    } catch {
      return null;
    }
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
          grid.innerHTML = '<div class="empty-state"><div class="empty-icon">hm.</div>Memories could not load (' + r.status + ') — try refresh.</div>';
          showToast('Memory load failed (' + r.status + ').', 'error');
        }
        return;
      }
      const data = await r.json();
      displayedMemories = data.memories || [];
      // The grid honours the type filter and search; the ribbon and header are
      // whole-brain readouts, so when either is active refresh the corpus from
      // an unfiltered request rather than reusing the filtered rows.
      if (activeFilter || search) {
        const corpus = await fetchCorpusMemories();
        if (corpus) corpusMemories = corpus;
      } else {
        corpusMemories = displayedMemories;
      }
      updateStats(data.stats || []);
      renderGrid(displayedMemories);
      // First-run onboarding keys off the whole-brain count, not the filtered view.
      renderOnboarding(corpusMemories.length);
      if (silent) window.scrollTo(0, scrollY);
    } catch(e) {
      grid.innerHTML = '<div class="empty-state"><div class="empty-icon">hm.</div>Connection error — check your network and try refresh.</div>';
      showToast('Connection error while loading memories.', 'error');
    } finally {
      if (refreshBtn) refreshBtn.classList.remove('syncing');
    }
  }

  // Counts come from the server stats (full-corpus, GROUP BY type), and the
  // ribbon/links read corpusMemories, so all of these stay whole-brain even
  // when the grid is filtered.
  function updateStats(stats) {
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
    const linkEnds = corpusMemories.reduce((sum, m) => sum + (Number.isFinite(Number(m.link_count)) ? Number(m.link_count) : 0), 0);
    const linkTotal = Math.round(linkEnds / 2);
    // State tiers are derived per-memory from the loaded corpus (capped at 500);
    // total is the full-corpus server count, so on very large brains the tier
    // counts cover the loaded window while the total stays whole-brain.
    const tsNow = Date.now() / 1000;
    let activeN = 0, settlingN = 0, fadingN = 0;
    for (const m of corpusMemories) {
      const tier = strengthTier(memoryStrength(m, tsNow));
      if (tier === 'active') activeN += 1;
      else if (tier === 'settling') settlingN += 1;
      else fadingN += 1;
    }
    const setText = (id, value) => { const el = document.getElementById(id); if (el) el.textContent = value; };
    setText('stat-total', total);
    setText('stat-active', activeN);
    setText('stat-settling', settlingN);
    setText('stat-fading', fadingN);
    setText('stat-links', linkTotal);
    renderPour(corpusMemories);
  }

  function clampUnit(value, fallback) {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.min(Math.max(n, 0), 1);
  }

  /* Strength = 0.45·importance + 0.20·confidence + 0.35·recency, where
     recency = exp(-daysSinceUpdate / 14). The server exposes no single
     strength value, so it is derived here from the dynamic scores it does
     return; the two-week decay makes recently reinforced memories read as
     bright cream and untouched ones recede. Tiers cut at 0.62 and 0.38. */
  function memoryStrength(m, tsNow) {
    const importance = clampUnit(m.dynamic_importance ?? m.importance, 0.5);
    const confidence = clampUnit(m.dynamic_confidence ?? m.confidence, 0.7);
    const updatedAt = Number(m.updated_at ?? m.created_at ?? 0);
    const days = Math.max(0, (tsNow - updatedAt) / 86400);
    const recency = Math.exp(-days / 14);
    return Math.min(Math.max(0.45 * importance + 0.2 * confidence + 0.35 * recency, 0), 1);
  }

  function strengthTier(strength) {
    if (strength >= 0.62) return 'active';
    if (strength >= 0.38) return 'settling';
    return 'resting';
  }

  function formatAccessionTime(ts) {
    if (!Number.isFinite(ts) || ts <= 0) return '';
    const then = new Date(ts * 1000);
    const now = new Date();
    const sameDay = then.toISOString().slice(0, 10) === now.toISOString().slice(0, 10);
    if (sameDay) return then.toISOString().slice(11, 16);
    const ageDays = (now.getTime() / 1000 - ts) / 86400;
    if (ageDays < 7) return then.toLocaleDateString([], { weekday: 'short' });
    return then.toLocaleDateString([], { month: 'short', day: 'numeric' });
  }

  function renderPour(memories) {
    const wrap = document.getElementById('pour-ticks');
    if (!wrap) return;
    const tsNow = Date.now() / 1000;
    const BUCKETS = 48;
    const windowSec = 24 * 3600;
    const counts = new Array(BUCKETS).fill(0);
    const peaks = new Array(BUCKETS).fill(0);
    let any = false;
    for (const m of memories) {
      const ts = Number(m.updated_at ?? m.created_at ?? 0);
      const age = tsNow - ts;
      if (age < 0 || age >= windowSec) continue;
      const bucket = BUCKETS - 1 - Math.floor(age / (windowSec / BUCKETS));
      counts[bucket] += 1;
      peaks[bucket] = Math.max(peaks[bucket], memoryStrength(m, tsNow));
      any = true;
    }
    if (!any && memories.length) {
      // Quiet day: echo the most recently touched memories instead,
      // newest at the right edge, so the seam still breathes.
      const recent = [...memories]
        .sort((a, b) => Number(b.updated_at ?? b.created_at ?? 0) - Number(a.updated_at ?? a.created_at ?? 0))
        .slice(0, BUCKETS / 2);
      recent.forEach((m, rank) => {
        const bucket = BUCKETS - 1 - rank * 2;
        if (bucket < 0) return;
        counts[bucket] = 1;
        peaks[bucket] = memoryStrength(m, tsNow);
      });
    }
    let html = '';
    for (let i = 0; i < BUCKETS; i++) {
      const t = i / (BUCKETS - 1);
      const activeTick = counts[i] > 0;
      const height = activeTick
        ? Math.min(38, Math.round((12 + counts[i] * 7 + peaks[i] * 8) * (0.55 + 0.45 * t)))
        : 2 + ((i * 5) % 3);
      const opacity = activeTick ? (0.18 + t * 0.78).toFixed(2) : '0.10';
      html += '<i style="height:' + height + 'px;opacity:' + opacity + ';animation-delay:' + Math.round(t * 240) + 'ms"></i>';
    }
    wrap.innerHTML = html;
  }

  const TIER_ORDER = [
    { key: 'active', label: 'Active', note: 'reinforced this week', bead: 'full' },
    { key: 'settling', label: 'Settling', note: 'quiet for a few days', bead: 'half' },
    { key: 'resting', label: 'Resting', note: 'fading — review soon', bead: 'ring' },
  ];

  function renderMemoryRow(m, index, strength, bead, dimmed, order) {
    const accId = 'MV·' + String(m.id || '').replace(/[^a-zA-Z0-9]/g, '').slice(-4).toUpperCase();
    const accTime = formatAccessionTime(Number(m.updated_at ?? m.created_at ?? 0));
    const kind = '<span class="kind">' + esc(m.type) + '</span>';
    const confidence = clampUnit(m.dynamic_confidence ?? m.confidence, 0.7);
    const verified = m.type === 'fact' && confidence >= 0.85 ? '<span class="ver">verified</span>' : '';
    const isLedgerFact = m.type === 'fact' && m.key;
    // For facts the key IS the title; the body shows only the value so the
    // key is never repeated on both lines.
    const titleText = isLedgerFact ? m.key : (m.title || m.key || 'untitled');
    const body = isLedgerFact
      ? '<div class="txt ledger"><span class="a">&rarr;</span><span class="v">' + esc(m.content) + '</span></div>'
      : '<div class="txt">' + esc(String(m.content || '').slice(0, 280)) + '</div>';
    const linked = Number(m.link_count) > 0
      ? '<span class="links">&#8627; ' + Number(m.link_count) + ' linked</span>'
      : '';
    const pct = Math.round(strength * 100);
    return '<div class="row' + (dimmed ? ' dim' : '') + '" data-type="' + esc(m.type) + '" data-action="expand-card" data-card-index="' + index + '" style="animation-delay:' + Math.min(order * 0.03, 0.36) + 's">' +
      '<div class="bead ' + bead + '"></div>' +
      '<div>' +
        '<div class="ttl">' + esc(titleText) + ' ' + kind + verified + '</div>' +
        body +
      '</div>' +
      '<div class="meta">' +
        '<span class="acc">' + esc(accId) + (accTime ? ' · ' + esc(accTime) : '') + '</span>' +
        '<span class="strength"><span class="lab">strength</span><span class="bar"><i style="width:' + pct + '%"></i></span></span>' +
        linked +
      '</div>' +
    '</div>';
  }

  function renderGrid(memories) {
    const grid = document.getElementById('grid');
    if (!memories.length) {
      grid.innerHTML = '<div class="empty-state"><div class="empty-icon">quiet.</div>Nothing here yet — save your first memory.</div>';
      return;
    }
    const tsNow = Date.now() / 1000;
    const scored = memories.map((m, index) => ({ m, index, strength: memoryStrength(m, tsNow) }));
    let html = '';
    let order = 0;
    let shown = 0;
    for (const tier of TIER_ORDER) {
      if (!memoryStateFilter.has(tier.key)) continue;
      const group = scored
        .filter((item) => strengthTier(item.strength) === tier.key)
        .sort((a, b) => b.strength - a.strength);
      if (!group.length) continue;
      html += '<div class="group"><span class="t">' + tier.label + '</span><span class="ln"></span><span class="n">' + tier.note + '</span></div>';
      for (const item of group) {
        html += renderMemoryRow(item.m, item.index, item.strength, tier.bead, tier.key === 'resting', order);
        order += 1;
        shown += 1;
      }
    }
    if (!shown) {
      grid.innerHTML = '<div class="empty-state"><div class="empty-icon">filtered.</div>No memories match the selected states.</div>';
      return;
    }
    grid.innerHTML = html;
  }

  function syncStateChips() {
    [['active', 'state-active'], ['settling', 'state-settling'], ['resting', 'state-resting']].forEach(([key, id]) => {
      const el = document.getElementById(id);
      if (el) el.classList.toggle('active', memoryStateFilter.has(key));
    });
  }

  function toggleStateFilter(state) {
    if (!['active', 'settling', 'resting'].includes(state)) return;
    if (memoryStateFilter.has(state)) {
      // Keep at least one tier on so the list never goes fully blank by toggle.
      if (memoryStateFilter.size === 1) return;
      memoryStateFilter.delete(state);
    } else {
      memoryStateFilter.add(state);
    }
    syncStateChips();
    renderGrid(displayedMemories);
  }

  function syncDensityToggle() {
    const btn = document.getElementById('density-toggle');
    if (!btn) return;
    const compact = !!(viewerSettings && viewerSettings.compact_cards);
    btn.classList.toggle('active', compact);
    btn.setAttribute('aria-pressed', compact ? 'true' : 'false');
    btn.innerHTML = compact ? '\\u229F comfortable' : '\\u229E compact';
  }

  function toggleDensity() {
    if (!viewerSettings) return;
    viewerSettings.compact_cards = !viewerSettings.compact_cards;
    persistViewerSettings();
    scheduleServerSettingsSave();
    applyViewerSettingsToRuntime({ restartPolling: false, rerenderGrid: false });
    const compactCheck = document.getElementById('settings-compact-cards');
    if (compactCheck) compactCheck.checked = viewerSettings.compact_cards;
    syncDensityToggle();
  }

  function expandCard(idx) {
    const m = displayedMemories[idx];
    if (!m) return;
    const date = new Date(m.created_at * 1000).toLocaleString();
    const updated = m.updated_at !== m.created_at ? '  ·  Updated ' + new Date(m.updated_at * 1000).toLocaleString() : '';
    const qualityChips = [
      m.source ? \`<span class="tag">src:\${esc(m.source)}</span>\` : '',
      Number.isFinite(Number(m.dynamic_confidence ?? m.confidence)) ? \`<span class="tag">conf:\${Math.round(Number(m.dynamic_confidence ?? m.confidence) * 100)}%</span>\` : '',
      Number.isFinite(Number(m.dynamic_importance ?? m.importance)) ? \`<span class="tag">imp:\${Math.round(Number(m.dynamic_importance ?? m.importance) * 100)}%</span>\` : '',
    ].filter(Boolean).join('');
    const headTitle = m.title || m.key || 'untitled';
    const showKeyLine = m.key && m.title && m.key !== m.title;
    document.getElementById('expand-header').innerHTML =
      \`<div style="display:flex;align-items:baseline;gap:10px;margin-bottom:0.5rem;flex-wrap:wrap">
        <span class="expand-title">\${esc(headTitle)}</span>
        <span class="kind">\${esc(m.type)}</span>
      </div>
      \${showKeyLine ? \`<div class="expand-key" style="margin-bottom:0.4rem">\${esc(m.key)}</div>\` : ''}
      \${m.tags ? \`<div style="display:flex;flex-wrap:wrap;gap:0.3rem;margin-bottom:0.25rem">\${m.tags.split(',').map(t => \`<span class="tag">\${esc(t.trim())}</span>\`).join('')}</div>\` : ''}
      \${qualityChips ? \`<div style="display:flex;flex-wrap:wrap;gap:0.3rem;margin-bottom:0.25rem">\${qualityChips}</div>\` : ''}\`;
    document.getElementById('expand-content').textContent = m.content;
    document.getElementById('expand-meta').textContent = 'ID: ' + m.id + '  ·  Created ' + date + updated;
    document.getElementById('expand-overlay').classList.add('open');
    document.body.style.overflow = 'hidden';

    // Lazy-load connections
    const connEl = document.getElementById('expand-connections');
    connEl.innerHTML = '<div style="font-size:12px;color:var(--cream-faint);margin-top:1rem">Loading connections…</div>';
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
          <div class="connections-title">Connections (\${links.length})</div>
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

  // New-memory composer. Writes a real memory through the same authenticated
  // MCP path agents use (memory_save); no new server surface, no faked data.
  let newMemorySaving = false;

  function setNewMemoryError(message) {
    const el = document.getElementById('newmem-err');
    if (!el) return;
    el.textContent = message || '';
    el.style.display = message ? 'block' : 'none';
  }

  function openNewMemory() {
    if (!ensureAppReady('New memory')) return;
    const overlay = document.getElementById('newmem-overlay');
    if (!overlay) return;
    setNewMemoryError('');
    overlay.classList.add('open');
    setTimeout(() => {
      const content = document.getElementById('newmem-content');
      if (content) content.focus();
    }, 0);
  }

  function closeNewMemory() {
    const overlay = document.getElementById('newmem-overlay');
    if (overlay) overlay.classList.remove('open');
  }

  function closeNewMemoryOverlay(event) {
    const overlay = document.getElementById('newmem-overlay');
    if (!overlay) return;
    if (event && event.target !== overlay) return;
    overlay.classList.remove('open');
  }

  async function submitNewMemory() {
    if (!ensureAppReady('New memory')) return;
    if (newMemorySaving) return;
    const typeEl = document.getElementById('newmem-type');
    const titleEl = document.getElementById('newmem-title');
    const keyEl = document.getElementById('newmem-key');
    const contentEl = document.getElementById('newmem-content');
    const btn = document.getElementById('newmem-save-btn');
    const type = (typeEl && typeEl.value) || 'note';
    const title = (titleEl && titleEl.value.trim()) || '';
    const key = (keyEl && keyEl.value.trim()) || '';
    const content = (contentEl && contentEl.value.trim()) || '';
    if (!content) {
      setNewMemoryError('Content is required.');
      if (contentEl) contentEl.focus();
      return;
    }
    setNewMemoryError('');
    newMemorySaving = true;
    if (btn) { btn.disabled = true; btn.textContent = 'Saving…'; }
    const args = { type: type, content: content, source: 'viewer' };
    if (title) args.title = title;
    if (key) args.key = key;
    try {
      await callMcpTool('memory_save', args, 'viewer-new-memory');
      closeNewMemory();
      if (typeEl) typeEl.value = 'note';
      if (titleEl) titleEl.value = '';
      if (keyEl) keyEl.value = '';
      if (contentEl) contentEl.value = '';
      showToast('Memory saved.', 'success', true);
      await loadMemories(true);
    } catch (err) {
      setNewMemoryError((err && err.message) || 'Could not save the memory.');
    } finally {
      newMemorySaving = false;
      if (btn) { btn.disabled = false; btn.textContent = 'Save memory'; }
    }
  }

`;
