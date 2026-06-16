// The right-column graph rail on the list home: a calm, smaller constellation
// of the current brain's graph plus a card for the selected memory and its
// linked memories. Performance is deliberately conservative — DPR is capped,
// the loop pauses when the tab is hidden, when the rail scrolls offscreen, or
// when the full graph view is open, and reduce-motion renders a single static
// frame. Full motion lives only in the expanded graph view. All helpers are
// rail-prefixed to stay clear of the shared client scope; updateRailSelection
// is the name selectRow() calls.
export const clientRail = `
  var railCanvas = null, railCtx = null, railDPR = 1, railW = 0, railH = 0;
  var railNodes = [], railEdges = [], railRAF = 0, railT = 0;
  var railVisible = true, railLastFetch = 0, railFetching = false;
  var RAIL_MAX_NODES = 40, RAIL_MAX_EDGES = 60;

  function railIsReduced() {
    if (viewerSettings && viewerSettings.reduce_motion) return true;
    return !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches);
  }

  function railColors() {
    var s = getComputedStyle(document.documentElement);
    var v = function (n, f) { var x = s.getPropertyValue(n).trim(); return x || f; };
    return {
      active: v('--mem-active', '#86E0B8'),
      settling: v('--mem-settling', '#FFCAA0'),
      fading: v('--mem-fading', '#565D72'),
      link: v('--mem-link', '#B9A3FF'),
      accent: v('--butter', '#8AB0FF'),
    };
  }

  function railRgba(color, alpha) {
    var c = String(color).trim();
    if (c.charAt(0) === '#') {
      var h = c.slice(1);
      if (h.length === 3) h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2];
      var r = parseInt(h.slice(0, 2), 16), g = parseInt(h.slice(2, 4), 16), b = parseInt(h.slice(4, 6), 16);
      if (!Number.isFinite(r) || !Number.isFinite(g) || !Number.isFinite(b)) return 'rgba(138,176,255,' + alpha + ')';
      return 'rgba(' + r + ',' + g + ',' + b + ',' + alpha + ')';
    }
    var m = c.match(/(\\d+)\\D+(\\d+)\\D+(\\d+)/);
    if (m) return 'rgba(' + m[1] + ',' + m[2] + ',' + m[3] + ',' + alpha + ')';
    return 'rgba(138,176,255,' + alpha + ')';
  }

  function railSize() {
    if (!railCanvas) return;
    var rect = railCanvas.getBoundingClientRect();
    railDPR = Math.min(window.devicePixelRatio || 1, 1.5);
    railW = railCanvas.width = Math.max(1, Math.round((rect.width || 280) * railDPR));
    railH = railCanvas.height = Math.max(1, Math.round((rect.height || 240) * railDPR));
  }

  // Deterministic [0,1) from an id + salt so node positions are stable.
  function railHash(str, salt) {
    var h = (2166136261 ^ salt) >>> 0;
    for (var i = 0; i < str.length; i++) { h = Math.imul(h ^ str.charCodeAt(i), 16777619) >>> 0; }
    return (h % 100000) / 100000;
  }

  function railBuildNodes() {
    var cols = railColors();
    var src = (lastGraphData && lastGraphData.nodes) ? lastGraphData.nodes : [];
    var used = src.slice(0, RAIL_MAX_NODES);
    var idIndex = {};
    railNodes = used.map(function (n, i) {
      var id = String(n.id != null ? n.id : i);
      idIndex[id] = i;
      // Same state tiers as the full graph and the list, so the rail is faithful.
      var tier = typeof graphNodeTier === 'function' ? graphNodeTier(n) : 'resting';
      var color = tier === 'active' ? cols.active : (tier === 'settling' ? cols.settling : cols.fading);
      return {
        id: id,
        a: railHash(id, 7) * 6.2832,
        rad: 0.16 + railHash(id, 13) * 0.32,
        color: color,
        sz: 1.0 + railHash(id, 29) * 1.3,
        ph: railHash(id, 41) * 6.2832,
        sp: 0.5 + railHash(id, 53) * 0.5,
        sel: false,
      };
    });
    var edgesSrc = [].concat(lastGraphData.edges || [], lastGraphData.inferred_edges || []);
    railEdges = [];
    for (var e = 0; e < edgesSrc.length && railEdges.length < RAIL_MAX_EDGES; e++) {
      var ed = edgesSrc[e];
      var f = idIndex[String(ed.source != null ? ed.source : ed.from_id)];
      var t = idIndex[String(ed.target != null ? ed.target : ed.to_id)];
      if (f != null && t != null && f !== t) railEdges.push([f, t]);
    }
    railApplySelectionFlag();
  }

  function railApplySelectionFlag() {
    for (var i = 0; i < railNodes.length; i++) {
      railNodes[i].sel = selectedMemoryId != null && railNodes[i].id === String(selectedMemoryId);
    }
  }

  function railDrawFrame() {
    if (!railCtx) return;
    var cols = railColors();
    railCtx.clearRect(0, 0, railW, railH);
    if (!railNodes.length) return;
    railCtx.globalCompositeOperation = 'lighter';
    var cx = railW / 2, cy = railH / 2, R = Math.min(railW, railH);
    var pos = railNodes.map(function (n) {
      var ang = n.a + railT * n.sp * 0.5;
      return [cx + Math.cos(ang) * n.rad * R, cy + Math.sin(ang) * n.rad * R];
    });
    railCtx.strokeStyle = railRgba(cols.accent, 0.13);
    railCtx.lineWidth = railDPR * 0.7;
    for (var e = 0; e < railEdges.length; e++) {
      var a = railEdges[e][0], b = railEdges[e][1];
      if (!pos[a] || !pos[b]) continue;
      railCtx.beginPath();
      railCtx.moveTo(pos[a][0], pos[a][1]);
      railCtx.lineTo(pos[b][0], pos[b][1]);
      railCtx.stroke();
    }
    for (var i = 0; i < railNodes.length; i++) {
      var node = railNodes[i], p = pos[i];
      var tw = node.sel ? 1 : (0.6 + 0.4 * Math.sin(railT * 30 * node.sp + node.ph));
      var base = node.sz * railDPR * (node.sel ? 6.5 : 4);
      var grad = railCtx.createRadialGradient(p[0], p[1], 0, p[0], p[1], base);
      grad.addColorStop(0, railRgba(node.color, 0.9 * tw));
      grad.addColorStop(0.5, railRgba(node.color, 0.2 * tw));
      grad.addColorStop(1, railRgba(node.color, 0));
      railCtx.fillStyle = grad;
      railCtx.beginPath();
      railCtx.arc(p[0], p[1], base, 0, 6.2832);
      railCtx.fill();
      railCtx.fillStyle = railRgba('#ffffff', 0.9 * tw);
      railCtx.beginPath();
      railCtx.arc(p[0], p[1], node.sz * railDPR * (node.sel ? 1.1 : 0.6), 0, 6.2832);
      railCtx.fill();
    }
    railCtx.globalCompositeOperation = 'source-over';
  }

  function railLoop() {
    railT += 0.0022;
    railDrawFrame();
    railRAF = requestAnimationFrame(railLoop);
  }

  function railStop() { if (railRAF) { cancelAnimationFrame(railRAF); railRAF = 0; } }

  // Run only when the rail is on screen, the tab is visible, the full graph is
  // closed, and motion is allowed. Otherwise hold a single static frame.
  function railSync() {
    var shouldRun = railVisible && !document.hidden && !graphVisible && railCanvas;
    if (shouldRun && railIsReduced()) { railStop(); railDrawFrame(); return; }
    if (shouldRun) { if (!railRAF) railRAF = requestAnimationFrame(railLoop); return; }
    railStop();
    if (railCanvas) railDrawFrame();
  }

  async function railEnsureGraph(force) {
    if (!railCanvas || !hasAuthenticatedSession()) return;
    var now = Date.now();
    if (!force && now - railLastFetch < 8000) return;
    if (railFetching) return;
    railFetching = true;
    try {
      var r = await apiFetch(BASE + '/api/graph');
      if (!r.ok) return;
      var data = await r.json();
      lastGraphData = {
        nodes: (data.nodes || []).map(function (n) { return Object.assign({}, n); }),
        edges: (data.edges || []).map(function (e) { return Object.assign({}, e); }),
        inferred_edges: (data.inferred_edges || []).map(function (e) { return Object.assign({}, e); }),
      };
      railLastFetch = now;
      railBuildNodes();
      railSync();
    } catch (e) { /* offline: keep the last constellation */ } finally {
      railFetching = false;
    }
  }

  function railDefaultCard() {
    var card = document.getElementById('rail-card');
    if (card) card.innerHTML = '<div class="rail-empty">Select a memory to see its links, or open the full graph.</div>';
  }

  // Called by selectRow(). Highlights the node and fills the card with the
  // selected memory plus its linked memories (real /api/links data).
  async function updateRailSelection(m) {
    railApplySelectionFlag();
    railSync();
    var card = document.getElementById('rail-card');
    if (!card) return;
    if (!m) { railDefaultCard(); return; }
    var keyText = m.key ? m.key : (m.type || 'memory');
    var titleText = (m.title && String(m.title).trim()) ? m.title : (m.content ? String(m.content).slice(0, 80) : (m.key || 'untitled'));
    card.innerHTML =
      '<div class="k2">' + esc(keyText) + '</div>' +
      '<div class="t2">' + esc(titleText) + '</div>' +
      '<div class="links" id="rail-links"><span class="rail-dim">Loading links…</span></div>' +
      '<button type="button" class="rail-detail" data-action="expand-card" data-card-index="' + selectedMemoryIndex + '">Open detail &#8599;</button>';
    var linksEl = document.getElementById('rail-links');
    try {
      var r = await apiFetch(BASE + '/api/links/' + m.id);
      if (!r.ok) { if (linksEl) linksEl.innerHTML = '<span class="rail-dim">No links.</span>'; return; }
      var links = await r.json();
      if (String(selectedMemoryId) !== String(m.id)) return; // selection moved on
      if (!links || !links.length) { if (linksEl) linksEl.innerHTML = '<span class="rail-dim">No linked memories yet.</span>'; return; }
      var names = links.slice(0, 8).map(function (l) {
        var cm = l.memory || {};
        var nm = cm.key || cm.title || (String(cm.content || '').slice(0, 26)) || 'memory';
        return '<a data-rail-link-id="' + esc(String(cm.id)) + '">' + esc(nm) + '</a>';
      });
      if (linksEl) linksEl.innerHTML = 'Linked to:<br>' + names.join(' · ');
    } catch (e) {
      if (linksEl) linksEl.innerHTML = '<span class="rail-dim">Links unavailable.</span>';
    }
  }

  function railInit() {
    railCanvas = document.getElementById('rail-canvas');
    if (!railCanvas) return;
    railCtx = railCanvas.getContext('2d');
    railSize();
    railDefaultCard();
    document.addEventListener('visibilitychange', railSync);
    if (window.IntersectionObserver) {
      var io = new IntersectionObserver(function (entries) {
        if (entries && entries[0]) railVisible = entries[0].isIntersecting;
        railSync();
      }, { threshold: 0.05 });
      io.observe(railCanvas);
    } else {
      railVisible = true;
    }
    var resizeTimer = null;
    window.addEventListener('resize', function () {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(function () { railSize(); railDrawFrame(); }, 120);
    });
    var card = document.getElementById('rail-card');
    if (card) {
      card.addEventListener('click', function (e) {
        var a = e.target.closest && e.target.closest('[data-rail-link-id]');
        if (a) expandById(a.getAttribute('data-rail-link-id'));
      });
    }
    railEnsureGraph(true);
  }
`;
