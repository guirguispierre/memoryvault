// Opt-in 3D mode for the full-screen graph view only (never the rail). Off by
// default. The 3d-force-graph UMD bundle (which carries three.js) is lazy-loaded
// from a CDN the first time the user enables 3D, so the default app stays
// dependency-free and build-free. Same real nodes/edges, same state colours and
// strength sizing, clickable. The WebGL renderer + loop are torn down when the
// user leaves the graph, switches back to 2D, or the tab is hidden, and the
// pixel ratio is capped; reduce-motion disables auto-rotate (manual still works).
//
// CDN (script-src already allows cdn.jsdelivr.net, used for d3):
//   https://cdn.jsdelivr.net/npm/3d-force-graph@1.79.0/dist/3d-force-graph.min.js
export const clientGraph3d = `
  var GRAPH_3D_CDN = 'https://cdn.jsdelivr.net/npm/3d-force-graph@1.79.0/dist/3d-force-graph.min.js';
  var graph3dActive = false;
  var graph3dInstance = null;
  var graph3dLibPromise = null;

  function graph3dEnabled() { return !!(viewerSettings && viewerSettings.graph_3d); }
  function graph3dReduced() {
    if (viewerSettings && viewerSettings.reduce_motion) return true;
    return !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches);
  }

  // Inject the CDN bundle once; resolves when window.ForceGraph3D is ready.
  function ensureGraph3dLib() {
    if (window.ForceGraph3D) return Promise.resolve(true);
    if (graph3dLibPromise) return graph3dLibPromise;
    graph3dLibPromise = new Promise(function (resolve, reject) {
      var s = document.createElement('script');
      s.src = GRAPH_3D_CDN;
      s.async = true;
      s.onload = function () { window.ForceGraph3D ? resolve(true) : reject(new Error('3D library missing after load')); };
      s.onerror = function () { graph3dLibPromise = null; reject(new Error('3D library failed to load')); };
      document.head.appendChild(s);
    });
    return graph3dLibPromise;
  }

  function graph3dCssVar(name, fallback) {
    var v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return v || fallback;
  }

  function graph3dStateColor(n) {
    var tier = typeof graphNodeTier === 'function' ? graphNodeTier(n) : 'resting';
    return graph3dCssVar(tier === 'active' ? '--mem-active' : (tier === 'settling' ? '--mem-settling' : '--mem-fading'), '#8AB0FF');
  }

  // Build {nodes, links} from the same real graph payload the 2D view uses,
  // honouring the relation filter and the inferred-edges toggle.
  function buildGraph3dData() {
    var nodes = (lastGraphData && lastGraphData.nodes) ? lastGraphData.nodes : [];
    var idSet = {};
    nodes.forEach(function (n) { idSet[String(n.id)] = true; });
    var nodeData = nodes.map(function (n) {
      return {
        id: String(n.id),
        name: n.title || n.key || (n.content || '').slice(0, 40) || String(n.id),
        color: graph3dStateColor(n),
        val: 1 + (typeof graphNodeStrength === 'function' ? graphNodeStrength(n) : 0.5) * 6,
      };
    });
    var links = [];
    (lastGraphData.edges || []).forEach(function (e) {
      var s = String(e.from_id), t = String(e.to_id);
      var r = String(e.relation_type || 'related').toLowerCase();
      if (idSet[s] && idSet[t] && graphRelationFilter.has(r)) links.push({ source: s, target: t });
    });
    if (graphShowInferred) {
      (lastGraphData.inferred_edges || []).forEach(function (e) {
        var s = String(e.from_id), t = String(e.to_id);
        if (idSet[s] && idSet[t]) links.push({ source: s, target: t, inferred: true });
      });
    }
    return { nodes: nodeData, links: links, linkColor: graph3dCssVar('--rule-bright', '#3A3D41') };
  }

  function graph3dContainerSize() {
    var view = document.getElementById('graph-view');
    var c = document.getElementById('graph-3d');
    var w = (c && c.clientWidth) || (view && view.clientWidth) || 800;
    var h = (c && c.clientHeight) || (view && view.clientHeight) || 600;
    return { w: w, h: h };
  }

  async function renderGraph3d() {
    var container = document.getElementById('graph-3d');
    if (!container) return;
    try {
      await ensureGraph3dLib();
    } catch (e) {
      showToast('3D graph could not load — staying in 2D.', 'error', true);
      teardownGraph3d();
      return;
    }
    // The view may have been left (or 3D turned off) while the CDN loaded.
    if (!graphVisible || !graph3dEnabled()) { teardownGraph3d(); return; }
    var data = buildGraph3dData();
    var size = graph3dContainerSize();
    if (!graph3dInstance) {
      graph3dInstance = window.ForceGraph3D({ controlType: 'orbit', rendererConfig: { antialias: true, alpha: false } })(container)
        .backgroundColor(graph3dCssVar('--ground', '#070810'))
        .nodeLabel(function (n) { return n.name; })
        .nodeColor(function (n) { return n.color; })
        .nodeVal(function (n) { return n.val; })
        .nodeOpacity(0.92)
        .linkColor(function () { return data.linkColor; })
        .linkOpacity(0.4)
        .linkWidth(function (l) { return l.inferred ? 0.4 : 1; })
        .onNodeClick(function (n) { if (typeof expandById === 'function') expandById(n.id); });
      try {
        var renderer = graph3dInstance.renderer();
        if (renderer && renderer.setPixelRatio) renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.5));
      } catch (e) {}
    }
    graph3dInstance.width(size.w).height(size.h);
    graph3dInstance.graphData(data);
    try {
      var ctrl = graph3dInstance.controls();
      if (ctrl) { ctrl.autoRotate = !graph3dReduced(); ctrl.autoRotateSpeed = 0.4; }
    } catch (e) {}
    container.style.display = 'block';
    container.setAttribute('aria-hidden', 'false');
    var svg = document.getElementById('graph-svg');
    if (svg) svg.style.display = 'none';
    graph3dActive = true;
    graph3dResume();
  }

  function teardownGraph3d() {
    graph3dActive = false;
    if (graph3dInstance) {
      try { if (graph3dInstance.pauseAnimation) graph3dInstance.pauseAnimation(); } catch (e) {}
      try { if (graph3dInstance._destructor) graph3dInstance._destructor(); } catch (e) {}
      graph3dInstance = null;
    }
    var container = document.getElementById('graph-3d');
    if (container) { container.innerHTML = ''; container.style.display = 'none'; container.setAttribute('aria-hidden', 'true'); }
    var svg = document.getElementById('graph-svg');
    if (svg) svg.style.display = '';
  }

  function graph3dPause() { if (graph3dInstance) { try { graph3dInstance.pauseAnimation(); } catch (e) {} } }
  function graph3dResume() { if (graph3dInstance && !document.hidden) { try { graph3dInstance.resumeAnimation(); } catch (e) {} } }

  // 2D vs 3D decision, run whenever the graph view opens or the preference flips.
  function applyGraphMode() {
    if (graphVisible && graph3dEnabled()) renderGraph3d();
    else teardownGraph3d();
    syncGraph3dButton();
  }

  // Cheap data refresh on the live 3D instance (relation/inferred toggles, polls).
  function graph3dUpdateData() {
    if (graph3dActive && graph3dInstance) {
      try { graph3dInstance.graphData(buildGraph3dData()); } catch (e) {}
    }
  }

  function syncGraph3dButton() {
    var btn = document.getElementById('graph-toggle-3d');
    if (!btn) return;
    var on = graph3dEnabled();
    btn.classList.toggle('active', on);
    btn.setAttribute('aria-pressed', on ? 'true' : 'false');
    btn.textContent = on ? '3D ON' : '3D OFF';
  }

  function toggleGraph3d() {
    if (!viewerSettings) return;
    viewerSettings.graph_3d = !viewerSettings.graph_3d;
    persistViewerSettings();
    scheduleServerSettingsSave();
    syncGraph3dButton();
    showToast(viewerSettings.graph_3d ? 'Loading 3D graph…' : 'Back to 2D graph.', 'info');
    applyGraphMode();
  }

  // Stop the loop when the tab is hidden; resume only if still in the 3D view.
  document.addEventListener('visibilitychange', function () {
    if (document.hidden) graph3dPause();
    else if (graphVisible && graph3dActive) graph3dResume();
  });

  window.addEventListener('resize', function () {
    if (graph3dActive && graph3dInstance) {
      var s = graph3dContainerSize();
      try { graph3dInstance.width(s.w).height(s.h); } catch (e) {}
    }
  });

  syncGraph3dButton();
`;
