export const clientGraph = `  function renderGraph(nodes, edges, inferredEdges = []) {
    const svgEl = document.getElementById('graph-svg');
    const emptyEl = document.getElementById('graph-empty');
    svgEl.innerHTML = '';
    if (emptyEl) emptyEl.style.display = 'none';

    if (!nodes.length) {
      const legendEl = document.getElementById('graph-legend');
      if (legendEl) legendEl.innerHTML = '';
      if (emptyEl) { emptyEl.style.display = 'flex'; }
      return;
    }

    const width = svgEl.clientWidth || 800;
    const height = svgEl.clientHeight || 600;
    const isMobile = window.matchMedia('(max-width: 640px)').matches;
    const _cs = getComputedStyle(document.documentElement);
    const typeColor = { note: _cs.getPropertyValue('--teal').trim(), fact: _cs.getPropertyValue('--amber').trim(), journal: _cs.getPropertyValue('--journal').trim() };
    const relationDistance = {
      related: isMobile ? 88 : 112,
      supports: isMobile ? 94 : 118,
      contradicts: isMobile ? 106 : 132,
      supersedes: isMobile ? 96 : 120,
      causes: isMobile ? 100 : 126,
      example_of: isMobile ? 90 : 114,
    };

    const nodeMap = new Map(nodes.map(n => [n.id, n]));
    const explicitLinks = edges
      .map((e) => {
        const relation = String(e.relation_type || 'related').toLowerCase();
        return { ...e, source: e.from_id, target: e.to_id, kind: 'explicit', relation_type: relation };
      })
      .filter((e) => nodeMap.has(e.source) && nodeMap.has(e.target))
      .filter((e) => graphRelationFilter.has(e.relation_type));
    const inferredCandidates = graphShowInferred
      ? inferredEdges
        .map((e) => ({
          ...e,
          source: e.from_id,
          target: e.to_id,
          kind: 'inferred',
          score: Number.isFinite(Number(e.score)) ? Number(e.score) : 0,
          strength: Number.isFinite(Number(e.strength)) ? Number(e.strength) : 1,
        }))
        .filter((e) => nodeMap.has(e.source) && nodeMap.has(e.target))
      : [];

    inferredCandidates.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return b.strength - a.strength;
    });
    const inferredPerNodeLimit = isMobile ? 3 : 5;
    const inferredMaxVisible = isMobile ? 120 : 220;
    const inferredNodeDegree = new Map();
    const inferredLinks = [];
    for (const edge of inferredCandidates) {
      if (inferredLinks.length >= inferredMaxVisible) break;
      if (edge.strength < 2 && edge.score < 0.85) continue;
      const fromDeg = inferredNodeDegree.get(edge.source) || 0;
      const toDeg = inferredNodeDegree.get(edge.target) || 0;
      if (fromDeg >= inferredPerNodeLimit || toDeg >= inferredPerNodeLimit) continue;
      inferredLinks.push(edge);
      inferredNodeDegree.set(edge.source, fromDeg + 1);
      inferredNodeDegree.set(edge.target, toDeg + 1);
    }
    const links = [...explicitLinks, ...inferredLinks];

    const normalizedSearch = graphSearchQuery.trim().toLowerCase();
    const matchingNodeIds = new Set();
    if (normalizedSearch) {
      nodes.forEach((n) => {
        const haystack = [
          n.title || '',
          n.key || '',
          n.content || '',
          n.tags || '',
          n.source || '',
        ].join(' ').toLowerCase();
        if (haystack.includes(normalizedSearch)) matchingNodeIds.add(n.id);
      });
    }
    const hasSearch = normalizedSearch.length > 0;
    const isNodeVisible = (id) => !hasSearch || matchingNodeIds.has(id);

    const degreeById = new Map();
    links.forEach((l) => {
      degreeById.set(l.source, (degreeById.get(l.source) || 0) + 1);
      degreeById.set(l.target, (degreeById.get(l.target) || 0) + 1);
    });
    const neighborhoodByNode = new Map();
    links.forEach((l) => {
      const fromId = String(l.source);
      const toId = String(l.target);
      const fromSet = neighborhoodByNode.get(fromId) || new Set();
      fromSet.add(toId);
      neighborhoodByNode.set(fromId, fromSet);
      const toSet = neighborhoodByNode.get(toId) || new Set();
      toSet.add(fromId);
      neighborhoodByNode.set(toId, toSet);
    });
    const baseNodeOpacity = (d) => {
      const confidence = Math.min(Math.max(Number.isFinite(Number(d.dynamic_confidence ?? d.confidence)) ? Number(d.dynamic_confidence ?? d.confidence) : 0.7, 0), 1);
      const visible = isNodeVisible(d.id);
      const baseOpacity = 0.42 + confidence * 0.5;
      return visible ? baseOpacity : Math.max(0.08, baseOpacity * 0.25);
    };
    const baseNodeStrokeOpacity = (d) => isNodeVisible(d.id) ? 1 : 0.2;
    const baseNodeTextOpacity = (d) => isNodeVisible(d.id) ? 1 : 0.2;

    const inferredHeavy = inferredLinks.length > explicitLinks.length;
    const simulation = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links).id(d => d.id).distance((d) => {
        if (d.kind === 'inferred') {
          const score = Math.min(Math.max(Number(d.score) || 0, 0), 1);
          const minDist = isMobile ? 92 : 116;
          const maxDist = isMobile ? 130 : 168;
          return maxDist - score * (maxDist - minDist);
        }
        return relationDistance[d.relation_type] ?? (isMobile ? 96 : 120);
      }).strength((d) => {
        if (d.kind === 'inferred') {
          const score = Math.min(Math.max(Number(d.score) || 0, 0), 1);
          return 0.018 + (score * 0.03);
        }
        if (d.relation_type === 'supports') return 0.5;
        if (d.relation_type === 'contradicts') return 0.35;
        if (d.relation_type === 'supersedes') return 0.55;
        if (d.relation_type === 'causes') return 0.45;
        if (d.relation_type === 'example_of') return 0.42;
        return 0.4;
      }))
      .force('charge', d3.forceManyBody().strength(isMobile ? (inferredHeavy ? -300 : -220) : (inferredHeavy ? -420 : -300)))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('x', d3.forceX((d) => {
        if (isMobile) return width / 2;
        const lane = d.type === 'note' ? 1 : (d.type === 'fact' ? 2 : 3);
        return (width / 4) * lane;
      }).strength(isMobile ? 0.01 : 0.035))
      .force('y', d3.forceY(height / 2).strength(isMobile ? 0.01 : 0.03))
      .force('collision', d3.forceCollide(isMobile ? (inferredHeavy ? 27 : 24) : (inferredHeavy ? 34 : 30)));
    graphSimulation = simulation;
    if (!graphPhysicsEnabled) simulation.stop();

    const svg = d3.select('#graph-svg');
    graphSvgSelection = svg;
    const defs = svg.append('defs');
    Object.entries(getGraphRelationColors()).forEach(([relation, color]) => {
      const markerId = 'arrow-' + relation.replace(/_/g, '-');
      defs.append('marker')
        .attr('id', markerId)
        .attr('viewBox', '0 0 10 10')
        .attr('refX', 13)
        .attr('refY', 5)
        .attr('markerWidth', 7)
        .attr('markerHeight', 7)
        .attr('orient', 'auto-start-reverse')
        .append('path')
        .attr('d', 'M 0 0 L 10 5 L 0 10 z')
        .attr('fill', color);
    });

    const relationCounts = {};
    explicitLinks.forEach((edge) => {
      const key = String(edge.relation_type || 'related');
      relationCounts[key] = (relationCounts[key] || 0) + 1;
    });
    const confidenceVals = nodes.map((n) => Number(n.dynamic_confidence ?? n.confidence)).filter((n) => Number.isFinite(n));
    const importanceVals = nodes.map((n) => Number(n.dynamic_importance ?? n.importance)).filter((n) => Number.isFinite(n));
    const avgConfidence = confidenceVals.length ? confidenceVals.reduce((a, b) => a + b, 0) / confidenceVals.length : null;
    const avgImportance = importanceVals.length ? importanceVals.reduce((a, b) => a + b, 0) / importanceVals.length : null;
    updateGraphLegend(
      nodes.length,
      explicitLinks.length,
      inferredLinks.length,
      inferredEdges.length,
      relationCounts,
      avgConfidence,
      avgImportance,
      hasSearch ? matchingNodeIds.size : null
    );
    const g = svg.append('g');

    const zoom = d3.zoom().scaleExtent([0.2, 4]).on('zoom', (event) => {
      g.attr('transform', event.transform);
    });
    graphZoomBehavior = zoom;
    svg.call(zoom);

    const getEndpointId = (endpoint) => (typeof endpoint === 'string' ? endpoint : (endpoint && endpoint.id ? endpoint.id : ''));
    const linkOpacity = (d) => {
      if (!hasSearch) return d.kind === 'inferred' ? 0.4 : 0.9;
      const sId = getEndpointId(d.source);
      const tId = getEndpointId(d.target);
      const match = matchingNodeIds.has(sId) || matchingNodeIds.has(tId);
      return match ? (d.kind === 'inferred' ? 0.55 : 1) : 0.06;
    };

    const link = g.append('g').selectAll('line')
      .data(links).join('line').attr('class', d => {
        if (d.kind !== 'explicit') return 'graph-link inferred';
        const relationClass = String(d.relation_type || 'related').replace(/_/g, '-').replace(/[^a-z-]/g, '').toLowerCase();
        return \`graph-link explicit relation-\${relationClass}\`;
      })
      .attr('marker-end', (d) => {
        if (d.kind !== 'explicit') return null;
        const relationClass = String(d.relation_type || 'related').replace(/_/g, '-').replace(/[^a-z-]/g, '').toLowerCase();
        return \`url(#arrow-\${relationClass})\`;
      })
      .attr('stroke-width', (d) => {
        if (d.kind !== 'inferred') return 1.5;
        const score = Math.min(Math.max(Number(d.score) || 0, 0), 1);
        return 0.8 + score * 0.7;
      })
      .attr('stroke-opacity', linkOpacity);

    const linkLabel = g.append('g').selectAll('text')
      .data(links).join('text').attr('class', 'graph-link-label')
      .style('display', graphShowLabels ? null : 'none')
      .style('opacity', (d) => linkOpacity(d) >= 0.5 ? 1 : 0)
      .text(d => {
        if (d.kind !== 'explicit') return '';
        if (d.label) return d.label;
        if (d.relation_type && d.relation_type !== 'related') return String(d.relation_type).replace('_', ' ');
        return '';
      });

    const node = g.append('g').selectAll('g')
      .data(nodes).join('g').attr('class', 'graph-node')
      .call(d3.drag()
        .on('start', (event, d) => { if (!event.active) simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
        .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
        .on('end', (event, d) => { if (!event.active) simulation.alphaTarget(0); d.fx = null; d.fy = null; })
      )
      .on('click', (event, d) => { expandById(d.id); });

    node.append('circle')
      .attr('r', d => {
        const degree = degreeById.get(d.id) || 0;
        const base = isMobile ? 8 : 6;
        const maxR = isMobile ? 17 : 15;
        const importance = Math.min(Math.max(Number.isFinite(Number(d.dynamic_importance ?? d.importance)) ? Number(d.dynamic_importance ?? d.importance) : 0.5, 0), 1);
        return Math.min(maxR, base + degree * 0.4 + importance * (isMobile ? 4.2 : 3.6));
      })
      .attr('fill', d => typeColor[d.type] || '#888')
      .attr('fill-opacity', baseNodeOpacity)
      .attr('stroke', d => typeColor[d.type] || '#888')
      .attr('stroke-opacity', baseNodeStrokeOpacity)
      .attr('stroke-width', (d) => {
        const importance = Math.min(Math.max(Number.isFinite(Number(d.dynamic_importance ?? d.importance)) ? Number(d.dynamic_importance ?? d.importance) : 0.5, 0), 1);
        return 1.4 + importance * 1.6;
      });

    node.append('text')
      .attr('dx', 12).attr('dy', 4)
      .style('opacity', baseNodeTextOpacity)
      .text(d => (d.title || d.key || d.content || '').slice(0, isMobile ? 18 : 24));

    const applyGraphFocus = (focusId) => {
      if (viewerSettings && viewerSettings.graph_focus_highlight === false) {
        focusId = '';
      }
      if (!focusId) {
        link.attr('stroke-opacity', linkOpacity);
        linkLabel.style('opacity', (d) => linkOpacity(d) >= 0.5 ? 1 : 0);
        node.select('circle')
          .attr('fill-opacity', (d) => baseNodeOpacity(d))
          .attr('stroke-opacity', (d) => baseNodeStrokeOpacity(d));
        node.select('text').style('opacity', (d) => baseNodeTextOpacity(d));
        return;
      }

      const neighborSet = neighborhoodByNode.get(focusId) ?? new Set();
      const focusSet = new Set([focusId]);
      neighborSet.forEach((neighborId) => focusSet.add(neighborId));
      const isFocusedNode = (id) => focusSet.has(String(id));
      const isFocusedEdge = (d) => {
        const sId = getEndpointId(d.source);
        const tId = getEndpointId(d.target);
        return isFocusedNode(sId) && isFocusedNode(tId);
      };

      link.attr('stroke-opacity', (d) => {
        if (!isFocusedEdge(d)) return 0.04;
        const base = linkOpacity(d);
        if (d.kind === 'inferred') return Math.max(base, 0.58);
        return Math.max(base, 1);
      });

      linkLabel.style('opacity', (d) => {
        if (!graphShowLabels) return 0;
        return isFocusedEdge(d) ? 1 : 0;
      });

      node.select('circle')
        .attr('fill-opacity', (d) => {
          const id = String(d.id);
          if (id === focusId) return 1;
          if (focusSet.has(id)) return Math.max(baseNodeOpacity(d), 0.78);
          return Math.min(baseNodeOpacity(d), 0.1);
        })
        .attr('stroke-opacity', (d) => {
          const id = String(d.id);
          if (id === focusId) return 1;
          if (focusSet.has(id)) return 0.95;
          return 0.12;
        });

      node.select('text').style('opacity', (d) => {
        const id = String(d.id);
        if (id === focusId) return 1;
        if (focusSet.has(id)) return 0.95;
        return 0.1;
      });
    };

    node
      .on('mouseenter', (event, d) => { applyGraphFocus(String(d.id)); })
      .on('mouseleave', () => { applyGraphFocus(''); });

    node.append('title').text((d) => {
      const label = d.title || d.key || (d.content || '').slice(0, 70) || d.id;
      const confidence = Math.round(Math.min(Math.max(Number(d.dynamic_confidence ?? d.confidence) || 0.7, 0), 1) * 100);
      const importance = Math.round(Math.min(Math.max(Number(d.dynamic_importance ?? d.importance) || 0.5, 0), 1) * 100);
      const source = d.source ? \`\\nsource: \${d.source}\` : '';
      return \`\${label}\\nconfidence: \${confidence}%\\nimportance: \${importance}%\${source}\`;
    });

    simulation.on('tick', () => {
      link
        .attr('x1', d => d.source.x).attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
      linkLabel
        .attr('x', d => (d.source.x + d.target.x) / 2)
        .attr('y', d => (d.source.y + d.target.y) / 2);
      node.attr('transform', d => \`translate(\${d.x},\${d.y})\`);
    });
  }

  window.addEventListener('resize', () => {
    clearTimeout(graphResizeTimer);
    graphResizeTimer = setTimeout(() => {
      if (!graphVisible) return;
      rerenderGraphFromCache();
    }, 120);
  });

  function bindViewerEventHandlers() {
    const bindInput = (id, handler) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.addEventListener('input', (event) => {
        const target = event.target;
        handler(target && typeof target.value === 'string' ? target.value : '');
      });
    };

    document.addEventListener('click', (event) => {
      const target = event.target instanceof Element ? event.target.closest('[data-action]') : null;
      if (!target) return;
      const action = target.getAttribute('data-action') || '';

      switch (action) {
        case 'login':
          doCredentialAuth('login');
          break;
        case 'signup':
          doCredentialAuth('signup');
          break;
        case 'token-login':
          doTokenLogin();
          break;
        case 'logout':
          doLogout();
          break;
        case 'set-filter':
          setFilter(target.getAttribute('data-filter') || '');
          break;
        case 'show-graph':
          showGraph();
          break;
        case 'show-list':
          showList();
          break;
        case 'open-new-memory':
          openNewMemory();
          break;
        case 'close-new-memory':
          closeNewMemory();
          break;
        case 'close-new-memory-overlay':
          closeNewMemoryOverlay(event);
          break;
        case 'submit-new-memory':
          submitNewMemory();
          break;
        case 'refresh-memories':
          loadMemories();
          break;
        case 'open-command-palette':
          openCommandPalette();
          break;
        case 'toggle-shortcuts-overlay':
          toggleShortcutsOverlay();
          break;
        case 'open-settings-overlay':
          openSettingsOverlay();
          break;
        case 'toggle-graph-inferred':
          toggleGraphInferred();
          break;
        case 'toggle-graph-labels':
          toggleGraphLabels();
          break;
        case 'toggle-graph-physics':
          toggleGraphPhysics();
          break;
        case 'reset-graph-view':
          resetGraphView();
          break;
        case 'toggle-graph-relation':
          toggleGraphRelation(target.getAttribute('data-relation') || '');
          break;
        case 'close-expand-overlay':
          closeExpand(event);
          break;
        case 'close-expand':
          closeExpandBtn();
          break;
        case 'close-command-palette-overlay':
          closeCommandPalette(event);
          break;
        case 'close-shortcuts-overlay':
          closeShortcutsOverlay(event);
          break;
        case 'close-shortcuts':
          closeShortcutsOverlay();
          break;
        case 'close-settings-overlay':
          closeSettingsOverlay(event);
          break;
        case 'close-settings':
          closeSettingsOverlay();
          break;
        case 'run-semantic-reindex':
          runSemanticReindexFromSettings();
          break;
        case 'run-export':
          runExport();
          break;
        case 'choose-import-file':
          chooseImportFile();
          break;
        case 'run-import':
          runImportFromSettings();
          break;
        case 'run-purge':
          runPurge();
          break;
        case 'open-changelog-overlay':
          openChangelogOverlay();
          break;
        case 'reset-viewer-settings':
          resetViewerSettings();
          break;
        case 'reset-custom-theme':
          resetCustomTheme();
          break;
        case 'apply-settings':
          applySettingsFromForm();
          break;
        case 'close-changelog-overlay':
          closeChangelogOverlay(event);
          break;
        case 'close-changelog':
          closeChangelogOverlay();
          break;
        case 'dismiss-update-banner':
          dismissUpdateBanner();
          break;
        case 'open-full-changelog':
          window.open('https://github.com/guirguispierre/memoryvault/blob/main/CHANGELOG.md', '_blank', 'noopener');
          break;
        case 'expand-card':
          expandCard(Number(target.getAttribute('data-card-index') || target.getAttribute('data-idx') || '-1'));
          break;
        default:
          break;
      }
    });

    document.addEventListener('click', (event) => {
      const el = event.target instanceof Element ? event.target : null;
      if (!el) return;

      const swatch = el.closest('.theme-swatch');
      if (swatch) {
        const themeValue = swatch.getAttribute('data-theme-value');
        if (!themeValue) return;
        viewerSettings = readSettingsFromForm();
        if (swatch.closest('#light-theme-picker')) {
          viewerSettings.light_theme = themeValue;
        } else {
          viewerSettings.theme = themeValue;
        }
        persistViewerSettings();
        scheduleServerSettingsSave();
        applyViewerSettingsToRuntime({ restartPolling: false, rerenderGraph: graphVisible, rerenderGrid: false });
        return;
      }

      const modeBtn = el.closest('.theme-mode-btn');
      if (modeBtn) {
        const mode = modeBtn.getAttribute('data-mode');
        if (!mode) return;
        viewerSettings = readSettingsFromForm();
        viewerSettings.theme_mode = mode;
        persistViewerSettings();
        scheduleServerSettingsSave();
        applyViewerSettingsToRuntime({ restartPolling: false, rerenderGraph: graphVisible, rerenderGrid: false });
        return;
      }
    });

    bindInput('search-input', onSearch);
    bindInput('graph-search-input', onGraphSearch);
    bindInput('cmd-input', onCommandFilter);

    const customBuilder = document.getElementById('custom-theme-builder');
    if (customBuilder) {
      const onBuilderChange = (event) => {
        const t = event.target;
        if (!(t instanceof Element)) return;
        const token = t.getAttribute('data-custom-token');
        const kind = t.getAttribute('data-custom-kind');
        if (!token || !kind) return;
        onCustomThemeFieldInput(token, kind, typeof t.value === 'string' ? t.value : '');
      };
      // 'input' drives live preview from the color pickers and hex fields;
      // 'change' covers the font <select>.
      customBuilder.addEventListener('input', onBuilderChange);
      customBuilder.addEventListener('change', onBuilderChange);
    }
  }

  syncGraphToolbarState();
  bindViewerEventHandlers();

  const importFileInput = document.getElementById('import-file-input');
  if (importFileInput) importFileInput.addEventListener('change', onImportFileSelected);
  const importStrategySelect = document.getElementById('import-strategy');
  if (importStrategySelect) importStrategySelect.addEventListener('change', onImportStrategyChanged);

  // Enter key on login
  document.getElementById('token-input').addEventListener('keydown', e => { if (e.key === 'Enter') doTokenLogin(); });
  document.getElementById('email-input').addEventListener('keydown', e => { if (e.key === 'Enter') doCredentialAuth('login'); });
  document.getElementById('password-input').addEventListener('keydown', e => { if (e.key === 'Enter') doCredentialAuth('login'); });
  document.getElementById('brain-name-input').addEventListener('keydown', e => { if (e.key === 'Enter') doCredentialAuth('signup'); });
  document.getElementById('cmd-input').addEventListener('keydown', e => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      moveCommandSelection(1);
      return;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      moveCommandSelection(-1);
      return;
    }
    if (e.key === 'Enter') {
      e.preventDefault();
      runCommandAction();
      return;
    }
    if (e.key === 'Escape') {
      e.preventDefault();
      closeCommandPalette();
    }
  });
  document.addEventListener('keydown', e => {
    const key = String(e.key || '').toLowerCase();
    const shortcutsOpen = document.getElementById('shortcuts-overlay').classList.contains('open');
    const settingsOpen = document.getElementById('settings-overlay').classList.contains('open');
    const changelogOpen = document.getElementById('changelog-overlay').classList.contains('open');
    const expandOpen = document.getElementById('expand-overlay').classList.contains('open');
    const newMemoryOpen = document.getElementById('newmem-overlay').classList.contains('open');
    const typing = isTypingTarget(e.target);

    if (newMemoryOpen) {
      if (key === 'escape') {
        e.preventDefault();
        closeNewMemory();
      }
      return;
    }

    if ((e.ctrlKey || e.metaKey) && key === 'k') {
      e.preventDefault();
      if (commandPaletteOpen) closeCommandPalette();
      else openCommandPalette();
      return;
    }

    if (commandPaletteOpen) {
      if (key === 'escape') {
        e.preventDefault();
        closeCommandPalette();
      }
      return;
    }

    if (shortcutsOpen) {
      if (key === 'escape') {
        e.preventDefault();
        closeShortcutsOverlay();
      }
      return;
    }

    if (changelogOpen) {
      if (key === 'escape') {
        e.preventDefault();
        closeChangelogOverlay();
      }
      return;
    }

    if (settingsOpen) {
      if (key === 'escape') {
        e.preventDefault();
        closeSettingsOverlay();
      }
      return;
    }

    if (e.key === '?' && !typing) {
      e.preventDefault();
      toggleShortcutsOverlay();
      return;
    }

    if (key === 'escape' && expandOpen) {
      e.preventDefault();
      closeExpandBtn();
      return;
    }

    if (typing) return;
    if (!hasAuthenticatedSession() || !appIsVisible()) return;

    if (key === '/') {
      e.preventDefault();
      const input = document.getElementById('search-input');
      if (!input) return;
      input.focus();
      input.select();
      return;
    }
    if (key === 'g') {
      e.preventDefault();
      showGraph();
      return;
    }
    if (key === 's') {
      e.preventDefault();
      openSettingsOverlay();
      return;
    }
    if (key === 'r') {
      e.preventDefault();
      loadMemories();
    }
  });
`;
