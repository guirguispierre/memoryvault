// First-run onboarding for the viewer. When the authenticated brain has no
// memories yet, the empty grid is replaced with a guided "connect your first
// agent" panel: this brain's own MCP endpoint, the real OAuth connection
// snippets, and a button that writes a genuine first memory through the
// authenticated MCP path. Once a memory exists the panel collapses to a small
// dismissible hint. Everything here uses only this brain's own origin and the
// shared in-scope helpers (BASE, callMcpTool, loadMemories, showToast, esc).
// The connection command/labels come from the shared connect-snippets module so
// this panel and the /docs connect guide are guaranteed to match.
import {
  CLAUDE_CODE_PREFIX,
  MCP_SERVER_LABEL,
  MCP_TRANSPORT_LABEL,
  MCP_AUTH_LABEL,
} from '../connect-snippets.js';

export const clientOnboarding = `
  var onboardingForceOpen = false;
  var ONBOARDING_DISMISS_KEY = 'mv_onboarding_dismissed_v1';

  function onboardingDismissed() {
    try { return localStorage.getItem(ONBOARDING_DISMISS_KEY) === '1'; } catch (e) { return false; }
  }

  function onboardingFullHtml() {
    var url = BASE + '/mcp';
    var claudeCmd = '${CLAUDE_CODE_PREFIX} ' + url;
    var jsonCfg = '{\\n  "mcpServers": {\\n    "${MCP_SERVER_LABEL}": {\\n      "url": "' + url + '"\\n    }\\n  }\\n}';
    return '' +
      '<div class="ob-card">' +
        '<div class="ob-head">' +
          '<div class="ob-eyebrow">First run</div>' +
          '<h2 class="ob-title">Connect your first agent</h2>' +
          '<p class="ob-lede">Three steps to get an AI agent reading and writing your memory. About two minutes, no guesswork.</p>' +
        '</div>' +
        '<ol class="ob-steps">' +
          '<li class="ob-step"><div class="ob-num">1</div><div class="ob-bd">' +
            '<h3>Your endpoint</h3>' +
            '<p>Your personal MCP server URL for this brain. This is what you paste into your agent.</p>' +
            '<div class="ob-row"><code class="ob-mono">' + esc(url) + '</code><button class="ob-copy" type="button" data-onb="copy">Copy</button></div>' +
          '</div></li>' +
          '<li class="ob-step"><div class="ob-num">2</div><div class="ob-bd">' +
            '<h3>Connect your agent</h3>' +
            '<div class="ob-tabs"><button class="ob-tab on" type="button" data-onb="tab" data-tab="claude">Claude Code</button><button class="ob-tab" type="button" data-onb="tab" data-tab="any">Codex / any client</button></div>' +
            '<div class="ob-pane" data-pane="claude">' +
              '<div class="ob-row"><code class="ob-mono">' + esc(claudeCmd) + '</code><button class="ob-copy" type="button" data-onb="copy">Copy</button></div>' +
              '<p class="ob-note">Run it, then sign in when your browser opens. Auth is OAuth, so there is no token to paste or leak.</p>' +
            '</div>' +
            '<div class="ob-pane" data-pane="any" hidden>' +
              '<p class="ob-note">Add a remote MCP server with these values. This works for Codex, Cursor, and any custom client.</p>' +
              '<div class="ob-kv"><span>Server URL</span><code>' + esc(url) + '</code></div>' +
              '<div class="ob-kv"><span>Transport</span><code>${MCP_TRANSPORT_LABEL}</code></div>' +
              '<div class="ob-kv"><span>Auth</span><code>${MCP_AUTH_LABEL}</code></div>' +
              '<div class="ob-row"><code class="ob-mono ob-pre">' + esc(jsonCfg) + '</code><button class="ob-copy" type="button" data-onb="copy">Copy</button></div>' +
            '</div>' +
          '</div></li>' +
          '<li class="ob-step"><div class="ob-num">3</div><div class="ob-bd">' +
            '<h3>Watch your first memory land</h3>' +
            '<p>Ask your connected agent to remember something, or write a sample memory now to see recall and the graph come alive. It is a real memory you can delete anytime.</p>' +
            '<button class="ob-primary" type="button" data-onb="test">Write a test memory</button>' +
          '</div></li>' +
        '</ol>' +
        '<button class="ob-skip" type="button" data-onb="dismiss">Skip for now</button>' +
      '</div>';
  }

  function onboardingHintHtml() {
    return '' +
      '<div class="ob-hint">' +
        '<span class="ob-hint-dot"></span>' +
        '<span class="ob-hint-text">Your agent is connected. Need the connection steps again?</span>' +
        '<button class="ob-hint-link" type="button" data-onb="reopen">Show steps</button>' +
        '<button class="ob-hint-x" type="button" data-onb="dismiss" title="Dismiss">&times;</button>' +
      '</div>';
  }

  function renderOnboarding(count) {
    var host = document.getElementById('onboarding');
    if (!host) return;
    if (onboardingDismissed() && !onboardingForceOpen) { host.hidden = true; host.innerHTML = ''; return; }
    if (count === 0 || onboardingForceOpen) {
      host.innerHTML = onboardingFullHtml();
      host.hidden = false;
      // The panel is the first-run content, so drop the bare empty-state below.
      if (count === 0) { var grid = document.getElementById('grid'); if (grid) grid.innerHTML = ''; }
      return;
    }
    host.innerHTML = onboardingHintHtml();
    host.hidden = false;
  }

  async function onboardingWriteTestMemory(btn) {
    if (btn) { btn.disabled = true; btn.textContent = 'Saving...'; }
    try {
      await callMcpTool('memory_save', {
        type: 'note',
        title: 'My first memory',
        content: 'MemoryVault is connected. This is a sample memory written from onboarding. You can delete it anytime.',
        source: 'onboarding',
      });
      onboardingForceOpen = false;
      showToast('First memory saved. Recall and the graph are live now.', 'success');
      await loadMemories(true);
    } catch (e) {
      if (btn) { btn.disabled = false; btn.textContent = 'Write a test memory'; }
      showToast((e && e.message) || 'Could not write the test memory.', 'error');
    }
  }

  function onboardingCopy(btn) {
    var row = btn.closest('.ob-row');
    var code = row ? row.querySelector('code') : null;
    if (!code) return;
    var text = code.textContent || '';
    function restore(label) { btn.textContent = label; }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(function () {
        var prev = btn.textContent; btn.textContent = 'Copied';
        setTimeout(function () { restore(prev); }, 1400);
      }).catch(function () { showToast('Copy failed. Select the text and copy manually.', 'error'); });
    } else {
      showToast('Copy is not available here. Select the text and copy manually.', 'error');
    }
  }

  // One delegated listener for the whole panel (the CSP forbids inline handlers).
  document.addEventListener('click', function (e) {
    var t = e.target.closest && e.target.closest('[data-onb]');
    if (!t) return;
    var action = t.getAttribute('data-onb');
    if (action === 'copy') { onboardingCopy(t); return; }
    if (action === 'test') { onboardingWriteTestMemory(t); return; }
    if (action === 'reopen') { onboardingForceOpen = true; renderOnboarding(1); return; }
    if (action === 'dismiss') {
      try { localStorage.setItem(ONBOARDING_DISMISS_KEY, '1'); } catch (err) {}
      onboardingForceOpen = false;
      var host = document.getElementById('onboarding');
      if (host) { host.hidden = true; host.innerHTML = ''; }
      return;
    }
    if (action === 'tab') {
      var tab = t.getAttribute('data-tab');
      var tabs = t.parentNode.querySelectorAll('.ob-tab');
      for (var i = 0; i < tabs.length; i++) tabs[i].classList.toggle('on', tabs[i] === t);
      var panes = t.closest('.ob-bd').querySelectorAll('.ob-pane');
      for (var j = 0; j < panes.length; j++) panes[j].hidden = panes[j].getAttribute('data-pane') !== tab;
    }
  });
`;
