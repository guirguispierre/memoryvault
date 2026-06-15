import {
  type Env,
  type AuthContext,
  type MemoryType,
  type EndpointGuide,
  type ToolArgs,
  VALID_TYPES,
  RELATION_TYPES,
} from './types.js';

import {
  SERVER_NAME,
  SERVER_VERSION,
  MCP_SSE_KEEPALIVE_INTERVAL_MS,
} from './constants.js';

import {
  jsonResponse,
  canMutateMemories,
  now,
  toFiniteNumber,
  normalizeSourceKey,
  normalizeRelation,
  clampToRange,
  isValidType,
  generateId,
  stableJson,
  escapeHtml,
} from './utils.js';

import {
  CORS_HEADERS,
  corsJsonResponse,
} from './cors.js';

import {
  loadLinkStatsMap,
  loadSourceTrustMap,
  getBrainPolicy,
  setBrainPolicy,
  getViewerSettings,
  setViewerSettings,
  logChangelog,
  insertWaitlistEmail,
} from './db.js';

import {
  safeSyncMemoriesToVectorIndex,
  safeDeleteMemoryVectors,
} from './vectorize.js';

import {
  enrichMemoryRowsWithDynamics,
  projectMemoryForClient,
  computeDynamicScores,
} from './scoring.js';

import {
  TOOLS,
  isMutatingTool,
} from './tools-schema.js';

import {
  callTool,
} from './tools/index.js';

import {
  mcpUrlFor,
  claudeCodeCommand,
  mcpJsonConfig,
  MCP_TRANSPORT_LABEL,
  MCP_AUTH_LABEL,
} from './connect-snippets.js';

import {
  FONT_LINK_TAGS,
  pageChromeCss,
} from './viewer/tokens.js';

import {
  constellationTokensCss,
  constellationHeadTags,
  constellationCalmField,
} from './viewer/constellation.js';

export async function processMcpBody(
  body: { jsonrpc: string; id?: unknown; method: string; params?: Record<string, unknown> },
  env: Env,
  authCtx: AuthContext
): Promise<unknown> {
  const { id, method, params = {} } = body;

  if (method === 'initialize') {
    return {
      jsonrpc: '2.0', id,
      result: {
        protocolVersion: '2024-11-05',
        capabilities: { tools: {} },
        serverInfo: { name: SERVER_NAME, version: SERVER_VERSION },
      },
    };
  }

  if (method === 'tools/list') {
    const tools = canMutateMemories(authCtx)
      ? TOOLS
      : TOOLS.filter((tool) => !isMutatingTool(tool.name));
    return { jsonrpc: '2.0', id, result: { tools } };
  }

  if (method === 'tools/call') {
    const { name, arguments: toolArgs = {} } = params as { name?: unknown; arguments?: ToolArgs };
    if (typeof name !== 'string' || !name.trim()) {
      return {
        jsonrpc: '2.0',
        id,
        error: { code: -32602, message: 'Invalid params: tool name is required.' },
      };
    }
    if (!canMutateMemories(authCtx) && isMutatingTool(name)) {
      return {
        jsonrpc: '2.0',
        id,
        error: {
          code: -32003,
          message: 'Forbidden: this session cannot modify memories. Re-authenticate and try again.',
        },
      };
    }
    const result = await callTool(name, toolArgs, env, authCtx.brainId);
    return { jsonrpc: '2.0', id, result };
  }

  if (method === 'notifications/initialized' || method.startsWith('notifications/')) {
    return null; // notifications get no response
  }

  return {
    jsonrpc: '2.0', id,
    error: { code: -32601, message: `Method not found: ${method}` },
  };
}

export async function handleMcp(request: Request, env: Env, url: URL, authCtx: AuthContext): Promise<Response> {
  const acceptsSse = (request.headers.get('Accept') ?? '').includes('text/event-stream');

  // SSE transport: GET /mcp opens the event stream
  if (request.method === 'GET' && acceptsSse) {
    const postUrl = `${url.origin}/mcp`;
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const enc = new TextEncoder();

    // Send the endpoint event immediately then keep-alive
    (async () => {
      // endpoint event tells client where to POST messages
      await writer.write(enc.encode(`event: endpoint\ndata: ${postUrl}\n\n`));
      // Keep the connection alive with periodic pings
      const interval = setInterval(async () => {
        try {
          await writer.write(enc.encode(': ping\n\n'));
        } catch {
          clearInterval(interval);
        }
      }, MCP_SSE_KEEPALIVE_INTERVAL_MS);
    })();

    return new Response(readable, {
      headers: {
        ...CORS_HEADERS,
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
      },
    });
  }

  // SSE transport: POST sends a message and returns SSE response
  if (request.method === 'POST') {
    let body: { jsonrpc: string; id?: unknown; method: string; params?: Record<string, unknown> };
    try {
      body = await request.json();
    } catch {
      return jsonResponse({ jsonrpc: '2.0', error: { code: -32700, message: 'Parse error' }, id: null });
    }

    let responseObj: unknown;
    try {
      responseObj = await processMcpBody(body, env, authCtx);
    } catch (err) {
      const code = (err instanceof Error && 'code' in err && typeof (err as { code?: unknown }).code === 'number')
        ? (err as { code: number }).code
        : -32603;
      const message = err instanceof Error ? err.message : 'Internal error';
      responseObj = { jsonrpc: '2.0', id: body.id, error: { code, message } };
    }

    // If client accepts SSE, stream the response as an SSE event
    if (acceptsSse || (request.headers.get('Accept') ?? '').includes('text/event-stream')) {
      if (responseObj === null) {
        return new Response(null, { status: 204, headers: CORS_HEADERS });
      }
      const sseBody = `event: message\ndata: ${JSON.stringify(responseObj)}\n\n`;
      return new Response(sseBody, {
        headers: {
          ...CORS_HEADERS,
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
        },
      });
    }

    // Plain HTTP JSON response (for standard MCP HTTP transport)
    if (responseObj === null) {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }
    return new Response(JSON.stringify(responseObj), {
      headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    });
  }

  return jsonResponse({ error: 'Method not allowed' }, 405);
}

export async function handleApiMemories(request: Request, env: Env, brainId: string): Promise<Response> {
  const url = new URL(request.url);
  const type = url.searchParams.get('type') ?? '';
  const search = url.searchParams.get('search') ?? '';
  const limitParam = parseInt(url.searchParams.get('limit') ?? '100', 10);
  const limit = Math.min(Math.max(Number.isNaN(limitParam) ? 100 : limitParam, 1), 500);

  let query = 'SELECT m.*, (SELECT COUNT(*) FROM memory_links ml WHERE ml.brain_id = ? AND (ml.from_id = m.id OR ml.to_id = m.id)) as link_count FROM memories m WHERE m.brain_id = ? AND m.archived_at IS NULL';
  const params: unknown[] = [brainId, brainId];
  if (type && VALID_TYPES.includes(type as MemoryType)) {
    query += ' AND type = ?'; params.push(type);
  }
  if (search) {
    const like = `%${search}%`;
    query += ' AND (m.id LIKE ? OR m.content LIKE ? OR m.title LIKE ? OR m.key LIKE ? OR m.source LIKE ?)';
    params.push(like, like, like, like, like);
  }
  query += ' ORDER BY m.created_at DESC LIMIT ?';
  params.push(limit);

  const results = await env.DB.prepare(query).bind(...params).all();
  const tsNow = now();
  const linkStatsMap = await loadLinkStatsMap(env, brainId);
  const sourceTrustMap = await loadSourceTrustMap(env, brainId);
  const enrichedMemories = enrichMemoryRowsWithDynamics(
    results.results as Array<Record<string, unknown>>,
    linkStatsMap,
    tsNow,
    sourceTrustMap
  );
  const projectedMemories = enrichedMemories.map(projectMemoryForClient);
  const sortedMemories = [...projectedMemories].sort(
    (a, b) => toFiniteNumber(b.created_at, 0) - toFiniteNumber(a.created_at, 0)
  );
  const stats = await env.DB.prepare('SELECT type, COUNT(*) as count FROM memories WHERE brain_id = ? AND archived_at IS NULL GROUP BY type').bind(brainId).all();
  const archived = await env.DB.prepare('SELECT COUNT(*) as count FROM memories WHERE brain_id = ? AND archived_at IS NOT NULL').bind(brainId).first<{ count: number }>();
  return new Response(JSON.stringify({ memories: sortedMemories, stats: stats.results, archived_count: archived?.count ?? 0 }), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

// Cheap change fingerprint for the live poll: count + newest updated_at +
// total links. This moves on edits, reinforcement, decay, and link changes —
// not just on a count change — without pulling any memory bodies.
export async function handleApiMemoriesSignature(env: Env, brainId: string): Promise<Response> {
  const mem = await env.DB.prepare(
    'SELECT COUNT(*) as count, COALESCE(MAX(updated_at), 0) as last_updated FROM memories WHERE brain_id = ? AND archived_at IS NULL'
  ).bind(brainId).first<{ count: number; last_updated: number }>();
  const links = await env.DB.prepare(
    'SELECT COUNT(*) as link_total FROM memory_links WHERE brain_id = ?'
  ).bind(brainId).first<{ link_total: number }>();
  return new Response(JSON.stringify({
    count: mem?.count ?? 0,
    last_updated: mem?.last_updated ?? 0,
    link_total: links?.link_total ?? 0,
  }), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

export async function handleApiLinks(memoryId: string, env: Env, brainId: string): Promise<Response> {
  const mem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL').bind(brainId, memoryId).first();
  if (!mem) return new Response(JSON.stringify({ error: 'Memory not found.' }), {
    status: 404, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });

  const fromLinks = await env.DB.prepare(
    'SELECT ml.id as link_id, ml.relation_type, ml.label, m.id, m.type, m.title, m.key, m.content, m.tags, m.source, m.confidence, m.importance, m.created_at, m.updated_at FROM memory_links ml JOIN memories m ON m.id = ml.to_id WHERE ml.brain_id = ? AND m.brain_id = ? AND ml.from_id = ? AND m.archived_at IS NULL'
  ).bind(brainId, brainId, memoryId).all();

  const toLinks = await env.DB.prepare(
    'SELECT ml.id as link_id, ml.relation_type, ml.label, m.id, m.type, m.title, m.key, m.content, m.tags, m.source, m.confidence, m.importance, m.created_at, m.updated_at FROM memory_links ml JOIN memories m ON m.id = ml.from_id WHERE ml.brain_id = ? AND m.brain_id = ? AND ml.to_id = ? AND m.archived_at IS NULL'
  ).bind(brainId, brainId, memoryId).all();

  const tsNow = now();
  const linkStatsMap = await loadLinkStatsMap(env, brainId);
  const sourceTrustMap = await loadSourceTrustMap(env, brainId);
  const toScoredMemory = (r: Record<string, unknown>): Record<string, unknown> => {
    const base = {
      id: r.id,
      type: r.type,
      title: r.title,
      key: r.key,
      content: r.content,
      tags: r.tags,
      source: r.source,
      confidence: r.confidence,
      importance: r.importance,
      created_at: r.created_at,
      updated_at: r.updated_at,
    } as Record<string, unknown>;
    const sourceKey = typeof base.source === 'string' ? normalizeSourceKey(base.source) : '';
    return projectMemoryForClient({
      ...base,
      ...computeDynamicScores(base, linkStatsMap.get(String(r.id ?? '')), tsNow, sourceKey ? sourceTrustMap.get(sourceKey) : undefined),
    });
  };

  const results = [
    ...fromLinks.results.map((r: Record<string, unknown>) => ({
      link_id: r.link_id,
      relation_type: r.relation_type,
      label: r.label,
      direction: 'from',
      memory: toScoredMemory(r),
    })),
    ...toLinks.results.map((r: Record<string, unknown>) => ({
      link_id: r.link_id,
      relation_type: r.relation_type,
      label: r.label,
      direction: 'to',
      memory: toScoredMemory(r),
    })),
  ];

  return new Response(JSON.stringify(results), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

export async function handleApiGraph(env: Env, brainId: string): Promise<Response> {
  const memories = await env.DB.prepare(
    'SELECT id, type, title, key, content, tags, source, confidence, importance FROM memories WHERE brain_id = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT 1000'
  ).bind(brainId).all();
  const links = await env.DB.prepare(
    'SELECT ml.id, ml.from_id, ml.to_id, ml.label, ml.relation_type FROM memory_links ml JOIN memories m1 ON m1.id = ml.from_id AND m1.brain_id = ? AND m1.archived_at IS NULL JOIN memories m2 ON m2.id = ml.to_id AND m2.brain_id = ? AND m2.archived_at IS NULL WHERE ml.brain_id = ? LIMIT 5000'
  ).bind(brainId, brainId, brainId).all();

  const tsNow = now();
  const policy = await getBrainPolicy(env, brainId);
  const linkStatsMap = await loadLinkStatsMap(env, brainId);
  const sourceTrustMap = await loadSourceTrustMap(env, brainId);
  const nodes = enrichMemoryRowsWithDynamics(
    memories.results as Array<Record<string, unknown>>,
    linkStatsMap,
    tsNow,
    sourceTrustMap
  ).map(projectMemoryForClient);
  const explicitEdges = links.results as Array<Record<string, unknown>>;

  // Build inferred (non-persisted) graph edges from shared tags.
  // This helps visualization when explicit links are sparse.
  const tagToIds = new Map<string, string[]>();
  for (const n of nodes) {
    const id = typeof n.id === 'string' ? n.id : '';
    if (!id) continue;
    const tags = typeof n.tags === 'string' ? n.tags : '';
    if (!tags) continue;
    for (const rawTag of tags.split(',')) {
      const tag = rawTag.trim().toLowerCase();
      if (!tag) continue;
      const ids = tagToIds.get(tag);
      if (ids) ids.push(id);
      else tagToIds.set(tag, [id]);
    }
  }

  const inferredByPair = new Map<string, { from_id: string; to_id: string; tags: Set<string>; score: number }>();
  for (const [tag, idsRaw] of tagToIds) {
    const ids = Array.from(new Set(idsRaw));
    if (ids.length < 2) continue;
    // Guard against explosive pair counts for broad tags.
    const limited = ids.slice(0, 28);
    const tagWeight = 1 / Math.sqrt(limited.length);
    for (let i = 0; i < limited.length; i++) {
      for (let j = i + 1; j < limited.length; j++) {
        const a = limited[i];
        const b = limited[j];
        const from_id = a < b ? a : b;
        const to_id = a < b ? b : a;
        const key = `${from_id}|${to_id}`;
        const existing = inferredByPair.get(key);
        if (existing) {
          existing.tags.add(tag);
          existing.score += tagWeight;
        } else {
          inferredByPair.set(key, { from_id, to_id, tags: new Set([tag]), score: tagWeight });
        }
      }
    }
  }

  const explicitPairs = new Set(
    explicitEdges.map((e) => {
      const a = String(e.from_id ?? '');
      const b = String(e.to_id ?? '');
      return a < b ? `${a}|${b}` : `${b}|${a}`;
    })
  );

  const inferredCandidates = Array.from(inferredByPair.entries())
    .filter(([pair]) => !explicitPairs.has(pair))
    .map(([pair, v]) => {
      const tags = Array.from(v.tags).sort();
      const preview = tags.slice(0, 3);
      const suffix = tags.length > 3 ? ` +${tags.length - 3}` : '';
      const score = Number(v.score.toFixed(3));
      return {
        id: `inf-${pair.replace('|', '-')}`,
        from_id: v.from_id,
        to_id: v.to_id,
        label: `shared: ${preview.join(', ')}${suffix}`,
        tags,
        strength: tags.length,
        score,
        inferred: true,
      };
    })
    // Keep only meaningful suggestions from shared context.
    .filter((e) => e.strength >= 2 || e.score >= 0.85)
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return b.strength - a.strength;
    });

  // Greedy sparsification to prevent inferred hubs from collapsing the graph.
  const inferredEdges: Array<{
    id: string;
    from_id: string;
    to_id: string;
    label: string;
    tags: string[];
    strength: number;
    score: number;
    inferred: boolean;
  }> = [];
  const inferredDegreeByNode = new Map<string, number>();
  const inferredMax = Math.min(Math.max(policy.max_inferred_edges, 30), 5000);
  const inferredPerNodeCap = 7;
  for (const edge of inferredCandidates) {
    if (inferredEdges.length >= inferredMax) break;
    const fromDeg = inferredDegreeByNode.get(edge.from_id) ?? 0;
    const toDeg = inferredDegreeByNode.get(edge.to_id) ?? 0;
    if (fromDeg >= inferredPerNodeCap || toDeg >= inferredPerNodeCap) continue;
    inferredEdges.push(edge);
    inferredDegreeByNode.set(edge.from_id, fromDeg + 1);
    inferredDegreeByNode.set(edge.to_id, toDeg + 1);
  }

  return new Response(JSON.stringify({ nodes, edges: explicitEdges, inferred_edges: inferredEdges }), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

export function handleApiTools(authCtx: AuthContext): Response {
  const tools = canMutateMemories(authCtx)
    ? TOOLS
    : TOOLS.filter((tool) => !isMutatingTool(tool.name));
  return new Response(JSON.stringify({
    server: { name: SERVER_NAME, version: SERVER_VERSION },
    tool_count: tools.length,
    tool_names: tools.map((t) => t.name),
    relation_types: RELATION_TYPES,
  }), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

const EXPORT_SCHEMA = 'memoryvault_export_v1';
type ImportStrategy = 'merge' | 'overwrite' | 'skip_existing';

export async function handleApiExport(env: Env, brainId: string): Promise<Response> {
  const ts = now();

  const memories = await env.DB.prepare(
    `SELECT id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at
     FROM memories WHERE brain_id = ? ORDER BY created_at DESC LIMIT 50000`
  ).bind(brainId).all<Record<string, unknown>>();

  const linksRows = await env.DB.prepare(
    `SELECT id, from_id, to_id, relation_type, label, created_at
     FROM memory_links WHERE brain_id = ? ORDER BY created_at DESC LIMIT 50000`
  ).bind(brainId).all<Record<string, unknown>>();
  const links = linksRows.results;

  const changelog = await env.DB.prepare(
    `SELECT id, event_type, entity_type, entity_id, summary, payload, created_at
     FROM memory_changelog WHERE brain_id = ? ORDER BY created_at DESC LIMIT 50000`
  ).bind(brainId).all<Record<string, unknown>>();

  const sourceTrust = await env.DB.prepare(
    'SELECT source_key, trust, notes, created_at, updated_at FROM brain_source_trust WHERE brain_id = ? ORDER BY source_key ASC'
  ).bind(brainId).all<Record<string, unknown>>();

  const policy = await getBrainPolicy(env, brainId);

  const conflictResolutions = await env.DB.prepare(
    'SELECT pair_key, a_id, b_id, status, canonical_id, note, created_at, updated_at FROM memory_conflict_resolutions WHERE brain_id = ? ORDER BY updated_at DESC LIMIT 50000'
  ).bind(brainId).all<Record<string, unknown>>();

  const aliases = await env.DB.prepare(
    'SELECT canonical_memory_id, alias_memory_id, note, confidence, created_at, updated_at FROM memory_entity_aliases WHERE brain_id = ? ORDER BY updated_at DESC LIMIT 50000'
  ).bind(brainId).all<Record<string, unknown>>();

  const watches = await env.DB.prepare(
    'SELECT name, event_types, query, webhook_url, is_active, created_at, updated_at FROM memory_watches WHERE brain_id = ? ORDER BY updated_at DESC LIMIT 1000'
  ).bind(brainId).all<Record<string, unknown>>();

  const sanitizedWatches = watches.results.map((w) => {
    const copy = { ...w };
    delete copy.webhook_url;
    delete copy.secret;
    return copy;
  });

  const payload = {
    schema: EXPORT_SCHEMA,
    exported_at: ts,
    data: {
      memories: memories.results,
      memory_links: links,
      memory_changelog: changelog.results,
      brain_source_trust: sourceTrust.results,
      brain_policy: policy,
      memory_conflict_resolutions: conflictResolutions.results,
      memory_entity_aliases: aliases.results,
      memory_watches: sanitizedWatches,
    },
    stats: {
      memories: memories.results.length,
      memory_links: links.length,
      memory_changelog: changelog.results.length,
      brain_source_trust: sourceTrust.results.length,
      memory_conflict_resolutions: conflictResolutions.results.length,
      memory_entity_aliases: aliases.results.length,
      memory_watches: sanitizedWatches.length,
    },
  };

  const dateStr = new Date(ts * 1000).toISOString().slice(0, 10);
  const filename = `memoryvault-export-${dateStr}.json`;

  return new Response(stableJson(payload), {
    headers: {
      ...CORS_HEADERS,
      'Content-Type': 'application/json',
      'Content-Disposition': `attachment; filename="${filename}"`,
      'Cache-Control': 'no-store',
      'Pragma': 'no-cache',
    },
  });
}

export async function handleApiImport(request: Request, env: Env, brainId: string): Promise<Response> {
  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ error: 'Method not allowed' }), {
      status: 405, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    });
  }

  let body: Record<string, unknown>;
  try {
    body = await request.json() as Record<string, unknown>;
  } catch {
    return new Response(JSON.stringify({ error: 'Invalid JSON body' }), {
      status: 400, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    });
  }

  if (body.schema !== EXPORT_SCHEMA) {
    return new Response(JSON.stringify({ error: `Unsupported schema. Expected "${EXPORT_SCHEMA}".` }), {
      status: 400, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    });
  }

  const strategyRaw = typeof body.strategy === 'string' ? body.strategy : 'merge';
  const validStrategies: ImportStrategy[] = ['merge', 'overwrite', 'skip_existing'];
  if (!validStrategies.includes(strategyRaw as ImportStrategy)) {
    return new Response(JSON.stringify({ error: `Invalid strategy. Must be one of: ${validStrategies.join(', ')}` }), {
      status: 400, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    });
  }
  const strategy = strategyRaw as ImportStrategy;

  const data = body.data && typeof body.data === 'object' && !Array.isArray(body.data)
    ? body.data as Record<string, unknown>
    : null;
  if (!data) {
    return new Response(JSON.stringify({ error: 'Missing or invalid "data" field.' }), {
      status: 400, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    });
  }

  const memoriesPayload = Array.isArray(data.memories) ? data.memories as Array<Record<string, unknown>> : [];
  const linksPayload = Array.isArray(data.memory_links) ? data.memory_links as Array<Record<string, unknown>> : [];
  const changelogPayload = Array.isArray(data.memory_changelog) ? data.memory_changelog as Array<Record<string, unknown>> : [];
  const sourceTrustPayload = Array.isArray(data.brain_source_trust) ? data.brain_source_trust as Array<Record<string, unknown>> : [];
  const policyPayload = data.brain_policy && typeof data.brain_policy === 'object' && !Array.isArray(data.brain_policy)
    ? data.brain_policy as Record<string, unknown>
    : null;
  const conflictResolutionsPayload = Array.isArray(data.memory_conflict_resolutions) ? data.memory_conflict_resolutions as Array<Record<string, unknown>> : [];
  const aliasesPayload = Array.isArray(data.memory_entity_aliases) ? data.memory_entity_aliases as Array<Record<string, unknown>> : [];
  const watchesPayload = Array.isArray(data.memory_watches) ? data.memory_watches as Array<Record<string, unknown>> : [];

  // Validate payload has restorable content before destructive overwrite
  if (strategy === 'overwrite' && memoriesPayload.length === 0) {
    return new Response(JSON.stringify({ error: 'Overwrite import requires at least one memory in the payload. Aborting to prevent data loss.' }), {
      status: 400, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    });
  }

  const ts = now();
  const counts = { memories: 0, memory_links: 0, memory_changelog: 0, brain_source_trust: 0, memory_conflict_resolutions: 0, memory_entity_aliases: 0, memory_watches: 0, skipped: 0 };
  const restoredMemoryRows: Array<Record<string, unknown>> = [];

  if (strategy === 'overwrite') {
    const existingIds = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? LIMIT 50000').bind(brainId).all<{ id: string }>();
    await safeDeleteMemoryVectors(env, brainId, existingIds.results.map((r) => r.id), 'import_overwrite_purge');
    await env.DB.prepare('DELETE FROM memory_links WHERE brain_id = ?').bind(brainId).run();
    await env.DB.prepare('DELETE FROM memory_entity_aliases WHERE brain_id = ?').bind(brainId).run();
    await env.DB.prepare('DELETE FROM memory_conflict_resolutions WHERE brain_id = ?').bind(brainId).run();
    await env.DB.prepare('DELETE FROM memory_changelog WHERE brain_id = ?').bind(brainId).run();
    await env.DB.prepare('DELETE FROM memory_watches WHERE brain_id = ?').bind(brainId).run();
    await env.DB.prepare('DELETE FROM brain_source_trust WHERE brain_id = ?').bind(brainId).run();
    await env.DB.prepare('DELETE FROM brain_snapshots WHERE brain_id = ?').bind(brainId).run();
    await env.DB.prepare('DELETE FROM memories WHERE brain_id = ?').bind(brainId).run();
  }

  let existingIdSet: Set<string> | null = null;
  if (strategy === 'skip_existing') {
    const existingRows = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? LIMIT 50000').bind(brainId).all<{ id: string }>();
    existingIdSet = new Set(existingRows.results.map((r) => r.id));
  }

  for (const raw of memoriesPayload) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) continue;
    const m = raw as Record<string, unknown>;
    const memoryId = typeof m.id === 'string' && m.id ? m.id : generateId();
    if (strategy === 'skip_existing' && existingIdSet?.has(memoryId)) { counts.skipped++; continue; }
    const type = isValidType(m.type) ? m.type : 'note';
    const content = typeof m.content === 'string' && m.content.trim() ? m.content.trim() : '';
    if (!content) continue;
    const archivedAt = m.archived_at == null ? null : Math.floor(toFiniteNumber(m.archived_at, ts));
    const createdAt = Math.floor(toFiniteNumber(m.created_at, ts));
    const updatedAt = Math.floor(toFiniteNumber(m.updated_at, ts));

    await env.DB.prepare(
      `INSERT INTO memories (id, brain_id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET
         type = excluded.type, title = excluded.title, key = excluded.key,
         content = excluded.content, tags = excluded.tags, source = excluded.source,
         confidence = excluded.confidence, importance = excluded.importance,
         archived_at = excluded.archived_at, created_at = excluded.created_at, updated_at = excluded.updated_at
       WHERE memories.brain_id = excluded.brain_id`
    ).bind(
      memoryId, brainId, type,
      typeof m.title === 'string' ? m.title : null,
      typeof m.key === 'string' ? m.key : null,
      content,
      typeof m.tags === 'string' ? m.tags : null,
      typeof m.source === 'string' ? m.source : null,
      clampToRange(m.confidence, 0.7),
      clampToRange(m.importance, 0.5),
      archivedAt, createdAt, updatedAt
    ).run();

    restoredMemoryRows.push({
      id: memoryId, type, content,
      title: typeof m.title === 'string' ? m.title : null,
      key: typeof m.key === 'string' ? m.key : null,
      tags: typeof m.tags === 'string' ? m.tags : null,
      source: typeof m.source === 'string' ? m.source : null,
    });
    counts.memories++;
  }

  if (restoredMemoryRows.length) {
    await safeSyncMemoriesToVectorIndex(env, brainId, restoredMemoryRows, 'import_sync');
  }

  const allMemoryIds = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? LIMIT 50000').bind(brainId).all<{ id: string }>();
  const memoryIdSet = new Set(allMemoryIds.results.map((r) => r.id));

  let existingLinkSet: Set<string> | null = null;
  if (strategy === 'skip_existing') {
    const existingLinks = await env.DB.prepare('SELECT id FROM memory_links WHERE brain_id = ? LIMIT 50000').bind(brainId).all<{ id: string }>();
    existingLinkSet = new Set(existingLinks.results.map((r) => r.id));
  }

  for (const raw of linksPayload) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) continue;
    const link = raw as Record<string, unknown>;
    const fromId = typeof link.from_id === 'string' ? link.from_id : '';
    const toId = typeof link.to_id === 'string' ? link.to_id : '';
    if (!fromId || !toId || !memoryIdSet.has(fromId) || !memoryIdSet.has(toId)) continue;
    const linkId = typeof link.id === 'string' && link.id ? link.id : generateId();
    if (strategy === 'skip_existing' && existingLinkSet?.has(linkId)) { counts.skipped++; continue; }
    await env.DB.prepare(
      `INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET
         from_id = excluded.from_id, to_id = excluded.to_id,
         relation_type = excluded.relation_type, label = excluded.label
       WHERE memory_links.brain_id = excluded.brain_id`
    ).bind(
      linkId, brainId, fromId, toId,
      normalizeRelation(link.relation_type),
      typeof link.label === 'string' ? link.label : null,
      Math.floor(toFiniteNumber(link.created_at, ts))
    ).run();
    counts.memory_links++;
  }

  for (const raw of changelogPayload) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) continue;
    const entry = raw as Record<string, unknown>;
    const entryId = typeof entry.id === 'string' && entry.id ? entry.id : generateId();
    const eventType = typeof entry.event_type === 'string' ? entry.event_type : '';
    const entityType = typeof entry.entity_type === 'string' ? entry.entity_type : '';
    const entityId = typeof entry.entity_id === 'string' ? entry.entity_id : '';
    const summary = typeof entry.summary === 'string' ? entry.summary : '';
    if (!eventType || !entityType || !entityId || !summary) continue;
    const result = await env.DB.prepare(
      `INSERT OR IGNORE INTO memory_changelog (id, brain_id, event_type, entity_type, entity_id, summary, payload, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
    ).bind(
      entryId, brainId, eventType, entityType, entityId, summary,
      typeof entry.payload === 'string' ? entry.payload : (entry.payload ? stableJson(entry.payload) : null),
      Math.floor(toFiniteNumber(entry.created_at, ts))
    ).run();
    if (result.meta.changes > 0) counts.memory_changelog++;
  }

  for (const raw of sourceTrustPayload) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) continue;
    const trustRow = raw as Record<string, unknown>;
    const sourceKey = typeof trustRow.source_key === 'string' ? normalizeSourceKey(trustRow.source_key) : '';
    if (!sourceKey) continue;
    await env.DB.prepare(
      `INSERT INTO brain_source_trust (id, brain_id, source_key, trust, notes, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(brain_id, source_key) DO UPDATE SET trust = excluded.trust, notes = excluded.notes, updated_at = excluded.updated_at`
    ).bind(
      generateId(), brainId, sourceKey,
      clampToRange(trustRow.trust, 0.5),
      typeof trustRow.notes === 'string' ? trustRow.notes : null,
      Math.floor(toFiniteNumber(trustRow.created_at, ts)), ts
    ).run();
    counts.brain_source_trust++;
  }

  for (const raw of conflictResolutionsPayload) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) continue;
    const res = raw as Record<string, unknown>;
    const aId = typeof res.a_id === 'string' ? res.a_id : '';
    const bId = typeof res.b_id === 'string' ? res.b_id : '';
    if (!aId || !bId || !memoryIdSet.has(aId) || !memoryIdSet.has(bId)) continue;
    const pk = [aId, bId].sort().join('::');
    await env.DB.prepare(
      `INSERT INTO memory_conflict_resolutions (id, brain_id, pair_key, a_id, b_id, status, canonical_id, note, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(brain_id, pair_key) DO UPDATE SET status = excluded.status, canonical_id = excluded.canonical_id, note = excluded.note, updated_at = excluded.updated_at`
    ).bind(
      generateId(), brainId, pk, aId, bId,
      typeof res.status === 'string' ? res.status : 'needs_review',
      typeof res.canonical_id === 'string' ? res.canonical_id : null,
      typeof res.note === 'string' ? res.note : null,
      Math.floor(toFiniteNumber(res.created_at, ts)), ts
    ).run();
    counts.memory_conflict_resolutions++;
  }

  for (const raw of aliasesPayload) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) continue;
    const alias = raw as Record<string, unknown>;
    const canonicalId = typeof alias.canonical_memory_id === 'string' ? alias.canonical_memory_id : '';
    const aliasId = typeof alias.alias_memory_id === 'string' ? alias.alias_memory_id : '';
    if (!canonicalId || !aliasId || !memoryIdSet.has(canonicalId) || !memoryIdSet.has(aliasId)) continue;
    await env.DB.prepare(
      `INSERT INTO memory_entity_aliases (id, brain_id, canonical_memory_id, alias_memory_id, note, confidence, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(brain_id, alias_memory_id) DO UPDATE SET canonical_memory_id = excluded.canonical_memory_id, note = excluded.note, confidence = excluded.confidence, updated_at = excluded.updated_at`
    ).bind(
      generateId(), brainId, canonicalId, aliasId,
      typeof alias.note === 'string' ? alias.note : null,
      clampToRange(alias.confidence, 0.9),
      Math.floor(toFiniteNumber(alias.created_at, ts)), ts
    ).run();
    counts.memory_entity_aliases++;
  }

  for (const raw of watchesPayload) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) continue;
    const watch = raw as Record<string, unknown>;
    const name = typeof watch.name === 'string' && watch.name.trim() ? watch.name.trim() : '';
    const eventTypes = typeof watch.event_types === 'string' ? watch.event_types : '';
    if (!name || !eventTypes) continue;
    await env.DB.prepare(
      `INSERT INTO memory_watches (id, brain_id, name, event_types, query, webhook_url, secret, is_active, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).bind(
      generateId(), brainId, name, eventTypes,
      typeof watch.query === 'string' ? watch.query : null,
      typeof watch.webhook_url === 'string' ? watch.webhook_url : null,
      null,
      watch.is_active === 0 ? 0 : 1,
      Math.floor(toFiniteNumber(watch.created_at, ts)), ts
    ).run();
    counts.memory_watches++;
  }

  if (policyPayload) {
    await setBrainPolicy(env, brainId, policyPayload);
  }

  await logChangelog(env, brainId, 'brain_data_imported', 'brain', brainId, `Imported brain data (${strategy})`, {
    strategy, ...counts,
  });

  return new Response(JSON.stringify({ ok: true, strategy, imported: counts }), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

export async function handleApiPurge(request: Request, env: Env, brainId: string): Promise<Response> {
  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ error: 'Method not allowed' }), {
      status: 405, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    });
  }

  let body: Record<string, unknown>;
  try {
    body = await request.json() as Record<string, unknown>;
  } catch {
    return new Response(JSON.stringify({ error: 'Invalid JSON body' }), {
      status: 400, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    });
  }

  if (body.confirm !== 'PURGE ALL DATA') {
    return new Response(JSON.stringify({ error: 'Confirmation required. Send { "confirm": "PURGE ALL DATA" }.' }), {
      status: 400, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    });
  }

  const existingIds = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? LIMIT 50000').bind(brainId).all<{ id: string }>();
  const memoryCount = existingIds.results.length;
  await safeDeleteMemoryVectors(env, brainId, existingIds.results.map((r) => r.id), 'purge_all');

  const linkCount = (await env.DB.prepare('SELECT COUNT(*) as c FROM memory_links WHERE brain_id = ?').bind(brainId).first<{ c: number }>())?.c ?? 0;

  await env.DB.prepare('DELETE FROM memory_links WHERE brain_id = ?').bind(brainId).run();
  await env.DB.prepare('DELETE FROM memory_entity_aliases WHERE brain_id = ?').bind(brainId).run();
  await env.DB.prepare('DELETE FROM memory_conflict_resolutions WHERE brain_id = ?').bind(brainId).run();
  await env.DB.prepare('DELETE FROM memory_changelog WHERE brain_id = ?').bind(brainId).run();
  await env.DB.prepare('DELETE FROM memory_watches WHERE brain_id = ?').bind(brainId).run();
  await env.DB.prepare('DELETE FROM brain_source_trust WHERE brain_id = ?').bind(brainId).run();
  await env.DB.prepare('DELETE FROM brain_snapshots WHERE brain_id = ?').bind(brainId).run();
  await env.DB.prepare('DELETE FROM brain_policies WHERE brain_id = ?').bind(brainId).run();
  await env.DB.prepare('DELETE FROM memories WHERE brain_id = ?').bind(brainId).run();

  return new Response(JSON.stringify({ ok: true, purged: { memories: memoryCount, links: linkCount } }), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

/* ------------------------------------------------------------------ */
/*  Viewer settings                                                   */
/* ------------------------------------------------------------------ */

// Server-side whitelist for viewer preferences. The client normalizes too,
// but this is the trust boundary: only known keys survive, numbers are
// clamped to the same ranges the UI enforces, and the custom palette is
// reduced to six hex colours plus a vetted font key.
const VIEWER_THEME_NAMES = new Set(['constellation', 'slate', 'paper', 'vanilla', 'midnight', 'solarized', 'ember', 'arctic', 'custom']);
const VIEWER_THEME_MODES = new Set(['auto', 'light', 'dark']);
const VIEWER_FONT_KEYS = new Set(['fraunces', 'grotesk', 'system', 'typewriter']);
const VIEWER_FILTERS = new Set(['', 'note', 'fact', 'journal']);
const VIEWER_HEX_RE = /^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/;
const VIEWER_CUSTOM_DEFAULTS: Record<string, string> = {
  ground: '#181511', ground_2: '#201c16', cream: '#f0e7d5',
  cream_dim: '#b5ab97', butter: '#e3c98f', rule: '#332c22',
};

function clampViewerInt(value: unknown, min: number, max: number, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(Math.round(n), min), max);
}

function asViewerBool(value: unknown, fallback: boolean): boolean {
  return typeof value === 'boolean' ? value : fallback;
}

function sanitizeCustomTheme(raw: unknown): Record<string, string> {
  const src = (raw && typeof raw === 'object') ? raw as Record<string, unknown> : {};
  const out: Record<string, string> = {};
  for (const key of Object.keys(VIEWER_CUSTOM_DEFAULTS)) {
    const v = typeof src[key] === 'string' ? (src[key] as string).trim().toLowerCase() : '';
    out[key] = VIEWER_HEX_RE.test(v) ? v : VIEWER_CUSTOM_DEFAULTS[key];
  }
  out.font = typeof src.font === 'string' && VIEWER_FONT_KEYS.has(src.font) ? src.font : 'fraunces';
  return out;
}

function sanitizeViewerSettings(raw: unknown): Record<string, unknown> {
  const src = (raw && typeof raw === 'object') ? raw as Record<string, unknown> : {};
  const theme = typeof src.theme === 'string' && VIEWER_THEME_NAMES.has(src.theme) ? src.theme : 'constellation';
  const lightTheme = typeof src.light_theme === 'string' && VIEWER_THEME_NAMES.has(src.light_theme) ? src.light_theme : 'paper';
  const mode = typeof src.theme_mode === 'string' && VIEWER_THEME_MODES.has(src.theme_mode) ? src.theme_mode : 'auto';
  const filter = typeof src.default_memory_filter === 'string' && VIEWER_FILTERS.has(src.default_memory_filter) ? src.default_memory_filter : '';
  return {
    theme,
    light_theme: lightTheme,
    theme_mode: mode,
    custom_theme: sanitizeCustomTheme(src.custom_theme),
    live_poll_enabled: asViewerBool(src.live_poll_enabled, true),
    live_poll_interval_sec: clampViewerInt(src.live_poll_interval_sec, 5, 120, 10),
    time_mode: src.time_mode === 'local' ? 'local' : 'utc',
    default_memory_filter: filter,
    search_debounce_ms: clampViewerInt(src.search_debounce_ms, 120, 1500, 300),
    compact_cards: asViewerBool(src.compact_cards, false),
    graph_show_inferred: asViewerBool(src.graph_show_inferred, true),
    graph_show_labels: asViewerBool(src.graph_show_labels, true),
    graph_physics_enabled: asViewerBool(src.graph_physics_enabled, true),
    graph_focus_highlight: asViewerBool(src.graph_focus_highlight, true),
    auto_open_graph: asViewerBool(src.auto_open_graph, false),
    toasts_enabled: asViewerBool(src.toasts_enabled, true),
    toast_duration_ms: clampViewerInt(src.toast_duration_ms, 1200, 8000, 2300),
    confirm_logout: asViewerBool(src.confirm_logout, false),
    show_scanlines: asViewerBool(src.show_scanlines, true),
    reduce_motion: asViewerBool(src.reduce_motion, false),
    semantic_reindex_wait_for_index: asViewerBool(src.semantic_reindex_wait_for_index, true),
    semantic_reindex_wait_timeout_seconds: clampViewerInt(src.semantic_reindex_wait_timeout_seconds, 1, 900, 180),
    semantic_reindex_limit: clampViewerInt(src.semantic_reindex_limit, 1, 2000, 500),
  };
}

export async function handleApiViewerSettings(request: Request, env: Env, brainId: string): Promise<Response> {
  const jsonHeaders = { ...CORS_HEADERS, 'Content-Type': 'application/json' };
  if (request.method === 'GET') {
    const stored = await getViewerSettings(env, brainId);
    const settings = stored ? sanitizeViewerSettings(stored) : null;
    return new Response(JSON.stringify({ settings }), { headers: jsonHeaders });
  }
  if (request.method === 'PUT') {
    let body: Record<string, unknown>;
    try {
      body = await request.json() as Record<string, unknown>;
    } catch {
      return new Response(JSON.stringify({ error: 'Invalid JSON body' }), { status: 400, headers: jsonHeaders });
    }
    const sanitized = sanitizeViewerSettings(body && typeof body === 'object' ? body.settings : null);
    await setViewerSettings(env, brainId, sanitized);
    return new Response(JSON.stringify({ settings: sanitized }), { headers: jsonHeaders });
  }
  return new Response(JSON.stringify({ error: 'Method not allowed' }), { status: 405, headers: jsonHeaders });
}



// Served at /landing.js (the page CSP forbids inline scripts). Builds the
// per-letter blur-up headline, wires scroll reveals via one IntersectionObserver,
// and runs the FAQ accordion. prefers-reduced-motion is handled by the page CSS
// plus the early returns here.
export const landingScript = `(function(){
  var reduce = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  // Scroll reveals: add .in once, then stop watching.
  (function(){
    var els = document.querySelectorAll('.reveal');
    if (reduce || !('IntersectionObserver' in window)) {
      for (var i = 0; i < els.length; i++) els[i].classList.add('in');
      return;
    }
    var io = new IntersectionObserver(function(entries){
      entries.forEach(function(e){
        if (e.isIntersecting) { e.target.classList.add('in'); io.unobserve(e.target); }
      });
    }, { threshold: 0.12, rootMargin: '0px 0px -8% 0px' });
    for (var j = 0; j < els.length; j++) io.observe(els[j]);
  })();

  // FAQ accordion.
  (function(){
    var items = document.querySelectorAll('.faq-item');
    for (var k = 0; k < items.length; k++) {
      (function(item){
        var q = item.querySelector('.faq-q');
        if (!q) return;
        q.addEventListener('click', function(){
          var open = item.classList.toggle('open');
          q.setAttribute('aria-expanded', open ? 'true' : 'false');
        });
      })(items[k]);
    }
  })();

  // Mobile nav: hamburger toggles the dropdown sheet; tapping a link closes it.
  (function(){
    var m = document.getElementById('menu');
    var sh = document.getElementById('sheet');
    if (!m || !sh) return;
    m.addEventListener('click', function(){
      var open = sh.classList.toggle('open');
      m.classList.toggle('open', open);
      m.setAttribute('aria-expanded', open ? 'true' : 'false');
    });
    var links = sh.querySelectorAll('a');
    for (var i = 0; i < links.length; i++) {
      links[i].addEventListener('click', function(){
        sh.classList.remove('open');
        m.classList.remove('open');
        m.setAttribute('aria-expanded', 'false');
      });
    }
  })();
})();`;

export function rootLandingHtml(url: URL): string {
  const origin = url.origin;
  const deploy = `${origin}/deploy`;
  const docs = `${origin}/docs`;
  const repo = 'https://github.com/guirguispierre/memoryvault';
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MemoryVault, open-source memory for AI agents</title>
<meta name="description" content="MemoryVault gives any AI agent a memory it actually keeps. Open source, self-hosted on your own Cloudflare account, nothing paywalled.">
${FONT_LINK_TAGS}
${constellationHeadTags}
<style>
${constellationTokensCss}  * { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; }
  body { font-family: var(--ui); background: var(--bg); color: var(--ink); -webkit-font-smoothing: antialiased; line-height: 1.5; overflow-x: hidden; }
  a { color: inherit; text-decoration: none; }
  .container { max-width: 1080px; margin: 0 auto; padding: 0 32px; }

  /* ── HERO (living memory graph) ── */
  /* The canvas overflows past the hero and its lower edge is masked off, so the
     stars thin out below the hero/section boundary instead of stopping on it. */
  .hero-wrap { position: relative; height: 100vh; overflow: visible; }
  #sky {
    position: absolute; top: 0; left: 0; width: 100%; height: 135vh;
    z-index: 0; display: block; pointer-events: none;
    -webkit-mask-image: linear-gradient(180deg, #000 0%, #000 60%, transparent 88%);
            mask-image: linear-gradient(180deg, #000 0%, #000 60%, transparent 88%);
  }
  .wrap { position: relative; z-index: 1; }

  nav { display: flex; align-items: center; gap: 24px; max-width: 1120px; margin: 0 auto; padding: 24px 32px; position: relative; z-index: 3; }
  .brand { font-family: var(--doc); font-size: 18px; font-weight: 600; color: var(--ink); }
  nav .links { display: flex; gap: 22px; margin-left: 14px; }
  nav .links a { font-size: 14px; color: var(--dim); }
  nav .links a:hover { color: var(--ink); }
  nav .r { margin-left: auto; display: flex; gap: 12px; }

  .btn { font-family: var(--ui); font-weight: 600; font-size: 14px; border-radius: 9px; padding: 10px 18px; border: 1px solid var(--accent); background: var(--accent); color: #070810; cursor: pointer; display: inline-flex; align-items: center; gap: 8px; transition: transform 0.15s, box-shadow 0.15s; }
  .btn:hover { transform: translateY(-1px); box-shadow: 0 10px 30px rgba(138, 176, 255, 0.3); }
  .btn.g { background: rgba(255, 255, 255, 0.05); color: var(--ink); border-color: rgba(255, 255, 255, 0.18); }
  :focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }

  .hero { max-width: 1120px; margin: 0 auto; padding: 14vh 32px 0; text-align: center; position: relative; z-index: 2; }
  .eyebrow { font-weight: 600; font-size: 13px; color: var(--accent); margin-bottom: 22px; }
  .hero h1 { font-family: var(--doc); font-weight: 500; font-size: 72px; line-height: 1.04; letter-spacing: -0.02em; max-width: 14ch; margin: 0 auto; color: var(--ink); text-shadow: 0 2px 36px rgba(8, 12, 28, 0.55); }
  .hero h1 em { font-style: italic; color: var(--accent); }
  .hero .sub { font-size: 19px; color: var(--dim); max-width: 44ch; margin: 26px auto 0; line-height: 1.55; }
  .cta { margin-top: 32px; display: flex; gap: 14px; justify-content: center; }

  /* ── sections (one continuous space, no dividers) ── */
  main { position: relative; z-index: 1; background: var(--bg); }
  main::before { content: ""; position: absolute; inset: 0; z-index: -1; pointer-events: none; background: radial-gradient(70% 40% at 50% 0%, rgba(40, 52, 110, 0.18), transparent 55%), radial-gradient(60% 50% at 85% 88%, rgba(30, 80, 70, 0.10), transparent 60%); }
  /* Ease the background tone across a tall band that starts up in the hero and
     finishes inside the first section, on a different row than the star fade. */
  main::after { content: ""; position: absolute; top: -40vh; left: 0; right: 0; height: 60vh; z-index: 0; pointer-events: none; background: linear-gradient(180deg, transparent 0%, var(--bg) 70%); }
  main > * { position: relative; z-index: 1; }
  .reveal { opacity: 0; transform: translateY(18px); transition: opacity 0.7s cubic-bezier(0.2,0.7,0.2,1), transform 0.7s cubic-bezier(0.2,0.7,0.2,1); }
  .reveal.in { opacity: 1; transform: none; }

  .feature { padding: 90px 0; }
  .fg { display: grid; grid-template-columns: 1fr 1fr; gap: 56px; align-items: center; }
  .feature h2 { font-family: var(--doc); font-weight: 500; font-size: 36px; letter-spacing: -0.02em; line-height: 1.14; margin-bottom: 16px; color: var(--ink); }
  .feature h2 em { font-style: italic; color: var(--accent); }
  .feature p { font-size: 16.5px; color: var(--dim); line-height: 1.6; margin-bottom: 12px; }
  .feature .mini { color: var(--faint); font-size: 14px; }
  .panel { background: linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0)), var(--surface); border: 1px solid var(--rule); border-radius: 18px; padding: 28px; box-shadow: 0 30px 80px rgba(0,0,0,0.45), inset 0 1px 0 rgba(255,255,255,0.05); }
  /* genuine config / identifiers are the only monospace on the page */
  .codeblock { font-family: var(--mono); font-size: 13px; line-height: 2; color: var(--dim); }
  .codeblock .cm { color: var(--faint); }
  .codeblock .c1 { color: var(--accent); }
  .codeblock .s { color: var(--ink); }
  .codeblock .ok { color: var(--good); }
  .ll { display: flex; align-items: baseline; gap: 10px; padding: 11px 0; border-bottom: 1px solid var(--rule); font-size: 14px; }
  .ll:last-child { border: none; }
  .ll .k { color: var(--accent); font-family: var(--mono); font-size: 12.5px; }
  .ll .arr { color: var(--faint); }
  .ll .v { color: var(--ink); }
  .ll .tag { margin-left: auto; font-size: 10px; letter-spacing: 0.04em; text-transform: uppercase; color: var(--good); border: 1px solid rgba(134,224,184,0.4); border-radius: 5px; padding: 2px 7px; }

  .how { padding: 90px 0; text-align: center; }
  .how h2 { font-family: var(--doc); font-weight: 500; font-size: 40px; letter-spacing: -0.02em; margin-bottom: 12px; color: var(--ink); }
  .how > p { color: var(--dim); max-width: 46ch; margin: 0 auto 50px; font-size: 17px; }
  .steps { display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; text-align: left; }
  .step { background: var(--surface); border: 1px solid var(--rule); border-radius: 16px; padding: 28px; transition: transform 0.2s, border-color 0.2s; }
  .step:hover { transform: translateY(-3px); border-color: rgba(138,176,255,0.35); }
  .step .n { font-family: var(--doc); font-style: italic; font-size: 15px; color: var(--accent); margin-bottom: 12px; }
  .step h3 { font-family: var(--doc); font-weight: 500; font-size: 20px; margin-bottom: 8px; color: var(--ink); }
  .step p { font-size: 14.5px; color: var(--dim); line-height: 1.55; }

  .pricing { padding: 90px 0; text-align: center; }
  .pricing h2 { font-family: var(--doc); font-weight: 500; font-size: 40px; letter-spacing: -0.02em; margin-bottom: 12px; color: var(--ink); }
  .pricing > p { color: var(--dim); max-width: 46ch; margin: 0 auto 50px; font-size: 17px; }
  .plans { display: grid; grid-template-columns: 1fr 1fr; gap: 22px; max-width: 760px; margin: 0 auto; text-align: left; }
  .plan { background: var(--surface); border: 1px solid var(--rule); border-radius: 18px; padding: 30px; display: flex; flex-direction: column; }
  .plan.feat { border-color: rgba(138,176,255,0.5); box-shadow: 0 0 0 1px rgba(138,176,255,0.2), 0 30px 80px rgba(0,0,0,0.4); }
  .plan .pname { font-family: var(--doc); font-size: 22px; font-weight: 500; margin-bottom: 4px; color: var(--ink); }
  .plan .price { font-family: var(--doc); font-size: 36px; margin-bottom: 6px; color: var(--ink); }
  .plan .price small { font-family: var(--ui); font-size: 14px; color: var(--faint); }
  .plan .desc { font-size: 14.5px; color: var(--dim); margin-bottom: 18px; line-height: 1.5; }
  .plan ul { list-style: none; margin-bottom: 22px; display: grid; gap: 9px; }
  .plan li { font-size: 14px; color: var(--dim); padding-left: 22px; position: relative; }
  .plan li::before { content: "\\2713"; position: absolute; left: 0; color: var(--good); }
  .plan .pcta { margin-top: auto; display: block; text-align: center; border-radius: 9px; padding: 11px 16px; font-family: var(--ui); font-weight: 600; font-size: 14px; border: 1px solid var(--rule); color: var(--dim); }
  .plan .pcta.primary { background: var(--accent); border-color: var(--accent); color: #070810; }
  .plan .note { font-size: 12px; color: var(--faint); text-align: center; margin-top: 10px; line-height: 1.4; }

  .faq { padding: 90px 0; }
  .faq h2 { font-family: var(--doc); font-weight: 500; font-size: 40px; letter-spacing: -0.02em; margin-bottom: 28px; text-align: center; color: var(--ink); }
  .qa { max-width: 760px; margin: 0 auto; }
  .faq-item { border-bottom: 1px solid var(--rule); }
  .faq-q { width: 100%; text-align: left; background: none; border: none; cursor: pointer; display: flex; align-items: center; gap: 12px; padding: 22px 0; font-family: var(--doc); font-size: 19px; font-weight: 500; color: var(--ink); }
  .faq-q .chev { margin-left: auto; flex-shrink: 0; color: var(--faint); transition: transform 0.25s; }
  .faq-item.open .faq-q .chev { transform: rotate(180deg); }
  .faq-a-wrap { display: grid; grid-template-rows: 0fr; transition: grid-template-rows 0.3s ease; }
  .faq-item.open .faq-a-wrap { grid-template-rows: 1fr; }
  .faq-a-inner { overflow: hidden; }
  .faq-a { padding: 0 0 22px; font-size: 15.5px; color: var(--dim); line-height: 1.6; max-width: 64ch; }

  /* glow bleeds beyond the section so there is no hard cutoff band */
  .final { padding: 130px 32px 120px; text-align: center; position: relative; overflow: visible; }
  .final::before { content: ""; position: absolute; left: 0; right: 0; top: -340px; bottom: -120px; background: radial-gradient(50% 60% at 50% 50%, rgba(138,176,255,0.14), transparent 70%); pointer-events: none; }
  .final h2 { position: relative; font-family: var(--doc); font-weight: 500; font-size: 46px; letter-spacing: -0.02em; max-width: 18ch; margin: 0 auto 26px; line-height: 1.08; color: var(--ink); }
  .final h2 em { font-style: italic; color: var(--accent); }
  .final .cta { position: relative; }

  footer { border-top: 1px solid var(--rule); padding: 36px 0; color: var(--faint); font-size: 13px; }
  .fi { max-width: 1080px; margin: 0 auto; padding: 0 32px; display: flex; justify-content: space-between; align-items: center; gap: 16px; flex-wrap: wrap; }
  .fi a:hover { color: var(--accent); }

  /* mobile nav: hamburger + dropdown sheet */
  .menu { display: none; margin-left: auto; width: 40px; height: 40px; border: 1px solid var(--rule); border-radius: 10px; background: rgba(255,255,255,0.04); cursor: pointer; flex-direction: column; gap: 4px; align-items: center; justify-content: center; }
  .menu span { display: block; width: 18px; height: 1.5px; background: var(--ink); border-radius: 2px; transition: 0.2s; }
  .menu.open span:nth-child(1) { transform: translateY(5.5px) rotate(45deg); }
  .menu.open span:nth-child(2) { opacity: 0; }
  .menu.open span:nth-child(3) { transform: translateY(-5.5px) rotate(-45deg); }
  .sheet { display: none; position: fixed; top: 70px; left: 16px; right: 16px; z-index: 60; background: rgba(13,15,26,0.96); backdrop-filter: blur(14px); border: 1px solid var(--rule); border-radius: 16px; padding: 10px; flex-direction: column; box-shadow: 0 30px 80px rgba(0,0,0,0.6); }
  .sheet.open { display: flex; }
  .sheet a { padding: 14px 16px; color: var(--ink); font-size: 16px; border-radius: 10px; }
  .sheet a:active { background: rgba(255,255,255,0.06); }
  .sheet a.primary { background: var(--accent); color: #070810; font-weight: 600; text-align: center; margin-top: 4px; }

  @media (max-width: 760px) {
    h1 { font-size: 38px; line-height: 1.06; }
    .hero { padding: 13vh 22px 0; }
    .hero .sub { font-size: 17px; margin-top: 20px; }
    .cta { flex-direction: column; gap: 12px; align-items: center; }
    .cta .btn { width: 100%; max-width: 300px; justify-content: center; }
    nav .links, nav .r { display: none; }
    .menu { display: flex; }
    .container { padding: 0 22px; }
    .fg { grid-template-columns: 1fr; gap: 26px; }
    .steps, .plans { grid-template-columns: 1fr; }
    .feature, .how, .pricing, .faq { padding: 64px 0; }
    .feature h2, .how h2, .pricing h2, .faq h2, .final h2 { font-size: 30px; }
    .final { padding: 90px 22px 100px; }
  }
  @media (prefers-reduced-motion: reduce) {
    html { scroll-behavior: auto; }
    .reveal { opacity: 1 !important; transform: none !important; transition: none !important; }
    .faq-a-wrap, .faq-q .chev, .step, .btn { transition: none !important; }
  }
</style>
<noscript><style>.reveal { opacity: 1; transform: none; }</style></noscript>
</head>
<body>
  <div class="space"></div>
  <div class="hero-wrap">
    <canvas id="sky" class="sky" aria-hidden="true"></canvas>
    <div class="wrap">
      <nav>
        <span class="brand">MemoryVault</span>
        <div class="links"><a href="#how">How it works</a><a href="#pricing">Pricing</a><a href="${docs}">Docs</a></div>
        <div class="r"><a class="btn g" href="${repo}" target="_blank" rel="noopener">GitHub</a><a class="btn" href="${deploy}">Deploy free</a></div>
        <button class="menu" id="menu" type="button" aria-label="Menu" aria-expanded="false"><span></span><span></span><span></span></button>
      </nav>
      <div class="sheet" id="sheet">
        <a href="#how">How it works</a><a href="#pricing">Pricing</a><a href="${docs}">Docs</a><a href="${repo}" target="_blank" rel="noopener">GitHub</a><a class="primary" href="${deploy}">Deploy free</a>
      </div>
      <section class="hero">
        <div class="eyebrow">Open-source memory for AI agents</div>
        <h1>Your agents forget everything. <em>Fix that.</em></h1>
        <p class="sub">MemoryVault gives any AI agent a memory it actually keeps. It learns what matters, drops what doesn't, and runs entirely on servers you own.</p>
        <div class="cta">
          <a class="btn" href="${deploy}">Deploy your own, free</a>
          <a class="btn g" href="${repo}" target="_blank" rel="noopener">View on GitHub</a>
        </div>
      </section>
    </div>
  </div>

  <main>
    <section class="feature container">
      <div class="fg">
        <div class="reveal">
          <h2>A graph of memory, <em>nothing paywalled</em></h2>
          <p>Most tools lock the graph behind a Pro tier, or keep it closed entirely. MemoryVault keeps it open from the first commit. Links, traversal, history, all of it.</p>
          <p class="mini">Notes, facts, and journal entries. Connected, weighted, searchable.</p>
        </div>
        <div class="panel reveal">
          <div class="ll"><span class="k">project.license</span><span class="arr">&rarr;</span><span class="v">MIT, fully open</span><span class="tag">verified</span></div>
          <div class="ll"><span class="k">user.timezone</span><span class="arr">&rarr;</span><span class="v">Europe/Lisbon</span><span class="tag">verified</span></div>
          <div class="ll"><span class="k">graph.enabled</span><span class="arr">&rarr;</span><span class="v">true. no upsell</span></div>
        </div>
      </div>
    </section>

    <section class="feature container">
      <div class="fg">
        <div class="panel reveal">
          <div class="codeblock">
            <div class="cm"># wrangler.toml</div>
            <div><span class="c1">[[vectorize]]</span></div>
            <div><span class="s">binding = "MEMORY_INDEX"</span></div>
            <div><span class="c1">[[d1_databases]]</span></div>
            <div><span class="s">database_name = "your-brain"</span></div>
            <div class="ok">&#10003; nothing leaves your Cloudflare account</div>
          </div>
        </div>
        <div class="reveal">
          <h2>Self-hosted. <em>You own the data.</em></h2>
          <p>Deploy to your own Cloudflare account in a few minutes. There's no server of ours in the middle. Your agent's memory lives under your keys, not ours.</p>
          <p class="mini">One command. Runs on Workers, D1, and Vectorize.</p>
        </div>
      </div>
    </section>

    <section class="how container" id="how">
      <h2 class="reveal">How it works</h2>
      <p class="reveal">Connect once. Your agents read your context before they answer, and write back what they learn.</p>
      <div class="steps">
        <div class="step reveal"><div class="n">First</div><h3>Deploy your vault</h3><p>One command stands up your own memory server on Cloudflare, under your account.</p></div>
        <div class="step reveal"><div class="n">Then</div><h3>Connect your agents</h3><p>Point Claude, Codex, or any client at it. They read before they answer, then write back after.</p></div>
        <div class="step reveal"><div class="n">Over time</div><h3>It gets sharper</h3><p>What you use is reinforced. What you don't fades. You stay in control of what sticks.</p></div>
      </div>
    </section>

    <section class="pricing container" id="pricing">
      <h2 class="reveal">Start free. Host it yourself, or let us.</h2>
      <p class="reveal">The whole thing is open source. You only pay if you want us to run it for you.</p>
      <div class="plans">
        <div class="plan reveal">
          <div class="pname">Self-host</div>
          <div class="price">$0</div>
          <div class="desc">Run it on your own Cloudflare account, forever.</div>
          <ul><li>Full source, MIT licensed</li><li>Graph and all features</li><li>Your data, your keys</li><li>Community support</li></ul>
          <a class="pcta" href="${deploy}">Deploy it yourself</a>
        </div>
        <div class="plan feat reveal">
          <div class="pname">Hosted</div>
          <div class="price">$12<small>/mo</small></div>
          <div class="desc">We run it for you. Backups, sync, and support.</div>
          <ul><li>Everything in Self-host</li><li>Managed multi-device sync</li><li>Automatic backups</li><li>Priority support</li></ul>
          <a class="pcta primary" href="${deploy}">Join the waitlist</a>
          <div class="note">Not billable yet. The GitHub repo is where to follow along.</div>
        </div>
      </div>
    </section>

    <section class="faq container" id="faq">
      <h2 class="reveal">Questions</h2>
      <div class="qa reveal">
        <div class="faq-item">
          <button class="faq-q" type="button" aria-expanded="false">Is it really fully open source?<svg class="chev" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg></button>
          <div class="faq-a-wrap"><div class="faq-a-inner"><div class="faq-a">Yes. MIT licensed, graph and all. There is no paywalled tier of the software itself. The only thing you would ever pay for is optional managed hosting.</div></div></div>
        </div>
        <div class="faq-item">
          <button class="faq-q" type="button" aria-expanded="false">Where does my data live?<svg class="chev" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg></button>
          <div class="faq-a-wrap"><div class="faq-a-inner"><div class="faq-a">In your own Cloudflare account. Your D1 database and Vectorize index, under your keys. If you self-host, your memories never touch our infrastructure.</div></div></div>
        </div>
        <div class="faq-item">
          <button class="faq-q" type="button" aria-expanded="false">Which agents does it work with?<svg class="chev" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg></button>
          <div class="faq-a-wrap"><div class="faq-a-inner"><div class="faq-a">Anything that speaks MCP. Claude, Claude Code, Codex, and custom agents, plus a plain REST API for everything else.</div></div></div>
        </div>
        <div class="faq-item">
          <button class="faq-q" type="button" aria-expanded="false">How is it different from other memory tools?<svg class="chev" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg></button>
          <div class="faq-a-wrap"><div class="faq-a-inner"><div class="faq-a">It is graph-aware and open with nothing paywalled, self-hosted so you own the data, and its tenant isolation is covered by an adversarial test suite, not asserted in a blog post.</div></div></div>
        </div>
      </div>
    </section>

    <section class="final">
      <h2>Give your agents a memory <em>you control</em></h2>
      <div class="cta">
        <a class="btn" href="${deploy}">Deploy your own, free</a>
        <a class="btn g" href="${docs}">Read the docs</a>
      </div>
    </section>

    <footer><div class="fi"><span>&copy; 2026 MemoryVault &middot; MIT licensed</span><span>built on Cloudflare</span></div></footer>
  </main>

  <script src="/starfield.js" defer></script>
  <script src="/landing.js"></script>
</body>
</html>`;
}

// Client script for the /deploy waitlist form: progressive enhancement over a
// native POST, so it submits with or without JS. Served from /deploy.js because
// the page CSP forbids inline scripts.
export const deployScript = `(function(){
  var form = document.getElementById('wl-form');
  if (!form) return;
  var status = document.getElementById('wl-status');
  form.addEventListener('submit', function(e){
    e.preventDefault();
    var input = form.querySelector('input[name="email"]');
    var btn = form.querySelector('button[type="submit"]');
    var email = (input.value || '').trim();
    if (!email) { input.focus(); return; }
    var prev = btn.textContent;
    btn.disabled = true; btn.textContent = 'Joining...';
    function fail(msg){ btn.disabled = false; btn.textContent = prev; status.textContent = msg; status.className = 'wl-status err'; }
    fetch('/api/waitlist', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
      body: JSON.stringify({ email: email })
    }).then(function(r){ return r.json().then(function(d){ return { ok: r.ok, d: d }; }); })
      .then(function(res){
        if (res.ok) {
          form.style.display = 'none';
          status.textContent = "You're on the list. We'll email you when hosting opens.";
          status.className = 'wl-status ok';
        } else {
          fail((res.d && res.d.error) || 'Something went wrong. Please try again.');
        }
      })
      .catch(function(){ fail('Network error. Please try again.'); });
  });
})();`;

// Minimal honest success page for a no-JS native form POST to /api/waitlist.
function waitlistConfirmationHtml(origin: string): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>You're on the list · MemoryVault</title>
${FONT_LINK_TAGS}
${constellationHeadTags}
<style>
${constellationTokensCss}  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: var(--ui); background: var(--bg); color: var(--ink); min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 2rem; text-align: center; }
  .card { max-width: 460px; }
  h1 { font-family: var(--doc); font-weight: 500; font-size: 30px; margin-bottom: 12px; }
  p { color: var(--dim); font-size: 16px; line-height: 1.6; margin-bottom: 22px; }
  a { color: var(--bg); background: var(--accent); font-weight: 600; font-size: 14px; padding: 11px 20px; border-radius: 9px; text-decoration: none; display: inline-block; }
</style>
</head>
<body>
  <div class="card">
    <h1>You're on the list.</h1>
    <p>We'll email you when hosting opens. In the meantime you can run MemoryVault yourself, free, on your own Cloudflare account.</p>
    <a href="${origin}/deploy">Back to deploy options</a>
  </div>
</body>
</html>`;
}

// Unauthenticated, write-only waitlist capture for the hosted tier. Accepts JSON
// (the enhanced form) or urlencoded (the no-JS fallback); validates the email
// shape and dedupes on the address. No read endpoint is exposed publicly.
export async function handleApiWaitlist(request: Request, env: Env, origin: string): Promise<Response> {
  if (request.method !== 'POST') {
    return corsJsonResponse({ error: 'Method not allowed' }, 405);
  }
  const contentType = request.headers.get('Content-Type') || '';
  const isJson = contentType.includes('application/json');
  let email = '';
  try {
    if (isJson) {
      const body = (await request.json()) as { email?: unknown };
      email = typeof body.email === 'string' ? body.email : '';
    } else {
      const form = await request.formData();
      const value = form.get('email');
      email = typeof value === 'string' ? value : '';
    }
  } catch {
    return corsJsonResponse({ error: 'Invalid request body' }, 400);
  }
  email = email.trim().toLowerCase();
  const valid = email.length >= 3 && email.length <= 254 && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  if (!valid) {
    return corsJsonResponse({ error: 'Please enter a valid email address.' }, 400);
  }
  const outcome = await insertWaitlistEmail(env, email);
  // A no-JS native form post wants a page back, not JSON.
  const wantsHtml = !isJson && (request.headers.get('Accept') || '').includes('text/html');
  if (wantsHtml) {
    return new Response(waitlistConfirmationHtml(origin), {
      headers: { 'Content-Type': 'text/html; charset=utf-8' },
    });
  }
  return corsJsonResponse({ ok: true, status: outcome });
}

// The deploy decision page: two honest paths, hosted featured and recommended,
// self-host kept genuinely free and complete. Hosted's action is an email
// waitlist (billing is not live yet); self-host offers the Cloudflare one-click
// button plus an accurate CLI quickstart.
export function deployHtml(url: URL): string {
  const origin = url.origin;
  const repo = 'https://github.com/guirguispierre/memoryvault';
  const cfDeploy = `https://deploy.workers.cloudflare.com/?url=${repo}`;
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Deploy MemoryVault</title>
<meta name="description" content="Two ways to run MemoryVault: let us host it for you, or deploy the open-source code to your own Cloudflare account, free.">
${FONT_LINK_TAGS}
${constellationHeadTags}
<style>
${constellationTokensCss}  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: var(--ui); background: var(--bg); color: var(--ink); -webkit-font-smoothing: antialiased; line-height: 1.5; overflow-x: hidden; }
  a { color: inherit; text-decoration: none; }
  .container { max-width: 1080px; margin: 0 auto; padding: 0 32px; position: relative; z-index: 1; }

  nav { display: flex; align-items: center; gap: 22px; max-width: 1120px; margin: 0 auto; padding: 24px 32px; position: relative; z-index: 1; }
  .brand { font-family: var(--doc); font-size: 18px; font-weight: 600; color: var(--ink); }
  nav .r { margin-left: auto; display: flex; gap: 12px; align-items: center; }
  nav .back { font-size: 14px; color: var(--dim); }
  nav .back:hover { color: var(--ink); }
  .btn { font-family: var(--ui); font-weight: 600; font-size: 14px; border-radius: 9px; padding: 10px 18px; border: 1px solid var(--accent); background: var(--accent); color: #070810; cursor: pointer; display: inline-flex; align-items: center; gap: 8px; transition: transform 0.15s, box-shadow 0.15s; }
  .btn:hover { transform: translateY(-1px); box-shadow: 0 10px 30px rgba(138, 176, 255, 0.3); }
  .btn.g { background: rgba(255, 255, 255, 0.05); color: var(--ink); border-color: rgba(255, 255, 255, 0.18); }
  :focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }

  .intro { text-align: center; padding: 6vh 32px 2vh; max-width: 720px; margin: 0 auto; position: relative; z-index: 1; }
  .intro h1 { font-family: var(--doc); font-weight: 500; font-size: 46px; letter-spacing: -0.02em; line-height: 1.06; margin-bottom: 14px; }
  .intro p { font-size: 18px; color: var(--dim); line-height: 1.55; }

  .paths { display: grid; grid-template-columns: minmax(0, 1.12fr) minmax(0, 0.88fr); gap: 24px; align-items: start; padding: 30px 0 24px; }
  .card { background: var(--surface); border: 1px solid var(--rule); border-radius: 20px; padding: 32px; position: relative; }
  .card.hosted { border-color: rgba(138, 176, 255, 0.55); background: linear-gradient(180deg, rgba(138, 176, 255, 0.06), rgba(255, 255, 255, 0)), var(--surface); box-shadow: 0 30px 90px rgba(0, 0, 0, 0.45), 0 0 0 1px rgba(138, 176, 255, 0.18); }
  .badge { display: inline-block; font-size: 11px; font-weight: 600; letter-spacing: 0.06em; text-transform: uppercase; color: var(--accent); border: 1px solid rgba(138, 176, 255, 0.4); border-radius: 999px; padding: 4px 11px; margin-bottom: 16px; }
  .badge.free { color: var(--good); border-color: rgba(134, 224, 184, 0.4); }
  .card h2 { font-family: var(--doc); font-weight: 500; letter-spacing: -0.01em; margin-bottom: 6px; }
  .card.hosted h2 { font-size: 30px; }
  .card.self h2 { font-size: 25px; }
  .price { font-family: var(--doc); font-size: 30px; margin-bottom: 16px; }
  .price small { font-family: var(--ui); font-size: 14px; color: var(--faint); }
  .lede { color: var(--dim); font-size: 15.5px; line-height: 1.6; margin-bottom: 18px; }
  ul { list-style: none; display: grid; gap: 10px; margin-bottom: 24px; }
  li { font-size: 14.5px; color: var(--dim); padding-left: 24px; position: relative; line-height: 1.5; }
  li::before { content: "\\2713"; position: absolute; left: 0; color: var(--accent); }
  .card.self li::before { color: var(--good); }

  .wl { display: flex; gap: 10px; flex-wrap: wrap; }
  .wl input { flex: 1; min-width: 180px; background: var(--bg2); border: 1px solid var(--rule); color: var(--ink); border-radius: 9px; padding: 12px 14px; font-family: var(--ui); font-size: 15px; outline: none; transition: border-color 0.15s; }
  .wl input::placeholder { color: var(--faint); }
  .wl input:focus { border-color: var(--accent); }
  .wl button { font-size: 15px; padding: 12px 20px; }
  .wl-status { margin-top: 12px; font-size: 14px; min-height: 1em; }
  .wl-status.ok { color: var(--good); }
  .wl-status.err { color: var(--warm); }
  .micro { margin-top: 12px; font-size: 12.5px; color: var(--faint); line-height: 1.5; }

  .cf { display: inline-flex; align-items: center; gap: 8px; width: 100%; justify-content: center; margin-bottom: 18px; }
  .or { text-align: center; font-size: 12px; letter-spacing: 0.08em; text-transform: uppercase; color: var(--faint); margin-bottom: 14px; }
  .code { font-family: var(--mono); font-size: 12.5px; line-height: 1.85; color: var(--dim); background: var(--bg2); border: 1px solid var(--rule); border-radius: 12px; padding: 16px 18px; overflow-x: auto; white-space: pre; }
  .code .cm { color: var(--faint); }
  .code .c1 { color: var(--accent); }

  .compare { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; max-width: 760px; margin: 8px auto 0; padding: 24px 0 0; border-top: 1px solid var(--rule); }
  .compare div { font-size: 14px; color: var(--dim); line-height: 1.55; }
  .compare strong { color: var(--ink); font-weight: 600; display: block; margin-bottom: 4px; }

  footer { border-top: 1px solid var(--rule); padding: 36px 0; margin-top: 50px; color: var(--faint); font-size: 13px; }
  .fi { max-width: 1080px; margin: 0 auto; padding: 0 32px; display: flex; justify-content: space-between; align-items: center; gap: 16px; flex-wrap: wrap; }

  @media (max-width: 760px) {
    .intro h1 { font-size: 32px; }
    .paths { grid-template-columns: 1fr; }
    .compare { grid-template-columns: 1fr; gap: 16px; }
    .container { padding: 0 20px; }
  }
</style>
</head>
<body>
  ${constellationCalmField}
  <nav>
    <a class="brand" href="${origin}/">MemoryVault</a>
    <div class="r"><a class="back" href="${origin}/">Back to home</a><a class="btn g" href="${repo}" target="_blank" rel="noopener">View on GitHub</a></div>
  </nav>

  <section class="intro">
    <h1>Deploy MemoryVault</h1>
    <p>Two honest ways to run it. Let us host it for you, or deploy the open-source code to your own Cloudflare account. Same software, nothing paywalled either way.</p>
  </section>

  <div class="container">
    <div class="paths">
      <div class="card hosted">
        <span class="badge">Recommended</span>
        <h2>Let us host it for you</h2>
        <div class="price">$12<small>/mo</small></div>
        <p class="lede">The fastest path. We run your instance, keep it updated, and back it up. No Cloudflare account, no CLI, no setup.</p>
        <ul>
          <li>Ready in seconds, nothing to configure</li>
          <li>Managed multi-device sync</li>
          <li>Automatic backups</li>
          <li>Priority support</li>
          <li>No Cloudflare account or CLI needed</li>
        </ul>
        <form class="wl" id="wl-form" method="post" action="/api/waitlist">
          <input type="email" name="email" placeholder="you@example.com" autocomplete="email" required aria-label="Email address">
          <button class="btn" type="submit">Join the waitlist</button>
        </form>
        <div class="wl-status" id="wl-status" role="status" aria-live="polite"></div>
        <p class="micro">Hosting is not open yet, so this is a waitlist, not a checkout. Join and we'll email you the moment it opens. You can self-host today in the meantime.</p>
      </div>

      <div class="card self">
        <span class="badge free">Free forever</span>
        <h2>Run it yourself</h2>
        <div class="price">$0</div>
        <p class="lede">Deploy the open-source code to your own Cloudflare account. Your data lives under your keys. Graph and every feature included.</p>
        <ul>
          <li>Full source, MIT licensed</li>
          <li>Your data, your keys, your account</li>
          <li>Nothing paywalled</li>
        </ul>
        <a class="btn cf" href="${cfDeploy}" target="_blank" rel="noopener">Deploy to Cloudflare</a>
        <div class="or">or deploy from your terminal</div>
        <div class="code"><span class="cm"># clone, install, and point wrangler.toml at your own resources</span>
git clone ${repo}.git
cd memoryvault
npm install
npx wrangler d1 create ai-memory
npx wrangler kv namespace create RATE_LIMIT_KV
npx wrangler vectorize create ai-memory-semantic-v1 --dimensions=768 --metric=cosine
<span class="cm"># set AUTH_SECRET, apply the schema, then deploy</span>
npx wrangler secret put AUTH_SECRET
npx wrangler d1 execute ai-memory --remote --file=schema.sql
npm run deploy</div>
      </div>
    </div>

    <div class="compare">
      <div><strong>Hosted</strong>We manage it. Updates, backups, and sync are handled for you, so you can point an agent at it and forget the infrastructure.</div>
      <div><strong>Self-host</strong>You manage it. Full control on your own Cloudflare account, your keys, your bill. The same graph and features, no upsell.</div>
    </div>
  </div>

  <footer><div class="fi"><span>&copy; 2026 MemoryVault &middot; MIT licensed</span><span>built on Cloudflare</span></div></footer>

  <script src="/deploy.js" defer></script>
</body>
</html>`;
}

// The docs area: quickstart, the connect guide (shared snippets), an organized
// API reference (the full per-endpoint reference stays at /endpoints), and a
// plain-language concepts page. Calm constellation theme, no animation behind text.
export function docsHtml(url: URL): string {
  const origin = url.origin;
  const app = `${origin}/view`;
  const deploy = `${origin}/deploy`;
  const endpointsRef = `${origin}/endpoints`;
  const repo = 'https://github.com/guirguispierre/memoryvault';
  const mcpUrl = mcpUrlFor(origin);
  const claudeCmd = escapeHtml(claudeCodeCommand(mcpUrl));
  const jsonCfg = escapeHtml(mcpJsonConfig(mcpUrl));
  const restExample = escapeHtml(
    'curl ' + origin + '/api/memories \\\n  -H "Authorization: Bearer YOUR_TOKEN"'
  );
  const mcpCurl = escapeHtml(
    'curl ' + mcpUrl + ' -X POST \\\n  -H "Authorization: Bearer YOUR_TOKEN" \\\n' +
    '  -H "Content-Type: application/json" \\\n  -d \'{"jsonrpc":"2.0","id":1,"method":"tools/list"}\''
  );
  const selfHost = escapeHtml(
    'git clone ' + repo + '.git\n' +
    'cd memoryvault\n' +
    'npm install\n' +
    'npx wrangler d1 create ai-memory\n' +
    'npx wrangler kv namespace create RATE_LIMIT_KV\n' +
    'npx wrangler vectorize create ai-memory-semantic-v1 --dimensions=768 --metric=cosine\n' +
    '# put the printed ids into wrangler.toml, then:\n' +
    'npx wrangler secret put AUTH_SECRET\n' +
    'npx wrangler d1 execute ai-memory --remote --file=schema.sql\n' +
    'npm run deploy'
  );

  const apiGroups: Array<{ title: string; rows: Array<[string, string, string]> }> = [
    { title: 'Auth', rows: [
      ['POST', '/auth/signup', 'Create an account and its brain'],
      ['POST', '/auth/login', 'Sign in, returns a session'],
      ['POST', '/auth/refresh', 'Exchange a refresh token'],
      ['POST', '/auth/logout', 'End the current session'],
      ['GET', '/auth/me', 'The signed-in account and brain'],
      ['GET', '/auth/sessions', 'List active sessions'],
      ['POST', '/auth/sessions/revoke', 'Revoke a session'],
    ] },
    { title: 'OAuth (for MCP clients)', rows: [
      ['POST', '/register', 'Dynamic client registration'],
      ['GET/POST', '/authorize', 'Authorization and sign-in screen'],
      ['POST', '/token', 'Token exchange with PKCE'],
      ['GET', '/.well-known/oauth-authorization-server', 'Discovery metadata'],
      ['GET', '/.well-known/oauth-protected-resource', 'Protected-resource metadata'],
    ] },
    { title: 'MCP', rows: [
      ['POST', '/mcp', 'JSON-RPC: initialize, tools/list, tools/call'],
      ['GET', '/mcp', 'SSE stream (streamable HTTP)'],
    ] },
    { title: 'Memory and data', rows: [
      ['GET', '/api/memories', 'List and search memories (type, search, limit)'],
      ['GET', '/api/memories/signature', 'Change fingerprint for live updates'],
      ['GET', '/api/links/:id', 'Links for one memory'],
      ['GET', '/api/graph', 'The memory graph'],
      ['GET', '/api/tools', 'Available MCP tools'],
      ['GET', '/api/export', 'Export the brain as JSON'],
      ['POST', '/api/import', 'Import memories'],
      ['POST', '/api/purge', 'Delete all data in the brain'],
    ] },
    { title: 'Viewer and waitlist', rows: [
      ['GET/PUT', '/api/viewer-settings', 'Per-brain viewer settings'],
      ['POST', '/api/waitlist', 'Hosted-tier email waitlist (public)'],
    ] },
  ];
  const apiHtml = apiGroups.map((g) =>
    `<div class="api-group"><h3>${escapeHtml(g.title)}</h3><div class="api-rows">` +
    g.rows.map(([m, p, d]) =>
      `<div class="api-row"><span class="api-m">${escapeHtml(m)}</span><code class="api-p">${escapeHtml(p)}</code><span class="api-d">${escapeHtml(d)}</span></div>`
    ).join('') + `</div></div>`
  ).join('');

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Docs · MemoryVault</title>
<meta name="description" content="MemoryVault docs: quickstart, connect your agent over MCP, the API reference, and how the living-memory model works.">
${FONT_LINK_TAGS}
${constellationHeadTags}
<style>
${constellationTokensCss}  * { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; }
  body { font-family: var(--ui); background: var(--bg); color: var(--ink); -webkit-font-smoothing: antialiased; line-height: 1.6; }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }
  .container { max-width: 1120px; margin: 0 auto; padding: 0 32px; position: relative; z-index: 1; }

  nav { display: flex; align-items: center; gap: 18px; max-width: 1120px; margin: 0 auto; padding: 24px 32px; position: relative; z-index: 1; }
  .brand { font-family: var(--doc); font-size: 18px; font-weight: 600; color: var(--ink); }
  nav .r { margin-left: auto; display: flex; gap: 16px; align-items: center; }
  nav .r a { font-size: 14px; color: var(--dim); }
  nav .r a:hover { color: var(--ink); text-decoration: none; }

  .dlayout { display: grid; grid-template-columns: 210px 1fr; gap: 40px; padding-top: 12px; padding-bottom: 80px; align-items: start; }
  .dside { position: sticky; top: 24px; display: flex; flex-direction: column; gap: 2px; }
  .dside-title { font-family: var(--mono); font-size: 0.62rem; letter-spacing: 0.14em; text-transform: uppercase; color: var(--faint); margin-bottom: 0.6rem; }
  .dside a { color: var(--dim); font-size: 0.92rem; padding: 0.4rem 0; }
  .dside a:hover { color: var(--ink); text-decoration: none; }
  .dside-sep { height: 1px; background: var(--rule); margin: 0.7rem 0; }

  .dmain { min-width: 0; }
  .dmain section { padding-bottom: 3.2rem; }
  .dmain section + section { border-top: 1px solid var(--rule); padding-top: 2.4rem; }
  .dmain h1 { font-family: var(--doc); font-weight: 500; font-size: 2rem; letter-spacing: -0.02em; color: var(--ink); margin-bottom: 0.5rem; }
  .dmain h2 { font-family: var(--doc); font-weight: 500; font-size: 1.5rem; letter-spacing: -0.01em; color: var(--ink); margin-bottom: 0.7rem; }
  .dmain h3 { font-family: var(--doc); font-weight: 500; font-size: 1.05rem; color: var(--ink); margin: 1.4rem 0 0.5rem; }
  .dmain p { color: var(--dim); font-size: 0.96rem; margin-bottom: 0.9rem; max-width: 68ch; }
  .dmain ol, .dmain ul { color: var(--dim); font-size: 0.96rem; margin: 0 0 0.9rem 1.2rem; }
  .dmain li { margin-bottom: 0.35rem; }
  .lead { color: var(--dim); font-size: 1.05rem; max-width: 64ch; margin-bottom: 1.4rem; }
  code { font-family: var(--mono); font-size: 0.85em; color: var(--ink); }
  .code { font-family: var(--mono); font-size: 0.8rem; line-height: 1.8; color: var(--ink); background: var(--surface); border: 1px solid var(--rule); border-radius: 12px; padding: 1rem 1.1rem; overflow-x: auto; white-space: pre; margin-bottom: 1rem; }

  .tracks { display: grid; grid-template-columns: minmax(0, 1.4fr) minmax(0, 1fr); gap: 18px; margin-bottom: 1.2rem; align-items: start; }
  .track { background: var(--surface); border: 1px solid var(--rule); border-radius: 14px; padding: 1.1rem 1.2rem; }
  .track h3 { margin-top: 0; }
  .track .tag { font-family: var(--mono); font-size: 0.62rem; letter-spacing: 0.1em; text-transform: uppercase; color: var(--accent); }
  .track.alt .tag { color: var(--good); }
  .btn { display: inline-block; font-family: var(--ui); font-weight: 600; font-size: 0.85rem; color: #070810; background: var(--accent); border-radius: 9px; padding: 0.55rem 1rem; margin-top: 0.4rem; }
  .btn:hover { text-decoration: none; filter: brightness(1.05); }

  .kv { display: flex; gap: 0.7rem; align-items: baseline; padding: 0.3rem 0; }
  .kv span { color: var(--faint); min-width: 96px; font-family: var(--mono); font-size: 0.66rem; text-transform: uppercase; letter-spacing: 0.06em; }
  .note { background: var(--surface); border: 1px solid var(--rule); border-left: 2px solid var(--accent); border-radius: 8px; padding: 0.7rem 0.9rem; font-size: 0.88rem; color: var(--dim); margin-bottom: 1rem; }

  .api-group { margin-bottom: 1.3rem; }
  .api-rows { border: 1px solid var(--rule); border-radius: 12px; overflow: hidden; }
  .api-row { display: grid; grid-template-columns: 70px minmax(0, 1.1fr) 1.4fr; gap: 0.8rem; align-items: baseline; padding: 0.6rem 0.9rem; border-top: 1px solid var(--rule); }
  .api-row:first-child { border-top: none; }
  .api-m { font-family: var(--mono); font-size: 0.68rem; color: var(--accent); letter-spacing: 0.04em; }
  .api-p { font-family: var(--mono); font-size: 0.8rem; color: var(--ink); overflow-x: auto; }
  .api-d { color: var(--dim); font-size: 0.84rem; }

  .tiers { display: grid; gap: 0.5rem; margin: 0.6rem 0 1.1rem; }
  .tier { display: flex; gap: 0.7rem; align-items: baseline; }
  .tier i { width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; transform: translateY(1px); }
  .tier b { color: var(--ink); font-weight: 600; min-width: 78px; display: inline-block; }
  .tier-active i { background: var(--good); }
  .tier-settling i { background: var(--warm); }
  .tier-resting i { background: var(--faint); }
  .rels { display: flex; flex-wrap: wrap; gap: 0.4rem; margin: 0.3rem 0 1rem; }
  .rels code { background: var(--surface); border: 1px solid var(--rule); border-radius: 6px; padding: 0.2rem 0.5rem; font-size: 0.78rem; }

  footer { border-top: 1px solid var(--rule); padding: 30px 0; color: var(--faint); font-size: 13px; }
  .fi { max-width: 1120px; margin: 0 auto; padding: 0 32px; display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap; }

  @media (max-width: 820px) {
    .dlayout { grid-template-columns: 1fr; gap: 18px; }
    .dside { position: static; flex-flow: row wrap; gap: 0.4rem 1rem; padding-bottom: 0.6rem; border-bottom: 1px solid var(--rule); }
    .dside-title { width: 100%; margin-bottom: 0.2rem; }
    .dside-sep { display: none; }
    .tracks { grid-template-columns: 1fr; }
    .api-row { grid-template-columns: 56px 1fr; }
    .api-row .api-d { grid-column: 1 / -1; }
  }
</style>
</head>
<body>
  ${constellationCalmField}
  <nav>
    <a class="brand" href="${origin}/">MemoryVault</a>
    <div class="r"><a href="${app}">Viewer</a><a href="${deploy}">Deploy</a><a href="${repo}" target="_blank" rel="noopener">GitHub</a></div>
  </nav>

  <div class="dlayout container">
    <aside class="dside">
      <div class="dside-title">Docs</div>
      <a href="#quickstart">Quickstart</a>
      <a href="#connect">Connect your agent</a>
      <a href="#api">API reference</a>
      <a href="#concepts">How it works</a>
      <div class="dside-sep"></div>
      <a href="${endpointsRef}">Full endpoint reference</a>
      <a href="${app}">Open the viewer</a>
    </aside>

    <main class="dmain">
      <section id="quickstart">
        <h1>Quickstart</h1>
        <p class="lead">Deploy MemoryVault, connect an agent, and watch your first memory land. Pick a track, then connect.</p>
        <div class="tracks">
          <div class="track">
            <div class="tag">Self-host, about 5 minutes</div>
            <h3>Run it on your own Cloudflare account</h3>
            <p>Free and open source, your data under your keys. You need Node and a Cloudflare account.</p>
            <div class="code">${selfHost}</div>
            <p>That stands up Workers, D1, and Vectorize under your account. The schema also applies itself on first request, so the explicit <code>d1 execute</code> is optional.</p>
          </div>
          <div class="track alt">
            <div class="tag">Hosted</div>
            <h3>Let us run it for you</h3>
            <p>Managed hosting is in waitlist. Billing is not open yet, so there is nothing to buy today. Join the list and we will email you when it opens.</p>
            <a class="btn" href="${deploy}">Join the waitlist</a>
          </div>
        </div>
        <h3>Then connect and write your first memory</h3>
        <ol>
          <li>Point your agent at your endpoint. See <a href="#connect">Connect your agent</a>.</li>
          <li>Ask the agent to remember something, or open the <a href="${app}">viewer</a> and use the first-run panel to write a test memory.</li>
          <li>Watch recall and the graph populate. See <a href="#concepts">How it works</a> for what the strengths and tiers mean.</li>
        </ol>
      </section>

      <section id="connect">
        <h1>Connect your agent</h1>
        <p class="lead">Your MCP endpoint is the same URL for every client. Auth is OAuth, so there is no token to paste: the client opens a sign-in once. These snippets are the single source of truth, identical to the in-app onboarding panel.</p>
        <div class="kv"><span>Endpoint</span><code>${escapeHtml(mcpUrl)}</code></div>
        <div class="kv"><span>Transport</span><code>${escapeHtml(MCP_TRANSPORT_LABEL)}</code></div>
        <div class="kv"><span>Auth</span><code>${escapeHtml(MCP_AUTH_LABEL)}</code></div>

        <h3>Claude Code</h3>
        <p>Add the server, then sign in when your browser opens.</p>
        <div class="code">${claudeCmd}</div>

        <h3>Codex, Cursor, and other MCP clients</h3>
        <p>Add a remote MCP server with the endpoint above. Many clients accept a config like this:</p>
        <div class="code">${jsonCfg}</div>

        <h3>REST API</h3>
        <p>Every <code>/api</code> and <code>/mcp</code> route is scoped to your brain by your bearer token. <code>YOUR_TOKEN</code> is an OAuth access token from the connect flow, or your <code>AUTH_SECRET</code> in legacy bearer mode.</p>
        <div class="code">${restExample}</div>
        <p>The MCP endpoint itself is plain JSON-RPC over POST:</p>
        <div class="code">${mcpCurl}</div>
      </section>

      <section id="api">
        <h1>API reference</h1>
        <p class="lead">The routes that actually exist in this server, grouped. For the full per-endpoint guides (methods, auth, payloads), see the <a href="${endpointsRef}">endpoint reference</a>.</p>
        <div class="note">Everything under <code>/api</code> and <code>/mcp</code> requires your bearer token or OAuth and only ever touches your own brain. <code>POST /api/waitlist</code> is the one public, write-only endpoint.</div>
        ${apiHtml}
      </section>

      <section id="concepts">
        <h1>How it works</h1>
        <p class="lead">MemoryVault is a living memory, not a static file. Memories carry weight, strengthen with use, fade when ignored, and link into a graph. Here is the model behind what you see in the viewer.</p>

        <h3>Every memory has weight</h3>
        <p>Each memory carries an <b>importance</b> and a <b>confidence</b> score, and the viewer factors in <b>recency</b> to show a single strength. Concretely the viewer reads strength as <code>0.45 importance + 0.20 confidence + 0.35 recency</code>, where recency decays over about two weeks. Use a memory and it is reinforced; leave it and recency pulls its strength down.</p>

        <h3>Strength tiers</h3>
        <p>The viewer groups recall into three tiers by that strength, the same ones you see in the index:</p>
        <div class="tiers">
          <div class="tier tier-active"><i></i><b>Active</b><span>Reinforced this week. Top of recall.</span></div>
          <div class="tier tier-settling"><i></i><b>Settling</b><span>Quiet for a few days, still solid.</span></div>
          <div class="tier tier-resting"><i></i><b>Resting</b><span>Fading from lack of use. Review soon.</span></div>
        </div>

        <h3>A graph, not a list</h3>
        <p>Memories link to each other, and the viewer's graph view shows it. Links carry a relation type:</p>
        <div class="rels"><code>related</code><code>supports</code><code>contradicts</code><code>supersedes</code><code>causes</code><code>example_of</code></div>
        <p>That is what makes recall connected: pulling one memory can surface the ones it supports, contradicts, or supersedes.</p>

        <h3>Your data stays yours</h3>
        <p>Each account gets its own isolated brain. Nothing crosses between tenants: reads, writes, search, graph, import, and export are all scoped by brain. That isolation is not just asserted, it is covered by an adversarial test suite that tries to reach across brains and must fail every time.</p>
      </section>
    </main>
  </div>

  <footer><div class="fi"><span>&copy; 2026 MemoryVault &middot; MIT licensed</span><span>built on Cloudflare</span></div></footer>

  <script src="/starfield.js" defer></script>
</body>
</html>`;
}

// Developer reference (formerly the bare root page), now served at /endpoints
// and linked from the marketing landing's Docs/footer.
export function endpointsIndexHtml(url: URL): string {
  const origin = url.origin;
  const mcpEndpoint = `${origin}/mcp`;
  const viewerEndpoint = `${origin}/view`;
  const authzMetadata = `${origin}/.well-known/oauth-authorization-server`;
  const resourceMetadata = `${origin}/.well-known/oauth-protected-resource`;
  const envLabel = url.hostname.includes('-dev') ? 'Development Environment' : 'Production Environment';
  const devEntries: Array<{ path: string; label: string }> = [
    { path: '/mcp', label: '/mcp' },
    { path: '/view', label: '/view' },
    { path: '/register', label: '/register' },
    { path: '/authorize', label: '/authorize' },
    { path: '/token', label: '/token' },
    { path: '/.well-known/oauth-authorization-server', label: '/.well-known/oauth-authorization-server' },
    { path: '/.well-known/oauth-protected-resource', label: '/.well-known/oauth-protected-resource' },
    { path: '/auth/signup', label: '/auth/signup' },
    { path: '/auth/login', label: '/auth/login' },
    { path: '/auth/refresh', label: '/auth/refresh' },
    { path: '/auth/logout', label: '/auth/logout' },
    { path: '/auth/me', label: '/auth/me' },
    { path: '/auth/sessions', label: '/auth/sessions' },
    { path: '/auth/sessions/revoke', label: '/auth/sessions/revoke' },
    { path: '/api/memories', label: '/api/memories' },
    { path: '/api/tools', label: '/api/tools' },
    { path: '/api/graph', label: '/api/graph' },
    { path: '/api/links/sample-memory-id', label: '/api/links/:memoryId' },
    { path: '/api/export', label: '/api/export' },
    { path: '/api/import', label: '/api/import' },
    { path: '/api/purge', label: '/api/purge' },
    { path: '/api/viewer-settings', label: '/api/viewer-settings' },
  ];
  const devRows = devEntries.map((entry) => {
    const guide = endpointGuideForPath(entry.path);
    const title = guide?.title
      ?? (entry.path === '/mcp' ? 'MCP Endpoint' : (entry.path === '/view' ? 'Web Viewer' : 'Endpoint'));
    const subtitle = guide?.subtitle
      ?? (entry.path === '/mcp'
        ? 'MCP JSON-RPC and SSE transport'
        : (entry.path === '/view' ? 'Human memory dashboard + graph explorer' : 'Endpoint surface'));
    const methods = guide?.methods ?? 'GET';
    const auth = guide?.auth
      ?? (entry.path === '/view'
        ? 'Browser login available in-page.'
        : (entry.path === '/mcp' ? 'Requires Bearer token/OAuth for tool calls.' : 'See endpoint guide.'));
    const endpointUrl = `${origin}${entry.path}`;
    return `<tr>
      <td><a class="endpoint" href="${endpointUrl}">${escapeHtml(entry.label)}</a></td>
      <td>${escapeHtml(title)}</td>
      <td><code>${escapeHtml(methods)}</code></td>
      <td>${escapeHtml(auth)}</td>
      <td>${escapeHtml(subtitle)}</td>
    </tr>`;
  }).join('');

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Endpoints · MemoryVault</title>
${FONT_LINK_TAGS}
${constellationHeadTags}
<style>
${constellationTokensCss}${pageChromeCss}  .wrap { max-width: 1180px; }
  .grid { display: grid; grid-template-columns: 1.05fr 1fr; gap: 1rem; }
  .metrics { margin-top: 1rem; display: flex; gap: 0.55rem; flex-wrap: wrap; }
  .metric {
    border: 1px solid var(--rule-soft);
    border-radius: 9px;
    padding: 0.55rem 0.65rem;
    min-width: 150px;
    background: var(--surface);
  }
  .metric .k {
    color: var(--cream-faint);
    display: block;
    font-family: var(--mono);
    font-size: 0.62rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
  }
  .metric .v {
    color: var(--cream);
    display: block;
    margin-top: 0.32rem;
    font-family: var(--disp);
    font-size: 1.05rem;
  }
  .dev {
    margin-top: 1.2rem;
    border: 1px solid var(--rule);
    border-radius: 12px;
    background: var(--surface-raised);
    overflow: hidden;
  }
  .dev-head {
    padding: 0.85rem 1rem;
    border-bottom: 1px solid var(--rule-soft);
    display: flex;
    gap: 0.5rem;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
  }
  .dev-head h2 { margin: 0; color: var(--cream); font-family: var(--disp); font-weight: 560; font-size: 0.95rem; }
  .dev-head p { color: var(--cream-faint); font-size: 0.78rem; }
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; min-width: 920px; }
  th, td {
    text-align: left;
    vertical-align: top;
    border-bottom: 1px solid var(--rule-soft);
    padding: 0.66rem 0.78rem;
    font-size: 0.82rem;
    line-height: 1.45;
  }
  th {
    color: var(--cream-faint);
    font-family: var(--mono);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    font-size: 0.62rem;
    position: sticky;
    top: 0;
    background: var(--ground-2);
    z-index: 2;
  }
  td { color: var(--cream-dim); }
  td code { font-size: 0.78rem; }
  .endpoint { display: inline-block; max-width: 320px; font-size: 0.82rem; }
  @media (max-width: 930px) { .grid { grid-template-columns: 1fr; } }
</style>
</head>
<body>
  ${constellationCalmField}
  <main class="wrap">
    <div class="pill">${escapeHtml(envLabel)}</div>
    <h1 class="title">MEMORY<span>VAULT</span> Endpoints</h1>
    <p class="sub"><a href="${origin}/" style="color:var(--cream-faint)">&larr; Home</a> &nbsp;·&nbsp; Developer reference for this MCP host</p>

    <div class="grid">
      <section class="card">
        <h2>Overview</h2>
        <p>This host serves the MemoryVault MCP, OAuth flow, web viewer, and diagnostic APIs. Use this page as the top-level map for all sub-sites and machine endpoints.</p>
        <div class="metrics">
          <div class="metric"><span class="k">Server</span><span class="v">${escapeHtml(SERVER_NAME)}</span></div>
          <div class="metric"><span class="k">Version</span><span class="v">${escapeHtml(SERVER_VERSION)}</span></div>
          <div class="metric"><span class="k">MCP Tools</span><span class="v">${TOOLS.length}</span></div>
        </div>
        <div class="actions">
          <a class="btn primary" href="${mcpEndpoint}">MCP Guide</a>
          <a class="btn" href="${viewerEndpoint}">Open Viewer</a>
          <a class="btn" href="${authzMetadata}">OAuth Metadata</a>
          <a class="btn" href="${resourceMetadata}">Resource Metadata</a>
        </div>
      </section>
      <section class="card">
        <h2>Quick Dev Notes</h2>
        <ul>
          <li>Browser navigation shows human-readable guides for MCP and API routes.</li>
          <li>Programmatic requests still receive OAuth challenge and normal JSON API behavior.</li>
          <li><code>/mcp</code> is the MCP endpoint for AI clients (JSON-RPC + SSE).</li>
          <li><code>/view</code> is the web UI for login, memory browsing, and graph exploration.</li>
        </ul>
      </section>
    </div>

    <section class="dev">
      <div class="dev-head">
        <h2>Dev Section: All Endpoints</h2>
        <p>Open any path for a friendly guide page or direct endpoint behavior.</p>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Path</th>
              <th>Surface</th>
              <th>Methods</th>
              <th>Auth</th>
              <th>Purpose</th>
            </tr>
          </thead>
          <tbody>${devRows}</tbody>
        </table>
      </div>
    </section>
  </main>
</body>
</html>`;
}

export function mcpLandingHtml(url: URL): string {
  const origin = url.origin;
  const mcpEndpoint = `${origin}/mcp`;
  const viewerEndpoint = `${origin}/view`;
  const authzMetadata = `${origin}/.well-known/oauth-authorization-server`;
  const resourceMetadata = `${origin}/.well-known/oauth-protected-resource`;
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MemoryVault MCP</title>
${FONT_LINK_TAGS}
${constellationHeadTags}
<style>
${constellationTokensCss}${pageChromeCss}  .wrap { max-width: 980px; }
  .grid { display: grid; grid-template-columns: 1.2fr 1fr; gap: 1rem; }
  .endpoint {
    display: block;
    margin-top: 0.5rem;
    background: var(--surface);
    border: 1px solid var(--rule);
    border-radius: 8px;
    padding: 0.5rem 0.55rem;
    font-size: 0.8rem;
  }
  .small { color: var(--cream-faint); font-size: 0.74rem; }
  @media (max-width: 860px) { .grid { grid-template-columns: 1fr; } }
</style>
</head>
<body>
  ${constellationCalmField}
  <main class="wrap">
    <h1 class="title">MEMORY<span>VAULT</span> MCP</h1>
    <p class="sub">Human Guide For The MCP Endpoint</p>

    <div class="grid">
      <section class="card">
        <h2>What This MCP Does</h2>
        <p>This server is a personal memory graph for AI clients. It stores memories (notes, facts, journal entries), links related memories, scores confidence/importance, supports snapshots, and exposes these capabilities as MCP tools.</p>
        <div class="actions">
          <a class="btn primary" href="${viewerEndpoint}">Open Web Viewer</a>
          <a class="btn" href="${authzMetadata}">OAuth Metadata</a>
          <a class="btn" href="${resourceMetadata}">Resource Metadata</a>
        </div>
      </section>

      <section class="card">
        <h2>Connect From AI Tools</h2>
        <ol>
          <li>Set your MCP server URL to <code>${mcpEndpoint}</code>.</li>
          <li>Leave API key blank to use OAuth sign-in.</li>
          <li>Authorize once; your client receives access/refresh tokens.</li>
          <li>Call MCP methods like <code>tools/list</code> and <code>tools/call</code>.</li>
        </ol>
      </section>

      <section class="card">
        <h2>Direct Endpoints</h2>
        <p class="small">MCP endpoint (JSON-RPC / SSE):</p>
        <a class="endpoint" href="${mcpEndpoint}">${mcpEndpoint}</a>
        <p class="small" style="margin-top:0.7rem">Viewer UI:</p>
        <a class="endpoint" href="${viewerEndpoint}">${viewerEndpoint}</a>
      </section>

      <section class="card">
        <h2>Why You See This Page</h2>
        <p>Browser navigation to <code>/mcp</code> now shows this guide. Programmatic MCP requests still receive OAuth challenge/auth-required responses unless authorized.</p>
      </section>
    </div>
  </main>
</body>
</html>`;
}


export function endpointGuideForPath(pathname: string): EndpointGuide | null {
  if (pathname === '/register') {
    return {
      title: 'OAuth Client Registration',
      subtitle: 'Dynamic client registration endpoint',
      endpointPath: '/register',
      methods: 'POST',
      auth: 'Trusted redirect domains can self-register; all other clients require an admin bearer token.',
      details: [
        'Registers an OAuth client for MCP access.',
        'Expected body includes redirect_uris and token_endpoint_auth_method.',
        'redirect_uris on poke.com or claude.ai can register without Authorization.',
        'All other redirect domains must send Authorization: Bearer ADMIN_TOKEN.',
        'Returns client_id and optional client_secret metadata.',
      ],
    };
  }
  if (pathname === '/authorize') {
    return {
      title: 'OAuth Authorization',
      subtitle: 'Authorization code + PKCE entry point',
      endpointPath: '/authorize',
      methods: 'GET, POST',
      auth: 'User authentication is performed here (signup/login/token mode).',
      details: [
        'Starts or completes the OAuth authorization flow.',
        'Returns an authorization code via redirect_uri.',
        'Used by MCP clients during first-time connection.',
      ],
    };
  }
  if (pathname === '/token') {
    return {
      title: 'OAuth Token Exchange',
      subtitle: 'Authorization code / refresh token exchange',
      endpointPath: '/token',
      methods: 'POST',
      auth: 'Client credentials vary by client type; PKCE is required for authorization_code.',
      details: [
        'Exchanges authorization codes for access and refresh tokens.',
        'Also rotates refresh tokens using grant_type=refresh_token.',
        'Returns OAuth-compliant token responses in JSON.',
      ],
    };
  }
  if (pathname === '/.well-known/oauth-authorization-server' || pathname === '/.well-known/openid-configuration') {
    return {
      title: 'Authorization Server Metadata',
      subtitle: 'OAuth discovery document',
      endpointPath: '/.well-known/oauth-authorization-server',
      methods: 'GET',
      auth: 'Public metadata endpoint.',
      details: [
        'Advertises authorization, token, and registration endpoints.',
        'Used by MCP and OAuth clients for auto-discovery.',
        'Includes supported grants, auth methods, and code challenge methods.',
      ],
    };
  }
  if (pathname === '/.well-known/oauth-protected-resource' || pathname.startsWith('/.well-known/oauth-protected-resource/')) {
    return {
      title: 'Protected Resource Metadata',
      subtitle: 'Resource metadata for MCP protected endpoints',
      endpointPath: '/.well-known/oauth-protected-resource',
      methods: 'GET',
      auth: 'Public metadata endpoint.',
      details: [
        'Describes which authorization server protects this resource.',
        'Used in WWW-Authenticate challenges for MCP endpoints.',
        'The /mcp-specific variant resolves metadata for that resource path.',
      ],
    };
  }
  if (pathname === '/auth/signup') {
    return {
      title: 'User Signup API',
      subtitle: 'Create account + primary brain',
      endpointPath: '/auth/signup',
      methods: 'POST',
      auth: 'No token required.',
      details: [
        'Creates a user account from email/password.',
        'Optionally accepts brain_name for the initial memory brain.',
        'Sets httpOnly auth_token and refresh_token cookies on success using SameSite=Lax.',
        'Returns { success: true, user } on success.',
      ],
    };
  }
  if (pathname === '/auth/login') {
    return {
      title: 'User Login API',
      subtitle: 'Credential login endpoint',
      endpointPath: '/auth/login',
      methods: 'POST',
      auth: 'No token required.',
      details: [
        'Authenticates user email/password credentials.',
        'Sets httpOnly auth_token and refresh_token cookies using SameSite=Lax.',
        'Returns { success: true, user } on success.',
        'Used by the web viewer and OAuth-assisted flows.',
      ],
    };
  }
  if (pathname === '/auth/refresh') {
    return {
      title: 'Token Refresh API',
      subtitle: 'Rotate session using refresh token',
      endpointPath: '/auth/refresh',
      methods: 'POST',
      auth: 'No access token required; requires refresh_token cookie.',
      details: [
        'Reads refresh_token from the Cookie header.',
        'Issues new auth_token and refresh_token cookies using SameSite=Lax.',
        'Revokes/replaces previous refresh token for session safety.',
        'Returns { success: true } on success.',
      ],
    };
  }
  if (pathname === '/auth/logout') {
    return {
      title: 'Logout API',
      subtitle: 'Revoke a refresh token session',
      endpointPath: '/auth/logout',
      methods: 'POST',
      auth: 'Clears auth cookies and revokes the current session when possible.',
      details: [
        'Clears both auth cookies on the server response with Max-Age=0.',
        'Returns { success: true } on success.',
        'Used when user signs out from the web viewer.',
      ],
    };
  }
  if (pathname === '/auth/me') {
    return {
      title: 'Session Check API',
      subtitle: 'Validate current authenticated session',
      endpointPath: '/auth/me',
      methods: 'GET',
      auth: 'Requires Authorization: Bearer <access_token> or auth_token cookie.',
      details: [
        'Validates current access token.',
        'Returns { ok: true } when the session is valid.',
      ],
    };
  }
  if (pathname === '/auth/sessions') {
    return {
      title: 'Session List API',
      subtitle: 'List active sessions for the current user',
      endpointPath: '/auth/sessions',
      methods: 'GET',
      auth: 'Requires Authorization: Bearer <access_token> or auth_token cookie.',
      details: [
        'Returns active sessions bound to the authenticated user.',
        'Used for account/session management and audit.',
      ],
    };
  }
  if (pathname === '/auth/sessions/revoke') {
    return {
      title: 'Session Revoke API',
      subtitle: 'Revoke one or more active sessions',
      endpointPath: '/auth/sessions/revoke',
      methods: 'POST',
      auth: 'Requires Authorization: Bearer <access_token> or auth_token cookie.',
      details: [
        'Revokes target session(s), including all-other-sessions mode.',
        'Used to lock out stale or compromised sessions.',
      ],
    };
  }
  if (pathname === '/api/memories') {
    return {
      title: 'Memories API',
      subtitle: 'List/search/create memory records',
      endpointPath: '/api/memories',
      methods: 'GET, POST',
      auth: 'Requires Authorization: Bearer <access_token>, auth_token cookie, or legacy AUTH_SECRET.',
      details: [
        'Returns memory records scoped to your brain.',
        'Supports type and search filtering via query params.',
        'Backs both web UI and MCP tool operations.',
      ],
    };
  }
  if (pathname === '/api/tools') {
    return {
      title: 'Tool Catalog API',
      subtitle: 'List MCP tools exposed by this server',
      endpointPath: '/api/tools',
      methods: 'GET',
      auth: 'Requires Authorization: Bearer <access_token>, auth_token cookie, or legacy AUTH_SECRET.',
      details: [
        'Returns the tool metadata available to MCP clients.',
        'Primarily useful for diagnostics and integration checks.',
      ],
    };
  }
  if (pathname === '/api/graph') {
    return {
      title: 'Memory Graph API',
      subtitle: 'Graph nodes + explicit/inferred links',
      endpointPath: '/api/graph',
      methods: 'GET',
      auth: 'Requires Authorization: Bearer <access_token>, auth_token cookie, or legacy AUTH_SECRET.',
      details: [
        'Returns graph nodes, explicit edges, and inferred edges.',
        'Used by the graph visualization in /view.',
      ],
    };
  }
  if (pathname.startsWith('/api/links/')) {
    return {
      title: 'Memory Links API',
      subtitle: 'Get links for a specific memory id',
      endpointPath: '/api/links/:memoryId',
      methods: 'GET',
      auth: 'Requires Authorization: Bearer <access_token>, auth_token cookie, or legacy AUTH_SECRET.',
      details: [
        'Returns outbound/inbound links for one memory.',
        'Path parameter is the target memory id.',
      ],
    };
  }
  if (pathname === '/api/export') {
    return {
      title: 'Data Export API',
      subtitle: 'Download a full backup of brain data as JSON',
      endpointPath: '/api/export',
      methods: 'GET',
      auth: 'Requires Authorization: Bearer <access_token>, auth_token cookie, or legacy AUTH_SECRET.',
      details: [
        'Returns a JSON file containing all memories, links, changelog, source trust, conflict resolutions, aliases, watches, and brain policy.',
        'Sensitive fields like webhook_url and secret are stripped from watch entries.',
        'Response includes a Content-Disposition header for browser download.',
        'Limited to 50,000 records per entity type.',
      ],
    };
  }
  if (pathname === '/api/import') {
    return {
      title: 'Data Import API',
      subtitle: 'Restore brain data from a previously exported backup',
      endpointPath: '/api/import',
      methods: 'POST',
      auth: 'Requires Authorization: Bearer <access_token>, auth_token cookie, or legacy AUTH_SECRET.',
      details: [
        'Accepts a JSON body matching the memoryvault_export_v1 schema.',
        'Supports three strategies: merge (add/update), skip_existing (add only), overwrite (delete all then import).',
        'Imported memories are synced to the vector index for semantic search.',
        'Links referencing non-existent memories are silently skipped.',
      ],
    };
  }
  if (pathname === '/api/purge') {
    return {
      title: 'Data Purge API',
      subtitle: 'Permanently delete all brain data (destructive)',
      endpointPath: '/api/purge',
      methods: 'POST',
      auth: 'Requires Authorization: Bearer <access_token>, auth_token cookie, or legacy AUTH_SECRET.',
      details: [
        'Permanently deletes all memories, links, changelog, snapshots, watches, source trust, aliases, and conflict resolutions.',
        'Requires a confirmation body: { "confirm": "PURGE ALL DATA" }.',
        'Vector index entries are also deleted.',
        'This action cannot be undone.',
      ],
    };
  }
  return null;
}

export function endpointGuideHtml(url: URL, guide: EndpointGuide): string {
  const origin = url.origin;
  const mcpEndpoint = `${origin}/mcp`;
  const viewerEndpoint = `${origin}/view`;
  const endpointUrl = guide.endpointPath.includes(':')
    ? `${origin}${guide.endpointPath}`
    : `${origin}${guide.endpointPath}`;
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${guide.title} · MemoryVault</title>
${FONT_LINK_TAGS}
${constellationHeadTags}
<style>
${constellationTokensCss}${pageChromeCss}  .wrap { max-width: 920px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0.95rem; }
  .span-2 { grid-column: 1 / -1; }
  .label {
    color: var(--butter);
    font-family: var(--mono);
    font-size: 0.64rem;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    margin: 0 0 0.45rem;
  }
  .endpoint {
    display: block;
    margin-top: 0.35rem;
    background: var(--surface);
    border: 1px solid var(--rule);
    border-radius: 8px;
    padding: 0.5rem 0.55rem;
    font-size: 0.8rem;
  }
  @media (max-width: 800px) {
    .grid { grid-template-columns: 1fr; }
    .span-2 { grid-column: auto; }
  }
</style>
</head>
<body>
  ${constellationCalmField}
  <main class="wrap">
    <h1 class="title">MEMORY<span>VAULT</span> Endpoint Guide</h1>
    <p class="sub">${guide.title}</p>
    <div class="grid">
      <section class="card span-2">
        <p class="label">Purpose</p>
        <p>${guide.subtitle}</p>
      </section>
      <section class="card">
        <p class="label">Endpoint</p>
        <a class="endpoint" href="${endpointUrl}">${endpointUrl}</a>
      </section>
      <section class="card">
        <p class="label">Methods</p>
        <p><code>${guide.methods}</code></p>
        <p class="label" style="margin-top:0.7rem">Auth</p>
        <p>${guide.auth}</p>
      </section>
      <section class="card span-2">
        <p class="label">How To Use</p>
        <ul>
          ${guide.details.map((item) => `<li>${item}</li>`).join('')}
        </ul>
        <div class="actions">
          <a class="btn primary" href="${mcpEndpoint}">MCP Guide</a>
          <a class="btn" href="${viewerEndpoint}">Open Viewer</a>
          <a class="btn" href="${origin}/.well-known/oauth-authorization-server">OAuth Metadata</a>
        </div>
      </section>
    </div>
  </main>
</body>
</html>`;
}
