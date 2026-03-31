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
} from './cors.js';

import {
  loadLinkStatsMap,
  loadSourceTrustMap,
  getBrainPolicy,
  setBrainPolicy,
  loadExplicitMemoryLinks,
  logChangelog,
  parseJsonObject,
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
} from './tools.js';

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
      }, 15000);
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

  const links = await loadExplicitMemoryLinks(env, brainId, 50000);

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
         brain_id = excluded.brain_id, type = excluded.type, title = excluded.title, key = excluded.key,
         content = excluded.content, tags = excluded.tags, source = excluded.source,
         confidence = excluded.confidence, importance = excluded.importance,
         archived_at = excluded.archived_at, created_at = excluded.created_at, updated_at = excluded.updated_at`
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

    restoredMemoryRows.push({ id: memoryId, type, content, tags: typeof m.tags === 'string' ? m.tags : null });
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
         brain_id = excluded.brain_id, from_id = excluded.from_id, to_id = excluded.to_id,
         relation_type = excluded.relation_type, label = excluded.label`
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
    await env.DB.prepare(
      `INSERT OR IGNORE INTO memory_changelog (id, brain_id, event_type, entity_type, entity_id, summary, payload, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
    ).bind(
      entryId, brainId, eventType, entityType, entityId, summary,
      typeof entry.payload === 'string' ? entry.payload : (entry.payload ? stableJson(entry.payload) : null),
      Math.floor(toFiniteNumber(entry.created_at, ts))
    ).run();
    counts.memory_changelog++;
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
  await env.DB.prepare('DELETE FROM memories WHERE brain_id = ?').bind(brainId).run();

  return new Response(JSON.stringify({ ok: true, purged: { memories: memoryCount, links: linkCount } }), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}



export function rootLandingHtml(url: URL): string {
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
<title>MemoryVault Dev Portal</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #060b12;
    --bg2: #0f1927;
    --line: #27466c;
    --line-soft: #1c334c;
    --text: #d6e5f4;
    --dim: #7390aa;
    --amber: #f0a500;
    --teal: #00c8b4;
    --mono: 'Share Tech Mono', monospace;
    --sans: 'Syne', sans-serif;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: var(--mono);
    color: var(--text);
    background:
      radial-gradient(78% 55% at 10% 0%, rgba(0, 200, 180, 0.14), transparent 70%),
      radial-gradient(70% 58% at 100% 100%, rgba(240, 165, 0, 0.1), transparent 72%),
      var(--bg);
    min-height: 100vh;
  }
  .wrap {
    max-width: 1180px;
    margin: 0 auto;
    padding: 2rem 1.1rem 2.6rem;
  }
  .title {
    margin: 0;
    font-family: var(--sans);
    font-size: clamp(1.65rem, 3vw, 2.75rem);
    letter-spacing: -0.02em;
    font-weight: 800;
    line-height: 1.05;
  }
  .title span { color: var(--amber); }
  .sub {
    margin: 0.5rem 0 1.2rem;
    color: var(--dim);
    letter-spacing: 0.11em;
    text-transform: uppercase;
    font-size: 0.72rem;
  }
  .pill {
    display: inline-flex;
    align-items: center;
    border: 1px solid var(--line);
    background: rgba(15, 25, 39, 0.78);
    color: var(--teal);
    font-size: 0.68rem;
    letter-spacing: 0.13em;
    text-transform: uppercase;
    padding: 0.3rem 0.55rem;
    margin-bottom: 1rem;
  }
  .grid {
    display: grid;
    grid-template-columns: 1.05fr 1fr;
    gap: 1rem;
  }
  .card {
    border: 1px solid var(--line);
    background: rgba(15, 25, 39, 0.84);
    padding: 1rem 1rem 0.95rem;
  }
  .card h2 {
    margin: 0 0 0.65rem;
    color: var(--amber);
    font-size: 0.79rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
  }
  p, li {
    margin: 0;
    line-height: 1.58;
    font-size: 0.84rem;
  }
  ul {
    margin: 0;
    padding-left: 1.1rem;
    display: grid;
    gap: 0.45rem;
  }
  .metrics {
    margin-top: 0.8rem;
    display: flex;
    gap: 0.55rem;
    flex-wrap: wrap;
  }
  .metric {
    border: 1px solid var(--line-soft);
    padding: 0.45rem 0.55rem;
    min-width: 150px;
    background: rgba(6, 11, 18, 0.68);
  }
  .metric .k {
    color: var(--dim);
    display: block;
    font-size: 0.66rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
  }
  .metric .v {
    color: var(--teal);
    display: block;
    margin-top: 0.3rem;
    font-size: 0.84rem;
  }
  .actions {
    margin-top: 0.85rem;
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
  }
  .btn {
    border: 1px solid var(--line);
    color: var(--text);
    text-decoration: none;
    font-size: 0.7rem;
    letter-spacing: 0.11em;
    text-transform: uppercase;
    padding: 0.46rem 0.62rem;
    display: inline-block;
  }
  .btn.primary {
    border-color: var(--amber);
    color: var(--amber);
  }
  .dev {
    margin-top: 1rem;
    border: 1px solid var(--line);
    background: rgba(15, 25, 39, 0.84);
    overflow: hidden;
  }
  .dev-head {
    padding: 0.75rem 0.9rem;
    border-bottom: 1px solid var(--line-soft);
    display: flex;
    gap: 0.5rem;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
  }
  .dev-head h2 {
    margin: 0;
    color: var(--amber);
    font-size: 0.8rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
  }
  .dev-head p {
    color: var(--dim);
    font-size: 0.72rem;
  }
  .table-wrap { overflow-x: auto; }
  table {
    width: 100%;
    border-collapse: collapse;
    min-width: 920px;
  }
  th, td {
    text-align: left;
    vertical-align: top;
    border-bottom: 1px solid var(--line-soft);
    padding: 0.62rem 0.72rem;
    font-size: 0.77rem;
    line-height: 1.45;
  }
  th {
    color: var(--dim);
    text-transform: uppercase;
    letter-spacing: 0.12em;
    font-size: 0.66rem;
    position: sticky;
    top: 0;
    background: #0f1927;
    z-index: 2;
  }
  td code {
    color: var(--teal);
    font-family: var(--mono);
    font-size: 0.74rem;
  }
  .endpoint {
    color: var(--teal);
    text-decoration: none;
    display: inline-block;
    max-width: 320px;
    overflow-wrap: anywhere;
  }
  @media (max-width: 930px) {
    .grid { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
  <main class="wrap">
    <div class="pill">${escapeHtml(envLabel)}</div>
    <h1 class="title">MEMORY<span>VAULT</span> Dev Portal</h1>
    <p class="sub">Human-Friendly Landing Page For This MCP Host</p>

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
  <script src="https://mcp.figma.com/mcp/html-to-design/capture.js" async></script>
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
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #070b10;
    --bg2: #101824;
    --line: #234061;
    --text: #d8e8f8;
    --dim: #6f8ea9;
    --amber: #f0a500;
    --teal: #00c8b4;
    --mono: 'Share Tech Mono', monospace;
    --sans: 'Syne', sans-serif;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: var(--mono);
    color: var(--text);
    background:
      radial-gradient(70% 50% at 12% 0%, rgba(0, 200, 180, 0.14), transparent 70%),
      radial-gradient(60% 60% at 100% 100%, rgba(240, 165, 0, 0.12), transparent 70%),
      var(--bg);
    min-height: 100vh;
  }
  .wrap {
    max-width: 980px;
    margin: 0 auto;
    padding: 2rem 1.2rem 2.6rem;
  }
  .title {
    font-family: var(--sans);
    font-size: clamp(1.55rem, 3vw, 2.5rem);
    font-weight: 800;
    letter-spacing: -0.02em;
    margin: 0 0 0.35rem;
  }
  .title span { color: var(--amber); }
  .sub {
    margin: 0 0 1.4rem;
    color: var(--dim);
    letter-spacing: 0.08em;
    font-size: 0.72rem;
    text-transform: uppercase;
  }
  .grid {
    display: grid;
    grid-template-columns: 1.2fr 1fr;
    gap: 1rem;
  }
  .card {
    border: 1px solid var(--line);
    background: rgba(16, 24, 36, 0.88);
    padding: 1rem 1rem 0.95rem;
  }
  .card h2 {
    margin: 0 0 0.65rem;
    color: var(--amber);
    font-size: 0.8rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
  }
  p, li {
    margin: 0;
    color: var(--text);
    line-height: 1.6;
    font-size: 0.86rem;
  }
  ul, ol {
    margin: 0;
    padding-left: 1.1rem;
    display: grid;
    gap: 0.45rem;
  }
  .actions {
    margin-top: 1rem;
    display: flex;
    flex-wrap: wrap;
    gap: 0.55rem;
  }
  .btn {
    border: 1px solid var(--line);
    color: var(--text);
    text-decoration: none;
    font-size: 0.72rem;
    letter-spacing: 0.11em;
    text-transform: uppercase;
    padding: 0.48rem 0.62rem;
    display: inline-block;
  }
  .btn.primary {
    border-color: var(--amber);
    color: var(--amber);
  }
  .endpoint {
    margin-top: 0.5rem;
    display: block;
    color: var(--teal);
    background: rgba(7, 11, 16, 0.85);
    border: 1px solid var(--line);
    padding: 0.45rem 0.5rem;
    font-size: 0.76rem;
    overflow-wrap: anywhere;
  }
  .small { color: var(--dim); font-size: 0.72rem; }
  code {
    font-family: var(--mono);
    color: var(--teal);
    font-size: 0.8rem;
  }
  @media (max-width: 860px) {
    .grid { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
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
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #070b10;
    --bg2: #101824;
    --line: #234061;
    --text: #d8e8f8;
    --dim: #6f8ea9;
    --amber: #f0a500;
    --teal: #00c8b4;
    --mono: 'Share Tech Mono', monospace;
    --sans: 'Syne', sans-serif;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: var(--mono);
    color: var(--text);
    background:
      radial-gradient(70% 50% at 12% 0%, rgba(0, 200, 180, 0.14), transparent 70%),
      radial-gradient(60% 60% at 100% 100%, rgba(240, 165, 0, 0.12), transparent 70%),
      var(--bg);
    min-height: 100vh;
  }
  .wrap {
    max-width: 920px;
    margin: 0 auto;
    padding: 2rem 1.2rem 2.6rem;
  }
  .title {
    font-family: var(--sans);
    font-size: clamp(1.4rem, 3vw, 2.2rem);
    font-weight: 800;
    letter-spacing: -0.02em;
    margin: 0;
  }
  .title span { color: var(--amber); }
  .sub {
    margin: 0.35rem 0 1.2rem;
    color: var(--dim);
    letter-spacing: 0.08em;
    font-size: 0.72rem;
    text-transform: uppercase;
  }
  .grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.95rem;
  }
  .card {
    border: 1px solid var(--line);
    background: rgba(16, 24, 36, 0.88);
    padding: 0.95rem 1rem;
  }
  .span-2 { grid-column: 1 / -1; }
  .label {
    color: var(--amber);
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    margin: 0 0 0.45rem;
  }
  p, li {
    margin: 0;
    line-height: 1.6;
    font-size: 0.84rem;
  }
  ul {
    margin: 0;
    padding-left: 1.05rem;
    display: grid;
    gap: 0.4rem;
  }
  .endpoint {
    display: block;
    margin-top: 0.35rem;
    color: var(--teal);
    background: rgba(7, 11, 16, 0.85);
    border: 1px solid var(--line);
    padding: 0.45rem 0.5rem;
    font-size: 0.76rem;
    overflow-wrap: anywhere;
    text-decoration: none;
  }
  .actions {
    display: flex;
    flex-wrap: wrap;
    gap: 0.55rem;
    margin-top: 0.8rem;
  }
  .btn {
    border: 1px solid var(--line);
    color: var(--text);
    text-decoration: none;
    font-size: 0.7rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 0.45rem 0.6rem;
  }
  .btn.primary { border-color: var(--amber); color: var(--amber); }
  code { color: var(--teal); }
  @media (max-width: 800px) {
    .grid { grid-template-columns: 1fr; }
    .span-2 { grid-column: auto; }
  }
</style>
</head>
<body>
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
