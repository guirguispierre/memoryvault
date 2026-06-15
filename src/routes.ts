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
} from './cors.js';

import {
  loadLinkStatsMap,
  loadSourceTrustMap,
  getBrainPolicy,
  setBrainPolicy,
  getViewerSettings,
  setViewerSettings,
  logChangelog,
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
  FONT_LINK_TAGS,
  vanillaTokensCss,
  pageChromeCss,
  themeBootstrapTag,
} from './viewer/tokens.js';

import { themeStyles } from './viewer/themes.js';

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
const VIEWER_THEME_NAMES = new Set(['slate', 'paper', 'vanilla', 'midnight', 'solarized', 'ember', 'arctic', 'custom']);
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
  const theme = typeof src.theme === 'string' && VIEWER_THEME_NAMES.has(src.theme) ? src.theme : 'slate';
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



export function rootLandingHtml(url: URL): string {
  const origin = url.origin;
  const app = `${origin}/view`;
  const endpointsRef = `${origin}/endpoints`;
  const repo = 'https://github.com/guirguispierre/memoryvault';
  // Deterministic recall ribbon for the product-shot facsimile.
  const ticks = Array.from({ length: 46 }, (_, i) => {
    const t = i / 45;
    const active = (i * 7 + 3) % 10 < 2 + t * 5;
    const h = active ? Math.round(8 + ((i * 13) % 17) * (0.4 + t)) : 2 + ((i * 5) % 3);
    const o = active ? (0.3 + t * 0.6).toFixed(2) : '0.16';
    return `<i style="height:${h}px;opacity:${o}"></i>`;
  }).join('');

  return `<!DOCTYPE html>
<html lang="en" data-theme="paper-light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MemoryVault — the memory layer your agents own</title>
<meta name="description" content="Persistent, graph-aware memory for AI agents. Open source, self-hosted on your own Cloudflare account, nothing paywalled.">
${FONT_LINK_TAGS}
${themeBootstrapTag}
<style>
${vanillaTokensCss}${themeStyles}  * { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; }
  body {
    font-family: var(--body);
    background: var(--ground);
    color: var(--cream);
    -webkit-font-smoothing: antialiased;
    line-height: 1.5;
  }
  a { color: inherit; text-decoration: none; }
  .container { max-width: 1080px; margin: 0 auto; padding: 0 32px; }

  nav { position: sticky; top: 0; z-index: 50; background: var(--ground); border-bottom: 1px solid var(--rule); }
  .nav-in { display: flex; align-items: center; gap: 28px; padding: 16px 32px; max-width: 1080px; margin: 0 auto; }
  .brand { font-family: var(--disp); font-weight: 600; font-size: 18px; color: var(--cream); }
  .brand .dot { color: var(--butter); }
  .brand .md { font-family: var(--mono); font-size: 13px; color: var(--cream-faint); }
  .nav-links { display: flex; gap: 22px; margin-left: 8px; }
  .nav-links a { font-size: 13.5px; color: var(--cream-dim); }
  .nav-links a:hover { color: var(--butter); }
  .nav-right { margin-left: auto; display: flex; align-items: center; gap: 14px; }
  .theme-toggle {
    font-family: var(--mono); font-size: 11px; color: var(--cream-faint);
    border: 1px solid var(--rule); border-radius: 7px; padding: 6px 10px;
    background: var(--surface-raised); cursor: pointer;
  }
  .theme-toggle:hover { color: var(--butter); border-color: var(--butter-deep); }
  .btn {
    font-family: var(--body); font-weight: 600; font-size: 13.5px;
    border-radius: 9px; padding: 9px 16px; cursor: pointer;
    border: 1px solid var(--butter); background: var(--butter); color: var(--on-butter);
    display: inline-block; transition: filter 0.15s, border-color 0.15s, color 0.15s;
  }
  .btn:hover { filter: brightness(1.05); }
  .btn.ghost { background: transparent; border-color: var(--rule); color: var(--cream-dim); }
  .btn.ghost:hover { filter: none; border-color: var(--butter-deep); color: var(--cream); }
  :focus-visible { outline: 2px solid var(--butter); outline-offset: 2px; }

  .hero { text-align: center; padding: 86px 0 30px; }
  .eyebrow { font-family: var(--mono); font-size: 11px; letter-spacing: 0.1em; text-transform: uppercase; color: var(--butter); margin-bottom: 20px; }
  .hero h1 { font-family: var(--disp); font-weight: 500; font-size: 62px; line-height: 1.04; letter-spacing: -0.02em; max-width: 14ch; margin: 0 auto 22px; color: var(--cream); }
  .hero h1 em { font-style: italic; color: var(--butter); }
  .hero p { font-size: 18px; color: var(--cream-dim); max-width: 54ch; margin: 0 auto 30px; line-height: 1.55; }
  .hero-cta { display: flex; gap: 12px; justify-content: center; align-items: center; flex-wrap: wrap; }
  .hero-sub { margin-top: 16px; font-family: var(--mono); font-size: 12px; color: var(--cream-faint); }

  .shot { max-width: 1000px; margin: 46px auto 0; border: 1px solid var(--rule); border-radius: 14px; overflow: hidden; box-shadow: 0 30px 80px var(--card-glow); background: var(--surface-raised); }
  .shot-top { display: flex; align-items: center; gap: 16px; padding: 13px 18px; border-bottom: 1px solid var(--rule); }
  .shot-wm { font-family: var(--disp); font-weight: 500; font-size: 15px; color: var(--cream); }
  .shot-wm .dot { color: var(--butter); }
  .shot-wm .path { font-family: var(--mono); font-size: 10px; color: var(--cream-faint); margin-left: 8px; }
  .shot-tabs { margin-left: auto; font-family: var(--mono); font-size: 10.5px; color: var(--cream-faint); letter-spacing: 0.04em; }
  .shot-body { padding: 18px 22px 24px; }
  .shot-fm { display: flex; align-items: baseline; justify-content: space-between; margin-bottom: 11px; }
  .shot-fm .t { font-family: var(--disp); font-style: italic; font-size: 14px; color: var(--cream-dim); }
  .shot-fm .t b { font-style: normal; font-weight: 500; color: var(--cream); }
  .shot-fm .m { font-family: var(--mono); font-size: 10.5px; color: var(--cream-faint); }
  .shot-ticks { display: flex; align-items: flex-end; gap: 3px; height: 30px; margin-bottom: 4px; }
  .shot-ticks i { flex: 1; min-width: 2px; border-radius: 1.5px; background: var(--butter); }
  .shot-sec { font-family: var(--disp); font-weight: 500; font-size: 15px; color: var(--cream); margin: 20px 0 4px; }
  .shot-entry { display: grid; grid-template-columns: 1fr auto; gap: 14px; padding: 12px 0; border-bottom: 1px solid var(--rule-soft); }
  .shot-entry:last-child { border-bottom: none; }
  .shot-eh { display: flex; align-items: center; gap: 9px; margin-bottom: 4px; }
  .shot-bead { width: 7px; height: 7px; border-radius: 50%; background: var(--butter); }
  .shot-et { font-family: var(--disp); font-weight: 500; font-size: 15.5px; color: var(--cream); }
  .shot-kind { font-family: var(--mono); font-size: 9px; letter-spacing: 0.06em; text-transform: uppercase; color: var(--cream-faint); }
  .shot-ver { font-family: var(--mono); font-size: 8.5px; letter-spacing: 0.04em; text-transform: uppercase; color: var(--sage); border: 1px solid var(--rule); border-radius: 4px; padding: 1px 5px; }
  .shot-eb { font-family: var(--disp); font-size: 14px; line-height: 1.5; color: var(--cream-dim); }
  .shot-eb .k { font-family: var(--mono); font-size: 12px; color: var(--butter); }
  .shot-eb .arr { color: var(--cream-faint); margin: 0 6px; }
  .shot-aside { text-align: right; font-family: var(--mono); font-size: 10px; color: var(--cream-faint); white-space: nowrap; }
  .shot-meter { display: inline-flex; align-items: center; gap: 6px; margin-top: 5px; }
  .shot-meter .bar { width: 48px; height: 4px; border-radius: 2px; background: var(--ground-3); overflow: hidden; }
  .shot-meter .bar i { display: block; height: 100%; background: var(--butter); }

  .feature { padding: 84px 0; border-top: 1px solid var(--rule-soft); }
  .feature-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 56px; align-items: center; }
  .feature h2 { font-family: var(--disp); font-weight: 500; font-size: 34px; letter-spacing: -0.015em; margin-bottom: 16px; line-height: 1.15; color: var(--cream); }
  .feature h2 em { font-style: italic; color: var(--butter); }
  .feature p { font-size: 16px; color: var(--cream-dim); margin-bottom: 14px; line-height: 1.6; }
  .feature .mini { font-family: var(--mono); font-size: 12px; color: var(--cream-faint); }
  .panel { background: var(--surface-raised); border: 1px solid var(--rule); border-radius: 12px; padding: 26px; box-shadow: 0 16px 40px var(--card-glow); }
  .ledger-line { display: flex; align-items: baseline; gap: 10px; padding: 9px 0; border-bottom: 1px solid var(--rule-soft); font-size: 14px; }
  .ledger-line:last-child { border: none; }
  .ledger-line .k { font-family: var(--mono); font-size: 12px; color: var(--butter); }
  .ledger-line .arr { color: var(--cream-faint); }
  .ledger-line .v { font-family: var(--disp); color: var(--cream); }
  .ledger-line .tag { margin-left: auto; font-family: var(--mono); font-size: 9px; text-transform: uppercase; color: var(--sage); border: 1px solid var(--rule); border-radius: 4px; padding: 1px 5px; }
  .config { font-family: var(--mono); font-size: 12.5px; color: var(--cream-dim); line-height: 1.9; }
  .config .c { color: var(--cream-faint); }
  .config .b { color: var(--butter); }
  .config .g { color: var(--sage); }

  .how { padding: 84px 0; border-top: 1px solid var(--rule-soft); text-align: center; }
  .how > h2 { font-family: var(--disp); font-weight: 500; font-size: 34px; margin-bottom: 12px; color: var(--cream); }
  .how > p { color: var(--cream-dim); max-width: 50ch; margin: 0 auto 48px; }
  .steps { display: grid; grid-template-columns: repeat(3, 1fr); gap: 28px; text-align: left; }
  .step { background: var(--surface-raised); border: 1px solid var(--rule); border-radius: 12px; padding: 26px; }
  .step .n { font-family: var(--mono); font-size: 12px; color: var(--butter); margin-bottom: 12px; }
  .step h3 { font-family: var(--disp); font-weight: 500; font-size: 19px; margin-bottom: 8px; color: var(--cream); }
  .step p { font-size: 14px; color: var(--cream-dim); line-height: 1.55; }

  .agents { padding: 60px 0; border-top: 1px solid var(--rule-soft); text-align: center; }
  .agents .lbl { font-family: var(--mono); font-size: 11px; letter-spacing: 0.1em; text-transform: uppercase; color: var(--cream-faint); margin-bottom: 22px; }
  .agent-row { display: flex; gap: 14px; justify-content: center; flex-wrap: wrap; }
  .chip { font-family: var(--mono); font-size: 13px; color: var(--cream-dim); border: 1px solid var(--rule); border-radius: 999px; padding: 8px 16px; background: var(--surface-raised); }

  .pricing { padding: 84px 0; border-top: 1px solid var(--rule-soft); text-align: center; }
  .pricing > h2 { font-family: var(--disp); font-weight: 500; font-size: 34px; margin-bottom: 12px; color: var(--cream); }
  .pricing > p { color: var(--cream-dim); margin-bottom: 44px; }
  .plans { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; max-width: 760px; margin: 0 auto; text-align: left; }
  .plan { background: var(--surface-raised); border: 1px solid var(--rule); border-radius: 14px; padding: 30px; display: flex; flex-direction: column; }
  .plan.feat { border-color: var(--butter); box-shadow: 0 0 0 3px var(--butter-glow); }
  .plan .name { font-family: var(--disp); font-size: 22px; font-weight: 500; margin-bottom: 4px; color: var(--cream); }
  .plan .price { font-family: var(--disp); font-size: 34px; margin-bottom: 4px; color: var(--cream); }
  .plan .price small { font-family: var(--body); font-size: 14px; color: var(--cream-faint); }
  .plan .desc { font-size: 14px; color: var(--cream-dim); margin-bottom: 18px; }
  .plan ul { list-style: none; margin-bottom: 22px; }
  .plan li { font-size: 13.5px; color: var(--cream-dim); padding: 6px 0 6px 22px; position: relative; }
  .plan li::before { content: "\\2713"; position: absolute; left: 0; color: var(--sage); font-weight: 600; }
  .plan .cta { margin-top: auto; display: block; text-align: center; }
  .plan .note { font-family: var(--mono); font-size: 10px; color: var(--cream-faint); text-align: center; margin-top: 8px; }

  .faq { padding: 84px 0; border-top: 1px solid var(--rule-soft); }
  .faq h2 { font-family: var(--disp); font-weight: 500; font-size: 34px; margin-bottom: 32px; text-align: center; color: var(--cream); }
  .qa { max-width: 760px; margin: 0 auto; }
  .qa .item { border-bottom: 1px solid var(--rule-soft); padding: 20px 0; }
  .qa .q { font-family: var(--disp); font-size: 18px; font-weight: 500; margin-bottom: 8px; color: var(--cream); }
  .qa .a { font-size: 15px; color: var(--cream-dim); line-height: 1.6; }

  .final { padding: 96px 0; border-top: 1px solid var(--rule-soft); text-align: center; }
  .final h2 { font-family: var(--disp); font-weight: 500; font-size: 46px; letter-spacing: -0.02em; max-width: 16ch; margin: 0 auto 24px; line-height: 1.1; color: var(--cream); }
  .final h2 em { font-style: italic; color: var(--butter); }

  footer { border-top: 1px solid var(--rule); padding: 40px 0; color: var(--cream-faint); font-size: 13px; }
  .foot-in { display: flex; justify-content: space-between; align-items: center; gap: 16px; flex-wrap: wrap; }
  .foot-in a:hover { color: var(--butter); }
  .ok { font-family: var(--mono); font-size: 11px; color: var(--sage); }

  @media (max-width: 760px) {
    .container { padding: 0 20px; }
    .nav-in { padding: 14px 20px; gap: 14px; }
    .nav-links { display: none; }
    .hero { padding: 56px 0 24px; }
    .hero h1 { font-size: 40px; }
    .hero p { font-size: 16px; }
    .feature-grid { grid-template-columns: 1fr; gap: 28px; }
    .feature, .how, .pricing, .faq, .final { padding: 56px 0; }
    .steps { grid-template-columns: 1fr; }
    .plans { grid-template-columns: 1fr; }
    .final h2 { font-size: 34px; }
    .shot-tabs { display: none; }
  }
</style>
</head>
<body>
  <nav>
    <div class="nav-in">
      <a class="brand" href="${origin}/">memoryvault<span class="dot">.</span><span class="md">md</span></a>
      <div class="nav-links"><a href="#features">Features</a><a href="#how">How it works</a><a href="#pricing">Pricing</a><a href="#faq">FAQ</a></div>
      <div class="nav-right">
        <button class="theme-toggle" data-theme-toggle type="button">&#9680; theme</button>
        <a class="btn ghost" style="padding:8px 14px" href="${endpointsRef}">Docs</a>
        <a class="btn" href="${app}">Get started</a>
      </div>
    </div>
  </nav>

  <header class="hero container">
    <div class="eyebrow">open source · self-hosted · graph-aware</div>
    <h1>The memory layer your agents <em>actually own</em></h1>
    <p>Persistent, graph-aware memory for any AI agent — fully open, nothing paywalled, running on infrastructure you control. Your memories never touch our servers.</p>
    <div class="hero-cta">
      <a class="btn" style="padding:12px 22px;font-size:15px" href="${app}">Deploy your own — free</a>
      <a class="btn ghost" style="padding:12px 22px;font-size:15px" href="${repo}" target="_blank" rel="noopener">View on GitHub</a>
    </div>
    <div class="hero-sub">MIT licensed · graph included · deploy to Cloudflare in ~5 min</div>
  </header>

  <div class="shot container">
    <div class="shot-top">
      <span class="shot-wm">memoryvault<span class="dot">.</span>md<span class="path">~/your-index</span></span>
      <span class="shot-tabs">All · Notes · Facts · Journal · Graph</span>
    </div>
    <div class="shot-body">
      <div class="shot-fm"><span class="t">Recall, <b>last 24 hours</b></span><span class="m">247 entries · 1,084 links · synced 14:02</span></div>
      <div class="shot-ticks">${ticks}</div>
      <div class="shot-sec">Active</div>
      <div class="shot-entry">
        <div>
          <div class="shot-eh"><span class="shot-bead"></span><span class="shot-et">project.license</span><span class="shot-kind">fact</span><span class="shot-ver">verified</span></div>
          <div class="shot-eb"><span class="k">project.license</span><span class="arr">&rarr;</span>MIT — fully open, graph features not paywalled.</div>
        </div>
        <div class="shot-aside">MV·0231 · 09:41<span class="shot-meter"><span class="bar"><i style="width:95%"></i></span></span></div>
      </div>
      <div class="shot-entry">
        <div>
          <div class="shot-eh"><span class="shot-bead"></span><span class="shot-et">Shipped the isolation suite</span><span class="shot-kind">journal</span></div>
          <div class="shot-eb">20 of 20 green against a live worker — tenant isolation proven, not asserted.</div>
        </div>
        <div class="shot-aside">MV·0247 · 14:02<span class="shot-meter"><span class="bar"><i style="width:88%"></i></span></span></div>
      </div>
    </div>
  </div>

  <section class="feature container" id="features">
    <div class="feature-grid">
      <div>
        <h2>A graph of memory, <em>nothing paywalled</em></h2>
        <p>Most memory tools lock the graph behind a Pro tier or keep it closed-source entirely. MemoryVault ships connected, graph-aware memory in the open — links, traversal, and provenance included from the first commit.</p>
        <p class="mini">notes · facts · journal — linked, scored, and queryable</p>
      </div>
      <div class="panel">
        <div class="ledger-line"><span class="k">project.license</span><span class="arr">&rarr;</span><span class="v">MIT, fully open</span><span class="tag">verified</span></div>
        <div class="ledger-line"><span class="k">user.timezone</span><span class="arr">&rarr;</span><span class="v">Europe/Lisbon</span><span class="tag">verified</span></div>
        <div class="ledger-line"><span class="k">graph.enabled</span><span class="arr">&rarr;</span><span class="v">true — no upsell</span></div>
      </div>
    </div>
  </section>

  <section class="feature container">
    <div class="feature-grid">
      <div class="panel config">
        <div class="c"># your account, your data</div>
        <div><span class="b">[[vectorize]]</span> binding = "MEMORY_INDEX"</div>
        <div><span class="b">[[d1_databases]]</span> name = "your-brain"</div>
        <div class="g">&#10003; memories never leave your Cloudflare account</div>
      </div>
      <div>
        <h2>Self-hosted. <em>You own the data.</em></h2>
        <p>Deploy to your own Cloudflare account in minutes. There's no MemoryVault server in the middle — your agent's memory lives in your D1 and Vectorize, under your keys. Nothing to trust us with.</p>
        <p class="mini">one-command deploy · D1 + Vectorize + Workers</p>
      </div>
    </div>
  </section>

  <section class="how container" id="how">
    <h2>How it works</h2>
    <p>Connect once. Your agents read and write memory before and after every answer.</p>
    <div class="steps">
      <div class="step"><div class="n">01</div><h3>Deploy your vault</h3><p>One command stands up your own memory server on Cloudflare — D1, Vectorize, and Workers, under your account.</p></div>
      <div class="step"><div class="n">02</div><h3>Connect your agents</h3><p>Point Claude, Codex, or any MCP client at your endpoint. They read your context before answering and write back what they learn.</p></div>
      <div class="step"><div class="n">03</div><h3>Watch it strengthen</h3><p>Memories reinforce, decay, and link over time. The index sharpens itself — you review what sticks.</p></div>
    </div>
  </section>

  <section class="agents container">
    <div class="lbl">Works with your stack</div>
    <div class="agent-row"><span class="chip">Claude</span><span class="chip">Claude Code</span><span class="chip">Codex</span><span class="chip">Any MCP client</span><span class="chip">REST API</span></div>
  </section>

  <section class="pricing container" id="pricing">
    <h2>Start free. Host it yourself, or let us.</h2>
    <p>The whole thing is open source. Pay only if you want managed hosting.</p>
    <div class="plans">
      <div class="plan">
        <div class="name">Self-host</div>
        <div class="price">$0</div>
        <div class="desc">Run it on your own Cloudflare account, forever.</div>
        <ul><li>Full source, MIT licensed</li><li>Graph + all features</li><li>Your data, your keys</li><li>Community support</li></ul>
        <a class="btn ghost cta" href="${repo}" target="_blank" rel="noopener">Deploy from GitHub</a>
      </div>
      <div class="plan feat">
        <div class="name">Hosted</div>
        <div class="price">$12<small>/mo</small></div>
        <div class="desc">We run it for you — backups, sync, and support.</div>
        <ul><li>Everything in Self-host</li><li>Managed multi-device sync</li><li>Automatic backups</li><li>Priority support</li></ul>
        <a class="btn cta" href="${repo}" target="_blank" rel="noopener">Coming soon</a>
        <div class="note">not yet billable — follow along on GitHub</div>
      </div>
    </div>
  </section>

  <section class="faq container" id="faq">
    <h2>Questions</h2>
    <div class="qa">
      <div class="item"><div class="q">Is it really fully open source?</div><div class="a">Yes — MIT licensed, graph and all. There's no paywalled tier of the software itself; the only thing you'd ever pay for is optional managed hosting.</div></div>
      <div class="item"><div class="q">Where does my data live?</div><div class="a">In your own Cloudflare account — your D1 database and Vectorize index, under your keys. If you self-host, your memories never touch our infrastructure.</div></div>
      <div class="item"><div class="q">Which agents does it work with?</div><div class="a">Anything that speaks MCP — Claude, Claude Code, Codex, and custom agents — plus a plain REST API for everything else.</div></div>
      <div class="item"><div class="q">How is it different from other memory tools?</div><div class="a">Graph-aware and open with nothing paywalled, self-hosted so you own the data, and tenant isolation that's covered by an adversarial test suite rather than asserted in a blog post.</div></div>
    </div>
  </section>

  <section class="final container">
    <h2>Give your agents a memory <em>you control</em></h2>
    <div class="hero-cta">
      <a class="btn" style="padding:13px 26px;font-size:15px" href="${app}">Deploy your own — free</a>
      <a class="btn ghost" style="padding:13px 26px;font-size:15px" href="${endpointsRef}">Read the docs</a>
    </div>
  </section>

  <footer><div class="container foot-in"><span>&copy; 2026 MemoryVault · MIT licensed · <a href="${endpointsRef}">endpoints</a></span><span class="ok">&#9679; all systems operational</span></div></footer>
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
<html lang="en" data-theme="slate">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MemoryVault — Endpoints</title>
${FONT_LINK_TAGS}
${themeBootstrapTag}
<style>
${vanillaTokensCss}${themeStyles}${pageChromeCss}  .wrap { max-width: 1180px; }
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
<html lang="en" data-theme="slate">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MemoryVault MCP</title>
${FONT_LINK_TAGS}
${themeBootstrapTag}
<style>
${vanillaTokensCss}${themeStyles}${pageChromeCss}  .wrap { max-width: 980px; }
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
<html lang="en" data-theme="slate">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${guide.title} · MemoryVault</title>
${FONT_LINK_TAGS}
${themeBootstrapTag}
<style>
${vanillaTokensCss}${themeStyles}${pageChromeCss}  .wrap { max-width: 920px; }
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
