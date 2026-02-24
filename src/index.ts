export interface Env {
  DB: D1Database;
  AUTH_SECRET: string;
}

function generateId(): string {
  return crypto.randomUUID();
}

function now(): number {
  return Math.floor(Date.now() / 1000);
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function unauthorized(): Response {
  return jsonResponse({ error: 'Unauthorized' }, 401);
}

function checkAuth(request: Request, env: Env): boolean {
  const auth = request.headers.get('Authorization');
  if (!auth) return false;
  const parts = auth.split(' ');
  return parts.length === 2 && parts[0] === 'Bearer' && parts[1] === env.AUTH_SECRET;
}

async function isRateLimited(ip: string, env: Env): Promise<boolean> {
  const window = Math.floor(Date.now() / (15 * 60 * 1000));
  const row = await env.DB.prepare(
    'SELECT count FROM rate_limits WHERE ip = ? AND window = ?'
  ).bind(ip, window).first<{ count: number }>();
  return (row?.count ?? 0) >= 10;
}

async function recordFailedAttempt(ip: string, env: Env): Promise<void> {
  const window = Math.floor(Date.now() / (15 * 60 * 1000));
  await env.DB.prepare(
    'INSERT INTO rate_limits (ip, window, count) VALUES (?, ?, 1) ON CONFLICT(ip, window) DO UPDATE SET count = count + 1'
  ).bind(ip, window).run();
  // 1% chance: delete rows older than 2 hours (8 windows) to prevent unbounded growth
  if (Math.random() < 0.01) {
    const cutoff = window - 8;
    await env.DB.prepare('DELETE FROM rate_limits WHERE window < ?').bind(cutoff).run();
  }
}

async function clearRateLimit(ip: string, env: Env): Promise<void> {
  const window = Math.floor(Date.now() / (15 * 60 * 1000));
  await env.DB.prepare(
    'DELETE FROM rate_limits WHERE ip = ? AND window = ?'
  ).bind(ip, window).run();
}

const VALID_TYPES = ['note', 'fact', 'journal'] as const;
type MemoryType = typeof VALID_TYPES[number];

function isValidType(t: unknown): t is MemoryType {
  return typeof t === 'string' && (VALID_TYPES as readonly string[]).includes(t);
}

const TOOLS = [
  {
    name: 'memory_save',
    description: 'Save a new memory. Use type="note" for titled knowledge entries, type="fact" for key=value pairs, type="journal" for free-form thoughts.',
    inputSchema: {
      type: 'object',
      properties: {
        type: { type: 'string', enum: ['note', 'fact', 'journal'], description: 'Memory type' },
        content: { type: 'string', description: 'The memory content' },
        title: { type: 'string', description: 'Title (for notes)' },
        key: { type: 'string', description: 'Key name (for facts, e.g. "user_name")' },
        tags: { type: 'string', description: 'Comma-separated tags' },
      },
      required: ['type', 'content'],
    },
  },
  {
    name: 'memory_get',
    description: 'Retrieve a specific memory by its ID.',
    inputSchema: {
      type: 'object',
      properties: { id: { type: 'string', description: 'Memory ID' } },
      required: ['id'],
    },
  },
  {
    name: 'memory_get_fact',
    description: 'Fast lookup of a fact memory by its key name.',
    inputSchema: {
      type: 'object',
      properties: { key: { type: 'string', description: 'The fact key to look up' } },
      required: ['key'],
    },
  },
  {
    name: 'memory_search',
    description: 'Search memories by text content across all memory types.',
    inputSchema: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Search query' },
        type: { type: 'string', enum: ['note', 'fact', 'journal'], description: 'Optionally filter by type' },
      },
      required: ['query'],
    },
  },
  {
    name: 'memory_list',
    description: 'List memories with optional filters by type or tag.',
    inputSchema: {
      type: 'object',
      properties: {
        type: { type: 'string', enum: ['note', 'fact', 'journal'], description: 'Filter by type' },
        tag: { type: 'string', description: 'Filter by tag' },
        limit: { type: 'number', description: 'Max results (1-100, default 20)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_update',
    description: 'Update an existing memory by ID. Only provided fields are updated.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Memory ID to update' },
        content: { type: 'string', description: 'New content' },
        title: { type: 'string', description: 'New title' },
        tags: { type: 'string', description: 'New comma-separated tags' },
      },
      required: ['id'],
    },
  },
  {
    name: 'memory_delete',
    description: 'Permanently delete a memory by ID.',
    inputSchema: {
      type: 'object',
      properties: { id: { type: 'string', description: 'Memory ID to delete' } },
      required: ['id'],
    },
  },
  {
    name: 'memory_stats',
    description: 'Get statistics about stored memories: counts by type and recent activity.',
    inputSchema: { type: 'object', properties: {}, required: [] },
  },
  {
    name: 'memory_link',
    description: 'Create a link between two memories. Use label to describe the relationship in natural language (e.g. "relates to work", "led to this decision", "supports", "contradicts").',
    inputSchema: {
      type: 'object',
      properties: {
        from_id: { type: 'string', description: 'Source memory ID' },
        to_id: { type: 'string', description: 'Target memory ID' },
        label: { type: 'string', description: 'Optional free-text description of the relationship' },
      },
      required: ['from_id', 'to_id'],
    },
  },
  {
    name: 'memory_unlink',
    description: 'Remove a link between two memories.',
    inputSchema: {
      type: 'object',
      properties: {
        from_id: { type: 'string', description: 'Source memory ID' },
        to_id: { type: 'string', description: 'Target memory ID' },
      },
      required: ['from_id', 'to_id'],
    },
  },
  {
    name: 'memory_links',
    description: 'Get all memories linked to a given memory, with full memory data and relationship labels. Use this to explore the knowledge graph around a memory.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Memory ID to get connections for' },
      },
      required: ['id'],
    },
  },
];

type ToolArgs = Record<string, unknown>;
type McpResult = { content: Array<{ type: string; text: string }> };

async function callTool(name: string, args: ToolArgs, env: Env): Promise<McpResult> {
  switch (name) {
    case 'memory_save': {
      const { type, content, title, key, tags } = args as {
        type: unknown; content: unknown; title?: unknown; key?: unknown; tags?: unknown;
      };
      if (!isValidType(type)) return { content: [{ type: 'text', text: 'Invalid type. Must be note, fact, or journal.' }] };
      if (typeof content !== 'string' || content.trim() === '') return { content: [{ type: 'text', text: 'content must be a non-empty string.' }] };
      const id = generateId();
      const ts = now();
      await env.DB.prepare(
        'INSERT INTO memories (id, type, title, key, content, tags, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
      ).bind(
        id, type,
        typeof title === 'string' ? title : null,
        typeof key === 'string' ? key : null,
        content.trim(),
        typeof tags === 'string' ? tags : null,
        ts, ts
      ).run();
      // Find up to 5 existing memories sharing at least one tag (for suggested linking)
      let suggestedLinks: unknown[] = [];
      if (typeof tags === 'string' && tags.trim()) {
        const tagList = tags.split(',').map((t: string) => t.trim()).filter(Boolean);
        if (tagList.length > 0) {
          const conditions = tagList.map(() => 'tags LIKE ?').join(' OR ');
          const bindings = tagList.map((t: string) => `%${t}%`);
          const suggestions = await env.DB.prepare(
            `SELECT id, type, title, key, tags FROM memories WHERE id != ? AND (${conditions}) LIMIT 5`
          ).bind(id, ...bindings).all();
          suggestedLinks = suggestions.results;
        }
      }

      const saveResult: Record<string, unknown> = { id, message: `Saved memory with id: ${id}` };
      if (suggestedLinks.length > 0) saveResult.suggested_links = suggestedLinks;
      return { content: [{ type: 'text', text: JSON.stringify(saveResult) }] };
    }

    case 'memory_get': {
      const { id } = args as { id: unknown };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };
      const row = await env.DB.prepare('SELECT * FROM memories WHERE id = ?').bind(id).first();
      if (!row) return { content: [{ type: 'text', text: 'Memory not found.' }] };
      return { content: [{ type: 'text', text: JSON.stringify(row, null, 2) }] };
    }

    case 'memory_get_fact': {
      const { key } = args as { key: unknown };
      if (typeof key !== 'string' || !key) return { content: [{ type: 'text', text: 'key must be a non-empty string.' }] };
      const row = await env.DB.prepare(
        'SELECT * FROM memories WHERE type = ? AND key = ? LIMIT 1'
      ).bind('fact', key).first();
      if (!row) return { content: [{ type: 'text', text: `No fact found with key: ${key}` }] };
      return { content: [{ type: 'text', text: JSON.stringify(row, null, 2) }] };
    }

    case 'memory_search': {
      const { query, type } = args as { query: unknown; type?: unknown };
      if (typeof query !== 'string' || query.trim() === '') return { content: [{ type: 'text', text: 'query must be a non-empty string.' }] };
      if (type !== undefined && !isValidType(type)) return { content: [{ type: 'text', text: 'Invalid type filter.' }] };
      const like = `%${query.trim()}%`;
      let stmt;
      if (type) {
        stmt = env.DB.prepare(
          'SELECT * FROM memories WHERE type = ? AND (content LIKE ? OR title LIKE ? OR key LIKE ?) ORDER BY created_at DESC LIMIT 20'
        ).bind(type, like, like, like);
      } else {
        stmt = env.DB.prepare(
          'SELECT * FROM memories WHERE content LIKE ? OR title LIKE ? OR key LIKE ? ORDER BY created_at DESC LIMIT 20'
        ).bind(like, like, like);
      }
      const results = await stmt.all();
      if (!results.results.length) return { content: [{ type: 'text', text: 'No memories found.' }] };
      return { content: [{ type: 'text', text: JSON.stringify(results.results, null, 2) }] };
    }

    case 'memory_list': {
      const { type, tag, limit: rawLimit } = args as { type?: unknown; tag?: unknown; limit?: unknown };
      if (type !== undefined && !isValidType(type)) return { content: [{ type: 'text', text: 'Invalid type filter.' }] };
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 20, 1), 100);
      let query = 'SELECT * FROM memories WHERE 1=1';
      const params: unknown[] = [];
      if (type) { query += ' AND type = ?'; params.push(type); }
      if (typeof tag === 'string' && tag) { query += ' AND tags LIKE ?'; params.push(`%${tag}%`); }
      query += ' ORDER BY created_at DESC LIMIT ?';
      params.push(limit);
      const results = await env.DB.prepare(query).bind(...params).all();
      if (!results.results.length) return { content: [{ type: 'text', text: 'No memories found.' }] };
      return { content: [{ type: 'text', text: JSON.stringify(results.results, null, 2) }] };
    }

    case 'memory_update': {
      const { id, content, title, tags } = args as {
        id: unknown; content?: unknown; title?: unknown; tags?: unknown;
      };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };
      const existing = await env.DB.prepare('SELECT * FROM memories WHERE id = ?').bind(id).first<{
        content: string; title: string | null; tags: string | null;
      }>();
      if (!existing) return { content: [{ type: 'text', text: 'Memory not found.' }] };
      await env.DB.prepare(
        'UPDATE memories SET content = ?, title = ?, tags = ?, updated_at = ? WHERE id = ?'
      ).bind(
        typeof content === 'string' && content.trim() ? content.trim() : existing.content,
        typeof title === 'string' ? title : existing.title,
        typeof tags === 'string' ? tags : existing.tags,
        now(),
        id
      ).run();
      return { content: [{ type: 'text', text: `Memory ${id} updated.` }] };
    }

    case 'memory_delete': {
      const { id } = args as { id: unknown };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };
      const result = await env.DB.prepare('DELETE FROM memories WHERE id = ?').bind(id).run();
      if (result.meta.changes === 0) return { content: [{ type: 'text', text: 'Memory not found.' }] };
      return { content: [{ type: 'text', text: `Memory ${id} deleted.` }] };
    }

    case 'memory_stats': {
      const total = await env.DB.prepare('SELECT COUNT(*) as count FROM memories').first<{ count: number }>();
      const byType = await env.DB.prepare('SELECT type, COUNT(*) as count FROM memories GROUP BY type').all();
      const recent = await env.DB.prepare(
        'SELECT id, type, title, key, created_at FROM memories ORDER BY created_at DESC LIMIT 5'
      ).all();
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            total: total?.count ?? 0,
            by_type: byType.results,
            recent_5: recent.results,
          }, null, 2),
        }],
      };
    }

    case 'memory_link': {
      const { from_id, to_id, label } = args as { from_id: unknown; to_id: unknown; label?: unknown };
      if (typeof from_id !== 'string' || !from_id) return { content: [{ type: 'text', text: 'from_id must be a non-empty string.' }] };
      if (typeof to_id !== 'string' || !to_id) return { content: [{ type: 'text', text: 'to_id must be a non-empty string.' }] };
      if (from_id === to_id) return { content: [{ type: 'text', text: 'Cannot link a memory to itself.' }] };

      // Verify both memories exist
      const fromMem = await env.DB.prepare('SELECT id FROM memories WHERE id = ?').bind(from_id).first();
      if (!fromMem) return { content: [{ type: 'text', text: `Memory not found: ${from_id}` }] };
      const toMem = await env.DB.prepare('SELECT id FROM memories WHERE id = ?').bind(to_id).first();
      if (!toMem) return { content: [{ type: 'text', text: `Memory not found: ${to_id}` }] };

      // Prevent duplicate links (either direction)
      const existing = await env.DB.prepare(
        'SELECT id FROM memory_links WHERE (from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?)'
      ).bind(from_id, to_id, to_id, from_id).first();
      if (existing) return { content: [{ type: 'text', text: 'A link between these memories already exists.' }] };

      const link_id = generateId();
      const labelVal = typeof label === 'string' && label.trim() ? label.trim() : null;
      await env.DB.prepare(
        'INSERT INTO memory_links (id, from_id, to_id, label, created_at) VALUES (?, ?, ?, ?, ?)'
      ).bind(link_id, from_id, to_id, labelVal, now()).run();

      return { content: [{ type: 'text', text: JSON.stringify({ link_id, from_id, to_id, label: labelVal }) }] };
    }

    case 'memory_unlink': {
      const { from_id, to_id } = args as { from_id: unknown; to_id: unknown };
      if (typeof from_id !== 'string' || !from_id) return { content: [{ type: 'text', text: 'from_id must be a non-empty string.' }] };
      if (typeof to_id !== 'string' || !to_id) return { content: [{ type: 'text', text: 'to_id must be a non-empty string.' }] };

      const result = await env.DB.prepare(
        'DELETE FROM memory_links WHERE (from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?)'
      ).bind(from_id, to_id, to_id, from_id).run();

      if (result.meta.changes === 0) return { content: [{ type: 'text', text: 'No link found between these memories.' }] };
      return { content: [{ type: 'text', text: `Link removed between ${from_id} and ${to_id}.` }] };
    }

    case 'memory_links': {
      const { id } = args as { id: unknown };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };

      // Verify memory exists
      const mem = await env.DB.prepare('SELECT id FROM memories WHERE id = ?').bind(id).first();
      if (!mem) return { content: [{ type: 'text', text: 'Memory not found.' }] };

      // Fetch links in both directions with full memory data
      const fromLinks = await env.DB.prepare(
        'SELECT ml.id as link_id, ml.label, ml.to_id as connected_id, m.* FROM memory_links ml JOIN memories m ON m.id = ml.to_id WHERE ml.from_id = ?'
      ).bind(id).all();

      const toLinks = await env.DB.prepare(
        'SELECT ml.id as link_id, ml.label, ml.from_id as connected_id, m.* FROM memory_links ml JOIN memories m ON m.id = ml.from_id WHERE ml.to_id = ?'
      ).bind(id).all();

      const results = [
        ...fromLinks.results.map((r: Record<string, unknown>) => ({
          link_id: r.link_id,
          label: r.label,
          direction: 'from',
          memory: { id: r.id, type: r.type, title: r.title, key: r.key, content: r.content, tags: r.tags, created_at: r.created_at, updated_at: r.updated_at },
        })),
        ...toLinks.results.map((r: Record<string, unknown>) => ({
          link_id: r.link_id,
          label: r.label,
          direction: 'to',
          memory: { id: r.id, type: r.type, title: r.title, key: r.key, content: r.content, tags: r.tags, created_at: r.created_at, updated_at: r.updated_at },
        })),
      ];

      if (!results.length) return { content: [{ type: 'text', text: 'No links found for this memory.' }] };
      return { content: [{ type: 'text', text: JSON.stringify(results, null, 2) }] };
    }

    default:
      throw Object.assign(new Error(`Unknown tool: ${name}`), { code: -32601 });
  }
}

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept',
};

async function processMcpBody(
  body: { jsonrpc: string; id?: unknown; method: string; params?: Record<string, unknown> },
  env: Env
): Promise<unknown> {
  const { id, method, params = {} } = body;

  if (method === 'initialize') {
    return {
      jsonrpc: '2.0', id,
      result: {
        protocolVersion: '2024-11-05',
        capabilities: { tools: {} },
        serverInfo: { name: 'ai-memory', version: '1.0.0' },
      },
    };
  }

  if (method === 'tools/list') {
    return { jsonrpc: '2.0', id, result: { tools: TOOLS } };
  }

  if (method === 'tools/call') {
    const { name, arguments: toolArgs = {} } = params as { name: string; arguments?: ToolArgs };
    const result = await callTool(name, toolArgs, env);
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

async function handleMcp(request: Request, env: Env, url: URL): Promise<Response> {
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
      responseObj = await processMcpBody(body, env);
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

async function handleApiMemories(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const type = url.searchParams.get('type') ?? '';
  const search = url.searchParams.get('search') ?? '';
  const limitParam = parseInt(url.searchParams.get('limit') ?? '100', 10);
  const limit = Math.min(Math.max(Number.isNaN(limitParam) ? 100 : limitParam, 1), 500);

  let query = 'SELECT m.*, (SELECT COUNT(*) FROM memory_links ml WHERE ml.from_id = m.id OR ml.to_id = m.id) as link_count FROM memories m WHERE 1=1';
  const params: unknown[] = [];
  if (type && VALID_TYPES.includes(type as MemoryType)) {
    query += ' AND type = ?'; params.push(type);
  }
  if (search) {
    const like = `%${search}%`;
    query += ' AND (content LIKE ? OR title LIKE ? OR key LIKE ?)';
    params.push(like, like, like);
  }
  query += ' ORDER BY m.created_at DESC LIMIT ?';
  params.push(limit);

  const results = await env.DB.prepare(query).bind(...params).all();
  const stats = await env.DB.prepare('SELECT type, COUNT(*) as count FROM memories GROUP BY type').all();
  return new Response(JSON.stringify({ memories: results.results, stats: stats.results }), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

async function handleApiLinks(memoryId: string, env: Env): Promise<Response> {
  const mem = await env.DB.prepare('SELECT id FROM memories WHERE id = ?').bind(memoryId).first();
  if (!mem) return new Response(JSON.stringify({ error: 'Memory not found.' }), {
    status: 404, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });

  const fromLinks = await env.DB.prepare(
    'SELECT ml.id as link_id, ml.label, m.id, m.type, m.title, m.key, m.content, m.tags, m.created_at, m.updated_at FROM memory_links ml JOIN memories m ON m.id = ml.to_id WHERE ml.from_id = ?'
  ).bind(memoryId).all();

  const toLinks = await env.DB.prepare(
    'SELECT ml.id as link_id, ml.label, m.id, m.type, m.title, m.key, m.content, m.tags, m.created_at, m.updated_at FROM memory_links ml JOIN memories m ON m.id = ml.from_id WHERE ml.to_id = ?'
  ).bind(memoryId).all();

  const results = [
    ...fromLinks.results.map((r: Record<string, unknown>) => ({
      link_id: r.link_id, label: r.label, direction: 'from',
      memory: { id: r.id, type: r.type, title: r.title, key: r.key, content: r.content, tags: r.tags, created_at: r.created_at, updated_at: r.updated_at },
    })),
    ...toLinks.results.map((r: Record<string, unknown>) => ({
      link_id: r.link_id, label: r.label, direction: 'to',
      memory: { id: r.id, type: r.type, title: r.title, key: r.key, content: r.content, tags: r.tags, created_at: r.created_at, updated_at: r.updated_at },
    })),
  ];

  return new Response(JSON.stringify(results), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

async function handleApiGraph(env: Env): Promise<Response> {
  const memories = await env.DB.prepare(
    'SELECT id, type, title, key, tags FROM memories ORDER BY created_at DESC LIMIT 1000'
  ).all();
  const links = await env.DB.prepare(
    'SELECT id, from_id, to_id, label FROM memory_links LIMIT 5000'
  ).all();

  return new Response(JSON.stringify({ nodes: memories.results, edges: links.results }), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

function viewerHtml(): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MEMORY VAULT</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<style>
  :root {
    --bg: #080c10;
    --bg2: #0d1219;
    --bg3: #111820;
    --border: #1e2d3d;
    --border-bright: #2a4060;
    --amber: #f0a500;
    --amber-dim: #7a5200;
    --amber-glow: rgba(240,165,0,0.12);
    --teal: #00c8b4;
    --red: #e05050;
    --text: #c8d8e8;
    --text-dim: #4a6070;
    --text-bright: #e8f4ff;
    --mono: 'Share Tech Mono', monospace;
    --sans: 'Syne', sans-serif;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--mono);
    min-height: 100vh;
    overflow-x: hidden;
  }

  .stat-pill, .refresh-btn, .logout-btn, .login-btn, .card, .connection-chip, .expand-close {
    touch-action: manipulation;
  }

  /* Scanline overlay */
  body::before {
    content: '';
    position: fixed;
    inset: 0;
    background: repeating-linear-gradient(
      0deg,
      transparent,
      transparent 2px,
      rgba(0,0,0,0.08) 2px,
      rgba(0,0,0,0.08) 4px
    );
    pointer-events: none;
    z-index: 9999;
  }

  /* ── LOGIN SCREEN ── */
  #login-screen {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    min-height: 100vh;
    padding: 2rem;
    animation: fadeIn 0.6s ease;
  }
  .login-box {
    width: 100%;
    max-width: 420px;
    border: 1px solid var(--border-bright);
    background: var(--bg2);
    padding: 3rem 2.5rem;
    position: relative;
  }
  .login-box::before {
    content: 'CLASSIFIED';
    position: absolute;
    top: -1px; left: 2rem;
    background: var(--amber);
    color: var(--bg);
    font-family: var(--mono);
    font-size: 0.6rem;
    font-weight: 700;
    letter-spacing: 0.2em;
    padding: 0.2rem 0.6rem;
  }
  .login-box::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, var(--amber), transparent);
  }
  .vault-logo {
    font-family: var(--sans);
    font-weight: 800;
    font-size: 2.2rem;
    letter-spacing: -0.02em;
    color: var(--text-bright);
    margin-bottom: 0.3rem;
  }
  .vault-logo span { color: var(--amber); }
  .vault-sub {
    font-size: 0.68rem;
    letter-spacing: 0.2em;
    color: var(--text-dim);
    text-transform: uppercase;
    margin-bottom: 2.5rem;
  }
  .field-label {
    font-size: 0.65rem;
    letter-spacing: 0.18em;
    color: var(--amber);
    text-transform: uppercase;
    margin-bottom: 0.5rem;
  }
  .token-input {
    width: 100%;
    background: var(--bg3);
    border: 1px solid var(--border);
    color: var(--teal);
    font-family: var(--mono);
    font-size: 0.85rem;
    padding: 0.75rem 1rem;
    outline: none;
    transition: border-color 0.2s;
    letter-spacing: 0.05em;
  }
  .token-input:focus { border-color: var(--amber); }
  .token-input::placeholder { color: var(--text-dim); }
  .login-btn {
    width: 100%;
    margin-top: 1.5rem;
    background: var(--amber);
    color: var(--bg);
    border: none;
    font-family: var(--mono);
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    padding: 0.9rem;
    cursor: pointer;
    transition: background 0.15s, transform 0.1s;
  }
  .login-btn:hover { background: #ffbc20; }
  .login-btn:active { transform: scale(0.99); }
  .login-error {
    margin-top: 1rem;
    font-size: 0.7rem;
    color: var(--red);
    letter-spacing: 0.1em;
    display: none;
  }

  /* ── MAIN APP ── */
  #app { display: none; flex-direction: column; min-height: 100vh; animation: fadeIn 0.4s ease; }

  /* Header */
  .hdr {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 1rem 2rem;
    border-bottom: 1px solid var(--border);
    background: var(--bg2);
    position: sticky;
    top: 0;
    z-index: 100;
  }
  .hdr-brand {
    font-family: var(--sans);
    font-weight: 800;
    font-size: 1.2rem;
    letter-spacing: -0.02em;
    color: var(--text-bright);
  }
  .hdr-brand span { color: var(--amber); }
  .hdr-meta {
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    color: var(--text-dim);
    text-align: right;
  }
  .hdr-meta strong { color: var(--amber); }
  .logout-btn {
    background: none;
    border: 1px solid var(--border);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.65rem;
    letter-spacing: 0.12em;
    padding: 0.35rem 0.8rem;
    cursor: pointer;
    transition: border-color 0.2s, color 0.2s;
    margin-left: 1.5rem;
    text-transform: uppercase;
  }
  .logout-btn:hover { border-color: var(--red); color: var(--red); }

  /* Stats bar */
  .stats-bar {
    display: flex;
    gap: 1px;
    background: var(--border);
    border-bottom: 1px solid var(--border);
  }
  .stat-pill {
    flex: 1;
    padding: 0.6rem 1.5rem;
    background: var(--bg2);
    text-align: center;
    cursor: pointer;
    transition: background 0.15s;
    position: relative;
  }
  .stat-pill:hover, .stat-pill.active { background: var(--bg3); }
  .stat-pill.active::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 2px;
    background: var(--amber);
  }
  .stat-num {
    font-family: var(--sans);
    font-size: 1.6rem;
    font-weight: 800;
    color: var(--amber);
    line-height: 1;
  }
  .stat-label {
    font-size: 0.6rem;
    letter-spacing: 0.18em;
    color: var(--text-dim);
    text-transform: uppercase;
    margin-top: 0.2rem;
  }

  /* Controls */
  .controls {
    display: flex;
    gap: 0.75rem;
    padding: 1rem 2rem;
    background: var(--bg2);
    border-bottom: 1px solid var(--border);
    flex-wrap: wrap;
    align-items: center;
  }
  .search-wrap {
    flex: 1;
    min-width: 200px;
    position: relative;
  }
  .search-wrap::before {
    content: '//';
    position: absolute;
    left: 0.75rem;
    top: 50%;
    transform: translateY(-50%);
    color: var(--amber);
    font-size: 0.75rem;
    pointer-events: none;
  }
  .search-input {
    width: 100%;
    background: var(--bg3);
    border: 1px solid var(--border);
    color: var(--text);
    font-family: var(--mono);
    font-size: 0.8rem;
    padding: 0.55rem 0.75rem 0.55rem 2.2rem;
    outline: none;
    transition: border-color 0.2s;
  }
  .search-input:focus { border-color: var(--amber); }
  .search-input::placeholder { color: var(--text-dim); }
  .filter-btn {
    background: var(--bg3);
    border: 1px solid var(--border);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.65rem;
    letter-spacing: 0.12em;
    padding: 0.55rem 1rem;
    cursor: pointer;
    text-transform: uppercase;
    transition: all 0.15s;
  }
  .filter-btn:hover { border-color: var(--amber-dim); color: var(--text); }
  .filter-btn.active { border-color: var(--amber); color: var(--amber); background: var(--amber-glow); }
  .refresh-btn {
    background: none;
    border: 1px solid var(--border-bright);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.65rem;
    padding: 0.55rem 0.9rem;
    cursor: pointer;
    letter-spacing: 0.1em;
    transition: all 0.15s;
    text-transform: uppercase;
  }
  .refresh-btn:hover { color: var(--teal); border-color: var(--teal); }

  /* Memory grid */
  .grid-wrap {
    flex: 1;
    padding: 1.5rem 2rem;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 1px;
    background: var(--border);
    align-content: start;
  }
  .empty-state {
    grid-column: 1/-1;
    padding: 5rem 2rem;
    text-align: center;
    color: var(--text-dim);
    font-size: 0.75rem;
    letter-spacing: 0.15em;
    text-transform: uppercase;
  }
  .empty-state .empty-icon { font-size: 2.5rem; margin-bottom: 1rem; opacity: 0.3; }

  /* Memory card */
  .card {
    background: var(--bg2);
    padding: 1.25rem 1.5rem;
    position: relative;
    transition: background 0.15s;
    animation: slideUp 0.3s ease backwards;
    cursor: default;
  }
  .card:hover { background: var(--bg3); }
  .card-type-stripe {
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 3px;
  }
  .card[data-type="note"] .card-type-stripe { background: var(--teal); }
  .card[data-type="fact"] .card-type-stripe { background: var(--amber); }
  .card[data-type="journal"] .card-type-stripe { background: #8888ff; }

  .card-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
  }
  .card-type-badge {
    font-size: 0.55rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    padding: 0.2rem 0.5rem;
    border: 1px solid;
    flex-shrink: 0;
  }
  .card[data-type="note"] .card-type-badge { border-color: var(--teal); color: var(--teal); }
  .card[data-type="fact"] .card-type-badge { border-color: var(--amber); color: var(--amber); }
  .card[data-type="journal"] .card-type-badge { border-color: #8888ff; color: #8888ff; }

  .card-title {
    font-family: var(--sans);
    font-size: 0.9rem;
    font-weight: 700;
    color: var(--text-bright);
    letter-spacing: -0.01em;
    line-height: 1.3;
    word-break: break-word;
  }
  .card-key {
    font-size: 0.7rem;
    color: var(--amber);
    letter-spacing: 0.08em;
    margin-bottom: 0.5rem;
  }
  .card-key span { color: var(--text-dim); }
  .card-content {
    font-size: 0.78rem;
    color: var(--text);
    line-height: 1.65;
    word-break: break-word;
    white-space: pre-wrap;
    max-height: 120px;
    overflow: hidden;
    position: relative;
  }
  .card-content::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 40px;
    background: linear-gradient(transparent, var(--bg2));
    pointer-events: none;
  }
  .card:hover .card-content::after {
    background: linear-gradient(transparent, var(--bg3));
  }
  .card-footer {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-top: 1rem;
    padding-top: 0.75rem;
    border-top: 1px solid var(--border);
  }
  .card-tags {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
  }
  .tag {
    font-size: 0.55rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    background: var(--bg);
    border: 1px solid var(--border);
    color: var(--text-dim);
    padding: 0.15rem 0.4rem;
  }
  .card-date {
    font-size: 0.6rem;
    color: var(--text-dim);
    letter-spacing: 0.05em;
    white-space: nowrap;
    flex-shrink: 0;
  }
  .card-id {
    font-size: 0.55rem;
    color: var(--text-dim);
    opacity: 0.5;
    letter-spacing: 0.04em;
    margin-top: 0.3rem;
  }

  /* Expand overlay */
  .expand-overlay {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(4,8,14,0.92);
    z-index: 200;
    padding: 2rem;
    overflow-y: auto;
    animation: fadeIn 0.2s ease;
  }
  .expand-overlay.open { display: flex; align-items: flex-start; justify-content: center; }
  .expand-box {
    width: 100%;
    max-width: 680px;
    background: var(--bg2);
    border: 1px solid var(--border-bright);
    padding: 2rem;
    position: relative;
    margin-top: 3rem;
    animation: slideUp 0.25s ease;
  }
  .expand-close {
    position: absolute;
    top: 1rem; right: 1rem;
    background: none;
    border: 1px solid var(--border);
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.7rem;
    padding: 0.3rem 0.6rem;
    cursor: pointer;
    transition: all 0.15s;
    letter-spacing: 0.1em;
  }
  .expand-close:hover { border-color: var(--red); color: var(--red); }
  .expand-content {
    font-size: 0.82rem;
    color: var(--text);
    line-height: 1.75;
    white-space: pre-wrap;
    word-break: break-word;
    margin-top: 1rem;
  }

  /* Loading */
  .loading {
    grid-column: 1/-1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 4rem;
    gap: 0.5rem;
    color: var(--amber);
    font-size: 0.7rem;
    letter-spacing: 0.2em;
  }
  .loading-dot {
    width: 4px; height: 4px;
    background: var(--amber);
    border-radius: 50%;
    animation: blink 1s infinite;
  }
  .loading-dot:nth-child(2) { animation-delay: 0.2s; }
  .loading-dot:nth-child(3) { animation-delay: 0.4s; }

  /* Footer */
  .footer {
    padding: 0.75rem 2rem;
    border-top: 1px solid var(--border);
    background: var(--bg2);
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .footer-text { font-size: 0.6rem; color: var(--text-dim); letter-spacing: 0.1em; text-transform: uppercase; }
  .cursor-blink {
    display: inline-block;
    width: 7px; height: 13px;
    background: var(--amber);
    margin-left: 3px;
    vertical-align: middle;
    animation: blink 1s infinite;
  }

  .card-links-badge {
    font-size: 0.58rem;
    letter-spacing: 0.1em;
    color: var(--teal);
    border: 1px solid var(--teal);
    padding: 0.15rem 0.4rem;
    opacity: 0.8;
  }
  .connections-section { margin-top: 1.5rem; padding-top: 1rem; border-top: 1px solid var(--border); }
  .connections-title { font-size: 0.6rem; letter-spacing: 0.2em; color: var(--amber); text-transform: uppercase; margin-bottom: 0.75rem; }
  .connection-chip {
    display: inline-flex; align-items: center; gap: 0.4rem;
    background: var(--bg3); border: 1px solid var(--border);
    padding: 0.35rem 0.7rem; margin: 0.25rem 0.25rem 0.25rem 0;
    cursor: pointer; transition: border-color 0.15s;
    font-size: 0.72rem; color: var(--text);
  }
  .connection-chip:hover { border-color: var(--amber); color: var(--amber); }
  .connection-chip .chip-type { font-size: 0.55rem; letter-spacing: 0.15em; text-transform: uppercase; opacity: 0.6; }
  .connection-chip .chip-label { font-size: 0.6rem; color: var(--text-dim); font-style: italic; }
  .graph-node circle { stroke-width: 2px; cursor: pointer; transition: r 0.15s; }
  .graph-node circle:hover { r: 10; }
  .graph-node text { font-family: var(--mono); font-size: 10px; fill: var(--text); pointer-events: none; }
  .graph-link { stroke: var(--border-bright); stroke-width: 1.5px; }
  .graph-link-label { font-family: var(--mono); font-size: 9px; fill: var(--text-dim); pointer-events: none; }

  @media (max-width: 900px) {
    .hdr { padding: 0.85rem 1rem; }
    .controls { padding: 0.75rem 1rem; }
    .grid-wrap { padding: 1rem; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); }
    .footer { padding: 0.65rem 1rem; flex-wrap: wrap; gap: 0.45rem; }
  }

  @media (max-width: 640px) {
    body::before { display: none; }
    #login-screen { padding: 1rem; }
    .login-box { padding: 2rem 1rem 1.5rem; }
    .login-box::before { left: 1rem; }
    .vault-logo { font-size: 1.65rem; }
    .vault-sub { margin-bottom: 1.5rem; font-size: 0.62rem; }
    .token-input, .search-input { font-size: 16px; }

    .hdr {
      position: static;
      flex-direction: column;
      align-items: flex-start;
      gap: 0.65rem;
      padding: 0.75rem 0.75rem 0.6rem;
    }
    .hdr-brand { font-size: 1.05rem; }
    .hdr-right {
      width: 100%;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.6rem;
    }
    .hdr-meta { text-align: left; font-size: 0.58rem; letter-spacing: 0.08em; }
    #live-indicator { font-size: 0.54rem !important; letter-spacing: 0.12em !important; }
    .logout-btn {
      margin-left: 0;
      min-height: 38px;
      padding: 0.45rem 0.72rem;
      font-size: 0.62rem;
    }

    .stats-bar {
      overflow-x: auto;
      overflow-y: hidden;
      -webkit-overflow-scrolling: touch;
      scrollbar-width: none;
    }
    .stats-bar::-webkit-scrollbar { display: none; }
    .stat-pill {
      flex: 0 0 88px;
      padding: 0.55rem 0.4rem;
    }
    .stat-num { font-size: 1.1rem; }
    .stat-label { font-size: 0.55rem; letter-spacing: 0.14em; }

    .controls {
      flex-direction: column;
      align-items: stretch;
      padding: 0.65rem 0.75rem;
      gap: 0.55rem;
    }
    .search-wrap { min-width: 0; width: 100%; }
    .refresh-btn {
      width: 100%;
      min-height: 42px;
      font-size: 0.62rem;
    }

    #graph-view { min-height: 54vh !important; }
    #graph-svg { min-height: 54vh !important; height: 54vh !important; }
    .graph-link-label { display: none; }

    .grid-wrap {
      padding: 0.5rem;
      grid-template-columns: 1fr;
      gap: 1px;
    }
    .card { padding: 1rem 1rem 0.95rem; }
    .card-content { max-height: 96px; }
    .card-footer {
      flex-direction: column;
      align-items: flex-start;
      gap: 0.45rem;
    }
    .card-date { align-self: flex-end; font-size: 0.58rem; }

    .expand-overlay {
      padding: 0;
      align-items: stretch;
    }
    .expand-box {
      margin-top: 0;
      max-width: none;
      min-height: 100vh;
      border: none;
      border-top: 1px solid var(--border-bright);
      padding: 3.25rem 1rem 1.25rem;
    }
    .expand-close {
      top: 0.65rem;
      right: 0.65rem;
      padding: 0.45rem 0.7rem;
      font-size: 0.62rem;
    }
    .expand-content { font-size: 0.8rem; line-height: 1.7; }
    .connection-chip {
      display: flex;
      width: 100%;
      margin-right: 0;
    }

    .footer { padding: 0.55rem 0.75rem; }
    .footer-text { font-size: 0.52rem; letter-spacing: 0.08em; }
    .footer .footer-text:last-child { display: none; }
  }
  @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
  @keyframes slideUp { from { opacity: 0; transform: translateY(16px); } to { opacity: 1; transform: translateY(0); } }
  @keyframes blink { 0%,100% { opacity: 1; } 50% { opacity: 0; } }
</style>
</head>
<body>

<!-- LOGIN -->
<div id="login-screen">
  <div class="login-box">
    <div class="vault-logo">MEMORY<span>VAULT</span></div>
    <div class="vault-sub">Secure Access Required</div>
    <div class="field-label">Access Token</div>
    <input type="password" class="token-input" id="token-input" placeholder="Enter bearer token..." autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false">
    <button class="login-btn" onclick="doLogin()">AUTHENTICATE →</button>
    <div class="login-error" id="login-error">⚠ ACCESS DENIED — invalid token</div>
  </div>
</div>

<!-- APP -->
<div id="app">
  <header class="hdr">
    <div class="hdr-brand">MEMORY<span>VAULT</span></div>
    <div class="hdr-right">
      <div class="hdr-meta">
        <div id="hdr-count">— entries</div>
        <div id="hdr-time"></div>
      </div>
      <div id="live-indicator" style="font-size:0.6rem;letter-spacing:0.15em;color:var(--text-dim);display:none;align-items:center">
        <span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--teal);margin-right:4px;animation:blink 2s infinite"></span>LIVE
      </div>
      <button class="logout-btn" onclick="doLogout()">LOCK</button>
    </div>
  </header>

  <div class="stats-bar">
    <div class="stat-pill active" id="stat-all" onclick="setFilter('')">
      <div class="stat-num" id="count-all">0</div>
      <div class="stat-label">All</div>
    </div>
    <div class="stat-pill" id="stat-note" onclick="setFilter('note')">
      <div class="stat-num" id="count-note">0</div>
      <div class="stat-label">Notes</div>
    </div>
    <div class="stat-pill" id="stat-fact" onclick="setFilter('fact')">
      <div class="stat-num" id="count-fact">0</div>
      <div class="stat-label">Facts</div>
    </div>
    <div class="stat-pill" id="stat-journal" onclick="setFilter('journal')">
      <div class="stat-num" id="count-journal">0</div>
      <div class="stat-label">Journal</div>
    </div>
    <div class="stat-pill" id="stat-graph" onclick="showGraph()">
      <div class="stat-num">⬡</div>
      <div class="stat-label">Graph</div>
    </div>
  </div>

  <div class="controls">
    <div class="search-wrap">
      <input type="text" class="search-input" id="search-input" placeholder="Search memories..." inputmode="search" oninput="onSearch(this.value)">
    </div>
    <button class="refresh-btn" onclick="loadMemories()">↻ REFRESH</button>
  </div>

  <div id="graph-view" style="display:none;flex:1;position:relative;background:var(--bg);min-height:600px">
    <svg id="graph-svg" style="width:100%;height:100%;min-height:600px"></svg>
    <div id="graph-empty" style="display:none;position:absolute;inset:0;align-items:center;justify-content:center;color:var(--text-dim);font-size:0.75rem;letter-spacing:0.15em">NO CONNECTIONS YET</div>
  </div>
  <div class="grid-wrap" id="grid">
    <div class="loading"><div class="loading-dot"></div><div class="loading-dot"></div><div class="loading-dot"></div></div>
  </div>

  <footer class="footer">
    <div class="footer-text">AI MEMORY MCP · CLOUDFLARE D1</div>
    <div class="footer-text">SECURE SESSION<span class="cursor-blink"></span></div>
  </footer>
</div>

<!-- EXPAND OVERLAY -->
<div class="expand-overlay" id="expand-overlay" onclick="closeExpand(event)">
  <div class="expand-box">
    <button class="expand-close" onclick="closeExpandBtn()">✕ CLOSE</button>
    <div id="expand-header"></div>
    <div class="expand-content" id="expand-content"></div>
    <div style="margin-top:1.5rem;padding-top:1rem;border-top:1px solid var(--border);font-size:0.6rem;color:var(--text-dim);letter-spacing:0.08em" id="expand-meta"></div>
    <div id="expand-connections"></div>
  </div>
</div>

<script>
  const BASE = location.origin;
  let TOKEN = '';
  let activeFilter = '';
  let searchTimeout = null;
  let allMemories = [];
  let expandGen = 0;
  let graphVisible = false;
  let lastGraphData = { nodes: [], edges: [] };
  let graphResizeTimer = null;

  function doLogin() {
    const val = document.getElementById('token-input').value.trim();
    if (!val) return;
    // Test the token by calling the API
    fetch(BASE + '/api/memories', {
      headers: { 'Authorization': 'Bearer ' + val }
    }).then(r => {
      if (r.ok) {
        TOKEN = val;
        document.getElementById('login-screen').style.display = 'none';
        document.getElementById('app').style.display = 'flex';
        document.getElementById('app').style.flexDirection = 'column';
        updateTime();
        loadMemories();
        startLivePolling();
      } else {
        document.getElementById('login-error').style.display = 'block';
        document.getElementById('token-input').style.borderColor = 'var(--red)';
      }
    }).catch(() => {
      document.getElementById('login-error').style.display = 'block';
    });
  }

  function doLogout() {
    TOKEN = '';
    location.reload();
  }

  function updateTime() {
    const el = document.getElementById('hdr-time');
    if (el) el.textContent = new Date().toISOString().replace('T',' ').slice(0,19) + ' UTC';
    setTimeout(updateTime, 1000);
  }

  async function loadMemories(silent = false) {
    const grid = document.getElementById('grid');
    const scrollY = window.scrollY;
    if (!silent) {
      grid.innerHTML = '<div class="loading"><div class="loading-dot"></div><div class="loading-dot"></div><div class="loading-dot"></div></div>';
    }
    const search = document.getElementById('search-input').value;
    let url = BASE + '/api/memories?limit=500';
    if (activeFilter) url += '&type=' + encodeURIComponent(activeFilter);
    if (search) url += '&search=' + encodeURIComponent(search);
    try {
      const r = await fetch(url, { headers: { 'Authorization': 'Bearer ' + TOKEN } });
      if (!r.ok) { doLogout(); return; }
      const data = await r.json();
      allMemories = data.memories || [];
      updateStats(data.stats || []);
      renderGrid(allMemories);
      if (silent) window.scrollTo(0, scrollY);
    } catch(e) {
      grid.innerHTML = '<div class="empty-state"><div class="empty-icon">⚠</div>CONNECTION ERROR</div>';
    }
  }

  function updateStats(stats) {
    const counts = { note: 0, fact: 0, journal: 0 };
    let total = 0;
    stats.forEach(s => { counts[s.type] = s.count; total += s.count; });
    document.getElementById('count-all').textContent = total;
    document.getElementById('count-note').textContent = counts.note;
    document.getElementById('count-fact').textContent = counts.fact;
    document.getElementById('count-journal').textContent = counts.journal;
    document.getElementById('hdr-count').textContent = total + ' entries';
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
      return \`<div class="card" data-type="\${m.type}" data-idx="\${i}" onclick="expandCard(\${i})" style="animation-delay:\${Math.min(i*0.04,0.4)}s">
        <div class="card-type-stripe"></div>
        <div class="card-header">
          <div>\${titleHtml}\${keyHtml}\${!m.title && !m.key ? '<div class="card-title" style="opacity:0.4">untitled</div>' : ''}</div>
          <span class="card-type-badge">\${m.type}</span>
        </div>
        <div class="card-content">\${esc(m.content)}</div>
        <div class="card-footer">
          <div class="card-tags">\${tags}\${linkBadge}</div>
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
    const typeColors = { note: 'var(--teal)', fact: 'var(--amber)', journal: '#8888ff' };
    document.getElementById('expand-header').innerHTML =
      \`<div style="display:flex;align-items:center;gap:0.75rem;margin-bottom:0.5rem;flex-wrap:wrap">
        <span style="font-size:0.6rem;letter-spacing:0.2em;text-transform:uppercase;border:1px solid \${typeColors[m.type]||'#fff'};color:\${typeColors[m.type]||'#fff'};padding:0.2rem 0.5rem">\${m.type}</span>
        \${m.title ? \`<span style="font-family:var(--sans);font-weight:700;font-size:1.1rem;color:var(--text-bright)">\${esc(m.title)}</span>\` : ''}
        \${m.key ? \`<span style="font-size:0.75rem;color:var(--amber)">KEY: \${esc(m.key)}</span>\` : ''}
      </div>
      \${m.tags ? \`<div style="display:flex;flex-wrap:wrap;gap:0.3rem;margin-bottom:0.25rem">\${m.tags.split(',').map(t => \`<span class="tag">\${esc(t.trim())}</span>\`).join('')}</div>\` : ''}\`;
    document.getElementById('expand-content').textContent = m.content;
    document.getElementById('expand-meta').textContent = 'ID: ' + m.id + '  ·  Created ' + date + updated;
    document.getElementById('expand-overlay').classList.add('open');
    document.body.style.overflow = 'hidden';

    // Lazy-load connections
    const connEl = document.getElementById('expand-connections');
    connEl.innerHTML = '<div style="font-size:0.65rem;color:var(--text-dim);letter-spacing:0.1em;margin-top:1rem">LOADING CONNECTIONS...</div>';
    const myGen = ++expandGen;
    fetch(BASE + '/api/links/' + m.id, { headers: { 'Authorization': 'Bearer ' + TOKEN } })
      .then(r => {
        if (r.status === 401) { doLogout(); return null; }
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
            const label = l.label ? \`<span class="chip-label">"\${esc(l.label)}"</span>\` : '';
            const name = cm.title || cm.key || (cm.content || '').slice(0, 40) + '…';
            return \`<span class="connection-chip" data-conn-id="\${esc(cm.id)}">
              <span class="chip-type">[\${esc(cm.type)}]</span>
              \${esc(name)}
              \${label}
              <span style="opacity:0.4">→</span>
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

  function setFilter(type) {
    graphVisible = false;
    document.getElementById('graph-view').style.display = 'none';
    document.querySelector('.grid-wrap').style.display = 'grid';
    activeFilter = type;
    ['all','note','fact','journal','graph'].forEach(t => {
      document.getElementById('stat-' + t).classList.toggle('active', (type === '' ? 'all' : type) === t);
    });
    loadMemories();
  }

  function onSearch(val) {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(loadMemories, 300);
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

  function startLivePolling() {
    if (pollIntervalId) return;
    const liveEl = document.getElementById('live-indicator');
    if (liveEl) liveEl.style.display = 'flex';
    pollIntervalId = setInterval(async () => {
      if (!TOKEN) return;
      try {
        const r = await fetch(BASE + '/api/memories?limit=1', { headers: { 'Authorization': 'Bearer ' + TOKEN } });
        if (!r.ok) return;
        const data = await r.json();
        const sig = (data.stats || []).map(s => s.type + ':' + s.count).join('|');
        if (lastPollSig && sig !== lastPollSig) {
          loadMemories(true); // silent refresh
        }
        lastPollSig = sig;
      } catch {}
    }, 10000);
  }

  async function showGraph() {
    graphVisible = true;
    ['all','note','fact','journal'].forEach(t => {
      document.getElementById('stat-' + t).classList.remove('active');
    });
    document.getElementById('stat-graph').classList.add('active');
    document.querySelector('.grid-wrap').style.display = 'none';
    document.getElementById('graph-view').style.display = 'block';
    const emptyEl = document.getElementById('graph-empty');
    if (emptyEl) emptyEl.style.display = 'none';

    const svg = document.getElementById('graph-svg');
    svg.innerHTML = '<text x="50%" y="50%" text-anchor="middle" style="fill:var(--amber);font-family:var(--mono);font-size:0.7rem;letter-spacing:0.15em">LOADING GRAPH...</text>';

    try {
      const r = await fetch(BASE + '/api/graph', { headers: { 'Authorization': 'Bearer ' + TOKEN } });
      if (r.status === 401) { doLogout(); return; }
      if (!r.ok) throw new Error('failed');
      const data = await r.json();
      lastGraphData = {
        nodes: (data.nodes || []).map(n => ({ ...n })),
        edges: (data.edges || []).map(e => ({ ...e })),
      };
      renderGraph(lastGraphData.nodes.map(n => ({ ...n })), lastGraphData.edges.map(e => ({ ...e })));
    } catch(e) {
      document.getElementById('graph-svg').innerHTML = '<text x="50%" y="50%" text-anchor="middle" style="fill:var(--red);font-family:var(--mono);font-size:0.7rem;letter-spacing:0.15em">ERROR LOADING GRAPH</text>';
    }
  }

  function renderGraph(nodes, edges) {
    const svgEl = document.getElementById('graph-svg');
    const emptyEl = document.getElementById('graph-empty');
    svgEl.innerHTML = '';
    if (emptyEl) emptyEl.style.display = 'none';

    if (!nodes.length) {
      if (emptyEl) { emptyEl.style.display = 'flex'; }
      return;
    }

    const width = svgEl.clientWidth || 800;
    const height = svgEl.clientHeight || 600;
    const isMobile = window.matchMedia('(max-width: 640px)').matches;
    const typeColor = { note: '#00c8b4', fact: '#f0a500', journal: '#8888ff' };

    const nodeMap = new Map(nodes.map(n => [n.id, n]));
    const links = edges.map(e => ({ ...e, source: e.from_id, target: e.to_id }))
      .filter(e => nodeMap.has(e.source) && nodeMap.has(e.target));

    const simulation = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links).id(d => d.id).distance(isMobile ? 96 : 120))
      .force('charge', d3.forceManyBody().strength(isMobile ? -220 : -300))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collision', d3.forceCollide(isMobile ? 24 : 30));

    const svg = d3.select('#graph-svg');
    const g = svg.append('g');

    svg.call(d3.zoom().scaleExtent([0.2, 4]).on('zoom', (event) => {
      g.attr('transform', event.transform);
    }));

    const link = g.append('g').selectAll('line')
      .data(links).join('line').attr('class', 'graph-link');

    const linkLabel = g.append('g').selectAll('text')
      .data(links).join('text').attr('class', 'graph-link-label')
      .text(d => d.label || '');

    const node = g.append('g').selectAll('g')
      .data(nodes).join('g').attr('class', 'graph-node')
      .call(d3.drag()
        .on('start', (event, d) => { if (!event.active) simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
        .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
        .on('end', (event, d) => { if (!event.active) simulation.alphaTarget(0); d.fx = null; d.fy = null; })
      )
      .on('click', (event, d) => { expandById(d.id); });

    node.append('circle')
      .attr('r', isMobile ? 10 : 8)
      .attr('fill', d => typeColor[d.type] || '#888')
      .attr('fill-opacity', 0.85)
      .attr('stroke', d => typeColor[d.type] || '#888');

    node.append('text')
      .attr('dx', 12).attr('dy', 4)
      .text(d => (d.title || d.key || d.content || '').slice(0, isMobile ? 18 : 24));

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
      renderGraph(lastGraphData.nodes.map(n => ({ ...n })), lastGraphData.edges.map(e => ({ ...e })));
    }, 120);
  });

  // Enter key on login
  document.getElementById('token-input').addEventListener('keydown', e => { if (e.key === 'Enter') doLogin(); });
</script>
</body>
</html>`;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS_HEADERS });
    }

    if (url.pathname === '/') {
      return jsonResponse({ name: 'ai-memory-mcp', version: '1.0.0', status: 'ok', tools: TOOLS.length });
    }

    if (url.pathname === '/view') {
      return new Response(viewerHtml(), {
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
      });
    }

    if (url.pathname === '/api/memories') {
      const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
      if (await isRateLimited(ip, env)) {
        return new Response(JSON.stringify({ error: 'Too many failed attempts. Try again later.' }), {
          status: 429,
          headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', 'Retry-After': '900' },
        });
      }
      if (!checkAuth(request, env)) {
        await recordFailedAttempt(ip, env);
        return unauthorized();
      }
      await clearRateLimit(ip, env);
      return handleApiMemories(request, env);
    }

    if (url.pathname === '/mcp') {
      const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
      if (await isRateLimited(ip, env)) {
        return new Response(JSON.stringify({ error: 'Too many failed attempts. Try again later.' }), {
          status: 429,
          headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', 'Retry-After': '900' },
        });
      }
      if (!checkAuth(request, env)) {
        await recordFailedAttempt(ip, env);
        return unauthorized();
      }
      await clearRateLimit(ip, env);
      return handleMcp(request, env, url);
    }

    // GET /api/links/:id
    if (url.pathname.startsWith('/api/links/')) {
      const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
      if (await isRateLimited(ip, env)) {
        return new Response(JSON.stringify({ error: 'Too many failed attempts. Try again later.' }), {
          status: 429, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', 'Retry-After': '900' },
        });
      }
      if (!checkAuth(request, env)) { await recordFailedAttempt(ip, env); return unauthorized(); }
      await clearRateLimit(ip, env);
      const memoryId = url.pathname.slice('/api/links/'.length);
      if (!memoryId) return jsonResponse({ error: 'Memory ID required' }, 400);
      return handleApiLinks(memoryId, env);
    }

    // GET /api/graph
    if (url.pathname === '/api/graph') {
      const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
      if (await isRateLimited(ip, env)) {
        return new Response(JSON.stringify({ error: 'Too many failed attempts. Try again later.' }), {
          status: 429, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', 'Retry-After': '900' },
        });
      }
      if (!checkAuth(request, env)) { await recordFailedAttempt(ip, env); return unauthorized(); }
      await clearRateLimit(ip, env);
      return handleApiGraph(env);
    }

    return jsonResponse({ error: 'Not found' }, 404);
  },
};
