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
  return parts[0] === 'Bearer' && parts[1] === env.AUTH_SECRET;
}

// Tool definitions for MCP tools/list response
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
];

type ToolArgs = Record<string, unknown>;

async function callTool(name: string, args: ToolArgs, env: Env): Promise<unknown> {
  switch (name) {
    case 'memory_save': {
      const { type, content, title, key, tags } = args as {
        type: string; content: string; title?: string; key?: string; tags?: string;
      };
      const id = generateId();
      const ts = now();
      await env.DB.prepare(
        'INSERT INTO memories (id, type, title, key, content, tags, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
      ).bind(id, type, title ?? null, key ?? null, content, tags ?? null, ts, ts).run();
      return { content: [{ type: 'text', text: `Saved memory with id: ${id}` }] };
    }

    case 'memory_get': {
      const { id } = args as { id: string };
      const row = await env.DB.prepare('SELECT * FROM memories WHERE id = ?').bind(id).first();
      if (!row) return { content: [{ type: 'text', text: 'Memory not found.' }] };
      return { content: [{ type: 'text', text: JSON.stringify(row, null, 2) }] };
    }

    case 'memory_get_fact': {
      const { key } = args as { key: string };
      const row = await env.DB.prepare(
        'SELECT * FROM memories WHERE type = ? AND key = ? LIMIT 1'
      ).bind('fact', key).first();
      if (!row) return { content: [{ type: 'text', text: `No fact found with key: ${key}` }] };
      return { content: [{ type: 'text', text: JSON.stringify(row, null, 2) }] };
    }

    case 'memory_search': {
      const { query, type } = args as { query: string; type?: string };
      const like = `%${query}%`;
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
      const { type, tag, limit = 20 } = args as { type?: string; tag?: string; limit?: number };
      let query = 'SELECT * FROM memories WHERE 1=1';
      const params: unknown[] = [];
      if (type) { query += ' AND type = ?'; params.push(type); }
      if (tag) { query += ' AND tags LIKE ?'; params.push(`%${tag}%`); }
      query += ' ORDER BY created_at DESC LIMIT ?';
      params.push(Math.min(Math.max(Number(limit), 1), 100));
      const results = await env.DB.prepare(query).bind(...params).all();
      if (!results.results.length) return { content: [{ type: 'text', text: 'No memories found.' }] };
      return { content: [{ type: 'text', text: JSON.stringify(results.results, null, 2) }] };
    }

    case 'memory_update': {
      const { id, content, title, tags } = args as {
        id: string; content?: string; title?: string; tags?: string;
      };
      const existing = await env.DB.prepare('SELECT * FROM memories WHERE id = ?').bind(id).first() as Record<string, unknown> | null;
      if (!existing) return { content: [{ type: 'text', text: 'Memory not found.' }] };
      await env.DB.prepare(
        'UPDATE memories SET content = ?, title = ?, tags = ?, updated_at = ? WHERE id = ?'
      ).bind(
        content ?? existing.content,
        title ?? existing.title,
        tags ?? existing.tags,
        now(),
        id
      ).run();
      return { content: [{ type: 'text', text: `Memory ${id} updated.` }] };
    }

    case 'memory_delete': {
      const { id } = args as { id: string };
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

    default:
      throw new Error(`Unknown tool: ${name}`);
  }
}

async function handleMcp(request: Request, env: Env): Promise<Response> {
  if (request.method !== 'POST') {
    return jsonResponse({ error: 'Method not allowed' }, 405);
  }

  let body: { jsonrpc: string; id?: unknown; method: string; params?: Record<string, unknown> };
  try {
    body = await request.json();
  } catch {
    return jsonResponse({ jsonrpc: '2.0', error: { code: -32700, message: 'Parse error' }, id: null });
  }

  const { id, method, params = {} } = body;

  try {
    if (method === 'initialize') {
      return jsonResponse({
        jsonrpc: '2.0', id,
        result: {
          protocolVersion: '2024-11-05',
          capabilities: { tools: {} },
          serverInfo: { name: 'ai-memory', version: '1.0.0' },
        },
      });
    }

    if (method === 'tools/list') {
      return jsonResponse({ jsonrpc: '2.0', id, result: { tools: TOOLS } });
    }

    if (method === 'tools/call') {
      const { name, arguments: toolArgs = {} } = params as { name: string; arguments?: ToolArgs };
      const result = await callTool(name, toolArgs, env);
      return jsonResponse({ jsonrpc: '2.0', id, result });
    }

    if (method === 'notifications/initialized') {
      return jsonResponse({ jsonrpc: '2.0', id, result: {} });
    }

    return jsonResponse({
      jsonrpc: '2.0', id,
      error: { code: -32601, message: `Method not found: ${method}` },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Internal error';
    return jsonResponse({
      jsonrpc: '2.0', id,
      error: { code: -32603, message },
    });
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        },
      });
    }

    if (url.pathname === '/') {
      return jsonResponse({ name: 'ai-memory-mcp', version: '1.0.0', status: 'ok', tools: TOOLS.length });
    }

    if (url.pathname === '/mcp') {
      if (!checkAuth(request, env)) return unauthorized();
      return handleMcp(request, env);
    }

    return jsonResponse({ error: 'Not found' }, 404);
  },
};
