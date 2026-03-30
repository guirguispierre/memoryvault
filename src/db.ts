import type {
  Env,
  MemoryType,
  LinkStats,
  BrainPolicy,
  MemoryGraphNode,
  MemoryGraphLink,
  BrainSummary,
} from './types.js';

import {
  LEGACY_BRAIN_ID,
  DEFAULT_BRAIN_POLICY,
} from './constants.js';

import {
  generateId,
  now,
  toFiniteNumber,
  clampToRange,
  normalizeSourceKey,
  normalizeRelation,
  stableJson,
  parseTagSet,
  parseJsonStringArray,
} from './utils.js';

/* ------------------------------------------------------------------ */
/*  Schema migration                                                  */
/* ------------------------------------------------------------------ */

export async function runMigrationStatement(env: Env, sql: string): Promise<void> {
  try {
    await env.DB.prepare(sql).run();
  } catch (err) {
    const msg = err instanceof Error ? err.message.toLowerCase() : '';
    if (msg.includes('duplicate column name') || msg.includes('already exists')) return;
    throw err;
  }
}

let schemaReady: Promise<void> | null = null;
export async function ensureSchema(env: Env): Promise<void> {
  if (!schemaReady) {
    schemaReady = (async () => {
      await runMigrationStatement(env, "ALTER TABLE memories ADD COLUMN source TEXT");
      await runMigrationStatement(env, "ALTER TABLE memories ADD COLUMN confidence REAL NOT NULL DEFAULT 0.7");
      await runMigrationStatement(env, "ALTER TABLE memories ADD COLUMN importance REAL NOT NULL DEFAULT 0.5");
      await runMigrationStatement(env, "ALTER TABLE memories ADD COLUMN archived_at INTEGER");
      await runMigrationStatement(env, "ALTER TABLE memories ADD COLUMN brain_id TEXT");
      await runMigrationStatement(env, "ALTER TABLE memory_links ADD COLUMN relation_type TEXT NOT NULL DEFAULT 'related'");
      await runMigrationStatement(env, "ALTER TABLE memory_links ADD COLUMN brain_id TEXT");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_archived ON memories(archived_at)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_importance ON memories(importance DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_confidence ON memories(confidence DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_links_relation_type ON memory_links(relation_type)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_memories_brain_created ON memories(brain_id, created_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_memories_brain_key ON memories(brain_id, key)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_links_brain_from ON memory_links(brain_id, from_id)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_links_brain_to ON memory_links(brain_id, to_id)");
      await runMigrationStatement(env, "UPDATE memories SET confidence = 0.7 WHERE confidence IS NULL");
      await runMigrationStatement(env, "UPDATE memories SET importance = 0.5 WHERE importance IS NULL");
      await runMigrationStatement(env, "UPDATE memory_links SET relation_type = 'related' WHERE relation_type IS NULL");
      await runMigrationStatement(env, `UPDATE memories SET brain_id = '${LEGACY_BRAIN_ID}' WHERE brain_id IS NULL OR brain_id = ''`);
      await runMigrationStatement(env, `UPDATE memory_links SET brain_id = '${LEGACY_BRAIN_ID}' WHERE brain_id IS NULL OR brain_id = ''`);
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS memory_changelog (
          id TEXT PRIMARY KEY,
          brain_id TEXT NOT NULL DEFAULT '${LEGACY_BRAIN_ID}',
          event_type TEXT NOT NULL,
          entity_type TEXT NOT NULL,
          entity_id TEXT NOT NULL,
          summary TEXT NOT NULL,
          payload TEXT,
          created_at INTEGER NOT NULL
        )`
      );
      await runMigrationStatement(env, `ALTER TABLE memory_changelog ADD COLUMN brain_id TEXT NOT NULL DEFAULT '${LEGACY_BRAIN_ID}'`);
      await runMigrationStatement(env, `UPDATE memory_changelog SET brain_id = '${LEGACY_BRAIN_ID}' WHERE brain_id IS NULL OR brain_id = ''`);
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_changelog_created ON memory_changelog(created_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_changelog_entity ON memory_changelog(entity_type, entity_id, created_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_changelog_brain_created ON memory_changelog(brain_id, created_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_changelog_brain_entity ON memory_changelog(brain_id, entity_type, entity_id, created_at DESC)");

      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS users (
          id TEXT PRIMARY KEY,
          email TEXT NOT NULL UNIQUE,
          password_hash TEXT NOT NULL,
          display_name TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        )`
      );
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS brains (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          slug TEXT UNIQUE,
          owner_user_id TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE SET NULL
        )`
      );
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS brain_memberships (
          brain_id TEXT NOT NULL,
          user_id TEXT NOT NULL,
          role TEXT NOT NULL DEFAULT 'member',
          created_at INTEGER NOT NULL,
          PRIMARY KEY (brain_id, user_id),
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE,
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_brain_memberships_user ON brain_memberships(user_id, brain_id)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_brains_owner ON brains(owner_user_id)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS auth_sessions (
          id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          brain_id TEXT NOT NULL,
          client_id TEXT,
          refresh_hash TEXT NOT NULL UNIQUE,
          expires_at INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          used_at INTEGER NOT NULL,
          revoked_at INTEGER,
          replaced_by TEXT,
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "ALTER TABLE auth_sessions ADD COLUMN client_id TEXT");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(user_id, expires_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_auth_sessions_brain ON auth_sessions(brain_id, expires_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_auth_sessions_client ON auth_sessions(client_id, expires_at DESC)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires ON auth_sessions(expires_at)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS oauth_clients (
          id TEXT PRIMARY KEY,
          client_id TEXT NOT NULL UNIQUE,
          client_name TEXT,
          redirect_uris TEXT NOT NULL,
          grant_types TEXT NOT NULL,
          response_types TEXT NOT NULL,
          token_endpoint_auth_method TEXT NOT NULL DEFAULT 'none',
          client_secret_hash TEXT,
          client_secret_expires_at INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_oauth_clients_client_id ON oauth_clients(client_id)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS oauth_authorization_codes (
          id TEXT PRIMARY KEY,
          code TEXT NOT NULL UNIQUE,
          client_id TEXT NOT NULL,
          redirect_uri TEXT NOT NULL,
          user_id TEXT NOT NULL,
          brain_id TEXT NOT NULL,
          code_challenge TEXT NOT NULL,
          code_challenge_method TEXT NOT NULL DEFAULT 'S256',
          scope TEXT,
          resource TEXT,
          created_at INTEGER NOT NULL,
          expires_at INTEGER NOT NULL,
          used_at INTEGER
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_oauth_codes_code ON oauth_authorization_codes(code)");
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_oauth_codes_expires ON oauth_authorization_codes(expires_at)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS brain_source_trust (
          id TEXT PRIMARY KEY,
          brain_id TEXT NOT NULL,
          source_key TEXT NOT NULL,
          trust REAL NOT NULL DEFAULT 0.5,
          notes TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          UNIQUE(brain_id, source_key),
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_source_trust_brain ON brain_source_trust(brain_id, source_key)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS brain_policies (
          brain_id TEXT PRIMARY KEY,
          policy_json TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS brain_snapshots (
          id TEXT PRIMARY KEY,
          brain_id TEXT NOT NULL,
          label TEXT,
          summary TEXT,
          memory_count INTEGER NOT NULL DEFAULT 0,
          link_count INTEGER NOT NULL DEFAULT 0,
          payload_json TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_brain_snapshots_brain_created ON brain_snapshots(brain_id, created_at DESC)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS memory_conflict_resolutions (
          id TEXT PRIMARY KEY,
          brain_id TEXT NOT NULL,
          pair_key TEXT NOT NULL,
          a_id TEXT NOT NULL,
          b_id TEXT NOT NULL,
          status TEXT NOT NULL,
          canonical_id TEXT,
          note TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          UNIQUE(brain_id, pair_key),
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_conflict_resolutions_brain_status ON memory_conflict_resolutions(brain_id, status, updated_at DESC)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS memory_entity_aliases (
          id TEXT PRIMARY KEY,
          brain_id TEXT NOT NULL,
          canonical_memory_id TEXT NOT NULL,
          alias_memory_id TEXT NOT NULL,
          note TEXT,
          confidence REAL NOT NULL DEFAULT 0.9,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          UNIQUE(brain_id, alias_memory_id),
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_entity_aliases_brain_canonical ON memory_entity_aliases(brain_id, canonical_memory_id)");
      await runMigrationStatement(env,
        `CREATE TABLE IF NOT EXISTS memory_watches (
          id TEXT PRIMARY KEY,
          brain_id TEXT NOT NULL,
          name TEXT NOT NULL,
          event_types TEXT NOT NULL,
          query TEXT,
          webhook_url TEXT,
          secret TEXT,
          is_active INTEGER NOT NULL DEFAULT 1,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          last_triggered_at INTEGER,
          last_error TEXT,
          FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
        )`
      );
      await runMigrationStatement(env, "CREATE INDEX IF NOT EXISTS idx_memory_watches_brain_active ON memory_watches(brain_id, is_active, updated_at DESC)");

      const ts = now();
      await env.DB.prepare(
        'INSERT OR IGNORE INTO brains (id, name, slug, owner_user_id, created_at, updated_at) VALUES (?, ?, ?, NULL, ?, ?)'
      ).bind(LEGACY_BRAIN_ID, 'Legacy Shared Brain', 'legacy-shared-brain', ts, ts).run();
    })().catch((err) => {
      schemaReady = null;
      throw err;
    });
  }
  await schemaReady;
}

/* ------------------------------------------------------------------ */
/*  JSON / Policy helpers                                             */
/* ------------------------------------------------------------------ */

export function parseJsonObject(raw: string | null | undefined): Record<string, unknown> | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
    return parsed as Record<string, unknown>;
  } catch {
    return null;
  }
}

export function sanitizePolicyPatch(patch: Record<string, unknown>, base: BrainPolicy): BrainPolicy {
  return {
    decay_days: Math.min(Math.max(Math.floor(toFiniteNumber(patch.decay_days, base.decay_days)), 1), 3650),
    max_inferred_edges: Math.min(Math.max(Math.floor(toFiniteNumber(patch.max_inferred_edges, base.max_inferred_edges)), 20), 5000),
    min_link_suggestion_score: clampToRange(patch.min_link_suggestion_score, base.min_link_suggestion_score, 0, 1),
    retention_days: Math.min(Math.max(Math.floor(toFiniteNumber(patch.retention_days, base.retention_days)), 7), 36500),
    private_mode: typeof patch.private_mode === 'boolean' ? patch.private_mode : base.private_mode,
    snapshot_retention: Math.min(Math.max(Math.floor(toFiniteNumber(patch.snapshot_retention, base.snapshot_retention)), 1), 500),
    path_max_hops: Math.min(Math.max(Math.floor(toFiniteNumber(patch.path_max_hops, base.path_max_hops)), 1), 8),
    subgraph_default_radius: Math.min(Math.max(Math.floor(toFiniteNumber(patch.subgraph_default_radius, base.subgraph_default_radius)), 1), 6),
  };
}

export function normalizeLinkStats(raw?: Partial<LinkStats>): LinkStats {
  return {
    link_count: toFiniteNumber(raw?.link_count, 0),
    supports_count: toFiniteNumber(raw?.supports_count, 0),
    contradicts_count: toFiniteNumber(raw?.contradicts_count, 0),
    supersedes_count: toFiniteNumber(raw?.supersedes_count, 0),
    causes_count: toFiniteNumber(raw?.causes_count, 0),
    example_of_count: toFiniteNumber(raw?.example_of_count, 0),
  };
}

/* ------------------------------------------------------------------ */
/*  Memory queries                                                    */
/* ------------------------------------------------------------------ */

export async function loadMemoryRowsByIds(
  env: Env,
  brainId: string,
  ids: string[],
  typeFilter?: MemoryType
): Promise<Record<string, unknown>[]> {
  const requestedIds = ids.map((id) => id.trim()).filter(Boolean);
  if (!requestedIds.length) return [];
  const uniqueIds = Array.from(new Set(requestedIds));
  const placeholders = uniqueIds.map(() => '?').join(', ');
  let sql = `SELECT * FROM memories WHERE brain_id = ? AND archived_at IS NULL`;
  const params: unknown[] = [brainId];
  if (typeFilter) {
    sql += ' AND type = ?';
    params.push(typeFilter);
  }
  sql += ` AND id IN (${placeholders})`;
  params.push(...uniqueIds);
  const rows = await env.DB.prepare(sql).bind(...params).all<Record<string, unknown>>();
  const byId = new Map<string, Record<string, unknown>>();
  for (const row of rows.results) {
    const rowId = typeof row.id === 'string' ? row.id : '';
    if (rowId) byId.set(rowId, row);
  }
  return requestedIds.map((id) => byId.get(id)).filter((row): row is Record<string, unknown> => Boolean(row));
}

export async function runLexicalMemorySearch(
  env: Env,
  brainId: string,
  query: string,
  typeFilter: MemoryType | undefined,
  limit: number
): Promise<Record<string, unknown>[]> {
  const trimmedQuery = query.trim();
  const fields = ['id', 'content', 'title', 'key', 'source', 'tags'];
  const phraseLike = `%${trimmedQuery}%`;
  const searchParams: unknown[] = [];
  let where = `(${fields.map((field) => `${field} LIKE ?`).join(' OR ')})`;
  searchParams.push(...fields.map(() => phraseLike));

  const tokens = Array.from(new Set(
    trimmedQuery
      .toLowerCase()
      .split(/[^a-z0-9]+/g)
      .map((token) => token.trim())
      .filter((token) => token.length >= 3)
  ));
  const meaningfulTokens = tokens.filter((token) => token !== trimmedQuery.toLowerCase());
  if (meaningfulTokens.length) {
    const tokenClauses: string[] = [];
    for (const token of meaningfulTokens) {
      tokenClauses.push(`(${fields.map((field) => `${field} LIKE ?`).join(' OR ')})`);
      const tokenLike = `%${token}%`;
      searchParams.push(...fields.map(() => tokenLike));
    }
    where = `(${where} OR ${tokenClauses.join(' OR ')})`;
  }

  if (typeFilter) {
    const results = await env.DB.prepare(
      `SELECT * FROM memories
       WHERE brain_id = ?
         AND archived_at IS NULL
         AND type = ?
         AND ${where}
       ORDER BY created_at DESC
       LIMIT ?`
    ).bind(brainId, typeFilter, ...searchParams, limit).all<Record<string, unknown>>();
    return results.results;
  }
  const results = await env.DB.prepare(
    `SELECT * FROM memories
     WHERE brain_id = ?
       AND archived_at IS NULL
       AND ${where}
     ORDER BY created_at DESC
     LIMIT ?`
  ).bind(brainId, ...searchParams, limit).all<Record<string, unknown>>();
  return results.results;
}

/* ------------------------------------------------------------------ */
/*  Link stats                                                        */
/* ------------------------------------------------------------------ */

export async function loadLinkStatsMap(env: Env, brainId: string): Promise<Map<string, LinkStats>> {
  const rows = await env.DB.prepare(
    `SELECT
      rel.memory_id,
      COUNT(*) AS link_count,
      SUM(CASE WHEN rel.relation_type = 'supports' THEN 1 ELSE 0 END) AS supports_count,
      SUM(CASE WHEN rel.relation_type = 'contradicts' THEN 1 ELSE 0 END) AS contradicts_count,
      SUM(CASE WHEN rel.relation_type = 'supersedes' THEN 1 ELSE 0 END) AS supersedes_count,
      SUM(CASE WHEN rel.relation_type = 'causes' THEN 1 ELSE 0 END) AS causes_count,
      SUM(CASE WHEN rel.relation_type = 'example_of' THEN 1 ELSE 0 END) AS example_of_count
    FROM (
      SELECT from_id AS memory_id, relation_type FROM memory_links WHERE brain_id = ?
      UNION ALL
      SELECT to_id AS memory_id, relation_type FROM memory_links WHERE brain_id = ?
    ) AS rel
    GROUP BY rel.memory_id`
  ).bind(brainId, brainId).all<Record<string, unknown>>();

  const statsMap = new Map<string, LinkStats>();
  for (const row of rows.results) {
    const memoryId = typeof row.memory_id === 'string' ? row.memory_id : '';
    if (!memoryId) continue;
    statsMap.set(memoryId, {
      link_count: toFiniteNumber(row.link_count, 0),
      supports_count: toFiniteNumber(row.supports_count, 0),
      contradicts_count: toFiniteNumber(row.contradicts_count, 0),
      supersedes_count: toFiniteNumber(row.supersedes_count, 0),
      causes_count: toFiniteNumber(row.causes_count, 0),
      example_of_count: toFiniteNumber(row.example_of_count, 0),
    });
  }
  return statsMap;
}

export async function loadSourceTrustMap(env: Env, brainId: string): Promise<Map<string, number>> {
  const rows = await env.DB.prepare(
    'SELECT source_key, trust FROM brain_source_trust WHERE brain_id = ?'
  ).bind(brainId).all<{ source_key: string; trust: number }>();
  const out = new Map<string, number>();
  for (const row of rows.results) {
    const sourceKey = typeof row.source_key === 'string' ? normalizeSourceKey(row.source_key) : '';
    if (!sourceKey) continue;
    out.set(sourceKey, clampToRange(row.trust, 0.5));
  }
  return out;
}

/* ------------------------------------------------------------------ */
/*  Brain policy                                                      */
/* ------------------------------------------------------------------ */

export async function getBrainPolicy(env: Env, brainId: string): Promise<BrainPolicy> {
  const row = await env.DB.prepare(
    'SELECT policy_json FROM brain_policies WHERE brain_id = ? LIMIT 1'
  ).bind(brainId).first<{ policy_json: string }>();
  const parsed = parseJsonObject(row?.policy_json ?? null);
  if (!parsed) return { ...DEFAULT_BRAIN_POLICY };
  return sanitizePolicyPatch(parsed, DEFAULT_BRAIN_POLICY);
}

export async function setBrainPolicy(env: Env, brainId: string, patch: Record<string, unknown>): Promise<BrainPolicy> {
  const existing = await getBrainPolicy(env, brainId);
  const merged = sanitizePolicyPatch({ ...existing, ...patch }, existing);
  const ts = now();
  await env.DB.prepare(
    `INSERT INTO brain_policies (brain_id, policy_json, created_at, updated_at)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(brain_id) DO UPDATE SET policy_json = excluded.policy_json, updated_at = excluded.updated_at`
  ).bind(brainId, stableJson(merged), ts, ts).run();
  return merged;
}

/* ------------------------------------------------------------------ */
/*  Graph node/link loading                                           */
/* ------------------------------------------------------------------ */

export async function loadActiveMemoryNodes(env: Env, brainId: string, limit = 1500): Promise<MemoryGraphNode[]> {
  const rows = await env.DB.prepare(
    `SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance
     FROM memories
     WHERE brain_id = ? AND archived_at IS NULL
     ORDER BY created_at DESC
     LIMIT ?`
  ).bind(brainId, limit).all<MemoryGraphNode>();
  return rows.results;
}

export async function loadExplicitMemoryLinks(env: Env, brainId: string, limit = 8000): Promise<MemoryGraphLink[]> {
  const rows = await env.DB.prepare(
    `SELECT id, from_id, to_id, relation_type, label
     FROM memory_links
     WHERE brain_id = ?
     LIMIT ?`
  ).bind(brainId, limit).all<{
    id: string;
    from_id: string;
    to_id: string;
    relation_type: string;
    label: string | null;
  }>();
  return rows.results
    .filter((row) => !!row.from_id && !!row.to_id)
    .map((row) => ({
      id: row.id,
      from_id: row.from_id,
      to_id: row.to_id,
      relation_type: normalizeRelation(row.relation_type),
      label: row.label,
    }));
}

/* ------------------------------------------------------------------ */
/*  Objective root                                                    */
/* ------------------------------------------------------------------ */

export async function ensureObjectiveRoot(
  env: Env,
  brainId: string,
  vectorSync?: (env: Env, brainId: string, memories: Array<Record<string, unknown>>, operation: string) => Promise<void>
): Promise<string> {
  const key = 'autonomous_objectives_root';
  const existing = await env.DB.prepare(
    'SELECT id FROM memories WHERE brain_id = ? AND key = ? AND archived_at IS NULL ORDER BY created_at ASC LIMIT 1'
  ).bind(brainId, key).first<{ id: string }>();
  if (existing?.id) return existing.id;

  const ts = now();
  const id = generateId();
  await env.DB.prepare(
    'INSERT INTO memories (id, brain_id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)'
  ).bind(
    id,
    brainId,
    'note',
    'Autonomous Objectives Network',
    key,
    'Root node for long-term goals and curiosities.',
    'objective_root,autonomous_objectives,system_node',
    'system',
    0.9,
    0.95,
    ts,
    ts
  ).run();
  if (vectorSync) {
    await vectorSync(env, brainId, [{
      id,
      type: 'note',
      title: 'Autonomous Objectives Network',
      key,
      content: 'Root node for long-term goals and curiosities.',
      tags: 'objective_root,autonomous_objectives,system_node',
      source: 'system',
      confidence: 0.9,
      importance: 0.95,
      archived_at: null,
      created_at: ts,
      updated_at: ts,
    }], 'objective_root_created');
  }
  await logChangelog(env, brainId, 'objective_root_created', 'memory', id, 'Created autonomous objectives root');
  return id;
}

/* ------------------------------------------------------------------ */
/*  Changelog + watch triggers                                        */
/* ------------------------------------------------------------------ */

export function parseWatchEventTypes(raw: string): string[] {
  const parsed = parseJsonStringArray(raw, []);
  const out: string[] = [];
  for (const item of parsed) {
    const value = item.trim();
    if (!value) continue;
    if (value === '*' || /^[a-z0-9_.:-]{2,64}$/i.test(value)) out.push(value);
  }
  return Array.from(new Set(out));
}

export function normalizeWatchEventInput(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  const out: string[] = [];
  for (const item of raw) {
    if (typeof item !== 'string') continue;
    const value = item.trim();
    if (!value) continue;
    if (value === '*' || /^[a-z0-9_.:-]{2,64}$/i.test(value)) out.push(value);
  }
  return Array.from(new Set(out));
}

async function triggerMemoryWatches(
  env: Env,
  params: {
    brain_id: string;
    event_type: string;
    entity_type: string;
    entity_id: string;
    summary: string;
    payload: unknown;
    created_at: number;
  }
): Promise<void> {
  const rows = await env.DB.prepare(
    `SELECT id, event_types, query, webhook_url, secret
     FROM memory_watches
     WHERE brain_id = ? AND is_active = 1
     ORDER BY updated_at DESC
     LIMIT 200`
  ).bind(params.brain_id).all<{
    id: string;
    event_types: string;
    query: string | null;
    webhook_url: string | null;
    secret: string | null;
  }>();

  const haystack = `${params.event_type} ${params.entity_type} ${params.entity_id} ${params.summary} ${stableJson(params.payload)}`
    .toLowerCase();
  for (const row of rows.results) {
    const eventTypes = parseWatchEventTypes(row.event_types);
    if (eventTypes.length && !eventTypes.includes('*') && !eventTypes.includes(params.event_type)) continue;
    const query = typeof row.query === 'string' ? row.query.trim().toLowerCase() : '';
    if (query && !haystack.includes(query)) continue;

    const ts = params.created_at;
    await env.DB.prepare(
      'UPDATE memory_watches SET last_triggered_at = ?, updated_at = ?, last_error = NULL WHERE id = ? AND brain_id = ?'
    ).bind(ts, ts, row.id, params.brain_id).run();

    const webhook = typeof row.webhook_url === 'string' ? row.webhook_url.trim() : '';
    if (!webhook || !(webhook.startsWith('https://') || webhook.startsWith('http://'))) continue;
    try {
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        'X-MemoryVault-Watch-Id': row.id,
      };
      if (row.secret) headers['X-MemoryVault-Watch-Secret'] = row.secret;
      const response = await fetch(webhook, {
        method: 'POST',
        headers,
        body: JSON.stringify({
          watch_id: row.id,
          event_type: params.event_type,
          entity_type: params.entity_type,
          entity_id: params.entity_id,
          summary: params.summary,
          payload: params.payload,
          created_at: params.created_at,
        }),
      });
      if (!response.ok) {
        await env.DB.prepare(
          'UPDATE memory_watches SET last_error = ?, updated_at = ? WHERE id = ? AND brain_id = ?'
        ).bind(`webhook_status_${response.status}`, ts, row.id, params.brain_id).run();
      }
    } catch (err) {
      const message = err instanceof Error ? err.message.slice(0, 300) : 'webhook_error';
      await env.DB.prepare(
        'UPDATE memory_watches SET last_error = ?, updated_at = ? WHERE id = ? AND brain_id = ?'
      ).bind(message, ts, row.id, params.brain_id).run();
    }
  }
}

export async function logChangelog(
  env: Env,
  brainId: string,
  eventType: string,
  entityType: string,
  entityId: string,
  summary: string,
  payload?: unknown
): Promise<void> {
  const ts = now();
  const payloadJson = payload === undefined ? null : stableJson(payload);
  await env.DB.prepare(
    'INSERT INTO memory_changelog (id, brain_id, event_type, entity_type, entity_id, summary, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
  ).bind(
    generateId(),
    brainId,
    eventType,
    entityType,
    entityId,
    summary,
    payloadJson,
    ts
  ).run();
  try {
    await triggerMemoryWatches(env, {
      brain_id: brainId,
      event_type: eventType,
      entity_type: entityType,
      entity_id: entityId,
      summary,
      payload: payload ?? null,
      created_at: ts,
    });
  } catch {
    // Watch dispatch is best-effort and should never break memory writes.
  }
}

/* ------------------------------------------------------------------ */
/*  Brain listing                                                     */
/* ------------------------------------------------------------------ */

export async function listBrainsForUser(userId: string, env: Env): Promise<BrainSummary[]> {
  const rows = await env.DB.prepare(
    `SELECT
      b.id,
      b.name,
      b.slug,
      b.created_at,
      b.updated_at,
      bm.role
     FROM brain_memberships bm
     JOIN brains b ON b.id = bm.brain_id
     WHERE bm.user_id = ?
     ORDER BY CASE WHEN bm.role = 'owner' THEN 0 ELSE 1 END ASC, bm.created_at ASC`
  ).bind(userId).all<BrainSummary>();
  return rows.results;
}

export function findActiveBrain(brains: BrainSummary[], preferredBrainId: string): BrainSummary | null {
  if (!brains.length) return null;
  const explicit = brains.find((b) => b.id === preferredBrainId);
  if (explicit) return explicit;
  return brains[0];
}
