import type {
  Env,
  MemorySearchMode,
  MemoryType,
  SemanticMemoryCandidate,
  RelationType,
  MemoryGraphNode,
  MemoryGraphLink,
  ToolArgs,
} from './types.js';

import {
  SERVER_NAME,
  SERVER_VERSION,
  EMPTY_LINK_STATS,
  MEMORY_SEARCH_DEFAULT_LIMIT,
  MEMORY_SEARCH_MAX_LIMIT,
  VECTORIZE_QUERY_TOP_K_MAX,
  VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS,
  VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS_MAX,
} from './constants.js';

import {
  generateId,
  now,
  clampToRange,
  isMemorySearchMode,
  hasSemanticSearchBindings,
  isValidType,
  isValidRelationType,
  normalizeRelation,
  normalizeSourceKey,
  normalizeTag,
  parseTagSet,
  stableJson,
  toFiniteNumber,
  slugify,
} from './utils.js';

import {
  parseJsonObject,
  loadMemoryRowsByIds,
  runLexicalMemorySearch,
  loadLinkStatsMap,
  loadSourceTrustMap,
  getBrainPolicy,
  setBrainPolicy,
  loadActiveMemoryNodes,
  loadExplicitMemoryLinks,
  ensureObjectiveRoot,
  logChangelog,
  normalizeWatchEventInput,
  parseWatchEventTypes,
} from './db.js';

import {
  safeSyncMemoriesToVectorIndex,
  syncMemoriesToVectorIndex,
  safeDeleteMemoryVectors,
  querySemanticMemoryCandidates,
  fuseSearchRows,
  waitForVectorMutationReady,
  waitForVectorQueryReady,
} from './vectorize.js';

import {
  clamp01,
  round3,
  computeDynamicScoreBreakdown,
  computeDynamicScores,
  enrichAndProjectRows,
  projectMemoryForClient,
} from './scoring.js';

import {
  TOOLS,
  TOOL_RELEASE_META,
  TOOL_CHANGELOG,
  getToolReleaseMeta,
  isToolDeprecated,
  compareSemver,
  parseSemver,
} from './tools-schema.js';

import {
  sha256DigestBase64Url,
} from './crypto.js';

function tokenizeText(raw: string, max = 80): string[] {
  const stopWords = new Set([
    'the', 'a', 'an', 'and', 'or', 'to', 'of', 'for', 'in', 'on', 'at', 'is', 'are', 'was', 'were', 'be',
    'with', 'as', 'by', 'it', 'this', 'that', 'from', 'but', 'not', 'if', 'then', 'so', 'we', 'you', 'i',
  ]);
  const cleaned = raw
    .toLowerCase()
    .replace(/[^a-z0-9\s]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!cleaned) return [];
  const out: string[] = [];
  for (const token of cleaned.split(' ')) {
    if (token.length < 2 || stopWords.has(token)) continue;
    out.push(token);
    if (out.length >= max) break;
  }
  return out;
}

function jaccardSimilarity(a: Set<string>, b: Set<string>): number {
  if (!a.size || !b.size) return 0;
  let intersection = 0;
  for (const item of a) {
    if (b.has(item)) intersection++;
  }
  const union = a.size + b.size - intersection;
  if (!union) return 0;
  return intersection / union;
}

function pairKey(a: string, b: string): string {
  return a < b ? `${a}|${b}` : `${b}|${a}`;
}


function canonicalizeForJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => canonicalizeForJson(item));
  }
  if (!value || typeof value !== 'object') {
    return value;
  }
  const obj = value as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(obj).sort()) {
    out[key] = canonicalizeForJson(obj[key]);
  }
  return out;
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(canonicalizeForJson(value));
}


type GraphEdge = { from: string; to: string; relation_type: RelationType };
type GraphNeighbor = { id: string; relation_type: RelationType };

function relationSignalWeight(relationType: RelationType): number {
  switch (relationType) {
    case 'supports': return 0.88;
    case 'causes': return 0.82;
    case 'example_of': return 0.7;
    case 'supersedes': return 0.65;
    case 'contradicts': return -0.75;
    case 'related':
    default:
      return 0.62;
  }
}

function relationSpreadWeight(relationType: RelationType): number {
  switch (relationType) {
    case 'supports': return 1;
    case 'causes': return 0.9;
    case 'example_of': return 0.75;
    case 'supersedes': return 0.72;
    case 'contradicts': return -0.65;
    case 'related':
    default:
      return 0.68;
  }
}

function buildAdjacencyFromEdges(edges: GraphEdge[]): Map<string, GraphNeighbor[]> {
  const adjacency = new Map<string, GraphNeighbor[]>();
  for (const edge of edges) {
    const rel = normalizeRelation(edge.relation_type);
    const fromArr = adjacency.get(edge.from);
    if (fromArr) fromArr.push({ id: edge.to, relation_type: rel });
    else adjacency.set(edge.from, [{ id: edge.to, relation_type: rel }]);
    const toArr = adjacency.get(edge.to);
    if (toArr) toArr.push({ id: edge.from, relation_type: rel });
    else adjacency.set(edge.to, [{ id: edge.from, relation_type: rel }]);
  }
  return adjacency;
}








export function buildTagInferredLinks(nodes: MemoryGraphNode[], maxEdges = 400): MemoryGraphLink[] {
  const tagToIds = new Map<string, string[]>();
  for (const node of nodes) {
    const tags = parseTagSet(node.tags);
    for (const tag of tags) {
      const ids = tagToIds.get(tag);
      if (ids) ids.push(node.id);
      else tagToIds.set(tag, [node.id]);
    }
  }

  const byPair = new Map<string, { from: string; to: string; score: number; shared: Set<string> }>();
  for (const [tag, idsRaw] of tagToIds) {
    const ids = Array.from(new Set(idsRaw));
    if (ids.length < 2) continue;
    const trimmed = ids.slice(0, 30);
    const weight = 1 / Math.sqrt(trimmed.length);
    for (let i = 0; i < trimmed.length; i++) {
      for (let j = i + 1; j < trimmed.length; j++) {
        const from = trimmed[i] < trimmed[j] ? trimmed[i] : trimmed[j];
        const to = trimmed[i] < trimmed[j] ? trimmed[j] : trimmed[i];
        const key = `${from}|${to}`;
        const existing = byPair.get(key);
        if (existing) {
          existing.score += weight;
          existing.shared.add(tag);
        } else {
          byPair.set(key, { from, to, score: weight, shared: new Set([tag]) });
        }
      }
    }
  }

  return Array.from(byPair.values())
    .map((row) => ({
      id: `inferred-${row.from}-${row.to}`,
      from_id: row.from,
      to_id: row.to,
      relation_type: 'related' as RelationType,
      label: `shared: ${Array.from(row.shared).slice(0, 3).join(', ')}`,
      inferred: true,
      score: round3(row.score),
    }))
    .filter((row) => (row.score ?? 0) >= 0.75)
    .sort((a, b) => toFiniteNumber(b.score, 0) - toFiniteNumber(a.score, 0))
    .slice(0, maxEdges);
}

export type McpResult = { content: Array<{ type: string; text: string }> };

export async function callTool(name: string, args: ToolArgs, env: Env, brainId: string): Promise<McpResult> {
  switch (name) {
    case 'memory_save': {
      const { type, content, title, key, tags, source, confidence, importance } = args as {
        type: unknown;
        content: unknown;
        title?: unknown;
        key?: unknown;
        tags?: unknown;
        source?: unknown;
        confidence?: unknown;
        importance?: unknown;
      };
      if (!isValidType(type)) return { content: [{ type: 'text', text: 'Invalid type. Must be note, fact, or journal.' }] };
      if (typeof content !== 'string' || content.trim() === '') return { content: [{ type: 'text', text: 'content must be a non-empty string.' }] };
      if (source !== undefined && typeof source !== 'string') return { content: [{ type: 'text', text: 'source must be a string when provided.' }] };
      const id = generateId();
      const ts = now();
      const confidenceVal = clampToRange(confidence, 0.7);
      const importanceVal = clampToRange(importance, 0.5);
      await env.DB.prepare(
        'INSERT INTO memories (id, brain_id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)'
      ).bind(
        id,
        brainId,
        type,
        typeof title === 'string' ? title : null,
        typeof key === 'string' ? key : null,
        content.trim(),
        typeof tags === 'string' ? tags : null,
        typeof source === 'string' ? source : null,
        confidenceVal,
        importanceVal,
        ts,
        ts
      ).run();
      // Find up to 5 existing memories sharing at least one tag (for suggested linking)
      let suggestedLinks: unknown[] = [];
      if (typeof tags === 'string' && tags.trim()) {
        const tagList = tags.split(',').map((t: string) => t.trim()).filter(Boolean);
        if (tagList.length > 0) {
          const conditions = tagList.map(() => 'tags LIKE ?').join(' OR ');
          const bindings = tagList.map((t: string) => `%${t}%`);
          const suggestions = await env.DB.prepare(
            `SELECT id, type, title, key, tags FROM memories WHERE brain_id = ? AND archived_at IS NULL AND id != ? AND (${conditions}) LIMIT 5`
          ).bind(brainId, id, ...bindings).all();
          suggestedLinks = suggestions.results;
        }
      }

      const insertedRow: Record<string, unknown> = {
        id,
        type,
        title: typeof title === 'string' ? title : null,
        key: typeof key === 'string' ? key : null,
        content: content.trim(),
        tags: typeof tags === 'string' ? tags : null,
        source: typeof source === 'string' ? source : null,
        confidence: confidenceVal,
        importance: importanceVal,
        archived_at: null,
        created_at: ts,
        updated_at: ts,
      };
      await safeSyncMemoriesToVectorIndex(env, brainId, [insertedRow], 'memory_save');
      let sourceTrust: number | undefined;
      if (typeof source === 'string' && source.trim()) {
        const sourceKey = normalizeSourceKey(source);
        const trustRow = await env.DB.prepare(
          'SELECT trust FROM brain_source_trust WHERE brain_id = ? AND source_key = ? LIMIT 1'
        ).bind(brainId, sourceKey).first<{ trust: number }>();
        if (trustRow && Number.isFinite(Number(trustRow.trust))) {
          sourceTrust = clampToRange(trustRow.trust, 0.5);
        }
      }
      const scoredMemory = projectMemoryForClient({
        ...insertedRow,
        ...computeDynamicScores(insertedRow, EMPTY_LINK_STATS, ts, sourceTrust),
      });

      const saveResult: Record<string, unknown> = {
        id,
        message: `Saved memory with id: ${id}`,
        confidence: scoredMemory.confidence,
        importance: scoredMemory.importance,
        dynamic_confidence: scoredMemory.dynamic_confidence,
        dynamic_importance: scoredMemory.dynamic_importance,
        base_confidence: scoredMemory.base_confidence,
        base_importance: scoredMemory.base_importance,
      };
      if (suggestedLinks.length > 0) saveResult.suggested_links = suggestedLinks;
      await logChangelog(env, brainId, 'memory_created', 'memory', id, 'Created memory', {
        type,
        title: typeof title === 'string' ? title : null,
        key: typeof key === 'string' ? key : null,
      });
      return { content: [{ type: 'text', text: JSON.stringify(saveResult) }] };
    }

    case 'memory_get': {
      const { id } = args as { id: unknown };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };
      const row = await env.DB.prepare('SELECT * FROM memories WHERE brain_id = ? AND id = ?').bind(brainId, id).first<Record<string, unknown>>();
      if (!row) return { content: [{ type: 'text', text: 'Memory not found.' }] };
      const [scored] = await enrichAndProjectRows(env, brainId, [row]);
      return { content: [{ type: 'text', text: JSON.stringify(scored ?? row, null, 2) }] };
    }

    case 'memory_get_fact': {
      const { key } = args as { key: unknown };
      if (typeof key !== 'string' || !key) return { content: [{ type: 'text', text: 'key must be a non-empty string.' }] };
      const row = await env.DB.prepare(
        'SELECT * FROM memories WHERE brain_id = ? AND type = ? AND key = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT 1'
      ).bind(brainId, 'fact', key).first<Record<string, unknown>>();
      if (!row) return { content: [{ type: 'text', text: `No fact found with key: ${key}` }] };
      const [scored] = await enrichAndProjectRows(env, brainId, [row]);
      return { content: [{ type: 'text', text: JSON.stringify(scored ?? row, null, 2) }] };
    }

    case 'memory_search': {
      const { query, type, mode: rawMode, limit: rawLimit, min_score: rawMinScore } = args as {
        query: unknown;
        type?: unknown;
        mode?: unknown;
        limit?: unknown;
        min_score?: unknown;
      };
      if (typeof query !== 'string' || query.trim() === '') return { content: [{ type: 'text', text: 'query must be a non-empty string.' }] };
      if (type !== undefined && !isValidType(type)) return { content: [{ type: 'text', text: 'Invalid type filter.' }] };
      if (rawMode !== undefined && !isMemorySearchMode(rawMode)) {
        return { content: [{ type: 'text', text: 'mode must be lexical, semantic, or hybrid.' }] };
      }
      const mode: MemorySearchMode = rawMode ?? 'hybrid';
      const limit = Math.min(
        Math.max(Number.isFinite(Number(rawLimit)) ? Math.floor(Number(rawLimit)) : MEMORY_SEARCH_DEFAULT_LIMIT, 1),
        MEMORY_SEARCH_MAX_LIMIT
      );
      const minScore = rawMinScore === undefined
        ? -1
        : Math.min(Math.max(toFiniteNumber(rawMinScore, -1), -1), 1);
      const typeFilter = type as MemoryType | undefined;

      const lexicalFetchLimit = Math.min(Math.max(limit * 3, limit), 60);
      const semanticFetchLimit = Math.min(Math.max(limit * 3, limit), VECTORIZE_QUERY_TOP_K_MAX);
      const lexicalRows = mode === 'semantic'
        ? []
        : await runLexicalMemorySearch(env, brainId, query, typeFilter, lexicalFetchLimit);

      let semanticCandidates: SemanticMemoryCandidate[] = [];
      if (mode !== 'lexical') {
        if (!hasSemanticSearchBindings(env)) {
          if (mode === 'semantic') {
            return { content: [{ type: 'text', text: 'Semantic search unavailable: AI and MEMORY_INDEX bindings are not configured.' }] };
          }
        } else {
          try {
            semanticCandidates = await querySemanticMemoryCandidates(env, brainId, query, semanticFetchLimit, minScore);
          } catch (err) {
            if (mode === 'semantic') {
              const message = err instanceof Error ? err.message : 'Semantic query failed.';
              return { content: [{ type: 'text', text: `Semantic search failed: ${message}` }] };
            }
            console.warn('[memory_search:semantic]', err);
          }
        }
      }

      const semanticRows = semanticCandidates.length
        ? await loadMemoryRowsByIds(env, brainId, semanticCandidates.map((candidate) => candidate.memory_id), typeFilter)
        : [];
      const fusedRows = fuseSearchRows(mode, lexicalRows, semanticRows, semanticCandidates, limit);
      if (!fusedRows.length) return { content: [{ type: 'text', text: 'No memories found.' }] };

      const scored = await enrichAndProjectRows(env, brainId, fusedRows);
      return { content: [{ type: 'text', text: JSON.stringify(scored, null, 2) }] };
    }

    case 'memory_reindex': {
      const {
        limit: rawLimit,
        include_archived: rawIncludeArchived,
        wait_for_index: rawWaitForIndex,
        wait_timeout_seconds: rawWaitTimeoutSeconds,
      } = args as {
        limit?: unknown;
        include_archived?: unknown;
        wait_for_index?: unknown;
        wait_timeout_seconds?: unknown;
      };
      if (rawIncludeArchived !== undefined && typeof rawIncludeArchived !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_archived must be a boolean when provided.' }] };
      }
      if (rawWaitForIndex !== undefined && typeof rawWaitForIndex !== 'boolean') {
        return { content: [{ type: 'text', text: 'wait_for_index must be a boolean when provided.' }] };
      }
      if (rawWaitTimeoutSeconds !== undefined && !Number.isFinite(Number(rawWaitTimeoutSeconds))) {
        return { content: [{ type: 'text', text: 'wait_timeout_seconds must be a finite number when provided.' }] };
      }
      if (!hasSemanticSearchBindings(env)) {
        return { content: [{ type: 'text', text: 'Semantic reindex unavailable: AI and MEMORY_INDEX bindings are not configured.' }] };
      }
      const limit = Math.min(
        Math.max(Number.isFinite(Number(rawLimit)) ? Math.floor(Number(rawLimit)) : 500, 1),
        2000
      );
      const includeArchived = rawIncludeArchived === true;
      const waitForIndex = rawWaitForIndex !== false;
      const waitTimeoutSeconds = Math.min(
        Math.max(
          Number.isFinite(Number(rawWaitTimeoutSeconds))
            ? Math.floor(Number(rawWaitTimeoutSeconds))
            : VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS,
          1
        ),
        VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS_MAX
      );
      let sql = `
        SELECT id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at
        FROM memories
        WHERE brain_id = ?`;
      const params: unknown[] = [brainId];
      if (!includeArchived) {
        sql += ' AND archived_at IS NULL';
      }
      sql += ' ORDER BY updated_at DESC LIMIT ?';
      params.push(limit);
      const rows = await env.DB.prepare(sql).bind(...params).all<Record<string, unknown>>();
      if (!rows.results.length) {
        return { content: [{ type: 'text', text: 'No memories available for reindex.' }] };
      }
      const stats = await syncMemoriesToVectorIndex(env, brainId, rows.results);
      let indexReady: boolean | null = null;
      let waitAttempts = 0;
      let waitElapsedMs = 0;
      let processedUpToMutation: string | null = null;
      const waitedForMutationId = stats.mutation_ids.length ? stats.mutation_ids[stats.mutation_ids.length - 1] : null;
      if (waitForIndex) {
        if (!stats.mutation_ids.length) {
          indexReady = true;
        } else {
          let waitResult = stats.probe_vector_id
            ? await waitForVectorQueryReady(env, brainId, stats.probe_vector_id, waitTimeoutSeconds)
            : await waitForVectorMutationReady(env, waitedForMutationId ?? '', waitTimeoutSeconds);
          if (!waitResult.ready && waitedForMutationId && stats.probe_vector_id) {
            const mutationWait = await waitForVectorMutationReady(env, waitedForMutationId, waitTimeoutSeconds);
            waitResult = mutationWait.ready ? mutationWait : waitResult;
          }
          indexReady = waitResult.ready;
          waitAttempts = waitResult.attempts;
          waitElapsedMs = waitResult.elapsed_ms;
          processedUpToMutation = waitResult.processed_up_to_mutation;
        }
      }
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            processed: rows.results.length,
            include_archived: includeArchived,
            upserted: stats.upserted,
            deleted: stats.deleted,
            skipped: stats.skipped,
            mutation_count: stats.mutation_ids.length,
            probe_vector_id: stats.probe_vector_id,
            wait_for_index: waitForIndex,
            wait_timeout_seconds: waitTimeoutSeconds,
            index_ready: indexReady,
            wait_attempts: waitAttempts,
            wait_elapsed_ms: waitElapsedMs,
            waited_for_mutation_id: waitedForMutationId,
            processed_up_to_mutation: processedUpToMutation,
          }, null, 2),
        }],
      };
    }

    case 'memory_list': {
      const { type, tag, limit: rawLimit } = args as { type?: unknown; tag?: unknown; limit?: unknown };
      if (type !== undefined && !isValidType(type)) return { content: [{ type: 'text', text: 'Invalid type filter.' }] };
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 20, 1), 100);
      let query = 'SELECT * FROM memories WHERE brain_id = ? AND archived_at IS NULL';
      const params: unknown[] = [brainId];
      if (type) { query += ' AND type = ?'; params.push(type); }
      if (typeof tag === 'string' && tag) { query += ' AND tags LIKE ?'; params.push(`%${tag}%`); }
      query += ' ORDER BY created_at DESC LIMIT ?';
      params.push(limit);
      const results = await env.DB.prepare(query).bind(...params).all<Record<string, unknown>>();
      if (!results.results.length) return { content: [{ type: 'text', text: 'No memories found.' }] };
      const scored = await enrichAndProjectRows(env, brainId, results.results);
      return { content: [{ type: 'text', text: JSON.stringify(scored, null, 2) }] };
    }

    case 'memory_update': {
      const { id, content, title, tags, source, confidence, importance, archived } = args as {
        id: unknown;
        content?: unknown;
        title?: unknown;
        tags?: unknown;
        source?: unknown;
        confidence?: unknown;
        importance?: unknown;
        archived?: unknown;
      };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };
      if (source !== undefined && typeof source !== 'string') return { content: [{ type: 'text', text: 'source must be a string when provided.' }] };
      if (archived !== undefined && typeof archived !== 'boolean') return { content: [{ type: 'text', text: 'archived must be a boolean when provided.' }] };
      const existing = await env.DB.prepare('SELECT * FROM memories WHERE brain_id = ? AND id = ?').bind(brainId, id).first<{
        content: string;
        title: string | null;
        tags: string | null;
        source: string | null;
        confidence: number | null;
        importance: number | null;
        archived_at: number | null;
      }>();
      if (!existing) return { content: [{ type: 'text', text: 'Memory not found.' }] };
      const nextArchivedAt = typeof archived === 'boolean'
        ? (archived ? now() : null)
        : (existing.archived_at ?? null);
      await env.DB.prepare(
        'UPDATE memories SET content = ?, title = ?, tags = ?, source = ?, confidence = ?, importance = ?, archived_at = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
      ).bind(
        typeof content === 'string' && content.trim() ? content.trim() : existing.content,
        typeof title === 'string' ? title : existing.title,
        typeof tags === 'string' ? tags : existing.tags,
        typeof source === 'string' ? source : existing.source,
        confidence === undefined ? clampToRange(existing.confidence, 0.7) : clampToRange(confidence, 0.7),
        importance === undefined ? clampToRange(existing.importance, 0.5) : clampToRange(importance, 0.5),
        nextArchivedAt,
        now(),
        brainId,
        id
      ).run();
      const updated = await env.DB.prepare('SELECT * FROM memories WHERE brain_id = ? AND id = ?').bind(brainId, id).first<Record<string, unknown>>();
      if (!updated) return { content: [{ type: 'text', text: `Memory ${id} updated.` }] };
      await safeSyncMemoriesToVectorIndex(env, brainId, [updated], 'memory_update');
      const [scored] = await enrichAndProjectRows(env, brainId, [updated]);
      await logChangelog(env, brainId, 'memory_updated', 'memory', id, 'Updated memory', {
        updated_fields: {
          content: content !== undefined,
          title: title !== undefined,
          tags: tags !== undefined,
          source: source !== undefined,
          confidence: confidence !== undefined,
          importance: importance !== undefined,
          archived: archived !== undefined,
        },
      });
      return { content: [{ type: 'text', text: JSON.stringify({ message: `Memory ${id} updated.`, memory: scored ?? updated }) }] };
    }

    case 'memory_delete': {
      const { id } = args as { id: unknown };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };
      const result = await env.DB.prepare('DELETE FROM memories WHERE brain_id = ? AND id = ?').bind(brainId, id).run();
      if (result.meta.changes === 0) return { content: [{ type: 'text', text: 'Memory not found.' }] };
      await safeDeleteMemoryVectors(env, brainId, [id], 'memory_delete');
      await logChangelog(env, brainId, 'memory_deleted', 'memory', id, 'Deleted memory');
      return { content: [{ type: 'text', text: `Memory ${id} deleted.` }] };
    }

    case 'memory_stats': {
      const total = await env.DB.prepare('SELECT COUNT(*) as count FROM memories WHERE brain_id = ? AND archived_at IS NULL').bind(brainId).first<{ count: number }>();
      const archived = await env.DB.prepare('SELECT COUNT(*) as count FROM memories WHERE brain_id = ? AND archived_at IS NOT NULL').bind(brainId).first<{ count: number }>();
      const byType = await env.DB.prepare('SELECT type, COUNT(*) as count FROM memories WHERE brain_id = ? AND archived_at IS NULL GROUP BY type').bind(brainId).all();
      const relationStats = await env.DB.prepare('SELECT relation_type, COUNT(*) as count FROM memory_links WHERE brain_id = ? GROUP BY relation_type').bind(brainId).all();
      const recent = await env.DB.prepare(
        'SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance FROM memories WHERE brain_id = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT 5'
      ).bind(brainId).all<Record<string, unknown>>();
      const recentScored = await enrichAndProjectRows(env, brainId, recent.results);
      const avgDynamicConfidence = recentScored.length
        ? round3(recentScored.reduce((sum, m) => sum + toFiniteNumber(m.dynamic_confidence, 0.7), 0) / recentScored.length)
        : null;
      const avgDynamicImportance = recentScored.length
        ? round3(recentScored.reduce((sum, m) => sum + toFiniteNumber(m.dynamic_importance, 0.5), 0) / recentScored.length)
        : null;
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            total: total?.count ?? 0,
            archived: archived?.count ?? 0,
            by_type: byType.results,
            by_relation: relationStats.results,
            avg_recent_dynamic_confidence: avgDynamicConfidence,
            avg_recent_dynamic_importance: avgDynamicImportance,
            recent_5: recentScored,
          }, null, 2),
        }],
      };
    }

    case 'memory_tag_stats': {
      const { limit: rawLimit, min_count: rawMinCount, include_pairs: rawIncludePairs } = args as {
        limit?: unknown;
        min_count?: unknown;
        include_pairs?: unknown;
      };
      if (rawIncludePairs !== undefined && typeof rawIncludePairs !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_pairs must be a boolean when provided.' }] };
      }
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 20, 1), 100);
      const minCount = Math.min(Math.max(Number.isInteger(rawMinCount) ? (rawMinCount as number) : 2, 1), 1000);
      const includePairs = rawIncludePairs !== false;
      const rows = await env.DB.prepare(
        'SELECT id, tags FROM memories WHERE brain_id = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT 5000'
      ).bind(brainId).all<{ id: string; tags: string | null }>();

      const tagCounts = new Map<string, number>();
      const tagMemoryIds = new Map<string, Set<string>>();
      const pairCounts = new Map<string, number>();

      for (const row of rows.results) {
        const memoryId = typeof row.id === 'string' ? row.id : '';
        if (!memoryId) continue;
        const tags = Array.from(parseTagSet(row.tags));
        if (!tags.length) continue;
        for (const tag of tags) {
          tagCounts.set(tag, (tagCounts.get(tag) ?? 0) + 1);
          const ids = tagMemoryIds.get(tag) ?? new Set<string>();
          ids.add(memoryId);
          tagMemoryIds.set(tag, ids);
        }
        if (!includePairs || tags.length < 2) continue;
        const sortedTags = tags.slice(0, 20).sort();
        for (let i = 0; i < sortedTags.length; i++) {
          for (let j = i + 1; j < sortedTags.length; j++) {
            const key = `${sortedTags[i]}|${sortedTags[j]}`;
            pairCounts.set(key, (pairCounts.get(key) ?? 0) + 1);
          }
        }
      }

      const topTags = Array.from(tagCounts.entries())
        .filter(([, count]) => count >= minCount)
        .sort((a, b) => {
          if (b[1] !== a[1]) return b[1] - a[1];
          return a[0].localeCompare(b[0]);
        })
        .slice(0, limit)
        .map(([tag, count]) => ({
          tag,
          count,
          sample_memory_ids: Array.from(tagMemoryIds.get(tag) ?? []).slice(0, 5),
        }));

      const topPairs = includePairs
        ? Array.from(pairCounts.entries())
          .filter(([, count]) => count >= Math.max(2, minCount - 1))
          .sort((a, b) => {
            if (b[1] !== a[1]) return b[1] - a[1];
            return a[0].localeCompare(b[0]);
          })
          .slice(0, Math.min(25, limit))
          .map(([pair, count]) => {
            const [a, b] = pair.split('|');
            return { tag_a: a, tag_b: b, count };
          })
        : [];

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            memory_count: rows.results.length,
            unique_tag_count: tagCounts.size,
            min_count: minCount,
            tags: topTags,
            top_pairs: topPairs,
          }, null, 2),
        }],
      };
    }

    case 'tool_manifest': {
      const { tool: rawTool, include_schema: rawIncludeSchema, include_hashes: rawIncludeHashes, include_deprecated: rawIncludeDeprecated } = args as {
        tool?: unknown;
        include_schema?: unknown;
        include_hashes?: unknown;
        include_deprecated?: unknown;
      };
      if (rawTool !== undefined && typeof rawTool !== 'string') {
        return { content: [{ type: 'text', text: 'tool must be a string when provided.' }] };
      }
      if (rawIncludeSchema !== undefined && typeof rawIncludeSchema !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_schema must be a boolean when provided.' }] };
      }
      if (rawIncludeHashes !== undefined && typeof rawIncludeHashes !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_hashes must be a boolean when provided.' }] };
      }
      if (rawIncludeDeprecated !== undefined && typeof rawIncludeDeprecated !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_deprecated must be a boolean when provided.' }] };
      }

      const toolFilter = typeof rawTool === 'string' ? rawTool.trim() : '';
      const includeSchema = rawIncludeSchema !== false;
      const includeHashes = rawIncludeHashes !== false;
      const includeDeprecated = rawIncludeDeprecated !== false;

      const selected = toolFilter
        ? TOOLS.filter((tool) => tool.name === toolFilter)
        : TOOLS;
      if (toolFilter && !selected.length) {
        return { content: [{ type: 'text', text: `Unknown tool: ${toolFilter}` }] };
      }

      const manifestTools: Array<Record<string, unknown>> = [];
      for (const toolDef of selected) {
        const meta = getToolReleaseMeta(toolDef.name);
        const deprecated = isToolDeprecated(meta);
        if (!includeDeprecated && deprecated) continue;

        const schemaJson = canonicalJson(toolDef.inputSchema);
        const entry: Record<string, unknown> = {
          name: toolDef.name,
          description: toolDef.description,
          introduced_in: meta.introduced_in,
          deprecated: deprecated,
          deprecated_in: meta.deprecated_in ?? null,
          replaced_by: meta.replaced_by ?? null,
          notes: meta.notes ?? null,
        };
        if (includeSchema) entry.input_schema = toolDef.inputSchema;
        if (includeHashes) {
          entry.schema_hash = await sha256DigestBase64Url(schemaJson);
          entry.definition_hash = await sha256DigestBase64Url(`${toolDef.name}\n${toolDef.description}\n${schemaJson}`);
        }
        manifestTools.push(entry);
      }

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            server: { name: SERVER_NAME, version: SERVER_VERSION },
            generated_at: now(),
            hash_algorithm: includeHashes ? 'sha256/base64url' : null,
            requested_tool: toolFilter || null,
            tool_count: manifestTools.length,
            deprecated_count: manifestTools.filter((t) => t.deprecated === true).length,
            tools: manifestTools,
          }, null, 2),
        }],
      };
    }

    case 'tool_changelog': {
      const { since_version: rawSinceVersion, since, limit: rawLimit } = args as {
        since_version?: unknown;
        since?: unknown;
        limit?: unknown;
      };
      if (rawSinceVersion !== undefined && typeof rawSinceVersion !== 'string') {
        return { content: [{ type: 'text', text: 'since_version must be a semver string when provided.' }] };
      }
      const sinceVersion = typeof rawSinceVersion === 'string' ? rawSinceVersion.trim() : '';
      if (sinceVersion && !parseSemver(sinceVersion)) {
        return { content: [{ type: 'text', text: 'since_version must match semver format (for example "1.6.0").' }] };
      }
      let sinceTs: number | null = null;
      if (since !== undefined) {
        const sinceVal = Number(since);
        if (!Number.isFinite(sinceVal) || sinceVal < 0) {
          return { content: [{ type: 'text', text: 'since must be a non-negative unix timestamp.' }] };
        }
        sinceTs = Math.floor(sinceVal);
      }
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 20, 1), 100);

      let entries = [...TOOL_CHANGELOG];
      if (sinceVersion) {
        entries = entries.filter((entry) => compareSemver(entry.version, sinceVersion) > 0);
      }
      if (sinceTs !== null) {
        entries = entries.filter((entry) => entry.released_at >= sinceTs);
      }
      entries.sort((a, b) => {
        if (b.released_at !== a.released_at) return b.released_at - a.released_at;
        return compareSemver(b.version, a.version);
      });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            server: { name: SERVER_NAME, version: SERVER_VERSION },
            latest_version: SERVER_VERSION,
            filter: {
              since_version: sinceVersion || null,
              since: sinceTs,
              limit,
            },
            count: Math.min(entries.length, limit),
            entries: entries.slice(0, limit),
          }, null, 2),
        }],
      };
    }

    case 'memory_explain_score': {
      const { id, at } = args as { id: unknown; at?: unknown };
      if (typeof id !== 'string' || !id.trim()) {
        return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };
      }
      let tsNow = now();
      if (at !== undefined) {
        const atNum = Number(at);
        if (!Number.isFinite(atNum) || atNum < 0) {
          return { content: [{ type: 'text', text: 'at must be a non-negative unix timestamp when provided.' }] };
        }
        tsNow = Math.floor(atNum);
      }

      const row = await env.DB.prepare(
        'SELECT id, type, title, key, content, tags, source, created_at, updated_at, archived_at, confidence, importance FROM memories WHERE brain_id = ? AND id = ? LIMIT 1'
      ).bind(brainId, id.trim()).first<Record<string, unknown>>();
      if (!row) return { content: [{ type: 'text', text: 'Memory not found.' }] };

      const linkStatsMap = await loadLinkStatsMap(env, brainId);
      const stats = linkStatsMap.get(String(row.id ?? '')) ?? EMPTY_LINK_STATS;
      const sourceTrustMap = await loadSourceTrustMap(env, brainId);
      const sourceKey = typeof row.source === 'string' ? normalizeSourceKey(row.source) : '';
      const sourceTrust = sourceKey ? sourceTrustMap.get(sourceKey) : undefined;
      const breakdown = computeDynamicScoreBreakdown(row, stats, tsNow, sourceTrust);
      const memory = projectMemoryForClient({
        ...row,
        ...breakdown.link_stats,
        dynamic_confidence: breakdown.dynamic_confidence,
        dynamic_importance: breakdown.dynamic_importance,
      });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            memory_id: row.id,
            memory,
            explanation: {
              ...breakdown,
              confidence_delta: round3(breakdown.dynamic_confidence - breakdown.base_confidence),
              importance_delta: round3(breakdown.dynamic_importance - breakdown.base_importance),
            },
          }, null, 2),
        }],
      };
    }

    case 'memory_link': {
      const { from_id, to_id, label, relation_type } = args as {
        from_id: unknown;
        to_id: unknown;
        label?: unknown;
        relation_type?: unknown;
      };
      if (typeof from_id !== 'string' || !from_id) return { content: [{ type: 'text', text: 'from_id must be a non-empty string.' }] };
      if (typeof to_id !== 'string' || !to_id) return { content: [{ type: 'text', text: 'to_id must be a non-empty string.' }] };
      if (from_id === to_id) return { content: [{ type: 'text', text: 'Cannot link a memory to itself.' }] };
      if (relation_type !== undefined && !isValidRelationType(relation_type)) return { content: [{ type: 'text', text: 'Invalid relation_type.' }] };
      const relationType = isValidRelationType(relation_type) ? relation_type : 'related';

      // Verify both memories exist
      const fromMem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL').bind(brainId, from_id).first();
      if (!fromMem) return { content: [{ type: 'text', text: `Memory not found: ${from_id}` }] };
      const toMem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL').bind(brainId, to_id).first();
      if (!toMem) return { content: [{ type: 'text', text: `Memory not found: ${to_id}` }] };

      // De-duplicate links (treating pair as undirected)
      const existing = await env.DB.prepare(
        'SELECT id FROM memory_links WHERE brain_id = ? AND ((from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?))'
      ).bind(brainId, from_id, to_id, to_id, from_id).first<{ id: string }>();

      const labelVal = typeof label === 'string' && label.trim() ? label.trim() : null;
      if (existing?.id) {
        await env.DB.prepare(
          'UPDATE memory_links SET relation_type = ?, label = ? WHERE brain_id = ? AND id = ?'
        ).bind(relationType, labelVal, brainId, existing.id).run();
        await logChangelog(env, brainId, 'memory_link_updated', 'memory_link', existing.id, 'Updated link relation', {
          from_id,
          to_id,
          relation_type: relationType,
        });
        return { content: [{ type: 'text', text: JSON.stringify({ link_id: existing.id, from_id, to_id, relation_type: relationType, label: labelVal, updated: true }) }] };
      }

      const link_id = generateId();
      await env.DB.prepare(
        'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
      ).bind(link_id, brainId, from_id, to_id, relationType, labelVal, now()).run();
      await logChangelog(env, brainId, 'memory_link_created', 'memory_link', link_id, 'Created memory link', {
        from_id,
        to_id,
        relation_type: relationType,
      });

      return { content: [{ type: 'text', text: JSON.stringify({ link_id, from_id, to_id, relation_type: relationType, label: labelVal }) }] };
    }

    case 'memory_unlink': {
      const { from_id, to_id, relation_type } = args as { from_id: unknown; to_id: unknown; relation_type?: unknown };
      if (typeof from_id !== 'string' || !from_id) return { content: [{ type: 'text', text: 'from_id must be a non-empty string.' }] };
      if (typeof to_id !== 'string' || !to_id) return { content: [{ type: 'text', text: 'to_id must be a non-empty string.' }] };
      if (relation_type !== undefined && !isValidRelationType(relation_type)) return { content: [{ type: 'text', text: 'Invalid relation_type.' }] };

      let sql = 'DELETE FROM memory_links WHERE brain_id = ? AND ((from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?))';
      const params: unknown[] = [brainId, from_id, to_id, to_id, from_id];
      if (relation_type) {
        sql += ' AND relation_type = ?';
        params.push(relation_type);
      }
      const result = await env.DB.prepare(sql).bind(...params).run();

      if (result.meta.changes === 0) return { content: [{ type: 'text', text: 'No link found between these memories.' }] };
      await logChangelog(env, brainId, 'memory_link_removed', 'memory_link', `${from_id}::${to_id}`, 'Removed memory link', {
        from_id,
        to_id,
        relation_type: relation_type ?? null,
      });
      return { content: [{ type: 'text', text: `Link removed between ${from_id} and ${to_id}.` }] };
    }

    case 'memory_links': {
      const { id } = args as { id: unknown };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };

      // Verify memory exists
      const mem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL').bind(brainId, id).first();
      if (!mem) return { content: [{ type: 'text', text: 'Memory not found.' }] };

      // Fetch links in both directions with full memory data
      const fromLinks = await env.DB.prepare(
        'SELECT ml.id as link_id, ml.label, ml.relation_type, ml.to_id as connected_id, m.* FROM memory_links ml JOIN memories m ON m.id = ml.to_id WHERE ml.brain_id = ? AND m.brain_id = ? AND ml.from_id = ? AND m.archived_at IS NULL'
      ).bind(brainId, brainId, id).all();

      const toLinks = await env.DB.prepare(
        'SELECT ml.id as link_id, ml.label, ml.relation_type, ml.from_id as connected_id, m.* FROM memory_links ml JOIN memories m ON m.id = ml.from_id WHERE ml.brain_id = ? AND m.brain_id = ? AND ml.to_id = ? AND m.archived_at IS NULL'
      ).bind(brainId, brainId, id).all();

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
        const scored = computeDynamicScores(
          base,
          linkStatsMap.get(String(r.id ?? '')),
          tsNow,
          sourceKey ? sourceTrustMap.get(sourceKey) : undefined
        );
        return projectMemoryForClient({ ...base, ...scored });
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

      if (!results.length) return { content: [{ type: 'text', text: 'No links found for this memory.' }] };
      return { content: [{ type: 'text', text: JSON.stringify(results, null, 2) }] };
    }

    case 'memory_consolidate': {
      const { type, tag, older_than_days, limit: rawLimit } = args as {
        type?: unknown;
        tag?: unknown;
        older_than_days?: unknown;
        limit?: unknown;
      };
      if (type !== undefined && !isValidType(type)) return { content: [{ type: 'text', text: 'Invalid type filter.' }] };
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 300, 1), 1000);
      const params: unknown[] = [brainId];
      let query = 'SELECT id, type, title, key, content, tags, importance, created_at FROM memories WHERE brain_id = ? AND archived_at IS NULL';
      if (type) {
        query += ' AND type = ?';
        params.push(type);
      }
      if (typeof tag === 'string' && tag.trim()) {
        query += ' AND tags LIKE ?';
        params.push(`%${tag.trim()}%`);
      }
      if (older_than_days !== undefined) {
        const days = Number(older_than_days);
        if (!Number.isFinite(days) || days < 0) return { content: [{ type: 'text', text: 'older_than_days must be a non-negative number.' }] };
        query += ' AND created_at <= ?';
        params.push(now() - Math.floor(days * 86400));
      }
      query += ' ORDER BY created_at DESC LIMIT ?';
      params.push(limit);

      const rows = await env.DB.prepare(query).bind(...params).all<Record<string, unknown>>();
      const byFingerprint = new Map<string, Array<Record<string, unknown>>>();

      for (const row of rows.results) {
        const kind = String(row.type ?? '');
        const keyVal = typeof row.key === 'string' ? row.key.trim().toLowerCase() : '';
        const titleVal = typeof row.title === 'string' ? row.title.trim().toLowerCase() : '';
        const contentVal = typeof row.content === 'string'
          ? row.content.trim().toLowerCase().replace(/\s+/g, ' ').slice(0, 160)
          : '';
        const fingerprint = keyVal
          ? `${kind}|key|${keyVal}`
          : titleVal
            ? `${kind}|title|${titleVal}`
            : `${kind}|content|${contentVal}`;
        if (!contentVal && !titleVal && !keyVal) continue;
        const arr = byFingerprint.get(fingerprint);
        if (arr) arr.push(row);
        else byFingerprint.set(fingerprint, [row]);
      }

      const ts = now();
      const groups: Array<{ canonical_id: string; archived_ids: string[]; fingerprint: string }> = [];
      let archivedCount = 0;
      let linkedCount = 0;
      const archivedMemoryIdsForVectors: string[] = [];

      for (const [fingerprint, group] of byFingerprint) {
        if (group.length < 2) continue;
        const sorted = [...group].sort((a, b) => {
          const impA = clampToRange(a.importance, 0.5);
          const impB = clampToRange(b.importance, 0.5);
          if (impB !== impA) return impB - impA;
          const createdA = Number(a.created_at ?? 0);
          const createdB = Number(b.created_at ?? 0);
          return createdB - createdA;
        });
        const canonical = sorted[0];
        const canonicalId = String(canonical.id ?? '');
        if (!canonicalId) continue;

        const archivedIds: string[] = [];
        for (const dup of sorted.slice(1)) {
          const dupId = String(dup.id ?? '');
          if (!dupId) continue;
          archivedIds.push(dupId);
          await env.DB.prepare(
            'UPDATE memories SET archived_at = ?, updated_at = ? WHERE brain_id = ? AND id = ? AND archived_at IS NULL'
          ).bind(ts, ts, brainId, dupId).run();
          archivedCount++;
          archivedMemoryIdsForVectors.push(dupId);

          const existingLink = await env.DB.prepare(
            'SELECT id FROM memory_links WHERE brain_id = ? AND ((from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?))'
          ).bind(brainId, canonicalId, dupId, dupId, canonicalId).first<{ id: string }>();
          if (existingLink?.id) {
            await env.DB.prepare(
              'UPDATE memory_links SET relation_type = ?, label = ? WHERE brain_id = ? AND id = ?'
            ).bind('supersedes', 'consolidated duplicate', brainId, existingLink.id).run();
          } else {
            await env.DB.prepare(
              'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
            ).bind(generateId(), brainId, canonicalId, dupId, 'supersedes', 'consolidated duplicate', ts).run();
          }
          linkedCount++;
        }

        if (archivedIds.length > 0) {
          groups.push({ canonical_id: canonicalId, archived_ids: archivedIds, fingerprint });
        }
      }

      if (groups.length > 0) {
        await safeDeleteMemoryVectors(env, brainId, archivedMemoryIdsForVectors, 'memory_consolidate');
        await logChangelog(env, brainId, 'memory_consolidated', 'memory', groups[0].canonical_id, 'Consolidated duplicate memories', {
          groups_consolidated: groups.length,
          archived_count: archivedCount,
        });
      }

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            scanned: rows.results.length,
            groups_consolidated: groups.length,
            archived_count: archivedCount,
            supersedes_links_written: linkedCount,
            groups,
          }, null, 2),
        }],
      };
    }

    case 'memory_forget': {
      const { id, mode: rawMode, tag, older_than_days, max_importance, limit: rawLimit } = args as {
        id?: unknown;
        mode?: unknown;
        tag?: unknown;
        older_than_days?: unknown;
        max_importance?: unknown;
        limit?: unknown;
      };
      const mode = rawMode === 'hard' ? 'hard' : 'soft';
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 25, 1), 200);

      if (typeof id === 'string' && id.trim()) {
        if (mode === 'hard') {
          const result = await env.DB.prepare('DELETE FROM memories WHERE brain_id = ? AND id = ?').bind(brainId, id).run();
          if (result.meta.changes === 0) return { content: [{ type: 'text', text: 'Memory not found.' }] };
          await safeDeleteMemoryVectors(env, brainId, [id], 'memory_forget_hard_single');
          return { content: [{ type: 'text', text: JSON.stringify({ mode, deleted: 1, ids: [id] }) }] };
        }
        const ts = now();
        const result = await env.DB.prepare(
          'UPDATE memories SET archived_at = ?, updated_at = ? WHERE brain_id = ? AND id = ? AND archived_at IS NULL'
        ).bind(ts, ts, brainId, id).run();
        if (result.meta.changes === 0) return { content: [{ type: 'text', text: 'Memory not found or already archived.' }] };
        await safeDeleteMemoryVectors(env, brainId, [id], 'memory_forget_soft_single');
        return { content: [{ type: 'text', text: JSON.stringify({ mode, archived: 1, ids: [id] }) }] };
      }

      const where: string[] = ['brain_id = ?', 'archived_at IS NULL'];
      const params: unknown[] = [brainId];
      if (typeof tag === 'string' && tag.trim()) {
        where.push('tags LIKE ?');
        params.push(`%${tag.trim()}%`);
      }
      if (older_than_days !== undefined) {
        const days = Number(older_than_days);
        if (!Number.isFinite(days) || days < 0) return { content: [{ type: 'text', text: 'older_than_days must be a non-negative number.' }] };
        where.push('created_at <= ?');
        params.push(now() - Math.floor(days * 86400));
      }
      if (max_importance !== undefined) {
        const maxImportance = clampToRange(max_importance, 0.5);
        where.push('importance <= ?');
        params.push(maxImportance);
      }
      if (where.length === 1) {
        return { content: [{ type: 'text', text: 'Batch forgetting requires at least one filter (tag, older_than_days, or max_importance).' }] };
      }

      const idsResult = await env.DB.prepare(
        `SELECT id FROM memories WHERE ${where.join(' AND ')} ORDER BY importance ASC, created_at ASC LIMIT ?`
      ).bind(...params, limit).all<{ id: string }>();
      const ids = idsResult.results.map((r) => r.id).filter(Boolean);
      if (!ids.length) return { content: [{ type: 'text', text: 'No memories matched forgetting policy.' }] };

      const placeholders = ids.map(() => '?').join(', ');
      if (mode === 'hard') {
        await env.DB.prepare(`DELETE FROM memories WHERE brain_id = ? AND id IN (${placeholders})`).bind(brainId, ...ids).run();
        await safeDeleteMemoryVectors(env, brainId, ids, 'memory_forget_hard_batch');
        await logChangelog(env, brainId, 'memory_forget_hard', 'memory', ids[0], 'Hard-forgot memories', { count: ids.length, ids });
        return { content: [{ type: 'text', text: JSON.stringify({ mode, deleted: ids.length, ids }, null, 2) }] };
      }

      const ts = now();
      await env.DB.prepare(
        `UPDATE memories SET archived_at = ?, updated_at = ? WHERE brain_id = ? AND id IN (${placeholders})`
      ).bind(ts, ts, brainId, ...ids).run();
      await safeDeleteMemoryVectors(env, brainId, ids, 'memory_forget_soft_batch');
      await logChangelog(env, brainId, 'memory_forget_soft', 'memory', ids[0], 'Soft-forgot memories', { count: ids.length, ids });
      return { content: [{ type: 'text', text: JSON.stringify({ mode, archived: ids.length, ids }, null, 2) }] };
    }

    case 'memory_activate': {
      const { seed_id, query, hops: rawHops, limit: rawLimit, include_inferred } = args as {
        seed_id?: unknown;
        query?: unknown;
        hops?: unknown;
        limit?: unknown;
        include_inferred?: unknown;
      };
      if (seed_id !== undefined && typeof seed_id !== 'string') return { content: [{ type: 'text', text: 'seed_id must be a string when provided.' }] };
      if (query !== undefined && typeof query !== 'string') return { content: [{ type: 'text', text: 'query must be a string when provided.' }] };
      const hops = Math.min(Math.max(Number.isInteger(rawHops) ? (rawHops as number) : 2, 1), 4);
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 20, 1), 100);
      const includeInferred = include_inferred === undefined ? true : Boolean(include_inferred);

      const memoriesResult = await env.DB.prepare(
        'SELECT id, type, title, key, content, tags, source, confidence, importance, created_at, updated_at FROM memories WHERE brain_id = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT 2000'
      ).bind(brainId).all<Record<string, unknown>>();
      const memories = memoriesResult.results;
      if (!memories.length) return { content: [{ type: 'text', text: 'No active memories found.' }] };

      const memoryMap = new Map<string, Record<string, unknown>>();
      for (const m of memories) {
        const id = typeof m.id === 'string' ? m.id : '';
        if (id) memoryMap.set(id, m);
      }

      const seedIds = new Set<string>();
      if (typeof seed_id === 'string' && seed_id.trim()) {
        if (!memoryMap.has(seed_id)) return { content: [{ type: 'text', text: `Seed memory not found: ${seed_id}` }] };
        seedIds.add(seed_id);
      }
      if (typeof query === 'string' && query.trim()) {
        const q = query.trim().toLowerCase();
        const scoredMatches = memories.map((m) => {
          const id = String(m.id ?? '');
          const title = String(m.title ?? '');
          const key = String(m.key ?? '');
          const content = String(m.content ?? '');
          const source = String(m.source ?? '');
          const tags = String(m.tags ?? '');
          const idLc = id.toLowerCase();
          const titleLc = title.toLowerCase();
          const keyLc = key.toLowerCase();
          const contentLc = content.toLowerCase();
          const sourceLc = source.toLowerCase();
          const tagsLc = tags.toLowerCase();

          let score = 0;
          if (idLc === q) score += 9;
          else if (idLc.startsWith(q)) score += 6;
          else if (idLc.includes(q)) score += 4;
          if (titleLc.includes(q)) score += 4.5;
          if (keyLc.includes(q)) score += 3.8;
          if (sourceLc.includes(q)) score += 2.4;
          if (tagsLc.includes(q)) score += 2.2;
          if (contentLc.includes(q)) score += 1.2;
          return { id, score };
        }).filter((m) => m.score > 0);

        scoredMatches.sort((a, b) => b.score - a.score);
        for (const match of scoredMatches.slice(0, 5)) seedIds.add(match.id);
      }
      if (!seedIds.size) return { content: [{ type: 'text', text: 'Provide seed_id or query that matches at least one memory.' }] };

      const linksResult = await env.DB.prepare(
        'SELECT from_id, to_id, relation_type FROM memory_links WHERE brain_id = ? LIMIT 12000'
      ).bind(brainId).all<Record<string, unknown>>();
      const edges: GraphEdge[] = [];
      for (const row of linksResult.results) {
        const from = typeof row.from_id === 'string' ? row.from_id : '';
        const to = typeof row.to_id === 'string' ? row.to_id : '';
        if (!from || !to || !memoryMap.has(from) || !memoryMap.has(to)) continue;
        edges.push({ from, to, relation_type: normalizeRelation(row.relation_type) });
      }
      const adjacency = buildAdjacencyFromEdges(edges);

      const tagToIds = new Map<string, string[]>();
      for (const memory of memories) {
        const id = String(memory.id ?? '');
        const tagsRaw = typeof memory.tags === 'string' ? memory.tags : '';
        if (!id || !tagsRaw) continue;
        for (const raw of tagsRaw.split(',')) {
          const tag = raw.trim().toLowerCase();
          if (!tag) continue;
          const ids = tagToIds.get(tag);
          if (ids) ids.push(id);
          else tagToIds.set(tag, [id]);
        }
      }

      const inferredNeighborsFor = (id: string): Array<{ id: string; weight: number; shared: number }> => {
        if (!includeInferred) return [];
        const memory = memoryMap.get(id);
        const tagsRaw = typeof memory?.tags === 'string' ? memory.tags : '';
        if (!tagsRaw) return [];
        const explicitNeighborIds = new Set((adjacency.get(id) ?? []).map((n) => n.id));
        const sharedCounts = new Map<string, number>();
        for (const raw of tagsRaw.split(',')) {
          const tag = raw.trim().toLowerCase();
          if (!tag) continue;
          const ids = tagToIds.get(tag) ?? [];
          for (const candidateId of ids) {
            if (candidateId === id || explicitNeighborIds.has(candidateId)) continue;
            sharedCounts.set(candidateId, (sharedCounts.get(candidateId) ?? 0) + 1);
          }
        }
        return Array.from(sharedCounts.entries())
          .map(([neighborId, shared]) => ({
            id: neighborId,
            shared,
            weight: Math.min(0.42, 0.16 + shared * 0.08),
          }))
          .filter((e) => e.shared >= 1)
          .sort((a, b) => b.weight - a.weight)
          .slice(0, 6);
      };

      const activation = new Map<string, number>();
      let frontier = new Map<string, number>();
      const contributions = new Map<string, Array<{ from_id: string; relation: string; delta: number }>>();
      for (const id of seedIds) {
        activation.set(id, 1);
        frontier.set(id, 1);
      }

      for (let hop = 1; hop <= hops; hop++) {
        const next = new Map<string, number>();
        for (const [sourceId, sourceSignal] of frontier) {
          const explicit = adjacency.get(sourceId) ?? [];
          for (const neighbor of explicit) {
            const delta = sourceSignal * relationSignalWeight(neighbor.relation_type) * Math.pow(0.78, hop - 1);
            if (Math.abs(delta) < 0.01) continue;
            next.set(neighbor.id, (next.get(neighbor.id) ?? 0) + delta);
            const arr = contributions.get(neighbor.id);
            const item = { from_id: sourceId, relation: neighbor.relation_type, delta: round3(delta) };
            if (arr) arr.push(item);
            else contributions.set(neighbor.id, [item]);
          }
          for (const neighbor of inferredNeighborsFor(sourceId)) {
            const delta = sourceSignal * neighbor.weight * Math.pow(0.72, hop - 1);
            if (Math.abs(delta) < 0.008) continue;
            next.set(neighbor.id, (next.get(neighbor.id) ?? 0) + delta);
            const arr = contributions.get(neighbor.id);
            const item = { from_id: sourceId, relation: `inferred(shared:${neighbor.shared})`, delta: round3(delta) };
            if (arr) arr.push(item);
            else contributions.set(neighbor.id, [item]);
          }
        }

        frontier = new Map<string, number>();
        for (const [id, signal] of next) {
          const damped = signal * 0.74;
          if (Math.abs(damped) < 0.006) continue;
          frontier.set(id, damped);
          activation.set(id, (activation.get(id) ?? 0) + damped);
        }
      }

      const scoredMemories = await enrichAndProjectRows(env, brainId, memories);
      const scoredMap = new Map<string, Record<string, unknown>>();
      for (const memory of scoredMemories) {
        const id = typeof memory.id === 'string' ? memory.id : '';
        if (id) scoredMap.set(id, memory);
      }

      const ranked = Array.from(activation.entries())
        .map(([id, act]) => {
          const memory = scoredMap.get(id);
          if (!memory) return null;
          const conf = toFiniteNumber(memory.confidence, 0.7);
          const imp = toFiniteNumber(memory.importance, 0.5);
          const seedBonus = seedIds.has(id) ? 0.45 : 0;
          const neuralScore = round3(act + imp * 0.45 + conf * 0.2 + seedBonus);
          const contribs = (contributions.get(id) ?? [])
            .sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta))
            .slice(0, 3);
          return {
            id,
            type: memory.type,
            title: memory.title,
            key: memory.key,
            confidence: memory.confidence,
            importance: memory.importance,
            activation: round3(act),
            neural_score: neuralScore,
            top_signals: contribs,
          };
        })
        .filter((r): r is NonNullable<typeof r> => Boolean(r))
        .sort((a, b) => b.neural_score - a.neural_score)
        .slice(0, limit);

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            seeds: Array.from(seedIds),
            hops,
            include_inferred: includeInferred,
            results: ranked,
          }, null, 2),
        }],
      };
    }

    case 'memory_reinforce': {
      const { id, delta_confidence, delta_importance, spread, hops } = args as {
        id: unknown;
        delta_confidence?: unknown;
        delta_importance?: unknown;
        spread?: unknown;
        hops?: unknown;
      };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };
      const deltaConf = clampToRange(delta_confidence, 0.04, -0.5, 0.5);
      const deltaImp = clampToRange(delta_importance, 0.06, -0.5, 0.5);
      const spreadFactor = clampToRange(spread, 0.35);
      const spreadHops = Math.min(Math.max(Number.isInteger(hops) ? (hops as number) : 1, 0), 3);

      const memoriesResult = await env.DB.prepare(
        'SELECT id, confidence, importance FROM memories WHERE brain_id = ? AND archived_at IS NULL'
      ).bind(brainId).all<Record<string, unknown>>();
      const memoryMap = new Map<string, { confidence: number; importance: number }>();
      for (const row of memoriesResult.results) {
        const memoryId = typeof row.id === 'string' ? row.id : '';
        if (!memoryId) continue;
        memoryMap.set(memoryId, {
          confidence: clamp01(toFiniteNumber(row.confidence, 0.7)),
          importance: clamp01(toFiniteNumber(row.importance, 0.5)),
        });
      }
      if (!memoryMap.has(id)) return { content: [{ type: 'text', text: `Memory not found: ${id}` }] };

      const linksResult = await env.DB.prepare(
        'SELECT from_id, to_id, relation_type FROM memory_links WHERE brain_id = ? LIMIT 12000'
      ).bind(brainId).all<Record<string, unknown>>();
      const edges: GraphEdge[] = [];
      for (const row of linksResult.results) {
        const from = typeof row.from_id === 'string' ? row.from_id : '';
        const to = typeof row.to_id === 'string' ? row.to_id : '';
        if (!from || !to || !memoryMap.has(from) || !memoryMap.has(to)) continue;
        edges.push({ from, to, relation_type: normalizeRelation(row.relation_type) });
      }
      const adjacency = buildAdjacencyFromEdges(edges);

      const updates = new Map<string, { delta_confidence: number; delta_importance: number; hops: number }>();
      updates.set(id, { delta_confidence: deltaConf, delta_importance: deltaImp, hops: 0 });

      let frontier = new Map<string, number>([[id, 1]]);
      for (let depth = 1; depth <= spreadHops; depth++) {
        const next = new Map<string, number>();
        for (const [sourceId, sourceEnergy] of frontier) {
          const neighbors = adjacency.get(sourceId) ?? [];
          for (const neighbor of neighbors) {
            const signal = sourceEnergy * relationSpreadWeight(neighbor.relation_type);
            if (Math.abs(signal) < 0.04) continue;
            next.set(neighbor.id, (next.get(neighbor.id) ?? 0) + signal);
          }
        }
        frontier = new Map<string, number>();
        for (const [targetId, signal] of next) {
          const dampedSignal = signal * Math.pow(0.62, depth - 1);
          if (Math.abs(dampedSignal) < 0.04) continue;
          frontier.set(targetId, dampedSignal);
          if (targetId === id) continue;
          const prev = updates.get(targetId) ?? { delta_confidence: 0, delta_importance: 0, hops: depth };
          prev.delta_confidence += deltaConf * spreadFactor * dampedSignal;
          prev.delta_importance += deltaImp * spreadFactor * dampedSignal;
          prev.hops = Math.min(prev.hops, depth);
          updates.set(targetId, prev);
        }
      }

      const rankedUpdateIds = Array.from(updates.entries())
        .sort((a, b) => {
          const absA = Math.abs(a[1].delta_confidence) + Math.abs(a[1].delta_importance);
          const absB = Math.abs(b[1].delta_confidence) + Math.abs(b[1].delta_importance);
          return absB - absA;
        })
        .slice(0, 300)
        .map(([memoryId]) => memoryId);

      const ts = now();
      const changedIds: string[] = [];
      const changeSummary: Array<Record<string, unknown>> = [];
      for (const memoryId of rankedUpdateIds) {
        const current = memoryMap.get(memoryId);
        const update = updates.get(memoryId);
        if (!current || !update) continue;
        const newConfidence = round3(clamp01(current.confidence + update.delta_confidence));
        const newImportance = round3(clamp01(current.importance + update.delta_importance));
        if (newConfidence === current.confidence && newImportance === current.importance) continue;
        await env.DB.prepare(
          'UPDATE memories SET confidence = ?, importance = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
        ).bind(newConfidence, newImportance, ts, brainId, memoryId).run();
        changedIds.push(memoryId);
        changeSummary.push({
          id: memoryId,
          hops: update.hops,
          confidence_before: round3(current.confidence),
          confidence_after: newConfidence,
          importance_before: round3(current.importance),
          importance_after: newImportance,
        });
      }

      const scoredChanged = changedIds.length
        ? await enrichAndProjectRows(
          env,
          brainId,
          (await env.DB.prepare(
            `SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance FROM memories WHERE brain_id = ? AND id IN (${changedIds.map(() => '?').join(',')})`
          ).bind(brainId, ...changedIds).all<Record<string, unknown>>()).results
        )
        : [];

      if (changedIds.length > 0) {
        await logChangelog(env, brainId, 'memory_reinforced', 'memory', id, 'Reinforced memory graph', {
          updated_count: changedIds.length,
          spread_hops: spreadHops,
          spread: spreadFactor,
        });
      }

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            seed_id: id,
            spread_hops: spreadHops,
            spread: spreadFactor,
            updated_count: changedIds.length,
            updates: changeSummary.slice(0, 25),
            updated_memories: scoredChanged.slice(0, 25),
          }, null, 2),
        }],
      };
    }

    case 'memory_decay': {
      const { older_than_days, max_link_count, decay_confidence, decay_importance, limit: rawLimit } = args as {
        older_than_days?: unknown;
        max_link_count?: unknown;
        decay_confidence?: unknown;
        decay_importance?: unknown;
        limit?: unknown;
      };
      const olderThanDays = Math.max(0, Number.isFinite(Number(older_than_days)) ? Number(older_than_days) : 30);
      const maxLinkCount = Math.max(0, Number.isFinite(Number(max_link_count)) ? Math.floor(Number(max_link_count)) : 1);
      const decayConf = Math.min(Math.max(Number.isFinite(Number(decay_confidence)) ? Number(decay_confidence) : 0.01, 0), 0.5);
      const decayImp = Math.min(Math.max(Number.isFinite(Number(decay_importance)) ? Number(decay_importance) : 0.03, 0), 0.5);
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 200, 1), 1000);
      const cutoffTs = now() - Math.floor(olderThanDays * 86400);

      const candidates = await env.DB.prepare(
        `SELECT
          m.id,
          m.confidence,
          m.importance,
          m.updated_at,
          (SELECT COUNT(*) FROM memory_links ml WHERE ml.brain_id = ? AND (ml.from_id = m.id OR ml.to_id = m.id)) AS link_count
        FROM memories m
        WHERE m.brain_id = ?
          AND m.archived_at IS NULL
          AND m.updated_at <= ?
          AND (SELECT COUNT(*) FROM memory_links ml2 WHERE ml2.brain_id = ? AND (ml2.from_id = m.id OR ml2.to_id = m.id)) <= ?
        ORDER BY m.updated_at ASC
        LIMIT ?`
      ).bind(brainId, brainId, cutoffTs, brainId, maxLinkCount, limit).all<Record<string, unknown>>();

      const ts = now();
      const decayedIds: string[] = [];
      const updates: Array<Record<string, unknown>> = [];
      for (const row of candidates.results) {
        const memoryId = typeof row.id === 'string' ? row.id : '';
        if (!memoryId) continue;
        const currentConf = clamp01(toFiniteNumber(row.confidence, 0.7));
        const currentImp = clamp01(toFiniteNumber(row.importance, 0.5));
        const newConf = round3(clamp01(currentConf - decayConf));
        const newImp = round3(clamp01(currentImp - decayImp));
        if (newConf === currentConf && newImp === currentImp) continue;
        await env.DB.prepare(
          'UPDATE memories SET confidence = ?, importance = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
        ).bind(newConf, newImp, ts, brainId, memoryId).run();
        decayedIds.push(memoryId);
        updates.push({
          id: memoryId,
          link_count: toFiniteNumber(row.link_count, 0),
          confidence_before: round3(currentConf),
          confidence_after: newConf,
          importance_before: round3(currentImp),
          importance_after: newImp,
        });
      }

      if (decayedIds.length > 0) {
        await logChangelog(env, brainId, 'memory_decayed', 'memory', decayedIds[0], 'Applied memory decay', {
          decayed_count: decayedIds.length,
          older_than_days: olderThanDays,
          max_link_count: maxLinkCount,
        });
      }

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            older_than_days: olderThanDays,
            max_link_count: maxLinkCount,
            decay_confidence: decayConf,
            decay_importance: decayImp,
            candidate_count: candidates.results.length,
            decayed_count: decayedIds.length,
            updates: updates.slice(0, 50),
          }, null, 2),
        }],
      };
    }

    case 'memory_changelog': {
      const { limit: rawLimit, since, event_type, entity_id } = args as {
        limit?: unknown;
        since?: unknown;
        event_type?: unknown;
        entity_id?: unknown;
      };
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 25, 1), 200);
      const where: string[] = ['brain_id = ?'];
      const params: unknown[] = [brainId];
      if (since !== undefined) {
        const sinceVal = Number(since);
        if (!Number.isFinite(sinceVal) || sinceVal < 0) return { content: [{ type: 'text', text: 'since must be a non-negative unix timestamp.' }] };
        where.push('created_at >= ?');
        params.push(Math.floor(sinceVal));
      }
      if (typeof event_type === 'string' && event_type.trim()) {
        where.push('event_type = ?');
        params.push(event_type.trim());
      }
      if (typeof entity_id === 'string' && entity_id.trim()) {
        where.push('entity_id = ?');
        params.push(entity_id.trim());
      }
      const rows = await env.DB.prepare(
        `SELECT id, event_type, entity_type, entity_id, summary, payload, created_at
         FROM memory_changelog
         WHERE ${where.join(' AND ')}
         ORDER BY created_at DESC
         LIMIT ?`
      ).bind(...params, limit).all<Record<string, unknown>>();

      const entries = rows.results.map((row) => {
        let parsedPayload: unknown = row.payload;
        if (typeof row.payload === 'string' && row.payload) {
          try {
            parsedPayload = JSON.parse(row.payload);
          } catch {
            parsedPayload = row.payload;
          }
        }
        return {
          id: row.id,
          event_type: row.event_type,
          entity_type: row.entity_type,
          entity_id: row.entity_id,
          summary: row.summary,
          payload: parsedPayload,
          created_at: row.created_at,
        };
      });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            server_version: SERVER_VERSION,
            count: entries.length,
            entries,
          }, null, 2),
        }],
      };
    }

    case 'memory_conflicts': {
      const { min_confidence, limit: rawLimit, include_resolved: rawIncludeResolved } = args as {
        min_confidence?: unknown;
        limit?: unknown;
        include_resolved?: unknown;
      };
      if (rawIncludeResolved !== undefined && typeof rawIncludeResolved !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_resolved must be a boolean when provided.' }] };
      }
      const minConfidence = clampToRange(min_confidence, 0.7);
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 40, 1), 200);
      const includeResolved = rawIncludeResolved === true;

      const factsResult = await env.DB.prepare(
        'SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance FROM memories WHERE brain_id = ? AND archived_at IS NULL AND type = ? LIMIT 3000'
      ).bind(brainId, 'fact').all<Record<string, unknown>>();
      const scoredFacts = await enrichAndProjectRows(env, brainId, factsResult.results);
      const factMap = new Map<string, Record<string, unknown>>();
      for (const fact of scoredFacts) {
        const id = typeof fact.id === 'string' ? fact.id : '';
        if (id) factMap.set(id, fact);
      }

      const conflicts: Array<Record<string, unknown>> = [];
      const seenPairs = new Set<string>();
      const normalizedContent = (v: unknown) => String(v ?? '').trim().toLowerCase().replace(/\s+/g, ' ');

      // Explicit contradiction edges between fact memories.
      const contradictionLinks = await env.DB.prepare(
        `SELECT ml.id as link_id, ml.label, ml.from_id, ml.to_id
         FROM memory_links ml
         JOIN memories m1 ON m1.id = ml.from_id AND m1.brain_id = ? AND m1.type = 'fact' AND m1.archived_at IS NULL
         JOIN memories m2 ON m2.id = ml.to_id AND m2.brain_id = ? AND m2.type = 'fact' AND m2.archived_at IS NULL
         WHERE ml.brain_id = ?
           AND ml.relation_type = 'contradicts'
         LIMIT 2000`
      ).bind(brainId, brainId, brainId).all<Record<string, unknown>>();
      for (const row of contradictionLinks.results) {
        const aId = String(row.from_id ?? '');
        const bId = String(row.to_id ?? '');
        const a = factMap.get(aId);
        const b = factMap.get(bId);
        if (!a || !b) continue;
        const confA = toFiniteNumber(a.confidence, 0.7);
        const confB = toFiniteNumber(b.confidence, 0.7);
        if (confA < minConfidence || confB < minConfidence) continue;
        const key = pairKey(aId, bId);
        if (seenPairs.has(key)) continue;
        seenPairs.add(key);
        conflicts.push({
          pair_key: key,
          conflict_type: 'explicit_contradiction_link',
          confidence_pair: [round3(confA), round3(confB)],
          link_id: row.link_id,
          link_label: row.label,
          a: { id: aId, key: a.key, title: a.title, content: a.content, confidence: a.confidence, importance: a.importance },
          b: { id: bId, key: b.key, title: b.title, content: b.content, confidence: b.confidence, importance: b.importance },
        });
      }

      // Key-based fact conflicts: same key with materially different values.
      const byKey = new Map<string, Array<Record<string, unknown>>>();
      for (const fact of scoredFacts) {
        const keyRaw = typeof fact.key === 'string' ? fact.key.trim().toLowerCase() : '';
        if (!keyRaw) continue;
        const arr = byKey.get(keyRaw);
        if (arr) arr.push(fact);
        else byKey.set(keyRaw, [fact]);
      }
      for (const [keyName, facts] of byKey) {
        if (facts.length < 2) continue;
        const sorted = [...facts].sort((a, b) => toFiniteNumber(b.confidence, 0) - toFiniteNumber(a.confidence, 0));
        for (let i = 0; i < sorted.length; i++) {
          for (let j = i + 1; j < sorted.length; j++) {
            const a = sorted[i];
            const b = sorted[j];
            const aId = String(a.id ?? '');
            const bId = String(b.id ?? '');
            if (!aId || !bId) continue;
            const confA = toFiniteNumber(a.confidence, 0.7);
            const confB = toFiniteNumber(b.confidence, 0.7);
            if (confA < minConfidence || confB < minConfidence) continue;
            const contentA = normalizedContent(a.content);
            const contentB = normalizedContent(b.content);
            if (!contentA || !contentB || contentA === contentB) continue;
            const key = pairKey(aId, bId);
            if (seenPairs.has(key)) continue;
            seenPairs.add(key);
            conflicts.push({
              pair_key: key,
              conflict_type: 'fact_key_value_conflict',
              fact_key: keyName,
              confidence_pair: [round3(confA), round3(confB)],
              a: { id: aId, content: a.content, confidence: a.confidence, importance: a.importance, updated_at: a.updated_at },
              b: { id: bId, content: b.content, confidence: b.confidence, importance: b.importance, updated_at: b.updated_at },
            });
            if (conflicts.length >= limit) break;
          }
          if (conflicts.length >= limit) break;
        }
        if (conflicts.length >= limit) break;
      }

      conflicts.sort((a, b) => {
        const aPair = Array.isArray(a.confidence_pair) ? a.confidence_pair : [0, 0];
        const bPair = Array.isArray(b.confidence_pair) ? b.confidence_pair : [0, 0];
        const aScore = toFiniteNumber(aPair[0], 0) + toFiniteNumber(aPair[1], 0);
        const bScore = toFiniteNumber(bPair[0], 0) + toFiniteNumber(bPair[1], 0);
        return bScore - aScore;
      });

      const keys = Array.from(new Set(conflicts.map((conflict) => String(conflict.pair_key ?? '')).filter(Boolean)));
      const resolutionMap = new Map<string, Record<string, unknown>>();
      if (keys.length) {
        const rows = await env.DB.prepare(
          `SELECT pair_key, status, canonical_id, note, updated_at
           FROM memory_conflict_resolutions
           WHERE brain_id = ? AND pair_key IN (${keys.map(() => '?').join(',')})`
        ).bind(brainId, ...keys).all<Record<string, unknown>>();
        for (const row of rows.results) {
          const key = typeof row.pair_key === 'string' ? row.pair_key : '';
          if (key) resolutionMap.set(key, row);
        }
      }

      const enrichedConflicts = conflicts
        .map((conflict) => {
          const key = typeof conflict.pair_key === 'string' ? conflict.pair_key : '';
          const resolution = key ? resolutionMap.get(key) : undefined;
          return {
            ...conflict,
            resolution_status: resolution?.status ?? null,
            resolution_canonical_id: resolution?.canonical_id ?? null,
            resolution_note: resolution?.note ?? null,
            resolution_updated_at: resolution?.updated_at ?? null,
          };
        })
        .filter((conflict) => {
          if (includeResolved) return true;
          const status = typeof conflict.resolution_status === 'string' ? conflict.resolution_status : '';
          return !(status === 'resolved' || status === 'superseded' || status === 'dismissed');
        });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            min_confidence: minConfidence,
            include_resolved: includeResolved,
            total_conflicts: enrichedConflicts.length,
            conflicts: enrichedConflicts.slice(0, limit),
          }, null, 2),
        }],
      };
    }

    case 'objective_set': {
      const { id: rawId, title, content, kind: rawKind, horizon: rawHorizon, status: rawStatus, priority, tags } = args as {
        id?: unknown;
        title: unknown;
        content?: unknown;
        kind?: unknown;
        horizon?: unknown;
        status?: unknown;
        priority?: unknown;
        tags?: unknown;
      };
      if (typeof title !== 'string' || !title.trim()) return { content: [{ type: 'text', text: 'title must be a non-empty string.' }] };
      if (content !== undefined && typeof content !== 'string') return { content: [{ type: 'text', text: 'content must be a string when provided.' }] };
      if (tags !== undefined && typeof tags !== 'string') return { content: [{ type: 'text', text: 'tags must be a comma-separated string when provided.' }] };
      const kind = rawKind === 'curiosity' ? 'curiosity' : 'goal';
      const horizon = rawHorizon === 'short' || rawHorizon === 'medium' || rawHorizon === 'long' ? rawHorizon : 'long';
      const status = rawStatus === 'paused' || rawStatus === 'done' ? rawStatus : 'active';
      const priorityVal = clampToRange(priority, kind === 'goal' ? 0.82 : 0.74);

      const rootId = await ensureObjectiveRoot(env, brainId, safeSyncMemoriesToVectorIndex);
      const ts = now();
      const extraTags = typeof tags === 'string'
        ? tags.split(',').map((t) => t.trim()).filter(Boolean)
        : [];
      const objectiveTags = Array.from(new Set([
        'objective_node',
        'autonomous_objective',
        `kind_${kind}`,
        `horizon_${horizon}`,
        `status_${status}`,
        ...extraTags,
      ])).join(',');
      const objectiveContent = typeof content === 'string' && content.trim()
        ? content.trim()
        : (kind === 'goal'
          ? `Long-term goal: ${title.trim()}`
          : `Curiosity to explore: ${title.trim()}`);

      let objectiveId = typeof rawId === 'string' && rawId.trim() ? rawId.trim() : '';
      if (objectiveId) {
        const exists = await env.DB.prepare(
          'SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL'
        ).bind(brainId, objectiveId).first<{ id: string }>();
        if (!exists?.id) return { content: [{ type: 'text', text: `Objective memory not found: ${objectiveId}` }] };
        await env.DB.prepare(
          'UPDATE memories SET type = ?, title = ?, content = ?, tags = ?, source = ?, importance = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
        ).bind('note', title.trim(), objectiveContent, objectiveTags, 'autonomous_objective', priorityVal, ts, brainId, objectiveId).run();
      } else {
        const key = `objective:${kind}:${slugify(title.trim())}`;
        const existing = await env.DB.prepare(
          'SELECT id FROM memories WHERE brain_id = ? AND key = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT 1'
        ).bind(brainId, key).first<{ id: string }>();
        if (existing?.id) {
          objectiveId = existing.id;
          await env.DB.prepare(
            'UPDATE memories SET title = ?, content = ?, tags = ?, source = ?, importance = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
          ).bind(title.trim(), objectiveContent, objectiveTags, 'autonomous_objective', priorityVal, ts, brainId, objectiveId).run();
        } else {
          objectiveId = generateId();
          await env.DB.prepare(
            'INSERT INTO memories (id, brain_id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)'
          ).bind(
            objectiveId,
            brainId,
            'note',
            title.trim(),
            key,
            objectiveContent,
            objectiveTags,
            'autonomous_objective',
            kind === 'goal' ? 0.84 : 0.72,
            priorityVal,
            ts,
            ts
          ).run();
        }
      }

      const linkRelation: RelationType = kind === 'goal' ? 'supports' : 'example_of';
      const linkLabel = kind === 'goal'
        ? `objective (${horizon})`
        : `curiosity (${horizon})`;
      const existingLink = await env.DB.prepare(
        'SELECT id FROM memory_links WHERE brain_id = ? AND ((from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?)) LIMIT 1'
      ).bind(brainId, rootId, objectiveId, objectiveId, rootId).first<{ id: string }>();
      if (existingLink?.id) {
        await env.DB.prepare(
          'UPDATE memory_links SET relation_type = ?, label = ? WHERE brain_id = ? AND id = ?'
        ).bind(linkRelation, linkLabel, brainId, existingLink.id).run();
      } else {
        await env.DB.prepare(
          'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
        ).bind(generateId(), brainId, rootId, objectiveId, linkRelation, linkLabel, ts).run();
      }

      const objectiveRow = await env.DB.prepare(
        'SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance FROM memories WHERE brain_id = ? AND id = ? LIMIT 1'
      ).bind(brainId, objectiveId).first<Record<string, unknown>>();
      if (objectiveRow) {
        await safeSyncMemoriesToVectorIndex(env, brainId, [{ ...objectiveRow, archived_at: null }], 'objective_set');
      }
      const [objectiveMemory] = objectiveRow ? await enrichAndProjectRows(env, brainId, [objectiveRow]) : [];
      await logChangelog(env, brainId, 'objective_upserted', 'memory', objectiveId, 'Upserted autonomous objective node', {
        kind,
        horizon,
        status,
        root_id: rootId,
      });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            root_objective_id: rootId,
            objective_id: objectiveId,
            kind,
            horizon,
            status,
            objective: objectiveMemory ?? objectiveRow,
          }, null, 2),
        }],
      };
    }

    case 'objective_list': {
      const { kind: rawKind, status: rawStatus, limit: rawLimit } = args as {
        kind?: unknown;
        status?: unknown;
        limit?: unknown;
      };
      const kind = rawKind === 'goal' || rawKind === 'curiosity' ? rawKind : null;
      const status = rawStatus === 'active' || rawStatus === 'paused' || rawStatus === 'done' ? rawStatus : null;
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 50, 1), 200);

      let query = 'SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance FROM memories WHERE brain_id = ? AND archived_at IS NULL AND tags LIKE ?';
      const params: unknown[] = [brainId, '%objective_node%'];
      if (kind) {
        query += ' AND tags LIKE ?';
        params.push(`%kind_${kind}%`);
      }
      if (status) {
        query += ' AND tags LIKE ?';
        params.push(`%status_${status}%`);
      }
      query += ' ORDER BY created_at DESC LIMIT ?';
      params.push(limit);

      const rows = await env.DB.prepare(query).bind(...params).all<Record<string, unknown>>();
      const objectives = await enrichAndProjectRows(env, brainId, rows.results);
      const root = await env.DB.prepare(
        'SELECT id FROM memories WHERE brain_id = ? AND key = ? AND archived_at IS NULL LIMIT 1'
      ).bind(brainId, 'autonomous_objectives_root').first<{ id: string }>();

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            root_objective_id: root?.id ?? null,
            count: objectives.length,
            objectives,
          }, null, 2),
        }],
      };
    }

    case 'objective_next_actions': {
      const { limit: rawLimit, include_done: rawIncludeDone } = args as { limit?: unknown; include_done?: unknown };
      if (rawIncludeDone !== undefined && typeof rawIncludeDone !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_done must be a boolean when provided.' }] };
      }
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 12, 1), 100);
      const includeDone = rawIncludeDone === true;

      const rows = await env.DB.prepare(
        `SELECT id, type, title, key, content, tags, source, created_at, updated_at, confidence, importance
         FROM memories
         WHERE brain_id = ? AND archived_at IS NULL AND tags LIKE ?
         ORDER BY updated_at DESC
         LIMIT 500`
      ).bind(brainId, '%objective_node%').all<Record<string, unknown>>();
      const objectives = await enrichAndProjectRows(env, brainId, rows.results);
      const tsNow = now();

      const actions: Array<Record<string, unknown>> = [];
      for (const objective of objectives) {
        const id = typeof objective.id === 'string' ? objective.id : '';
        if (!id) continue;
        const tags = parseTagSet(objective.tags);
        const status = tags.has('status_done')
          ? 'done'
          : tags.has('status_paused')
            ? 'paused'
            : 'active';
        if (!includeDone && status === 'done') continue;
        if (status === 'paused') continue;
        const kind = tags.has('kind_curiosity') ? 'curiosity' : 'goal';
        const horizon = tags.has('horizon_short')
          ? 'short'
          : tags.has('horizon_medium')
            ? 'medium'
            : 'long';
        const title = typeof objective.title === 'string' && objective.title.trim()
          ? objective.title.trim()
          : (typeof objective.key === 'string' && objective.key.trim() ? objective.key.trim() : id);
        const updatedAt = toFiniteNumber(objective.updated_at, tsNow);
        const ageDays = Math.max(0, (tsNow - updatedAt) / 86400);
        const freshness = ageDays < 3 ? 1 : ageDays < 14 ? 0.75 : ageDays < 45 ? 0.45 : 0.2;
        const importanceScore = clampToRange(objective.dynamic_importance ?? objective.importance, 0.6);
        const urgency = horizon === 'short' ? 0.2 : horizon === 'medium' ? 0.12 : 0.06;
        const actionScore = round3(clamp01((importanceScore * 0.68) + (freshness * 0.22) + urgency));
        const actionText = kind === 'curiosity'
          ? `Run one focused exploration step for "${title}" and capture one concrete finding.`
          : `Advance "${title}" with one concrete deliverable-level action today.`;
        actions.push({
          objective_id: id,
          title,
          kind,
          horizon,
          status,
          action: actionText,
          score: actionScore,
          dynamic_importance: round3(importanceScore),
          last_updated_days_ago: round3(ageDays),
        });
      }

      actions.sort((a, b) => toFiniteNumber(b.score, 0) - toFiniteNumber(a.score, 0));
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            count: Math.min(actions.length, limit),
            actions: actions.slice(0, limit),
          }, null, 2),
        }],
      };
    }

    case 'memory_link_suggest': {
      const { id: rawId, query: rawQuery, limit: rawLimit, min_score: rawMinScore, include_existing: rawIncludeExisting } = args as {
        id?: unknown;
        query?: unknown;
        limit?: unknown;
        min_score?: unknown;
        include_existing?: unknown;
      };
      if (rawId !== undefined && typeof rawId !== 'string') return { content: [{ type: 'text', text: 'id must be a string when provided.' }] };
      if (rawQuery !== undefined && typeof rawQuery !== 'string') return { content: [{ type: 'text', text: 'query must be a string when provided.' }] };
      if (rawIncludeExisting !== undefined && typeof rawIncludeExisting !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_existing must be a boolean when provided.' }] };
      }
      const policy = await getBrainPolicy(env, brainId);
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 20, 1), 120);
      const minScore = clampToRange(rawMinScore, policy.min_link_suggestion_score);
      const includeExisting = rawIncludeExisting === true;

      const nodes = await loadActiveMemoryNodes(env, brainId, 1400);
      if (!nodes.length) return { content: [{ type: 'text', text: 'No memories available.' }] };
      const nodeById = new Map(nodes.map((node) => [node.id, node]));
      const seedIds = new Set<string>();
      if (typeof rawId === 'string' && rawId.trim()) {
        const id = rawId.trim();
        if (!nodeById.has(id)) return { content: [{ type: 'text', text: `Seed memory not found: ${id}` }] };
        seedIds.add(id);
      }
      if (typeof rawQuery === 'string' && rawQuery.trim()) {
        const query = rawQuery.trim().toLowerCase();
        const scoredMatches = nodes.map((node) => {
          const text = `${node.id} ${node.title ?? ''} ${node.key ?? ''} ${node.content} ${node.source ?? ''}`.toLowerCase();
          const exact = text.includes(query) ? 1 : 0;
          const tokenSet = new Set(tokenizeText(text, 120));
          const queryTokens = tokenizeText(query, 24);
          let tokenHits = 0;
          for (const token of queryTokens) if (tokenSet.has(token)) tokenHits++;
          const score = (exact * 0.7) + (queryTokens.length ? (tokenHits / queryTokens.length) * 0.3 : 0);
          return { id: node.id, score };
        })
          .filter((row) => row.score > 0)
          .sort((a, b) => b.score - a.score)
          .slice(0, 8);
        for (const match of scoredMatches) seedIds.add(match.id);
      }
      if (!seedIds.size) {
        for (const node of nodes.slice(0, 3)) seedIds.add(node.id);
      }

      const links = await loadExplicitMemoryLinks(env, brainId, 9000);
      const existingPairs = new Set(links.map((edge) => pairKey(edge.from_id, edge.to_id)));
      const tokenCache = new Map<string, Set<string>>();
      const tagCache = new Map<string, Set<string>>();
      const getTokenSet = (node: MemoryGraphNode): Set<string> => {
        const existing = tokenCache.get(node.id);
        if (existing) return existing;
        const tokens = new Set(tokenizeText(`${node.title ?? ''} ${node.key ?? ''} ${node.content} ${node.source ?? ''}`, 120));
        tokenCache.set(node.id, tokens);
        return tokens;
      };
      const getTagSet = (node: MemoryGraphNode): Set<string> => {
        const existing = tagCache.get(node.id);
        if (existing) return existing;
        const tags = parseTagSet(node.tags);
        tagCache.set(node.id, tags);
        return tags;
      };

      const suggestionsByPair = new Map<string, Record<string, unknown>>();
      for (const seedId of seedIds) {
        const seed = nodeById.get(seedId);
        if (!seed) continue;
        const seedTokens = getTokenSet(seed);
        const seedTags = getTagSet(seed);
        const seedSource = seed.source ? normalizeSourceKey(seed.source) : '';
        for (const candidate of nodes) {
          if (candidate.id === seed.id) continue;
          const key = pairKey(seed.id, candidate.id);
          if (!includeExisting && existingPairs.has(key)) continue;
          const candidateTokens = getTokenSet(candidate);
          const candidateTags = getTagSet(candidate);
          let sharedTagCount = 0;
          const sharedTags: string[] = [];
          for (const tag of seedTags) {
            if (!candidateTags.has(tag)) continue;
            sharedTagCount++;
            if (sharedTags.length < 5) sharedTags.push(tag);
          }
          const tagScore = Math.min(1, sharedTagCount / 3);
          const lexicalScore = jaccardSimilarity(seedTokens, candidateTokens);
          const sourceScore = seedSource && candidate.source && seedSource === normalizeSourceKey(candidate.source) ? 1 : 0;
          const ageDeltaDays = Math.abs(toFiniteNumber(seed.updated_at, 0) - toFiniteNumber(candidate.updated_at, 0)) / 86400;
          const temporalScore = ageDeltaDays < 7 ? 1 : ageDeltaDays < 30 ? 0.65 : ageDeltaDays < 120 ? 0.3 : 0.08;
          const typeScore = seed.type === candidate.type ? 1 : 0.45;
          const score = round3(
            (tagScore * 0.45)
            + (lexicalScore * 0.35)
            + (sourceScore * 0.1)
            + (temporalScore * 0.05)
            + (typeScore * 0.05)
          );
          if (score < minScore) continue;

          const prev = suggestionsByPair.get(key);
          if (prev && toFiniteNumber(prev.score, 0) >= score) continue;
          suggestionsByPair.set(key, {
            from_id: seed.id,
            to_id: candidate.id,
            relation_hint: 'related',
            score,
            reasons: {
              shared_tags: sharedTags,
              lexical_similarity: round3(lexicalScore),
              same_source: sourceScore === 1,
              temporal_score: round3(temporalScore),
              type_score: round3(typeScore),
            },
          });
        }
      }

      const suggestions = Array.from(suggestionsByPair.values())
        .sort((a, b) => toFiniteNumber(b.score, 0) - toFiniteNumber(a.score, 0))
        .slice(0, limit);

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            seed_ids: Array.from(seedIds),
            min_score: minScore,
            count: suggestions.length,
            suggestions,
          }, null, 2),
        }],
      };
    }

    case 'memory_path_find': {
      const { from_id: rawFrom, to_id: rawTo, max_hops: rawMaxHops, limit: rawLimit } = args as {
        from_id: unknown;
        to_id: unknown;
        max_hops?: unknown;
        limit?: unknown;
      };
      if (typeof rawFrom !== 'string' || !rawFrom.trim()) return { content: [{ type: 'text', text: 'from_id must be a non-empty string.' }] };
      if (typeof rawTo !== 'string' || !rawTo.trim()) return { content: [{ type: 'text', text: 'to_id must be a non-empty string.' }] };
      const fromId = rawFrom.trim();
      const toId = rawTo.trim();
      if (fromId === toId) {
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              from_id: fromId,
              to_id: toId,
              count: 1,
              paths: [{ nodes: [fromId], edges: [], hops: 0, avg_score: 1 }],
            }, null, 2),
          }],
        };
      }

      const policy = await getBrainPolicy(env, brainId);
      const maxHops = Math.min(Math.max(Number.isInteger(rawMaxHops) ? (rawMaxHops as number) : policy.path_max_hops, 1), 8);
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 5, 1), 20);

      const fromExists = await env.DB.prepare(
        'SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL LIMIT 1'
      ).bind(brainId, fromId).first<{ id: string }>();
      if (!fromExists?.id) return { content: [{ type: 'text', text: `Memory not found: ${fromId}` }] };
      const toExists = await env.DB.prepare(
        'SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL LIMIT 1'
      ).bind(brainId, toId).first<{ id: string }>();
      if (!toExists?.id) return { content: [{ type: 'text', text: `Memory not found: ${toId}` }] };

      const links = await loadExplicitMemoryLinks(env, brainId, 12000);
      const adjacency = new Map<string, Array<{ id: string; relation_type: RelationType; link_id: string; label: string | null; weight: number }>>();
      for (const link of links) {
        const weight = relationSignalWeight(link.relation_type);
        const fromArr = adjacency.get(link.from_id);
        const fromEdge = { id: link.to_id, relation_type: link.relation_type, link_id: link.id, label: link.label, weight };
        if (fromArr) fromArr.push(fromEdge);
        else adjacency.set(link.from_id, [fromEdge]);
        const toArr = adjacency.get(link.to_id);
        const toEdge = { id: link.from_id, relation_type: link.relation_type, link_id: link.id, label: link.label, weight };
        if (toArr) toArr.push(toEdge);
        else adjacency.set(link.to_id, [toEdge]);
      }

      const paths: Array<Record<string, unknown>> = [];
      const visited = new Set<string>([fromId]);
      let expansions = 0;
      const maxExpansions = 50000;
      const dfs = (
        currentId: string,
        depth: number,
        nodesPath: string[],
        edgesPath: Array<Record<string, unknown>>,
        cumulativeScore: number
      ): void => {
        if (depth >= maxHops || expansions >= maxExpansions) return;
        const neighbors = [...(adjacency.get(currentId) ?? [])]
          .sort((a, b) => b.weight - a.weight)
          .slice(0, 18);
        for (const neighbor of neighbors) {
          if (expansions >= maxExpansions) break;
          if (visited.has(neighbor.id)) continue;
          expansions++;
          visited.add(neighbor.id);
          const nextNodes = [...nodesPath, neighbor.id];
          const nextEdges = [...edgesPath, {
            link_id: neighbor.link_id,
            from_id: currentId,
            to_id: neighbor.id,
            relation_type: neighbor.relation_type,
            label: neighbor.label,
            weight: round3(neighbor.weight),
          }];
          const nextScore = cumulativeScore + neighbor.weight;
          if (neighbor.id === toId) {
            const hops = nextEdges.length;
            const avgScore = hops ? round3(nextScore / hops) : 0;
            paths.push({
              nodes: nextNodes,
              edges: nextEdges,
              hops,
              cumulative_score: round3(nextScore),
              avg_score: avgScore,
            });
          } else {
            dfs(neighbor.id, depth + 1, nextNodes, nextEdges, nextScore);
          }
          visited.delete(neighbor.id);
        }
      };
      dfs(fromId, 0, [fromId], [], 0);

      paths.sort((a, b) => {
        const scoreDelta = toFiniteNumber(b.avg_score, 0) - toFiniteNumber(a.avg_score, 0);
        if (scoreDelta !== 0) return scoreDelta;
        return toFiniteNumber(a.hops, 999) - toFiniteNumber(b.hops, 999);
      });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            from_id: fromId,
            to_id: toId,
            max_hops: maxHops,
            explored_paths: paths.length,
            expansions,
            count: Math.min(paths.length, limit),
            paths: paths.slice(0, limit),
          }, null, 2),
        }],
      };
    }

    case 'memory_conflict_resolve': {
      const { a_id: rawA, b_id: rawB, status: rawStatus, canonical_id: rawCanonical, note: rawNote } = args as {
        a_id: unknown;
        b_id: unknown;
        status: unknown;
        canonical_id?: unknown;
        note?: unknown;
      };
      if (typeof rawA !== 'string' || !rawA.trim()) return { content: [{ type: 'text', text: 'a_id must be a non-empty string.' }] };
      if (typeof rawB !== 'string' || !rawB.trim()) return { content: [{ type: 'text', text: 'b_id must be a non-empty string.' }] };
      if (rawA === rawB) return { content: [{ type: 'text', text: 'a_id and b_id must be different.' }] };
      if (typeof rawStatus !== 'string') return { content: [{ type: 'text', text: 'status is required.' }] };
      const allowed = new Set(['needs_review', 'resolved', 'superseded', 'dismissed']);
      const status = rawStatus.trim();
      if (!allowed.has(status)) return { content: [{ type: 'text', text: 'Invalid status. Use needs_review|resolved|superseded|dismissed.' }] };
      if (rawCanonical !== undefined && typeof rawCanonical !== 'string') return { content: [{ type: 'text', text: 'canonical_id must be a string when provided.' }] };
      if (rawNote !== undefined && typeof rawNote !== 'string') return { content: [{ type: 'text', text: 'note must be a string when provided.' }] };

      const aId = rawA.trim();
      const bId = rawB.trim();
      const aMem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? LIMIT 1').bind(brainId, aId).first<{ id: string }>();
      const bMem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? LIMIT 1').bind(brainId, bId).first<{ id: string }>();
      if (!aMem?.id || !bMem?.id) return { content: [{ type: 'text', text: 'Both conflict memory IDs must exist in this brain.' }] };

      const canonicalId = typeof rawCanonical === 'string' && rawCanonical.trim() ? rawCanonical.trim() : null;
      if (canonicalId && canonicalId !== aId && canonicalId !== bId) {
        return { content: [{ type: 'text', text: 'canonical_id must match either a_id or b_id.' }] };
      }

      const ts = now();
      const key = pairKey(aId, bId);
      await env.DB.prepare(
        `INSERT INTO memory_conflict_resolutions
          (id, brain_id, pair_key, a_id, b_id, status, canonical_id, note, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(brain_id, pair_key)
         DO UPDATE SET status = excluded.status, canonical_id = excluded.canonical_id, note = excluded.note, updated_at = excluded.updated_at`
      ).bind(
        generateId(),
        brainId,
        key,
        aId,
        bId,
        status,
        canonicalId,
        typeof rawNote === 'string' && rawNote.trim() ? rawNote.trim().slice(0, 600) : null,
        ts,
        ts
      ).run();

      if (canonicalId && (status === 'resolved' || status === 'superseded')) {
        const otherId = canonicalId === aId ? bId : aId;
        const existingLink = await env.DB.prepare(
          'SELECT id FROM memory_links WHERE brain_id = ? AND from_id = ? AND to_id = ? LIMIT 1'
        ).bind(brainId, canonicalId, otherId).first<{ id: string }>();
        if (existingLink?.id) {
          await env.DB.prepare(
            'UPDATE memory_links SET relation_type = ?, label = ? WHERE brain_id = ? AND id = ?'
          ).bind('supersedes', 'conflict_resolution', brainId, existingLink.id).run();
        } else {
          await env.DB.prepare(
            'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
          ).bind(generateId(), brainId, canonicalId, otherId, 'supersedes', 'conflict_resolution', ts).run();
        }
      }

      const resolution = await env.DB.prepare(
        'SELECT id, pair_key, a_id, b_id, status, canonical_id, note, created_at, updated_at FROM memory_conflict_resolutions WHERE brain_id = ? AND pair_key = ? LIMIT 1'
      ).bind(brainId, key).first<Record<string, unknown>>();
      await logChangelog(env, brainId, 'memory_conflict_resolved', 'memory_conflict', key, `Conflict marked as ${status}`, {
        a_id: aId,
        b_id: bId,
        status,
        canonical_id: canonicalId,
      });
      return { content: [{ type: 'text', text: JSON.stringify(resolution, null, 2) }] };
    }

    case 'memory_entity_resolve': {
      const { mode: rawMode, canonical_id: rawCanonicalId, alias_id: rawAliasId, alias_ids: rawAliasIds, archive_aliases: rawArchiveAliases, confidence: rawConfidence, note: rawNote, limit: rawLimit } = args as {
        mode?: unknown;
        canonical_id?: unknown;
        alias_id?: unknown;
        alias_ids?: unknown;
        archive_aliases?: unknown;
        confidence?: unknown;
        note?: unknown;
        limit?: unknown;
      };
      if (rawMode !== undefined && typeof rawMode !== 'string') return { content: [{ type: 'text', text: 'mode must be a string when provided.' }] };
      const mode = typeof rawMode === 'string' ? rawMode.trim().toLowerCase() : 'resolve';
      if (!['resolve', 'lookup', 'list'].includes(mode)) return { content: [{ type: 'text', text: 'mode must be resolve|lookup|list.' }] };

      if (mode === 'list') {
        const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 100, 1), 500);
        const rows = await env.DB.prepare(
          `SELECT ea.id, ea.canonical_memory_id, ea.alias_memory_id, ea.note, ea.confidence, ea.created_at, ea.updated_at,
                  c.title AS canonical_title, c.key AS canonical_key, a.title AS alias_title, a.key AS alias_key
           FROM memory_entity_aliases ea
           LEFT JOIN memories c ON c.id = ea.canonical_memory_id
           LEFT JOIN memories a ON a.id = ea.alias_memory_id
           WHERE ea.brain_id = ?
           ORDER BY ea.updated_at DESC
           LIMIT ?`
        ).bind(brainId, limit).all<Record<string, unknown>>();
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({ count: rows.results.length, aliases: rows.results }, null, 2),
          }],
        };
      }

      if (mode === 'lookup') {
        if (typeof rawAliasId !== 'string' || !rawAliasId.trim()) {
          return { content: [{ type: 'text', text: 'alias_id is required for lookup mode.' }] };
        }
        const aliasId = rawAliasId.trim();
        const row = await env.DB.prepare(
          `SELECT ea.id, ea.canonical_memory_id, ea.alias_memory_id, ea.note, ea.confidence, ea.created_at, ea.updated_at,
                  c.title AS canonical_title, c.key AS canonical_key
           FROM memory_entity_aliases ea
           LEFT JOIN memories c ON c.id = ea.canonical_memory_id
           WHERE ea.brain_id = ? AND ea.alias_memory_id = ?
           LIMIT 1`
        ).bind(brainId, aliasId).first<Record<string, unknown>>();
        if (!row) return { content: [{ type: 'text', text: 'No alias mapping found for alias_id.' }] };
        return { content: [{ type: 'text', text: JSON.stringify(row, null, 2) }] };
      }

      if (typeof rawCanonicalId !== 'string' || !rawCanonicalId.trim()) {
        return { content: [{ type: 'text', text: 'canonical_id is required for resolve mode.' }] };
      }
      if (rawAliasId !== undefined && typeof rawAliasId !== 'string') return { content: [{ type: 'text', text: 'alias_id must be a string when provided.' }] };
      if (rawAliasIds !== undefined && (!Array.isArray(rawAliasIds) || rawAliasIds.some((id) => typeof id !== 'string'))) {
        return { content: [{ type: 'text', text: 'alias_ids must be an array of strings when provided.' }] };
      }
      if (rawArchiveAliases !== undefined && typeof rawArchiveAliases !== 'boolean') {
        return { content: [{ type: 'text', text: 'archive_aliases must be a boolean when provided.' }] };
      }
      if (rawNote !== undefined && typeof rawNote !== 'string') return { content: [{ type: 'text', text: 'note must be a string when provided.' }] };

      const canonicalId = rawCanonicalId.trim();
      const canonicalExists = await env.DB.prepare(
        'SELECT id FROM memories WHERE brain_id = ? AND id = ? LIMIT 1'
      ).bind(brainId, canonicalId).first<{ id: string }>();
      if (!canonicalExists?.id) return { content: [{ type: 'text', text: `Canonical memory not found: ${canonicalId}` }] };

      const aliasIds = new Set<string>();
      if (typeof rawAliasId === 'string' && rawAliasId.trim()) aliasIds.add(rawAliasId.trim());
      if (Array.isArray(rawAliasIds)) {
        for (const aliasId of rawAliasIds) {
          const trimmed = aliasId.trim();
          if (trimmed) aliasIds.add(trimmed);
        }
      }
      aliasIds.delete(canonicalId);
      if (!aliasIds.size) return { content: [{ type: 'text', text: 'Provide alias_id or alias_ids for resolve mode.' }] };

      const confidence = clampToRange(rawConfidence, 0.9);
      const archiveAliases = rawArchiveAliases === true;
      const ts = now();
      const mapped: Array<Record<string, unknown>> = [];
      const archivedAliasIds: string[] = [];
      for (const aliasId of aliasIds) {
        const aliasExists = await env.DB.prepare(
          'SELECT id FROM memories WHERE brain_id = ? AND id = ? LIMIT 1'
        ).bind(brainId, aliasId).first<{ id: string }>();
        if (!aliasExists?.id) continue;
        await env.DB.prepare(
          `INSERT INTO memory_entity_aliases
            (id, brain_id, canonical_memory_id, alias_memory_id, note, confidence, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(brain_id, alias_memory_id)
           DO UPDATE SET canonical_memory_id = excluded.canonical_memory_id, note = excluded.note, confidence = excluded.confidence, updated_at = excluded.updated_at`
        ).bind(
          generateId(),
          brainId,
          canonicalId,
          aliasId,
          typeof rawNote === 'string' && rawNote.trim() ? rawNote.trim().slice(0, 600) : null,
          confidence,
          ts,
          ts
        ).run();

        const existingLink = await env.DB.prepare(
          'SELECT id FROM memory_links WHERE brain_id = ? AND from_id = ? AND to_id = ? LIMIT 1'
        ).bind(brainId, canonicalId, aliasId).first<{ id: string }>();
        if (existingLink?.id) {
          await env.DB.prepare(
            'UPDATE memory_links SET relation_type = ?, label = ? WHERE brain_id = ? AND id = ?'
          ).bind('supersedes', 'entity_alias', brainId, existingLink.id).run();
        } else {
          await env.DB.prepare(
            'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
          ).bind(generateId(), brainId, canonicalId, aliasId, 'supersedes', 'entity_alias', ts).run();
        }
        if (archiveAliases) {
          await env.DB.prepare(
            'UPDATE memories SET archived_at = ?, updated_at = ? WHERE brain_id = ? AND id = ? AND archived_at IS NULL'
          ).bind(ts, ts, brainId, aliasId).run();
          archivedAliasIds.push(aliasId);
        }
        mapped.push({ canonical_id: canonicalId, alias_id: aliasId, confidence, archived: archiveAliases });
      }

      if (archivedAliasIds.length) {
        await safeDeleteMemoryVectors(env, brainId, archivedAliasIds, 'memory_entity_resolve_archive_aliases');
      }

      await logChangelog(env, brainId, 'memory_entity_resolved', 'memory_entity', canonicalId, 'Updated entity alias mappings', {
        canonical_id: canonicalId,
        mapped_count: mapped.length,
      });
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            canonical_id: canonicalId,
            mapped_count: mapped.length,
            mappings: mapped,
          }, null, 2),
        }],
      };
    }

    case 'memory_source_trust_set': {
      const { source: rawSource, trust: rawTrust, notes: rawNotes } = args as { source: unknown; trust: unknown; notes?: unknown };
      if (typeof rawSource !== 'string' || !rawSource.trim()) return { content: [{ type: 'text', text: 'source must be a non-empty string.' }] };
      if (rawNotes !== undefined && typeof rawNotes !== 'string') return { content: [{ type: 'text', text: 'notes must be a string when provided.' }] };
      const sourceKey = normalizeSourceKey(rawSource);
      const trust = clampToRange(rawTrust, NaN);
      if (!Number.isFinite(trust)) return { content: [{ type: 'text', text: 'trust must be a number between 0 and 1.' }] };
      const ts = now();
      await env.DB.prepare(
        `INSERT INTO brain_source_trust (id, brain_id, source_key, trust, notes, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(brain_id, source_key)
         DO UPDATE SET trust = excluded.trust, notes = excluded.notes, updated_at = excluded.updated_at`
      ).bind(
        generateId(),
        brainId,
        sourceKey,
        trust,
        typeof rawNotes === 'string' && rawNotes.trim() ? rawNotes.trim().slice(0, 400) : null,
        ts,
        ts
      ).run();
      const row = await env.DB.prepare(
        'SELECT source_key, trust, notes, created_at, updated_at FROM brain_source_trust WHERE brain_id = ? AND source_key = ? LIMIT 1'
      ).bind(brainId, sourceKey).first<Record<string, unknown>>();
      await logChangelog(env, brainId, 'memory_source_trust_set', 'source', sourceKey, 'Updated source trust score', {
        source: sourceKey,
        trust,
      });
      return { content: [{ type: 'text', text: JSON.stringify(row, null, 2) }] };
    }

    case 'memory_source_trust_get': {
      const { source: rawSource, limit: rawLimit } = args as { source?: unknown; limit?: unknown };
      if (rawSource !== undefined && typeof rawSource !== 'string') return { content: [{ type: 'text', text: 'source must be a string when provided.' }] };
      const sourceKey = typeof rawSource === 'string' ? normalizeSourceKey(rawSource) : '';
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 200, 1), 1000);
      if (sourceKey) {
        const row = await env.DB.prepare(
          'SELECT source_key, trust, notes, created_at, updated_at FROM brain_source_trust WHERE brain_id = ? AND source_key = ? LIMIT 1'
        ).bind(brainId, sourceKey).first<Record<string, unknown>>();
        return { content: [{ type: 'text', text: JSON.stringify({ count: row ? 1 : 0, sources: row ? [row] : [] }, null, 2) }] };
      }
      const rows = await env.DB.prepare(
        'SELECT source_key, trust, notes, created_at, updated_at FROM brain_source_trust WHERE brain_id = ? ORDER BY trust DESC, updated_at DESC LIMIT ?'
      ).bind(brainId, limit).all<Record<string, unknown>>();
      return { content: [{ type: 'text', text: JSON.stringify({ count: rows.results.length, sources: rows.results }, null, 2) }] };
    }

    case 'brain_policy_set': {
      const policy = await setBrainPolicy(env, brainId, args);
      await logChangelog(env, brainId, 'brain_policy_set', 'brain_policy', brainId, 'Updated brain policy', policy);
      return { content: [{ type: 'text', text: JSON.stringify({ brain_id: brainId, policy }, null, 2) }] };
    }

    case 'brain_policy_get': {
      const policy = await getBrainPolicy(env, brainId);
      return { content: [{ type: 'text', text: JSON.stringify({ brain_id: brainId, policy }, null, 2) }] };
    }

    case 'brain_snapshot_create': {
      const { label: rawLabel, summary: rawSummary, include_archived: rawIncludeArchived } = args as {
        label?: unknown;
        summary?: unknown;
        include_archived?: unknown;
      };
      if (rawLabel !== undefined && typeof rawLabel !== 'string') return { content: [{ type: 'text', text: 'label must be a string when provided.' }] };
      if (rawSummary !== undefined && typeof rawSummary !== 'string') return { content: [{ type: 'text', text: 'summary must be a string when provided.' }] };
      if (rawIncludeArchived !== undefined && typeof rawIncludeArchived !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_archived must be a boolean when provided.' }] };
      }
      const includeArchived = rawIncludeArchived === true;
      const ts = now();
      const memories = await env.DB.prepare(
        `SELECT id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at
         FROM memories
         WHERE brain_id = ? ${includeArchived ? '' : 'AND archived_at IS NULL'}
         ORDER BY created_at DESC
         LIMIT 5000`
      ).bind(brainId).all<Record<string, unknown>>();
      const memoryIds = new Set(memories.results.map((m) => String(m.id ?? '')).filter(Boolean));
      const links = (await loadExplicitMemoryLinks(env, brainId, 12000))
        .filter((link) => memoryIds.has(link.from_id) && memoryIds.has(link.to_id));
      const sourceTrustRows = await env.DB.prepare(
        'SELECT source_key, trust, notes, created_at, updated_at FROM brain_source_trust WHERE brain_id = ? ORDER BY source_key ASC'
      ).bind(brainId).all<Record<string, unknown>>();
      const aliasRows = await env.DB.prepare(
        'SELECT canonical_memory_id, alias_memory_id, note, confidence, created_at, updated_at FROM memory_entity_aliases WHERE brain_id = ? ORDER BY updated_at DESC LIMIT 5000'
      ).bind(brainId).all<Record<string, unknown>>();
      const conflictResolutionRows = await env.DB.prepare(
        'SELECT pair_key, a_id, b_id, status, canonical_id, note, created_at, updated_at FROM memory_conflict_resolutions WHERE brain_id = ? ORDER BY updated_at DESC LIMIT 5000'
      ).bind(brainId).all<Record<string, unknown>>();
      const policy = await getBrainPolicy(env, brainId);
      const payload = {
        schema: 'brain_snapshot_v1',
        brain_id: brainId,
        exported_at: ts,
        include_archived: includeArchived,
        memories: memories.results,
        links,
        source_trust: sourceTrustRows.results,
        aliases: aliasRows.results,
        conflict_resolutions: conflictResolutionRows.results,
        policy,
      };
      const snapshotId = generateId();
      await env.DB.prepare(
        `INSERT INTO brain_snapshots (id, brain_id, label, summary, memory_count, link_count, payload_json, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
      ).bind(
        snapshotId,
        brainId,
        typeof rawLabel === 'string' && rawLabel.trim() ? rawLabel.trim().slice(0, 160) : null,
        typeof rawSummary === 'string' && rawSummary.trim() ? rawSummary.trim().slice(0, 500) : null,
        memories.results.length,
        links.length,
        stableJson(payload),
        ts
      ).run();

      const retention = policy.snapshot_retention;
      const snapshotRows = await env.DB.prepare(
        'SELECT id FROM brain_snapshots WHERE brain_id = ? ORDER BY created_at DESC LIMIT 2000'
      ).bind(brainId).all<{ id: string }>();
      const staleIds = snapshotRows.results.slice(retention).map((row) => row.id);
      for (const staleId of staleIds) {
        await env.DB.prepare('DELETE FROM brain_snapshots WHERE brain_id = ? AND id = ?').bind(brainId, staleId).run();
      }

      await logChangelog(env, brainId, 'brain_snapshot_created', 'brain_snapshot', snapshotId, 'Created brain snapshot', {
        memory_count: memories.results.length,
        link_count: links.length,
      });
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            snapshot_id: snapshotId,
            memory_count: memories.results.length,
            link_count: links.length,
            retention_applied: retention,
            pruned_snapshots: staleIds.length,
          }, null, 2),
        }],
      };
    }

    case 'brain_snapshot_list': {
      const { limit: rawLimit } = args as { limit?: unknown };
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 50, 1), 500);
      const rows = await env.DB.prepare(
        `SELECT id, label, summary, memory_count, link_count, created_at
         FROM brain_snapshots
         WHERE brain_id = ?
         ORDER BY created_at DESC
         LIMIT ?`
      ).bind(brainId, limit).all<Record<string, unknown>>();
      return { content: [{ type: 'text', text: JSON.stringify({ count: rows.results.length, snapshots: rows.results }, null, 2) }] };
    }

    case 'brain_snapshot_restore': {
      const { snapshot_id: rawSnapshotId, mode: rawMode, restore_policy: rawRestorePolicy, restore_source_trust: rawRestoreTrust } = args as {
        snapshot_id: unknown;
        mode?: unknown;
        restore_policy?: unknown;
        restore_source_trust?: unknown;
      };
      if (typeof rawSnapshotId !== 'string' || !rawSnapshotId.trim()) return { content: [{ type: 'text', text: 'snapshot_id must be a non-empty string.' }] };
      if (rawMode !== undefined && typeof rawMode !== 'string') return { content: [{ type: 'text', text: 'mode must be replace or merge.' }] };
      if (rawRestorePolicy !== undefined && typeof rawRestorePolicy !== 'boolean') return { content: [{ type: 'text', text: 'restore_policy must be a boolean when provided.' }] };
      if (rawRestoreTrust !== undefined && typeof rawRestoreTrust !== 'boolean') return { content: [{ type: 'text', text: 'restore_source_trust must be a boolean when provided.' }] };
      const mode = rawMode === 'replace' ? 'replace' : 'merge';
      const restorePolicy = rawRestorePolicy !== false;
      const restoreTrust = rawRestoreTrust !== false;

      const snapshot = await env.DB.prepare(
        'SELECT id, payload_json, created_at FROM brain_snapshots WHERE brain_id = ? AND id = ? LIMIT 1'
      ).bind(brainId, rawSnapshotId.trim()).first<{ id: string; payload_json: string; created_at: number }>();
      if (!snapshot?.id) return { content: [{ type: 'text', text: 'Snapshot not found.' }] };
      const payload = parseJsonObject(snapshot.payload_json);
      if (!payload) return { content: [{ type: 'text', text: 'Snapshot payload is invalid JSON.' }] };
      const memoriesPayload = Array.isArray(payload.memories) ? payload.memories : [];
      const linksPayload = Array.isArray(payload.links) ? payload.links : [];
      const sourceTrustPayload = Array.isArray(payload.source_trust) ? payload.source_trust : [];
      const aliasesPayload = Array.isArray(payload.aliases) ? payload.aliases : [];
      const resolutionsPayload = Array.isArray(payload.conflict_resolutions) ? payload.conflict_resolutions : [];
      const policyPayload = payload.policy && typeof payload.policy === 'object' && !Array.isArray(payload.policy)
        ? payload.policy as Record<string, unknown>
        : null;
      const ts = now();
      const restoredMemoryRowsForVectorSync: Array<Record<string, unknown>> = [];

      if (mode === 'replace') {
        const existingMemoryIdsBeforeReplace = await env.DB.prepare(
          'SELECT id FROM memories WHERE brain_id = ? LIMIT 50000'
        ).bind(brainId).all<{ id: string }>();
        await safeDeleteMemoryVectors(
          env,
          brainId,
          existingMemoryIdsBeforeReplace.results.map((row) => row.id),
          'brain_snapshot_restore_replace_purge'
        );
        await env.DB.prepare('DELETE FROM memory_links WHERE brain_id = ?').bind(brainId).run();
        await env.DB.prepare('DELETE FROM memory_entity_aliases WHERE brain_id = ?').bind(brainId).run();
        await env.DB.prepare('DELETE FROM memory_conflict_resolutions WHERE brain_id = ?').bind(brainId).run();
        await env.DB.prepare('DELETE FROM memories WHERE brain_id = ?').bind(brainId).run();
        if (restoreTrust) {
          await env.DB.prepare('DELETE FROM brain_source_trust WHERE brain_id = ?').bind(brainId).run();
        }
      }

      let memoryCount = 0;
      for (const rawMemory of memoriesPayload) {
        if (!rawMemory || typeof rawMemory !== 'object' || Array.isArray(rawMemory)) continue;
        const memory = rawMemory as Record<string, unknown>;
        const memoryId = typeof memory.id === 'string' && memory.id ? memory.id : generateId();
        const type = isValidType(memory.type) ? memory.type : 'note';
        const archivedAt = memory.archived_at === null || memory.archived_at === undefined
          ? null
          : Math.floor(toFiniteNumber(memory.archived_at, ts));
        const createdAt = Math.floor(toFiniteNumber(memory.created_at, ts));
        const updatedAt = Math.floor(toFiniteNumber(memory.updated_at, ts));
        const content = typeof memory.content === 'string' && memory.content.trim() ? memory.content.trim() : '';
        await env.DB.prepare(
          `INSERT INTO memories
            (id, brain_id, type, title, key, content, tags, source, confidence, importance, archived_at, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
             brain_id = excluded.brain_id,
             type = excluded.type,
             title = excluded.title,
             key = excluded.key,
             content = excluded.content,
             tags = excluded.tags,
             source = excluded.source,
             confidence = excluded.confidence,
             importance = excluded.importance,
             archived_at = excluded.archived_at,
             created_at = excluded.created_at,
             updated_at = excluded.updated_at`
        ).bind(
          memoryId,
          brainId,
          type,
          typeof memory.title === 'string' ? memory.title : null,
          typeof memory.key === 'string' ? memory.key : null,
          content,
          typeof memory.tags === 'string' ? memory.tags : null,
          typeof memory.source === 'string' ? memory.source : null,
          clampToRange(memory.confidence, 0.7),
          clampToRange(memory.importance, 0.5),
          archivedAt,
          createdAt,
          updatedAt
        ).run();
        restoredMemoryRowsForVectorSync.push({
          id: memoryId,
          type,
          title: typeof memory.title === 'string' ? memory.title : null,
          key: typeof memory.key === 'string' ? memory.key : null,
          content,
          tags: typeof memory.tags === 'string' ? memory.tags : null,
          source: typeof memory.source === 'string' ? memory.source : null,
          confidence: clampToRange(memory.confidence, 0.7),
          importance: clampToRange(memory.importance, 0.5),
          archived_at: archivedAt,
          created_at: createdAt,
          updated_at: updatedAt,
        });
        memoryCount++;
      }

      if (restoredMemoryRowsForVectorSync.length) {
        await safeSyncMemoriesToVectorIndex(env, brainId, restoredMemoryRowsForVectorSync, 'brain_snapshot_restore');
      }

      const existingMemoryRows = await env.DB.prepare(
        'SELECT id FROM memories WHERE brain_id = ? LIMIT 10000'
      ).bind(brainId).all<{ id: string }>();
      const existingMemoryIds = new Set(existingMemoryRows.results.map((row) => row.id));

      let linkCount = 0;
      for (const rawLink of linksPayload) {
        if (!rawLink || typeof rawLink !== 'object' || Array.isArray(rawLink)) continue;
        const link = rawLink as Record<string, unknown>;
        const fromId = typeof link.from_id === 'string' ? link.from_id : '';
        const toId = typeof link.to_id === 'string' ? link.to_id : '';
        if (!fromId || !toId || !existingMemoryIds.has(fromId) || !existingMemoryIds.has(toId)) continue;
        const linkId = typeof link.id === 'string' && link.id ? link.id : generateId();
        const relationType = normalizeRelation(link.relation_type);
        await env.DB.prepare(
          `INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
             brain_id = excluded.brain_id,
             from_id = excluded.from_id,
             to_id = excluded.to_id,
             relation_type = excluded.relation_type,
             label = excluded.label`
        ).bind(
          linkId,
          brainId,
          fromId,
          toId,
          relationType,
          typeof link.label === 'string' ? link.label : null,
          Math.floor(toFiniteNumber(link.created_at, ts))
        ).run();
        linkCount++;
      }

      let sourceTrustCount = 0;
      if (restoreTrust) {
        for (const rawTrust of sourceTrustPayload) {
          if (!rawTrust || typeof rawTrust !== 'object' || Array.isArray(rawTrust)) continue;
          const trustRow = rawTrust as Record<string, unknown>;
          const sourceKey = typeof trustRow.source_key === 'string' ? normalizeSourceKey(trustRow.source_key) : '';
          if (!sourceKey) continue;
          await env.DB.prepare(
            `INSERT INTO brain_source_trust (id, brain_id, source_key, trust, notes, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(brain_id, source_key) DO UPDATE SET trust = excluded.trust, notes = excluded.notes, updated_at = excluded.updated_at`
          ).bind(
            generateId(),
            brainId,
            sourceKey,
            clampToRange(trustRow.trust, 0.5),
            typeof trustRow.notes === 'string' ? trustRow.notes : null,
            Math.floor(toFiniteNumber(trustRow.created_at, ts)),
            ts
          ).run();
          sourceTrustCount++;
        }
      }

      let aliasCount = 0;
      for (const rawAlias of aliasesPayload) {
        if (!rawAlias || typeof rawAlias !== 'object' || Array.isArray(rawAlias)) continue;
        const alias = rawAlias as Record<string, unknown>;
        const canonicalId = typeof alias.canonical_memory_id === 'string' ? alias.canonical_memory_id : '';
        const aliasId = typeof alias.alias_memory_id === 'string' ? alias.alias_memory_id : '';
        if (!canonicalId || !aliasId || !existingMemoryIds.has(canonicalId) || !existingMemoryIds.has(aliasId)) continue;
        await env.DB.prepare(
          `INSERT INTO memory_entity_aliases
            (id, brain_id, canonical_memory_id, alias_memory_id, note, confidence, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(brain_id, alias_memory_id)
           DO UPDATE SET canonical_memory_id = excluded.canonical_memory_id, note = excluded.note, confidence = excluded.confidence, updated_at = excluded.updated_at`
        ).bind(
          generateId(),
          brainId,
          canonicalId,
          aliasId,
          typeof alias.note === 'string' ? alias.note : null,
          clampToRange(alias.confidence, 0.9),
          Math.floor(toFiniteNumber(alias.created_at, ts)),
          ts
        ).run();
        aliasCount++;
      }

      let resolutionCount = 0;
      for (const rawResolution of resolutionsPayload) {
        if (!rawResolution || typeof rawResolution !== 'object' || Array.isArray(rawResolution)) continue;
        const resolution = rawResolution as Record<string, unknown>;
        const aId = typeof resolution.a_id === 'string' ? resolution.a_id : '';
        const bId = typeof resolution.b_id === 'string' ? resolution.b_id : '';
        if (!aId || !bId || !existingMemoryIds.has(aId) || !existingMemoryIds.has(bId)) continue;
        const status = typeof resolution.status === 'string' ? resolution.status : 'needs_review';
        const resolvedKey = pairKey(aId, bId);
        await env.DB.prepare(
          `INSERT INTO memory_conflict_resolutions
            (id, brain_id, pair_key, a_id, b_id, status, canonical_id, note, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(brain_id, pair_key)
           DO UPDATE SET status = excluded.status, canonical_id = excluded.canonical_id, note = excluded.note, updated_at = excluded.updated_at`
        ).bind(
          generateId(),
          brainId,
          resolvedKey,
          aId,
          bId,
          status,
          typeof resolution.canonical_id === 'string' ? resolution.canonical_id : null,
          typeof resolution.note === 'string' ? resolution.note : null,
          Math.floor(toFiniteNumber(resolution.created_at, ts)),
          ts
        ).run();
        resolutionCount++;
      }

      if (restorePolicy && policyPayload) {
        await setBrainPolicy(env, brainId, policyPayload);
      }

      await logChangelog(env, brainId, 'brain_snapshot_restored', 'brain_snapshot', snapshot.id, `Restored brain snapshot (${mode})`, {
        mode,
        memory_count: memoryCount,
        link_count: linkCount,
        source_trust_count: sourceTrustCount,
      });
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            snapshot_id: snapshot.id,
            mode,
            restored: {
              memories: memoryCount,
              links: linkCount,
              source_trust: sourceTrustCount,
              aliases: aliasCount,
              conflict_resolutions: resolutionCount,
            },
            restore_policy: restorePolicy,
          }, null, 2),
        }],
      };
    }

    case 'memory_graph_stats': {
      const { include_inferred: rawIncludeInferred, top_hubs: rawTopHubs, top_tags: rawTopTags } = args as {
        include_inferred?: unknown;
        top_hubs?: unknown;
        top_tags?: unknown;
      };
      if (rawIncludeInferred !== undefined && typeof rawIncludeInferred !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_inferred must be a boolean when provided.' }] };
      }
      const includeInferred = rawIncludeInferred !== false;
      const topHubs = Math.min(Math.max(Number.isInteger(rawTopHubs) ? (rawTopHubs as number) : 12, 1), 50);
      const topTags = Math.min(Math.max(Number.isInteger(rawTopTags) ? (rawTopTags as number) : 12, 1), 50);
      const nodes = await loadActiveMemoryNodes(env, brainId, 2200);
      const explicitLinks = await loadExplicitMemoryLinks(env, brainId, 16000);
      const explicitPairs = new Set(explicitLinks.map((link) => pairKey(link.from_id, link.to_id)));
      const policy = await getBrainPolicy(env, brainId);
      const inferredLinks = includeInferred
        ? buildTagInferredLinks(nodes, Math.min(policy.max_inferred_edges, 3000))
          .filter((link) => !explicitPairs.has(pairKey(link.from_id, link.to_id)))
        : [];
      const allLinks = [...explicitLinks, ...inferredLinks];

      const adjacency = new Map<string, string[]>();
      const degreeById = new Map<string, number>();
      const relationCounts: Record<string, number> = {
        related: 0, supports: 0, contradicts: 0, supersedes: 0, causes: 0, example_of: 0, inferred: 0,
      };
      const perNodeRelation = new Map<string, Record<string, number>>();

      for (const node of nodes) {
        if (!adjacency.has(node.id)) adjacency.set(node.id, []);
      }
      for (const link of allLinks) {
        if (!adjacency.has(link.from_id)) adjacency.set(link.from_id, []);
        if (!adjacency.has(link.to_id)) adjacency.set(link.to_id, []);
        adjacency.get(link.from_id)?.push(link.to_id);
        adjacency.get(link.to_id)?.push(link.from_id);
        degreeById.set(link.from_id, (degreeById.get(link.from_id) ?? 0) + 1);
        degreeById.set(link.to_id, (degreeById.get(link.to_id) ?? 0) + 1);
        const relationKey = link.inferred ? 'inferred' : normalizeRelation(link.relation_type);
        relationCounts[relationKey] = (relationCounts[relationKey] ?? 0) + 1;

        const fromStats = perNodeRelation.get(link.from_id) ?? {
          related: 0, supports: 0, contradicts: 0, supersedes: 0, causes: 0, example_of: 0, inferred: 0,
        };
        fromStats[relationKey] = (fromStats[relationKey] ?? 0) + 1;
        perNodeRelation.set(link.from_id, fromStats);
        const toStats = perNodeRelation.get(link.to_id) ?? {
          related: 0, supports: 0, contradicts: 0, supersedes: 0, causes: 0, example_of: 0, inferred: 0,
        };
        toStats[relationKey] = (toStats[relationKey] ?? 0) + 1;
        perNodeRelation.set(link.to_id, toStats);
      }

      let connectedComponents = 0;
      let isolatedNodes = 0;
      const componentSizes: number[] = [];
      const visited = new Set<string>();
      for (const node of nodes) {
        const seedId = node.id;
        if (visited.has(seedId)) continue;
        connectedComponents++;
        let size = 0;
        const queue = [seedId];
        visited.add(seedId);
        while (queue.length) {
          const current = queue.shift();
          if (!current) break;
          size++;
          const neighbors = adjacency.get(current) ?? [];
          for (const neighbor of neighbors) {
            if (visited.has(neighbor)) continue;
            visited.add(neighbor);
            queue.push(neighbor);
          }
        }
        componentSizes.push(size);
        if (size === 1 && (degreeById.get(seedId) ?? 0) === 0) isolatedNodes++;
      }
      componentSizes.sort((a, b) => b - a);

      const projectedNodes = await enrichAndProjectRows(
        env,
        brainId,
        nodes as unknown as Array<Record<string, unknown>>
      );
      const projectedById = new Map(projectedNodes.map((node) => [String(node.id), node]));
      const topHubIds = nodes
        .map((node) => node.id)
        .sort((a, b) => {
          const byDegree = (degreeById.get(b) ?? 0) - (degreeById.get(a) ?? 0);
          if (byDegree !== 0) return byDegree;
          return a.localeCompare(b);
        })
        .slice(0, topHubs);
      const hubs = topHubIds.map((id) => ({
        id,
        degree: degreeById.get(id) ?? 0,
        relations: perNodeRelation.get(id) ?? {},
        memory: projectedById.get(id) ?? null,
      }));

      const tagCounts = new Map<string, number>();
      for (const node of nodes) {
        for (const tag of parseTagSet(node.tags)) {
          tagCounts.set(tag, (tagCounts.get(tag) ?? 0) + 1);
        }
      }
      const topTagRows = Array.from(tagCounts.entries())
        .sort((a, b) => {
          if (b[1] !== a[1]) return b[1] - a[1];
          return a[0].localeCompare(b[0]);
        })
        .slice(0, topTags)
        .map(([tag, count]) => ({ tag, count }));

      const avgConfidence = projectedNodes.length
        ? round3(projectedNodes.reduce((sum, node) => sum + toFiniteNumber(node.dynamic_confidence, 0.7), 0) / projectedNodes.length)
        : null;
      const avgImportance = projectedNodes.length
        ? round3(projectedNodes.reduce((sum, node) => sum + toFiniteNumber(node.dynamic_importance, 0.5), 0) / projectedNodes.length)
        : null;
      const density = nodes.length > 1
        ? round3((2 * allLinks.length) / (nodes.length * (nodes.length - 1)))
        : 0;

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            node_count: nodes.length,
            explicit_edge_count: explicitLinks.length,
            inferred_edge_count: inferredLinks.length,
            total_edge_count: allLinks.length,
            connected_components: connectedComponents,
            isolated_nodes: isolatedNodes,
            largest_component_size: componentSizes[0] ?? 0,
            density,
            relation_counts: relationCounts,
            avg_dynamic_confidence: avgConfidence,
            avg_dynamic_importance: avgImportance,
            top_hubs: hubs,
            top_tags: topTagRows,
          }, null, 2),
        }],
      };
    }

    case 'memory_neighbors': {
      const { id: rawId, query: rawQuery, max_hops: rawMaxHops, limit_nodes: rawLimitNodes, relation_type: rawRelationType, include_inferred: rawIncludeInferred } = args as {
        id?: unknown;
        query?: unknown;
        max_hops?: unknown;
        limit_nodes?: unknown;
        relation_type?: unknown;
        include_inferred?: unknown;
      };
      if (rawId !== undefined && typeof rawId !== 'string') return { content: [{ type: 'text', text: 'id must be a string when provided.' }] };
      if (rawQuery !== undefined && typeof rawQuery !== 'string') return { content: [{ type: 'text', text: 'query must be a string when provided.' }] };
      if (rawIncludeInferred !== undefined && typeof rawIncludeInferred !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_inferred must be a boolean when provided.' }] };
      }
      if (rawRelationType !== undefined && !isValidRelationType(rawRelationType)) {
        return { content: [{ type: 'text', text: 'relation_type must be one of related|supports|contradicts|supersedes|causes|example_of.' }] };
      }
      const relationFilter = isValidRelationType(rawRelationType) ? rawRelationType : null;
      const maxHops = Math.min(Math.max(Number.isInteger(rawMaxHops) ? (rawMaxHops as number) : 1, 1), 4);
      const limitNodes = Math.min(Math.max(Number.isInteger(rawLimitNodes) ? (rawLimitNodes as number) : 80, 5), 1000);
      const includeInferred = rawIncludeInferred !== false && (relationFilter === null || relationFilter === 'related');

      const nodes = await loadActiveMemoryNodes(env, brainId, 2200);
      if (!nodes.length) return { content: [{ type: 'text', text: 'No memories available.' }] };
      const nodeById = new Map(nodes.map((node) => [node.id, node]));

      let seedId = '';
      if (typeof rawId === 'string' && rawId.trim() && nodeById.has(rawId.trim())) {
        seedId = rawId.trim();
      }
      if (!seedId && typeof rawQuery === 'string' && rawQuery.trim()) {
        const q = rawQuery.trim().toLowerCase();
        const qTokens = new Set(tokenizeText(q, 24));
        const scored = nodes.map((node) => {
          const blob = `${node.id} ${node.title ?? ''} ${node.key ?? ''} ${node.content} ${node.tags ?? ''} ${node.source ?? ''}`.toLowerCase();
          const direct = blob.includes(q) ? 0.75 : 0;
          const overlap = qTokens.size ? jaccardSimilarity(new Set(tokenizeText(blob, 100)), qTokens) : 0;
          return { id: node.id, score: direct + overlap * 0.25 };
        }).filter((row) => row.score > 0)
          .sort((a, b) => b.score - a.score);
        seedId = scored[0]?.id ?? '';
      }
      if (!seedId) {
        return { content: [{ type: 'text', text: 'Provide id or query to select a seed memory.' }] };
      }

      const explicitLinks = (await loadExplicitMemoryLinks(env, brainId, 16000))
        .filter((link) => nodeById.has(link.from_id) && nodeById.has(link.to_id))
        .filter((link) => !relationFilter || normalizeRelation(link.relation_type) === relationFilter);
      const explicitPairs = new Set(explicitLinks.map((link) => pairKey(link.from_id, link.to_id)));
      const policy = await getBrainPolicy(env, brainId);
      const inferredLinks = includeInferred
        ? buildTagInferredLinks(nodes, Math.min(policy.max_inferred_edges, 1800))
          .filter((link) => !explicitPairs.has(pairKey(link.from_id, link.to_id)))
        : [];

      const adjacency = new Map<string, string[]>();
      for (const edge of [...explicitLinks, ...inferredLinks]) {
        const fromArr = adjacency.get(edge.from_id);
        if (fromArr) fromArr.push(edge.to_id);
        else adjacency.set(edge.from_id, [edge.to_id]);
        const toArr = adjacency.get(edge.to_id);
        if (toArr) toArr.push(edge.from_id);
        else adjacency.set(edge.to_id, [edge.from_id]);
      }

      const depthByNode = new Map<string, number>();
      const queue: string[] = [seedId];
      depthByNode.set(seedId, 0);
      while (queue.length && depthByNode.size < limitNodes) {
        const current = queue.shift();
        if (!current) break;
        const depth = depthByNode.get(current) ?? 0;
        if (depth >= maxHops) continue;
        const neighbors = adjacency.get(current) ?? [];
        for (const neighborId of neighbors) {
          if (depthByNode.has(neighborId)) continue;
          depthByNode.set(neighborId, depth + 1);
          queue.push(neighborId);
          if (depthByNode.size >= limitNodes) break;
        }
      }

      const selectedIds = new Set(depthByNode.keys());
      const selectedNodes = nodes.filter((node) => selectedIds.has(node.id));
      const selectedEdges = explicitLinks.filter((edge) => selectedIds.has(edge.from_id) && selectedIds.has(edge.to_id));
      const selectedInferred = inferredLinks.filter((edge) => selectedIds.has(edge.from_id) && selectedIds.has(edge.to_id));
      const projectedNodes = await enrichAndProjectRows(
        env,
        brainId,
        selectedNodes as unknown as Array<Record<string, unknown>>
      );
      const projectedById = new Map(projectedNodes.map((node) => [String(node.id), node]));
      const depthObject: Record<string, number> = {};
      for (const [nodeId, depth] of depthByNode.entries()) depthObject[nodeId] = depth;

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            seed_id: seedId,
            seed: projectedById.get(seedId) ?? null,
            max_hops: maxHops,
            relation_filter: relationFilter,
            include_inferred: includeInferred,
            node_count: projectedNodes.length,
            edge_count: selectedEdges.length,
            inferred_edge_count: selectedInferred.length,
            depth_by_node: depthObject,
            nodes: projectedNodes,
            edges: selectedEdges,
            inferred_edges: selectedInferred,
          }, null, 2),
        }],
      };
    }

    case 'memory_subgraph': {
      const { seed_id: rawSeedId, query: rawQuery, tag: rawTag, radius: rawRadius, limit_nodes: rawLimitNodes, include_inferred: rawIncludeInferred } = args as {
        seed_id?: unknown;
        query?: unknown;
        tag?: unknown;
        radius?: unknown;
        limit_nodes?: unknown;
        include_inferred?: unknown;
      };
      if (rawSeedId !== undefined && typeof rawSeedId !== 'string') return { content: [{ type: 'text', text: 'seed_id must be a string when provided.' }] };
      if (rawQuery !== undefined && typeof rawQuery !== 'string') return { content: [{ type: 'text', text: 'query must be a string when provided.' }] };
      if (rawTag !== undefined && typeof rawTag !== 'string') return { content: [{ type: 'text', text: 'tag must be a string when provided.' }] };
      if (rawIncludeInferred !== undefined && typeof rawIncludeInferred !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_inferred must be a boolean when provided.' }] };
      }
      const policy = await getBrainPolicy(env, brainId);
      const radius = Math.min(Math.max(Number.isInteger(rawRadius) ? (rawRadius as number) : policy.subgraph_default_radius, 1), 6);
      const limitNodes = Math.min(Math.max(Number.isInteger(rawLimitNodes) ? (rawLimitNodes as number) : 120, 10), 1000);
      const includeInferred = rawIncludeInferred !== false;
      const nodes = await loadActiveMemoryNodes(env, brainId, 1800);
      if (!nodes.length) return { content: [{ type: 'text', text: 'No memories available.' }] };
      const tagFilter = typeof rawTag === 'string' && rawTag.trim() ? normalizeTag(rawTag) : '';
      const nodeById = new Map(nodes.map((node) => [node.id, node]));
      const candidateSeeds = tagFilter
        ? nodes.filter((node) => parseTagSet(node.tags).has(tagFilter))
        : nodes;
      const seedIds = new Set<string>();
      if (typeof rawSeedId === 'string' && rawSeedId.trim() && nodeById.has(rawSeedId.trim())) {
        const seed = rawSeedId.trim();
        if (!tagFilter || parseTagSet(nodeById.get(seed)?.tags).has(tagFilter)) seedIds.add(seed);
      }
      if (typeof rawQuery === 'string' && rawQuery.trim()) {
        const query = rawQuery.trim().toLowerCase();
        const scored = candidateSeeds.map((node) => {
          const text = `${node.id} ${node.title ?? ''} ${node.key ?? ''} ${node.content} ${node.source ?? ''}`.toLowerCase();
          const direct = text.includes(query) ? 1 : 0;
          const overlap = jaccardSimilarity(
            new Set(tokenizeText(text, 100)),
            new Set(tokenizeText(query, 24))
          );
          return { id: node.id, score: direct * 0.7 + overlap * 0.3 };
        }).filter((row) => row.score > 0)
          .sort((a, b) => b.score - a.score)
          .slice(0, 8);
        for (const item of scored) seedIds.add(item.id);
      }
      if (!seedIds.size) {
        for (const node of candidateSeeds.slice(0, 3)) seedIds.add(node.id);
      }
      if (!seedIds.size) return { content: [{ type: 'text', text: 'No seed nodes matched the requested filters.' }] };

      const links = await loadExplicitMemoryLinks(env, brainId, 12000);
      const adjacency = new Map<string, string[]>();
      for (const link of links) {
        if (!nodeById.has(link.from_id) || !nodeById.has(link.to_id)) continue;
        const fromArr = adjacency.get(link.from_id);
        if (fromArr) fromArr.push(link.to_id);
        else adjacency.set(link.from_id, [link.to_id]);
        const toArr = adjacency.get(link.to_id);
        if (toArr) toArr.push(link.from_id);
        else adjacency.set(link.to_id, [link.from_id]);
      }

      const depthByNode = new Map<string, number>();
      const queue: Array<{ id: string; depth: number }> = [];
      for (const seedId of seedIds) {
        depthByNode.set(seedId, 0);
        queue.push({ id: seedId, depth: 0 });
      }
      while (queue.length > 0 && depthByNode.size < limitNodes) {
        const current = queue.shift();
        if (!current) break;
        if (current.depth >= radius) continue;
        const neighbors = adjacency.get(current.id) ?? [];
        for (const neighbor of neighbors) {
          if (depthByNode.has(neighbor)) continue;
          depthByNode.set(neighbor, current.depth + 1);
          queue.push({ id: neighbor, depth: current.depth + 1 });
          if (depthByNode.size >= limitNodes) break;
        }
      }

      const selectedIds = new Set(depthByNode.keys());
      const selectedNodes = nodes.filter((node) => selectedIds.has(node.id));
      const selectedEdges = links.filter((link) => selectedIds.has(link.from_id) && selectedIds.has(link.to_id));
      const explicitPairs = new Set(selectedEdges.map((edge) => pairKey(edge.from_id, edge.to_id)));
      const inferredEdges = includeInferred
        ? buildTagInferredLinks(selectedNodes, Math.min(policy.max_inferred_edges, 1200))
          .filter((edge) => !explicitPairs.has(pairKey(edge.from_id, edge.to_id)))
        : [];

      const projectedNodes = await enrichAndProjectRows(env, brainId, selectedNodes as unknown as Array<Record<string, unknown>>);
      const depthObject: Record<string, number> = {};
      for (const [nodeId, depth] of depthByNode) depthObject[nodeId] = depth;

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            seed_ids: Array.from(seedIds),
            radius,
            node_count: projectedNodes.length,
            edge_count: selectedEdges.length,
            inferred_edge_count: inferredEdges.length,
            depth_by_node: depthObject,
            nodes: projectedNodes,
            edges: selectedEdges,
            inferred_edges: inferredEdges,
          }, null, 2),
        }],
      };
    }

    case 'memory_watch': {
      const { mode: rawMode, id: rawId, name: rawName, event_types: rawEventTypes, query: rawQuery, webhook_url: rawWebhook, secret: rawSecret, active: rawActive, limit: rawLimit } = args as {
        mode?: unknown;
        id?: unknown;
        name?: unknown;
        event_types?: unknown;
        query?: unknown;
        webhook_url?: unknown;
        secret?: unknown;
        active?: unknown;
        limit?: unknown;
      };
      if (rawMode !== undefined && typeof rawMode !== 'string') return { content: [{ type: 'text', text: 'mode must be a string when provided.' }] };
      const mode = typeof rawMode === 'string' ? rawMode.trim().toLowerCase() : 'list';

      if (mode === 'list') {
        const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 100, 1), 500);
        const rows = await env.DB.prepare(
          `SELECT id, name, event_types, query, webhook_url, is_active, created_at, updated_at, last_triggered_at, last_error
           FROM memory_watches
           WHERE brain_id = ?
           ORDER BY updated_at DESC
           LIMIT ?`
        ).bind(brainId, limit).all<Record<string, unknown>>();
        const watches = rows.results.map((row) => ({
          ...row,
          event_types: typeof row.event_types === 'string' ? parseWatchEventTypes(row.event_types) : [],
          is_active: Number(row.is_active ?? 0) === 1,
        }));
        return { content: [{ type: 'text', text: JSON.stringify({ count: watches.length, watches }, null, 2) }] };
      }

      if (mode === 'create') {
        if (typeof rawName !== 'string' || !rawName.trim()) return { content: [{ type: 'text', text: 'name is required for create mode.' }] };
        if (rawQuery !== undefined && typeof rawQuery !== 'string') return { content: [{ type: 'text', text: 'query must be a string when provided.' }] };
        if (rawWebhook !== undefined && typeof rawWebhook !== 'string') return { content: [{ type: 'text', text: 'webhook_url must be a string when provided.' }] };
        if (rawSecret !== undefined && typeof rawSecret !== 'string') return { content: [{ type: 'text', text: 'secret must be a string when provided.' }] };
        const eventTypes = normalizeWatchEventInput(rawEventTypes);
        const finalEventTypes = eventTypes.length ? eventTypes : ['*'];
        const webhookUrl = typeof rawWebhook === 'string' && rawWebhook.trim() ? rawWebhook.trim() : null;
        if (webhookUrl && !(webhookUrl.startsWith('https://') || webhookUrl.startsWith('http://'))) {
          return { content: [{ type: 'text', text: 'webhook_url must start with http:// or https://.' }] };
        }
        const ts = now();
        const watchId = generateId();
        await env.DB.prepare(
          `INSERT INTO memory_watches
            (id, brain_id, name, event_types, query, webhook_url, secret, is_active, created_at, updated_at, last_triggered_at, last_error)
           VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, NULL, NULL)`
        ).bind(
          watchId,
          brainId,
          rawName.trim().slice(0, 120),
          stableJson(finalEventTypes),
          typeof rawQuery === 'string' && rawQuery.trim() ? rawQuery.trim().slice(0, 200) : null,
          webhookUrl,
          typeof rawSecret === 'string' && rawSecret.trim() ? rawSecret.trim().slice(0, 200) : null,
          ts,
          ts
        ).run();
        const row = await env.DB.prepare(
          'SELECT id, name, event_types, query, webhook_url, is_active, created_at, updated_at FROM memory_watches WHERE brain_id = ? AND id = ? LIMIT 1'
        ).bind(brainId, watchId).first<Record<string, unknown>>();
        await logChangelog(env, brainId, 'memory_watch_created', 'memory_watch', watchId, 'Created memory watch', {
          event_types: finalEventTypes,
        });
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              watch: row
                ? {
                    ...row,
                    event_types: typeof row.event_types === 'string' ? parseWatchEventTypes(row.event_types) : [],
                    is_active: Number(row.is_active ?? 0) === 1,
                  }
                : null,
            }, null, 2),
          }],
        };
      }

      if (mode === 'delete') {
        if (typeof rawId !== 'string' || !rawId.trim()) return { content: [{ type: 'text', text: 'id is required for delete mode.' }] };
        const watchId = rawId.trim();
        const result = await env.DB.prepare(
          'DELETE FROM memory_watches WHERE brain_id = ? AND id = ?'
        ).bind(brainId, watchId).run();
        if ((result.meta.changes ?? 0) === 0) return { content: [{ type: 'text', text: 'Watch not found.' }] };
        await logChangelog(env, brainId, 'memory_watch_deleted', 'memory_watch', watchId, 'Deleted memory watch');
        return { content: [{ type: 'text', text: JSON.stringify({ deleted: true, id: watchId }) }] };
      }

      if (mode === 'set_active') {
        if (typeof rawId !== 'string' || !rawId.trim()) return { content: [{ type: 'text', text: 'id is required for set_active mode.' }] };
        if (typeof rawActive !== 'boolean') return { content: [{ type: 'text', text: 'active must be true or false for set_active mode.' }] };
        const watchId = rawId.trim();
        const ts = now();
        const result = await env.DB.prepare(
          'UPDATE memory_watches SET is_active = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
        ).bind(rawActive ? 1 : 0, ts, brainId, watchId).run();
        if ((result.meta.changes ?? 0) === 0) return { content: [{ type: 'text', text: 'Watch not found.' }] };
        await logChangelog(env, brainId, 'memory_watch_updated', 'memory_watch', watchId, 'Updated memory watch activation', {
          active: rawActive,
        });
        return { content: [{ type: 'text', text: JSON.stringify({ id: watchId, active: rawActive }) }] };
      }

      if (mode === 'test') {
        if (typeof rawId !== 'string' || !rawId.trim()) return { content: [{ type: 'text', text: 'id is required for test mode.' }] };
        const watchId = rawId.trim();
        const watch = await env.DB.prepare(
          'SELECT id, webhook_url, secret, is_active FROM memory_watches WHERE brain_id = ? AND id = ? LIMIT 1'
        ).bind(brainId, watchId).first<{ id: string; webhook_url: string | null; secret: string | null; is_active: number }>();
        if (!watch?.id) return { content: [{ type: 'text', text: 'Watch not found.' }] };
        const webhook = typeof watch.webhook_url === 'string' ? watch.webhook_url.trim() : '';
        const ts = now();
        if (!webhook) {
          await env.DB.prepare(
            'UPDATE memory_watches SET last_triggered_at = ?, last_error = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
          ).bind(ts, 'test_no_webhook', ts, brainId, watchId).run();
          return { content: [{ type: 'text', text: JSON.stringify({ id: watchId, tested: true, delivered: false, reason: 'No webhook_url configured.' }) }] };
        }
        if (!(webhook.startsWith('https://') || webhook.startsWith('http://'))) {
          return { content: [{ type: 'text', text: 'Configured webhook_url is invalid. It must start with http:// or https://.' }] };
        }
        try {
          const headers: Record<string, string> = {
            'Content-Type': 'application/json',
            'X-MemoryVault-Watch-Id': watchId,
          };
          if (watch.secret) headers['X-MemoryVault-Watch-Secret'] = watch.secret;
          const response = await fetch(webhook, {
            method: 'POST',
            headers,
            body: JSON.stringify({
              watch_id: watchId,
              event_type: 'watch_test',
              entity_type: 'memory_watch',
              entity_id: watchId,
              summary: 'Manual watch test',
              payload: { mode: 'test' },
              created_at: ts,
            }),
          });
          await env.DB.prepare(
            'UPDATE memory_watches SET last_triggered_at = ?, last_error = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
          ).bind(ts, response.ok ? null : `webhook_status_${response.status}`, ts, brainId, watchId).run();
          return { content: [{ type: 'text', text: JSON.stringify({ id: watchId, tested: true, delivered: response.ok, status: response.status }) }] };
        } catch (err) {
          const message = err instanceof Error ? err.message.slice(0, 280) : 'webhook_error';
          await env.DB.prepare(
            'UPDATE memory_watches SET last_triggered_at = ?, last_error = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
          ).bind(ts, message, ts, brainId, watchId).run();
          return { content: [{ type: 'text', text: JSON.stringify({ id: watchId, tested: true, delivered: false, error: message }) }] };
        }
      }

      return { content: [{ type: 'text', text: 'Invalid mode. Use create|list|delete|set_active|test.' }] };
    }

    case 'memory_merge': {
      const { memory_ids, primary_id, merged_content, merged_title } = args as {
        memory_ids?: unknown;
        primary_id?: unknown;
        merged_content?: unknown;
        merged_title?: unknown;
      };
      if (!Array.isArray(memory_ids) || memory_ids.length < 2) {
        return { content: [{ type: 'text', text: 'memory_ids must be an array of at least 2 memory IDs.' }] };
      }
      const ids = memory_ids.map(String).filter(Boolean);
      if (ids.length < 2) return { content: [{ type: 'text', text: 'Need at least 2 valid memory IDs.' }] };

      // Load all memories
      const placeholders = ids.map(() => '?').join(',');
      const rows = await env.DB.prepare(
        `SELECT id, type, title, key, content, tags, source, confidence, importance, created_at, updated_at
         FROM memories WHERE brain_id = ? AND id IN (${placeholders}) AND archived_at IS NULL`
      ).bind(brainId, ...ids).all<Record<string, unknown>>();

      if (rows.results.length < 2) {
        return { content: [{ type: 'text', text: `Found only ${rows.results.length} active memories from the provided IDs.` }] };
      }

      // Select primary: explicit or highest importance then newest
      let primary: Record<string, unknown>;
      if (typeof primary_id === 'string' && primary_id) {
        const found = rows.results.find((r) => r.id === primary_id);
        if (!found) return { content: [{ type: 'text', text: `primary_id "${primary_id}" not found among provided memories.` }] };
        primary = found;
      } else {
        const sorted = [...rows.results].sort((a, b) => {
          const impA = clampToRange(a.importance, 0.5);
          const impB = clampToRange(b.importance, 0.5);
          if (impB !== impA) return impB - impA;
          return Number(b.created_at ?? 0) - Number(a.created_at ?? 0);
        });
        primary = sorted[0];
      }
      const primaryId = String(primary.id);
      const others = rows.results.filter((r) => r.id !== primaryId);

      // Merge content
      const finalContent = typeof merged_content === 'string' && merged_content.trim()
        ? merged_content.trim()
        : [primary, ...others].map((r) => String(r.content ?? '')).filter(Boolean).join('\n\n---\n\n');

      // Merge tags
      const allTags = new Set<string>();
      for (const r of rows.results) {
        if (typeof r.tags === 'string') {
          r.tags.split(',').map((t: string) => t.trim()).filter(Boolean).forEach((t: string) => allTags.add(t));
        }
      }
      const mergedTags = allTags.size > 0 ? Array.from(allTags).join(',') : null;

      // Use the highest importance and confidence from all sources
      const maxImportance = Math.max(...rows.results.map((r) => clampToRange(r.importance, 0.5)));
      const maxConfidence = Math.max(...rows.results.map((r) => clampToRange(r.confidence, 0.7)));

      // Title
      const finalTitle = typeof merged_title === 'string' && merged_title.trim()
        ? merged_title.trim()
        : typeof primary.title === 'string' ? primary.title : null;

      const ts = now();

      // Update primary memory with merged data
      await env.DB.prepare(
        `UPDATE memories SET content = ?, tags = ?, title = ?, confidence = ?, importance = ?, updated_at = ?
         WHERE brain_id = ? AND id = ?`
      ).bind(finalContent, mergedTags, finalTitle, maxConfidence, maxImportance, ts, brainId, primaryId).run();

      // Archive others and create supersedes links
      const archivedIds: string[] = [];
      for (const other of others) {
        const otherId = String(other.id);
        await env.DB.prepare(
          'UPDATE memories SET archived_at = ?, updated_at = ? WHERE brain_id = ? AND id = ?'
        ).bind(ts, ts, brainId, otherId).run();
        archivedIds.push(otherId);

        // Create supersedes link
        const existingLink = await env.DB.prepare(
          'SELECT id FROM memory_links WHERE brain_id = ? AND ((from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?))'
        ).bind(brainId, primaryId, otherId, otherId, primaryId).first<{ id: string }>();
        if (existingLink?.id) {
          await env.DB.prepare(
            'UPDATE memory_links SET relation_type = ?, label = ? WHERE brain_id = ? AND id = ?'
          ).bind('supersedes', 'merged', brainId, existingLink.id).run();
        } else {
          await env.DB.prepare(
            'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
          ).bind(generateId(), brainId, primaryId, otherId, 'supersedes', 'merged', ts).run();
        }

        // Transfer links from archived memories to primary
        const incomingLinks = await env.DB.prepare(
          'SELECT id, from_id, relation_type, label FROM memory_links WHERE brain_id = ? AND to_id = ? AND from_id != ?'
        ).bind(brainId, otherId, primaryId).all<Record<string, unknown>>();
        for (const link of incomingLinks.results) {
          const fromId = String(link.from_id);
          const exists = await env.DB.prepare(
            'SELECT id FROM memory_links WHERE brain_id = ? AND from_id = ? AND to_id = ? AND relation_type = ?'
          ).bind(brainId, fromId, primaryId, link.relation_type).first<{ id: string }>();
          if (!exists) {
            await env.DB.prepare(
              'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
            ).bind(generateId(), brainId, fromId, primaryId, link.relation_type, link.label ?? null, ts).run();
          }
        }

        const outgoingLinks = await env.DB.prepare(
          'SELECT id, to_id, relation_type, label FROM memory_links WHERE brain_id = ? AND from_id = ? AND to_id != ?'
        ).bind(brainId, otherId, primaryId).all<Record<string, unknown>>();
        for (const link of outgoingLinks.results) {
          const toId = String(link.to_id);
          const exists = await env.DB.prepare(
            'SELECT id FROM memory_links WHERE brain_id = ? AND from_id = ? AND to_id = ? AND relation_type = ?'
          ).bind(brainId, primaryId, toId, link.relation_type).first<{ id: string }>();
          if (!exists) {
            await env.DB.prepare(
              'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
            ).bind(generateId(), brainId, primaryId, toId, link.relation_type, link.label ?? null, ts).run();
          }
        }
      }

      // Sync vectors
      await safeDeleteMemoryVectors(env, brainId, archivedIds, 'memory_merge');
      const updatedRow = await env.DB.prepare(
        'SELECT * FROM memories WHERE brain_id = ? AND id = ?'
      ).bind(brainId, primaryId).first<Record<string, unknown>>();
      if (updatedRow) {
        await safeSyncMemoriesToVectorIndex(env, brainId, [updatedRow], 'memory_merge');
      }

      await logChangelog(env, brainId, 'memory_merged', 'memory', primaryId, 'Merged memories', {
        primary_id: primaryId,
        archived_ids: archivedIds,
        source_count: rows.results.length,
      });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            primary_id: primaryId,
            merged_count: rows.results.length,
            archived_ids: archivedIds,
            tags: mergedTags,
            confidence: maxConfidence,
            importance: maxImportance,
          }, null, 2),
        }],
      };
    }

    case 'memory_temporal_cluster': {
      const { start, end, window: windowArg, type, tag, include_links, limit_per_window } = args as {
        start?: unknown;
        end?: unknown;
        window?: unknown;
        type?: unknown;
        tag?: unknown;
        include_links?: unknown;
        limit_per_window?: unknown;
      };
      if (type !== undefined && !isValidType(type)) return { content: [{ type: 'text', text: 'Invalid type filter.' }] };

      const tsNow = now();
      const endTs = Number.isFinite(Number(end)) ? Math.floor(Number(end)) : tsNow;
      const startTs = Number.isFinite(Number(start)) ? Math.floor(Number(start)) : endTs - 7 * 86400;
      if (startTs >= endTs) return { content: [{ type: 'text', text: 'start must be before end.' }] };

      const windowSize = windowArg === 'hour' ? 3600 : windowArg === 'week' ? 604800 : 86400;
      const windowName = windowArg === 'hour' ? 'hour' : windowArg === 'week' ? 'week' : 'day';
      const perWindow = Math.min(Math.max(Number.isFinite(Number(limit_per_window)) ? Math.floor(Number(limit_per_window)) : 50, 1), 200);
      const wantLinks = include_links !== false;

      // Query memories in the time range
      const params: unknown[] = [brainId, startTs, endTs];
      let query = 'SELECT id, type, title, key, content, tags, source, confidence, importance, created_at, updated_at FROM memories WHERE brain_id = ? AND archived_at IS NULL AND created_at >= ? AND created_at <= ?';
      if (type) {
        query += ' AND type = ?';
        params.push(type);
      }
      if (typeof tag === 'string' && tag.trim()) {
        query += ' AND tags LIKE ?';
        params.push(`%${tag.trim()}%`);
      }
      query += ' ORDER BY created_at ASC';

      const allRows = await env.DB.prepare(query).bind(...params).all<Record<string, unknown>>();

      // Group into time windows
      const clusters = new Map<number, { window_start: number; window_end: number; label: string; memories: Record<string, unknown>[] }>();

      for (const row of allRows.results) {
        const createdAt = Number(row.created_at ?? 0);
        const windowStart = startTs + Math.floor((createdAt - startTs) / windowSize) * windowSize;
        const windowEnd = windowStart + windowSize;

        let cluster = clusters.get(windowStart);
        if (!cluster) {
          const date = new Date(windowStart * 1000);
          let label: string;
          if (windowName === 'hour') {
            label = `${date.toISOString().slice(0, 13)}:00Z`;
          } else if (windowName === 'week') {
            label = `week of ${date.toISOString().slice(0, 10)}`;
          } else {
            label = date.toISOString().slice(0, 10);
          }
          cluster = { window_start: windowStart, window_end: windowEnd, label, memories: [] };
          clusters.set(windowStart, cluster);
        }
        if (cluster.memories.length < perWindow) {
          cluster.memories.push(row);
        }
      }

      // Optionally load links between memories in each cluster
      const result: Array<Record<string, unknown>> = [];
      for (const cluster of clusters.values()) {
        const clusterOut: Record<string, unknown> = {
          window_start: cluster.window_start,
          window_end: cluster.window_end,
          label: cluster.label,
          memory_count: cluster.memories.length,
          memories: cluster.memories.map((m) => ({
            id: m.id,
            type: m.type,
            title: m.title,
            key: m.key,
            content: String(m.content ?? '').slice(0, 300),
            tags: m.tags,
            source: m.source,
            confidence: m.confidence,
            importance: m.importance,
            created_at: m.created_at,
          })),
        };

        if (wantLinks && cluster.memories.length > 1) {
          const memIds = cluster.memories.map((m) => String(m.id));
          const linkPlaceholders = memIds.map(() => '?').join(',');
          const clusterLinks = await env.DB.prepare(
            `SELECT id, from_id, to_id, relation_type, label FROM memory_links
             WHERE brain_id = ? AND from_id IN (${linkPlaceholders}) AND to_id IN (${linkPlaceholders})`
          ).bind(brainId, ...memIds, ...memIds).all<Record<string, unknown>>();
          clusterOut.links = clusterLinks.results;
        }

        result.push(clusterOut);
      }

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            range: { start: startTs, end: endTs },
            window: windowName,
            total_memories: allRows.results.length,
            cluster_count: result.length,
            clusters: result,
          }, null, 2),
        }],
      };
    }

    case 'memory_spaced_repetition': {
      const { min_importance, min_age_days, max_confidence, limit: rawLimit, include_score_breakdown } = args as {
        min_importance?: unknown;
        min_age_days?: unknown;
        max_confidence?: unknown;
        limit?: unknown;
        include_score_breakdown?: unknown;
      };

      const minImp = clamp01(Number.isFinite(Number(min_importance)) ? Number(min_importance) : 0.4);
      const minAgeDays = Math.max(0, Number.isFinite(Number(min_age_days)) ? Number(min_age_days) : 7);
      const maxConf = clamp01(Number.isFinite(Number(max_confidence)) ? Number(max_confidence) : 0.8);
      const limit = Math.min(Math.max(Number.isFinite(Number(rawLimit)) ? Math.floor(Number(rawLimit)) : 15, 1), 50);
      const wantBreakdown = include_score_breakdown !== false;

      const tsNow = now();
      const ageCutoff = tsNow - Math.floor(minAgeDays * 86400);

      // Find important memories that have low confidence or haven't been accessed recently
      const candidates = await env.DB.prepare(
        `SELECT
          m.id, m.type, m.title, m.key, m.content, m.tags, m.source,
          m.confidence, m.importance, m.created_at, m.updated_at,
          (SELECT COUNT(*) FROM memory_links ml WHERE ml.brain_id = ? AND (ml.from_id = m.id OR ml.to_id = m.id)) AS link_count
        FROM memories m
        WHERE m.brain_id = ?
          AND m.archived_at IS NULL
          AND m.importance >= ?
          AND m.created_at <= ?
        ORDER BY m.updated_at ASC
        LIMIT 500`
      ).bind(brainId, brainId, minImp, ageCutoff).all<Record<string, unknown>>();

      // Score each memory for review urgency
      const scored: Array<{ memory: Record<string, unknown>; urgency: number; breakdown: Record<string, unknown> }> = [];

      for (const row of candidates.results) {
        const confidence = clamp01(toFiniteNumber(row.confidence, 0.7));
        const importance = clamp01(toFiniteNumber(row.importance, 0.5));
        const updatedAt = Number(row.updated_at ?? row.created_at ?? 0);
        const createdAt = Number(row.created_at ?? 0);
        const linkCount = toFiniteNumber(row.link_count, 0);

        // Skip if confidence is already high (doesn't need review)
        if (confidence > maxConf) continue;

        const daysSinceUpdate = (tsNow - updatedAt) / 86400;
        const daysSinceCreation = (tsNow - createdAt) / 86400;

        // Urgency scoring: higher = more urgently needs review
        // Importance drives base urgency
        const importanceSignal = importance * 0.35;
        // Low confidence = needs reinforcement
        const confidenceGap = (1 - confidence) * 0.25;
        // Staleness: longer since last update = more urgent
        const stalenessSignal = Math.min(daysSinceUpdate / 90, 1) * 0.25;
        // Isolation: fewer links = more likely to be forgotten
        const isolationSignal = (1 / (1 + linkCount)) * 0.15;

        const urgency = round3(importanceSignal + confidenceGap + stalenessSignal + isolationSignal);

        scored.push({
          memory: {
            id: row.id,
            type: row.type,
            title: row.title,
            key: row.key,
            content: String(row.content ?? '').slice(0, 300),
            tags: row.tags,
            source: row.source,
            confidence,
            importance,
            created_at: createdAt,
            updated_at: updatedAt,
            link_count: linkCount,
            age_days: round3(daysSinceCreation),
            stale_days: round3(daysSinceUpdate),
          },
          urgency,
          breakdown: {
            importance_signal: round3(importanceSignal),
            confidence_gap: round3(confidenceGap),
            staleness_signal: round3(stalenessSignal),
            isolation_signal: round3(isolationSignal),
          },
        });
      }

      // Sort by urgency descending
      scored.sort((a, b) => b.urgency - a.urgency);
      const topResults = scored.slice(0, limit);

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            candidates_scanned: candidates.results.length,
            review_count: topResults.length,
            filters: { min_importance: minImp, min_age_days: minAgeDays, max_confidence: maxConf },
            memories: topResults.map((r) => ({
              ...r.memory,
              urgency_score: r.urgency,
              ...(wantBreakdown ? { urgency_breakdown: r.breakdown } : {}),
            })),
          }, null, 2),
        }],
      };
    }

    default:
      throw Object.assign(new Error(`Unknown tool: ${name}`), { code: -32601 });
  }
}
