import type {
  Env,
  MemorySearchMode,
  MemoryType,
  SemanticMemoryCandidate,
  ToolArgs,
} from '../types.js';

import {
  EMPTY_LINK_STATS,
  MEMORY_SEARCH_DEFAULT_LIMIT,
  MEMORY_SEARCH_MAX_LIMIT,
  VECTORIZE_QUERY_TOP_K_MAX,
  VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS,
  VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS_MAX,
} from '../constants.js';

import {
  generateId,
  now,
  clampToRange,
  isMemorySearchMode,
  hasSemanticSearchBindings,
  isValidType,
  normalizeSourceKey,
  toFiniteNumber,
} from '../utils.js';

import {
  loadMemoryRowsByIds,
  runLexicalMemorySearch,
  logChangelog,
} from '../db.js';

import {
  safeSyncMemoriesToVectorIndex,
  syncMemoriesToVectorIndex,
  safeDeleteMemoryVectors,
  querySemanticMemoryCandidates,
  fuseSearchRows,
  waitForVectorMutationReady,
  waitForVectorQueryReady,
} from '../vectorize.js';

import {
  round3,
  computeDynamicScores,
  enrichAndProjectRows,
  projectMemoryForClient,
} from '../scoring.js';

import type { McpResult } from './shared.js';

export async function memoryTools(name: string, args: ToolArgs, env: Env, brainId: string): Promise<McpResult | null> {
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

    default:
      return null;
  }
}
