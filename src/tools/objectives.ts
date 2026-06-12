import type {
  Env,
  RelationType,
  ToolArgs,
} from '../types.js';

import {
  generateId,
  now,
  clampToRange,
  parseTagSet,
  toFiniteNumber,
  slugify,
} from '../utils.js';

import {
  ensureObjectiveRoot,
  logChangelog,
} from '../db.js';

import {
  safeSyncMemoriesToVectorIndex,
} from '../vectorize.js';

import {
  clamp01,
  round3,
  enrichAndProjectRows,
} from '../scoring.js';

import type { McpResult } from './shared.js';

export async function objectiveTools(name: string, args: ToolArgs, env: Env, brainId: string): Promise<McpResult | null> {
  switch (name) {
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

    default:
      return null;
  }
}
