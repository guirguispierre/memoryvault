import type {
  Env,
  ToolArgs,
} from '../types.js';

import {
  generateId,
  now,
  clampToRange,
  normalizeSourceKey,
} from '../utils.js';

import {
  getBrainPolicy,
  setBrainPolicy,
  logChangelog,
} from '../db.js';

import type { McpResult } from './shared.js';

export async function trustPolicyTools(name: string, args: ToolArgs, env: Env, brainId: string): Promise<McpResult | null> {
  switch (name) {
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

    default:
      return null;
  }
}
