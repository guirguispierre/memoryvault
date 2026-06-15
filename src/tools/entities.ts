import type { Env, ToolArgs } from '../types.js';

import { generateId, now, clampToRange } from '../utils.js';

import { logChangelog } from '../db.js';

import { safeDeleteMemoryVectors } from '../vectorize.js';

import type { McpResult } from './shared.js';

export async function entityTools(name: string, args: ToolArgs, env: Env, brainId: string): Promise<McpResult | null> {
  switch (name) {
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

    default:
      return null;
  }
}
