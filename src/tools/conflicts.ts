import type { Env, ToolArgs } from '../types.js';

import {
  generateId,
  now,
  clampToRange,
  toFiniteNumber,
} from '../utils.js';

import { logChangelog } from '../db.js';

import { round3, enrichAndProjectRows } from '../scoring.js';

import { pairKey } from './shared.js';

import type { McpResult } from './shared.js';

export async function conflictTools(name: string, args: ToolArgs, env: Env, brainId: string): Promise<McpResult | null> {
  switch (name) {
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

    default:
      return null;
  }
}
