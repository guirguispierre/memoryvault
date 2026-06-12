import type { Env, ToolArgs } from '../types.js';

import {
  generateId,
  now,
  clampToRange,
  isValidType,
  normalizeRelation,
  parseTagSet,
  toFiniteNumber,
} from '../utils.js';

import { logChangelog } from '../db.js';

import { safeSyncMemoriesToVectorIndex, safeDeleteMemoryVectors } from '../vectorize.js';

import { clamp01, round3, enrichAndProjectRows } from '../scoring.js';

import { relationSignalWeight, relationSpreadWeight, buildAdjacencyFromEdges } from './shared.js';

import type { McpResult, GraphEdge } from './shared.js';

export async function knowledgeTools(name: string, args: ToolArgs, env: Env, brainId: string): Promise<McpResult | null> {
  switch (name) {
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
      if (memory_ids.length > 20) {
        return { content: [{ type: 'text', text: 'memory_ids cannot exceed 20 entries.' }] };
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

      // Merge tags (normalized and deduplicated)
      const allTags = new Set<string>();
      for (const r of rows.results) {
        if (typeof r.tags === 'string') {
          for (const t of parseTagSet(r.tags)) allTags.add(t);
        }
      }
      const mergedTags = allTags.size > 0 ? Array.from(allTags).sort((a, b) => a.localeCompare(b)).join(',') : null;

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

        // Clean up old links from archived memory (except the supersedes link we just created)
        await env.DB.prepare(
          'DELETE FROM memory_links WHERE brain_id = ? AND (from_id = ? OR to_id = ?) AND id NOT IN (SELECT id FROM memory_links WHERE brain_id = ? AND from_id = ? AND to_id = ? AND relation_type = ?)'
        ).bind(brainId, otherId, otherId, brainId, primaryId, otherId, 'supersedes').run();
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

    default:
      return null;
  }
}
