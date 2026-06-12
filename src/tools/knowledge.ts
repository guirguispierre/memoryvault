import type {
  Env,
  ToolArgs,
} from '../types.js';

import {
  generateId,
  now,
  clampToRange,
  isValidType,
  normalizeRelation,
  parseTagSet,
  toFiniteNumber,
} from '../utils.js';

import {
  logChangelog,
} from '../db.js';

import {
  safeSyncMemoriesToVectorIndex,
  safeDeleteMemoryVectors,
} from '../vectorize.js';

import {
  clamp01,
  round3,
  enrichAndProjectRows,
} from '../scoring.js';

import {
  pairKey,
  relationSignalWeight,
  relationSpreadWeight,
  buildAdjacencyFromEdges,
} from './shared.js';

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

      // Query memories in the time range [start, end)
      const params: unknown[] = [brainId, startTs, endTs];
      let query = 'SELECT id, type, title, key, content, tags, source, confidence, importance, created_at, updated_at FROM memories WHERE brain_id = ? AND archived_at IS NULL AND created_at >= ? AND created_at < ?';
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
        // Bucket by UTC-aligned boundaries
        let windowStart: number;
        if (windowName === 'hour') {
          windowStart = createdAt - (createdAt % 3600);
        } else if (windowName === 'week') {
          // Align to Monday 00:00 UTC (epoch was a Thursday, so offset by 3 days)
          const dayTs = createdAt - (createdAt % 86400);
          const dayOfWeek = (Math.floor(dayTs / 86400) + 4) % 7; // 0=Sun
          const mondayOffset = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
          windowStart = dayTs - mondayOffset * 86400;
        } else {
          windowStart = createdAt - (createdAt % 86400);
        }
        const windowEnd = Math.min(windowStart + windowSize, endTs);

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

      // Load all links between returned memories in a single batch query
      const allMemIds = allRows.results.map((r) => String(r.id));
      const allMemIdSet = new Set(allMemIds);
      let allLinks: Record<string, unknown>[] = [];
      if (wantLinks && allMemIds.length > 1) {
        const linkPlaceholders = allMemIds.map(() => '?').join(',');
        const linkRows = await env.DB.prepare(
          `SELECT id, from_id, to_id, relation_type, label FROM memory_links
           WHERE brain_id = ? AND from_id IN (${linkPlaceholders}) AND to_id IN (${linkPlaceholders})`
        ).bind(brainId, ...allMemIds, ...allMemIds).all<Record<string, unknown>>();
        allLinks = linkRows.results;
      }

      const result: Array<Record<string, unknown>> = [];
      for (const cluster of clusters.values()) {
        const clusterMemIds = new Set(cluster.memories.map((m) => String(m.id)));
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
          clusterOut.links = allLinks.filter(
            (l) => clusterMemIds.has(String(l.from_id)) && clusterMemIds.has(String(l.to_id))
          );
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
      return null;
  }
}
