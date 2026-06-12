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
  normalizeSourceKey,
  stableJson,
  toFiniteNumber,
} from '../utils.js';

import {
  parseJsonObject,
  getBrainPolicy,
  setBrainPolicy,
  loadExplicitMemoryLinks,
  logChangelog,
} from '../db.js';

import {
  safeSyncMemoriesToVectorIndex,
  safeDeleteMemoryVectors,
} from '../vectorize.js';

import {
  pairKey,
} from './shared.js';

import type { McpResult } from './shared.js';

export async function snapshotTools(name: string, args: ToolArgs, env: Env, brainId: string): Promise<McpResult | null> {
  switch (name) {
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

    default:
      return null;
  }
}
