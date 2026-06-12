import type { Env, ToolArgs } from '../types.js';

import { now, isValidType, toFiniteNumber } from '../utils.js';

import { clamp01, round3 } from '../scoring.js';

import type { McpResult } from './shared.js';

export async function reviewTools(name: string, args: ToolArgs, env: Env, brainId: string): Promise<McpResult | null> {
  switch (name) {
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
