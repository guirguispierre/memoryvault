import type {
  Env,
  ToolArgs,
} from '../types.js';

import {
  SERVER_NAME,
  SERVER_VERSION,
  EMPTY_LINK_STATS,
} from '../constants.js';

import {
  generateId,
  now,
  normalizeSourceKey,
  stableJson,
} from '../utils.js';

import {
  loadLinkStatsMap,
  loadSourceTrustMap,
  logChangelog,
  normalizeWatchEventInput,
  parseWatchEventTypes,
} from '../db.js';

import {
  round3,
  computeDynamicScoreBreakdown,
  projectMemoryForClient,
} from '../scoring.js';

import {
  TOOLS,
  TOOL_CHANGELOG,
  getToolReleaseMeta,
  isToolDeprecated,
  compareSemver,
  parseSemver,
} from '../tools-schema.js';

import {
  sha256DigestBase64Url,
} from '../crypto.js';

import {
  canonicalJson,
} from './shared.js';

import type { McpResult } from './shared.js';

export async function observabilityTools(name: string, args: ToolArgs, env: Env, brainId: string): Promise<McpResult | null> {
  switch (name) {
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

    default:
      return null;
  }
}
