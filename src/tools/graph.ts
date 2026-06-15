import type {
  Env,
  RelationType,
  MemoryGraphNode,
  ToolArgs,
} from '../types.js';

import {
  generateId,
  now,
  clampToRange,
  isValidRelationType,
  normalizeRelation,
  normalizeSourceKey,
  normalizeTag,
  parseTagSet,
  toFiniteNumber,
} from '../utils.js';

import {
  loadLinkStatsMap,
  loadSourceTrustMap,
  getBrainPolicy,
  loadActiveMemoryNodes,
  loadExplicitMemoryLinks,
  logChangelog,
} from '../db.js';

import {
  round3,
  computeDynamicScores,
  enrichAndProjectRows,
  projectMemoryForClient,
} from '../scoring.js';

import {
  tokenizeText,
  jaccardSimilarity,
  pairKey,
  relationSignalWeight,
  buildTagInferredLinks,
} from './shared.js';

import type { McpResult } from './shared.js';

export async function graphTools(name: string, args: ToolArgs, env: Env, brainId: string): Promise<McpResult | null> {
  switch (name) {
    case 'memory_link': {
      const { from_id, to_id, label, relation_type } = args as {
        from_id: unknown;
        to_id: unknown;
        label?: unknown;
        relation_type?: unknown;
      };
      if (typeof from_id !== 'string' || !from_id) return { content: [{ type: 'text', text: 'from_id must be a non-empty string.' }] };
      if (typeof to_id !== 'string' || !to_id) return { content: [{ type: 'text', text: 'to_id must be a non-empty string.' }] };
      if (from_id === to_id) return { content: [{ type: 'text', text: 'Cannot link a memory to itself.' }] };
      if (relation_type !== undefined && !isValidRelationType(relation_type)) return { content: [{ type: 'text', text: 'Invalid relation_type.' }] };
      const relationType = isValidRelationType(relation_type) ? relation_type : 'related';

      // Verify both memories exist
      const fromMem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL').bind(brainId, from_id).first();
      if (!fromMem) return { content: [{ type: 'text', text: `Memory not found: ${from_id}` }] };
      const toMem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL').bind(brainId, to_id).first();
      if (!toMem) return { content: [{ type: 'text', text: `Memory not found: ${to_id}` }] };

      // De-duplicate links (treating pair as undirected)
      const existing = await env.DB.prepare(
        'SELECT id FROM memory_links WHERE brain_id = ? AND ((from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?))'
      ).bind(brainId, from_id, to_id, to_id, from_id).first<{ id: string }>();

      const labelVal = typeof label === 'string' && label.trim() ? label.trim() : null;
      if (existing?.id) {
        await env.DB.prepare(
          'UPDATE memory_links SET relation_type = ?, label = ? WHERE brain_id = ? AND id = ?'
        ).bind(relationType, labelVal, brainId, existing.id).run();
        await logChangelog(env, brainId, 'memory_link_updated', 'memory_link', existing.id, 'Updated link relation', {
          from_id,
          to_id,
          relation_type: relationType,
        });
        return { content: [{ type: 'text', text: JSON.stringify({ link_id: existing.id, from_id, to_id, relation_type: relationType, label: labelVal, updated: true }) }] };
      }

      const link_id = generateId();
      await env.DB.prepare(
        'INSERT INTO memory_links (id, brain_id, from_id, to_id, relation_type, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
      ).bind(link_id, brainId, from_id, to_id, relationType, labelVal, now()).run();
      await logChangelog(env, brainId, 'memory_link_created', 'memory_link', link_id, 'Created memory link', {
        from_id,
        to_id,
        relation_type: relationType,
      });

      return { content: [{ type: 'text', text: JSON.stringify({ link_id, from_id, to_id, relation_type: relationType, label: labelVal }) }] };
    }

    case 'memory_unlink': {
      const { from_id, to_id, relation_type } = args as { from_id: unknown; to_id: unknown; relation_type?: unknown };
      if (typeof from_id !== 'string' || !from_id) return { content: [{ type: 'text', text: 'from_id must be a non-empty string.' }] };
      if (typeof to_id !== 'string' || !to_id) return { content: [{ type: 'text', text: 'to_id must be a non-empty string.' }] };
      if (relation_type !== undefined && !isValidRelationType(relation_type)) return { content: [{ type: 'text', text: 'Invalid relation_type.' }] };

      let sql = 'DELETE FROM memory_links WHERE brain_id = ? AND ((from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?))';
      const params: unknown[] = [brainId, from_id, to_id, to_id, from_id];
      if (relation_type) {
        sql += ' AND relation_type = ?';
        params.push(relation_type);
      }
      const result = await env.DB.prepare(sql).bind(...params).run();

      if (result.meta.changes === 0) return { content: [{ type: 'text', text: 'No link found between these memories.' }] };
      await logChangelog(env, brainId, 'memory_link_removed', 'memory_link', `${from_id}::${to_id}`, 'Removed memory link', {
        from_id,
        to_id,
        relation_type: relation_type ?? null,
      });
      return { content: [{ type: 'text', text: `Link removed between ${from_id} and ${to_id}.` }] };
    }

    case 'memory_links': {
      const { id } = args as { id: unknown };
      if (typeof id !== 'string' || !id) return { content: [{ type: 'text', text: 'id must be a non-empty string.' }] };

      // Verify memory exists
      const mem = await env.DB.prepare('SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL').bind(brainId, id).first();
      if (!mem) return { content: [{ type: 'text', text: 'Memory not found.' }] };

      // Fetch links in both directions with full memory data
      const fromLinks = await env.DB.prepare(
        'SELECT ml.id as link_id, ml.label, ml.relation_type, ml.to_id as connected_id, m.* FROM memory_links ml JOIN memories m ON m.id = ml.to_id WHERE ml.brain_id = ? AND m.brain_id = ? AND ml.from_id = ? AND m.archived_at IS NULL'
      ).bind(brainId, brainId, id).all();

      const toLinks = await env.DB.prepare(
        'SELECT ml.id as link_id, ml.label, ml.relation_type, ml.from_id as connected_id, m.* FROM memory_links ml JOIN memories m ON m.id = ml.from_id WHERE ml.brain_id = ? AND m.brain_id = ? AND ml.to_id = ? AND m.archived_at IS NULL'
      ).bind(brainId, brainId, id).all();

      const tsNow = now();
      const linkStatsMap = await loadLinkStatsMap(env, brainId);
      const sourceTrustMap = await loadSourceTrustMap(env, brainId);
      const toScoredMemory = (r: Record<string, unknown>): Record<string, unknown> => {
        const base = {
          id: r.id,
          type: r.type,
          title: r.title,
          key: r.key,
          content: r.content,
          tags: r.tags,
          source: r.source,
          confidence: r.confidence,
          importance: r.importance,
          created_at: r.created_at,
          updated_at: r.updated_at,
        } as Record<string, unknown>;
        const sourceKey = typeof base.source === 'string' ? normalizeSourceKey(base.source) : '';
        const scored = computeDynamicScores(
          base,
          linkStatsMap.get(String(r.id ?? '')),
          tsNow,
          sourceKey ? sourceTrustMap.get(sourceKey) : undefined
        );
        return projectMemoryForClient({ ...base, ...scored });
      };

      const results = [
        ...fromLinks.results.map((r: Record<string, unknown>) => ({
          link_id: r.link_id,
          relation_type: r.relation_type,
          label: r.label,
          direction: 'from',
          memory: toScoredMemory(r),
        })),
        ...toLinks.results.map((r: Record<string, unknown>) => ({
          link_id: r.link_id,
          relation_type: r.relation_type,
          label: r.label,
          direction: 'to',
          memory: toScoredMemory(r),
        })),
      ];

      if (!results.length) return { content: [{ type: 'text', text: 'No links found for this memory.' }] };
      return { content: [{ type: 'text', text: JSON.stringify(results, null, 2) }] };
    }

    case 'memory_link_suggest': {
      const { id: rawId, query: rawQuery, limit: rawLimit, min_score: rawMinScore, include_existing: rawIncludeExisting } = args as {
        id?: unknown;
        query?: unknown;
        limit?: unknown;
        min_score?: unknown;
        include_existing?: unknown;
      };
      if (rawId !== undefined && typeof rawId !== 'string') return { content: [{ type: 'text', text: 'id must be a string when provided.' }] };
      if (rawQuery !== undefined && typeof rawQuery !== 'string') return { content: [{ type: 'text', text: 'query must be a string when provided.' }] };
      if (rawIncludeExisting !== undefined && typeof rawIncludeExisting !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_existing must be a boolean when provided.' }] };
      }
      const policy = await getBrainPolicy(env, brainId);
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 20, 1), 120);
      const minScore = clampToRange(rawMinScore, policy.min_link_suggestion_score);
      const includeExisting = rawIncludeExisting === true;

      const nodes = await loadActiveMemoryNodes(env, brainId, 1400);
      if (!nodes.length) return { content: [{ type: 'text', text: 'No memories available.' }] };
      const nodeById = new Map(nodes.map((node) => [node.id, node]));
      const seedIds = new Set<string>();
      if (typeof rawId === 'string' && rawId.trim()) {
        const id = rawId.trim();
        if (!nodeById.has(id)) return { content: [{ type: 'text', text: `Seed memory not found: ${id}` }] };
        seedIds.add(id);
      }
      if (typeof rawQuery === 'string' && rawQuery.trim()) {
        const query = rawQuery.trim().toLowerCase();
        const scoredMatches = nodes.map((node) => {
          const text = `${node.id} ${node.title ?? ''} ${node.key ?? ''} ${node.content} ${node.source ?? ''}`.toLowerCase();
          const exact = text.includes(query) ? 1 : 0;
          const tokenSet = new Set(tokenizeText(text, 120));
          const queryTokens = tokenizeText(query, 24);
          let tokenHits = 0;
          for (const token of queryTokens) if (tokenSet.has(token)) tokenHits++;
          const score = (exact * 0.7) + (queryTokens.length ? (tokenHits / queryTokens.length) * 0.3 : 0);
          return { id: node.id, score };
        })
          .filter((row) => row.score > 0)
          .sort((a, b) => b.score - a.score)
          .slice(0, 8);
        for (const match of scoredMatches) seedIds.add(match.id);
      }
      if (!seedIds.size) {
        for (const node of nodes.slice(0, 3)) seedIds.add(node.id);
      }

      const links = await loadExplicitMemoryLinks(env, brainId, 9000);
      const existingPairs = new Set(links.map((edge) => pairKey(edge.from_id, edge.to_id)));
      const tokenCache = new Map<string, Set<string>>();
      const tagCache = new Map<string, Set<string>>();
      const getTokenSet = (node: MemoryGraphNode): Set<string> => {
        const existing = tokenCache.get(node.id);
        if (existing) return existing;
        const tokens = new Set(tokenizeText(`${node.title ?? ''} ${node.key ?? ''} ${node.content} ${node.source ?? ''}`, 120));
        tokenCache.set(node.id, tokens);
        return tokens;
      };
      const getTagSet = (node: MemoryGraphNode): Set<string> => {
        const existing = tagCache.get(node.id);
        if (existing) return existing;
        const tags = parseTagSet(node.tags);
        tagCache.set(node.id, tags);
        return tags;
      };

      const suggestionsByPair = new Map<string, Record<string, unknown>>();
      for (const seedId of seedIds) {
        const seed = nodeById.get(seedId);
        if (!seed) continue;
        const seedTokens = getTokenSet(seed);
        const seedTags = getTagSet(seed);
        const seedSource = seed.source ? normalizeSourceKey(seed.source) : '';
        for (const candidate of nodes) {
          if (candidate.id === seed.id) continue;
          const key = pairKey(seed.id, candidate.id);
          if (!includeExisting && existingPairs.has(key)) continue;
          const candidateTokens = getTokenSet(candidate);
          const candidateTags = getTagSet(candidate);
          let sharedTagCount = 0;
          const sharedTags: string[] = [];
          for (const tag of seedTags) {
            if (!candidateTags.has(tag)) continue;
            sharedTagCount++;
            if (sharedTags.length < 5) sharedTags.push(tag);
          }
          const tagScore = Math.min(1, sharedTagCount / 3);
          const lexicalScore = jaccardSimilarity(seedTokens, candidateTokens);
          const sourceScore = seedSource && candidate.source && seedSource === normalizeSourceKey(candidate.source) ? 1 : 0;
          const ageDeltaDays = Math.abs(toFiniteNumber(seed.updated_at, 0) - toFiniteNumber(candidate.updated_at, 0)) / 86400;
          const temporalScore = ageDeltaDays < 7 ? 1 : ageDeltaDays < 30 ? 0.65 : ageDeltaDays < 120 ? 0.3 : 0.08;
          const typeScore = seed.type === candidate.type ? 1 : 0.45;
          const score = round3(
            (tagScore * 0.45)
            + (lexicalScore * 0.35)
            + (sourceScore * 0.1)
            + (temporalScore * 0.05)
            + (typeScore * 0.05)
          );
          if (score < minScore) continue;

          const prev = suggestionsByPair.get(key);
          if (prev && toFiniteNumber(prev.score, 0) >= score) continue;
          suggestionsByPair.set(key, {
            from_id: seed.id,
            to_id: candidate.id,
            relation_hint: 'related',
            score,
            reasons: {
              shared_tags: sharedTags,
              lexical_similarity: round3(lexicalScore),
              same_source: sourceScore === 1,
              temporal_score: round3(temporalScore),
              type_score: round3(typeScore),
            },
          });
        }
      }

      const suggestions = Array.from(suggestionsByPair.values())
        .sort((a, b) => toFiniteNumber(b.score, 0) - toFiniteNumber(a.score, 0))
        .slice(0, limit);

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            seed_ids: Array.from(seedIds),
            min_score: minScore,
            count: suggestions.length,
            suggestions,
          }, null, 2),
        }],
      };
    }

    case 'memory_path_find': {
      const { from_id: rawFrom, to_id: rawTo, max_hops: rawMaxHops, limit: rawLimit } = args as {
        from_id: unknown;
        to_id: unknown;
        max_hops?: unknown;
        limit?: unknown;
      };
      if (typeof rawFrom !== 'string' || !rawFrom.trim()) return { content: [{ type: 'text', text: 'from_id must be a non-empty string.' }] };
      if (typeof rawTo !== 'string' || !rawTo.trim()) return { content: [{ type: 'text', text: 'to_id must be a non-empty string.' }] };
      const fromId = rawFrom.trim();
      const toId = rawTo.trim();
      if (fromId === toId) {
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              from_id: fromId,
              to_id: toId,
              count: 1,
              paths: [{ nodes: [fromId], edges: [], hops: 0, avg_score: 1 }],
            }, null, 2),
          }],
        };
      }

      const policy = await getBrainPolicy(env, brainId);
      const maxHops = Math.min(Math.max(Number.isInteger(rawMaxHops) ? (rawMaxHops as number) : policy.path_max_hops, 1), 8);
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 5, 1), 20);

      const fromExists = await env.DB.prepare(
        'SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL LIMIT 1'
      ).bind(brainId, fromId).first<{ id: string }>();
      if (!fromExists?.id) return { content: [{ type: 'text', text: `Memory not found: ${fromId}` }] };
      const toExists = await env.DB.prepare(
        'SELECT id FROM memories WHERE brain_id = ? AND id = ? AND archived_at IS NULL LIMIT 1'
      ).bind(brainId, toId).first<{ id: string }>();
      if (!toExists?.id) return { content: [{ type: 'text', text: `Memory not found: ${toId}` }] };

      const links = await loadExplicitMemoryLinks(env, brainId, 12000);
      const adjacency = new Map<string, Array<{ id: string; relation_type: RelationType; link_id: string; label: string | null; weight: number }>>();
      for (const link of links) {
        const weight = relationSignalWeight(link.relation_type);
        const fromArr = adjacency.get(link.from_id);
        const fromEdge = { id: link.to_id, relation_type: link.relation_type, link_id: link.id, label: link.label, weight };
        if (fromArr) fromArr.push(fromEdge);
        else adjacency.set(link.from_id, [fromEdge]);
        const toArr = adjacency.get(link.to_id);
        const toEdge = { id: link.from_id, relation_type: link.relation_type, link_id: link.id, label: link.label, weight };
        if (toArr) toArr.push(toEdge);
        else adjacency.set(link.to_id, [toEdge]);
      }

      const paths: Array<Record<string, unknown>> = [];
      const visited = new Set<string>([fromId]);
      let expansions = 0;
      const maxExpansions = 50000;
      const dfs = (
        currentId: string,
        depth: number,
        nodesPath: string[],
        edgesPath: Array<Record<string, unknown>>,
        cumulativeScore: number
      ): void => {
        if (depth >= maxHops || expansions >= maxExpansions) return;
        const neighbors = [...(adjacency.get(currentId) ?? [])]
          .sort((a, b) => b.weight - a.weight)
          .slice(0, 18);
        for (const neighbor of neighbors) {
          if (expansions >= maxExpansions) break;
          if (visited.has(neighbor.id)) continue;
          expansions++;
          visited.add(neighbor.id);
          const nextNodes = [...nodesPath, neighbor.id];
          const nextEdges = [...edgesPath, {
            link_id: neighbor.link_id,
            from_id: currentId,
            to_id: neighbor.id,
            relation_type: neighbor.relation_type,
            label: neighbor.label,
            weight: round3(neighbor.weight),
          }];
          const nextScore = cumulativeScore + neighbor.weight;
          if (neighbor.id === toId) {
            const hops = nextEdges.length;
            const avgScore = hops ? round3(nextScore / hops) : 0;
            paths.push({
              nodes: nextNodes,
              edges: nextEdges,
              hops,
              cumulative_score: round3(nextScore),
              avg_score: avgScore,
            });
          } else {
            dfs(neighbor.id, depth + 1, nextNodes, nextEdges, nextScore);
          }
          visited.delete(neighbor.id);
        }
      };
      dfs(fromId, 0, [fromId], [], 0);

      paths.sort((a, b) => {
        const scoreDelta = toFiniteNumber(b.avg_score, 0) - toFiniteNumber(a.avg_score, 0);
        if (scoreDelta !== 0) return scoreDelta;
        return toFiniteNumber(a.hops, 999) - toFiniteNumber(b.hops, 999);
      });

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            from_id: fromId,
            to_id: toId,
            max_hops: maxHops,
            explored_paths: paths.length,
            expansions,
            count: Math.min(paths.length, limit),
            paths: paths.slice(0, limit),
          }, null, 2),
        }],
      };
    }

    case 'memory_subgraph': {
      const { seed_id: rawSeedId, query: rawQuery, tag: rawTag, radius: rawRadius, limit_nodes: rawLimitNodes, include_inferred: rawIncludeInferred } = args as {
        seed_id?: unknown;
        query?: unknown;
        tag?: unknown;
        radius?: unknown;
        limit_nodes?: unknown;
        include_inferred?: unknown;
      };
      if (rawSeedId !== undefined && typeof rawSeedId !== 'string') return { content: [{ type: 'text', text: 'seed_id must be a string when provided.' }] };
      if (rawQuery !== undefined && typeof rawQuery !== 'string') return { content: [{ type: 'text', text: 'query must be a string when provided.' }] };
      if (rawTag !== undefined && typeof rawTag !== 'string') return { content: [{ type: 'text', text: 'tag must be a string when provided.' }] };
      if (rawIncludeInferred !== undefined && typeof rawIncludeInferred !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_inferred must be a boolean when provided.' }] };
      }
      const policy = await getBrainPolicy(env, brainId);
      const radius = Math.min(Math.max(Number.isInteger(rawRadius) ? (rawRadius as number) : policy.subgraph_default_radius, 1), 6);
      const limitNodes = Math.min(Math.max(Number.isInteger(rawLimitNodes) ? (rawLimitNodes as number) : 120, 10), 1000);
      const includeInferred = rawIncludeInferred !== false;
      const nodes = await loadActiveMemoryNodes(env, brainId, 1800);
      if (!nodes.length) return { content: [{ type: 'text', text: 'No memories available.' }] };
      const tagFilter = typeof rawTag === 'string' && rawTag.trim() ? normalizeTag(rawTag) : '';
      const nodeById = new Map(nodes.map((node) => [node.id, node]));
      const candidateSeeds = tagFilter
        ? nodes.filter((node) => parseTagSet(node.tags).has(tagFilter))
        : nodes;
      const seedIds = new Set<string>();
      if (typeof rawSeedId === 'string' && rawSeedId.trim() && nodeById.has(rawSeedId.trim())) {
        const seed = rawSeedId.trim();
        if (!tagFilter || parseTagSet(nodeById.get(seed)?.tags).has(tagFilter)) seedIds.add(seed);
      }
      if (typeof rawQuery === 'string' && rawQuery.trim()) {
        const query = rawQuery.trim().toLowerCase();
        const scored = candidateSeeds.map((node) => {
          const text = `${node.id} ${node.title ?? ''} ${node.key ?? ''} ${node.content} ${node.source ?? ''}`.toLowerCase();
          const direct = text.includes(query) ? 1 : 0;
          const overlap = jaccardSimilarity(
            new Set(tokenizeText(text, 100)),
            new Set(tokenizeText(query, 24))
          );
          return { id: node.id, score: direct * 0.7 + overlap * 0.3 };
        }).filter((row) => row.score > 0)
          .sort((a, b) => b.score - a.score)
          .slice(0, 8);
        for (const item of scored) seedIds.add(item.id);
      }
      if (!seedIds.size) {
        for (const node of candidateSeeds.slice(0, 3)) seedIds.add(node.id);
      }
      if (!seedIds.size) return { content: [{ type: 'text', text: 'No seed nodes matched the requested filters.' }] };

      const links = await loadExplicitMemoryLinks(env, brainId, 12000);
      const adjacency = new Map<string, string[]>();
      for (const link of links) {
        if (!nodeById.has(link.from_id) || !nodeById.has(link.to_id)) continue;
        const fromArr = adjacency.get(link.from_id);
        if (fromArr) fromArr.push(link.to_id);
        else adjacency.set(link.from_id, [link.to_id]);
        const toArr = adjacency.get(link.to_id);
        if (toArr) toArr.push(link.from_id);
        else adjacency.set(link.to_id, [link.from_id]);
      }

      const depthByNode = new Map<string, number>();
      const queue: Array<{ id: string; depth: number }> = [];
      for (const seedId of seedIds) {
        depthByNode.set(seedId, 0);
        queue.push({ id: seedId, depth: 0 });
      }
      while (queue.length > 0 && depthByNode.size < limitNodes) {
        const current = queue.shift();
        if (!current) break;
        if (current.depth >= radius) continue;
        const neighbors = adjacency.get(current.id) ?? [];
        for (const neighbor of neighbors) {
          if (depthByNode.has(neighbor)) continue;
          depthByNode.set(neighbor, current.depth + 1);
          queue.push({ id: neighbor, depth: current.depth + 1 });
          if (depthByNode.size >= limitNodes) break;
        }
      }

      const selectedIds = new Set(depthByNode.keys());
      const selectedNodes = nodes.filter((node) => selectedIds.has(node.id));
      const selectedEdges = links.filter((link) => selectedIds.has(link.from_id) && selectedIds.has(link.to_id));
      const explicitPairs = new Set(selectedEdges.map((edge) => pairKey(edge.from_id, edge.to_id)));
      const inferredEdges = includeInferred
        ? buildTagInferredLinks(selectedNodes, Math.min(policy.max_inferred_edges, 1200))
          .filter((edge) => !explicitPairs.has(pairKey(edge.from_id, edge.to_id)))
        : [];

      const projectedNodes = await enrichAndProjectRows(env, brainId, selectedNodes as unknown as Array<Record<string, unknown>>);
      const depthObject: Record<string, number> = {};
      for (const [nodeId, depth] of depthByNode) depthObject[nodeId] = depth;

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            seed_ids: Array.from(seedIds),
            radius,
            node_count: projectedNodes.length,
            edge_count: selectedEdges.length,
            inferred_edge_count: inferredEdges.length,
            depth_by_node: depthObject,
            nodes: projectedNodes,
            edges: selectedEdges,
            inferred_edges: inferredEdges,
          }, null, 2),
        }],
      };
    }

    case 'memory_neighbors': {
      const { id: rawId, query: rawQuery, max_hops: rawMaxHops, limit_nodes: rawLimitNodes, relation_type: rawRelationType, include_inferred: rawIncludeInferred } = args as {
        id?: unknown;
        query?: unknown;
        max_hops?: unknown;
        limit_nodes?: unknown;
        relation_type?: unknown;
        include_inferred?: unknown;
      };
      if (rawId !== undefined && typeof rawId !== 'string') return { content: [{ type: 'text', text: 'id must be a string when provided.' }] };
      if (rawQuery !== undefined && typeof rawQuery !== 'string') return { content: [{ type: 'text', text: 'query must be a string when provided.' }] };
      if (rawIncludeInferred !== undefined && typeof rawIncludeInferred !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_inferred must be a boolean when provided.' }] };
      }
      if (rawRelationType !== undefined && !isValidRelationType(rawRelationType)) {
        return { content: [{ type: 'text', text: 'relation_type must be one of related|supports|contradicts|supersedes|causes|example_of.' }] };
      }
      const relationFilter = isValidRelationType(rawRelationType) ? rawRelationType : null;
      const maxHops = Math.min(Math.max(Number.isInteger(rawMaxHops) ? (rawMaxHops as number) : 1, 1), 4);
      const limitNodes = Math.min(Math.max(Number.isInteger(rawLimitNodes) ? (rawLimitNodes as number) : 80, 5), 1000);
      const includeInferred = rawIncludeInferred !== false && (relationFilter === null || relationFilter === 'related');

      const nodes = await loadActiveMemoryNodes(env, brainId, 2200);
      if (!nodes.length) return { content: [{ type: 'text', text: 'No memories available.' }] };
      const nodeById = new Map(nodes.map((node) => [node.id, node]));

      let seedId = '';
      if (typeof rawId === 'string' && rawId.trim() && nodeById.has(rawId.trim())) {
        seedId = rawId.trim();
      }
      if (!seedId && typeof rawQuery === 'string' && rawQuery.trim()) {
        const q = rawQuery.trim().toLowerCase();
        const qTokens = new Set(tokenizeText(q, 24));
        const scored = nodes.map((node) => {
          const blob = `${node.id} ${node.title ?? ''} ${node.key ?? ''} ${node.content} ${node.tags ?? ''} ${node.source ?? ''}`.toLowerCase();
          const direct = blob.includes(q) ? 0.75 : 0;
          const overlap = qTokens.size ? jaccardSimilarity(new Set(tokenizeText(blob, 100)), qTokens) : 0;
          return { id: node.id, score: direct + overlap * 0.25 };
        }).filter((row) => row.score > 0)
          .sort((a, b) => b.score - a.score);
        seedId = scored[0]?.id ?? '';
      }
      if (!seedId) {
        return { content: [{ type: 'text', text: 'Provide id or query to select a seed memory.' }] };
      }

      const explicitLinks = (await loadExplicitMemoryLinks(env, brainId, 16000))
        .filter((link) => nodeById.has(link.from_id) && nodeById.has(link.to_id))
        .filter((link) => !relationFilter || normalizeRelation(link.relation_type) === relationFilter);
      const explicitPairs = new Set(explicitLinks.map((link) => pairKey(link.from_id, link.to_id)));
      const policy = await getBrainPolicy(env, brainId);
      const inferredLinks = includeInferred
        ? buildTagInferredLinks(nodes, Math.min(policy.max_inferred_edges, 1800))
          .filter((link) => !explicitPairs.has(pairKey(link.from_id, link.to_id)))
        : [];

      const adjacency = new Map<string, string[]>();
      for (const edge of [...explicitLinks, ...inferredLinks]) {
        const fromArr = adjacency.get(edge.from_id);
        if (fromArr) fromArr.push(edge.to_id);
        else adjacency.set(edge.from_id, [edge.to_id]);
        const toArr = adjacency.get(edge.to_id);
        if (toArr) toArr.push(edge.from_id);
        else adjacency.set(edge.to_id, [edge.from_id]);
      }

      const depthByNode = new Map<string, number>();
      const queue: string[] = [seedId];
      depthByNode.set(seedId, 0);
      while (queue.length && depthByNode.size < limitNodes) {
        const current = queue.shift();
        if (!current) break;
        const depth = depthByNode.get(current) ?? 0;
        if (depth >= maxHops) continue;
        const neighbors = adjacency.get(current) ?? [];
        for (const neighborId of neighbors) {
          if (depthByNode.has(neighborId)) continue;
          depthByNode.set(neighborId, depth + 1);
          queue.push(neighborId);
          if (depthByNode.size >= limitNodes) break;
        }
      }

      const selectedIds = new Set(depthByNode.keys());
      const selectedNodes = nodes.filter((node) => selectedIds.has(node.id));
      const selectedEdges = explicitLinks.filter((edge) => selectedIds.has(edge.from_id) && selectedIds.has(edge.to_id));
      const selectedInferred = inferredLinks.filter((edge) => selectedIds.has(edge.from_id) && selectedIds.has(edge.to_id));
      const projectedNodes = await enrichAndProjectRows(
        env,
        brainId,
        selectedNodes as unknown as Array<Record<string, unknown>>
      );
      const projectedById = new Map(projectedNodes.map((node) => [String(node.id), node]));
      const depthObject: Record<string, number> = {};
      for (const [nodeId, depth] of depthByNode.entries()) depthObject[nodeId] = depth;

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            seed_id: seedId,
            seed: projectedById.get(seedId) ?? null,
            max_hops: maxHops,
            relation_filter: relationFilter,
            include_inferred: includeInferred,
            node_count: projectedNodes.length,
            edge_count: selectedEdges.length,
            inferred_edge_count: selectedInferred.length,
            depth_by_node: depthObject,
            nodes: projectedNodes,
            edges: selectedEdges,
            inferred_edges: selectedInferred,
          }, null, 2),
        }],
      };
    }

    case 'memory_graph_stats': {
      const { include_inferred: rawIncludeInferred, top_hubs: rawTopHubs, top_tags: rawTopTags } = args as {
        include_inferred?: unknown;
        top_hubs?: unknown;
        top_tags?: unknown;
      };
      if (rawIncludeInferred !== undefined && typeof rawIncludeInferred !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_inferred must be a boolean when provided.' }] };
      }
      const includeInferred = rawIncludeInferred !== false;
      const topHubs = Math.min(Math.max(Number.isInteger(rawTopHubs) ? (rawTopHubs as number) : 12, 1), 50);
      const topTags = Math.min(Math.max(Number.isInteger(rawTopTags) ? (rawTopTags as number) : 12, 1), 50);
      const nodes = await loadActiveMemoryNodes(env, brainId, 2200);
      const explicitLinks = await loadExplicitMemoryLinks(env, brainId, 16000);
      const explicitPairs = new Set(explicitLinks.map((link) => pairKey(link.from_id, link.to_id)));
      const policy = await getBrainPolicy(env, brainId);
      const inferredLinks = includeInferred
        ? buildTagInferredLinks(nodes, Math.min(policy.max_inferred_edges, 3000))
          .filter((link) => !explicitPairs.has(pairKey(link.from_id, link.to_id)))
        : [];
      const allLinks = [...explicitLinks, ...inferredLinks];

      const adjacency = new Map<string, string[]>();
      const degreeById = new Map<string, number>();
      const relationCounts: Record<string, number> = {
        related: 0, supports: 0, contradicts: 0, supersedes: 0, causes: 0, example_of: 0, inferred: 0,
      };
      const perNodeRelation = new Map<string, Record<string, number>>();

      for (const node of nodes) {
        if (!adjacency.has(node.id)) adjacency.set(node.id, []);
      }
      for (const link of allLinks) {
        if (!adjacency.has(link.from_id)) adjacency.set(link.from_id, []);
        if (!adjacency.has(link.to_id)) adjacency.set(link.to_id, []);
        adjacency.get(link.from_id)?.push(link.to_id);
        adjacency.get(link.to_id)?.push(link.from_id);
        degreeById.set(link.from_id, (degreeById.get(link.from_id) ?? 0) + 1);
        degreeById.set(link.to_id, (degreeById.get(link.to_id) ?? 0) + 1);
        const relationKey = link.inferred ? 'inferred' : normalizeRelation(link.relation_type);
        relationCounts[relationKey] = (relationCounts[relationKey] ?? 0) + 1;

        const fromStats = perNodeRelation.get(link.from_id) ?? {
          related: 0, supports: 0, contradicts: 0, supersedes: 0, causes: 0, example_of: 0, inferred: 0,
        };
        fromStats[relationKey] = (fromStats[relationKey] ?? 0) + 1;
        perNodeRelation.set(link.from_id, fromStats);
        const toStats = perNodeRelation.get(link.to_id) ?? {
          related: 0, supports: 0, contradicts: 0, supersedes: 0, causes: 0, example_of: 0, inferred: 0,
        };
        toStats[relationKey] = (toStats[relationKey] ?? 0) + 1;
        perNodeRelation.set(link.to_id, toStats);
      }

      let connectedComponents = 0;
      let isolatedNodes = 0;
      const componentSizes: number[] = [];
      const visited = new Set<string>();
      for (const node of nodes) {
        const seedId = node.id;
        if (visited.has(seedId)) continue;
        connectedComponents++;
        let size = 0;
        const queue = [seedId];
        visited.add(seedId);
        while (queue.length) {
          const current = queue.shift();
          if (!current) break;
          size++;
          const neighbors = adjacency.get(current) ?? [];
          for (const neighbor of neighbors) {
            if (visited.has(neighbor)) continue;
            visited.add(neighbor);
            queue.push(neighbor);
          }
        }
        componentSizes.push(size);
        if (size === 1 && (degreeById.get(seedId) ?? 0) === 0) isolatedNodes++;
      }
      componentSizes.sort((a, b) => b - a);

      const projectedNodes = await enrichAndProjectRows(
        env,
        brainId,
        nodes as unknown as Array<Record<string, unknown>>
      );
      const projectedById = new Map(projectedNodes.map((node) => [String(node.id), node]));
      const topHubIds = nodes
        .map((node) => node.id)
        .sort((a, b) => {
          const byDegree = (degreeById.get(b) ?? 0) - (degreeById.get(a) ?? 0);
          if (byDegree !== 0) return byDegree;
          return a.localeCompare(b);
        })
        .slice(0, topHubs);
      const hubs = topHubIds.map((id) => ({
        id,
        degree: degreeById.get(id) ?? 0,
        relations: perNodeRelation.get(id) ?? {},
        memory: projectedById.get(id) ?? null,
      }));

      const tagCounts = new Map<string, number>();
      for (const node of nodes) {
        for (const tag of parseTagSet(node.tags)) {
          tagCounts.set(tag, (tagCounts.get(tag) ?? 0) + 1);
        }
      }
      const topTagRows = Array.from(tagCounts.entries())
        .sort((a, b) => {
          if (b[1] !== a[1]) return b[1] - a[1];
          return a[0].localeCompare(b[0]);
        })
        .slice(0, topTags)
        .map(([tag, count]) => ({ tag, count }));

      const avgConfidence = projectedNodes.length
        ? round3(projectedNodes.reduce((sum, node) => sum + toFiniteNumber(node.dynamic_confidence, 0.7), 0) / projectedNodes.length)
        : null;
      const avgImportance = projectedNodes.length
        ? round3(projectedNodes.reduce((sum, node) => sum + toFiniteNumber(node.dynamic_importance, 0.5), 0) / projectedNodes.length)
        : null;
      const density = nodes.length > 1
        ? round3((2 * allLinks.length) / (nodes.length * (nodes.length - 1)))
        : 0;

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            node_count: nodes.length,
            explicit_edge_count: explicitLinks.length,
            inferred_edge_count: inferredLinks.length,
            total_edge_count: allLinks.length,
            connected_components: connectedComponents,
            isolated_nodes: isolatedNodes,
            largest_component_size: componentSizes[0] ?? 0,
            density,
            relation_counts: relationCounts,
            avg_dynamic_confidence: avgConfidence,
            avg_dynamic_importance: avgImportance,
            top_hubs: hubs,
            top_tags: topTagRows,
          }, null, 2),
        }],
      };
    }

    case 'memory_tag_stats': {
      const { limit: rawLimit, min_count: rawMinCount, include_pairs: rawIncludePairs } = args as {
        limit?: unknown;
        min_count?: unknown;
        include_pairs?: unknown;
      };
      if (rawIncludePairs !== undefined && typeof rawIncludePairs !== 'boolean') {
        return { content: [{ type: 'text', text: 'include_pairs must be a boolean when provided.' }] };
      }
      const limit = Math.min(Math.max(Number.isInteger(rawLimit) ? (rawLimit as number) : 20, 1), 100);
      const minCount = Math.min(Math.max(Number.isInteger(rawMinCount) ? (rawMinCount as number) : 2, 1), 1000);
      const includePairs = rawIncludePairs !== false;
      const rows = await env.DB.prepare(
        'SELECT id, tags FROM memories WHERE brain_id = ? AND archived_at IS NULL ORDER BY created_at DESC LIMIT 5000'
      ).bind(brainId).all<{ id: string; tags: string | null }>();

      const tagCounts = new Map<string, number>();
      const tagMemoryIds = new Map<string, Set<string>>();
      const pairCounts = new Map<string, number>();

      for (const row of rows.results) {
        const memoryId = typeof row.id === 'string' ? row.id : '';
        if (!memoryId) continue;
        const tags = Array.from(parseTagSet(row.tags));
        if (!tags.length) continue;
        for (const tag of tags) {
          tagCounts.set(tag, (tagCounts.get(tag) ?? 0) + 1);
          const ids = tagMemoryIds.get(tag) ?? new Set<string>();
          ids.add(memoryId);
          tagMemoryIds.set(tag, ids);
        }
        if (!includePairs || tags.length < 2) continue;
        const sortedTags = tags.slice(0, 20).sort();
        for (let i = 0; i < sortedTags.length; i++) {
          for (let j = i + 1; j < sortedTags.length; j++) {
            const key = `${sortedTags[i]}|${sortedTags[j]}`;
            pairCounts.set(key, (pairCounts.get(key) ?? 0) + 1);
          }
        }
      }

      const topTags = Array.from(tagCounts.entries())
        .filter(([, count]) => count >= minCount)
        .sort((a, b) => {
          if (b[1] !== a[1]) return b[1] - a[1];
          return a[0].localeCompare(b[0]);
        })
        .slice(0, limit)
        .map(([tag, count]) => ({
          tag,
          count,
          sample_memory_ids: Array.from(tagMemoryIds.get(tag) ?? []).slice(0, 5),
        }));

      const topPairs = includePairs
        ? Array.from(pairCounts.entries())
          .filter(([, count]) => count >= Math.max(2, minCount - 1))
          .sort((a, b) => {
            if (b[1] !== a[1]) return b[1] - a[1];
            return a[0].localeCompare(b[0]);
          })
          .slice(0, Math.min(25, limit))
          .map(([pair, count]) => {
            const [a, b] = pair.split('|');
            return { tag_a: a, tag_b: b, count };
          })
        : [];

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            memory_count: rows.results.length,
            unique_tag_count: tagCounts.size,
            min_count: minCount,
            tags: topTags,
            top_pairs: topPairs,
          }, null, 2),
        }],
      };
    }

    default:
      return null;
  }
}
