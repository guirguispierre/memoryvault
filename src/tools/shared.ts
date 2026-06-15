import type {
  Env,
  MemorySearchMode,
  MemoryType,
  SemanticMemoryCandidate,
  RelationType,
  MemoryGraphNode,
  MemoryGraphLink,
  ToolArgs,
} from '../types.js';

import {
  SERVER_NAME,
  SERVER_VERSION,
  EMPTY_LINK_STATS,
  MEMORY_SEARCH_DEFAULT_LIMIT,
  MEMORY_SEARCH_MAX_LIMIT,
  VECTORIZE_QUERY_TOP_K_MAX,
  VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS,
  VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS_MAX,
} from '../constants.js';

import {
  generateId,
  now,
  clampToRange,
  isMemorySearchMode,
  hasSemanticSearchBindings,
  isValidType,
  isValidRelationType,
  normalizeRelation,
  normalizeSourceKey,
  normalizeTag,
  parseTagSet,
  stableJson,
  toFiniteNumber,
  slugify,
} from '../utils.js';

import {
  parseJsonObject,
  loadMemoryRowsByIds,
  runLexicalMemorySearch,
  loadLinkStatsMap,
  loadSourceTrustMap,
  getBrainPolicy,
  setBrainPolicy,
  loadActiveMemoryNodes,
  loadExplicitMemoryLinks,
  ensureObjectiveRoot,
  logChangelog,
  normalizeWatchEventInput,
  parseWatchEventTypes,
} from '../db.js';

import {
  safeSyncMemoriesToVectorIndex,
  syncMemoriesToVectorIndex,
  safeDeleteMemoryVectors,
  querySemanticMemoryCandidates,
  fuseSearchRows,
  waitForVectorMutationReady,
  waitForVectorQueryReady,
} from '../vectorize.js';

import {
  clamp01,
  round3,
  computeDynamicScoreBreakdown,
  computeDynamicScores,
  enrichAndProjectRows,
  projectMemoryForClient,
} from '../scoring.js';

import {
  TOOLS,
  TOOL_RELEASE_META,
  TOOL_CHANGELOG,
  getToolReleaseMeta,
  isToolDeprecated,
  compareSemver,
  parseSemver,
} from '../tools-schema.js';

import {
  sha256DigestBase64Url,
} from '../crypto.js';

export function tokenizeText(raw: string, max = 80): string[] {
  const stopWords = new Set([
    'the', 'a', 'an', 'and', 'or', 'to', 'of', 'for', 'in', 'on', 'at', 'is', 'are', 'was', 'were', 'be',
    'with', 'as', 'by', 'it', 'this', 'that', 'from', 'but', 'not', 'if', 'then', 'so', 'we', 'you', 'i',
  ]);
  const cleaned = raw
    .toLowerCase()
    .replace(/[^a-z0-9\s]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!cleaned) return [];
  const out: string[] = [];
  for (const token of cleaned.split(' ')) {
    if (token.length < 2 || stopWords.has(token)) continue;
    out.push(token);
    if (out.length >= max) break;
  }
  return out;
}

export function jaccardSimilarity(a: Set<string>, b: Set<string>): number {
  if (!a.size || !b.size) return 0;
  let intersection = 0;
  for (const item of a) {
    if (b.has(item)) intersection++;
  }
  const union = a.size + b.size - intersection;
  if (!union) return 0;
  return intersection / union;
}

export function pairKey(a: string, b: string): string {
  return a < b ? `${a}|${b}` : `${b}|${a}`;
}


export function canonicalizeForJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => canonicalizeForJson(item));
  }
  if (!value || typeof value !== 'object') {
    return value;
  }
  const obj = value as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(obj).sort()) {
    out[key] = canonicalizeForJson(obj[key]);
  }
  return out;
}

export function canonicalJson(value: unknown): string {
  return JSON.stringify(canonicalizeForJson(value));
}


export type GraphEdge = { from: string; to: string; relation_type: RelationType };
export type GraphNeighbor = { id: string; relation_type: RelationType };

export function relationSignalWeight(relationType: RelationType): number {
  switch (relationType) {
    case 'supports': return 0.88;
    case 'causes': return 0.82;
    case 'example_of': return 0.7;
    case 'supersedes': return 0.65;
    case 'contradicts': return -0.75;
    case 'related':
    default:
      return 0.62;
  }
}

export function relationSpreadWeight(relationType: RelationType): number {
  switch (relationType) {
    case 'supports': return 1;
    case 'causes': return 0.9;
    case 'example_of': return 0.75;
    case 'supersedes': return 0.72;
    case 'contradicts': return -0.65;
    case 'related':
    default:
      return 0.68;
  }
}

export function buildAdjacencyFromEdges(edges: GraphEdge[]): Map<string, GraphNeighbor[]> {
  const adjacency = new Map<string, GraphNeighbor[]>();
  for (const edge of edges) {
    const rel = normalizeRelation(edge.relation_type);
    const fromArr = adjacency.get(edge.from);
    if (fromArr) fromArr.push({ id: edge.to, relation_type: rel });
    else adjacency.set(edge.from, [{ id: edge.to, relation_type: rel }]);
    const toArr = adjacency.get(edge.to);
    if (toArr) toArr.push({ id: edge.from, relation_type: rel });
    else adjacency.set(edge.to, [{ id: edge.from, relation_type: rel }]);
  }
  return adjacency;
}








export function buildTagInferredLinks(nodes: MemoryGraphNode[], maxEdges = 400): MemoryGraphLink[] {
  const tagToIds = new Map<string, string[]>();
  for (const node of nodes) {
    const tags = parseTagSet(node.tags);
    for (const tag of tags) {
      const ids = tagToIds.get(tag);
      if (ids) ids.push(node.id);
      else tagToIds.set(tag, [node.id]);
    }
  }

  const byPair = new Map<string, { from: string; to: string; score: number; shared: Set<string> }>();
  for (const [tag, idsRaw] of tagToIds) {
    const ids = Array.from(new Set(idsRaw));
    if (ids.length < 2) continue;
    const trimmed = ids.slice(0, 30);
    const weight = 1 / Math.sqrt(trimmed.length);
    for (let i = 0; i < trimmed.length; i++) {
      for (let j = i + 1; j < trimmed.length; j++) {
        const from = trimmed[i] < trimmed[j] ? trimmed[i] : trimmed[j];
        const to = trimmed[i] < trimmed[j] ? trimmed[j] : trimmed[i];
        const key = `${from}|${to}`;
        const existing = byPair.get(key);
        if (existing) {
          existing.score += weight;
          existing.shared.add(tag);
        } else {
          byPair.set(key, { from, to, score: weight, shared: new Set([tag]) });
        }
      }
    }
  }

  return Array.from(byPair.values())
    .map((row) => ({
      id: `inferred-${row.from}-${row.to}`,
      from_id: row.from,
      to_id: row.to,
      relation_type: 'related' as RelationType,
      label: `shared: ${Array.from(row.shared).slice(0, 3).join(', ')}`,
      inferred: true,
      score: round3(row.score),
    }))
    .filter((row) => (row.score ?? 0) >= 0.75)
    .sort((a, b) => toFiniteNumber(b.score, 0) - toFiniteNumber(a.score, 0))
    .slice(0, maxEdges);
}

export type McpResult = { content: Array<{ type: string; text: string }> };
