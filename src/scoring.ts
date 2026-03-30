import type {
  Env,
  LinkStats,
  ScoreComponent,
  DynamicScoreBreakdown,
} from './types.js';

import {
  EMPTY_LINK_STATS,
} from './constants.js';

import {
  now,
  toFiniteNumber,
  clampToRange,
  normalizeSourceKey,
} from './utils.js';

import {
  normalizeLinkStats,
  loadLinkStatsMap,
  loadSourceTrustMap,
} from './db.js';

export function clamp01(value: number): number {
  return Math.min(Math.max(value, 0), 1);
}

export function round3(value: number): number {
  return Math.round(value * 1000) / 1000;
}

export function countKeywordHits(haystack: string, terms: string[]): number {
  let count = 0;
  for (const term of terms) {
    if (haystack.includes(term)) count++;
  }
  return count;
}


export function computeDynamicScoreBreakdown(
  memory: Record<string, unknown>,
  rawStats?: Partial<LinkStats>,
  tsNow = now(),
  sourceTrustOverride?: number | null
): DynamicScoreBreakdown {
  const stats = normalizeLinkStats(rawStats);
  const baseConfidence = clamp01(toFiniteNumber(memory.confidence, 0.7));
  const baseImportance = clamp01(toFiniteNumber(memory.importance, 0.5));
  const createdAt = toFiniteNumber(memory.created_at, tsNow);
  const updatedAt = toFiniteNumber(memory.updated_at, createdAt);
  const ageDays = Math.max(0, (tsNow - updatedAt) / 86400);
  const memoryType = typeof memory.type === 'string' ? memory.type.toLowerCase() : '';
  const sourceText = typeof memory.source === 'string' ? memory.source.trim().toLowerCase() : '';
  const textBlob = [
    typeof memory.title === 'string' ? memory.title : '',
    typeof memory.key === 'string' ? memory.key : '',
    typeof memory.content === 'string' ? memory.content : '',
    typeof memory.tags === 'string' ? memory.tags : '',
  ].join(' ').toLowerCase();

  const certaintyHits = countKeywordHits(textBlob, ['verified', 'confirmed', 'exact', 'measured', 'token', 'id', 'official', 'passed']);
  const hedgeHits = countKeywordHits(textBlob, ['maybe', 'might', 'perhaps', 'guess', 'probably', 'vague', 'unsure', 'i think']);
  const importanceHits = countKeywordHits(textBlob, ['goal', 'strategy', 'deadline', 'todo', 'must', 'critical', 'priority', 'plan', 'task', 'decision', 'launch', 'ship']);
  const highSignalSource = sourceText
    ? countKeywordHits(sourceText, ['api', 'system', 'log', 'metric', 'official', 'doc', 'test', 'monitor']) > 0
    : false;
  const lowSignalSource = sourceText
    ? countKeywordHits(sourceText, ['rumor', 'guess', 'hearsay', 'vibe', 'idea']) > 0
    : false;
  const contentLength = textBlob.replace(/\s+/g, '').length;

  const sourceBonus = highSignalSource
    ? 0.09
    : sourceText
      ? 0.04
      : 0;
  const sourcePenalty = lowSignalSource ? 0.07 : 0;
  const sourceTrust = sourceTrustOverride === undefined || sourceTrustOverride === null
    ? null
    : clampToRange(sourceTrustOverride, 0.5);
  const sourceTrustConfidenceDelta = sourceTrust === null
    ? 0
    : (sourceTrust - 0.5) * 0.4;
  const sourceTrustImportanceDelta = sourceTrust === null
    ? 0
    : (sourceTrust - 0.5) * 0.14;
  const certaintySignal = Math.min(0.2, certaintyHits * 0.04);
  const hedgePenalty = Math.min(0.2, hedgeHits * 0.055);
  const importanceKeywordSignal = Math.min(0.24, importanceHits * 0.045);
  const contentDepthSignal = Math.min(0.08, Math.max(0, (contentLength - 80) / 420) * 0.08);
  const typeConfidenceBias = memoryType === 'fact' ? 0.08 : memoryType === 'journal' ? -0.06 : 0;
  const typeImportanceBias = memoryType === 'note' ? 0.04 : memoryType === 'fact' ? 0.02 : 0.01;
  const linkSignal = Math.min(0.18, Math.log1p(stats.link_count) * 0.06);
  const supportSignal = Math.min(0.22, stats.supports_count * 0.05);
  const contradictionPenalty = Math.min(0.28, stats.contradicts_count * 0.09);
  const causeSignal = Math.min(0.14, stats.causes_count * 0.04);
  const exampleSignal = Math.min(0.08, stats.example_of_count * 0.02);
  const supersedeSignal = Math.min(0.08, stats.supersedes_count * 0.02);
  const stalePenalty = Math.min(0.2, ageDays / 365 * 0.16);
  const recencyImportance = ageDays < 3
    ? 0.12
    : ageDays < 14
      ? 0.07
      : ageDays < 60
        ? 0.03
        : -Math.min(0.18, (ageDays - 60) / 365 * 0.18);

  const confidenceComponentsRaw: ScoreComponent[] = [
    { name: 'base_confidence', delta: baseConfidence },
    { name: 'source_bonus', delta: sourceBonus },
    { name: 'source_trust_delta', delta: sourceTrustConfidenceDelta },
    { name: 'certainty_signal', delta: certaintySignal },
    { name: 'type_confidence_bias', delta: typeConfidenceBias },
    { name: 'support_signal', delta: supportSignal },
    { name: 'link_signal', delta: linkSignal * 0.35 },
    { name: 'example_signal', delta: exampleSignal * 0.25 },
    { name: 'contradiction_penalty', delta: -contradictionPenalty },
    { name: 'hedge_penalty', delta: -hedgePenalty },
    { name: 'source_penalty', delta: -sourcePenalty },
    { name: 'stale_penalty', delta: -stalePenalty },
  ];
  const importanceComponentsRaw: ScoreComponent[] = [
    { name: 'base_importance', delta: baseImportance },
    { name: 'importance_keyword_signal', delta: importanceKeywordSignal },
    { name: 'source_trust_delta', delta: sourceTrustImportanceDelta },
    { name: 'content_depth_signal', delta: contentDepthSignal },
    { name: 'type_importance_bias', delta: typeImportanceBias },
    { name: 'link_signal', delta: linkSignal },
    { name: 'cause_signal', delta: causeSignal },
    { name: 'example_signal', delta: exampleSignal },
    { name: 'supersede_signal', delta: supersedeSignal },
    { name: 'recency_signal', delta: recencyImportance },
    { name: 'contradiction_penalty', delta: -(contradictionPenalty * 0.25) },
  ];

  const rawConfidence = confidenceComponentsRaw.reduce((sum, c) => sum + c.delta, 0);
  const rawImportance = importanceComponentsRaw.reduce((sum, c) => sum + c.delta, 0);
  const dynamicConfidence = round3(clamp01(rawConfidence));
  const dynamicImportance = round3(clamp01(rawImportance));

  return {
    score_model: 'memoryvault-dynamic-v1',
    evaluated_at: tsNow,
    memory_type: memoryType || 'unknown',
    source: sourceText || null,
    age_days: round3(ageDays),
    link_stats: stats,
    base_confidence: round3(baseConfidence),
    base_importance: round3(baseImportance),
    raw_confidence: round3(rawConfidence),
    raw_importance: round3(rawImportance),
    dynamic_confidence: dynamicConfidence,
    dynamic_importance: dynamicImportance,
    confidence_components: confidenceComponentsRaw.map((c) => ({ name: c.name, delta: round3(c.delta) })),
    importance_components: importanceComponentsRaw.map((c) => ({ name: c.name, delta: round3(c.delta) })),
    signals: {
      certainty_hits: certaintyHits,
      hedge_hits: hedgeHits,
      importance_hits: importanceHits,
      source_trust: sourceTrust,
      high_signal_source: highSignalSource,
      low_signal_source: lowSignalSource,
      content_length: contentLength,
    },
  };
}

export function computeDynamicScores(
  memory: Record<string, unknown>,
  rawStats?: Partial<LinkStats>,
  tsNow = now(),
  sourceTrustOverride?: number | null
): Record<string, unknown> {
  const breakdown = computeDynamicScoreBreakdown(memory, rawStats, tsNow, sourceTrustOverride);
  return {
    ...breakdown.link_stats,
    dynamic_confidence: breakdown.dynamic_confidence,
    dynamic_importance: breakdown.dynamic_importance,
  };
}

export function enrichMemoryRowsWithDynamics(
  rows: Array<Record<string, unknown>>,
  linkStatsMap: Map<string, LinkStats>,
  tsNow = now(),
  sourceTrustMap?: Map<string, number>
): Array<Record<string, unknown>> {
  return rows.map((row) => {
    const id = typeof row.id === 'string' ? row.id : '';
    const stats = id ? (linkStatsMap.get(id) ?? EMPTY_LINK_STATS) : EMPTY_LINK_STATS;
    const sourceKey = typeof row.source === 'string' ? normalizeSourceKey(row.source) : '';
    const sourceTrust = sourceKey && sourceTrustMap ? sourceTrustMap.get(sourceKey) : undefined;
    return {
      ...row,
      ...computeDynamicScores(row, stats, tsNow, sourceTrust),
    };
  });
}

export function projectMemoryForClient(row: Record<string, unknown>): Record<string, unknown> {
  const baseConfidence = clamp01(toFiniteNumber(row.confidence, 0.7));
  const baseImportance = clamp01(toFiniteNumber(row.importance, 0.5));
  const dynConfidence = clamp01(toFiniteNumber(row.dynamic_confidence, baseConfidence));
  const dynImportance = clamp01(toFiniteNumber(row.dynamic_importance, baseImportance));
  return {
    ...row,
    base_confidence: round3(baseConfidence),
    base_importance: round3(baseImportance),
    confidence: round3(dynConfidence),
    importance: round3(dynImportance),
    dynamic_confidence: round3(dynConfidence),
    dynamic_importance: round3(dynImportance),
  };
}

export async function enrichAndProjectRows(
  env: Env,
  brainId: string,
  rows: Array<Record<string, unknown>>,
  tsNow = now()
): Promise<Array<Record<string, unknown>>> {
  if (!rows.length) return [];
  const linkStatsMap = await loadLinkStatsMap(env, brainId);
  const sourceTrustMap = await loadSourceTrustMap(env, brainId);
  return enrichMemoryRowsWithDynamics(rows, linkStatsMap, tsNow, sourceTrustMap).map(projectMemoryForClient);
}
