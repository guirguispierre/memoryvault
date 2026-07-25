import { toFiniteNumber } from './utils.js';
import { clamp01, round3 } from './scoring.js';

const LEXICAL_FIELD_WEIGHTS: Array<{ field: string; weight: number }> = [
  { field: 'title', weight: 3 },
  { field: 'key', weight: 2.5 },
  { field: 'tags', weight: 1.5 },
  { field: 'source', weight: 1.2 },
  { field: 'content', weight: 1 },
];

export function tokenizeQuery(query: string): string[] {
  return Array.from(new Set(
    query
      .toLowerCase()
      .split(/[^a-z0-9]+/g)
      .map((token) => token.trim())
      .filter((token) => token.length >= 2)
  )).slice(0, 24);
}

export function scoreLexicalRelevance(row: Record<string, unknown>, phrase: string, tokens: string[]): number {
  let score = 0;
  for (const { field, weight } of LEXICAL_FIELD_WEIGHTS) {
    const value = row[field];
    const text = typeof value === 'string' ? value.toLowerCase() : '';
    if (!text) continue;
    if (phrase && text.includes(phrase)) score += weight * 0.8;
    if (tokens.length) {
      let hits = 0;
      for (const token of tokens) {
        if (text.includes(token)) hits++;
      }
      score += weight * (hits / tokens.length);
    }
  }
  return score;
}

// guirguispierre 2026-07-24: lexical candidates were previously ranked by recency, not relevance, which skewed rank fusion.
export function rankLexicalRows(
  rows: Array<Record<string, unknown>>,
  query: string
): Array<Record<string, unknown>> {
  if (rows.length < 2) return rows;
  const phrase = query.trim().toLowerCase();
  const tokens = tokenizeQuery(phrase);
  return rows
    .map((row) => ({ row, score: scoreLexicalRelevance(row, phrase, tokens) }))
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return toFiniteNumber(b.row.updated_at, 0) - toFiniteNumber(a.row.updated_at, 0);
    })
    .map((entry) => entry.row);
}

export function estimateTokens(text: string): number {
  return Math.max(1, Math.ceil(text.length / 4));
}

export type ContextPackEntry = {
  id: string;
  text: string;
  utility: number;
  tokens: number;
};

export type PackedContextEntry = {
  id: string;
  text: string;
  tokens: number;
  truncated: boolean;
  utility: number;
};

export type PackedContext = {
  selected: PackedContextEntry[];
  used_tokens: number;
  skipped_over_budget: number;
};

// guirguispierre 2026-07-24: greedy by utility; one partial (truncated) entry allowed when >=120 tokens remain.
export function packContextEntries(
  entries: ContextPackEntry[],
  tokenBudget: number,
  maxEntries: number
): PackedContext {
  const sorted = [...entries].sort((a, b) => b.utility - a.utility);
  const selected: PackedContextEntry[] = [];
  let used = 0;
  let skipped = 0;
  for (const entry of sorted) {
    if (selected.length >= maxEntries) {
      skipped++;
      continue;
    }
    const remaining = tokenBudget - used;
    if (remaining <= 0) {
      skipped++;
      continue;
    }
    if (entry.tokens <= remaining) {
      selected.push({ id: entry.id, text: entry.text, tokens: entry.tokens, truncated: false, utility: entry.utility });
      used += entry.tokens;
    } else if (remaining >= 120) {
      const text = `${entry.text.slice(0, Math.max(0, remaining * 4 - 2))}…`;
      const tokens = estimateTokens(text);
      selected.push({ id: entry.id, text, tokens, truncated: true, utility: entry.utility });
      used += tokens;
    } else {
      skipped++;
    }
  }
  return { selected, used_tokens: used, skipped_over_budget: skipped };
}

export type SearchRankingContext = {
  fusedScores: Map<string, number>;
  supersededBy: Map<string, string[]>;
  conflictLosers: Set<string>;
};

export function rankSearchResults(
  projectedRows: Array<Record<string, unknown>>,
  context: SearchRankingContext,
  limit: number
): Array<Record<string, unknown>> {
  if (!projectedRows.length) return [];
  const fusedValues = projectedRows.map((row) => context.fusedScores.get(String(row.id ?? '')) ?? 0);
  const maxFused = Math.max(...fusedValues);

  const scored = projectedRows.map((row) => {
    const id = String(row.id ?? '');
    const fused = context.fusedScores.get(id) ?? 0;
    // guirguispierre 2026-07-24: max-relative, not min-max — adjacent RRF ranks must not span the whole [0,1] range.
    const fusedNorm = maxFused > 0 ? fused / maxFused : 0;
    const importance = clamp01(toFiniteNumber(row.importance, 0.5));
    const confidence = clamp01(toFiniteNumber(row.confidence, 0.7));
    const supersededBy = context.supersededBy.get(id) ?? [];
    const conflictLoser = context.conflictLosers.has(id);
    let relevance = 0.75 * fusedNorm + 0.25 * (0.6 * importance + 0.4 * confidence);
    if (supersededBy.length) relevance -= 0.2;
    if (conflictLoser) relevance -= 0.25;
    relevance = Math.max(0, relevance);
    const retrieval: Record<string, unknown> = {
      fused_score: round3(fused),
      relevance_score: round3(relevance),
    };
    if (supersededBy.length) retrieval.superseded_by = supersededBy;
    if (conflictLoser) retrieval.conflict_loser = true;
    const annotated: Record<string, unknown> = { ...row, retrieval };
    return { row: annotated, relevance, updatedAt: toFiniteNumber(row.updated_at, 0) };
  });

  return scored
    .sort((a, b) => {
      if (b.relevance !== a.relevance) return b.relevance - a.relevance;
      return b.updatedAt - a.updatedAt;
    })
    .slice(0, limit)
    .map((entry) => entry.row);
}
