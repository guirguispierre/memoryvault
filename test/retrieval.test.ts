import { describe, it, expect } from 'vitest';
import {
  tokenizeQuery,
  scoreLexicalRelevance,
  rankLexicalRows,
  rankSearchResults,
} from '../src/retrieval.js';
import { fuseSearchCandidates } from '../src/vectorize.js';

function row(id: string, overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id,
    type: 'note',
    title: null,
    key: null,
    content: '',
    tags: null,
    source: null,
    confidence: 0.7,
    importance: 0.5,
    created_at: 1000,
    updated_at: 1000,
    ...overrides,
  };
}

describe('tokenizeQuery', () => {
  it('lowercases, splits, dedupes, and drops single characters', () => {
    expect(tokenizeQuery('Deploy the API, deploy it!')).toEqual(['deploy', 'the', 'api', 'it']);
  });
});

describe('rankLexicalRows', () => {
  it('ranks a title match above a content-only match regardless of recency', () => {
    const titleMatch = row('title-match', { title: 'Deploy checklist', updated_at: 1000 });
    const contentMatch = row('content-match', { content: 'notes mention deploy once', updated_at: 9999 });
    const ranked = rankLexicalRows([contentMatch, titleMatch], 'deploy');
    expect(ranked.map((r) => r.id)).toEqual(['title-match', 'content-match']);
  });

  it('rewards exact phrase hits over scattered token hits', () => {
    const phrase = row('phrase', { content: 'the deploy checklist lives here' });
    const scattered = row('scattered', { content: 'checklist for turtles; deploy tomorrow' });
    const ranked = rankLexicalRows([scattered, phrase], 'deploy checklist');
    expect(ranked[0].id).toBe('phrase');
  });

  it('breaks relevance ties by recency', () => {
    const older = row('older', { content: 'deploy', updated_at: 1000 });
    const newer = row('newer', { content: 'deploy', updated_at: 2000 });
    const ranked = rankLexicalRows([older, newer], 'deploy');
    expect(ranked[0].id).toBe('newer');
  });

  it('gives zero score to rows without any hit', () => {
    expect(scoreLexicalRelevance(row('x', { content: 'nothing relevant' }), 'zebra', ['zebra'])).toBe(0);
  });
});

describe('fuseSearchCandidates', () => {
  it('returns every candidate with fused scores, sorted descending', () => {
    const a = row('a');
    const b = row('b');
    const c = row('c');
    const fused = fuseSearchCandidates(
      'hybrid',
      [a, b],
      [c, a],
      [{ memory_id: 'c', score: 0.9, rank: 1 }, { memory_id: 'a', score: 0.5, rank: 2 }]
    );
    expect(fused).toHaveLength(3);
    const scores = fused.map((f) => f.fused_score);
    expect([...scores].sort((x, y) => y - x)).toEqual(scores);
    const ids = fused.map((f) => String(f.row.id));
    expect(new Set(ids)).toEqual(new Set(['a', 'b', 'c']));
  });

  it('uses only lexical rank in lexical mode', () => {
    const fused = fuseSearchCandidates('lexical', [row('a'), row('b')], [], []);
    expect(fused[0].row.id).toBe('a');
    expect(fused[0].fused_score).toBeGreaterThan(fused[1].fused_score);
    expect(fused[0].semantic_rank).toBeNull();
  });
});

describe('rankSearchResults', () => {
  const emptyContext = () => ({
    fusedScores: new Map<string, number>(),
    supersededBy: new Map<string, string[]>(),
    conflictLosers: new Set<string>(),
  });

  it('lets dynamic scores reorder retrieval ties', () => {
    const context = emptyContext();
    context.fusedScores.set('low', 0.02);
    context.fusedScores.set('high', 0.02);
    const ranked = rankSearchResults(
      [row('low', { importance: 0.2, confidence: 0.4 }), row('high', { importance: 0.9, confidence: 0.9 })],
      context,
      10
    );
    expect(ranked.map((r) => r.id)).toEqual(['high', 'low']);
  });

  it('keeps clearly better retrieval matches ahead of higher-importance weak matches', () => {
    const context = emptyContext();
    context.fusedScores.set('strong-match', 0.1);
    context.fusedScores.set('weak-match', 0.001);
    const ranked = rankSearchResults(
      [row('strong-match', { importance: 0.3 }), row('weak-match', { importance: 0.9 })],
      context,
      10
    );
    expect(ranked[0].id).toBe('strong-match');
  });

  it('down-ranks superseded memories and annotates them', () => {
    const context = emptyContext();
    context.fusedScores.set('old', 0.05);
    context.fusedScores.set('new', 0.04);
    context.supersededBy.set('old', ['new']);
    const ranked = rankSearchResults([row('old'), row('new')], context, 10);
    expect(ranked[0].id).toBe('new');
    const oldRow = ranked.find((r) => r.id === 'old');
    expect((oldRow?.retrieval as Record<string, unknown>).superseded_by).toEqual(['new']);
  });

  it('down-ranks conflict losers and annotates them', () => {
    const context = emptyContext();
    context.fusedScores.set('loser', 0.05);
    context.fusedScores.set('canonical', 0.04);
    context.conflictLosers.add('loser');
    const ranked = rankSearchResults([row('loser'), row('canonical')], context, 10);
    expect(ranked[0].id).toBe('canonical');
    const loserRow = ranked.find((r) => r.id === 'loser');
    expect((loserRow?.retrieval as Record<string, unknown>).conflict_loser).toBe(true);
  });

  it('respects the limit and attaches retrieval metadata to every result', () => {
    const context = emptyContext();
    ['a', 'b', 'c'].forEach((id, i) => context.fusedScores.set(id, 0.1 - i * 0.01));
    const ranked = rankSearchResults([row('a'), row('b'), row('c')], context, 2);
    expect(ranked).toHaveLength(2);
    for (const r of ranked) {
      const retrieval = r.retrieval as Record<string, unknown>;
      expect(typeof retrieval.fused_score).toBe('number');
      expect(typeof retrieval.relevance_score).toBe('number');
    }
  });

  it('handles a single candidate without dividing by zero', () => {
    const context = emptyContext();
    context.fusedScores.set('only', 0.05);
    const ranked = rankSearchResults([row('only')], context, 10);
    expect(ranked).toHaveLength(1);
    expect((ranked[0].retrieval as Record<string, unknown>).relevance_score).toBeGreaterThan(0);
  });
});
