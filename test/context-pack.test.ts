import { describe, it, expect } from 'vitest';
import { estimateTokens, packContextEntries } from '../src/retrieval.js';
import type { ContextPackEntry } from '../src/retrieval.js';

function entry(id: string, utility: number, chars: number): ContextPackEntry {
  const text = 'x'.repeat(chars);
  return { id, text, utility, tokens: estimateTokens(text) };
}

describe('estimateTokens', () => {
  it('approximates four characters per token with a floor of one', () => {
    expect(estimateTokens('')).toBe(1);
    expect(estimateTokens('abcd')).toBe(1);
    expect(estimateTokens('abcde')).toBe(2);
    expect(estimateTokens('x'.repeat(400))).toBe(100);
  });
});

describe('packContextEntries', () => {
  it('selects entries in utility order until the budget is spent', () => {
    const packed = packContextEntries(
      [entry('low', 0.2, 400), entry('high', 0.9, 400), entry('mid', 0.5, 400)],
      250,
      10
    );
    expect(packed.selected.map((e) => e.id)).toEqual(['high', 'mid']);
    expect(packed.used_tokens).toBe(200);
    expect(packed.skipped_over_budget).toBe(1);
  });

  it('respects the max entry count even with budget remaining', () => {
    const packed = packContextEntries(
      [entry('a', 0.9, 40), entry('b', 0.8, 40), entry('c', 0.7, 40)],
      1000,
      2
    );
    expect(packed.selected).toHaveLength(2);
    expect(packed.skipped_over_budget).toBe(1);
  });

  it('packs whole entries first, then truncates one partial when at least 120 tokens remain', () => {
    const packed = packContextEntries(
      [entry('fits-whole', 0.9, 1600), entry('gets-truncated', 0.8, 4000)],
      700,
      10
    );
    expect(packed.selected.map((e) => e.id)).toEqual(['fits-whole', 'gets-truncated']);
    expect(packed.selected[0].truncated).toBe(false);
    expect(packed.selected[1].truncated).toBe(true);
    expect(packed.selected[1].text.endsWith('…')).toBe(true);
    expect(packed.used_tokens).toBeLessThanOrEqual(700);
  });

  it('truncates the top entry itself when it alone exceeds the budget', () => {
    const packed = packContextEntries([entry('giant', 0.9, 8000)], 300, 10);
    expect(packed.selected).toHaveLength(1);
    expect(packed.selected[0].truncated).toBe(true);
    expect(packed.selected[0].text.endsWith('…')).toBe(true);
    expect(packed.selected[0].tokens).toBeLessThanOrEqual(300);
    expect(packed.used_tokens).toBeLessThanOrEqual(300);
  });

  it('skips instead of truncating when fewer than 120 tokens remain', () => {
    const packed = packContextEntries(
      [entry('fits', 0.9, 1800), entry('too-big', 0.8, 2000)],
      500,
      10
    );
    expect(packed.selected.map((e) => e.id)).toEqual(['fits']);
    expect(packed.skipped_over_budget).toBe(1);
  });

  it('returns empty output for empty input', () => {
    const packed = packContextEntries([], 1000, 10);
    expect(packed.selected).toHaveLength(0);
    expect(packed.used_tokens).toBe(0);
  });
});
