import { describe, it, expect } from 'vitest';
import {
  clamp01,
  round3,
  computeDecayPeriods,
  computeDynamicScoreBreakdown,
} from '../src/scoring.js';

const TS_NOW = 1_800_000_000;
const DAY = 86400;

function baseMemory(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: 'mem-1',
    type: 'fact',
    title: 'Server region',
    key: 'server_region',
    content: 'Primary region is us-east-1.',
    tags: 'infra',
    source: 'doc',
    confidence: 0.7,
    importance: 0.5,
    created_at: TS_NOW - 300 * DAY,
    updated_at: TS_NOW - 300 * DAY,
    ...overrides,
  };
}

describe('computeDecayPeriods', () => {
  it('returns 0 when there is no idle time', () => {
    expect(computeDecayPeriods(0, 30)).toBe(0);
    expect(computeDecayPeriods(-5, 30)).toBe(0);
    expect(computeDecayPeriods(Number.NaN, 30)).toBe(0);
  });

  it('returns one period when idle time equals the window', () => {
    expect(computeDecayPeriods(30, 30)).toBe(1);
  });

  it('scales linearly with idle time', () => {
    expect(computeDecayPeriods(60, 30)).toBe(2);
    expect(computeDecayPeriods(45, 30)).toBeCloseTo(1.5, 5);
  });

  it('caps at 3 periods by default', () => {
    expect(computeDecayPeriods(900, 30)).toBe(3);
    expect(computeDecayPeriods(900, 30, 5)).toBe(5);
  });

  it('treats a zero-day window as a single period', () => {
    expect(computeDecayPeriods(10, 0)).toBe(1);
  });
});

describe('computeDynamicScoreBreakdown usage signals', () => {
  it('reports the v2 score model', () => {
    const breakdown = computeDynamicScoreBreakdown(baseMemory(), undefined, TS_NOW);
    expect(breakdown.score_model).toBe('memoryvault-dynamic-v2');
  });

  it('boosts importance and confidence for frequently accessed memories', () => {
    const cold = computeDynamicScoreBreakdown(baseMemory(), undefined, TS_NOW);
    const hot = computeDynamicScoreBreakdown(
      baseMemory({ access_count: 20, last_accessed_at: TS_NOW - 100 * DAY }),
      undefined,
      TS_NOW
    );
    expect(hot.dynamic_importance).toBeGreaterThan(cold.dynamic_importance);
    expect(hot.dynamic_confidence).toBeGreaterThan(cold.dynamic_confidence);

    const usage = hot.importance_components.find((c) => c.name === 'usage_signal');
    expect(usage?.delta).toBeCloseTo(round3(Math.min(0.16, Math.log1p(20) * 0.05)), 3);
  });

  it('counts staleness from the most recent write or recall', () => {
    const neverRecalled = computeDynamicScoreBreakdown(baseMemory(), undefined, TS_NOW);
    const recentlyRecalled = computeDynamicScoreBreakdown(
      baseMemory({ access_count: 1, last_accessed_at: TS_NOW - 1 * DAY }),
      undefined,
      TS_NOW
    );

    expect(recentlyRecalled.activity_age_days).toBeCloseTo(1, 2);
    expect(neverRecalled.activity_age_days).toBeCloseTo(300, 2);

    const stale = (b: typeof neverRecalled) =>
      b.confidence_components.find((c) => c.name === 'stale_penalty')?.delta ?? 0;
    expect(stale(recentlyRecalled)).toBeGreaterThan(stale(neverRecalled));

    const recency = (b: typeof neverRecalled) =>
      b.importance_components.find((c) => c.name === 'recency_signal')?.delta ?? 0;
    expect(recency(recentlyRecalled)).toBeGreaterThan(recency(neverRecalled));
  });

  it('treats never-accessed memories like the write-age-only model', () => {
    const breakdown = computeDynamicScoreBreakdown(baseMemory(), undefined, TS_NOW);
    expect(breakdown.signals.access_count).toBe(0);
    expect(breakdown.signals.recall_age_days).toBeNull();
    expect(breakdown.activity_age_days).toBe(breakdown.age_days);

    const recall = breakdown.importance_components.find((c) => c.name === 'recall_recency_signal');
    expect(recall?.delta).toBe(0);
    const usage = breakdown.importance_components.find((c) => c.name === 'usage_signal');
    expect(usage?.delta).toBe(0);
  });

  it('grants a recall recency bonus that fades with recall age', () => {
    const recallDelta = (daysAgo: number) => {
      const breakdown = computeDynamicScoreBreakdown(
        baseMemory({ access_count: 3, last_accessed_at: TS_NOW - daysAgo * DAY }),
        undefined,
        TS_NOW
      );
      return breakdown.importance_components.find((c) => c.name === 'recall_recency_signal')?.delta ?? 0;
    };
    expect(recallDelta(1)).toBeGreaterThan(recallDelta(10));
    expect(recallDelta(10)).toBeGreaterThan(recallDelta(30));
    expect(recallDelta(90)).toBe(0);
  });

  it('sums components into the raw scores and clamps dynamics to [0, 1]', () => {
    const breakdown = computeDynamicScoreBreakdown(
      baseMemory({ access_count: 500, last_accessed_at: TS_NOW }),
      { link_count: 10, supports_count: 8 },
      TS_NOW
    );
    const confSum = breakdown.confidence_components.reduce((sum, c) => sum + c.delta, 0);
    const impSum = breakdown.importance_components.reduce((sum, c) => sum + c.delta, 0);
    expect(breakdown.raw_confidence).toBeCloseTo(round3(confSum), 2);
    expect(breakdown.raw_importance).toBeCloseTo(round3(impSum), 2);
    expect(breakdown.dynamic_confidence).toBe(clamp01(breakdown.dynamic_confidence));
    expect(breakdown.dynamic_importance).toBe(clamp01(breakdown.dynamic_importance));
    expect(breakdown.dynamic_confidence).toBeLessThanOrEqual(1);
    expect(breakdown.dynamic_importance).toBeLessThanOrEqual(1);
  });

  it('supports counterfactual evaluation timestamps after a recall', () => {
    const lastAccess = TS_NOW - 10 * DAY;
    const beforeAccess = computeDynamicScoreBreakdown(
      baseMemory({ access_count: 1, last_accessed_at: lastAccess }),
      undefined,
      lastAccess - 5 * DAY
    );
    expect(Number.isFinite(beforeAccess.activity_age_days)).toBe(true);
    expect(beforeAccess.activity_age_days).toBeGreaterThanOrEqual(0);
  });
});
