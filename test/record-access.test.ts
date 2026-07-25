import { describe, it, expect } from 'vitest';
import { recordMemoryAccess } from '../src/db.js';
import type { Env } from '../src/types.js';

type CapturedCall = { sql: string; args: unknown[] };

function makeFakeEnv(captured: CapturedCall[], failWith?: Error): Env {
  return {
    DB: {
      prepare(sql: string) {
        return {
          bind(...args: unknown[]) {
            return {
              async run() {
                if (failWith) throw failWith;
                captured.push({ sql, args });
                return { success: true };
              },
            };
          },
        };
      },
    },
  } as unknown as Env;
}

describe('recordMemoryAccess', () => {
  it('does nothing for empty or blank id lists', async () => {
    const calls: CapturedCall[] = [];
    await recordMemoryAccess(makeFakeEnv(calls), 'brain-1', [], 1000);
    await recordMemoryAccess(makeFakeEnv(calls), 'brain-1', ['', '   '], 1000);
    expect(calls).toHaveLength(0);
  });

  it('deduplicates ids and binds timestamp, brain, and ids', async () => {
    const calls: CapturedCall[] = [];
    await recordMemoryAccess(makeFakeEnv(calls), 'brain-1', ['a', 'b', 'a', ' b '], 1234);
    expect(calls).toHaveLength(1);
    expect(calls[0].sql).toContain('access_count = access_count + 1');
    expect(calls[0].sql).toContain('last_accessed_at = ?');
    expect(calls[0].args).toEqual([1234, 'brain-1', 'a', 'b']);
  });

  it('chunks large id lists into multiple statements', async () => {
    const calls: CapturedCall[] = [];
    const ids = Array.from({ length: 120 }, (_, i) => `id-${i}`);
    await recordMemoryAccess(makeFakeEnv(calls), 'brain-1', ids, 1000);
    expect(calls).toHaveLength(3);
    const boundIds = calls.flatMap((c) => c.args.slice(2));
    expect(boundIds).toHaveLength(120);
    expect(new Set(boundIds).size).toBe(120);
  });

  it('never throws when the database write fails', async () => {
    const calls: CapturedCall[] = [];
    const env = makeFakeEnv(calls, new Error('D1 unavailable'));
    await expect(recordMemoryAccess(env, 'brain-1', ['a'], 1000)).resolves.toBeUndefined();
  });
});
