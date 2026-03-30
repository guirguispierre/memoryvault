import type {
  Env,
  MemorySearchMode,
  MemoryType,
  SemanticMemoryCandidate,
  VectorSyncStats,
} from './types.js';

import {
  EMBEDDING_MODEL,
  VECTORIZE_QUERY_TOP_K_MAX,
  VECTORIZE_UPSERT_BATCH_SIZE,
  VECTORIZE_DELETE_BATCH_SIZE,
  VECTORIZE_SETTLE_POLL_INTERVAL_MS,
  EMBEDDING_BATCH_SIZE,
  MEMORY_SEARCH_FUSION_K,
  VECTOR_ID_PREFIX,
  VECTOR_ID_MAX_MEMORY_ID_LENGTH,
} from './constants.js';

import {
  parseTags,
  hasSemanticSearchBindings,
  normalizeSemanticScore,
  truncateForMetadata,
  toFiniteNumber,
} from './utils.js';

import { sha256DigestBase64Url } from './crypto.js';

import { loadMemoryRowsByIds } from './db.js';

export function buildMemoryEmbeddingText(memory: Record<string, unknown>): string {
  const parts: string[] = [];
  if (typeof memory.type === 'string' && memory.type.trim()) parts.push(`type: ${memory.type.trim()}`);
  if (typeof memory.title === 'string' && memory.title.trim()) parts.push(`title: ${memory.title.trim()}`);
  if (typeof memory.key === 'string' && memory.key.trim()) parts.push(`key: ${memory.key.trim()}`);
  if (typeof memory.source === 'string' && memory.source.trim()) parts.push(`source: ${memory.source.trim()}`);
  const tagList = parseTags(memory.tags);
  if (tagList.length) parts.push(`tags: ${tagList.join(', ')}`);
  if (typeof memory.content === 'string' && memory.content.trim()) parts.push(`content: ${memory.content.trim()}`);
  if (!parts.length) return '';
  return parts.join('\n').slice(0, 8000);
}

export function extractEmbeddingList(response: unknown): number[][] {
  const payload = response as { data?: unknown };
  if (!Array.isArray(payload?.data)) {
    throw new Error('Embedding response missing data array.');
  }
  const vectors: number[][] = [];
  for (const row of payload.data) {
    if (!Array.isArray(row)) {
      throw new Error('Embedding response contained a non-vector entry.');
    }
    const vector: number[] = [];
    for (const value of row) {
      const num = Number(value);
      if (!Number.isFinite(num)) throw new Error('Embedding response contained a non-numeric value.');
      vector.push(num);
    }
    vectors.push(vector);
  }
  return vectors;
}

export async function embedTexts(env: Env, texts: string[]): Promise<number[][]> {
  if (!hasSemanticSearchBindings(env)) return [];
  if (!texts.length) return [];
  const result = await env.AI.run(EMBEDDING_MODEL, { text: texts });
  return extractEmbeddingList(result);
}

export async function makeLegacyVectorId(brainId: string, memoryId: string): Promise<string> {
  const digest = await sha256DigestBase64Url(`${brainId}:${memoryId}`);
  return `m_${digest}`;
}

export async function makeVectorId(brainId: string, memoryId: string): Promise<string> {
  const normalized = memoryId.trim();
  if (normalized.length > 0 && normalized.length <= VECTOR_ID_MAX_MEMORY_ID_LENGTH) {
    return `${VECTOR_ID_PREFIX}${normalized}`;
  }
  return makeLegacyVectorId(brainId, memoryId);
}

export function parseMemoryIdFromVectorId(vectorId: string): string {
  if (!vectorId.startsWith(VECTOR_ID_PREFIX)) return '';
  return vectorId.slice(VECTOR_ID_PREFIX.length).trim();
}

export function looksLikeMemoryId(value: string): boolean {
  return /^[a-z0-9-]{16,}$/i.test(value.trim());
}

export function buildMemoryVectorMetadata(brainId: string, memory: Record<string, unknown>): Record<string, string | number | boolean> {
  const memoryId = typeof memory.id === 'string' ? memory.id : '';
  const metadata: Record<string, string | number | boolean> = {
    brain_id: brainId,
    memory_id: memoryId,
  };
  if (typeof memory.type === 'string' && memory.type.trim()) metadata.type = truncateForMetadata(memory.type, 32);
  if (typeof memory.key === 'string' && memory.key.trim()) metadata.key = truncateForMetadata(memory.key, 96);
  if (typeof memory.title === 'string' && memory.title.trim()) metadata.title = truncateForMetadata(memory.title, 120);
  if (typeof memory.source === 'string' && memory.source.trim()) metadata.source = truncateForMetadata(memory.source, 120);
  if (typeof memory.tags === 'string' && memory.tags.trim()) metadata.tags = truncateForMetadata(memory.tags, 200);
  if (typeof memory.created_at === 'number' && Number.isFinite(memory.created_at)) metadata.created_at = Math.floor(memory.created_at);
  if (typeof memory.updated_at === 'number' && Number.isFinite(memory.updated_at)) metadata.updated_at = Math.floor(memory.updated_at);
  metadata.archived = memory.archived_at !== null && memory.archived_at !== undefined;
  return metadata;
}

export function extractVectorMutationId(result: unknown): string {
  if (!result || typeof result !== 'object' || Array.isArray(result)) return '';
  const value = result as Record<string, unknown>;
  if (typeof value.mutationId === 'string' && value.mutationId.trim()) return value.mutationId.trim();
  if (typeof value.mutation_id === 'string' && value.mutation_id.trim()) return value.mutation_id.trim();
  return '';
}

export function sleepMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function readVectorizeProcessedMutation(env: Env): Promise<string> {
  if (!env.MEMORY_INDEX) return '';
  try {
    const details = await env.MEMORY_INDEX.describe() as unknown as {
      processedUpToMutation?: unknown;
      processed_up_to_mutation?: unknown;
    };
    if (typeof details.processedUpToMutation === 'string') return details.processedUpToMutation.trim();
    if (typeof details.processed_up_to_mutation === 'string') return details.processed_up_to_mutation.trim();
    return '';
  } catch (err) {
    console.warn('[semantic-index:describe]', err);
    return '';
  }
}

export async function waitForVectorMutationReady(
  env: Env,
  mutationId: string,
  timeoutSeconds: number
): Promise<{ ready: boolean; attempts: number; elapsed_ms: number; processed_up_to_mutation: string | null }> {
  const target = mutationId.trim();
  if (!target) {
    return { ready: false, attempts: 0, elapsed_ms: 0, processed_up_to_mutation: null };
  }
  const timeoutMs = Math.max(1, Math.floor(timeoutSeconds * 1000));
  const startedAt = Date.now();
  let attempts = 0;
  let processed = '';
  while (Date.now() - startedAt <= timeoutMs) {
    attempts += 1;
    processed = await readVectorizeProcessedMutation(env);
    if (processed === target) {
      return { ready: true, attempts, elapsed_ms: Date.now() - startedAt, processed_up_to_mutation: processed || null };
    }
    if (Date.now() - startedAt >= timeoutMs) break;
    await sleepMs(VECTORIZE_SETTLE_POLL_INTERVAL_MS);
  }
  return { ready: false, attempts, elapsed_ms: Date.now() - startedAt, processed_up_to_mutation: processed || null };
}

export async function waitForVectorQueryReady(
  env: Env,
  brainId: string,
  vectorId: string,
  timeoutSeconds: number
): Promise<{ ready: boolean; attempts: number; elapsed_ms: number; processed_up_to_mutation: string | null }> {
  const targetVectorId = vectorId.trim();
  if (!targetVectorId) {
    return { ready: false, attempts: 0, elapsed_ms: 0, processed_up_to_mutation: null };
  }
  const indexMaybe = env.MEMORY_INDEX as unknown as {
    queryById?: (vectorId: string, options?: Record<string, unknown>) => Promise<unknown>;
  };
  if (typeof indexMaybe.queryById !== 'function') {
    return { ready: false, attempts: 0, elapsed_ms: 0, processed_up_to_mutation: null };
  }
  const timeoutMs = Math.max(1, Math.floor(timeoutSeconds * 1000));
  const startedAt = Date.now();
  let attempts = 0;
  let processed = '';
  while (Date.now() - startedAt <= timeoutMs) {
    attempts += 1;
    try {
      const queryResult = await indexMaybe.queryById(targetVectorId, {
        topK: 1,
        namespace: brainId,
        returnMetadata: 'none',
        returnValues: false,
      });
      const payload = queryResult as { matches?: unknown[]; results?: unknown[] };
      const matches = Array.isArray(payload.matches)
        ? payload.matches
        : (Array.isArray(payload.results) ? payload.results : []);
      if (matches.length > 0) {
        return {
          ready: true,
          attempts,
          elapsed_ms: Date.now() - startedAt,
          processed_up_to_mutation: processed || null,
        };
      }
    } catch (err) {
      console.warn('[semantic-index:query-by-id]', err);
    }
    processed = await readVectorizeProcessedMutation(env);
    if (Date.now() - startedAt >= timeoutMs) break;
    await sleepMs(VECTORIZE_SETTLE_POLL_INTERVAL_MS);
  }
  return { ready: false, attempts, elapsed_ms: Date.now() - startedAt, processed_up_to_mutation: processed || null };
}

export async function deleteMemoryVectors(
  env: Env,
  brainId: string,
  memoryIds: string[]
): Promise<{ deleted: number; mutation_ids: string[] }> {
  if (!hasSemanticSearchBindings(env)) return { deleted: 0, mutation_ids: [] };
  const uniqueIds = Array.from(new Set(memoryIds.map((id) => id.trim()).filter(Boolean)));
  if (!uniqueIds.length) return { deleted: 0, mutation_ids: [] };
  const vectorIdsCurrent = await Promise.all(uniqueIds.map((id) => makeVectorId(brainId, id)));
  const vectorIdsLegacy = await Promise.all(uniqueIds.map((id) => makeLegacyVectorId(brainId, id)));
  const vectorIds = Array.from(new Set([...vectorIdsCurrent, ...vectorIdsLegacy]));
  const mutationIds: string[] = [];
  for (let i = 0; i < vectorIds.length; i += VECTORIZE_DELETE_BATCH_SIZE) {
    const mutation = await env.MEMORY_INDEX.deleteByIds(vectorIds.slice(i, i + VECTORIZE_DELETE_BATCH_SIZE));
    const mutationId = extractVectorMutationId(mutation);
    if (mutationId) mutationIds.push(mutationId);
  }
  return { deleted: uniqueIds.length, mutation_ids: Array.from(new Set(mutationIds)) };
}

export async function syncMemoriesToVectorIndex(
  env: Env,
  brainId: string,
  memories: Array<Record<string, unknown>>
): Promise<VectorSyncStats> {
  if (!hasSemanticSearchBindings(env) || !memories.length) {
    return { upserted: 0, deleted: 0, skipped: memories.length, mutation_ids: [], probe_vector_id: null };
  }

  const toDeleteIds: string[] = [];
  const embeddable: Array<{
    memory_id: string;
    text: string;
    metadata: Record<string, string | number | boolean>;
  }> = [];
  let skipped = 0;
  const mutationIds: string[] = [];
  let probeVectorId: string | null = null;

  for (const memory of memories) {
    const memoryId = typeof memory.id === 'string' ? memory.id.trim() : '';
    if (!memoryId) {
      skipped++;
      continue;
    }
    if (memory.archived_at !== null && memory.archived_at !== undefined) {
      toDeleteIds.push(memoryId);
      continue;
    }
    const text = buildMemoryEmbeddingText(memory);
    if (!text) {
      toDeleteIds.push(memoryId);
      continue;
    }
    embeddable.push({
      memory_id: memoryId,
      text,
      metadata: buildMemoryVectorMetadata(brainId, memory),
    });
  }

  let deleted = 0;
  if (toDeleteIds.length) {
    const deleteStats = await deleteMemoryVectors(env, brainId, toDeleteIds);
    deleted = deleteStats.deleted;
    mutationIds.push(...deleteStats.mutation_ids);
  }

  let upserted = 0;
  for (let i = 0; i < embeddable.length; i += EMBEDDING_BATCH_SIZE) {
    const chunk = embeddable.slice(i, i + EMBEDDING_BATCH_SIZE);
    const embeddings = await embedTexts(env, chunk.map((entry) => entry.text));
    if (embeddings.length !== chunk.length) {
      throw new Error(`Embedding count mismatch. Expected ${chunk.length}, got ${embeddings.length}.`);
    }
    const vectors: Array<{ id: string; namespace: string; values: number[]; metadata: Record<string, string | number | boolean> }> = [];
    const legacyIdsToDelete: string[] = [];
    for (let idx = 0; idx < chunk.length; idx++) {
      const entry = chunk[idx];
      const vectorId = await makeVectorId(brainId, entry.memory_id);
      const legacyId = await makeLegacyVectorId(brainId, entry.memory_id);
      if (legacyId !== vectorId) legacyIdsToDelete.push(legacyId);
      vectors.push({
        id: vectorId,
        namespace: brainId,
        values: embeddings[idx],
        metadata: entry.metadata,
      });
    }
    if (legacyIdsToDelete.length) {
      for (let j = 0; j < legacyIdsToDelete.length; j += VECTORIZE_DELETE_BATCH_SIZE) {
        const legacyDeleteMutation = await env.MEMORY_INDEX.deleteByIds(legacyIdsToDelete.slice(j, j + VECTORIZE_DELETE_BATCH_SIZE));
        const legacyDeleteMutationId = extractVectorMutationId(legacyDeleteMutation);
        if (legacyDeleteMutationId) mutationIds.push(legacyDeleteMutationId);
      }
    }
    for (let j = 0; j < vectors.length; j += VECTORIZE_UPSERT_BATCH_SIZE) {
      const upsertChunk = vectors.slice(j, j + VECTORIZE_UPSERT_BATCH_SIZE);
      const upsertMutation = await env.MEMORY_INDEX.upsert(upsertChunk);
      const upsertMutationId = extractVectorMutationId(upsertMutation);
      if (upsertMutationId) mutationIds.push(upsertMutationId);
      const lastVector = upsertChunk[upsertChunk.length - 1];
      if (lastVector?.id) probeVectorId = lastVector.id;
    }
    upserted += vectors.length;
  }

  return {
    upserted,
    deleted,
    skipped,
    mutation_ids: Array.from(new Set(mutationIds)),
    probe_vector_id: probeVectorId,
  };
}

export async function safeSyncMemoriesToVectorIndex(
  env: Env,
  brainId: string,
  memories: Array<Record<string, unknown>>,
  operation: string
): Promise<void> {
  if (!hasSemanticSearchBindings(env) || !memories.length) return;
  try {
    await syncMemoriesToVectorIndex(env, brainId, memories);
  } catch (err) {
    console.warn(`[semantic-sync:${operation}]`, err);
  }
}

export async function safeDeleteMemoryVectors(env: Env, brainId: string, memoryIds: string[], operation: string): Promise<void> {
  if (!hasSemanticSearchBindings(env) || !memoryIds.length) return;
  try {
    await deleteMemoryVectors(env, brainId, memoryIds);
  } catch (err) {
    console.warn(`[semantic-delete:${operation}]`, err);
  }
}

export async function querySemanticMemoryCandidates(
  env: Env,
  brainId: string,
  query: string,
  topK: number,
  minScore: number
): Promise<SemanticMemoryCandidate[]> {
  if (!hasSemanticSearchBindings(env)) return [];
  const [queryEmbedding] = await embedTexts(env, [query.trim()]);
  if (!queryEmbedding) return [];
  const matches = await env.MEMORY_INDEX.query(queryEmbedding, {
    topK: Math.min(Math.max(topK, 1), VECTORIZE_QUERY_TOP_K_MAX),
    namespace: brainId,
    returnMetadata: 'all',
    returnValues: false,
  });
  const matchesAny = matches as unknown as { matches?: unknown[]; results?: unknown[] };
  const matchesArray = Array.isArray(matchesAny.matches)
    ? matchesAny.matches
    : (Array.isArray(matchesAny.results) ? matchesAny.results : []);
  const deduped = new Map<string, SemanticMemoryCandidate>();
  let rank = 0;
  for (const rawMatch of matchesArray) {
    if (!rawMatch || typeof rawMatch !== 'object' || Array.isArray(rawMatch)) continue;
    const match = rawMatch as Record<string, unknown>;
    rank += 1;
    const score = toFiniteNumber(match.score ?? match.similarity ?? match.distance, 0);
    if (score < minScore) continue;
    const vectorId = typeof match.id === 'string'
      ? match.id
      : (typeof match.vectorId === 'string'
        ? match.vectorId
        : (typeof match.vector_id === 'string' ? match.vector_id : ''));
    const fromVectorId = vectorId ? parseMemoryIdFromVectorId(vectorId) : '';
    const metadata = typeof match.metadata === 'object' && match.metadata !== null
      ? match.metadata as Record<string, unknown>
      : (typeof match.meta === 'object' && match.meta !== null
        ? match.meta as Record<string, unknown>
        : null);
    const fromMetadata = metadata && typeof metadata.memory_id === 'string'
      ? metadata.memory_id.trim()
      : (metadata && typeof metadata.memoryId === 'string' ? metadata.memoryId.trim() : '');
    const fromRawVectorId = fromVectorId
      ? fromVectorId
      : (vectorId && looksLikeMemoryId(vectorId) ? vectorId.trim() : '');
    const memoryId = fromRawVectorId || fromMetadata;
    if (!memoryId) continue;
    const metadataBrainId = metadata && typeof metadata.brain_id === 'string'
      ? metadata.brain_id.trim()
      : (metadata && typeof metadata.brainId === 'string' ? metadata.brainId.trim() : '');
    if (metadataBrainId && metadataBrainId !== brainId) continue;
    const existing = deduped.get(memoryId);
    if (!existing || score > existing.score) {
      deduped.set(memoryId, { memory_id: memoryId, score, rank });
    }
  }
  return Array.from(deduped.values()).sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return a.rank - b.rank;
  });
}


export function fuseSearchRows(
  mode: MemorySearchMode,
  lexicalRows: Record<string, unknown>[],
  semanticRows: Record<string, unknown>[],
  semanticCandidates: SemanticMemoryCandidate[],
  limit: number
): Record<string, unknown>[] {
  const rowById = new Map<string, Record<string, unknown>>();
  const lexicalRank = new Map<string, number>();
  const semanticRank = new Map<string, number>();
  const semanticScore = new Map<string, number>();

  lexicalRows.forEach((row, idx) => {
    const id = typeof row.id === 'string' ? row.id : '';
    if (!id) return;
    rowById.set(id, row);
    lexicalRank.set(id, idx + 1);
  });
  semanticRows.forEach((row, idx) => {
    const id = typeof row.id === 'string' ? row.id : '';
    if (!id) return;
    if (!rowById.has(id)) rowById.set(id, row);
    if (!semanticRank.has(id)) semanticRank.set(id, idx + 1);
  });
  semanticCandidates.forEach((candidate) => {
    semanticScore.set(candidate.memory_id, candidate.score);
  });

  const ids = Array.from(rowById.keys());
  ids.sort((a, b) => {
    const lexA = lexicalRank.has(a) ? 1 / (MEMORY_SEARCH_FUSION_K + (lexicalRank.get(a) ?? 0)) : 0;
    const lexB = lexicalRank.has(b) ? 1 / (MEMORY_SEARCH_FUSION_K + (lexicalRank.get(b) ?? 0)) : 0;
    const semA = semanticRank.has(a) ? 1 / (MEMORY_SEARCH_FUSION_K + (semanticRank.get(a) ?? 0)) : 0;
    const semB = semanticRank.has(b) ? 1 / (MEMORY_SEARCH_FUSION_K + (semanticRank.get(b) ?? 0)) : 0;
    const semScoreA = normalizeSemanticScore(toFiniteNumber(semanticScore.get(a), -1));
    const semScoreB = normalizeSemanticScore(toFiniteNumber(semanticScore.get(b), -1));

    let fusedA = lexA;
    let fusedB = lexB;
    if (mode === 'semantic') {
      fusedA = semA + (semScoreA * 0.25);
      fusedB = semB + (semScoreB * 0.25);
    } else if (mode === 'hybrid') {
      fusedA = (semA * 0.7) + (lexA * 0.3) + (semScoreA * 0.15);
      fusedB = (semB * 0.7) + (lexB * 0.3) + (semScoreB * 0.15);
    }
    if (fusedB !== fusedA) return fusedB - fusedA;

    const rowA = rowById.get(a);
    const rowB = rowById.get(b);
    const updatedA = toFiniteNumber(rowA?.updated_at, toFiniteNumber(rowA?.created_at, 0));
    const updatedB = toFiniteNumber(rowB?.updated_at, toFiniteNumber(rowB?.created_at, 0));
    return updatedB - updatedA;
  });

  return ids.slice(0, limit).map((id) => rowById.get(id)).filter((row): row is Record<string, unknown> => Boolean(row));
}
