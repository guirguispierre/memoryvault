import type { BrainPolicy, LinkStats } from './types.js';

export const SERVER_NAME = 'memoryvault';
export const SERVER_VERSION = '1.11.0';
export const LEGACY_BRAIN_ID = 'legacy-default-brain';
export const LEGACY_USER_ID = 'legacy-token-user';
export const LEGACY_USER_EMAIL = 'legacy-token@memoryvault.local';
export const ACCESS_TOKEN_TTL_SECONDS = 60 * 60; // 1 hour
export const REFRESH_TOKEN_TTL_SECONDS = 60 * 60 * 24 * 30; // 30 days
export const AUTH_TOKEN_COOKIE_NAME = 'auth_token';
export const REFRESH_TOKEN_COOKIE_NAME = 'refresh_token';
export const AUTH_TOKEN_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24; // 24 hours
export const AUTH_TOKEN_COOKIE_PATH = '/';
export const REFRESH_TOKEN_COOKIE_PATH = '/';
export const SESSION_COOKIE_SAME_SITE = 'Lax' as const;
export const AUTH_RATE_LIMIT_MAX_ATTEMPTS = 10;
export const AUTH_RATE_LIMIT_WINDOW_SECONDS = 60 * 15;
export const PBKDF2_ITERATIONS = 100_000;
export const EMBEDDING_MODEL = '@cf/baai/bge-base-en-v1.5';
export const VECTORIZE_QUERY_TOP_K_MAX = 20;
export const VECTORIZE_UPSERT_BATCH_SIZE = 500;
export const VECTORIZE_DELETE_BATCH_SIZE = 500;
export const VECTORIZE_SETTLE_POLL_INTERVAL_MS = 3000;
export const VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS = 180;
export const VECTORIZE_REINDEX_WAIT_TIMEOUT_SECONDS_MAX = 900;
export const EMBEDDING_BATCH_SIZE = 16;
export const MEMORY_SEARCH_FUSION_K = 60;
export const MEMORY_SEARCH_DEFAULT_LIMIT = 20;
export const MEMORY_SEARCH_MAX_LIMIT = 20;
export const VECTOR_ID_PREFIX = 'm:';
export const VECTOR_ID_MAX_MEMORY_ID_LENGTH = 62; // 2-byte prefix + 62-byte id = 64-byte vector id limit.
export const DEFAULT_OAUTH_REDIRECT_DOMAIN_ALLOWLIST = ['localhost', '127.0.0.1'] as const;
export const TRUSTED_REDIRECT_DOMAINS = ['poke.com', 'claude.ai'] as const;

export const DEFAULT_BRAIN_POLICY: BrainPolicy = {
  decay_days: 30,
  max_inferred_edges: 360,
  min_link_suggestion_score: 0.25,
  retention_days: 3650,
  private_mode: true,
  snapshot_retention: 50,
  path_max_hops: 5,
  subgraph_default_radius: 2,
};

export const EMPTY_LINK_STATS: LinkStats = {
  link_count: 0,
  supports_count: 0,
  contradicts_count: 0,
  supersedes_count: 0,
  causes_count: 0,
  example_of_count: 0,
};
