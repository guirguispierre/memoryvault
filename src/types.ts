export interface Env {
  DB: D1Database;
  RATE_LIMIT_KV: KVNamespace;
  AUTH_SECRET: string;
  ADMIN_TOKEN: string;
  OAUTH_REDIRECT_DOMAIN_ALLOWLIST?: string;
  AI?: Ai;
  MEMORY_INDEX?: Vectorize;
}

export type MemorySearchMode = 'lexical' | 'semantic' | 'hybrid';

export type SemanticMemoryCandidate = {
  memory_id: string;
  score: number;
  rank: number;
};

export type VectorSyncStats = {
  upserted: number;
  deleted: number;
  skipped: number;
  mutation_ids: string[];
  probe_vector_id: string | null;
};

export type SessionTokens = {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  refresh_expires_in: number;
  session_id: string;
};

export type CorsJsonResponseOptions = {
  headers?: HeadersInit;
  cookies?: string[];
};

export type AuthContext = {
  kind: 'legacy' | 'user';
  brainId: string;
  userId: string | null;
  sessionId: string | null;
  clientId: string | null;
};

export type AccessTokenPayload = {
  typ: 'access';
  sub: string;
  bid: string;
  sid: string;
  iat: number;
  exp: number;
};

export type RefreshSessionRow = {
  id: string;
  user_id: string;
  brain_id: string;
  client_id: string | null;
  expires_at: number;
  revoked_at: number | null;
};

export const VALID_TYPES = ['note', 'fact', 'journal'] as const;
export type MemoryType = typeof VALID_TYPES[number];

export const RELATION_TYPES = ['related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of'] as const;
export type RelationType = typeof RELATION_TYPES[number];

export type LinkStats = {
  link_count: number;
  supports_count: number;
  contradicts_count: number;
  supersedes_count: number;
  causes_count: number;
  example_of_count: number;
};

export type ScoreComponent = {
  name: string;
  delta: number;
};

export type DynamicScoreBreakdown = {
  score_model: string;
  evaluated_at: number;
  memory_type: string;
  source: string | null;
  age_days: number;
  link_stats: LinkStats;
  base_confidence: number;
  base_importance: number;
  raw_confidence: number;
  raw_importance: number;
  dynamic_confidence: number;
  dynamic_importance: number;
  confidence_components: ScoreComponent[];
  importance_components: ScoreComponent[];
  signals: {
    certainty_hits: number;
    hedge_hits: number;
    importance_hits: number;
    source_trust: number | null;
    high_signal_source: boolean;
    low_signal_source: boolean;
    content_length: number;
  };
};

export type BrainPolicy = {
  decay_days: number;
  max_inferred_edges: number;
  min_link_suggestion_score: number;
  retention_days: number;
  private_mode: boolean;
  snapshot_retention: number;
  path_max_hops: number;
  subgraph_default_radius: number;
};

export type ToolDefinition = {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
};

export type ToolReleaseMeta = {
  introduced_in: string;
  deprecated_in?: string;
  replaced_by?: string;
  notes?: string;
};

export type ToolChangelogChange = {
  type: 'added' | 'updated' | 'deprecated' | 'security' | 'fix';
  target: 'tool' | 'endpoint' | 'scoring' | 'auth';
  name: string;
  description: string;
};

export type ToolChangelogEntry = {
  id: string;
  version: string;
  released_at: number;
  summary: string;
  changes: ToolChangelogChange[];
};

export type MemoryGraphNode = {
  id: string;
  type: string;
  title: string | null;
  key: string | null;
  content: string;
  tags: string | null;
  source: string | null;
  created_at: number;
  updated_at: number;
  confidence: number;
  importance: number;
};

export type MemoryGraphLink = {
  id: string;
  from_id: string;
  to_id: string;
  relation_type: RelationType;
  label: string | null;
  inferred?: boolean;
  score?: number;
};

export type UserRow = {
  id: string;
  email: string;
  password_hash: string;
  display_name: string | null;
  created_at: number;
};

export type BrainSummary = {
  id: string;
  name: string;
  slug: string | null;
  role: string;
  created_at: number;
  updated_at: number;
};

export type EndpointGuide = {
  title: string;
  subtitle: string;
  endpointPath: string;
  methods: string;
  auth: string;
  details: string[];
};

export type ToolArgs = Record<string, unknown>;
