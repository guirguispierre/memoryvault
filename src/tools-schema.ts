import type { ToolDefinition, ToolReleaseMeta, ToolChangelogEntry } from './types.js';
import { SERVER_VERSION } from './constants.js';

export function parseSemver(version: string): [number, number, number] | null {
  const trimmed = version.trim();
  const match = /^v?(\d+)\.(\d+)\.(\d+)$/.exec(trimmed);
  if (!match) return null;
  return [Number(match[1]), Number(match[2]), Number(match[3])];
}

export function compareSemver(a: string, b: string): number {
  const aParts = parseSemver(a);
  const bParts = parseSemver(b);
  if (!aParts || !bParts) {
    return a.localeCompare(b);
  }
  if (aParts[0] !== bParts[0]) return aParts[0] - bParts[0];
  if (aParts[1] !== bParts[1]) return aParts[1] - bParts[1];
  return aParts[2] - bParts[2];
}

export const TOOL_RELEASE_META: Record<string, ToolReleaseMeta> = {
  memory_graph_stats: {
    introduced_in: '1.8.0',
    notes: 'Graph-level structural metrics, hubs, and topology analytics.',
  },
  memory_neighbors: {
    introduced_in: '1.8.0',
    notes: 'Seeded graph neighborhood extraction around a memory id/query.',
  },
  memory_tag_stats: {
    introduced_in: '1.8.0',
    notes: 'Tag frequency and co-occurrence analytics for knowledge grooming.',
  },
  memory_link: {
    introduced_in: '1.4.0',
    notes: 'Structured memory relationships and graph reasoning.',
  },
  memory_unlink: {
    introduced_in: '1.4.0',
    notes: 'Relationship cleanup and correction.',
  },
  memory_links: {
    introduced_in: '1.4.0',
    notes: 'Neighborhood inspection for a specific memory.',
  },
  memory_changelog: {
    introduced_in: '1.5.0',
    notes: 'Memory-level audit/event stream for agent sync.',
  },
  memory_conflicts: {
    introduced_in: '1.5.0',
    notes: 'Contradiction detection across high-confidence facts.',
  },
  objective_set: {
    introduced_in: '1.5.0',
    notes: 'Autonomous objective node creation and updates.',
  },
  objective_list: {
    introduced_in: '1.5.0',
    notes: 'Objective planning view for long-term goals.',
  },
  tool_manifest: {
    introduced_in: '1.6.0',
    notes: 'Canonical MCP tool registry with definition hashes.',
  },
  tool_changelog: {
    introduced_in: '1.6.0',
    notes: 'Versioned tool/endpoints/scoring change feed.',
  },
  memory_explain_score: {
    introduced_in: '1.6.0',
    notes: 'Explainable confidence/importance scoring breakdown.',
  },
  memory_search: {
    introduced_in: '1.0.0',
    notes: 'Hybrid retrieval upgraded with semantic + lexical fusion in 1.9.0.',
  },
  memory_reindex: {
    introduced_in: '1.9.0',
    notes: 'Backfill/repair semantic vector embeddings from D1 memories with optional readiness waiting.',
  },
  memory_link_suggest: {
    introduced_in: '1.7.0',
    notes: 'Scored relationship suggestions for graph expansion.',
  },
  memory_path_find: {
    introduced_in: '1.7.0',
    notes: 'Path search between memory nodes for reasoning traces.',
  },
  memory_conflict_resolve: {
    introduced_in: '1.7.0',
    notes: 'Conflict resolution state tracking for contradictions.',
  },
  memory_entity_resolve: {
    introduced_in: '1.7.0',
    notes: 'Canonical entity alias mapping and merge support.',
  },
  memory_source_trust_set: {
    introduced_in: '1.7.0',
    notes: 'Set source trust weights that influence dynamic confidence.',
  },
  memory_source_trust_get: {
    introduced_in: '1.7.0',
    notes: 'Inspect source trust map for a brain.',
  },
  brain_policy_set: {
    introduced_in: '1.7.0',
    notes: 'Configure retention, decay, and graph policy defaults.',
  },
  brain_policy_get: {
    introduced_in: '1.7.0',
    notes: 'Read effective brain policy.',
  },
  brain_snapshot_create: {
    introduced_in: '1.7.0',
    notes: 'Create a point-in-time brain snapshot.',
  },
  brain_snapshot_list: {
    introduced_in: '1.7.0',
    notes: 'List saved brain snapshots.',
  },
  brain_snapshot_restore: {
    introduced_in: '1.7.0',
    notes: 'Restore a brain snapshot in merge/replace mode.',
  },
  objective_next_actions: {
    introduced_in: '1.7.0',
    notes: 'Generate ranked next steps from objective nodes.',
  },
  memory_subgraph: {
    introduced_in: '1.7.0',
    notes: 'Focused graph extraction around seed/query.',
  },
  memory_watch: {
    introduced_in: '1.7.0',
    notes: 'Create/list/manage event watches with optional webhooks.',
  },
};

export const TOOL_CHANGELOG: ToolChangelogEntry[] = [
  {
    id: 'semantic-1.9.0',
    version: '1.9.0',
    released_at: 1772667243,
    summary: 'Semantic memory retrieval with Cloudflare Vectorize + Workers AI.',
    changes: [
      {
        type: 'updated',
        target: 'tool',
        name: 'memory_search',
        description: 'Added lexical/semantic/hybrid retrieval modes with score fusion and semantic thresholds.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'memory_reindex',
        description: 'Added vector backfill/repair tool for indexing existing D1 memories.',
      },
      {
        type: 'updated',
        target: 'tool',
        name: 'Vector index sync',
        description: 'Memory mutations now keep Vectorize in sync for save/update/delete/archive/restore paths.',
      },
    ],
  },
  {
    id: 'release-1.8.1',
    version: '1.8.1',
    released_at: 1772666466,
    summary: 'Viewer release: settings version/changelog plus stricter write permissions.',
    changes: [
      {
        type: 'updated',
        target: 'auth',
        name: 'Read-only human sessions',
        description: 'Human email/password sessions are now read-only for memory mutations; AI-agent OAuth sessions retain write access.',
      },
      {
        type: 'updated',
        target: 'endpoint',
        name: 'GET /view settings',
        description: 'Added in-settings version badge and in-app changelog modal powered by tool_changelog.',
      },
    ],
  },
  {
    id: 'graph-tools-1.8.0',
    version: '1.8.0',
    released_at: 1772660000,
    summary: 'Graph analytics and UX polish release.',
    changes: [
      {
        type: 'added',
        target: 'tool',
        name: 'memory_graph_stats + memory_neighbors + memory_tag_stats',
        description: 'Added graph topology metrics, seeded neighborhood extraction, and tag analytics tools.',
      },
      {
        type: 'updated',
        target: 'endpoint',
        name: 'GET /view graph UX',
        description: 'Improved graph exploration with neighborhood hover focus and physics pause/resume controls.',
      },
    ],
  },
  {
    id: 'autonomy-1.7.0',
    version: '1.7.0',
    released_at: 1771966600,
    summary: 'Autonomy, policy, and graph intelligence expansion.',
    changes: [
      {
        type: 'added',
        target: 'tool',
        name: 'memory_link_suggest + memory_path_find',
        description: 'Added link suggestion scoring and pathfinding for graph reasoning.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'memory_conflict_resolve + memory_entity_resolve',
        description: 'Added conflict lifecycle management and canonical entity alias resolution.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'memory_source_trust_set/get + brain_policy_set/get',
        description: 'Added trust and policy controls that influence dynamic scoring defaults.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'brain_snapshot_create/list/restore + memory_subgraph + memory_watch + objective_next_actions',
        description: 'Added snapshot lifecycle, focused subgraph extraction, watch subscriptions, and objective action planning.',
      },
    ],
  },
  {
    id: 'tooling-1.6.0',
    version: '1.6.0',
    released_at: 1771963200,
    summary: 'Tool discovery and scoring transparency release.',
    changes: [
      {
        type: 'added',
        target: 'tool',
        name: 'tool_manifest',
        description: 'Introduced canonical tool manifest output with schema/definition hashes and release metadata.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'tool_changelog',
        description: 'Introduced versioned changelog feed for MCP tools, auth updates, and scoring model updates.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'memory_explain_score',
        description: 'Added explainable scoring API for confidence/importance values and contributing factors.',
      },
      {
        type: 'updated',
        target: 'scoring',
        name: 'memory scoring model',
        description: 'Standardized explainable dynamic score model output as memoryvault-dynamic-v1.',
      },
    ],
  },
  {
    id: 'auth-1.5.1',
    version: '1.5.1',
    released_at: 1771933500,
    summary: 'User session governance endpoints.',
    changes: [
      {
        type: 'added',
        target: 'endpoint',
        name: 'GET /auth/sessions',
        description: 'Added per-user session inventory with active/current flags.',
      },
      {
        type: 'added',
        target: 'endpoint',
        name: 'POST /auth/sessions/revoke',
        description: 'Added single-session and bulk session revocation controls.',
      },
    ],
  },
  {
    id: 'oauth-1.5.0',
    version: '1.5.0',
    released_at: 1771888500,
    summary: 'OAuth-first multi-tenant MemoryVault rollout.',
    changes: [
      {
        type: 'added',
        target: 'auth',
        name: 'OAuth authorization code + PKCE',
        description: 'Enabled dynamic registration and OAuth metadata discovery for keyless MCP setup.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'memory_conflicts',
        description: 'Added contradiction detection for high-confidence facts.',
      },
      {
        type: 'added',
        target: 'tool',
        name: 'objective_set/objective_list',
        description: 'Added autonomous objective graph nodes for long-term planning.',
      },
    ],
  },
];


export function getToolReleaseMeta(toolName: string): ToolReleaseMeta {
  return TOOL_RELEASE_META[toolName] ?? { introduced_in: '1.0.0' };
}

export function isToolDeprecated(meta: ToolReleaseMeta): boolean {
  if (!meta.deprecated_in) return false;
  return compareSemver(SERVER_VERSION, meta.deprecated_in) >= 0;
}

export const TOOLS: ToolDefinition[] = [
  {
    name: 'memory_save',
    description: 'Save a new memory. Use type="note" for titled knowledge entries, type="fact" for key=value pairs, type="journal" for free-form thoughts.',
    inputSchema: {
      type: 'object',
      properties: {
        type: { type: 'string', enum: ['note', 'fact', 'journal'], description: 'Memory type' },
        content: { type: 'string', description: 'The memory content' },
        title: { type: 'string', description: 'Title (for notes)' },
        key: { type: 'string', description: 'Key name (for facts, e.g. "user_name")' },
        tags: { type: 'string', description: 'Comma-separated tags' },
        source: { type: 'string', description: 'Source system/person for this memory' },
        confidence: { type: 'number', minimum: 0, maximum: 1, description: 'Reliability score from 0 to 1 (default 0.7)' },
        importance: { type: 'number', minimum: 0, maximum: 1, description: 'Priority score from 0 to 1 (default 0.5)' },
      },
      required: ['type', 'content'],
    },
  },
  {
    name: 'memory_get',
    description: 'Retrieve a specific memory by its ID.',
    inputSchema: {
      type: 'object',
      properties: { id: { type: 'string', description: 'Memory ID' } },
      required: ['id'],
    },
  },
  {
    name: 'memory_get_fact',
    description: 'Fast lookup of a fact memory by its key name.',
    inputSchema: {
      type: 'object',
      properties: { key: { type: 'string', description: 'The fact key to look up' } },
      required: ['key'],
    },
  },
  {
    name: 'memory_search',
    description: 'Search memories with lexical, semantic, or hybrid retrieval across title/key/id/source/content.',
    inputSchema: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Search query' },
        type: { type: 'string', enum: ['note', 'fact', 'journal'], description: 'Optionally filter by type' },
        mode: { type: 'string', enum: ['lexical', 'semantic', 'hybrid'], description: 'Retrieval mode (default: hybrid)' },
        limit: { type: 'number', description: 'Max results (1-20, default 20)' },
        min_score: { type: 'number', description: 'Minimum semantic score threshold for semantic/hybrid modes (default -1)' },
      },
      required: ['query'],
    },
  },
  {
    name: 'memory_reindex',
    description: 'Rebuild semantic vectors for recent memories in the current brain and optionally wait for Vectorize readiness.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: { type: 'number', description: 'Max memories to process (1-2000, default 500)' },
        include_archived: { type: 'boolean', description: 'Also process archived memories (archived rows trigger vector deletion)' },
        wait_for_index: { type: 'boolean', description: 'Wait for Vectorize mutation processing before returning (default true)' },
        wait_timeout_seconds: { type: 'number', description: 'Max wait time when wait_for_index=true (1-900, default 180)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_list',
    description: 'List memories with optional filters by type or tag.',
    inputSchema: {
      type: 'object',
      properties: {
        type: { type: 'string', enum: ['note', 'fact', 'journal'], description: 'Filter by type' },
        tag: { type: 'string', description: 'Filter by tag' },
        limit: { type: 'number', description: 'Max results (1-100, default 20)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_update',
    description: 'Update an existing memory by ID. Only provided fields are updated.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Memory ID to update' },
        content: { type: 'string', description: 'New content' },
        title: { type: 'string', description: 'New title' },
        tags: { type: 'string', description: 'New comma-separated tags' },
        source: { type: 'string', description: 'New source' },
        confidence: { type: 'number', minimum: 0, maximum: 1, description: 'Updated confidence score (0..1)' },
        importance: { type: 'number', minimum: 0, maximum: 1, description: 'Updated importance score (0..1)' },
        archived: { type: 'boolean', description: 'Set true to archive, false to restore' },
      },
      required: ['id'],
    },
  },
  {
    name: 'memory_delete',
    description: 'Permanently delete a memory by ID.',
    inputSchema: {
      type: 'object',
      properties: { id: { type: 'string', description: 'Memory ID to delete' } },
      required: ['id'],
    },
  },
  {
    name: 'memory_stats',
    description: 'Get statistics about stored memories: counts by type and recent activity.',
    inputSchema: { type: 'object', properties: {}, required: [] },
  },
  {
    name: 'memory_tag_stats',
    description: 'Analyze tag frequency and co-occurrence across active memories.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: { type: 'number', description: 'Max tags returned (default 20, max 100)' },
        min_count: { type: 'number', description: 'Only include tags with at least this many memories (default 2)' },
        include_pairs: { type: 'boolean', description: 'Include top tag-pair co-occurrences (default true)' },
      },
      required: [],
    },
  },
  {
    name: 'tool_manifest',
    description: 'Return canonical MCP tool definitions, schema hashes, and release/deprecation metadata.',
    inputSchema: {
      type: 'object',
      properties: {
        tool: { type: 'string', description: 'Optional tool name filter' },
        include_schema: { type: 'boolean', description: 'Include full input schema in each result (default true)' },
        include_hashes: { type: 'boolean', description: 'Include schema_hash and definition_hash fields (default true)' },
        include_deprecated: { type: 'boolean', description: 'Include deprecated tools (default true)' },
      },
      required: [],
    },
  },
  {
    name: 'tool_changelog',
    description: 'Return versioned tool/auth/scoring changes so agents can detect what is new.',
    inputSchema: {
      type: 'object',
      properties: {
        since_version: { type: 'string', description: 'Only return entries with version greater than this semver' },
        since: { type: 'number', description: 'Only return entries released at/after this unix timestamp (seconds)' },
        limit: { type: 'number', description: 'Max entries to return (default 20, max 100)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_explain_score',
    description: 'Explain why a memory has its current dynamic confidence/importance values.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Memory ID to explain' },
        at: { type: 'number', description: 'Optional evaluation timestamp (unix seconds) for what-if analysis' },
      },
      required: ['id'],
    },
  },
  {
    name: 'memory_link',
    description: 'Create or update a relationship between two memories. Set relation_type for graph reasoning (supports/contradicts/supersedes/etc).',
    inputSchema: {
      type: 'object',
      properties: {
        from_id: { type: 'string', description: 'Source memory ID' },
        to_id: { type: 'string', description: 'Target memory ID' },
        relation_type: { type: 'string', enum: ['related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of'], description: 'Structured relationship type (default "related")' },
        label: { type: 'string', description: 'Optional free-text description of the relationship' },
      },
      required: ['from_id', 'to_id'],
    },
  },
  {
    name: 'memory_unlink',
    description: 'Remove a link between two memories.',
    inputSchema: {
      type: 'object',
      properties: {
        from_id: { type: 'string', description: 'Source memory ID' },
        to_id: { type: 'string', description: 'Target memory ID' },
        relation_type: { type: 'string', enum: ['related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of'], description: 'Optional relation type filter when unlinking' },
      },
      required: ['from_id', 'to_id'],
    },
  },
  {
    name: 'memory_links',
    description: 'Get all memories linked to a given memory, including relation_type and labels.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Memory ID to get connections for' },
      },
      required: ['id'],
    },
  },
  {
    name: 'memory_consolidate',
    description: 'Consolidate likely-duplicate memories by keeping one canonical memory and archiving duplicates with supersedes links.',
    inputSchema: {
      type: 'object',
      properties: {
        type: { type: 'string', enum: ['note', 'fact', 'journal'], description: 'Optional memory type filter' },
        tag: { type: 'string', description: 'Optional tag filter' },
        older_than_days: { type: 'number', description: 'Only consolidate memories older than this age' },
        limit: { type: 'number', description: 'Max memories to scan (default 300, max 1000)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_forget',
    description: 'Archive or delete memories by ID or policy filters for controlled forgetting.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Specific memory ID to forget' },
        mode: { type: 'string', enum: ['soft', 'hard'], description: 'soft archives; hard deletes (default soft)' },
        tag: { type: 'string', description: 'Optional tag filter for batch mode' },
        older_than_days: { type: 'number', description: 'Optional minimum age in days for batch mode' },
        max_importance: { type: 'number', minimum: 0, maximum: 1, description: 'Only forget memories with importance <= this threshold' },
        limit: { type: 'number', description: 'Batch size (default 25, max 200)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_activate',
    description: 'Run spreading-activation retrieval from seed memories (id/query) across the memory graph.',
    inputSchema: {
      type: 'object',
      properties: {
        seed_id: { type: 'string', description: 'Optional seed memory id' },
        query: { type: 'string', description: 'Optional query to select seed memories by id/name/content' },
        hops: { type: 'number', description: 'Propagation depth (1-4, default 2)' },
        limit: { type: 'number', description: 'Max returned activations (1-100, default 20)' },
        include_inferred: { type: 'boolean', description: 'Include tag-based inferred synapses (default true)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_reinforce',
    description: 'Apply Hebbian-style reinforcement to a memory and optionally spread updates to connected neighbors.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Memory id to reinforce' },
        delta_confidence: { type: 'number', description: 'Base confidence delta (default +0.04)' },
        delta_importance: { type: 'number', description: 'Base importance delta (default +0.06)' },
        spread: { type: 'number', minimum: 0, maximum: 1, description: 'How much update spreads to neighbors (default 0.35)' },
        hops: { type: 'number', description: 'Spread depth (0-3, default 1)' },
      },
      required: ['id'],
    },
  },
  {
    name: 'memory_decay',
    description: 'Apply homeostatic decay to stale low-connectivity memories.',
    inputSchema: {
      type: 'object',
      properties: {
        older_than_days: { type: 'number', description: 'Only decay memories older than N days (default 30)' },
        max_link_count: { type: 'number', description: 'Only decay memories with links <= this count (default 1)' },
        decay_confidence: { type: 'number', description: 'Confidence decrement per memory (default 0.01)' },
        decay_importance: { type: 'number', description: 'Importance decrement per memory (default 0.03)' },
        limit: { type: 'number', description: 'Max memories to decay (default 200)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_changelog',
    description: 'Read the memory changelog so agents can quickly detect what changed since last run.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: { type: 'number', description: 'Max entries to return (default 25, max 200)' },
        since: { type: 'number', description: 'Unix timestamp (seconds). Return entries after this time' },
        event_type: { type: 'string', description: 'Optional event type filter' },
        entity_id: { type: 'string', description: 'Optional entity id filter' },
      },
      required: [],
    },
  },
  {
    name: 'memory_conflicts',
    description: 'Detect contradictions between high-confidence facts (explicit contradict links + conflicting fact keys).',
    inputSchema: {
      type: 'object',
      properties: {
        min_confidence: { type: 'number', minimum: 0, maximum: 1, description: 'Only include conflicts when both sides are >= this confidence (default 0.7)' },
        limit: { type: 'number', description: 'Max conflicts returned (default 40)' },
        include_resolved: { type: 'boolean', description: 'Include conflicts already marked resolved/dismissed (default false)' },
      },
      required: [],
    },
  },
  {
    name: 'objective_set',
    description: 'Create or update a dedicated autonomous objective node (goal or curiosity) and connect it to the root objective node.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Existing objective memory id (optional)' },
        title: { type: 'string', description: 'Objective title' },
        content: { type: 'string', description: 'Objective details or rationale' },
        kind: { type: 'string', enum: ['goal', 'curiosity'], description: 'Objective type (default goal)' },
        horizon: { type: 'string', enum: ['short', 'medium', 'long'], description: 'Planning horizon (default long)' },
        status: { type: 'string', enum: ['active', 'paused', 'done'], description: 'Objective status (default active)' },
        priority: { type: 'number', minimum: 0, maximum: 1, description: 'Base priority/importance (default 0.8)' },
        tags: { type: 'string', description: 'Additional comma-separated tags' },
      },
      required: ['title'],
    },
  },
  {
    name: 'objective_list',
    description: 'List autonomous objective nodes (goals/curiosities) for planning.',
    inputSchema: {
      type: 'object',
      properties: {
        kind: { type: 'string', enum: ['goal', 'curiosity'], description: 'Optional objective kind filter' },
        status: { type: 'string', enum: ['active', 'paused', 'done'], description: 'Optional status filter' },
        limit: { type: 'number', description: 'Max objectives to return (default 50)' },
      },
      required: [],
    },
  },
  {
    name: 'objective_next_actions',
    description: 'Generate prioritized next actions from active objective nodes.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: { type: 'number', description: 'Max actions to return (default 12)' },
        include_done: { type: 'boolean', description: 'Include completed objectives (default false)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_link_suggest',
    description: 'Suggest high-value links between memories using tags, lexical overlap, source, and recency signals.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Optional seed memory id' },
        query: { type: 'string', description: 'Optional seed query (id/name/content/source)' },
        limit: { type: 'number', description: 'Max suggestions (default 20)' },
        min_score: { type: 'number', minimum: 0, maximum: 1, description: 'Minimum suggestion score (default from brain policy)' },
        include_existing: { type: 'boolean', description: 'Include already-linked pairs (default false)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_path_find',
    description: 'Find strongest explicit relationship paths between two memories.',
    inputSchema: {
      type: 'object',
      properties: {
        from_id: { type: 'string', description: 'Start memory id' },
        to_id: { type: 'string', description: 'Target memory id' },
        max_hops: { type: 'number', description: 'Maximum path length in hops (default from policy)' },
        limit: { type: 'number', description: 'Max paths to return (default 5)' },
      },
      required: ['from_id', 'to_id'],
    },
  },
  {
    name: 'memory_conflict_resolve',
    description: 'Record or update a contradiction resolution state for a memory pair.',
    inputSchema: {
      type: 'object',
      properties: {
        a_id: { type: 'string', description: 'First memory id in the conflict pair' },
        b_id: { type: 'string', description: 'Second memory id in the conflict pair' },
        status: { type: 'string', enum: ['needs_review', 'resolved', 'superseded', 'dismissed'], description: 'Resolution status' },
        canonical_id: { type: 'string', description: 'Winning/canonical memory id (optional)' },
        note: { type: 'string', description: 'Optional resolver note' },
      },
      required: ['a_id', 'b_id', 'status'],
    },
  },
  {
    name: 'memory_entity_resolve',
    description: 'Resolve entity aliases to a canonical memory and optionally archive aliases.',
    inputSchema: {
      type: 'object',
      properties: {
        mode: { type: 'string', enum: ['resolve', 'lookup', 'list'], description: 'Operation mode (default resolve)' },
        canonical_id: { type: 'string', description: 'Canonical memory id (required for resolve mode)' },
        alias_id: { type: 'string', description: 'Single alias id (for resolve or lookup)' },
        alias_ids: { type: 'array', items: { type: 'string' }, description: 'Alias ids to map to canonical memory' },
        archive_aliases: { type: 'boolean', description: 'Archive alias memories after mapping (default false)' },
        confidence: { type: 'number', minimum: 0, maximum: 1, description: 'Alias mapping confidence (default 0.9)' },
        note: { type: 'string', description: 'Optional note on alias resolution' },
        limit: { type: 'number', description: 'Max rows for list mode (default 100)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_source_trust_set',
    description: 'Set a trust score for a source key to influence dynamic confidence.',
    inputSchema: {
      type: 'object',
      properties: {
        source: { type: 'string', description: 'Source key/name' },
        trust: { type: 'number', minimum: 0, maximum: 1, description: 'Trust score from 0 to 1' },
        notes: { type: 'string', description: 'Optional note about why this trust score is set' },
      },
      required: ['source', 'trust'],
    },
  },
  {
    name: 'memory_source_trust_get',
    description: 'Read source trust values for the brain.',
    inputSchema: {
      type: 'object',
      properties: {
        source: { type: 'string', description: 'Optional source key filter' },
        limit: { type: 'number', description: 'Max rows returned (default 200)' },
      },
      required: [],
    },
  },
  {
    name: 'brain_policy_set',
    description: 'Set brain-level policy defaults for decay, retention, and graph behavior.',
    inputSchema: {
      type: 'object',
      properties: {
        decay_days: { type: 'number', description: 'Default days-before-decay threshold' },
        max_inferred_edges: { type: 'number', description: 'Default inferred graph edge cap' },
        min_link_suggestion_score: { type: 'number', minimum: 0, maximum: 1, description: 'Default minimum link suggestion score' },
        retention_days: { type: 'number', description: 'Default retention window in days' },
        private_mode: { type: 'boolean', description: 'Whether strict private mode is enabled' },
        snapshot_retention: { type: 'number', description: 'Number of snapshots to retain automatically' },
        path_max_hops: { type: 'number', description: 'Default hop limit for path finding' },
        subgraph_default_radius: { type: 'number', description: 'Default BFS radius for subgraph extraction' },
      },
      required: [],
    },
  },
  {
    name: 'brain_policy_get',
    description: 'Get effective brain policy values.',
    inputSchema: { type: 'object', properties: {}, required: [] },
  },
  {
    name: 'brain_snapshot_create',
    description: 'Create a point-in-time snapshot of memories, links, trust map, and policy.',
    inputSchema: {
      type: 'object',
      properties: {
        label: { type: 'string', description: 'Optional snapshot label' },
        summary: { type: 'string', description: 'Optional summary/reason' },
        include_archived: { type: 'boolean', description: 'Include archived memories in the snapshot (default false)' },
      },
      required: [],
    },
  },
  {
    name: 'brain_snapshot_list',
    description: 'List stored snapshots for the current brain.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: { type: 'number', description: 'Max snapshots to return (default 50)' },
      },
      required: [],
    },
  },
  {
    name: 'brain_snapshot_restore',
    description: 'Restore a snapshot in merge or replace mode.',
    inputSchema: {
      type: 'object',
      properties: {
        snapshot_id: { type: 'string', description: 'Snapshot id to restore' },
        mode: { type: 'string', enum: ['replace', 'merge'], description: 'replace wipes existing brain data before import (default merge)' },
        restore_policy: { type: 'boolean', description: 'Restore policy from snapshot payload (default true)' },
        restore_source_trust: { type: 'boolean', description: 'Restore source trust map from snapshot payload (default true)' },
      },
      required: ['snapshot_id'],
    },
  },
  {
    name: 'memory_subgraph',
    description: 'Return a focused subgraph around a seed/query/tag for efficient reasoning.',
    inputSchema: {
      type: 'object',
      properties: {
        seed_id: { type: 'string', description: 'Seed memory id' },
        query: { type: 'string', description: 'Seed query text' },
        tag: { type: 'string', description: 'Optional tag filter for seed selection' },
        radius: { type: 'number', description: 'Hop radius (default from policy)' },
        limit_nodes: { type: 'number', description: 'Max nodes in response (default 120)' },
        include_inferred: { type: 'boolean', description: 'Include inferred shared-tag edges (default true)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_graph_stats',
    description: 'Return structural graph metrics, relation distributions, and top hub memories.',
    inputSchema: {
      type: 'object',
      properties: {
        include_inferred: { type: 'boolean', description: 'Include inferred shared-tag edges (default true)' },
        top_hubs: { type: 'number', description: 'Max hub memories returned (default 12)' },
        top_tags: { type: 'number', description: 'Max top tags returned (default 12)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_neighbors',
    description: 'Get a seeded neighborhood around a memory id/query with hop depth and edge context.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Seed memory id' },
        query: { type: 'string', description: 'Fallback seed query if id is not provided' },
        max_hops: { type: 'number', description: 'Neighborhood depth (default 1, max 4)' },
        limit_nodes: { type: 'number', description: 'Max nodes returned (default 80)' },
        relation_type: { type: 'string', enum: ['related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of'], description: 'Optional explicit relation filter for traversal' },
        include_inferred: { type: 'boolean', description: 'Include inferred shared-tag edges (default true)' },
      },
      required: [],
    },
  },
  {
    name: 'memory_watch',
    description: 'Manage watch subscriptions for changelog events with optional webhook delivery.',
    inputSchema: {
      type: 'object',
      properties: {
        mode: { type: 'string', enum: ['create', 'list', 'delete', 'set_active', 'test'], description: 'Watch operation mode (default list)' },
        id: { type: 'string', description: 'Watch id for delete/set_active/test' },
        name: { type: 'string', description: 'Watch name for create mode' },
        event_types: { type: 'array', items: { type: 'string' }, description: 'Event type filters (for create mode)' },
        query: { type: 'string', description: 'Optional text query filter' },
        webhook_url: { type: 'string', description: 'Optional webhook URL for event delivery' },
        secret: { type: 'string', description: 'Optional webhook secret header value' },
        active: { type: 'boolean', description: 'Desired active state for set_active mode' },
        limit: { type: 'number', description: 'Max watch rows for list mode (default 100)' },
      },
      required: [],
    },
  },
];

export const MUTATING_TOOL_NAMES = new Set<string>([
  'memory_save',
  'memory_update',
  'memory_delete',
  'memory_link',
  'memory_unlink',
  'memory_consolidate',
  'memory_forget',
  'memory_reinforce',
  'memory_decay',
  'memory_reindex',
  'objective_set',
  'memory_conflict_resolve',
  'memory_entity_resolve',
  'memory_source_trust_set',
  'brain_policy_set',
  'brain_snapshot_create',
  'brain_snapshot_restore',
  'memory_watch',
]);

export function isMutatingTool(toolName: string): boolean {
  return MUTATING_TOOL_NAMES.has(toolName);
}
