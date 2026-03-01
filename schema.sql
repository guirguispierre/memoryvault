CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  display_name TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS brains (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  slug TEXT UNIQUE,
  owner_user_id TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS brain_memberships (
  brain_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'member',
  created_at INTEGER NOT NULL,
  PRIMARY KEY (brain_id, user_id),
  FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_brain_memberships_user ON brain_memberships(user_id, brain_id);
CREATE INDEX IF NOT EXISTS idx_brains_owner ON brains(owner_user_id);

CREATE TABLE IF NOT EXISTS auth_sessions (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  brain_id TEXT NOT NULL,
  client_id TEXT,
  refresh_hash TEXT NOT NULL UNIQUE,
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  used_at INTEGER NOT NULL,
  revoked_at INTEGER,
  replaced_by TEXT,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(user_id, expires_at DESC);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_brain ON auth_sessions(brain_id, expires_at DESC);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_client ON auth_sessions(client_id, expires_at DESC);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires ON auth_sessions(expires_at);

CREATE TABLE IF NOT EXISTS oauth_clients (
  id TEXT PRIMARY KEY,
  client_id TEXT NOT NULL UNIQUE,
  client_name TEXT,
  redirect_uris TEXT NOT NULL,
  grant_types TEXT NOT NULL,
  response_types TEXT NOT NULL,
  token_endpoint_auth_method TEXT NOT NULL DEFAULT 'none',
  client_secret_hash TEXT,
  client_secret_expires_at INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_oauth_clients_client_id ON oauth_clients(client_id);

CREATE TABLE IF NOT EXISTS oauth_authorization_codes (
  id TEXT PRIMARY KEY,
  code TEXT NOT NULL UNIQUE,
  client_id TEXT NOT NULL,
  redirect_uri TEXT NOT NULL,
  user_id TEXT NOT NULL,
  brain_id TEXT NOT NULL,
  code_challenge TEXT NOT NULL,
  code_challenge_method TEXT NOT NULL DEFAULT 'S256',
  scope TEXT,
  resource TEXT,
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  used_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_oauth_codes_code ON oauth_authorization_codes(code);
CREATE INDEX IF NOT EXISTS idx_oauth_codes_expires ON oauth_authorization_codes(expires_at);

CREATE TABLE IF NOT EXISTS memories (
  id TEXT PRIMARY KEY,
  brain_id TEXT NOT NULL DEFAULT 'legacy-default-brain',
  type TEXT NOT NULL CHECK(type IN ('note', 'fact', 'journal')),
  title TEXT,
  key TEXT,
  content TEXT NOT NULL,
  tags TEXT,
  source TEXT,
  confidence REAL NOT NULL DEFAULT 0.7 CHECK(confidence >= 0 AND confidence <= 1),
  importance REAL NOT NULL DEFAULT 0.5 CHECK(importance >= 0 AND importance <= 1),
  archived_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_type ON memories(type);
CREATE INDEX IF NOT EXISTS idx_key ON memories(key);
CREATE INDEX IF NOT EXISTS idx_created ON memories(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_archived ON memories(archived_at);
CREATE INDEX IF NOT EXISTS idx_importance ON memories(importance DESC);
CREATE INDEX IF NOT EXISTS idx_confidence ON memories(confidence DESC);
CREATE INDEX IF NOT EXISTS idx_memories_brain_created ON memories(brain_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_memories_brain_key ON memories(brain_id, key);

CREATE TABLE IF NOT EXISTS rate_limits (
  ip TEXT NOT NULL,
  window INTEGER NOT NULL,
  count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ip, window)
);

CREATE TABLE IF NOT EXISTS memory_links (
  id TEXT PRIMARY KEY,
  brain_id TEXT NOT NULL DEFAULT 'legacy-default-brain',
  from_id TEXT NOT NULL,
  to_id TEXT NOT NULL,
  relation_type TEXT NOT NULL DEFAULT 'related' CHECK(relation_type IN ('related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of')),
  label TEXT,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (from_id) REFERENCES memories(id) ON DELETE CASCADE,
  FOREIGN KEY (to_id) REFERENCES memories(id) ON DELETE CASCADE,
  FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_links_from ON memory_links(from_id);
CREATE INDEX IF NOT EXISTS idx_links_to ON memory_links(to_id);
CREATE INDEX IF NOT EXISTS idx_links_relation_type ON memory_links(relation_type);
CREATE INDEX IF NOT EXISTS idx_links_brain_from ON memory_links(brain_id, from_id);
CREATE INDEX IF NOT EXISTS idx_links_brain_to ON memory_links(brain_id, to_id);

CREATE TABLE IF NOT EXISTS memory_changelog (
  id TEXT PRIMARY KEY,
  brain_id TEXT NOT NULL DEFAULT 'legacy-default-brain',
  event_type TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  summary TEXT NOT NULL,
  payload TEXT,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_changelog_created ON memory_changelog(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_changelog_entity ON memory_changelog(entity_type, entity_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_changelog_brain_created ON memory_changelog(brain_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_changelog_brain_entity ON memory_changelog(brain_id, entity_type, entity_id, created_at DESC);

CREATE TABLE IF NOT EXISTS brain_source_trust (
  id TEXT PRIMARY KEY,
  brain_id TEXT NOT NULL,
  source_key TEXT NOT NULL,
  trust REAL NOT NULL DEFAULT 0.5,
  notes TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(brain_id, source_key),
  FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_source_trust_brain ON brain_source_trust(brain_id, source_key);

CREATE TABLE IF NOT EXISTS brain_policies (
  brain_id TEXT PRIMARY KEY,
  policy_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS brain_snapshots (
  id TEXT PRIMARY KEY,
  brain_id TEXT NOT NULL,
  label TEXT,
  summary TEXT,
  memory_count INTEGER NOT NULL DEFAULT 0,
  link_count INTEGER NOT NULL DEFAULT 0,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_brain_snapshots_brain_created ON brain_snapshots(brain_id, created_at DESC);

CREATE TABLE IF NOT EXISTS memory_conflict_resolutions (
  id TEXT PRIMARY KEY,
  brain_id TEXT NOT NULL,
  pair_key TEXT NOT NULL,
  a_id TEXT NOT NULL,
  b_id TEXT NOT NULL,
  status TEXT NOT NULL,
  canonical_id TEXT,
  note TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(brain_id, pair_key),
  FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_conflict_resolutions_brain_status ON memory_conflict_resolutions(brain_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS memory_entity_aliases (
  id TEXT PRIMARY KEY,
  brain_id TEXT NOT NULL,
  canonical_memory_id TEXT NOT NULL,
  alias_memory_id TEXT NOT NULL,
  note TEXT,
  confidence REAL NOT NULL DEFAULT 0.9,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(brain_id, alias_memory_id),
  FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_entity_aliases_brain_canonical ON memory_entity_aliases(brain_id, canonical_memory_id);

CREATE TABLE IF NOT EXISTS memory_watches (
  id TEXT PRIMARY KEY,
  brain_id TEXT NOT NULL,
  name TEXT NOT NULL,
  event_types TEXT NOT NULL,
  query TEXT,
  webhook_url TEXT,
  secret TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  last_triggered_at INTEGER,
  last_error TEXT,
  FOREIGN KEY (brain_id) REFERENCES brains(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_memory_watches_brain_active ON memory_watches(brain_id, is_active, updated_at DESC);

INSERT OR IGNORE INTO brains (id, name, slug, owner_user_id, created_at, updated_at)
VALUES (
  'legacy-default-brain',
  'Legacy Shared Brain',
  'legacy-shared-brain',
  NULL,
  CAST(strftime('%s', 'now') AS INTEGER),
  CAST(strftime('%s', 'now') AS INTEGER)
);
