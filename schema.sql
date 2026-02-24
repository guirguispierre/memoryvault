CREATE TABLE IF NOT EXISTS memories (
  id TEXT PRIMARY KEY,
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
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_type ON memories(type);
CREATE INDEX IF NOT EXISTS idx_key ON memories(key);
CREATE INDEX IF NOT EXISTS idx_created ON memories(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_archived ON memories(archived_at);
CREATE INDEX IF NOT EXISTS idx_importance ON memories(importance DESC);
CREATE INDEX IF NOT EXISTS idx_confidence ON memories(confidence DESC);

CREATE TABLE IF NOT EXISTS rate_limits (
  ip TEXT NOT NULL,
  window INTEGER NOT NULL,
  count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ip, window)
);

CREATE TABLE IF NOT EXISTS memory_links (
  id TEXT PRIMARY KEY,
  from_id TEXT NOT NULL,
  to_id TEXT NOT NULL,
  relation_type TEXT NOT NULL DEFAULT 'related' CHECK(relation_type IN ('related', 'supports', 'contradicts', 'supersedes', 'causes', 'example_of')),
  label TEXT,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (from_id) REFERENCES memories(id) ON DELETE CASCADE,
  FOREIGN KEY (to_id) REFERENCES memories(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_links_from ON memory_links(from_id);
CREATE INDEX IF NOT EXISTS idx_links_to ON memory_links(to_id);
CREATE INDEX IF NOT EXISTS idx_links_relation_type ON memory_links(relation_type);
