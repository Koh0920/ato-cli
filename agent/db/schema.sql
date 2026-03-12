CREATE TABLE IF NOT EXISTS success_patterns (
    env_hash        TEXT PRIMARY KEY,
    env_key         TEXT NOT NULL,
    repo_path       TEXT NOT NULL,
    capsule_toml    TEXT NOT NULL,
    provider_used   TEXT,
    model_used      TEXT,
    success_count   INTEGER DEFAULT 1,
    correction_iter INTEGER,
    test_framework  TEXT,
    updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_success_patterns_env_key
    ON success_patterns(env_key);
