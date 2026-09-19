-- 初始 schema:packages/upstreams/kv/metrics_minutely
CREATE TABLE packages (
    name TEXT PRIMARY KEY,
    manifest_json TEXT NOT NULL,
    parts_json TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    enabled INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE upstreams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    base_package TEXT NOT NULL,
    extras_json TEXT NOT NULL DEFAULT '[]',
    models_json TEXT NOT NULL DEFAULT '[]',
    targets_json TEXT NOT NULL DEFAULT '[]',
    params_json TEXT NOT NULL DEFAULT '{}',
    filter_params_json TEXT NOT NULL DEFAULT '{}',
    filters_enabled_json TEXT NOT NULL DEFAULT '{}',
    strategy_json TEXT NOT NULL DEFAULT '{}',
    enabled INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE kv (
    ns TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (ns, key)
);

CREATE TABLE metrics_minutely (
    minute TEXT PRIMARY KEY,
    requests INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0,
    max_concurrent INTEGER NOT NULL DEFAULT 0,
    by_upstream_json TEXT NOT NULL DEFAULT '{}',
    by_target_json TEXT NOT NULL DEFAULT '{}'
);
