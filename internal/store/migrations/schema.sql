-- 终态 schema(新板直建,无历史迁移链;全部 IF NOT EXISTS 幂等)

CREATE TABLE IF NOT EXISTS packages (
    name TEXT PRIMARY KEY,
    manifest_json TEXT NOT NULL,
    parts_json TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    enabled INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    declaration_json TEXT NOT NULL DEFAULT '{}',
    source TEXT NOT NULL DEFAULT 'local'
);

CREATE TABLE IF NOT EXISTS package_keys (
    name TEXT PRIMARY KEY,
    data_json TEXT NOT NULL DEFAULT '{}',
    prev_json TEXT,
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS package_settings (
    name TEXT PRIMARY KEY,
    data_json TEXT NOT NULL DEFAULT '{}',
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS kv (
    ns TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (ns, key)
);

CREATE TABLE IF NOT EXISTS metrics_minutely (
    minute TEXT PRIMARY KEY,
    requests INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0,
    max_concurrent INTEGER NOT NULL DEFAULT 0,
    by_upstream_json TEXT NOT NULL DEFAULT '{}',
    by_target_json TEXT NOT NULL DEFAULT '{}',
    by_package_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS model_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prefix TEXT NOT NULL UNIQUE,
    base_package TEXT NOT NULL,
    params_json TEXT NOT NULL DEFAULT '{}',
    enabled INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS model_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL REFERENCES model_groups(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (group_id, name)
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    repo_url TEXT NOT NULL,
    user TEXT NOT NULL DEFAULT '',
    branch TEXT NOT NULL DEFAULT '',
    path TEXT NOT NULL DEFAULT '',
    token TEXT NOT NULL DEFAULT '',
    last_synced_commit TEXT NOT NULL DEFAULT '',
    last_synced_at TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT ''
);

-- 权威槽标记(与 kv 值行同事务写;标记终身有效,delete 不清除)
CREATE TABLE IF NOT EXISTS slot_authority (
    ns TEXT NOT NULL,
    key TEXT NOT NULL,
    PRIMARY KEY (ns, key)
);
