-- 006 模型组化补丁:为已应用旧版 004(单表模型行)的现存库补建双表并删除旧表;新库(已由新 004 建表)幂等无副作用
DROP TABLE IF EXISTS upstreams;

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
