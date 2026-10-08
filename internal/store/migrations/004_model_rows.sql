-- 004 模型组化 v3:upstreams 单表拆为 model_groups(组=上游连接,前缀唯一)+ model_members(组内短名唯一)
-- 旧 upstreams 表直接删除(不兼容旧版,数据不迁移)
DROP TABLE IF EXISTS upstreams;

CREATE TABLE model_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prefix TEXT NOT NULL UNIQUE,
    base_package TEXT NOT NULL,
    params_json TEXT NOT NULL DEFAULT '{}',
    enabled INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE model_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL REFERENCES model_groups(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (group_id, name)
);
