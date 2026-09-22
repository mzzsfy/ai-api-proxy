-- 004 模型行 v2:upstreams 表收缩为 {name, plugin, params, enabled};旧表整体保留为 upstreams_v1
-- 数据转换(密钥并入包 keys、models 拆行、参数收敛)由 registry 启动期 Go 侧执行,完成后 DROP upstreams_v1
ALTER TABLE upstreams RENAME TO upstreams_v1;

CREATE TABLE upstreams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    base_package TEXT NOT NULL,
    params_json TEXT NOT NULL DEFAULT '{}',
    enabled INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (name, base_package)
);
