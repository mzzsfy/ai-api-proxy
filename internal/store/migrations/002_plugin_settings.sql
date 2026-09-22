-- 002 插件配置:同构 blob 表 + 声明列 + keys 自 kv 迁出(行拷贝,无 JSON 解析)
CREATE TABLE package_keys (
    name TEXT PRIMARY KEY,
    data_json TEXT NOT NULL DEFAULT '{}',
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE package_settings (
    name TEXT PRIMARY KEY,
    data_json TEXT NOT NULL DEFAULT '{}',
    updated_at INTEGER NOT NULL DEFAULT 0
);

ALTER TABLE packages ADD COLUMN declaration_json TEXT NOT NULL DEFAULT '{}';

INSERT INTO package_keys (name, data_json, updated_at)
SELECT ns, value, 0 FROM kv WHERE key = '__keys__';

DELETE FROM kv WHERE key = '__keys__';
