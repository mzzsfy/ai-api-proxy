-- 订阅表:git 仓库订阅条目(name 唯一保证 source 标记与联动卸载不歧义)
CREATE TABLE subscriptions (
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
