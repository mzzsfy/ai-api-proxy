-- 包来源标记:local(本地上传/目录导入)或 subscription/<name>(订阅同步)
ALTER TABLE packages ADD COLUMN source TEXT NOT NULL DEFAULT 'local';
