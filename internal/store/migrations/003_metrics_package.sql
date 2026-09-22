-- 监控包维度:metrics_minutely 增 by_package_json
ALTER TABLE metrics_minutely ADD COLUMN by_package_json TEXT NOT NULL DEFAULT '{}';
