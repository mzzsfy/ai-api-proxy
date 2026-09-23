-- 005 keys 单键化:加备份列 + 存量全清(用户拍板不做数据搬迁,部署后经 GUI 重录)
ALTER TABLE package_keys ADD COLUMN prev_json TEXT;
DELETE FROM package_keys;
