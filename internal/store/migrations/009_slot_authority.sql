-- 权威槽标记(与 kv 值行同事务写;标记终身有效,delete 不清除)
CREATE TABLE IF NOT EXISTS slot_authority (
    ns TEXT NOT NULL,
    key TEXT NOT NULL,
    PRIMARY KEY (ns, key)
);
