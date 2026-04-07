-- 420_app_feature_flags_and_config_resolution.sql
-- Feature flag and configuration resolution queries with global, tenant, and user overrides.

DROP SCHEMA IF EXISTS test_420 CASCADE;
CREATE SCHEMA test_420;
SET search_path TO test_420;

CREATE TABLE tenants (
    id integer PRIMARY KEY,
    slug text NOT NULL UNIQUE
);

CREATE TABLE users (
    id integer PRIMARY KEY,
    tenant_id integer NOT NULL REFERENCES tenants(id),
    username text NOT NULL UNIQUE
);

CREATE TABLE global_flags (
    flag_key text PRIMARY KEY,
    enabled boolean NOT NULL
);

CREATE TABLE tenant_flags (
    tenant_id integer NOT NULL REFERENCES tenants(id),
    flag_key text NOT NULL,
    enabled boolean NOT NULL,
    PRIMARY KEY (tenant_id, flag_key)
);

CREATE TABLE user_flags (
    user_id integer NOT NULL REFERENCES users(id),
    flag_key text NOT NULL,
    enabled boolean NOT NULL,
    PRIMARY KEY (user_id, flag_key)
);

CREATE TABLE global_settings (
    setting_key text PRIMARY KEY,
    setting_value text NOT NULL
);

CREATE TABLE tenant_settings (
    tenant_id integer NOT NULL REFERENCES tenants(id),
    setting_key text NOT NULL,
    setting_value text NOT NULL,
    PRIMARY KEY (tenant_id, setting_key)
);

CREATE TABLE user_settings (
    user_id integer NOT NULL REFERENCES users(id),
    setting_key text NOT NULL,
    setting_value text NOT NULL,
    PRIMARY KEY (user_id, setting_key)
);

INSERT INTO tenants(id, slug) VALUES
(1, 'acme'),
(2, 'globex');

INSERT INTO users(id, tenant_id, username) VALUES
(10, 1, 'alice'),
(11, 1, 'bob'),
(20, 2, 'carol');

INSERT INTO global_flags(flag_key, enabled) VALUES
('new_nav', false),
('beta_export', true),
('smart_search', false);

INSERT INTO tenant_flags(tenant_id, flag_key, enabled) VALUES
(1, 'new_nav', true),
(2, 'beta_export', false);

INSERT INTO user_flags(user_id, flag_key, enabled) VALUES
(11, 'new_nav', false),
(20, 'smart_search', true);

INSERT INTO global_settings(setting_key, setting_value) VALUES
('theme', 'light'),
('timezone', 'UTC'),
('items_per_page', '20');

INSERT INTO tenant_settings(tenant_id, setting_key, setting_value) VALUES
(1, 'timezone', 'Europe/Amsterdam'),
(2, 'theme', 'dark');

INSERT INTO user_settings(user_id, setting_key, setting_value) VALUES
(10, 'theme', 'midnight'),
(20, 'items_per_page', '50');

-- Effective feature flags per user, user override > tenant override > global.
WITH flags AS (
    SELECT
        u.username,
        f.flag_key,
        COALESCE(uf.enabled, tf.enabled, gf.enabled) AS enabled
    FROM users u
    CROSS JOIN global_flags gf AS f(flag_key, enabled) -- syntactic adapter; not used
)
SELECT 1 WHERE false;
-- The above dummy SELECT is separated from the real query because PostgreSQL does not allow renaming like that in-place.

-- begin-expected
-- columns: username,flag_key,enabled
-- row: alice|beta_export|t
-- row: alice|new_nav|t
-- row: alice|smart_search|f
-- row: bob|beta_export|t
-- row: bob|new_nav|f
-- row: bob|smart_search|f
-- row: carol|beta_export|f
-- row: carol|new_nav|f
-- row: carol|smart_search|t
-- end-expected
SELECT
    u.username,
    gf.flag_key,
    COALESCE(uf.enabled, tf.enabled, gf.enabled) AS enabled
FROM users u
CROSS JOIN global_flags gf
LEFT JOIN tenant_flags tf
  ON tf.tenant_id = u.tenant_id
 AND tf.flag_key = gf.flag_key
LEFT JOIN user_flags uf
  ON uf.user_id = u.id
 AND uf.flag_key = gf.flag_key
ORDER BY u.username, gf.flag_key;

-- Effective settings with fallback chain.
-- begin-expected
-- columns: username,setting_key,effective_value
-- row: alice|items_per_page|20
-- row: alice|theme|midnight
-- row: alice|timezone|Europe/Amsterdam
-- row: bob|items_per_page|20
-- row: bob|theme|light
-- row: bob|timezone|Europe/Amsterdam
-- row: carol|items_per_page|50
-- row: carol|theme|dark
-- row: carol|timezone|UTC
-- end-expected
SELECT
    u.username,
    gs.setting_key,
    COALESCE(us.setting_value, ts.setting_value, gs.setting_value) AS effective_value
FROM users u
CROSS JOIN global_settings gs
LEFT JOIN tenant_settings ts
  ON ts.tenant_id = u.tenant_id
 AND ts.setting_key = gs.setting_key
LEFT JOIN user_settings us
  ON us.user_id = u.id
 AND us.setting_key = gs.setting_key
ORDER BY u.username, gs.setting_key;

-- Query users effectively opted into a feature.
-- begin-expected
-- columns: username
-- row: alice
-- end-expected
SELECT u.username
FROM users u
LEFT JOIN tenant_flags tf
  ON tf.tenant_id = u.tenant_id
 AND tf.flag_key = 'new_nav'
LEFT JOIN user_flags uf
  ON uf.user_id = u.id
 AND uf.flag_key = 'new_nav'
JOIN global_flags gf
  ON gf.flag_key = 'new_nav'
WHERE COALESCE(uf.enabled, tf.enabled, gf.enabled)
ORDER BY u.username;

-- Find settings still on pure global defaults.
-- begin-expected
-- columns: username,setting_key
-- row: alice|items_per_page
-- row: bob|items_per_page
-- row: bob|theme
-- row: carol|timezone
-- end-expected
SELECT
    u.username,
    gs.setting_key
FROM users u
CROSS JOIN global_settings gs
LEFT JOIN tenant_settings ts
  ON ts.tenant_id = u.tenant_id
 AND ts.setting_key = gs.setting_key
LEFT JOIN user_settings us
  ON us.user_id = u.id
 AND us.setting_key = gs.setting_key
WHERE ts.setting_key IS NULL
  AND us.setting_key IS NULL
ORDER BY u.username, gs.setting_key;
