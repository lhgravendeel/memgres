DROP SCHEMA IF EXISTS test_720 CASCADE;
CREATE SCHEMA test_720;
SET search_path TO test_720;

CREATE TABLE things (
    thing_id integer PRIMARY KEY,
    thing_name text NOT NULL
);

INSERT INTO things VALUES (1, 'widget');

-- begin-expected
-- columns: has_select_priv
-- row: t
-- end-expected
SELECT has_table_privilege(current_user, 'test_720.things', 'SELECT') AS has_select_priv;

-- begin-expected
-- columns: has_insert_priv
-- row: t
-- end-expected
SELECT has_table_privilege(current_user, 'test_720.things', 'INSERT') AS has_insert_priv;

-- The relation belongs to whoever created it, so the answer is read against that role rather
-- than against a name only one connection would produce.
-- begin-expected
-- columns: owned_by_creator
-- row: t
-- end-expected
SELECT pg_get_userbyid(c.relowner) = current_user AS owned_by_creator
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'test_720'
  AND c.relname = 'things';

-- begin-expected
-- columns: visible
-- row: t
-- end-expected
SELECT pg_table_is_visible('test_720.things'::regclass) AS visible;

