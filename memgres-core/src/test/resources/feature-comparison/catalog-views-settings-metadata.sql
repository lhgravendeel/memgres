-- What a tool reads before it does anything: which relations the catalog names, which settings
-- the server admits to having, how relations are classified, and whether a view column may be null.

DROP VIEW IF EXISTS cvs_v CASCADE;
DROP TABLE IF EXISTS cvs_t CASCADE;
CREATE TABLE cvs_t (id int PRIMARY KEY, name text NOT NULL, note text);
CREATE VIEW cvs_v AS SELECT id, name FROM cvs_t;

-- The catalog views applications and migration tools read directly.
-- begin-expected
-- columns: n
-- row: 12
-- end-expected
SELECT count(*) AS n FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'pg_catalog' AND c.relname IN
  ('pg_indexes','pg_policies','pg_rules','pg_stats','pg_user','pg_shadow','pg_group',
   'pg_timezone_abbrevs','pg_user_mappings','pg_publication_tables','pg_stat_io','pg_stat_archiver');

-- An index is a relation, but not a table. (A handful of PG views are also named "..._index",
-- so this asks about the indexes themselves rather than about the spelling.)
-- begin-expected
-- columns: k
-- row: i
-- end-expected
SELECT relkind::text AS k FROM pg_class WHERE relname = 'pg_class_oid_index';

-- begin-expected
-- columns: k
-- row: i
-- end-expected
SELECT relkind::text AS k FROM pg_class WHERE relname = 'pg_type_oid_index';

-- begin-expected
-- columns: k
-- row: r
-- end-expected
SELECT relkind::text AS k FROM pg_class WHERE relname = 'pg_class';

-- information_schema describes itself.
-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (count(*) >= 60)::text AS a FROM information_schema.tables
 WHERE table_schema = 'information_schema';

-- begin-expected
-- columns: n
-- row: 8
-- end-expected
SELECT count(*) AS n FROM information_schema.tables
 WHERE table_schema = 'information_schema'
   AND table_name IN ('tables','columns','views','routines','schemata',
                      'table_constraints','key_column_usage','sql_features');

-- The user's own tables are still listed.
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM information_schema.tables
 WHERE table_schema = 'public' AND table_name = 'cvs_t';

-- Settings a client reads at startup.
-- begin-expected
-- columns: n
-- row: 9
-- end-expected
SELECT count(*) AS n FROM pg_settings WHERE name IN
  ('array_nulls','backslash_quote','block_size','autovacuum','archive_mode','bgwriter_delay',
   'checkpoint_timeout','wal_level','max_wal_size');

-- begin-expected
-- columns: a
-- row: on
-- end-expected
SELECT current_setting('array_nulls') AS a;

-- begin-expected
-- columns: a
-- row: 8192
-- end-expected
SELECT current_setting('block_size') AS a;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_settings WHERE name = 'no_such_setting';

-- A view column is a query result: it carries no NOT NULL of its own.
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT a.attnotnull::text AS a FROM pg_attribute a
 WHERE a.attrelid = 'cvs_v'::regclass AND a.attname = 'name';

-- The base column keeps its constraint.
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT a.attnotnull::text AS a FROM pg_attribute a
 WHERE a.attrelid = 'cvs_t'::regclass AND a.attname = 'name';

-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT a.attnotnull::text AS a FROM pg_attribute a
 WHERE a.attrelid = 'cvs_t'::regclass AND a.attname = 'note';

-- information_schema agrees with pg_attribute about the view column.
-- begin-expected
-- columns: is_nullable
-- row: YES
-- end-expected
SELECT is_nullable FROM information_schema.columns
 WHERE table_name = 'cvs_v' AND column_name = 'name';

-- begin-expected
-- columns: is_nullable
-- row: NO
-- end-expected
SELECT is_nullable FROM information_schema.columns
 WHERE table_name = 'cvs_t' AND column_name = 'name';

DROP VIEW cvs_v;
DROP TABLE cvs_t;
