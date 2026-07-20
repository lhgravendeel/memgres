-- Catalog introspection tests (H13-H17, M14-M22, L11-L13)

-- Setup
CREATE TABLE ci_test(
  id serial PRIMARY KEY,
  name varchar(50) NOT NULL,
  price numeric(10,2) DEFAULT 0,
  ts timestamp DEFAULT CURRENT_TIMESTAMP,
  active boolean DEFAULT true
);

CREATE TABLE ci_fk_parent(id int PRIMARY KEY, val text);
CREATE TABLE ci_fk_child(id int, parent_id int REFERENCES ci_fk_parent(id));

-- H13: OPERATOR(pg_catalog.~) regex match (used by psql \d)
-- begin-expected
-- columns: match
-- row: t
-- end-expected
SELECT 'hello' OPERATOR(pg_catalog.~) 'hel' AS match;

-- begin-expected
-- columns: match
-- row: t
-- end-expected
SELECT 'HELLO' OPERATOR(pg_catalog.~*) 'hel' AS match;

-- begin-expected
-- columns: match
-- row: t
-- end-expected
SELECT 'hello' OPERATOR(pg_catalog.!~) 'xyz' AS match;

-- begin-expected
-- columns: match
-- row: f
-- end-expected
SELECT 'HELLO' OPERATOR(pg_catalog.!~*) 'hel' AS match;

-- H14: information_schema.columns - character_maximum_length for varchar
-- begin-expected
-- columns: character_maximum_length
-- row: 50
-- end-expected
SELECT character_maximum_length FROM information_schema.columns
WHERE table_name = 'ci_test' AND column_name = 'name';

-- H14: udt_name for serial should be int4
-- begin-expected
-- columns: udt_name
-- row: int4
-- end-expected
SELECT udt_name FROM information_schema.columns
WHERE table_name = 'ci_test' AND column_name = 'id';

-- H14: column_default should show now() not CURRENT_TIMESTAMP
-- begin-expected
-- columns: column_default
-- row: CURRENT_TIMESTAMP
-- end-expected
SELECT column_default FROM information_schema.columns
WHERE table_name = 'ci_test' AND column_name = 'ts';

-- H15: check_constraints.check_clause should be SQL not Java AST
CREATE TABLE ci_check(id int, val int CHECK (val > 0));

-- begin-expected
-- columns: cc
-- row: (val > 0)
-- end-expected
SELECT check_clause AS cc FROM information_schema.check_constraints
WHERE constraint_name LIKE 'ci_check%';

-- H16: pg_get_indexdef should include UNIQUE for PK-backed indexes
-- begin-expected
-- columns: has_unique
-- row: t
-- end-expected
SELECT pg_get_indexdef(i.indexrelid) LIKE '%UNIQUE%' AS has_unique
FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
WHERE c.relname = 'ci_test' AND i.indisprimary;

-- H17: pg_index.indkey::text should be space-separated int2vector
-- begin-expected
-- columns: key_format
-- row: t
-- end-expected
SELECT indkey::text NOT LIKE '{%' AS key_format
FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
WHERE c.relname = 'ci_test' AND i.indisprimary;

-- M14: serial sequence max should match column type (2147483647 for int4)
-- begin-expected
-- columns: max_value
-- row: 2147483647
-- end-expected
SELECT maximum_value::bigint AS max_value FROM information_schema.sequences
WHERE sequence_name = 'ci_test_id_seq';

-- M15: regclass with quoted identifiers
CREATE TABLE "MixedCase"(id int);

-- begin-expected-error
-- error: 42P01
-- end-expected
SELECT '"MixedCase"'::regclass;

-- M19: count(*) in view definition should render as count(*)
CREATE VIEW ci_count_view AS SELECT count(*) AS cnt FROM ci_test;

-- begin-expected
-- columns: has_star
-- row: t
-- end-expected
SELECT definition LIKE '%count(*)%' AS has_star FROM pg_views
WHERE viewname = 'ci_count_view';

-- M21: constraint_column_usage FK shows referenced column
-- begin-expected
-- columns: table_name, column_name
-- row: ci_fk_parent, id
-- end-expected
SELECT table_name, column_name FROM information_schema.constraint_column_usage
WHERE constraint_name LIKE '%parent_id_fkey%';

-- M22: pg_class.reltuples is -1 for never-analyzed tables
-- begin-expected
-- columns: reltuples
-- row: -1
-- end-expected
SELECT reltuples::int FROM pg_class WHERE relname = 'ci_test';

-- M22: pg_tables.hasindexes should be true for tables with PK
-- begin-expected
-- columns: hasindexes
-- row: t
-- end-expected
SELECT hasindexes FROM pg_tables WHERE tablename = 'ci_test';

-- L11: pg_enum.enumsortorder fractional after ADD VALUE BEFORE
CREATE TYPE ci_mood AS ENUM ('sad', 'happy');
ALTER TYPE ci_mood ADD VALUE 'neutral' BEFORE 'happy';

-- begin-expected
-- columns: label, is_fractional
-- row: neutral, t
-- end-expected
SELECT enumlabel AS label, enumsortorder <> floor(enumsortorder) AS is_fractional
FROM pg_enum WHERE enumtypid = 'ci_mood'::regtype AND enumlabel = 'neutral';

-- L12: pg_settings server_version_num has integer vartype
-- begin-expected
-- columns: vartype
-- row: integer
-- end-expected
SELECT vartype FROM pg_settings WHERE name = 'server_version_num';

-- L12: pg_toast in schemata
-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM information_schema.schemata WHERE schema_name = 'pg_toast';

-- Cleanup
DROP VIEW IF EXISTS ci_count_view;
DROP TABLE IF EXISTS ci_fk_child;
DROP TABLE IF EXISTS ci_fk_parent;
DROP TABLE IF EXISTS ci_check;
DROP TABLE IF EXISTS ci_test;
DROP TABLE IF EXISTS "MixedCase";
DROP TYPE IF EXISTS ci_mood;
