-- ============================================================================
-- Feature Comparison: Catalog Metadata Functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests pg_get_functiondef(), pg_get_ruledef(), pg_get_function_arguments(),
-- pg_get_function_identity_arguments(), pg_describe_object(), and related
-- catalog introspection functions that are important for pg_dump and tools.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS catmeta_test CASCADE;
CREATE SCHEMA catmeta_test;
SET search_path = catmeta_test, public;

-- ============================================================================
-- SECTION A: pg_get_functiondef()
-- ============================================================================

-- ============================================================================
-- 1. pg_get_functiondef: simple SQL function
-- ============================================================================

CREATE FUNCTION catmeta_add(a integer, b integer) RETURNS integer
LANGUAGE sql AS $$ SELECT a + b $$;

-- note: pg_get_functiondef should return the full CREATE OR REPLACE FUNCTION statement
-- begin-expected
-- columns: has_def
-- row: true
-- end-expected
SELECT pg_get_functiondef(oid) IS NOT NULL AND pg_get_functiondef(oid) <> '' AS has_def
FROM pg_proc WHERE proname = 'catmeta_add' AND pronamespace = 'catmeta_test'::regnamespace;

-- begin-expected
-- columns: contains_name
-- row: true
-- end-expected
SELECT pg_get_functiondef(oid) LIKE '%catmeta_add%' AS contains_name
FROM pg_proc WHERE proname = 'catmeta_add' AND pronamespace = 'catmeta_test'::regnamespace;

-- ============================================================================
-- 2. pg_get_functiondef: PL/pgSQL function
-- ============================================================================

CREATE FUNCTION catmeta_greet(name text) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  RETURN 'Hello, ' || name || '!';
END;
$$;

-- begin-expected
-- columns: has_def
-- row: true
-- end-expected
SELECT pg_get_functiondef(oid) IS NOT NULL AND pg_get_functiondef(oid) <> '' AS has_def
FROM pg_proc WHERE proname = 'catmeta_greet' AND pronamespace = 'catmeta_test'::regnamespace;

-- begin-expected
-- columns: has_plpgsql
-- row: true
-- end-expected
SELECT pg_get_functiondef(oid) LIKE '%plpgsql%' AS has_plpgsql
FROM pg_proc WHERE proname = 'catmeta_greet' AND pronamespace = 'catmeta_test'::regnamespace;

-- ============================================================================
-- 3. pg_get_functiondef: function with defaults
-- ============================================================================

CREATE FUNCTION catmeta_with_defaults(a integer, b integer DEFAULT 10) RETURNS integer
LANGUAGE sql AS $$ SELECT a + b $$;

-- begin-expected
-- columns: has_default
-- row: true
-- end-expected
SELECT pg_get_functiondef(oid) LIKE '%DEFAULT%' OR pg_get_functiondef(oid) LIKE '%default%' AS has_default
FROM pg_proc WHERE proname = 'catmeta_with_defaults' AND pronamespace = 'catmeta_test'::regnamespace;

-- ============================================================================
-- 4. pg_get_functiondef: procedure
-- ============================================================================

CREATE PROCEDURE catmeta_proc(x integer)
LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE 'x = %', x;
END;
$$;

-- begin-expected
-- columns: has_def
-- row: true
-- end-expected
SELECT pg_get_functiondef(oid) IS NOT NULL AND pg_get_functiondef(oid) <> '' AS has_def
FROM pg_proc WHERE proname = 'catmeta_proc' AND pronamespace = 'catmeta_test'::regnamespace;

-- ============================================================================
-- 5. pg_get_functiondef: function with OUT params
-- ============================================================================

CREATE FUNCTION catmeta_out_params(IN x integer, OUT doubled integer, OUT tripled integer)
LANGUAGE sql AS $$ SELECT x * 2, x * 3 $$;

-- begin-expected
-- columns: has_out
-- row: true
-- end-expected
SELECT pg_get_functiondef(oid) LIKE '%OUT%' OR pg_get_functiondef(oid) LIKE '%out%' AS has_out
FROM pg_proc WHERE proname = 'catmeta_out_params' AND pronamespace = 'catmeta_test'::regnamespace;

-- ============================================================================
-- 6. pg_get_functiondef: SETOF return type
-- ============================================================================

CREATE FUNCTION catmeta_setof() RETURNS SETOF integer
LANGUAGE sql AS $$ SELECT generate_series(1, 3) $$;

-- begin-expected
-- columns: has_setof
-- row: true
-- end-expected
SELECT pg_get_functiondef(oid) LIKE '%SETOF%' OR pg_get_functiondef(oid) LIKE '%setof%' AS has_setof
FROM pg_proc WHERE proname = 'catmeta_setof' AND pronamespace = 'catmeta_test'::regnamespace;

-- ============================================================================
-- 7. pg_get_functiondef: nonexistent OID returns NULL
-- ============================================================================

-- begin-expected
-- columns: result
-- row: NULL
-- end-expected
SELECT pg_get_functiondef(0) AS result;

-- ============================================================================
-- SECTION B: pg_get_function_arguments()
-- ============================================================================

-- ============================================================================
-- 8. pg_get_function_arguments: basic
-- ============================================================================

-- begin-expected
-- columns: has_args
-- row: true
-- end-expected
SELECT pg_get_function_arguments(oid) IS NOT NULL AND pg_get_function_arguments(oid) <> '' AS has_args
FROM pg_proc WHERE proname = 'catmeta_add' AND pronamespace = 'catmeta_test'::regnamespace;

-- note: Should contain "integer" type names
-- begin-expected
-- columns: has_integer
-- row: true
-- end-expected
SELECT pg_get_function_arguments(oid) LIKE '%integer%' AS has_integer
FROM pg_proc WHERE proname = 'catmeta_add' AND pronamespace = 'catmeta_test'::regnamespace;

-- ============================================================================
-- 9. pg_get_function_arguments: with OUT params
-- ============================================================================

-- begin-expected
-- columns: has_args
-- row: true
-- end-expected
SELECT pg_get_function_arguments(oid) IS NOT NULL AND pg_get_function_arguments(oid) <> '' AS has_args
FROM pg_proc WHERE proname = 'catmeta_out_params' AND pronamespace = 'catmeta_test'::regnamespace;

-- ============================================================================
-- SECTION C: pg_get_function_identity_arguments()
-- ============================================================================

-- ============================================================================
-- 10. pg_get_function_identity_arguments: identifies by input args only
-- ============================================================================

-- note: Identity arguments exclude OUT params — used for DROP FUNCTION
-- begin-expected
-- columns: has_ident
-- row: true
-- end-expected
SELECT pg_get_function_identity_arguments(oid) IS NOT NULL
  AND pg_get_function_identity_arguments(oid) <> '' AS has_ident
FROM pg_proc WHERE proname = 'catmeta_out_params' AND pronamespace = 'catmeta_test'::regnamespace;

-- ============================================================================
-- SECTION D: pg_describe_object()
-- ============================================================================

-- ============================================================================
-- 11. pg_describe_object: describe a function
-- ============================================================================

-- note: classid 1255 = pg_proc, objsubid 0
-- begin-expected
-- columns: has_desc
-- row: true
-- end-expected
SELECT pg_describe_object(1255, oid, 0) IS NOT NULL
  AND pg_describe_object(1255, oid, 0) <> '' AS has_desc
FROM pg_proc WHERE proname = 'catmeta_add' AND pronamespace = 'catmeta_test'::regnamespace;

-- begin-expected
-- columns: desc_has_func
-- row: true
-- end-expected
SELECT pg_describe_object(1255, oid, 0) LIKE '%catmeta_add%' AS desc_has_func
FROM pg_proc WHERE proname = 'catmeta_add' AND pronamespace = 'catmeta_test'::regnamespace;

-- ============================================================================
-- 12. pg_describe_object: describe a table
-- ============================================================================

CREATE TABLE catmeta_tbl (id integer PRIMARY KEY, val text);

-- note: classid 1259 = pg_class
-- begin-expected
-- columns: has_desc
-- row: true
-- end-expected
SELECT pg_describe_object(1259, oid, 0) IS NOT NULL
  AND pg_describe_object(1259, oid, 0) <> '' AS has_desc
FROM pg_class WHERE relname = 'catmeta_tbl' AND relnamespace = 'catmeta_test'::regnamespace;

-- ============================================================================
-- 13. pg_describe_object: describe a schema
-- ============================================================================

-- note: classid 2615 = pg_namespace
-- begin-expected
-- columns: has_desc
-- row: true
-- end-expected
SELECT pg_describe_object(2615, oid, 0) IS NOT NULL
  AND pg_describe_object(2615, oid, 0) <> '' AS has_desc
FROM pg_namespace WHERE nspname = 'catmeta_test';

-- ============================================================================
-- SECTION E: pg_get_constraintdef()
-- ============================================================================

-- ============================================================================
-- 14. pg_get_constraintdef: PRIMARY KEY
-- ============================================================================

-- begin-expected
-- columns: has_def
-- row: true
-- end-expected
SELECT pg_get_constraintdef(oid) IS NOT NULL AND pg_get_constraintdef(oid) <> '' AS has_def
FROM pg_constraint
WHERE conrelid = 'catmeta_test.catmeta_tbl'::regclass AND contype = 'p';

-- begin-expected
-- columns: has_pk
-- row: true
-- end-expected
SELECT pg_get_constraintdef(oid) LIKE '%PRIMARY KEY%' AS has_pk
FROM pg_constraint
WHERE conrelid = 'catmeta_test.catmeta_tbl'::regclass AND contype = 'p';

-- ============================================================================
-- 15. pg_get_constraintdef: CHECK constraint
-- ============================================================================

CREATE TABLE catmeta_chk (val integer CONSTRAINT val_pos CHECK (val > 0));

-- begin-expected
-- columns: has_check
-- row: true
-- end-expected
SELECT pg_get_constraintdef(oid) LIKE '%val > 0%' OR pg_get_constraintdef(oid) LIKE '%val) > 0%' AS has_check
FROM pg_constraint
WHERE conrelid = 'catmeta_test.catmeta_chk'::regclass AND contype = 'c';

-- ============================================================================
-- 16. pg_get_constraintdef: FOREIGN KEY
-- ============================================================================

CREATE TABLE catmeta_fk_parent (id integer PRIMARY KEY);
CREATE TABLE catmeta_fk_child (id integer, pid integer REFERENCES catmeta_fk_parent(id));

-- begin-expected
-- columns: has_fk
-- row: true
-- end-expected
SELECT pg_get_constraintdef(oid) LIKE '%FOREIGN KEY%' OR pg_get_constraintdef(oid) LIKE '%REFERENCES%' AS has_fk
FROM pg_constraint
WHERE conrelid = 'catmeta_test.catmeta_fk_child'::regclass AND contype = 'f';

-- ============================================================================
-- SECTION F: pg_get_indexdef()
-- ============================================================================

-- ============================================================================
-- 17. pg_get_indexdef: default index
-- ============================================================================

CREATE TABLE catmeta_idx_tbl (id integer, val text);
CREATE INDEX catmeta_idx_val ON catmeta_idx_tbl (val);

-- begin-expected
-- columns: has_def
-- row: true
-- end-expected
SELECT pg_get_indexdef(oid) IS NOT NULL AND pg_get_indexdef(oid) <> '' AS has_def
FROM pg_class WHERE relname = 'catmeta_idx_val';

-- begin-expected
-- columns: has_idx_name
-- row: true
-- end-expected
SELECT pg_get_indexdef(oid) LIKE '%catmeta_idx_val%' AS has_idx_name
FROM pg_class WHERE relname = 'catmeta_idx_val';

-- ============================================================================
-- 18. pg_get_indexdef: unique index
-- ============================================================================

CREATE UNIQUE INDEX catmeta_idx_uniq ON catmeta_idx_tbl (id);

-- begin-expected
-- columns: has_unique
-- row: true
-- end-expected
SELECT pg_get_indexdef(oid) LIKE '%UNIQUE%' AS has_unique
FROM pg_class WHERE relname = 'catmeta_idx_uniq';

-- ============================================================================
-- SECTION G: pg_get_viewdef()
-- ============================================================================

-- ============================================================================
-- 19. pg_get_viewdef: simple view
-- ============================================================================

CREATE VIEW catmeta_view AS SELECT id, val FROM catmeta_tbl WHERE id > 0;

-- begin-expected
-- columns: has_def
-- row: true
-- end-expected
SELECT pg_get_viewdef(oid) IS NOT NULL AND pg_get_viewdef(oid) <> '' AS has_def
FROM pg_class WHERE relname = 'catmeta_view';

-- ============================================================================
-- 20. pg_get_viewdef with pretty-print flag
-- ============================================================================

-- begin-expected
-- columns: has_def
-- row: true
-- end-expected
SELECT pg_get_viewdef('catmeta_test.catmeta_view'::regclass, true) IS NOT NULL AS has_def;

-- ============================================================================
-- SECTION H: Other catalog functions
-- ============================================================================

-- ============================================================================
-- 21. pg_get_serial_sequence: serial column
-- ============================================================================

CREATE TABLE catmeta_serial (id serial PRIMARY KEY, val text);

-- begin-expected
-- columns: has_seq
-- row: true
-- end-expected
SELECT pg_get_serial_sequence('catmeta_test.catmeta_serial', 'id') IS NOT NULL AS has_seq;

-- ============================================================================
-- 22. pg_get_expr: expression from pg_attrdef (default values)
-- ============================================================================

-- begin-expected
-- columns: has_expr
-- row: true
-- end-expected
SELECT pg_get_expr(adbin, adrelid) IS NOT NULL AND pg_get_expr(adbin, adrelid) <> '' AS has_expr
FROM pg_attrdef
WHERE adrelid = 'catmeta_test.catmeta_serial'::regclass;

-- ============================================================================
-- 23. format_type: type formatting
-- ============================================================================

-- begin-expected
-- columns: t1, t2, t3
-- row: integer, text, boolean
-- end-expected
SELECT
  format_type(23, -1) AS t1,
  format_type(25, -1) AS t2,
  format_type(16, -1) AS t3;

-- begin-expected
-- columns: varchar_type
-- row: character varying(100)
-- end-expected
SELECT format_type(1043, 104) AS varchar_type;

-- ============================================================================
-- 24. pg_typeof: runtime type detection
-- ============================================================================

-- begin-expected
-- columns: t1, t2, t3, t4
-- row: integer, text, boolean, numeric
-- end-expected
SELECT
  pg_typeof(1) AS t1,
  pg_typeof('hello'::text) AS t2,
  pg_typeof(true) AS t3,
  pg_typeof(3.14) AS t4;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA catmeta_test CASCADE;
SET search_path = public;
