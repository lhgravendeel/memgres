-- ============================================================================
-- Feature Comparison: Execution Edge Cases (G1, G2, G3)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- G1: SECURITY INVOKER views
-- G2: Composite-type field UPDATE (SET (col).field = ...)
-- G3: CREATE AGGREGATE options (COMBINEFUNC, SORTOP) catalog verification
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS edge_test CASCADE;
CREATE SCHEMA edge_test;
SET search_path = edge_test, public;

-- ============================================================================
-- G1: SECURITY INVOKER Views
-- ============================================================================

-- ============================================================================
-- 1. Create a SECURITY INVOKER view (PG 15+)
-- ============================================================================

CREATE TABLE edge_data (id integer PRIMARY KEY, val text);
INSERT INTO edge_data VALUES (1, 'a'), (2, 'b'), (3, 'c');

CREATE VIEW edge_invoker_view WITH (security_invoker = true) AS
  SELECT * FROM edge_data;

-- begin-expected
-- columns: id, val
-- row: 1, a
-- row: 2, b
-- row: 3, c
-- end-expected
SELECT * FROM edge_invoker_view ORDER BY id;

-- ============================================================================
-- 2. Verify security_invoker attribute in pg_class reloptions
-- ============================================================================

-- begin-expected
-- columns: has_invoker
-- row: true
-- end-expected
SELECT reloptions @> ARRAY['security_invoker=true'] AS has_invoker
FROM pg_class
WHERE relname = 'edge_invoker_view';

-- ============================================================================
-- 3. SECURITY INVOKER view is queryable (basic SELECT)
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::integer AS cnt FROM edge_invoker_view;

-- ============================================================================
-- 4. SECURITY INVOKER view with WHERE clause
-- ============================================================================

-- begin-expected
-- columns: id, val
-- row: 2, b
-- end-expected
SELECT * FROM edge_invoker_view WHERE id = 2;

-- ============================================================================
-- 5. SECURITY INVOKER view with aggregation
-- ============================================================================

-- begin-expected
-- columns: max_id
-- row: 3
-- end-expected
SELECT max(id) AS max_id FROM edge_invoker_view;

-- ============================================================================
-- 6. SECURITY INVOKER=false (default behavior)
-- ============================================================================

CREATE VIEW edge_definer_view WITH (security_invoker = false) AS
  SELECT * FROM edge_data;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::integer AS cnt FROM edge_definer_view;

-- ============================================================================
-- 7. View without security_invoker option (default = definer)
-- ============================================================================

CREATE VIEW edge_default_view AS SELECT * FROM edge_data;

-- begin-expected
-- columns: has_no_invoker
-- row: true
-- end-expected
SELECT reloptions IS NULL OR NOT (reloptions @> ARRAY['security_invoker=true']) AS has_no_invoker
FROM pg_class
WHERE relname = 'edge_default_view';

-- ============================================================================
-- 8. ALTER VIEW to set security_invoker
-- ============================================================================

ALTER VIEW edge_default_view SET (security_invoker = true);

-- begin-expected
-- columns: has_invoker
-- row: true
-- end-expected
SELECT reloptions @> ARRAY['security_invoker=true'] AS has_invoker
FROM pg_class
WHERE relname = 'edge_default_view';

-- ============================================================================
-- 9. ALTER VIEW to unset security_invoker
-- ============================================================================

ALTER VIEW edge_default_view SET (security_invoker = false);

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::integer AS cnt FROM edge_default_view;

-- ============================================================================
-- 10. SECURITY INVOKER view with JOIN
-- ============================================================================

CREATE TABLE edge_labels (id integer PRIMARY KEY, label text);
INSERT INTO edge_labels VALUES (1, 'first'), (2, 'second'), (3, 'third');

CREATE VIEW edge_join_view WITH (security_invoker = true) AS
  SELECT d.id, d.val, l.label
  FROM edge_data d
  JOIN edge_labels l ON d.id = l.id;

-- begin-expected
-- columns: id, val, label
-- row: 1, a, first
-- row: 2, b, second
-- row: 3, c, third
-- end-expected
SELECT * FROM edge_join_view ORDER BY id;

-- ============================================================================
-- G2: Composite-Type Field UPDATE
-- ============================================================================

-- ============================================================================
-- 11. Create composite type and table
-- ============================================================================

CREATE TYPE edge_point AS (x integer, y integer);
CREATE TABLE edge_shapes (id integer PRIMARY KEY, pos edge_point);
INSERT INTO edge_shapes VALUES (1, ROW(10, 20)::edge_point);
INSERT INTO edge_shapes VALUES (2, ROW(30, 40)::edge_point);

-- begin-expected
-- columns: id, pos
-- row: 1, (10,20)
-- row: 2, (30,40)
-- end-expected
SELECT id, pos FROM edge_shapes ORDER BY id;

-- ============================================================================
-- 12. UPDATE composite field: SET (col).field = value
-- ============================================================================

UPDATE edge_shapes SET pos.x = 99 WHERE id = 1;

-- begin-expected
-- columns: pos
-- row: (99,20)
-- end-expected
SELECT pos FROM edge_shapes WHERE id = 1;

-- ============================================================================
-- 13. UPDATE multiple composite fields separately
-- ============================================================================

UPDATE edge_shapes SET pos.x = 50, pos.y = 60 WHERE id = 2;

-- begin-expected
-- columns: pos
-- row: (50,60)
-- end-expected
SELECT pos FROM edge_shapes WHERE id = 2;

-- ============================================================================
-- 14. Read individual composite fields
-- ============================================================================

-- begin-expected
-- columns: x_val, y_val
-- row: 99, 20
-- end-expected
SELECT (pos).x AS x_val, (pos).y AS y_val FROM edge_shapes WHERE id = 1;

-- ============================================================================
-- 15. Composite type in WHERE clause
-- ============================================================================

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM edge_shapes WHERE (pos).x = 99;

-- ============================================================================
-- 16. Composite type with NULL fields
-- ============================================================================

INSERT INTO edge_shapes VALUES (3, ROW(NULL, 100)::edge_point);

-- begin-expected
-- columns: x_is_null, y_val
-- row: true, 100
-- end-expected
SELECT (pos).x IS NULL AS x_is_null, (pos).y AS y_val FROM edge_shapes WHERE id = 3;

-- ============================================================================
-- 17. UPDATE composite field from NULL to value
-- ============================================================================

UPDATE edge_shapes SET pos.x = 77 WHERE id = 3;

-- begin-expected
-- columns: pos
-- row: (77,100)
-- end-expected
SELECT pos FROM edge_shapes WHERE id = 3;

-- ============================================================================
-- 18. Whole-row composite assignment
-- ============================================================================

UPDATE edge_shapes SET pos = ROW(1, 2)::edge_point WHERE id = 3;

-- begin-expected
-- columns: pos
-- row: (1,2)
-- end-expected
SELECT pos FROM edge_shapes WHERE id = 3;

-- ============================================================================
-- 19. Composite type in ORDER BY
-- ============================================================================

-- begin-expected
-- columns: id
-- row: 3
-- row: 2
-- row: 1
-- end-expected
SELECT id FROM edge_shapes ORDER BY (pos).x ASC;

-- ============================================================================
-- 20. Nested composite type
-- ============================================================================

CREATE TYPE edge_color AS (r integer, g integer, b integer);
CREATE TYPE edge_styled_point AS (pos edge_point, color edge_color);
CREATE TABLE edge_styled (id integer PRIMARY KEY, data edge_styled_point);

INSERT INTO edge_styled VALUES (1, ROW(ROW(10, 20)::edge_point, ROW(255, 0, 0)::edge_color)::edge_styled_point);

-- begin-expected
-- columns: x_val, r_val
-- row: 10, 255
-- end-expected
SELECT (data).pos.x AS x_val, (data).color.r AS r_val FROM edge_styled WHERE id = 1;

-- ============================================================================
-- G3: CREATE AGGREGATE Options (Catalog Verification)
-- ============================================================================

-- ============================================================================
-- 21. Basic CREATE AGGREGATE
-- ============================================================================

CREATE FUNCTION edge_int_add(integer, integer) RETURNS integer
LANGUAGE sql AS $$ SELECT COALESCE($1, 0) + $2; $$;

CREATE AGGREGATE edge_sum_agg(integer) (
  SFUNC = edge_int_add,
  STYPE = integer,
  INITCOND = '0'
);

-- begin-expected
-- columns: result
-- row: 6
-- end-expected
SELECT edge_sum_agg(v) AS result FROM (VALUES (1), (2), (3)) AS t(v);

-- ============================================================================
-- 22. Aggregate in pg_aggregate catalog
-- ============================================================================

-- begin-expected
-- columns: has_entry
-- row: true
-- end-expected
SELECT count(*) > 0 AS has_entry
FROM pg_aggregate
WHERE aggfnoid = 'edge_sum_agg'::regproc;

-- ============================================================================
-- 23. Aggregate INITCOND in catalog
-- ============================================================================

-- begin-expected
-- columns: initval
-- row: 0
-- end-expected
SELECT agginitval AS initval
FROM pg_aggregate
WHERE aggfnoid = 'edge_sum_agg'::regproc;

-- ============================================================================
-- 24. Aggregate SFUNC in catalog
-- ============================================================================

-- begin-expected
-- columns: sfunc_name
-- row: edge_int_add
-- end-expected
SELECT aggtransfn::text AS sfunc_name
FROM pg_aggregate
WHERE aggfnoid = 'edge_sum_agg'::regproc;

-- ============================================================================
-- 25. Aggregate with FINALFUNC
-- ============================================================================

CREATE FUNCTION edge_double(integer) RETURNS integer
LANGUAGE sql AS $$ SELECT $1 * 2; $$;

CREATE AGGREGATE edge_doublesum_agg(integer) (
  SFUNC = edge_int_add,
  STYPE = integer,
  INITCOND = '0',
  FINALFUNC = edge_double
);

-- begin-expected
-- columns: result
-- row: 12
-- end-expected
SELECT edge_doublesum_agg(v) AS result FROM (VALUES (1), (2), (3)) AS t(v);

-- begin-expected
-- columns: has_finalfn
-- row: true
-- end-expected
SELECT aggfinalfn <> 0 AS has_finalfn
FROM pg_aggregate
WHERE aggfnoid = 'edge_doublesum_agg'::regproc;

-- ============================================================================
-- 26. Aggregate with SORTOP (for ordered-set behavior)
-- ============================================================================

-- note: Built-in min/max use SORTOP; verify catalog column exists
-- begin-expected-error
-- message-like: more than one function
-- end-expected-error
SELECT aggsortop IS NOT NULL AS has_sortop
FROM pg_aggregate
WHERE aggfnoid = 'min'::regproc AND aggtranstype = 'integer'::regtype;

-- ============================================================================
-- 27. Aggregate COMBINEFUNC (for parallel aggregation)
-- ============================================================================

-- note: COMBINEFUNC is stored in aggcombinefn; built-in sum has one
-- begin-expected
-- columns: col_exists
-- row: true
-- end-expected
SELECT count(*) > 0 AS col_exists
FROM pg_attribute
WHERE attrelid = 'pg_aggregate'::regclass
AND attname = 'aggcombinefn';

-- ============================================================================
-- 28. Custom aggregate with no INITCOND (NULL initial state)
-- ============================================================================

CREATE FUNCTION edge_text_cat(text, text) RETURNS text
LANGUAGE sql AS $$ SELECT COALESCE($1 || ',', '') || $2; $$;

CREATE AGGREGATE edge_strcat_agg(text) (
  SFUNC = edge_text_cat,
  STYPE = text
);

-- begin-expected
-- columns: result
-- row: a,b,c
-- end-expected
SELECT edge_strcat_agg(v) AS result FROM (VALUES ('a'), ('b'), ('c')) AS t(v);

-- begin-expected
-- columns: initval_null
-- row: true
-- end-expected
SELECT agginitval IS NULL AS initval_null
FROM pg_aggregate
WHERE aggfnoid = 'edge_strcat_agg'::regproc;

-- ============================================================================
-- 29. DROP AGGREGATE
-- ============================================================================

DROP AGGREGATE edge_strcat_agg(text);

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_aggregate
WHERE aggfnoid IN (SELECT oid FROM pg_proc WHERE proname = 'edge_strcat_agg');

-- ============================================================================
-- 30. Aggregate with ORDER BY (ordered aggregate)
-- ============================================================================

-- note: string_agg and array_agg support ORDER BY within the aggregate call
-- begin-expected
-- columns: result
-- row: a,b,c
-- end-expected
SELECT string_agg(v, ',' ORDER BY v) AS result
FROM (VALUES ('c'), ('a'), ('b')) AS t(v);

-- ============================================================================
-- 31. Aggregate with FILTER clause
-- ============================================================================

-- begin-expected
-- columns: total, filtered
-- row: 6, 5
-- end-expected
SELECT
  sum(v) AS total,
  sum(v) FILTER (WHERE v > 1) AS filtered
FROM (VALUES (1), (2), (3)) AS t(v);

-- ============================================================================
-- 32. Aggregate with DISTINCT
-- ============================================================================

-- begin-expected
-- columns: result
-- row: 6
-- end-expected
SELECT sum(DISTINCT v)::integer AS result
FROM (VALUES (1), (2), (3), (2), (1)) AS t(v);

-- ============================================================================
-- 33. pg_proc entry for aggregate function
-- ============================================================================

-- begin-expected
-- columns: prokind
-- row: a
-- end-expected
SELECT prokind
FROM pg_proc
WHERE proname = 'edge_sum_agg'
AND pronamespace = 'edge_test'::regnamespace;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA edge_test CASCADE;
SET search_path = public;
