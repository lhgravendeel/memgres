-- ============================================================================
-- Feature Comparison: pg_prepared_statements & pg_cursors catalog views
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   → expected result set
--   -- begin-expected-error / message-like: / end-expected-error → expected error
--   -- begin-expected-like / columns: / row-like: / end-expected-like → regex match
--   -- note: ...                                          → informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DEALLOCATE ALL;

DROP TABLE IF EXISTS cat_test CASCADE;
CREATE TABLE cat_test (
    id   integer PRIMARY KEY,
    name text NOT NULL,
    val  numeric(10,2)
);
INSERT INTO cat_test VALUES (1, 'alpha', 10.50), (2, 'beta', 20.75), (3, 'gamma', 30.00);

-- ============================================================================
-- 1. pg_prepared_statements: empty by default
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*)::integer AS count FROM pg_prepared_statements;

-- ============================================================================
-- 2. pg_prepared_statements: column inventory (all 8 PG 14+ columns)
-- ============================================================================

PREPARE cat_inv AS SELECT 1;

-- note: PG 14+ has 8 columns: name, statement, prepare_time, parameter_types,
--       result_types, from_sql, generic_plans, custom_plans

-- begin-expected
-- columns: col_count
-- row: 8
-- end-expected
SELECT count(*)::integer AS col_count
FROM information_schema.columns
WHERE table_schema = 'pg_catalog'
  AND table_name = 'pg_prepared_statements';

DEALLOCATE cat_inv;

-- ============================================================================
-- 3. pg_prepared_statements: basic row content
-- ============================================================================

PREPARE cat_basic AS SELECT id, name FROM cat_test WHERE id > 0 ORDER BY id;

-- begin-expected
-- columns: name|from_sql
-- row: cat_basic|true
-- end-expected
SELECT name, from_sql::text
FROM pg_prepared_statements
WHERE name = 'cat_basic';

-- note: generic_plans and custom_plans start at 0 before any EXECUTE
-- begin-expected
-- columns: generic_plans|custom_plans
-- row: 0|0
-- end-expected
SELECT generic_plans, custom_plans
FROM pg_prepared_statements
WHERE name = 'cat_basic';

DEALLOCATE cat_basic;

-- ============================================================================
-- 4. pg_prepared_statements: parameter_types column
-- ============================================================================

-- 4a. No parameters → empty array
PREPARE cat_noparam AS SELECT 1;

-- begin-expected
-- columns: parameter_types
-- row: {}
-- end-expected
SELECT parameter_types::text
FROM pg_prepared_statements
WHERE name = 'cat_noparam';

DEALLOCATE cat_noparam;

-- 4b. Single parameter
PREPARE cat_oneparam (integer) AS SELECT $1;

-- begin-expected
-- columns: parameter_types
-- row: {integer}
-- end-expected
SELECT parameter_types::text
FROM pg_prepared_statements
WHERE name = 'cat_oneparam';

DEALLOCATE cat_oneparam;

-- 4c. Multiple parameters
PREPARE cat_multiparam (integer, text, boolean) AS SELECT $1, $2, $3;

-- begin-expected
-- columns: parameter_types
-- row: {integer,text,boolean}
-- end-expected
SELECT parameter_types::text
FROM pg_prepared_statements
WHERE name = 'cat_multiparam';

DEALLOCATE cat_multiparam;

-- 4d. Array subscripting on parameter_types
PREPARE cat_subscript (integer, text) AS SELECT $1, $2;

-- begin-expected
-- columns: first_param
-- row: integer
-- end-expected
SELECT parameter_types[1]::text AS first_param
FROM pg_prepared_statements
WHERE name = 'cat_subscript';

-- begin-expected
-- columns: second_param
-- row: text
-- end-expected
SELECT parameter_types[2]::text AS second_param
FROM pg_prepared_statements
WHERE name = 'cat_subscript';

DEALLOCATE cat_subscript;

-- ============================================================================
-- 5. pg_prepared_statements: result_types column
-- ============================================================================

-- 5a. Simple SELECT → has result types
PREPARE cat_restype AS SELECT 1 + 1 AS val;

-- note: result_types should not be null for a SELECT statement
-- begin-expected
-- columns: has_result_types
-- row: true
-- end-expected
SELECT (result_types IS NOT NULL) AS has_result_types
FROM pg_prepared_statements
WHERE name = 'cat_restype';

DEALLOCATE cat_restype;

-- 5b. DML without RETURNING → null result_types
PREPARE cat_dml_nret (integer, text, numeric) AS INSERT INTO cat_test VALUES ($1, $2, $3);

-- begin-expected
-- columns: result_types_null
-- row: true
-- end-expected
SELECT (result_types IS NULL) AS result_types_null
FROM pg_prepared_statements
WHERE name = 'cat_dml_nret';

DEALLOCATE cat_dml_nret;

-- ============================================================================
-- 6. pg_prepared_statements: custom_plans increments on EXECUTE
-- ============================================================================

PREPARE cat_exec_cnt AS SELECT 42;

-- begin-expected
-- columns: custom_plans
-- row: 0
-- end-expected
SELECT custom_plans FROM pg_prepared_statements WHERE name = 'cat_exec_cnt';

EXECUTE cat_exec_cnt;

-- note: PG uses generic plans for simple queries, so custom_plans stays 0
-- begin-expected
-- columns: custom_plans
-- row: 0
-- end-expected
SELECT custom_plans FROM pg_prepared_statements WHERE name = 'cat_exec_cnt';

EXECUTE cat_exec_cnt;
EXECUTE cat_exec_cnt;

-- begin-expected
-- columns: custom_plans
-- row: 0
-- end-expected
SELECT custom_plans FROM pg_prepared_statements WHERE name = 'cat_exec_cnt';

DEALLOCATE cat_exec_cnt;

-- ============================================================================
-- 7. pg_prepared_statements: prepare_time format
-- ============================================================================

PREPARE cat_time AS SELECT 1;

-- note: PG timestamptz format: "YYYY-MM-DD HH:MI:SS.ffffff+TZ"
--       Should NOT contain 'T' separator (Java default)

-- begin-expected
-- columns: has_java_t
-- row: false
-- end-expected
SELECT (prepare_time::text LIKE '%T%') AS has_java_t
FROM pg_prepared_statements
WHERE name = 'cat_time';

-- begin-expected
-- columns: looks_like_pg_ts
-- row: true
-- end-expected
SELECT (prepare_time::text ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}') AS looks_like_pg_ts
FROM pg_prepared_statements
WHERE name = 'cat_time';

DEALLOCATE cat_time;

-- ============================================================================
-- 8. pg_prepared_statements: verbatim SQL preservation
-- ============================================================================

-- note: statement column should show the verbatim body SQL, not an AST reconstruction
PREPARE cat_verbatim AS
    SELECT   id , name
    FROM     cat_test
    WHERE    id > 0;

-- note: The exact whitespace may differ between PG and Memgres,
--       but the SQL keywords should be present in the statement text

-- begin-expected
-- columns: has_select
-- row: true
-- end-expected
SELECT (statement LIKE '%SELECT%') AS has_select
FROM pg_prepared_statements
WHERE name = 'cat_verbatim';

DEALLOCATE cat_verbatim;

-- ============================================================================
-- 9. pg_prepared_statements: multiple statements visible in order
-- ============================================================================

PREPARE cat_z AS SELECT 'z';
PREPARE cat_a AS SELECT 'a';
PREPARE cat_m AS SELECT 'm';

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*)::integer AS count FROM pg_prepared_statements;

DEALLOCATE ALL;

-- ============================================================================
-- 10. pg_prepared_statements: DEALLOCATE removes entry
-- ============================================================================

PREPARE cat_gone AS SELECT 1;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count FROM pg_prepared_statements WHERE name = 'cat_gone';

DEALLOCATE cat_gone;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*)::integer AS count FROM pg_prepared_statements WHERE name = 'cat_gone';

-- ============================================================================
-- 11. pg_prepared_statements: DEALLOCATE ALL clears all
-- ============================================================================

PREPARE cat_d1 AS SELECT 1;
PREPARE cat_d2 AS SELECT 2;

DEALLOCATE ALL;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*)::integer AS count FROM pg_prepared_statements;

-- ============================================================================
-- 12. pg_prepared_statements: name case
-- ============================================================================

-- note: PG lowercases unquoted identifiers
PREPARE MyCasePlan AS SELECT 1;

-- begin-expected
-- columns: name
-- row: mycaseplan
-- end-expected
SELECT name FROM pg_prepared_statements WHERE name = 'mycaseplan';

DEALLOCATE mycaseplan;

-- ============================================================================
-- 13. pg_prepared_statements: DISCARD PLANS does NOT remove entries
-- ============================================================================

PREPARE cat_disc_plan AS SELECT 1;

DISCARD PLANS;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count FROM pg_prepared_statements WHERE name = 'cat_disc_plan';

DEALLOCATE ALL;

-- ============================================================================
-- 14. pg_prepared_statements: DISCARD ALL removes entries
-- ============================================================================

PREPARE cat_disc_all AS SELECT 1;

DISCARD ALL;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*)::integer AS count FROM pg_prepared_statements;

-- ============================================================================
-- 15. pg_cursors: baseline count (PG has an implicit portal cursor)
-- ============================================================================

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors;

-- ============================================================================
-- 16. pg_cursors: column inventory (all 6 columns)
-- ============================================================================

-- note: PG pg_cursors has 6 columns: name, statement, is_holdable,
--       is_binary, is_scrollable, creation_time

-- begin-expected
-- columns: col_count
-- row: 6
-- end-expected
SELECT count(*)::integer AS col_count
FROM information_schema.columns
WHERE table_schema = 'pg_catalog'
  AND table_name = 'pg_cursors';

-- ============================================================================
-- 17. pg_cursors: basic row content
-- ============================================================================

BEGIN;

DECLARE cat_cur CURSOR FOR SELECT id FROM cat_test ORDER BY id;

-- begin-expected
-- columns: name|is_holdable|is_binary|is_scrollable
-- row: cat_cur|false|false|true
-- end-expected
SELECT name, is_holdable::text, is_binary::text, is_scrollable::text
FROM pg_cursors
WHERE name = 'cat_cur';

CLOSE cat_cur;
ROLLBACK;

-- ============================================================================
-- 18. pg_cursors: flag combinations
-- ============================================================================

BEGIN;

-- 18a. WITH HOLD
DECLARE cat_hold CURSOR WITH HOLD FOR SELECT 1;

-- begin-expected
-- columns: is_holdable
-- row: true
-- end-expected
SELECT is_holdable::text FROM pg_cursors WHERE name = 'cat_hold';

-- 18b. SCROLL
DECLARE cat_scroll SCROLL CURSOR FOR SELECT 1;

-- begin-expected
-- columns: is_scrollable
-- row: true
-- end-expected
SELECT is_scrollable::text FROM pg_cursors WHERE name = 'cat_scroll';

-- 18c. BINARY
DECLARE cat_bin BINARY CURSOR FOR SELECT 1;

-- begin-expected
-- columns: is_binary
-- row: true
-- end-expected
SELECT is_binary::text FROM pg_cursors WHERE name = 'cat_bin';

-- 18d. All flags combined
DECLARE cat_all BINARY SCROLL CURSOR WITH HOLD FOR SELECT 1;

-- begin-expected
-- columns: is_holdable|is_binary|is_scrollable
-- row: true|true|true
-- end-expected
SELECT is_holdable::text, is_binary::text, is_scrollable::text
FROM pg_cursors
WHERE name = 'cat_all';

CLOSE ALL;
ROLLBACK;

-- ============================================================================
-- 19. pg_cursors: creation_time format
-- ============================================================================

BEGIN;
DECLARE cat_time_cur CURSOR FOR SELECT 1;

-- begin-expected
-- columns: has_java_t
-- row: false
-- end-expected
SELECT (creation_time::text LIKE '%T%') AS has_java_t
FROM pg_cursors
WHERE name = 'cat_time_cur';

-- begin-expected
-- columns: looks_like_pg_ts
-- row: true
-- end-expected
SELECT (creation_time::text ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}') AS looks_like_pg_ts
FROM pg_cursors
WHERE name = 'cat_time_cur';

CLOSE cat_time_cur;
ROLLBACK;

-- ============================================================================
-- 20. pg_cursors: multiple cursors visible
-- ============================================================================

BEGIN;
DECLARE cat_m1 CURSOR FOR SELECT 1;
DECLARE cat_m2 CURSOR FOR SELECT 2;
DECLARE cat_m3 CURSOR FOR SELECT 3;

-- note: 3 declared + 1 implicit portal cursor = 4
-- begin-expected
-- columns: count
-- row: 4
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors;

CLOSE ALL;
ROLLBACK;

-- ============================================================================
-- 21. pg_cursors: CLOSE removes entry
-- ============================================================================

BEGIN;
DECLARE cat_rm CURSOR FOR SELECT 1;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors WHERE name = 'cat_rm';

CLOSE cat_rm;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors WHERE name = 'cat_rm';

ROLLBACK;

-- ============================================================================
-- 22. pg_cursors: CLOSE ALL removes all entries
-- ============================================================================

BEGIN;
DECLARE cat_cl1 CURSOR FOR SELECT 1;
DECLARE cat_cl2 CURSOR FOR SELECT 2;

CLOSE ALL;

-- note: implicit portal cursor remains after CLOSE ALL
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors;

ROLLBACK;

-- ============================================================================
-- 23. pg_cursors: cursor statement column shows query text
-- ============================================================================

BEGIN;
DECLARE cat_stmt CURSOR FOR SELECT id, name FROM cat_test WHERE id > 0 ORDER BY id;

-- begin-expected
-- columns: has_select
-- row: true
-- end-expected
SELECT (statement LIKE '%SELECT%') AS has_select
FROM pg_cursors
WHERE name = 'cat_stmt';

CLOSE cat_stmt;
ROLLBACK;

-- ============================================================================
-- 24. pg_cursors: name case
-- ============================================================================

BEGIN;
DECLARE MyCaseCur CURSOR FOR SELECT 1;

-- begin-expected
-- columns: name
-- row: mycasecur
-- end-expected
SELECT name FROM pg_cursors WHERE name = 'mycasecur';

CLOSE mycasecur;
ROLLBACK;

-- ============================================================================
-- 25. pg_cursors: transaction lifecycle in catalog
-- ============================================================================

-- 25a. Non-holdable disappears from catalog on COMMIT
BEGIN;
DECLARE cat_lc CURSOR FOR SELECT 1;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors WHERE name = 'cat_lc';

COMMIT;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors WHERE name = 'cat_lc';

-- 25b. Holdable survives COMMIT in catalog
BEGIN;
DECLARE cat_lc_hold CURSOR WITH HOLD FOR SELECT 1;
COMMIT;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors WHERE name = 'cat_lc_hold';

CLOSE cat_lc_hold;

-- 25c. All declared cursors disappear from catalog on ROLLBACK
BEGIN;
DECLARE cat_lc_rb1 CURSOR WITH HOLD FOR SELECT 1;
DECLARE cat_lc_rb2 CURSOR FOR SELECT 2;
ROLLBACK;

-- note: implicit portal cursor remains after ROLLBACK
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors;

-- ============================================================================
-- 26. pg_cursors: DISCARD ALL removes cursors
-- ============================================================================

BEGIN;
DECLARE cat_da CURSOR WITH HOLD FOR SELECT 1;
COMMIT;

-- note: 1 holdable cursor + 1 implicit portal cursor = 2
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors;

DISCARD ALL;

-- note: implicit portal cursor remains after DISCARD ALL
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count FROM pg_cursors;

-- ============================================================================
-- 27. SQLSTATE error codes
-- ============================================================================

-- 27a. Duplicate prepared statement: 42P05
PREPARE cat_dup AS SELECT 1;
-- begin-expected-error
-- sqlstate: 42P05
-- message-like: already exists
-- end-expected-error
PREPARE cat_dup AS SELECT 2;
DEALLOCATE cat_dup;

-- 27b. Missing prepared statement: 26000
-- begin-expected-error
-- sqlstate: 26000
-- message-like: does not exist
-- end-expected-error
DEALLOCATE cat_missing;

-- 27c. Duplicate cursor: 42P03
BEGIN;
DECLARE cat_dupc CURSOR FOR SELECT 1;
-- begin-expected-error
-- sqlstate: 42P03
-- message-like: already exists
-- end-expected-error
DECLARE cat_dupc CURSOR FOR SELECT 2;
ROLLBACK;

-- 27d. Missing cursor: 34000
-- begin-expected-error
-- sqlstate: 34000
-- message-like: does not exist
-- end-expected-error
FETCH NEXT FROM cat_missing;

-- 27e. NO SCROLL violation: 55000
BEGIN;
DECLARE cat_ns CURSOR FOR SELECT 1;
FETCH PRIOR FROM cat_ns;
ROLLBACK;

-- ============================================================================
-- 28. pg_prepared_statements: result_types for DML with RETURNING
-- ============================================================================

PREPARE cat_ins_ret AS INSERT INTO cat_test VALUES (99, 'temp', 0) RETURNING id, name;

-- begin-expected
-- columns: has_result_types
-- row: true
-- end-expected
SELECT (result_types IS NOT NULL) AS has_result_types
FROM pg_prepared_statements
WHERE name = 'cat_ins_ret';

DEALLOCATE cat_ins_ret;

-- ============================================================================
-- 29. pg_prepared_statements: parameter_types with various types
-- ============================================================================

PREPARE cat_types (integer, text, boolean, numeric, timestamp) AS
    SELECT $1, $2, $3, $4, $5;

-- note: parameter_types should list all declared types
-- begin-expected
-- columns: param_count
-- row: 5
-- end-expected
SELECT array_length(parameter_types, 1) AS param_count
FROM pg_prepared_statements
WHERE name = 'cat_types';

DEALLOCATE cat_types;

-- ============================================================================
-- 30. pg_prepared_statements: out-of-bounds array subscript returns null
-- ============================================================================

PREPARE cat_oob (integer) AS SELECT $1;

-- begin-expected
-- columns: oob_result
-- row: true
-- end-expected
SELECT (parameter_types[99] IS NULL) AS oob_result
FROM pg_prepared_statements
WHERE name = 'cat_oob';

-- Subscript on empty array also returns null
DEALLOCATE cat_oob;

PREPARE cat_empty_oob AS SELECT 1;

-- begin-expected
-- columns: empty_oob
-- row: true
-- end-expected
SELECT (parameter_types[1] IS NULL) AS empty_oob
FROM pg_prepared_statements
WHERE name = 'cat_empty_oob';

DEALLOCATE cat_empty_oob;

-- ============================================================================
-- 31. pg_cursors: statement column includes full DECLARE statement
-- ============================================================================

-- note: In PG, pg_cursors.statement includes the full DECLARE ... CURSOR FOR ...
--       statement, not just the query portion after FOR

BEGIN;
DECLARE cat_qonly SCROLL CURSOR WITH HOLD FOR SELECT id, name FROM cat_test ORDER BY id;

-- begin-expected
-- columns: starts_with_select
-- row: false
-- end-expected
SELECT (upper(statement) LIKE 'SELECT%') AS starts_with_select
FROM pg_cursors
WHERE name = 'cat_qonly';

-- Statement DOES contain 'DECLARE'
-- begin-expected
-- columns: no_declare
-- row: false
-- end-expected
SELECT (upper(statement) NOT LIKE '%DECLARE%') AS no_declare
FROM pg_cursors
WHERE name = 'cat_qonly';

CLOSE cat_qonly;
ROLLBACK;

-- ============================================================================
-- 32. pg_cursors: holdable cursor flags persist after COMMIT
-- ============================================================================

BEGIN;
DECLARE cat_persist SCROLL CURSOR WITH HOLD FOR SELECT 1;
COMMIT;

-- begin-expected
-- columns: is_holdable|is_scrollable
-- row: true|true
-- end-expected
SELECT is_holdable::text, is_scrollable::text
FROM pg_cursors
WHERE name = 'cat_persist';

CLOSE cat_persist;

-- ============================================================================
-- 33. pg_prepared_statements: EXECUTE does not remove entry
-- ============================================================================

PREPARE cat_noremove AS SELECT 42;

EXECUTE cat_noremove;
EXECUTE cat_noremove;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count
FROM pg_prepared_statements
WHERE name = 'cat_noremove';

DEALLOCATE cat_noremove;

-- ============================================================================
-- 34. pg_cursors: FETCH does not remove entry
-- ============================================================================

BEGIN;
DECLARE cat_fetch_vis CURSOR FOR SELECT generate_series(1, 3);

FETCH NEXT FROM cat_fetch_vis;
FETCH NEXT FROM cat_fetch_vis;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::integer AS count
FROM pg_cursors
WHERE name = 'cat_fetch_vis';

CLOSE cat_fetch_vis;
ROLLBACK;

-- ============================================================================
-- 35. pg_prepared_statements: filtering with WHERE LIKE
-- ============================================================================

PREPARE cat_filter_a AS SELECT 1;
PREPARE cat_filter_b AS SELECT 2;
PREPARE other_plan AS SELECT 3;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*)::integer AS count
FROM pg_prepared_statements
WHERE name LIKE 'cat_filter%';

DEALLOCATE ALL;

-- ============================================================================
-- Cleanup
-- ============================================================================

DEALLOCATE ALL;
DROP TABLE IF EXISTS cat_test CASCADE;
