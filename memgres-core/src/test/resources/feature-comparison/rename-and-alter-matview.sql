-- ============================================================================
-- Feature Comparison: Rename operations + ALTER MATERIALIZED VIEW
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS ren_test CASCADE;
CREATE SCHEMA ren_test;
SET search_path = ren_test, public;

-- ============================================================================
-- SECTION A: ALTER TABLE RENAME
-- ============================================================================

CREATE TABLE ren_t1 (id int PRIMARY KEY, val text);
INSERT INTO ren_t1 VALUES (1, 'hello');

ALTER TABLE ren_t1 RENAME TO ren_t1_new;

-- 1. Renamed table is accessible
-- begin-expected
-- columns: val
-- row: hello
-- end-expected
SELECT val FROM ren_t1_new WHERE id = 1;

-- 2. Old name no longer exists
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected
SELECT * FROM ren_t1;

-- ============================================================================
-- SECTION B: ALTER TABLE RENAME COLUMN
-- ============================================================================

ALTER TABLE ren_t1_new RENAME COLUMN val TO description;

-- 3. Renamed column accessible
-- begin-expected
-- columns: description
-- row: hello
-- end-expected
SELECT description FROM ren_t1_new WHERE id = 1;

-- 4. Old column name errors
-- begin-expected-error
-- sqlstate: 42703
-- end-expected
SELECT val FROM ren_t1_new;

-- ============================================================================
-- SECTION C: ALTER TABLE RENAME CONSTRAINT
-- ============================================================================

CREATE TABLE ren_c1 (id int CONSTRAINT ren_c1_pk PRIMARY KEY);

ALTER TABLE ren_c1 RENAME CONSTRAINT ren_c1_pk TO ren_c1_pk_new;

-- 5. Renamed constraint visible in pg_constraint
-- begin-expected
-- columns: conname
-- row: ren_c1_pk_new
-- end-expected
SELECT conname FROM pg_constraint WHERE conrelid = 'ren_test.ren_c1'::regclass AND contype = 'p';

-- ============================================================================
-- SECTION D: ALTER VIEW RENAME TO
-- ============================================================================

CREATE VIEW ren_v1 AS SELECT id, description FROM ren_t1_new;

ALTER VIEW ren_v1 RENAME TO ren_v1_new;

-- 6. Renamed view works
-- begin-expected
-- columns: id|description
-- row: 1|hello
-- end-expected
SELECT * FROM ren_v1_new;

-- 7. Old view name errors
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected
SELECT * FROM ren_v1;

-- ============================================================================
-- SECTION E: ALTER MATERIALIZED VIEW RENAME TO
-- ============================================================================

CREATE MATERIALIZED VIEW ren_mv1 AS SELECT id, description FROM ren_t1_new;

ALTER MATERIALIZED VIEW ren_mv1 RENAME TO ren_mv1_new;

-- 8. Renamed materialized view works
-- begin-expected
-- columns: id|description
-- row: 1|hello
-- end-expected
SELECT * FROM ren_mv1_new;

-- 9. Old matview name errors
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected
SELECT * FROM ren_mv1;

-- 10. REFRESH works after rename
REFRESH MATERIALIZED VIEW ren_mv1_new;

-- begin-expected
-- columns: id|description
-- row: 1|hello
-- end-expected
SELECT * FROM ren_mv1_new;

-- ============================================================================
-- SECTION F: ALTER MATERIALIZED VIEW IF EXISTS
-- ============================================================================

-- 11. IF EXISTS on nonexistent — no error
ALTER MATERIALIZED VIEW IF EXISTS ren_mv_nonexistent RENAME TO ren_mv_something;

-- 12. Without IF EXISTS on nonexistent — error
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected
ALTER MATERIALIZED VIEW ren_mv_nonexistent RENAME TO ren_mv_something;

-- ============================================================================
-- SECTION G: ALTER MATERIALIZED VIEW OWNER TO
-- ============================================================================

CREATE ROLE ren_test_role;

ALTER MATERIALIZED VIEW ren_mv1_new OWNER TO ren_test_role;

-- 13. Owner change reflected in pg_class
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (SELECT rolname FROM pg_roles WHERE oid = c.relowner) = 'ren_test_role' AS ok
FROM pg_class c WHERE c.relname = 'ren_mv1_new';

DROP ROLE ren_test_role;

-- ============================================================================
-- SECTION H: ALTER MATERIALIZED VIEW SET storage options (no-op in Memgres)
-- ============================================================================

-- 14. SET (fillfactor) is accepted
ALTER MATERIALIZED VIEW ren_mv1_new SET (fillfactor = 70);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*) AS cnt FROM ren_mv1_new;

-- ============================================================================
-- SECTION I: ALTER MATERIALIZED VIEW — invalid operations
-- ============================================================================

-- 15. Cannot INSERT into materialized view
-- begin-expected-error
-- end-expected
INSERT INTO ren_mv1_new VALUES (2, 'world');

-- 16. Cannot UPDATE materialized view
-- begin-expected-error
-- end-expected
UPDATE ren_mv1_new SET description = 'changed' WHERE id = 1;

-- 17. Cannot DELETE from materialized view
-- begin-expected-error
-- end-expected
DELETE FROM ren_mv1_new WHERE id = 1;

-- ============================================================================
-- SECTION J: ALTER FUNCTION RENAME TO
-- ============================================================================

CREATE FUNCTION ren_fn1(x int) RETURNS int LANGUAGE sql AS $$ SELECT x * 2 $$;

ALTER FUNCTION ren_fn1(int) RENAME TO ren_fn1_new;

-- 18. Renamed function works
-- begin-expected
-- columns: result
-- row: 10
-- end-expected
SELECT ren_fn1_new(5) AS result;

-- 19. Old function name errors
-- begin-expected-error
-- sqlstate: 42883
-- end-expected
SELECT ren_fn1(5);

-- ============================================================================
-- SECTION K: ALTER PROCEDURE RENAME TO
-- ============================================================================

CREATE TABLE ren_proc_log (msg text);

CREATE PROCEDURE ren_proc1(t text) LANGUAGE plpgsql AS $$
BEGIN INSERT INTO ren_proc_log VALUES (t); END;
$$;

ALTER PROCEDURE ren_proc1(text) RENAME TO ren_proc1_new;

CALL ren_proc1_new('test');

-- 20. Renamed procedure works
-- begin-expected
-- columns: msg
-- row: test
-- end-expected
SELECT msg FROM ren_proc_log;

-- ============================================================================
-- SECTION L: ALTER SEQUENCE RENAME TO
-- ============================================================================

CREATE SEQUENCE ren_seq1 START 100;

ALTER SEQUENCE ren_seq1 RENAME TO ren_seq1_new;

-- 21. Renamed sequence works
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT nextval('ren_seq1_new') >= 100 AS ok;

-- 22. Old sequence name errors
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected
SELECT nextval('ren_seq1');

-- ============================================================================
-- SECTION M: ALTER TYPE RENAME TO
-- ============================================================================

CREATE TYPE ren_color AS ENUM ('red', 'green', 'blue');

ALTER TYPE ren_color RENAME TO ren_color_new;

-- 23. Renamed type works in cast
-- begin-expected
-- columns: c
-- row: red
-- end-expected
SELECT 'red'::ren_color_new AS c;

-- ============================================================================
-- SECTION N: ALTER TYPE RENAME VALUE
-- ============================================================================

ALTER TYPE ren_color_new RENAME VALUE 'red' TO 'crimson';

-- 24. Renamed enum value works
-- begin-expected
-- columns: c
-- row: crimson
-- end-expected
SELECT 'crimson'::ren_color_new AS c;

-- 25. Old enum value errors
-- begin-expected-error
-- sqlstate: 22P02
-- end-expected
SELECT 'red'::ren_color_new;

-- ============================================================================
-- SECTION O: ALTER INDEX RENAME TO
-- ============================================================================

CREATE INDEX ren_idx1 ON ren_t1_new (description);

ALTER INDEX ren_idx1 RENAME TO ren_idx1_new;

-- 26. Renamed index visible in pg_indexes
-- begin-expected
-- columns: indexname
-- row: ren_idx1_new
-- end-expected
SELECT indexname FROM pg_indexes WHERE schemaname = 'ren_test' AND indexname = 'ren_idx1_new';

-- ============================================================================
-- SECTION P: ALTER DATABASE RENAME TO
-- ============================================================================

CREATE DATABASE ren_db1;

ALTER DATABASE ren_db1 RENAME TO ren_db2;

-- 27. Renamed database visible
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT count(*) > 0 AS ok FROM pg_database WHERE datname = 'ren_db2';

DROP DATABASE ren_db2;

-- ============================================================================
-- SECTION Q: ALTER EVENT TRIGGER RENAME TO
-- ============================================================================

CREATE OR REPLACE FUNCTION ren_evt_fn() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN NULL; END;
$$;

CREATE EVENT TRIGGER ren_evt1 ON ddl_command_end EXECUTE FUNCTION ren_evt_fn();

ALTER EVENT TRIGGER ren_evt1 RENAME TO ren_evt1_new;

-- 28. Renamed event trigger visible
-- begin-expected
-- columns: evtname
-- row: ren_evt1_new
-- end-expected
SELECT evtname FROM pg_event_trigger WHERE evtname = 'ren_evt1_new';

DROP EVENT TRIGGER ren_evt1_new;

-- ============================================================================
-- SECTION R: ALTER POLICY RENAME TO
-- ============================================================================

CREATE TABLE ren_rls_t (id int, owner_name text);
ALTER TABLE ren_rls_t ENABLE ROW LEVEL SECURITY;
CREATE POLICY ren_pol1 ON ren_rls_t FOR SELECT USING (true);

ALTER POLICY ren_pol1 ON ren_rls_t RENAME TO ren_pol1_new;

-- 29. Renamed policy visible
-- begin-expected
-- columns: polname
-- row: ren_pol1_new
-- end-expected
SELECT polname FROM pg_policy WHERE polrelid = 'ren_test.ren_rls_t'::regclass;

-- ============================================================================
-- SECTION S: ALTER STATISTICS RENAME TO
-- ============================================================================

CREATE TABLE ren_stat_t (a int, b int, c int);
CREATE STATISTICS ren_stat1 ON a, b FROM ren_stat_t;

ALTER STATISTICS ren_stat1 RENAME TO ren_stat1_new;

-- 30. Renamed statistics visible
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT count(*) > 0 AS ok FROM pg_statistic_ext WHERE stxname = 'ren_stat1_new';

-- ============================================================================
-- SECTION T: RENAME non-existent objects errors
-- ============================================================================

-- 31. Rename non-existent table
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected
ALTER TABLE ren_nonexistent RENAME TO ren_something;

-- 32. Rename non-existent view
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected
ALTER VIEW ren_nonexistent RENAME TO ren_something;

-- 33. Rename non-existent sequence
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected
ALTER SEQUENCE ren_nonexistent RENAME TO ren_something;

-- 34. Rename non-existent function
-- begin-expected-error
-- sqlstate: 42883
-- end-expected
ALTER FUNCTION ren_nonexistent() RENAME TO ren_something;

-- 35. Rename to existing name conflicts
-- begin-expected-error
-- sqlstate: 42P07
-- end-expected
ALTER TABLE ren_c1 RENAME TO ren_t1_new;

-- ============================================================================
-- SECTION U: ALTER MATERIALIZED VIEW — additional PG syntax
-- ============================================================================

-- 36. ALTER MATERIALIZED VIEW RENAME COLUMN
CREATE MATERIALIZED VIEW ren_mv2 AS SELECT id AS mv_id, description AS mv_desc FROM ren_t1_new;

ALTER MATERIALIZED VIEW ren_mv2 RENAME COLUMN mv_id TO renamed_id;

-- begin-expected
-- columns: renamed_id|mv_desc
-- row: 1|hello
-- end-expected
SELECT * FROM ren_mv2;

-- 37. ALTER MATERIALIZED VIEW with CLUSTER ON is accepted (no-op)
CREATE INDEX ren_mv2_idx ON ren_mv2 (renamed_id);
ALTER MATERIALIZED VIEW ren_mv2 CLUSTER ON ren_mv2_idx;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*) AS cnt FROM ren_mv2;

-- 38. ALTER MATERIALIZED VIEW SET WITHOUT CLUSTER is accepted (no-op)
ALTER MATERIALIZED VIEW ren_mv2 SET WITHOUT CLUSTER;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*) AS cnt FROM ren_mv2;

-- ============================================================================
-- SECTION V: Cleanup
-- ============================================================================

DROP SCHEMA ren_test CASCADE;
