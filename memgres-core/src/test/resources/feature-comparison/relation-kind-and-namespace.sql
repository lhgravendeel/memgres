-- ============================================================================
-- Feature Comparison: relation kinds and schema namespace integrity
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Covers:
--   1. A materialized view is not a view, a table, or writable
--   2. REFRESH ... CONCURRENTLY needs a unique index
--   3. ALTER TABLE reaches views and sequences only for naming actions
--   4. Renaming a view keeps it a view
--   5. Moving a relation to a schema that does not exist
--   6. ALTER SCHEMA ... RENAME actually renames
-- ============================================================================

-- ============================================================================
-- 1. Materialized view identity
-- ============================================================================
DROP MATERIALIZED VIEW IF EXISTS rkn_mv CASCADE;
DROP TABLE IF EXISTS rkn_t CASCADE;
CREATE TABLE rkn_t (i int, j text);
INSERT INTO rkn_t VALUES (1,'a'),(2,'b');
CREATE MATERIALIZED VIEW rkn_mv AS SELECT i, j FROM rkn_t;

-- a materialized view holds a stored result, so it cannot be written to
INSERT INTO rkn_mv VALUES (9,'z');
UPDATE rkn_mv SET j = 'x';
DELETE FROM rkn_mv;
MERGE INTO rkn_mv t USING rkn_t s ON t.i = s.i WHEN MATCHED THEN DELETE;
-- and none of that changed it
SELECT count(*)::text AS a FROM rkn_mv;

-- dropping it by the wrong kind must refuse rather than destroy it
DROP VIEW rkn_mv;
SELECT count(*)::text AS a FROM rkn_mv;
DROP TABLE rkn_mv;
SELECT count(*)::text AS a FROM rkn_mv;
DROP VIEW IF EXISTS rkn_mv;
SELECT count(*)::text AS a FROM rkn_mv;

-- and a plain view is not a materialized view
DROP VIEW IF EXISTS rkn_v CASCADE;
CREATE VIEW rkn_v AS SELECT i FROM rkn_t;
DROP MATERIALIZED VIEW rkn_v;
SELECT count(*)::text AS a FROM rkn_v;

-- ============================================================================
-- 2. REFRESH ... CONCURRENTLY requires a unique index
-- ============================================================================
REFRESH MATERIALIZED VIEW CONCURRENTLY rkn_mv;
-- a partial unique index does not cover every row
CREATE UNIQUE INDEX rkn_mv_partial ON rkn_mv (i) WHERE i > 1;
REFRESH MATERIALIZED VIEW CONCURRENTLY rkn_mv;
DROP INDEX rkn_mv_partial;
-- a full unique index does
CREATE UNIQUE INDEX rkn_mv_ix ON rkn_mv (i);
REFRESH MATERIALIZED VIEW CONCURRENTLY rkn_mv;
SELECT count(*)::text AS a FROM rkn_mv;
-- CONCURRENTLY cannot be combined with WITH NO DATA
REFRESH MATERIALIZED VIEW CONCURRENTLY rkn_mv WITH NO DATA;
-- a plain refresh still works
REFRESH MATERIALIZED VIEW rkn_mv;
SELECT count(*)::text AS a FROM rkn_mv;

-- ============================================================================
-- 3. ALTER TABLE against other relation kinds
-- ============================================================================
ALTER TABLE rkn_v ALTER COLUMN i SET NOT NULL;
ALTER TABLE rkn_v ALTER COLUMN i DROP NOT NULL;
ALTER TABLE rkn_v ADD COLUMN z int;
ALTER TABLE rkn_v DROP COLUMN i;
ALTER TABLE rkn_v ADD CONSTRAINT rkn_ck CHECK (i > 0);
ALTER TABLE rkn_mv ADD COLUMN z int;
DROP SEQUENCE IF EXISTS rkn_s CASCADE;
CREATE SEQUENCE rkn_s;
ALTER TABLE rkn_s ADD COLUMN z int;
ALTER TABLE rkn_s ALTER COLUMN z SET NOT NULL;
-- a view column may carry a default, which INSERT through the view uses
ALTER TABLE rkn_v ALTER COLUMN i SET DEFAULT 1;
ALTER TABLE rkn_v ALTER COLUMN i DROP DEFAULT;
-- the table itself is unaffected
ALTER TABLE rkn_t ADD COLUMN z int;
SELECT count(*)::text AS a FROM information_schema.columns WHERE table_name = 'rkn_t';
ALTER TABLE rkn_t DROP COLUMN z;

-- ============================================================================
-- 4. Renaming a view keeps it a view
-- ============================================================================
ALTER TABLE rkn_v RENAME COLUMN i TO k;
SELECT count(*)::text AS a FROM information_schema.columns
 WHERE table_name = 'rkn_v' AND column_name = 'k';
SELECT count(*)::text AS a FROM information_schema.columns
 WHERE table_name = 'rkn_v' AND column_name = 'i';
ALTER TABLE rkn_v RENAME TO rkn_v2;
SELECT count(*)::text AS a FROM rkn_v2;
SELECT count(*)::text AS a FROM information_schema.views WHERE table_name = 'rkn_v2';
SELECT relkind AS a FROM pg_class WHERE relname = 'rkn_v2';
DROP VIEW rkn_v2;
-- ALTER VIEW spellings behave the same way
DROP VIEW IF EXISTS rkn_v3 CASCADE;
CREATE VIEW rkn_v3 AS SELECT i FROM rkn_t;
ALTER VIEW rkn_v3 RENAME COLUMN i TO m;
SELECT count(*)::text AS a FROM information_schema.columns
 WHERE table_name = 'rkn_v3' AND column_name = 'm';
ALTER VIEW rkn_v3 RENAME TO rkn_v4;
SELECT count(*)::text AS a FROM rkn_v4;
DROP VIEW rkn_v4;

-- ============================================================================
-- 5. Moving a relation to a schema that does not exist
-- ============================================================================
DROP SCHEMA IF EXISTS rkn_s1 CASCADE;
DROP SCHEMA IF EXISTS rkn_s2 CASCADE;
CREATE SCHEMA rkn_s1;
DROP TABLE IF EXISTS rkn_moved CASCADE;
CREATE TABLE rkn_s1.rkn_moved (i int);
INSERT INTO rkn_s1.rkn_moved VALUES (1),(2);
ALTER TABLE rkn_s1.rkn_moved SET SCHEMA rkn_no_such_schema;
-- the table is still where it was
SELECT count(*)::text AS a FROM rkn_s1.rkn_moved;
SELECT table_schema AS a FROM information_schema.tables WHERE table_name = 'rkn_moved';
-- moving it somewhere real works
CREATE SCHEMA rkn_s2;
ALTER TABLE rkn_s1.rkn_moved SET SCHEMA rkn_s2;
SELECT count(*)::text AS a FROM rkn_s2.rkn_moved;
SELECT table_schema AS a FROM information_schema.tables WHERE table_name = 'rkn_moved';

-- ============================================================================
-- 6. ALTER SCHEMA ... RENAME
-- ============================================================================
ALTER SCHEMA rkn_s2 RENAME TO rkn_s2b;
SELECT count(*)::text AS a FROM rkn_s2b.rkn_moved;
SELECT count(*)::text AS a FROM information_schema.schemata WHERE schema_name = 'rkn_s2b';
SELECT count(*)::text AS a FROM information_schema.schemata WHERE schema_name = 'rkn_s2';
SELECT table_schema AS a FROM information_schema.tables WHERE table_name = 'rkn_moved';
-- the old name no longer resolves
SELECT count(*)::text AS a FROM rkn_s2.rkn_moved;
-- renaming something that does not exist, or onto a name in use
ALTER SCHEMA rkn_no_such RENAME TO rkn_x;
ALTER SCHEMA rkn_s2b RENAME TO rkn_s1;
ALTER SCHEMA rkn_s2b RENAME TO public;

DROP SCHEMA IF EXISTS rkn_s2b CASCADE;
DROP SCHEMA IF EXISTS rkn_s1 CASCADE;
DROP MATERIALIZED VIEW IF EXISTS rkn_mv CASCADE;
DROP SEQUENCE IF EXISTS rkn_s CASCADE;
DROP TABLE IF EXISTS rkn_t CASCADE;
