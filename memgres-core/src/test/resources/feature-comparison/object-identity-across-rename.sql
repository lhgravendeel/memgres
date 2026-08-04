-- ============================================================================
-- Feature Comparison: object identity across ALTER ... RENAME / SET SCHEMA
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- An OID names an object, not a name. PostgreSQL assigns one when the object is
-- created and never moves it, so a tool that caches an OID and looks it up again
-- finds the same object under whatever name it now answers to, a tool that looks
-- up the old name gets 42P01, and a number whose object has been dropped prints
-- as the number. Everything filed against the object -- its comment, its grants,
-- the index built on it -- is filed against that OID and so notices nothing.
--
-- No OID value is asserted anywhere below: PostgreSQL's are arbitrary. Each case
-- stashes the number it read in a table and compares it with what the catalog
-- says afterwards, which is an answer both engines can be held to.
-- ============================================================================

DROP SCHEMA IF EXISTS oi_moved CASCADE;
DROP SCHEMA IF EXISTS oi_sa CASCADE;
DROP SCHEMA IF EXISTS oi_sb CASCADE;
DROP SCHEMA IF EXISTS oi_ia CASCADE;
DROP SCHEMA IF EXISTS oi_ib CASCADE;
DROP SCHEMA IF EXISTS oi_dropme CASCADE;
CREATE SCHEMA oi_moved;

-- ============================================================================
-- 1. A table keeps its OID across ALTER TABLE ... RENAME TO
-- ============================================================================

CREATE TABLE oi_t (a integer PRIMARY KEY, b text);
CREATE TABLE oi_t_keep AS SELECT 'oi_t'::regclass::oid AS o;

ALTER TABLE oi_t RENAME TO oi_t2;

-- begin-expected
-- columns: stable
-- row: true
-- end-expected
SELECT (SELECT o FROM oi_t_keep) = 'oi_t2'::regclass::oid AS stable;

-- begin-expected
-- columns: nm
-- row: oi_t2
-- end-expected
SELECT (SELECT o FROM oi_t_keep)::regclass::text AS nm;

-- begin-expected
-- columns: relname
-- row: oi_t2
-- end-expected
SELECT relname FROM pg_class WHERE oid = (SELECT o FROM oi_t_keep);

-- The name the rename freed is nobody's.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "oi_t" does not exist
-- end-expected-error
SELECT 'oi_t'::regclass::oid;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE relname = 'oi_t';

-- ============================================================================
-- 2. A table keeps its OID across ALTER TABLE ... SET SCHEMA
-- ============================================================================

ALTER TABLE oi_t2 SET SCHEMA oi_moved;

-- begin-expected
-- columns: stable
-- row: true
-- end-expected
SELECT (SELECT o FROM oi_t_keep) = 'oi_moved.oi_t2'::regclass::oid AS stable;

-- begin-expected
-- columns: nspname
-- row: oi_moved
-- end-expected
SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = (SELECT o FROM oi_t_keep);

-- ============================================================================
-- 3. A dropped OID stops naming anything
-- ============================================================================

CREATE TABLE oi_gone (a integer);
CREATE TABLE oi_gone_keep AS SELECT 'oi_gone'::regclass::oid AS o;

DROP TABLE oi_gone;

-- An OID that resolves to nothing prints as the number it is.
-- begin-expected
-- columns: prints_number
-- row: true
-- end-expected
SELECT (SELECT o::regclass::text FROM oi_gone_keep) = (SELECT o::text FROM oi_gone_keep)
       AS prints_number;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE oid = (SELECT o FROM oi_gone_keep);

-- A table created again under the freed name is a different object.
CREATE TABLE oi_gone (a integer);

-- begin-expected
-- columns: recycled
-- row: false
-- end-expected
SELECT (SELECT o FROM oi_gone_keep) = 'oi_gone'::regclass::oid AS recycled;

-- ============================================================================
-- 4. Sequences, views and indexes keep their OIDs too
-- ============================================================================

CREATE SEQUENCE oi_q;
CREATE VIEW oi_v AS SELECT 1 AS x;
CREATE TABLE oi_it (a integer, b integer);
CREATE INDEX oi_ix ON oi_it (b);
CREATE TABLE oi_rel_keep AS
  SELECT 'oi_q'::regclass::oid AS q, 'oi_v'::regclass::oid AS v, 'oi_ix'::regclass::oid AS i;

ALTER SEQUENCE oi_q RENAME TO oi_q2;
ALTER VIEW oi_v RENAME TO oi_v2;
ALTER INDEX oi_ix RENAME TO oi_ix2;

-- begin-expected
-- columns: seq_stable
-- row: true
-- end-expected
SELECT (SELECT q FROM oi_rel_keep) = 'oi_q2'::regclass::oid AS seq_stable;

-- begin-expected
-- columns: view_stable
-- row: true
-- end-expected
SELECT (SELECT v FROM oi_rel_keep) = 'oi_v2'::regclass::oid AS view_stable;

-- begin-expected
-- columns: idx_stable
-- row: true
-- end-expected
SELECT (SELECT i FROM oi_rel_keep) = 'oi_ix2'::regclass::oid AS idx_stable;

-- begin-expected
-- columns: relname
-- row: oi_ix2
-- end-expected
SELECT relname FROM pg_class WHERE oid = (SELECT i FROM oi_rel_keep);

-- ============================================================================
-- 5. Enum types and domains keep their OIDs, and a column follows the rename
-- ============================================================================

CREATE TYPE oi_e AS ENUM ('a', 'b');
CREATE DOMAIN oi_d AS integer CHECK (VALUE > 0);
CREATE TABLE oi_et (c oi_e);
CREATE TABLE oi_type_keep AS SELECT 'oi_e'::regtype::oid AS e, 'oi_d'::regtype::oid AS d;

ALTER TYPE oi_e RENAME TO oi_e2;
ALTER DOMAIN oi_d RENAME TO oi_d2;

-- begin-expected
-- columns: enum_stable
-- row: true
-- end-expected
SELECT (SELECT e FROM oi_type_keep) = 'oi_e2'::regtype::oid AS enum_stable;

-- begin-expected
-- columns: domain_stable
-- row: true
-- end-expected
SELECT (SELECT d FROM oi_type_keep) = 'oi_d2'::regtype::oid AS domain_stable;

-- The column records the type's OID, so it reports the name the type has now.
-- begin-expected
-- columns: t
-- row: oi_e2
-- end-expected
SELECT atttypid::regtype::text AS t FROM pg_attribute
WHERE attrelid = 'oi_et'::regclass AND attname = 'c';

-- begin-expected
-- columns: t
-- row: oi_e2
-- end-expected
SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute
WHERE attrelid = 'oi_et'::regclass AND attname = 'c';

-- ============================================================================
-- 6. A comment belongs to the object, not to the name
-- ============================================================================

CREATE TABLE oi_c (a integer, b text);
COMMENT ON TABLE oi_c IS 'the table';
COMMENT ON COLUMN oi_c.b IS 'the column';

ALTER TABLE oi_c RENAME TO oi_c2;

-- begin-expected
-- columns: obj_description
-- row: the table
-- end-expected
SELECT obj_description('oi_c2'::regclass, 'pg_class');

-- begin-expected
-- columns: col_description
-- row: the column
-- end-expected
SELECT col_description('oi_c2'::regclass, 2);

-- And does not outlive it: a table created again under a dropped name has none.
CREATE TABLE oi_cc (a integer);
COMMENT ON TABLE oi_cc IS 'said of the first one';
DROP TABLE oi_cc;
CREATE TABLE oi_cc (a integer);

-- begin-expected
-- columns: obj_description
-- row: NULL
-- end-expected
SELECT obj_description('oi_cc'::regclass, 'pg_class');

-- ============================================================================
-- 7. A grant belongs to the object
-- ============================================================================

CREATE TABLE oi_p (a integer);
GRANT SELECT ON oi_p TO PUBLIC;

ALTER TABLE oi_p RENAME TO oi_p2;

-- begin-expected
-- columns: has_table_privilege
-- row: true
-- end-expected
SELECT has_table_privilege('public', 'oi_p2', 'SELECT');

-- ============================================================================
-- 8. An index is built on the object, so a table rename does not orphan it
-- ============================================================================

CREATE TABLE oi_u (a integer, b integer);
CREATE INDEX oi_uix ON oi_u (b);

ALTER TABLE oi_u RENAME TO oi_u2;

-- begin-expected
-- columns: indexrelid
-- row: oi_uix
-- end-expected
SELECT indexrelid::regclass::text AS indexrelid FROM pg_index
WHERE indrelid = 'oi_u2'::regclass ORDER BY 1;

-- begin-expected
-- columns: indexname
-- row: oi_uix
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'oi_u2' ORDER BY 1;

-- begin-expected
-- columns: relname
-- row: oi_u2
-- row: oi_uix
-- end-expected
SELECT relname FROM pg_class WHERE relname LIKE 'oi\_u%' ORDER BY 1;

-- The index moves with the table it was built on.
ALTER TABLE oi_u2 SET SCHEMA oi_moved;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) AS count FROM pg_index WHERE indrelid = 'oi_moved.oi_u2'::regclass;

-- ============================================================================
-- 9. A drop beside a create is not a rename, however alike the two look
-- ============================================================================

CREATE TABLE oi_x (a integer, b text);
COMMENT ON TABLE oi_x IS 'said of x';

DO $$ BEGIN DROP TABLE oi_x; CREATE TABLE oi_y (a integer, b text); END $$;

-- begin-expected
-- columns: obj_description
-- row: NULL
-- end-expected
SELECT obj_description('oi_y'::regclass, 'pg_class');

-- The dropped table's number is not handed to the table created beside it.
CREATE TABLE oi_dokeep (o oid);
CREATE TABLE oi_do1 (i integer);
INSERT INTO oi_dokeep SELECT 'oi_do1'::regclass::oid;

DO $$ BEGIN DROP TABLE oi_do1; CREATE TABLE oi_do2 (i integer); END $$;

-- begin-expected
-- columns: recycled
-- row: false
-- end-expected
SELECT (SELECT o FROM oi_dokeep) = 'oi_do2'::regclass::oid AS recycled;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE oid = (SELECT o FROM oi_dokeep);

-- ============================================================================
-- 10. A rename says nothing about the relation of that name in another schema
-- ============================================================================
-- A comment and an owner are filed against the object, so renaming one schema's
-- table leaves both of them on the table of the same name next door.

CREATE SCHEMA oi_sa;
CREATE SCHEMA oi_sb;
CREATE TABLE oi_sa.t (i integer);
CREATE TABLE oi_sb.t (i integer);
COMMENT ON TABLE oi_sb.t IS 'keepme';
COMMENT ON COLUMN oi_sb.t.i IS 'colkeep';

ALTER TABLE oi_sa.t RENAME TO t2;

-- begin-expected
-- columns: obj_description
-- row: keepme
-- end-expected
SELECT obj_description('oi_sb.t'::regclass);

-- begin-expected
-- columns: col_description
-- row: colkeep
-- end-expected
SELECT col_description('oi_sb.t'::regclass, 1);

-- begin-expected
-- columns: obj_description
-- row: NULL
-- end-expected
SELECT obj_description('oi_sa.t2'::regclass);

-- The same for an index, which is a relation like any other.
CREATE SCHEMA oi_ia;
CREATE SCHEMA oi_ib;
CREATE TABLE oi_ia.t (i integer);
CREATE TABLE oi_ib.t (i integer);
CREATE INDEX icx ON oi_ia.t (i);
CREATE INDEX icx ON oi_ib.t (i);
COMMENT ON INDEX oi_ib.icx IS 'ickeep';

ALTER INDEX oi_ia.icx RENAME TO icx2;

-- begin-expected
-- columns: obj_description
-- row: ickeep
-- end-expected
SELECT obj_description('oi_ib.icx'::regclass);

-- begin-expected
-- columns: obj_description
-- row: NULL
-- end-expected
SELECT obj_description('oi_ia.icx2'::regclass);

-- ============================================================================
-- 11. A trigger and a rule are on the object, so a rename does not disturb them
-- ============================================================================

CREATE TABLE oi_tr (a integer);
CREATE TABLE oi_trlog (a integer);
CREATE FUNCTION oi_trf() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;
CREATE TRIGGER oi_trg BEFORE INSERT ON oi_tr FOR EACH ROW EXECUTE FUNCTION oi_trf();
CREATE RULE oi_rul AS ON INSERT TO oi_tr DO ALSO INSERT INTO oi_trlog VALUES (1);

ALTER TABLE oi_tr RENAME TO oi_tr2;

-- begin-expected
-- columns: tgrelid
-- row: oi_tr2
-- end-expected
SELECT tgrelid::regclass::text AS tgrelid FROM pg_trigger WHERE tgname = 'oi_trg';

-- begin-expected
-- columns: tablename
-- row: oi_tr2
-- end-expected
SELECT tablename FROM pg_rules WHERE rulename = 'oi_rul';

-- The rule still fires for the relation it was written on.
INSERT INTO oi_tr2 VALUES (1);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) AS count FROM oi_trlog;

-- ============================================================================
-- 12. A materialized view keeps its OID across a rename
-- ============================================================================

CREATE MATERIALIZED VIEW oi_mv AS SELECT 1 AS x;
CREATE TABLE oi_mv_keep AS SELECT 'oi_mv'::regclass::oid AS o;

ALTER MATERIALIZED VIEW oi_mv RENAME TO oi_mv2;

-- begin-expected
-- columns: mv_stable
-- row: true
-- end-expected
SELECT (SELECT o FROM oi_mv_keep) = 'oi_mv2'::regclass::oid AS mv_stable;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "oi_mv" does not exist
-- end-expected-error
SELECT 'oi_mv'::regclass::oid;

-- ============================================================================
-- 13. A dropped view, sequence and type do not lend their numbers
-- ============================================================================

CREATE VIEW oi_gv AS SELECT 1 AS x;
CREATE SEQUENCE oi_gs;
CREATE TYPE oi_ge AS ENUM ('a');
CREATE TABLE oi_gk AS SELECT 'oi_gv'::regclass::oid AS v, 'oi_gs'::regclass::oid AS s,
                            'oi_ge'::regtype::oid AS e;

DROP VIEW oi_gv;
DROP SEQUENCE oi_gs;
DROP TYPE oi_ge;

CREATE VIEW oi_gv AS SELECT 1 AS x;
CREATE SEQUENCE oi_gs;
CREATE TYPE oi_ge AS ENUM ('a');

-- begin-expected
-- columns: v_recycled
-- row: false
-- end-expected
SELECT (SELECT v FROM oi_gk) = 'oi_gv'::regclass::oid AS v_recycled;

-- begin-expected
-- columns: s_recycled
-- row: false
-- end-expected
SELECT (SELECT s FROM oi_gk) = 'oi_gs'::regclass::oid AS s_recycled;

-- begin-expected
-- columns: e_recycled
-- row: false
-- end-expected
SELECT (SELECT e FROM oi_gk) = 'oi_ge'::regtype::oid AS e_recycled;

-- ============================================================================
-- 14. Dropping a schema retires the OIDs of everything that was in it
-- ============================================================================

CREATE SCHEMA oi_dropme;
CREATE TABLE oi_dropme.k (a integer);
CREATE TABLE oi_dropme_keep AS SELECT 'oi_dropme.k'::regclass::oid AS o;

DROP SCHEMA oi_dropme CASCADE;

-- begin-expected
-- columns: prints_number
-- row: true
-- end-expected
SELECT (SELECT o::regclass::text FROM oi_dropme_keep) = (SELECT o::text FROM oi_dropme_keep)
       AS prints_number;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM pg_class WHERE oid = (SELECT o FROM oi_dropme_keep);

-- ============================================================================
-- 15. Renaming a table and renaming it back costs it nothing
-- ============================================================================

CREATE TABLE oi_rr (i integer);
COMMENT ON TABLE oi_rr IS 'still here';
CREATE INDEX oi_rrix ON oi_rr (i);
CREATE TABLE oi_rr_keep AS SELECT 'oi_rr'::regclass::oid AS o;

ALTER TABLE oi_rr RENAME TO oi_rr9;
ALTER TABLE oi_rr9 RENAME TO oi_rr;
ALTER TABLE oi_rr RENAME TO oi_rr9;
ALTER TABLE oi_rr9 RENAME TO oi_rr;

-- begin-expected
-- columns: obj_description
-- row: still here
-- end-expected
SELECT obj_description('oi_rr'::regclass);

-- begin-expected
-- columns: indexname
-- row: oi_rrix
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'oi_rr' ORDER BY 1;

-- begin-expected
-- columns: stable
-- row: true
-- end-expected
SELECT (SELECT o FROM oi_rr_keep) = 'oi_rr'::regclass::oid AS stable;
