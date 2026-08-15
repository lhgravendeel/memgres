-- DROP VIEW, DROP SEQUENCE, DROP INDEX and DROP TYPE all take a list of names, and each settles
-- the whole list before it takes any of it.
--
-- A view over a view the same statement names is dropped once, in either order. A name that never
-- existed takes the whole statement with it and leaves every other name where it was, whatever
-- kind the statement is for -- and IF EXISTS passes over that name and takes the rest. A name of
-- the wrong kind is refused with the SQLSTATE and the hint that name's own kind earns, and one
-- relation written twice in the same list is one name settled twice, not a name that went missing.
--
-- Every answer below was read off PostgreSQL 18.

-- ============================================================================
-- DROP VIEW: a view over a view the same statement names
-- ============================================================================
CREATE TABLE zzr7gn_vt (i int);
CREATE VIEW zzr7gn_va AS SELECT * FROM zzr7gn_vt;
CREATE VIEW zzr7gn_vb AS SELECT * FROM zzr7gn_va;

-- stmt 1: the view that is read named first, with CASCADE
DROP VIEW zzr7gn_va, zzr7gn_vb CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_va','zzr7gn_vb');

-- stmt 2: the reader named first, without CASCADE
CREATE VIEW zzr7gn_va AS SELECT * FROM zzr7gn_vt;
CREATE VIEW zzr7gn_vb AS SELECT * FROM zzr7gn_va;
DROP VIEW zzr7gn_vb, zzr7gn_va;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_va','zzr7gn_vb');

-- stmt 3: the same for materialized views, in each order
CREATE MATERIALIZED VIEW zzr7gn_ma AS SELECT * FROM zzr7gn_vt;
CREATE MATERIALIZED VIEW zzr7gn_mb AS SELECT * FROM zzr7gn_ma;
DROP MATERIALIZED VIEW zzr7gn_ma, zzr7gn_mb CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_ma','zzr7gn_mb');

CREATE MATERIALIZED VIEW zzr7gn_ma AS SELECT * FROM zzr7gn_vt;
CREATE MATERIALIZED VIEW zzr7gn_mb AS SELECT * FROM zzr7gn_ma;
DROP MATERIALIZED VIEW zzr7gn_mb, zzr7gn_ma;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_ma','zzr7gn_mb');

-- stmt 4: a name that never existed, and IF EXISTS passing over it
CREATE VIEW zzr7gn_va AS SELECT * FROM zzr7gn_vt;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: view "zzr7gn_nosuch" does not exist
-- end-expected-error
DROP VIEW zzr7gn_va, zzr7gn_nosuch;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'zzr7gn_va';

-- stmt 5: a name of the wrong kind is refused even under IF EXISTS
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzr7gn_vt" is not a view
-- end-expected-error
DROP VIEW IF EXISTS zzr7gn_va, zzr7gn_nosuch, zzr7gn_vt;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzr7gn_va" is not a table
-- end-expected-error
DROP TABLE IF EXISTS zzr7gn_nosuch, zzr7gn_va;

DROP VIEW IF EXISTS zzr7gn_nosuch, zzr7gn_va;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'zzr7gn_va';

-- stmt 6: one view written twice
CREATE VIEW zzr7gn_va AS SELECT * FROM zzr7gn_vt;
DROP VIEW zzr7gn_va, zzr7gn_va;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'zzr7gn_va';

DROP TABLE zzr7gn_vt;

-- ============================================================================
-- DROP SEQUENCE
-- ============================================================================
CREATE SEQUENCE zzr7gn_qa;
CREATE SEQUENCE zzr7gn_qb;

-- stmt 7: a name that never existed leaves both sequences where they were
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: sequence "zzr7gn_nosuch" does not exist
-- end-expected-error
DROP SEQUENCE zzr7gn_qa, zzr7gn_nosuch;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_qa','zzr7gn_qb');

-- stmt 8: IF EXISTS passes over it and takes both
DROP SEQUENCE IF EXISTS zzr7gn_nosuch, zzr7gn_qa, zzr7gn_qb;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_qa','zzr7gn_qb');

-- stmt 9: one sequence written twice
CREATE SEQUENCE zzr7gn_qa;
DROP SEQUENCE zzr7gn_qa, zzr7gn_qa;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'zzr7gn_qa';

-- ============================================================================
-- DROP INDEX
-- ============================================================================
CREATE TABLE zzr7gn_jt (i int, j int);
CREATE INDEX zzr7gn_ja ON zzr7gn_jt (i);
CREATE INDEX zzr7gn_jb ON zzr7gn_jt (j);

-- stmt 10: a name that never existed leaves both indexes where they were
-- begin-expected-error
-- sqlstate: 42704
-- message-like: index "zzr7gn_nosuch" does not exist
-- end-expected-error
DROP INDEX zzr7gn_ja, zzr7gn_nosuch;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_ja','zzr7gn_jb');

-- stmt 11: both names together, and one of them written twice
DROP INDEX zzr7gn_ja, zzr7gn_jb;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_ja','zzr7gn_jb');

CREATE INDEX zzr7gn_ja ON zzr7gn_jt (i);
DROP INDEX zzr7gn_ja, zzr7gn_ja;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'zzr7gn_ja';

DROP TABLE zzr7gn_jt;

-- ============================================================================
-- DROP TYPE
-- ============================================================================
CREATE TYPE zzr7gn_ea AS ENUM ('a');
CREATE TYPE zzr7gn_eb AS ENUM ('b');

-- stmt 12: a name that never existed leaves both types where they were
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zzr7gn_nosuch" does not exist
-- end-expected-error
DROP TYPE zzr7gn_ea, zzr7gn_nosuch;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM pg_type WHERE typname IN ('zzr7gn_ea','zzr7gn_eb');

-- stmt 13: a type named where a table is asked for is a name that is not there at all
CREATE TABLE zzr7gn_et (i zzr7gn_ea);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: table "zzr7gn_ea" does not exist
-- end-expected-error
DROP TABLE zzr7gn_et, zzr7gn_ea;

DROP TABLE zzr7gn_et;

-- stmt 14: IF EXISTS passes over the name that was never there and takes both types
DROP TYPE IF EXISTS zzr7gn_nosuch, zzr7gn_ea, zzr7gn_eb;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_type WHERE typname IN ('zzr7gn_ea','zzr7gn_eb');

-- stmt 15: one type written twice
CREATE TYPE zzr7gn_ea AS ENUM ('a');
DROP TYPE zzr7gn_ea, zzr7gn_ea;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_type WHERE typname = 'zzr7gn_ea';

-- cleanup
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname LIKE 'zzr7gn!_%' ESCAPE '!';
