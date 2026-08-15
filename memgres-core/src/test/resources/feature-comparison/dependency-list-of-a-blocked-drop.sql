-- What a DROP reports it would break. PostgreSQL refuses with 2BP01 and names, in its
-- DETAIL, every object that depends on the one being dropped -- views over a table, the
-- column defaults that call a sequence, the columns declared with a type -- and it walks
-- the tree depth first, following each dependent's own dependents before the next sibling.
-- Nothing is dropped by the refusal; CASCADE drops the whole set.
--
-- The DETAIL text itself cannot be checked here (the harness compares only the first line
-- of an error message); the ordered DETAIL and the NOTICE that CASCADE raises are asserted
-- over JDBC in CatalogueTextAndRuleLifecycleTest. Everything below was read off PostgreSQL 18.

-- stmt 1: three views over one table block the drop, and the table survives the refusal
CREATE TABLE dbd_ord (id int);
CREATE VIEW dbd_zview AS SELECT id FROM dbd_ord;
CREATE VIEW dbd_mview AS SELECT id FROM dbd_ord;
CREATE VIEW dbd_aview AS SELECT id FROM dbd_ord;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table dbd_ord because other objects depend on it
-- end-expected-error
DROP TABLE dbd_ord;

-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('dbd_ord','dbd_zview','dbd_mview','dbd_aview');

DROP TABLE dbd_ord CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('dbd_ord','dbd_zview','dbd_mview','dbd_aview');

-- stmt 2: a view over a view is reached through the view that carries it
CREATE TABLE dbd_dfs (id int);
CREATE VIEW dbd_v1 AS SELECT id FROM dbd_dfs;
CREATE VIEW dbd_v2 AS SELECT id FROM dbd_dfs;
CREATE VIEW dbd_v1a AS SELECT id FROM dbd_v1;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table dbd_dfs because other objects depend on it
-- end-expected-error
DROP TABLE dbd_dfs;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop view dbd_v1 because other objects depend on it
-- end-expected-error
DROP VIEW dbd_v1;

DROP TABLE dbd_dfs CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('dbd_dfs','dbd_v1','dbd_v2','dbd_v1a');

-- stmt 3: a sequence is held by every column default that calls it
CREATE SEQUENCE dbd_sq1;
CREATE TABLE dbd_d1 (a int DEFAULT nextval('dbd_sq1'), b int DEFAULT nextval('dbd_sq1'), c int DEFAULT nextval('dbd_sq1'));
CREATE TABLE dbd_d2 (a int DEFAULT nextval('dbd_sq1'));

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop sequence dbd_sq1 because other objects depend on it
-- end-expected-error
DROP SEQUENCE dbd_sq1;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'dbd_sq1';

-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*) AS n FROM pg_attrdef d JOIN pg_class c ON c.oid = d.adrelid WHERE c.relname IN ('dbd_d1','dbd_d2');

-- CASCADE takes the defaults with it and leaves the tables standing
DROP SEQUENCE dbd_sq1 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'dbd_sq1';

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_attrdef d JOIN pg_class c ON c.oid = d.adrelid WHERE c.relname IN ('dbd_d1','dbd_d2');

DROP TABLE dbd_d1;
DROP TABLE dbd_d2;

-- stmt 4: a column declared with a composite type holds the type
CREATE TYPE dbd_ty AS (x int);
CREATE TABLE dbd_ut (c dbd_ty);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type dbd_ty because other objects depend on it
-- end-expected-error
DROP TYPE dbd_ty;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_type WHERE typname = 'dbd_ty';

DROP TYPE dbd_ty CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_type WHERE typname = 'dbd_ty';

-- the dropped column keeps its attribute number rather than vanishing
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'dbd_ut' AND a.attnum > 0;

DROP TABLE dbd_ut;

-- stmt 5: a single dependent is dropped by CASCADE just the same
CREATE TABLE dbd_one (id int);
CREATE VIEW dbd_ov1 AS SELECT id FROM dbd_one;
DROP TABLE dbd_one CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('dbd_one','dbd_ov1');
