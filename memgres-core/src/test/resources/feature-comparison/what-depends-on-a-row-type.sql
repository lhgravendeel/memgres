-- What depends on a type, and what CASCADE does about it.
--
-- A relation carries a composite type of its own name, and a routine written in terms of that
-- type -- answering with rows of it, taking one as an argument, taking an array of them --
-- depends on the type rather than on the relation. PostgreSQL says so in as many words: the line
-- reads "depends on type", not "depends on table", and a plain DROP TABLE is refused for it.
-- What a refusal names, CASCADE has to take away: the column declared as the type goes out of
-- information_schema.columns, the index over that column goes with it, the default drawing on a
-- dropped sequence is cleared, and a typed table goes whole.
--
-- The order the lines of a DETAIL read in is asserted over JDBC in
-- CatalogueTextAndRuleLifecycleTest, because this harness compares two errors by SQLSTATE alone.
-- Every answer below was measured against PostgreSQL 18.


-- ============================================================================
-- A routine taking the relation's row type as an argument depends on the type,
-- and the refusal says so
-- ============================================================================

CREATE TABLE wdt_t1 (a int, b text);
CREATE FUNCTION wdt_f1(r wdt_t1) RETURNS int LANGUAGE sql AS 'SELECT 1';

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table wdt_t1 because other objects depend on it
-- detail-like: function wdt_f1(wdt_t1) depends on type wdt_t1
-- hint-like: Use DROP ... CASCADE to drop the dependent objects too.
-- end-expected-error
DROP TABLE wdt_t1;

-- RESTRICT is the default, and IF EXISTS says nothing about dependents.
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table wdt_t1 because other objects depend on it
-- end-expected-error
DROP TABLE wdt_t1 RESTRICT;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table wdt_t1 because other objects depend on it
-- end-expected-error
DROP TABLE IF EXISTS wdt_t1;

-- The refusal changed nothing.
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname = 'wdt_f1';

DROP TABLE wdt_t1 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname = 'wdt_f1';

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'wdt_t1';


-- ============================================================================
-- The whole family: answering with the type, with SETOF it, taking it, taking an
-- array of it, taking it with a default, and a procedure taking it -- all six
-- stand in the way, and CASCADE takes all six
-- ============================================================================

CREATE TABLE wdt_t2 (a int, b text);
CREATE FUNCTION wdt_ret(x int) RETURNS wdt_t2 LANGUAGE sql AS 'SELECT 1, ''a''';
CREATE FUNCTION wdt_setof(x int) RETURNS SETOF wdt_t2 LANGUAGE sql AS 'SELECT 1, ''a''';
CREATE FUNCTION wdt_arg(r wdt_t2) RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE FUNCTION wdt_arr(r wdt_t2[]) RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE FUNCTION wdt_def(r wdt_t2 DEFAULT NULL) RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE PROCEDURE wdt_proc(r wdt_t2) LANGUAGE sql AS 'SELECT 1';

-- What the array reaches is reported first, and against the array type; the rest follow in the
-- order they were created.
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table wdt_t2 because other objects depend on it
-- detail-like: function wdt_arr(wdt_t2[]) depends on type wdt_t2[]
-- end-expected-error
DROP TABLE wdt_t2;

-- begin-expected
-- columns: n
-- row: 6
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname LIKE 'wdt!_%' ESCAPE '!';

DROP TABLE wdt_t2 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname LIKE 'wdt!_%' ESCAPE '!';


-- ============================================================================
-- A routine in another schema is named with the schema the search path does not
-- reach, and only the overload written in terms of the type goes
-- ============================================================================

CREATE SCHEMA wdt_s3;
CREATE TABLE wdt_t3 (a int, b text);
CREATE FUNCTION wdt_s3.wdt_g3(r wdt_t3) RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE FUNCTION wdt_g3(r int) RETURNS int LANGUAGE sql AS 'SELECT 2';

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table wdt_t3 because other objects depend on it
-- detail-like: function wdt_s3.wdt_g3(wdt_t3) depends on type wdt_t3
-- end-expected-error
DROP TABLE wdt_t3;

DROP TABLE wdt_t3 CASCADE;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname = 'wdt_g3';

-- begin-expected
-- columns: v
-- row: 2
-- end-expected
SELECT wdt_g3(5) AS v;

DROP FUNCTION wdt_g3(int);
DROP SCHEMA wdt_s3 CASCADE;


-- ============================================================================
-- An aggregate over a type, and the routine it takes its state from, both stand
-- in the way of DROP TYPE and both go with CASCADE
-- ============================================================================

CREATE TYPE wdt_c3 AS (x int, y text);
CREATE FUNCTION wdt_h3(r wdt_c3) RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE FUNCTION wdt_st3(s int, r wdt_c3) RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE AGGREGATE wdt_ag3(wdt_c3) (SFUNC = wdt_st3, STYPE = int, INITCOND = '0');

-- A routine of two arguments is written with no space after the comma.
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type wdt_c3 because other objects depend on it
-- detail-like: function wdt_st3(integer,wdt_c3) depends on type wdt_c3
-- end-expected-error
DROP TYPE wdt_c3;

DROP TYPE wdt_c3 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname LIKE 'wdt!_%3' ESCAPE '!';


-- ============================================================================
-- DROP TYPE ... CASCADE takes away the column declared as the type, and the
-- index over that column goes with the column
-- ============================================================================

CREATE TYPE wdt_e1 AS ENUM ('a','b');
CREATE TABLE wdt_u1 (k int, c wdt_e1, d int);
CREATE INDEX wdt_ix1 ON wdt_u1 (c);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type wdt_e1 because other objects depend on it
-- detail-like: column c of table wdt_u1 depends on type wdt_e1
-- end-expected-error
DROP TYPE wdt_e1 RESTRICT;

DROP TYPE wdt_e1 CASCADE;

-- The column is gone, and the two that were not declared as the type keep their places.
-- begin-expected
-- columns: column_name
-- row: k
-- row: d
-- end-expected
SELECT column_name FROM information_schema.columns
WHERE table_name = 'wdt_u1' ORDER BY ordinal_position;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'wdt_ix1';

DROP TABLE wdt_u1;


-- ============================================================================
-- A typed table has no shape of its own once the type has gone, so CASCADE
-- takes the whole relation
-- ============================================================================

CREATE TYPE wdt_c5 AS (x int, y text);
CREATE TABLE wdt_of5 OF wdt_c5;
CREATE TABLE wdt_u5 (k int, c wdt_c5, d int);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type wdt_c5 because other objects depend on it
-- detail-like: table wdt_of5 depends on type wdt_c5
-- end-expected-error
DROP TYPE wdt_c5;

DROP TYPE wdt_c5 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'wdt_of5';

-- begin-expected
-- columns: column_name
-- row: k
-- row: d
-- end-expected
SELECT column_name FROM information_schema.columns
WHERE table_name = 'wdt_u5' ORDER BY ordinal_position;

DROP TABLE wdt_u5;


-- ============================================================================
-- A domain is a type like any other: a column declared as one and a routine
-- written in terms of one both go with DROP DOMAIN ... CASCADE
-- ============================================================================

CREATE DOMAIN wdt_dm AS int CHECK (VALUE > 0);
CREATE TABLE wdt_ud (k int, c wdt_dm, d int);
CREATE FUNCTION wdt_fd(r wdt_dm) RETURNS int LANGUAGE sql AS 'SELECT 1';

-- The refusal calls a domain a type, whichever spelling the DROP was written with.
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type wdt_dm because other objects depend on it
-- detail-like: column c of table wdt_ud depends on type wdt_dm
-- end-expected-error
DROP DOMAIN wdt_dm;

DROP DOMAIN wdt_dm CASCADE;

-- begin-expected
-- columns: column_name
-- row: k
-- row: d
-- end-expected
SELECT column_name FROM information_schema.columns
WHERE table_name = 'wdt_ud' ORDER BY ordinal_position;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname = 'wdt_fd';

DROP TABLE wdt_ud;


-- ============================================================================
-- DROP SCHEMA ... CASCADE reaches what hangs from the types and the sequences it
-- holds: the column outside declared as the type, the routine outside written in
-- terms of it, and the default outside drawing on the sequence
-- ============================================================================

CREATE SCHEMA wdt_s2;
CREATE TYPE wdt_s2.wdt_e2 AS ENUM ('a','b');
CREATE SEQUENCE wdt_s2.wdt_q2;
CREATE TABLE wdt_u2 (k int DEFAULT nextval('wdt_s2.wdt_q2'), c wdt_s2.wdt_e2, d int);
CREATE INDEX wdt_ix2 ON wdt_u2 (c);
CREATE FUNCTION wdt_fa2(r wdt_s2.wdt_e2) RETURNS int LANGUAGE sql AS 'SELECT 1';

-- The type comes before the sequence because it was created first, and what hangs from each is
-- reported straight after it.
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop schema wdt_s2 because other objects depend on it
-- detail-like: type wdt_s2.wdt_e2 depends on schema wdt_s2
-- end-expected-error
DROP SCHEMA wdt_s2;

DROP SCHEMA wdt_s2 CASCADE;

-- The column declared as the type is gone and the default drawing on the sequence is cleared.
-- begin-expected
-- columns: column_name, column_default
-- row: k|NULL
-- row: d|NULL
-- end-expected
SELECT column_name, column_default FROM information_schema.columns
WHERE table_name = 'wdt_u2' ORDER BY ordinal_position;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'wdt_ix2';

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname = 'wdt_fa2';

DROP TABLE wdt_u2;


-- ============================================================================
-- A column of a relation the schema is taking with it is covered by the
-- relation's own line, so it is not reported twice
-- ============================================================================

CREATE SCHEMA wdt_sa;
CREATE TYPE wdt_sa.wdt_ea AS ENUM ('a');
CREATE TABLE wdt_sa.wdt_ta (k int, c wdt_sa.wdt_ea);
CREATE FUNCTION wdt_sa.wdt_fa(r wdt_sa.wdt_ea) RETURNS int LANGUAGE sql AS 'SELECT 1';

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop schema wdt_sa because other objects depend on it
-- detail-like: type wdt_sa.wdt_ea depends on schema wdt_sa
-- end-expected-error
DROP SCHEMA wdt_sa;

DROP SCHEMA wdt_sa CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_namespace WHERE nspname = 'wdt_sa';


-- ============================================================================
-- What is not written in terms of the row type is no dependency at all, and a
-- plain DROP TABLE goes on succeeding
-- ============================================================================

CREATE TABLE wdt_k1 (a int, b text);
CREATE FUNCTION wdt_kf1(x int) RETURNS int LANGUAGE sql AS 'SELECT x';
CREATE FUNCTION wdt_kf2(x text) RETURNS text LANGUAGE sql AS 'SELECT x';
CREATE FUNCTION wdt_kf3() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM wdt_k1';
CREATE FUNCTION wdt_kf4() RETURNS SETOF record LANGUAGE sql AS 'SELECT 1, 2';

-- A routine written over the column types, and one whose unparsed body merely names the relation,
-- record no dependency: the bare DROP TABLE is carried out.
DROP TABLE wdt_k1;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'wdt_k1';

-- All four are still standing, and still answer.
-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*) AS n FROM pg_proc WHERE proname LIKE 'wdt!_kf%' ESCAPE '!';

-- begin-expected
-- columns: v
-- row: 7
-- end-expected
SELECT wdt_kf1(7) AS v;

DROP FUNCTION wdt_kf1(int);
DROP FUNCTION wdt_kf2(text);
DROP FUNCTION wdt_kf3();
DROP FUNCTION wdt_kf4();

-- A routine dropped first takes its dependency with it, so the bare DROP TABLE succeeds after it.
CREATE TABLE wdt_k2 (a int, b text);
CREATE FUNCTION wdt_kg(r wdt_k2) RETURNS int LANGUAGE sql AS 'SELECT 1';
DROP FUNCTION wdt_kg(wdt_k2);
DROP TABLE wdt_k2;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'wdt_k2';

-- One relation's row type is nothing to another's drop.
CREATE TABLE wdt_k3 (a int, b text);
CREATE TABLE wdt_k4 (a int, b text);
CREATE FUNCTION wdt_kh(r wdt_k4) RETURNS int LANGUAGE sql AS 'SELECT 1';
DROP TABLE wdt_k3;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'wdt_k3';

DROP TABLE wdt_k4 CASCADE;

-- Nothing but a drop is held up by it: the relation still takes TRUNCATE and ALTER.
CREATE TABLE wdt_k5 (a int, b text);
CREATE FUNCTION wdt_ki(r wdt_k5) RETURNS int LANGUAGE sql AS 'SELECT 1';
TRUNCATE wdt_k5;
ALTER TABLE wdt_k5 ADD COLUMN c int;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*) AS n FROM information_schema.columns WHERE table_name = 'wdt_k5';

DROP TABLE wdt_k5 CASCADE;
