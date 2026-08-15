-- ============================================================================
-- What a stored expression may name
--
-- A CHECK, a generation expression, a column DEFAULT, an index key, an index
-- predicate and an ALTER COLUMN ... USING are all settled where they are
-- written, not at the first row that reaches them. PostgreSQL resolves every
-- name in the expression against the relation being defined, judges every call
-- against the signatures its name is declared under, and resolves a qualifier
-- as a schema before it looks for anything of that name inside it.
--
-- Every value below was read off PostgreSQL 18 before it was written down.
-- ============================================================================

-- ============================================================================
-- A name PostgreSQL declares routines under, but none taking these types
-- ============================================================================

CREATE TABLE zzm3sd_base (a int, k text, b boolean);
CREATE TYPE zzm3sd_comp AS (x int, y int);
CREATE DOMAIN zzm3sd_dom AS int CHECK (VALUE > 0);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(integer) does not exist
-- end-expected-error
CREATE TABLE zzm3sd_c1 (a int, CHECK (lower(a) > 'x'));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function upper(integer) does not exist
-- end-expected-error
CREATE TABLE zzm3sd_c2 (a int, CHECK (upper(a) > 'x'));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function length(integer) does not exist
-- end-expected-error
CREATE TABLE zzm3sd_c3 (a int, CHECK (length(a) > 0));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function substr(integer, integer) does not exist
-- end-expected-error
CREATE TABLE zzm3sd_c4 (a int, CHECK (substr(a, 1) > 'x'));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function repeat(integer, integer) does not exist
-- end-expected-error
CREATE TABLE zzm3sd_c5 (a int, CHECK (repeat(a, 2) > 'x'));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function btrim(integer) does not exist
-- end-expected-error
CREATE TABLE zzm3sd_c6 (a int, CHECK (btrim(a) > 'x'));

-- the argument's type is what the name is resolved with, however it was written
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(integer) does not exist
-- end-expected-error
CREATE TABLE zzm3sd_c7 (a int, CHECK (lower(a + 1) > 'x'));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(boolean) does not exist
-- end-expected-error
CREATE TABLE zzm3sd_c8 (b boolean, CHECK (lower(b) > 'x'));

-- and every call the name does take is stored
CREATE TABLE zzm3sd_ok1 (k text, CHECK (lower(k) <> 'zz'));
CREATE TABLE zzm3sd_ok2 (a int, CHECK (abs(a) > 0));
CREATE TABLE zzm3sd_ok3 (a int, CHECK (lower(a::text) > 'x'));
CREATE TABLE zzm3sd_ok4 (a int, CHECK (lower('x') > 'x'));
CREATE TABLE zzm3sd_ok5 (a int, CHECK (to_char(a, 'FM999') > 'x'));
CREATE TABLE zzm3sd_ok6 (a int, k text, CHECK (coalesce(k, 'x') <> ''));
CREATE TABLE zzm3sd_ok7 (a int, CHECK (greatest(a, 1) > 0));
CREATE TABLE zzm3sd_ok8 (a int, CHECK (text(a) > 'x'));
CREATE TABLE zzm3sd_ok9 (a int, CHECK (int8(a) > 0));
CREATE TABLE zzm3sd_ok10 (a int, CHECK (zzm3sd_dom(a) IS NOT NULL));

-- begin-expected
-- columns: n
-- row: 10
-- end-expected
SELECT count(*)::int AS n FROM information_schema.tables WHERE table_name LIKE 'zzm3sd\_ok%';

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM information_schema.tables WHERE table_name LIKE 'zzm3sd\_c%';

-- ============================================================================
-- A call whose name carries a schema of its own
-- ============================================================================

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zzm3sd_nos" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_s1 (a int, CHECK (zzm3sd_nos.nosuchfunc(a) > 0));

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zzm3sd_nos" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_s2 (a int, b int GENERATED ALWAYS AS (zzm3sd_nos.f(a)) STORED);

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zzm3sd_nos" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_s3 (a int DEFAULT zzm3sd_nos.f(1));

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zzm3sd_nos" does not exist
-- end-expected-error
CREATE INDEX zzm3sd_s4 ON zzm3sd_base (a) WHERE zzm3sd_nos.f(a) > 0;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zzm3sd_nos" does not exist
-- end-expected-error
ALTER TABLE zzm3sd_base ALTER COLUMN k TYPE text USING zzm3sd_nos.f(a);

-- the arguments are transformed before the name is looked for
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_s5 (a int, CHECK (zzm3sd_nos.f(nosuchcol) > 0));

-- an unquoted qualifier is folded the way every other unquoted name is
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "nosuchzzm" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_s6 (a int, CHECK (NoSuchZzm.NoFunc(a) > 0));

-- a qualifier that does name a schema holds the name to what is really in it
CREATE TABLE zzm3sd_s7 (a int, CHECK (pg_catalog.abs(a) > 0));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog.lower(integer) does not exist
-- end-expected-error
CREATE TABLE zzm3sd_s8 (a int, CHECK (pg_catalog.lower(a) > 'x'));

DROP TABLE zzm3sd_s7;

-- ============================================================================
-- A name written where the walk has to reach
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n1 (a int, CHECK (CASE WHEN nosuchcol > 0 THEN true END));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n2 (a int, CHECK (CASE a WHEN nosuchcol THEN true ELSE false END));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n2b (a int, CHECK (CASE WHEN a > 0 THEN nosuchcol > 1 END));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n3 (a int, CHECK (CASE WHEN a > 0 THEN true ELSE nosuchcol > 1 END));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n4 (a int, CHECK (a IN (nosuchcol)));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n5 (a int, CHECK (nosuchcol IN (1, 2)));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n6 (a int, CHECK (a BETWEEN nosuchcol AND 3));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n7 (a int, CHECK (a BETWEEN 1 AND nosuchcol));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n8 (a int, CHECK (a = ANY (ARRAY[nosuchcol])));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n9 (a int, CHECK ((ROW(nosuchcol, 1)) IS NOT NULL));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n10 (a int, CHECK ((nosuchcol) IS NOT NULL));

-- the places the walk already reached, kept beside the ones it did not
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n11 (a int, CHECK (coalesce(nosuchcol, 1) > 0));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n12 (a int, CHECK (nosuchcol::int > 0));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n13 (a int, CHECK (a > nosuchcol));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n14 (a int, CHECK (abs(abs(nosuchcol)) > 0));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n15 (a int, CHECK (nosuchcol LIKE 'x'));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n16 (a int, CHECK (nullif(nosuchcol, 1) > 0));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n17 (a int, CHECK (a > (nosuchcol + 1)));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n18 (a int, CHECK (a IS NOT NULL AND nosuchcol > 0));

-- a composite type written as a call of one argument is not a cast PostgreSQL
-- performs: it has no input function to read another type's value with
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zzm3sd_comp(integer) does not exist
-- end-expected-error
CREATE TABLE zzm3sd_n19 (a int, CHECK (zzm3sd_comp(a) IS NOT NULL));

DROP TABLE zzm3sd_base;
DROP TABLE zzm3sd_ok1;
DROP TABLE zzm3sd_ok2;
DROP TABLE zzm3sd_ok3;
DROP TABLE zzm3sd_ok4;
DROP TABLE zzm3sd_ok5;
DROP TABLE zzm3sd_ok6;
DROP TABLE zzm3sd_ok7;
DROP TABLE zzm3sd_ok8;
DROP TABLE zzm3sd_ok9;
DROP TABLE zzm3sd_ok10;
DROP DOMAIN zzm3sd_dom;
DROP TYPE zzm3sd_comp;

-- ============================================================================
-- Every place a stored expression is written goes the same way
-- ============================================================================

CREATE TABLE zzm3sd_t (a int, k text);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(integer) does not exist
-- end-expected-error
CREATE TABLE zzm3sd_g (a int, b text GENERATED ALWAYS AS (lower(a)) STORED);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(integer) does not exist
-- end-expected-error
CREATE TABLE zzm3sd_d (a int DEFAULT lower(1));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(integer) does not exist
-- end-expected-error
CREATE INDEX zzm3sd_i1 ON zzm3sd_t (a) WHERE lower(a) > 'x';

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(integer) does not exist
-- end-expected-error
CREATE INDEX zzm3sd_i2 ON zzm3sd_t (lower(a));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(integer) does not exist
-- end-expected-error
ALTER TABLE zzm3sd_t ALTER COLUMN k TYPE text USING lower(a);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(integer) does not exist
-- end-expected-error
ALTER TABLE zzm3sd_t ADD COLUMN c int DEFAULT lower(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(integer) does not exist
-- end-expected-error
ALTER TABLE zzm3sd_t ALTER COLUMN a SET DEFAULT lower(1);

-- and a name written inside a CASE arm reaches every one of them
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE zzm3sd_g2 (a int, b int GENERATED ALWAYS AS (CASE WHEN nosuchcol > 0 THEN 1 END) STORED);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE INDEX zzm3sd_i3 ON zzm3sd_t (a) WHERE CASE WHEN nosuchcol > 0 THEN true END;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE INDEX zzm3sd_i4 ON zzm3sd_t ((CASE WHEN nosuchcol > 0 THEN 1 END));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
ALTER TABLE zzm3sd_t ALTER COLUMN k TYPE text USING (CASE WHEN nosuchcol > 0 THEN 'x' END);

-- an IN list written as an index key is an expression, not a call
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE INDEX zzm3sd_i5 ON zzm3sd_t ((a IN (nosuchcol)));

-- ============================================================================
-- An index's predicate is transformed before its keys
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchpred" does not exist
-- end-expected-error
CREATE UNIQUE INDEX zzm3sd_i6 ON zzm3sd_t (lower(nosuchkey)) WHERE nosuchpred IS NULL;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchpred" does not exist
-- end-expected-error
CREATE INDEX zzm3sd_i7 ON zzm3sd_t (k) WHERE nosuchpred IS NULL;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchkey" does not exist
-- end-expected-error
CREATE INDEX zzm3sd_i8 ON zzm3sd_t (lower(nosuchkey)) WHERE a > 0;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_class WHERE relname LIKE 'zzm3sd\_i%';

DROP TABLE zzm3sd_t;

-- ============================================================================
-- The relation a generation expression names, spelled out with its schema
-- ============================================================================

CREATE TABLE zzm3sd_q1 (a int, b int GENERATED ALWAYS AS (public.zzm3sd_q1.a + 1) STORED);
INSERT INTO zzm3sd_q1 VALUES (7);

-- begin-expected
-- columns: a | b
-- row: 7 | 8
-- end-expected
SELECT a, b FROM zzm3sd_q1;

CREATE TABLE zzm3sd_q2 (a int, CHECK (public.zzm3sd_q2.a > 0));
INSERT INTO zzm3sd_q2 VALUES (3);

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT a FROM zzm3sd_q2;

-- another schema's relation of the same name is not this one
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzm3sd_q3"
-- end-expected-error
CREATE TABLE zzm3sd_q3 (a int, b int GENERATED ALWAYS AS (zzm3sd_nos.zzm3sd_q3.a + 1) STORED);

CREATE SCHEMA zzm3sd_s;

-- a relation created in another schema names that schema
CREATE TABLE zzm3sd_s.zzm3sd_q4 (a int, b int GENERATED ALWAYS AS (zzm3sd_s.zzm3sd_q4.a + 1) STORED);
INSERT INTO zzm3sd_s.zzm3sd_q4 VALUES (5);

-- begin-expected
-- columns: a | b
-- row: 5 | 6
-- end-expected
SELECT a, b FROM zzm3sd_s.zzm3sd_q4;

-- and public is not that schema
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzm3sd_q5"
-- end-expected-error
CREATE TABLE zzm3sd_s.zzm3sd_q5 (a int, b int GENERATED ALWAYS AS (public.zzm3sd_q5.a + 1) STORED);

-- the column under the full name is still looked for
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column zzm3sd_q6.nosuchcol does not exist
-- end-expected-error
CREATE TABLE zzm3sd_q6 (a int, b int GENERATED ALWAYS AS (public.zzm3sd_q6.nosuchcol + 1) STORED);

-- a quoted qualifier keeps the case the writer wrote
CREATE SCHEMA "zzm3sd_MixEd";
CREATE FUNCTION "zzm3sd_MixEd".f(int) RETURNS int AS 'SELECT $1' LANGUAGE sql;
CREATE TABLE zzm3sd_q7 (a int, CHECK ("zzm3sd_MixEd".f(a) > 0));

DROP TABLE zzm3sd_q1;
DROP TABLE zzm3sd_q2;
DROP TABLE zzm3sd_q7;
DROP TABLE zzm3sd_s.zzm3sd_q4;
DROP SCHEMA zzm3sd_s;
DROP FUNCTION "zzm3sd_MixEd".f(int);
DROP SCHEMA "zzm3sd_MixEd";

-- ============================================================================
-- The calls that do resolve, in every one of those places
-- ============================================================================

CREATE TABLE zzm3sd_e (a int, k text);
INSERT INTO zzm3sd_e VALUES (3, 'Ab');
CREATE INDEX zzm3sd_ei1 ON zzm3sd_e (a) WHERE lower(k) > 'x';
CREATE INDEX zzm3sd_ei2 ON zzm3sd_e (lower(k));
CREATE INDEX zzm3sd_ei3 ON zzm3sd_e (abs(a));
CREATE INDEX zzm3sd_ei4 ON zzm3sd_e ((CASE WHEN a > 0 THEN k ELSE 'z' END));
CREATE INDEX zzm3sd_ei5 ON zzm3sd_e (a) WHERE a IN (1, 2, 3);
CREATE INDEX zzm3sd_ei6 ON zzm3sd_e (a) WHERE a BETWEEN 1 AND 9;
CREATE INDEX zzm3sd_ei7 ON zzm3sd_e ((a = ANY (ARRAY[1, 2])));

-- begin-expected
-- columns: n
-- row: 7
-- end-expected
SELECT count(*)::int AS n FROM pg_class WHERE relname LIKE 'zzm3sd\_ei%';

ALTER TABLE zzm3sd_e ALTER COLUMN k TYPE text USING lower(k);

-- begin-expected
-- columns: a | k
-- row: 3 | ab
-- end-expected
SELECT a, k FROM zzm3sd_e;

ALTER TABLE zzm3sd_e ADD COLUMN c text DEFAULT lower('Q');
ALTER TABLE zzm3sd_e ALTER COLUMN a SET DEFAULT abs(-4);
INSERT INTO zzm3sd_e (k) VALUES ('z');

-- begin-expected
-- columns: a | k | c
-- row: 3 | ab | q
-- row: 4 | z | q
-- end-expected
SELECT a, k, c FROM zzm3sd_e ORDER BY k;

DROP TABLE zzm3sd_e;

CREATE TABLE zzm3sd_gg (a int, k text, b text GENERATED ALWAYS AS (lower(k)) STORED, c int GENERATED ALWAYS AS (abs(a) + 1) STORED, d text GENERATED ALWAYS AS (CASE WHEN a > 0 THEN 'p' ELSE 'n' END) STORED, e text GENERATED ALWAYS AS (lower(a::text)) STORED);
INSERT INTO zzm3sd_gg (a, k) VALUES (-2, 'Ab');

-- begin-expected
-- columns: a | k | b | c | d | e
-- row: -2 | Ab | ab | 3 | n | -2
-- end-expected
SELECT a, k, b, c, d, e FROM zzm3sd_gg;

DROP TABLE zzm3sd_gg;

CREATE TABLE zzm3sd_dd (a text DEFAULT lower('Q'), b int DEFAULT abs(-3), c text DEFAULT to_char(5, 'FM999'), d int DEFAULT greatest(1, 2), e text DEFAULT coalesce(NULL, 'x'));
INSERT INTO zzm3sd_dd DEFAULT VALUES;

-- begin-expected
-- columns: a | b | c | d | e
-- row: q | 3 | 5 | 2 | x
-- end-expected
SELECT a, b, c, d, e FROM zzm3sd_dd;

DROP TABLE zzm3sd_dd;
