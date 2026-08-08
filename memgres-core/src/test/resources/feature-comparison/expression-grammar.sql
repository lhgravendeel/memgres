-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "<"
-- end-expected-error
SELECT 1 < 0 < 5;

-- begin-expected
-- columns: a
-- row: f
-- end-expected
SELECT 1 = 1 IS NULL AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT 1 < 2 IS TRUE AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT 1 = 1 IS DISTINCT FROM false AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT 1 BETWEEN 0 AND 2 IS TRUE AS a;

-- begin-expected
-- columns: a|b|c
-- row: -4|-11|2
-- end-expected
SELECT ~ 2 + 1 AS a, ~ 5 * 2 AS b, @ -3 + 1 AS c;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT 'abc' LIKE ANY(ARRAY['a%','z%']) AS a;

-- begin-expected
-- columns: a
-- row: f
-- end-expected
SELECT 'abc' LIKE ALL(ARRAY['a%','z%']) AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT 'abc' ~ ANY(ARRAY['^a','^z']) AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT 'abc' ILIKE ANY(ARRAY['A%','z%']) AS a;

-- begin-expected
-- columns: a|b|c
-- row: 16|15|10
-- end-expected
SELECT 0x10 AS a, 0o17 AS b, 0b1010 AS c;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: invalid Unicode escape
-- end-expected-error
SELECT U&'a\b' AS a, length(U&'\') AS b;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "$"
-- end-expected-error
SELECT $ 'hello' $;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near ","
-- end-expected-error
SELECT NULLIF(1, 2, 3);

-- begin-expected
-- columns: float8
-- row: 1.5
-- end-expected
SELECT double precision '1.5';

-- begin-expected
-- columns: varchar
-- row: 1.5
-- end-expected
SELECT character varying '1.5';

-- begin-expected
-- columns: varbit
-- row: 101
-- end-expected
SELECT bit varying '101';

-- begin-expected
-- columns: pg_collation_for
-- row: "C"
-- end-expected
SELECT collation for ('a' COLLATE "C");

-- begin-expected
-- columns: pg_collation_for
-- row: NULL
-- end-expected
SELECT collation for ('a');

-- begin-expected
-- columns: column1
-- row: 1
-- row: 2
-- end-expected
VALUES (1),(2) LIMIT 1+1;

-- begin-expected
-- columns: column1
-- row: 3
-- end-expected
VALUES (1),(2),(3) OFFSET 1+1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "order"
-- end-expected-error
CREATE TABLE zz_gr_kw (order int);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "select"
-- end-expected-error
CREATE TABLE zz_gr_kw2 (select int);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "user"
-- end-expected-error
CREATE TABLE zz_gr_kw3 (user int);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "left"
-- end-expected-error
CREATE TABLE zz_gr_kw4 (left int);

CREATE TABLE zz_gr_cn (exists int, trim int, greatest int);

INSERT INTO zz_gr_cn VALUES (1, 7, 5);

-- begin-expected
-- columns: exists
-- row: 1
-- end-expected
SELECT exists FROM zz_gr_cn;

-- begin-expected
-- columns: trim
-- row: 7
-- end-expected
SELECT trim FROM zz_gr_cn;

-- begin-expected
-- columns: greatest
-- row: 5
-- end-expected
SELECT greatest FROM zz_gr_cn;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT user IS NOT NULL AS a;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "left"
-- end-expected-error
SELECT * FROM zz_gr_cn AS left;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "AS"
-- end-expected-error
SELECT * FROM zz_gr_cn x AS y;

CREATE TABLE zz_gr_m1 (a int, b int);

INSERT INTO zz_gr_m1 VALUES (1,10),(2,20);

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT (zz_gr_m1).a FROM zz_gr_m1 ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT (zz_gr_m1.*).a FROM zz_gr_m1 ORDER BY 1;

CREATE FUNCTION zz_gr_f(a int) RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM zz_gr_m1 WHERE zz_gr_m1.a = $1 $$;

-- begin-expected
-- columns: zz_gr_f
-- row: 1
-- end-expected
SELECT zz_gr_f(1);

CREATE TABLE ZZ_GR_Q1 (a int);

-- begin-expected
-- columns: text
-- row: zz_gr_q1
-- end-expected
SELECT 'ZZ_GR_Q1'::regclass::text;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "$"
-- end-expected-error
SET search_path = $user, public;

-- begin-expected-error
-- sqlstate: 42939
-- message-like: ERROR: unacceptable schema name "pg_zz_ns"
-- end-expected-error
CREATE SCHEMA pg_zz_ns;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "EXISTS"
-- end-expected-error
ALTER SCHEMA IF EXISTS zz_nosuch RENAME TO zz_x;

CREATE TABLE zz_gr_q2 ("MiXeD" int);

INSERT INTO zz_gr_q2 VALUES (1);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "mixed" does not exist
-- end-expected-error
SELECT "mixed" FROM zz_gr_q2;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "mixed" of relation "zz_gr_q2" does not exist
-- end-expected-error
UPDATE zz_gr_q2 SET "mixed" = 9;

-- begin-expected
-- columns: MiXeD
-- row: 1
-- end-expected
SELECT "MiXeD" FROM zz_gr_q2;

CREATE TABLE zz_gr_dc (keep int, gone int);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "GONE" of relation "zz_gr_dc" does not exist
-- end-expected-error
ALTER TABLE zz_gr_dc DROP COLUMN "GONE";

-- begin-expected
-- columns: column_name
-- row: keep
-- row: gone
-- end-expected
SELECT column_name FROM information_schema.columns WHERE table_name = 'zz_gr_dc' ORDER BY ordinal_position;

DROP TABLE zz_gr_cn, zz_gr_m1, zz_gr_q2, zz_gr_dc, zz_gr_q1;

DROP FUNCTION zz_gr_f(int);

