-- source: investigation-2026-08.md
-- finding: 366
-- title: A composite stored in a column is written by a third, quoting-free writer — TypeCoercion.toString(PgRow), reached from coerceForStorage — and read back by a spl
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf2_rt_c AS (a text, b text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_rt_ct (c zz_vf2_rt_c);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_rt_ct VALUES (ROW('a"b','c,d'));
-- begin-expected
-- columns: c:text
-- row: ("a""b","c,d")
-- rowcount: 1
-- end-expected
SELECT c::text FROM zz_vf2_rt_ct;
-- begin-expected
-- columns: a:text | b:text
-- row: <a"b> | <c,d>
-- rowcount: 1
-- end-expected
SELECT '<' || (c).a || '>' AS a, '<' || (c).b || '>' AS b FROM zz_vf2_rt_ct;
-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "zz_vf2_rt_c" already exists
-- end-expected-error
CREATE TYPE zz_vf2_rt_c AS (a text, b text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_rt_cu (c zz_vf2_rt_c);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf2_rt_cu VALUES (ROW('a','b,c')), (ROW('a,b','c'));
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(DISTINCT c) FROM zz_vf2_rt_cu;
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM (SELECT c FROM zz_vf2_rt_cu GROUP BY c) g;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_rt_cx (c zz_vf2_rt_c UNIQUE);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_rt_cx VALUES (ROW('a','b,c'));
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_rt_cx VALUES (ROW('a,b','c'));
-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "zz_vf2_rt_c" already exists
-- end-expected-error
CREATE TYPE zz_vf2_rt_c AS (a text, b text);
-- begin-expected
-- columns: a1:text
-- row: a"b
-- rowcount: 1
-- end-expected
SELECT ('("a""b",z)'::zz_vf2_rt_c).a AS a1;
-- begin-expected
-- columns: a2:text
-- row: a\b
-- rowcount: 1
-- end-expected
SELECT ('("a\\b",z)'::zz_vf2_rt_c).a AS a2;
-- begin-expected
-- columns: ok:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT (ROW('a"b','z')::zz_vf2_rt_c::text::zz_vf2_rt_c).a = 'a"b' AS ok;
