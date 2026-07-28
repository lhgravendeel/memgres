-- GROUP BY validation: what a grouped query may select.
--
-- Two rules, both measured against PostgreSQL 18.
--
--  1. Grouping by a table's PRIMARY KEY functionally determines every other column of that
--     table, so SELECT id, other FROM t GROUP BY id is valid SQL. A UNIQUE NOT NULL column
--     does not grant this, and neither does a view, a sub-select or a CTE over the table.
--  2. Grouping by an expression licenses that expression, not the columns inside it. Anything
--     else outside an aggregate -- in the select list, in HAVING or in ORDER BY -- is an error,
--     not an arbitrary row's value.

-- setup
DROP VIEW IF EXISTS gbv_v CASCADE;
DROP TABLE IF EXISTS gbv_child CASCADE;
DROP TABLE IF EXISTS gbv_t CASCADE;
DROP TABLE IF EXISTS gbv_u CASCADE;
DROP TABLE IF EXISTS gbv_m CASCADE;
DROP TABLE IF EXISTS gbv_p CASCADE;

CREATE TABLE gbv_t (id int PRIMARY KEY, other text, n int NOT NULL);
INSERT INTO gbv_t VALUES (1, 'a', 10), (2, 'b', 20);

CREATE TABLE gbv_child (cid int PRIMARY KEY, tid int, amt int);
INSERT INTO gbv_child VALUES (1, 1, 5), (2, 1, 6), (3, 2, 7);

CREATE TABLE gbv_u (uid int UNIQUE NOT NULL, ucol text);
INSERT INTO gbv_u VALUES (1, 'x'), (2, 'y');

CREATE TABLE gbv_m (a int, b int, c text, PRIMARY KEY (a, b));
INSERT INTO gbv_m VALUES (1, 1, 'p'), (1, 2, 'q');

CREATE TABLE gbv_p (a int, b int, tx text);
INSERT INTO gbv_p VALUES (1, 10, 'p'), (2, 20, 'q'), (1, 30, 'r');

CREATE VIEW gbv_v AS SELECT id, other, n FROM gbv_t;

-- 1: grouping by the primary key selects the rest of the row

-- begin-expected
-- columns: id | other
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT id, other FROM gbv_t GROUP BY id ORDER BY id;

-- begin-expected
-- columns: id | other | n
-- row: 1, a, 10
-- row: 2, b, 20
-- end-expected
SELECT id, other, n FROM gbv_t GROUP BY id ORDER BY id;

-- 2: the key may be written qualified, unqualified, by ordinal or by output alias

-- begin-expected
-- columns: id | other
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT t.id, t.other FROM gbv_t t GROUP BY t.id ORDER BY 1;

-- begin-expected
-- columns: id | other
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT id, other FROM gbv_t t GROUP BY t.id ORDER BY 1;

-- begin-expected
-- columns: id | other
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT id, other FROM gbv_t GROUP BY 1 ORDER BY 1;

-- begin-expected
-- columns: k | other
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT t.id AS k, t.other FROM gbv_t t GROUP BY k ORDER BY 1;

-- 3: the dependency survives a join, a HAVING, an expression and a star

-- begin-expected
-- columns: id | other | count
-- row: 1, a, 2
-- row: 2, b, 1
-- end-expected
SELECT t.id, t.other, count(c.cid) FROM gbv_t t
  LEFT JOIN gbv_child c ON c.tid = t.id GROUP BY t.id ORDER BY 1;

-- begin-expected
-- columns: id | count
-- row: 1, 1
-- end-expected
SELECT id, count(*) FROM gbv_t GROUP BY id HAVING other = 'a';

-- begin-expected
-- columns: id | upper
-- row: 1, A
-- row: 2, B
-- end-expected
SELECT id, upper(other) FROM gbv_t GROUP BY id ORDER BY 1;

-- begin-expected
-- columns: id | other
-- row: 2, b
-- row: 1, a
-- end-expected
SELECT id, other FROM gbv_t GROUP BY id ORDER BY other DESC;

-- begin-expected
-- columns: id | other | n
-- row: 1, a, 10
-- row: 2, b, 20
-- end-expected
SELECT * FROM gbv_t GROUP BY id ORDER BY 1;

-- 4: each relation keeps its own key

-- begin-expected
-- columns: other | amt
-- row: a, 5
-- row: a, 6
-- row: b, 7
-- end-expected
SELECT t.other, c.amt FROM gbv_t t JOIN gbv_child c ON c.tid = t.id
  GROUP BY t.id, c.cid ORDER BY 1, 2;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "c.amt" must appear in the GROUP BY clause
-- end-expected-error
SELECT t.other, c.amt FROM gbv_t t JOIN gbv_child c ON c.tid = t.id GROUP BY t.id ORDER BY 1;

-- 5: a multi-column key has to be grouped whole

-- begin-expected
-- columns: a | b | c
-- row: 1, 1, p
-- row: 1, 2, q
-- end-expected
SELECT a, b, c FROM gbv_m GROUP BY b, a ORDER BY 1, 2;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_m.c" must appear in the GROUP BY clause
-- end-expected-error
SELECT a, c FROM gbv_m GROUP BY a ORDER BY 1;

-- 6: only a PRIMARY KEY determines a row -- UNIQUE NOT NULL does not

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_u.ucol" must appear in the GROUP BY clause
-- end-expected-error
SELECT uid, ucol FROM gbv_u GROUP BY uid ORDER BY 1;

-- 7: a derived relation exposes columns but carries no key

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_v.other" must appear in the GROUP BY clause
-- end-expected-error
SELECT id, other FROM gbv_v GROUP BY id ORDER BY 1;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "x.other" must appear in the GROUP BY clause
-- end-expected-error
SELECT x.id, x.other FROM (SELECT * FROM gbv_t) x GROUP BY x.id ORDER BY 1;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "x.other" must appear in the GROUP BY clause
-- end-expected-error
WITH x AS (SELECT * FROM gbv_t) SELECT x.id, x.other FROM x GROUP BY x.id ORDER BY 1;

-- 8: a grouping-set spec determines a row only when every set groups the key

-- begin-expected
-- columns: id | other
-- row: 1, a
-- row: 2, b
-- end-expected
SELECT id, other FROM gbv_t GROUP BY GROUPING SETS ((id)) ORDER BY 1;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_t.other" must appear in the GROUP BY clause
-- end-expected-error
SELECT id, other FROM gbv_t GROUP BY ROLLUP (id) ORDER BY 1;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_t.other" must appear in the GROUP BY clause
-- end-expected-error
SELECT id, other FROM gbv_t GROUP BY GROUPING SETS ((id), ()) ORDER BY 1;

-- 9: grouping by an expression licenses the expression, not the columns in it

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT b FROM gbv_p GROUP BY a + 0;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT b FROM gbv_p GROUP BY a::text;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT b FROM gbv_p GROUP BY abs(a);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT b FROM gbv_p GROUP BY lower('x');

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.a" must appear in the GROUP BY clause
-- end-expected-error
SELECT a FROM gbv_p GROUP BY a + 0;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.a" must appear in the GROUP BY clause
-- end-expected-error
SELECT a FROM gbv_p GROUP BY CASE WHEN a > 0 THEN a ELSE 0 END;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.tx" must appear in the GROUP BY clause
-- end-expected-error
SELECT tx FROM gbv_p GROUP BY upper(tx);

-- 10: the grouped expression itself, and expressions built over it, stay available

-- begin-expected
-- columns: ?column?
-- row: 2
-- row: 4
-- end-expected
SELECT (a + 0) * 2 FROM gbv_p GROUP BY a + 0 ORDER BY 1;

-- begin-expected
-- columns: length
-- row: 1
-- row: 1
-- row: 1
-- end-expected
SELECT length(upper(tx)) FROM gbv_p GROUP BY upper(tx) ORDER BY 1;

-- begin-expected
-- columns: count
-- row: 2
-- row: 1
-- end-expected
SELECT count(*) FROM gbv_p GROUP BY a + 0 ORDER BY a + 0;

-- 11: an ungrouped column is rejected wherever it appears

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT count(*) FROM gbv_p GROUP BY a HAVING b > 0;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT count(*) FROM gbv_p GROUP BY a ORDER BY b + 1;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.tx" must appear in the GROUP BY clause
-- end-expected-error
SELECT count(*) FROM gbv_p GROUP BY a ORDER BY upper(tx);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.a" must appear in the GROUP BY clause
-- end-expected-error
SELECT count(*) FROM gbv_p HAVING a > 0;

-- 12: the error names the column, qualified by its relation, not the expression around it

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.tx" must appear in the GROUP BY clause
-- end-expected-error
SELECT upper(tx) FROM gbv_p GROUP BY a;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT b + 1 FROM gbv_p GROUP BY a;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "p.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT p.b FROM gbv_p p GROUP BY p.a;

-- 13: an ordinal has to name an output column

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: GROUP BY position 3 is not in select list
-- end-expected-error
SELECT count(*) FROM gbv_p GROUP BY 3;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: GROUP BY position 0 is not in select list
-- end-expected-error
SELECT count(*) FROM gbv_p GROUP BY 0;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: GROUP BY position -1 is not in select list
-- end-expected-error
SELECT count(*) FROM gbv_p GROUP BY -1;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: GROUP BY position 4 is not in select list
-- end-expected-error
SELECT * FROM gbv_t GROUP BY 4;

-- 14: a bare constant that is not an integer is not a grouping expression at all

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in GROUP BY
-- end-expected-error
SELECT count(*) FROM gbv_p GROUP BY 'x';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in GROUP BY
-- end-expected-error
SELECT count(*) FROM gbv_p GROUP BY NULL;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in GROUP BY
-- end-expected-error
SELECT count(*) FROM gbv_p GROUP BY true;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: non-integer constant in GROUP BY
-- end-expected-error
SELECT count(*) FROM gbv_p GROUP BY 1.5;

-- 15: a constant reached through an expression is a value, not a position

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.a" must appear in the GROUP BY clause
-- end-expected-error
SELECT a, count(*) FROM gbv_p GROUP BY 1 + 0;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.a" must appear in the GROUP BY clause
-- end-expected-error
SELECT a, count(*) FROM gbv_p GROUP BY 1::int;

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM gbv_p GROUP BY DATE '2020-01-01';

-- Parentheses do not hide a position.
-- begin-expected
-- columns: a | count
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT a, count(*) FROM gbv_p GROUP BY (1) ORDER BY 1;

-- 16: aggregates and window functions are not grouping expressions

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in GROUP BY
-- end-expected-error
SELECT a FROM gbv_p GROUP BY sum(a);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in GROUP BY
-- end-expected-error
SELECT count(*) FROM gbv_p GROUP BY 1;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in GROUP BY
-- end-expected-error
SELECT a, count(*) FROM gbv_p GROUP BY 2;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in GROUP BY
-- end-expected-error
SELECT a FROM gbv_p GROUP BY row_number() OVER ();

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate function calls cannot be nested
-- end-expected-error
SELECT sum(sum(b)) FROM gbv_p;

-- 17: a GROUP BY name is the input column when one has it, never the shadowing output alias

-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT b AS a, count(*) FROM gbv_p GROUP BY a;

-- Grouping both resolves it, and the query then groups on a as well as b.
-- begin-expected
-- columns: a | count
-- row: 10, 1
-- row: 20, 1
-- row: 30, 1
-- end-expected
SELECT b AS a, count(*) FROM gbv_p GROUP BY a, b ORDER BY 1;

-- A name no relation exposes still falls back to the output alias.
-- begin-expected
-- columns: z | count
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT a + 0 AS z, count(*) FROM gbv_p GROUP BY z ORDER BY 1;

-- 18: ordinary grouped queries are unaffected

-- begin-expected
-- columns: a | count
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT a, count(*) FROM gbv_p GROUP BY a ORDER BY 1;

-- begin-expected
-- columns: a | b | count
-- row: 1, 10, 1
-- row: 1, 30, 1
-- row: 2, 20, 1
-- end-expected
SELECT a, b, count(*) FROM gbv_p GROUP BY 1, 2 ORDER BY 1, 2;

-- begin-expected
-- columns: a | ct
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT a, count(*) AS ct FROM gbv_p GROUP BY a ORDER BY ct DESC, a;

-- begin-expected
-- columns: a | string_agg
-- row: 1, p-r
-- row: 2, q
-- end-expected
SELECT a, string_agg(tx, '-' ORDER BY tx) FROM gbv_p GROUP BY a ORDER BY 1;

-- begin-expected
-- columns: a | sum
-- row: 1, 30
-- row: 2, 20
-- end-expected
SELECT a, sum(b) FILTER (WHERE b > 10) FROM gbv_p GROUP BY a ORDER BY 1;

-- begin-expected
-- columns: a | b | count
-- row: 1, 10, 1
-- row: 1, 30, 1
-- row: 1, NULL, 2
-- row: 2, 20, 1
-- row: 2, NULL, 1
-- row: NULL, NULL, 3
-- end-expected
SELECT a, b, count(*) FROM gbv_p GROUP BY ROLLUP (a, b) ORDER BY 1, 2;

-- begin-expected
-- columns: x | count
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT sub.x, count(*) FROM (SELECT a AS x FROM gbv_p) sub GROUP BY sub.x ORDER BY 1;

-- A derived window column groups and filters like any other column.
-- begin-expected
-- columns: rn
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT sub.rn FROM (SELECT row_number() OVER () AS rn FROM gbv_p) sub
  GROUP BY sub.rn HAVING sub.rn >= 1 ORDER BY 1;

-- begin-expected
-- columns: id | sum
-- row: 1, 11
-- row: 2, 7
-- end-expected
SELECT t.id, sum(c.amt) FROM gbv_t t
  LEFT JOIN LATERAL (SELECT * FROM gbv_child c WHERE c.tid = t.id) c ON true
  GROUP BY t.id ORDER BY 1;

-- begin-expected
-- columns: a | count
-- row: 1, 2
-- row: 2, 2
-- end-expected
SELECT a, count(*) OVER () FROM gbv_p GROUP BY a ORDER BY 1;

-- A window function's arguments are read after grouping, so they need grouping too.
-- begin-expected-error
-- sqlstate: 42803
-- message-like: column "gbv_p.b" must appear in the GROUP BY clause
-- end-expected-error
SELECT a, sum(b) OVER () FROM gbv_p GROUP BY a;

-- cleanup
DROP VIEW gbv_v;
DROP TABLE gbv_child;
DROP TABLE gbv_t;
DROP TABLE gbv_u;
DROP TABLE gbv_m;
DROP TABLE gbv_p;
