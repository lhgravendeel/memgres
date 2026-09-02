-- source: review-2026-08.md
-- finding: Root cause 4: SqlUnparser is a second deparser that covers a fraction of the AST, wrapped in a formatter that rescans its own output
-- area: The parsers
-- title: Root cause 4: SqlUnparser is a second deparser that covers a fraction of the AST, wrapped in a formatter that rescans its own output
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_t (a int, s text, arr int[], ts timestamptz);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf2_w1 AS SELECT a, row_number() OVER (ORDER BY a) AS rn FROM zz_vf2_t;
-- begin-expected
-- columns: pg_get_viewdef:text
-- row:  SELECT a,\n    row_number() OVER (ORDER BY a) AS rn\n   FROM zz_vf2_t;
-- rowcount: 1
-- end-expected
SELECT pg_get_viewdef('zz_vf2_w1'::regclass);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf2_h1 AS WITH zz_vf2_cte AS (SELECT 42 AS a) SELECT a FROM zz_vf2_cte;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "b" does not exist
-- end-expected-error
CREATE VIEW zz_vf2_h2 AS SELECT DISTINCT ON (a) a, b FROM zz_vf2_t ORDER BY a, b;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "b" does not exist
-- end-expected-error
CREATE VIEW zz_vf2_h3 AS SELECT a, sum(b) AS s FROM zz_vf2_t GROUP BY GROUPING SETS ((a), ());
-- begin-expected
-- columns: pg_get_viewdef:text
-- row:  WITH zz_vf2_cte AS (\n         SELECT 42 AS a\n        )\n SELECT a\n   FROM zz_vf2_cte;
-- rowcount: 1
-- end-expected
SELECT pg_get_viewdef('zz_vf2_h1'::regclass);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_h2" does not exist
-- end-expected-error
SELECT pg_get_viewdef('zz_vf2_h2'::regclass);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_h3" does not exist
-- end-expected-error
SELECT pg_get_viewdef('zz_vf2_h3'::regclass);
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf2_s;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_s.t2 (c int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_qt ("From" int, "A b" int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf2_k1 AS SELECT "From" FROM zz_vf2_qt;
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf2_k2 AS SELECT "A b" AS "Out Col" FROM zz_vf2_qt;
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf2_k3 AS SELECT c FROM zz_vf2_s.t2;
-- begin-expected
-- columns: pg_get_viewdef:text
-- row:  SELECT "From"\n   FROM zz_vf2_qt;
-- rowcount: 1
-- end-expected
SELECT pg_get_viewdef('zz_vf2_k1'::regclass);
-- begin-expected
-- columns: pg_get_viewdef:text
-- row:  SELECT "A b" AS "Out Col"\n   FROM zz_vf2_qt;
-- rowcount: 1
-- end-expected
SELECT pg_get_viewdef('zz_vf2_k2'::regclass);
-- begin-expected
-- columns: pg_get_viewdef:text
-- row:  SELECT c\n   FROM zz_vf2_s.t2;
-- rowcount: 1
-- end-expected
SELECT pg_get_viewdef('zz_vf2_k3'::regclass);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf2_v1 AS SELECT 'a,b' AS x, a FROM zz_vf2_t;
-- begin-expected
-- columns: pg_get_viewdef:text
-- row:  SELECT 'a,b'::text AS x,\n    a\n   FROM zz_vf2_t;
-- rowcount: 1
-- end-expected
SELECT pg_get_viewdef('zz_vf2_v1'::regclass);
