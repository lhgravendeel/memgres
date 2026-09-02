-- source: investigation-2026-08.md
-- finding: 346
-- title: SqlUnparser is a second, weaker deparser wrapped in a formatter that rescans its own output: an unhandled Expression falls through to expr.toString(), selectToS
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vf2_h1 AS WITH zz_vf2_cte AS (SELECT 42 AS a) SELECT a FROM zz_vf2_cte;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_t" does not exist
-- end-expected-error
CREATE VIEW zz_vf2_h2 AS SELECT DISTINCT ON (a) a, b FROM zz_vf2_t ORDER BY a, b;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_t" does not exist
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
CREATE TABLE zz_vf2_t (a int);
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
