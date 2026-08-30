CREATE TABLE zh_q ("c c" int);
CREATE INDEX zh_qix ON zh_q ("c c") WHERE "c c" > 0;
-- begin-expected
-- columns: pg_get_indexdef
-- row: CREATE INDEX zh_qix ON public.zh_q USING btree ("c c") WHERE ("c c" > 0)
-- end-expected
SELECT pg_get_indexdef('zh_qix'::regclass);
CREATE TABLE zh_g ("a b" int, t int GENERATED ALWAYS AS ("a b" * 2) STORED);
INSERT INTO zh_g ("a b") VALUES (21);
-- begin-expected
-- columns: t
-- row: 42
-- end-expected
SELECT t FROM zh_g;
CREATE TABLE zh_r ("select" int, t int GENERATED ALWAYS AS ("select" * 2) STORED);
CREATE TABLE zh_b ("y z" int);
CREATE FUNCTION zh_f2() RETURNS int LANGUAGE sql RETURN (SELECT max("y z") FROM zh_b);
CREATE TABLE zh_tw ("a b" int, n int);
CREATE FUNCTION zh_twf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER zh_tg BEFORE INSERT ON zh_tw FOR EACH ROW WHEN (NEW."a b" > 0) EXECUTE FUNCTION zh_twf();
CREATE TABLE zh_c (i int);
ALTER TABLE zh_c ADD CONSTRAINT zh_ck1 CHECK (i NOT BETWEEN 1 AND 10);
ALTER TABLE zh_c ADD CONSTRAINT zh_ck2 CHECK (i NOT BETWEEN SYMMETRIC 1 AND 10);
ALTER TABLE zh_c ADD CONSTRAINT zh_ck3 CHECK (i BETWEEN 1 AND 10);
ALTER TABLE zh_c ADD CONSTRAINT zh_ck4 CHECK (i BETWEEN SYMMETRIC 1 AND 10);
-- begin-expected
-- columns: n | d
-- row: zh_ck1 | CHECK (((i < 1) OR (i > 10)))
-- row: zh_ck2 | CHECK ((((i < 1) OR (i > 10)) AND ((i < 10) OR (i > 1))))
-- row: zh_ck3 | CHECK (((i >= 1) AND (i <= 10)))
-- row: zh_ck4 | CHECK ((((i >= 1) AND (i <= 10)) OR ((i >= 10) AND (i <= 1))))
-- end-expected
SELECT conname::text AS n, pg_get_constraintdef(oid) AS d FROM pg_constraint WHERE conrelid='zh_c'::regclass ORDER BY conname;
CREATE INDEX zh_px ON zh_c ((i + 1)) WHERE i NOT BETWEEN 1 AND 5;
-- begin-expected
-- columns: pg_get_indexdef
-- row: CREATE INDEX zh_px ON public.zh_c USING btree (((i + 1))) WHERE ((i < 1) OR (i > 5))
-- end-expected
SELECT pg_get_indexdef('zh_px'::regclass);
CREATE TABLE zh_t (a int, b int, c int);
CREATE VIEW zh_v1 AS SELECT a, sum(c) AS s FROM zh_t GROUP BY GROUPING SETS ((a), ());
CREATE VIEW zh_v2 AS SELECT a, b, sum(c) AS s FROM zh_t GROUP BY ROLLUP (a, b);
CREATE VIEW zh_v3 AS SELECT a, b, sum(c) AS s FROM zh_t GROUP BY CUBE (a, b);
CREATE VIEW zh_v4 AS SELECT a, sum(c) AS s FROM zh_t GROUP BY a, ROLLUP (b);
CREATE VIEW zh_v5 AS SELECT a, sum(c) AS s FROM zh_t GROUP BY GROUPING SETS (a, b);
CREATE VIEW zh_v6 AS SELECT a, sum(c) AS s FROM zh_t GROUP BY DISTINCT ROLLUP (a, b);
CREATE VIEW zh_v7 AS SELECT a, b, sum(c) AS s FROM zh_t GROUP BY a, b;
CREATE VIEW zh_v8 AS SELECT a, sum(c) AS s FROM zh_t GROUP BY GROUPING SETS ((a, b), (a), ());
CREATE VIEW zh_v9 AS SELECT a, sum(c) AS s FROM zh_t GROUP BY GROUPING SETS (ROLLUP(a, b), (c));
CREATE VIEW zh_va AS SELECT a, sum(c) AS s FROM zh_t GROUP BY ROLLUP(a), CUBE(b);
-- begin-expected
-- columns: d
-- row:  SELECT a,     sum(c) AS s    FROM zh_t   GROUP BY GROUPING SETS ((a), ());
-- end-expected
SELECT replace(pg_get_viewdef('zh_v1'::regclass), chr(10), ' ') AS d;
-- begin-expected
-- columns: d
-- row:  SELECT a,     b,     sum(c) AS s    FROM zh_t   GROUP BY ROLLUP(a, b);
-- end-expected
SELECT replace(pg_get_viewdef('zh_v2'::regclass), chr(10), ' ') AS d;
-- begin-expected
-- columns: d
-- row:  SELECT a,     b,     sum(c) AS s    FROM zh_t   GROUP BY CUBE(a, b);
-- end-expected
SELECT replace(pg_get_viewdef('zh_v3'::regclass), chr(10), ' ') AS d;
-- begin-expected
-- columns: d
-- row:  SELECT a,     sum(c) AS s    FROM zh_t   GROUP BY a, ROLLUP(b);
-- end-expected
SELECT replace(pg_get_viewdef('zh_v4'::regclass), chr(10), ' ') AS d;
-- begin-expected
-- columns: d
-- row:  SELECT a,     sum(c) AS s    FROM zh_t   GROUP BY GROUPING SETS ((a), (b));
-- end-expected
SELECT replace(pg_get_viewdef('zh_v5'::regclass), chr(10), ' ') AS d;
-- begin-expected
-- columns: d
-- row:  SELECT a,     sum(c) AS s    FROM zh_t   GROUP BY DISTINCT ROLLUP(a, b);
-- end-expected
SELECT replace(pg_get_viewdef('zh_v6'::regclass), chr(10), ' ') AS d;
-- begin-expected
-- columns: d
-- row:  SELECT a,     b,     sum(c) AS s    FROM zh_t   GROUP BY a, b;
-- end-expected
SELECT replace(pg_get_viewdef('zh_v7'::regclass), chr(10), ' ') AS d;
-- begin-expected
-- columns: d
-- row:  SELECT a,     sum(c) AS s    FROM zh_t   GROUP BY GROUPING SETS ((a, b), (a), ());
-- end-expected
SELECT replace(pg_get_viewdef('zh_v8'::regclass), chr(10), ' ') AS d;
-- begin-expected
-- columns: d
-- row:  SELECT a,     sum(c) AS s    FROM zh_t   GROUP BY GROUPING SETS (ROLLUP(a, b), (c));
-- end-expected
SELECT replace(pg_get_viewdef('zh_v9'::regclass), chr(10), ' ') AS d;
-- begin-expected
-- columns: d
-- row:  SELECT a,     sum(c) AS s    FROM zh_t   GROUP BY ROLLUP(a), CUBE(b);
-- end-expected
SELECT replace(pg_get_viewdef('zh_va'::regclass), chr(10), ' ') AS d;
CREATE TABLE zh_base (a int);
INSERT INTO zh_base VALUES (1);
CREATE VIEW zh_cv AS WITH zh_base AS (SELECT 42 AS a) SELECT a FROM zh_base;
ALTER TABLE zh_base RENAME TO zh_base2;
UPDATE zh_base2 SET a = 99;
-- begin-expected
-- columns: a
-- row: 42
-- end-expected
SELECT a FROM zh_cv;
CREATE TABLE zh_pp (a int);
INSERT INTO zh_pp VALUES (7);
CREATE TEMP TABLE zh_pp (a int);
CREATE VIEW zh_tv AS SELECT a FROM public.zh_pp;
-- begin-expected
-- columns: in_public
-- row: t
-- end-expected
SELECT (table_schema = 'public') AS in_public FROM information_schema.views WHERE table_name='zh_tv';
-- begin-expected
-- columns: a
-- row: 7
-- end-expected
SELECT a FROM zh_tv;
CREATE TABLE zh_u (a int);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zh_nosuchtable" does not exist
-- end-expected-error
CREATE MATERIALIZED VIEW zh_mv AS SELECT * FROM zh_nosuchtable WITH NO DATA;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE MATERIALIZED VIEW zh_mv2 AS SELECT nosuchcol FROM zh_u WITH NO DATA;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_class WHERE relname IN ('zh_mv','zh_mv2');
CREATE MATERIALIZED VIEW zh_mv3 AS SELECT a FROM zh_u WITH NO DATA;
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_class WHERE relname='zh_mv3';
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: WITH CHECK OPTION is supported only on automatically updatable views
-- end-expected-error
CREATE VIEW zh_co AS WITH c AS (SELECT a FROM zh_u) SELECT a FROM c WITH CHECK OPTION;
DROP MATERIALIZED VIEW zh_mv3;
DROP VIEW zh_v1, zh_v2, zh_v3, zh_v4, zh_v5, zh_v6, zh_v7, zh_v8, zh_v9, zh_va, zh_cv, zh_tv;
DROP TABLE zh_q, zh_g, zh_r, zh_b, zh_tw, zh_c, zh_t, zh_base2, zh_u CASCADE;
DROP TABLE pg_temp.zh_pp;
DROP TABLE public.zh_pp;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zh_f2() does not exist
-- end-expected-error
DROP FUNCTION zh_f2();
DROP FUNCTION zh_twf();
