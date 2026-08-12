-- A rule depends on the relations its actions name, and a relation belongs to a schema. So a
-- DROP SCHEMA ... CASCADE reaches exactly the rules whose actions named something in the schema
-- it drops: a relation of the same name in a schema that is still there is nothing to them, and
-- the relation such a rule sits on goes on taking writes.

-- ============================================================================
-- Dropping a schema that only shares a relation's name leaves the rule alone
-- ============================================================================
CREATE SCHEMA crs_n;
CREATE TABLE public.crs_dep (i int);
CREATE TABLE public.crs_keep (i int);
CREATE TABLE crs_n.crs_dep (i int);
CREATE TABLE crs_n.crs_keep (i int);
CREATE RULE crs_rk AS ON INSERT TO public.crs_keep DO ALSO INSERT INTO public.crs_dep VALUES (new.i);
INSERT INTO public.crs_keep VALUES (1);

-- begin-expected
-- columns: a | b
-- row: 1, 0
-- end-expected
SELECT (SELECT count(*) FROM public.crs_dep) AS a, (SELECT count(*) FROM crs_n.crs_dep) AS b;

DROP SCHEMA crs_n CASCADE;

-- begin-expected
-- columns: d
-- row: public/crs_keep/crs_rk
-- end-expected
SELECT schemaname || '/' || tablename || '/' || rulename AS d FROM pg_rules WHERE rulename = 'crs_rk';

-- the relation the rule sits on still takes every write, and the rule still fires
INSERT INTO public.crs_keep VALUES (2);
INSERT INTO public.crs_keep VALUES (3), (4);
INSERT INTO public.crs_keep SELECT 5;

-- begin-expected
-- columns: k | d
-- row: 1/2/3/4/5, 1/2/3/4/5
-- end-expected
SELECT (SELECT string_agg(i::text, '/' ORDER BY i) FROM public.crs_keep) AS k,
       (SELECT string_agg(i::text, '/' ORDER BY i) FROM public.crs_dep) AS d;

UPDATE public.crs_keep SET i = i * 10 WHERE i = 1;
DELETE FROM public.crs_keep WHERE i = 3;

-- begin-expected
-- columns: k | d
-- row: 2/4/5/10, 1/2/3/4/5
-- end-expected
SELECT (SELECT string_agg(i::text, '/' ORDER BY i) FROM public.crs_keep) AS k,
       (SELECT string_agg(i::text, '/' ORDER BY i) FROM public.crs_dep) AS d;

-- relhasrules is "has (or once had) rules", and this relation still has one
-- begin-expected
-- columns: h
-- row: t
-- end-expected
SELECT relhasrules AS h FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relname = 'crs_keep' AND n.nspname = 'public';

-- and the rule still stands in the way of dropping what its action names
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table crs_dep because other objects depend on it
-- end-expected-error
DROP TABLE public.crs_dep;

DROP TABLE public.crs_keep CASCADE;
DROP TABLE public.crs_dep CASCADE;

-- ============================================================================
-- The rule whose action names the dropped schema goes; its neighbour stays
-- ============================================================================
CREATE SCHEMA crs_m;
CREATE TABLE crs_m.crs_ml (i int);
CREATE TABLE public.crs_ml (i int);
CREATE TABLE public.crs_mt (i int);
CREATE RULE crs_r1 AS ON INSERT TO public.crs_mt DO ALSO INSERT INTO crs_m.crs_ml VALUES (new.i);
CREATE RULE crs_r2 AS ON INSERT TO public.crs_mt DO ALSO INSERT INTO public.crs_ml VALUES (new.i + 100);
INSERT INTO public.crs_mt VALUES (1);

-- begin-expected
-- columns: s | p
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM crs_m.crs_ml) AS s, (SELECT count(*) FROM public.crs_ml) AS p;

DROP SCHEMA crs_m CASCADE;

-- begin-expected
-- columns: r
-- row: crs_r2
-- end-expected
SELECT rulename AS r FROM pg_rules WHERE rulename IN ('crs_r1', 'crs_r2') ORDER BY 1;

INSERT INTO public.crs_mt VALUES (2);

-- begin-expected
-- columns: t | l
-- row: 2, 101/102
-- end-expected
SELECT (SELECT count(*) FROM public.crs_mt) AS t,
       (SELECT string_agg(i::text, '/' ORDER BY i) FROM public.crs_ml) AS l;

-- the name the drop freed can be written again, and the new rule fires
CREATE RULE crs_r1 AS ON INSERT TO public.crs_mt DO ALSO INSERT INTO public.crs_ml VALUES (new.i + 200);
INSERT INTO public.crs_mt VALUES (3);

-- begin-expected
-- columns: l
-- row: 101/102/103/203
-- end-expected
SELECT string_agg(i::text, '/' ORDER BY i) AS l FROM public.crs_ml;

DROP TABLE public.crs_mt CASCADE;
DROP TABLE public.crs_ml CASCADE;

-- ============================================================================
-- A rule written inside the dropped schema goes; the same-named relation
-- outside it keeps its own rule
-- ============================================================================
CREATE SCHEMA crs_i;
CREATE TABLE crs_i.src (i int);
CREATE TABLE public.crs_src (i int);
CREATE TABLE public.crs_out (i int);
CREATE RULE crs_ri AS ON INSERT TO crs_i.src DO ALSO INSERT INTO public.crs_out VALUES (new.i);
CREATE RULE crs_rp AS ON INSERT TO public.crs_src DO ALSO INSERT INTO public.crs_out VALUES (new.i + 100);
INSERT INTO crs_i.src VALUES (1);
INSERT INTO public.crs_src VALUES (2);

-- begin-expected
-- columns: d
-- row: 1/102
-- end-expected
SELECT string_agg(i::text, '/' ORDER BY i) AS d FROM public.crs_out;

-- begin-expected
-- columns: d
-- row: crs_i/src/crs_ri
-- row: public/crs_src/crs_rp
-- end-expected
SELECT schemaname || '/' || tablename || '/' || rulename AS d FROM pg_rules
 WHERE rulename IN ('crs_ri', 'crs_rp') ORDER BY 1;

DROP SCHEMA crs_i CASCADE;

-- begin-expected
-- columns: d
-- row: public/crs_src/crs_rp
-- end-expected
SELECT schemaname || '/' || tablename || '/' || rulename AS d FROM pg_rules
 WHERE rulename IN ('crs_ri', 'crs_rp') ORDER BY 1;

INSERT INTO public.crs_src VALUES (3);

-- begin-expected
-- columns: s | o
-- row: 2, 1/102/103
-- end-expected
SELECT (SELECT count(*) FROM public.crs_src) AS s,
       (SELECT string_agg(i::text, '/' ORDER BY i) FROM public.crs_out) AS o;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table crs_out because other objects depend on it
-- end-expected-error
DROP TABLE public.crs_out;

DROP TABLE public.crs_src CASCADE;
DROP TABLE public.crs_out CASCADE;

-- ============================================================================
-- An action that only reads the dropped schema takes the rule with it too
-- ============================================================================
CREATE SCHEMA crs_z;
CREATE TABLE crs_z.rd (i int);
CREATE TABLE public.crs_rd (i int);
CREATE TABLE public.crs_zs (i int);
CREATE TABLE public.crs_zl (i int);
CREATE RULE crs_rrd AS ON INSERT TO public.crs_zs DO ALSO INSERT INTO public.crs_zl SELECT count(*) FROM crs_z.rd;
INSERT INTO public.crs_zs VALUES (1);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM public.crs_zl;

DROP SCHEMA crs_z CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_rules WHERE rulename = 'crs_rrd';

INSERT INTO public.crs_zs VALUES (2);

-- begin-expected
-- columns: s | l
-- row: 2, 1
-- end-expected
SELECT (SELECT count(*) FROM public.crs_zs) AS s, (SELECT count(*) FROM public.crs_zl) AS l;

DROP TABLE public.crs_zs CASCADE;
DROP TABLE public.crs_zl CASCADE;
DROP TABLE public.crs_rd CASCADE;

-- ============================================================================
-- RESTRICT drops the empty schema and reaches no rule at all
-- ============================================================================
CREATE SCHEMA crs_e;
CREATE TABLE public.crs_ed (i int);
CREATE TABLE public.crs_ek (i int);
CREATE RULE crs_re AS ON INSERT TO public.crs_ek DO ALSO INSERT INTO public.crs_ed VALUES (new.i);
DROP SCHEMA crs_e RESTRICT;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_rules WHERE rulename = 'crs_re';

INSERT INTO public.crs_ek VALUES (1);

-- begin-expected
-- columns: k | d
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM public.crs_ek) AS k, (SELECT count(*) FROM public.crs_ed) AS d;

-- a schema that is not there is nothing to the rule either
DROP SCHEMA IF EXISTS crs_e CASCADE;
DROP SCHEMA IF EXISTS crs_nothere CASCADE;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_rules WHERE rulename = 'crs_re';

INSERT INTO public.crs_ek VALUES (2);

-- begin-expected
-- columns: k | d
-- row: 2, 2
-- end-expected
SELECT (SELECT count(*) FROM public.crs_ek) AS k, (SELECT count(*) FROM public.crs_ed) AS d;

DROP TABLE public.crs_ek CASCADE;
DROP TABLE public.crs_ed CASCADE;

-- ============================================================================
-- A drop rolled back leaves the rule where it was and the relation writable
-- ============================================================================
CREATE SCHEMA crs_g;
CREATE TABLE public.crs_gd (i int);
CREATE TABLE public.crs_gk (i int);
CREATE TABLE crs_g.crs_gd (i int);
CREATE RULE crs_rg AS ON INSERT TO public.crs_gk DO ALSO INSERT INTO public.crs_gd VALUES (new.i);
BEGIN;
DROP SCHEMA crs_g CASCADE;
INSERT INTO public.crs_gk VALUES (1);

-- begin-expected
-- columns: k | d
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM public.crs_gk) AS k, (SELECT count(*) FROM public.crs_gd) AS d;

ROLLBACK;

-- begin-expected
-- columns: d
-- row: public/crs_gk/crs_rg
-- end-expected
SELECT schemaname || '/' || tablename || '/' || rulename AS d FROM pg_rules WHERE rulename = 'crs_rg';

INSERT INTO public.crs_gk VALUES (2);

-- begin-expected
-- columns: k | d
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM public.crs_gk) AS k, (SELECT count(*) FROM public.crs_gd) AS d;

DROP SCHEMA IF EXISTS crs_g CASCADE;
DROP TABLE public.crs_gk CASCADE;
DROP TABLE public.crs_gd CASCADE;

-- ============================================================================
-- A view is a relation like any other, and so is the view a rule sits on
-- ============================================================================
CREATE SCHEMA crs_vs;
CREATE TABLE public.crs_vb (i int);
CREATE VIEW public.crs_vw AS SELECT i FROM public.crs_vb;
CREATE RULE crs_rvw AS ON INSERT TO public.crs_vw DO INSTEAD INSERT INTO public.crs_vb VALUES (new.i);
CREATE TABLE crs_vs.crs_vb (i int);
CREATE VIEW crs_vs.crs_vw AS SELECT i FROM crs_vs.crs_vb;
CREATE RULE crs_rvw AS ON INSERT TO crs_vs.crs_vw DO INSTEAD INSERT INTO crs_vs.crs_vb VALUES (new.i + 50);
INSERT INTO public.crs_vw VALUES (1);
INSERT INTO crs_vs.crs_vw VALUES (2);

-- begin-expected
-- columns: a | b
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM public.crs_vb) AS a, (SELECT count(*) FROM crs_vs.crs_vb) AS b;

DROP SCHEMA crs_vs CASCADE;

-- begin-expected
-- columns: d
-- row: public/crs_vw/crs_rvw
-- end-expected
SELECT schemaname || '/' || tablename || '/' || rulename AS d FROM pg_rules
 WHERE rulename = 'crs_rvw' ORDER BY 1;

-- the view still takes the write the surviving rule turns into one on its table
INSERT INTO public.crs_vw VALUES (3);

-- begin-expected
-- columns: b | v
-- row: 1/3, 2
-- end-expected
SELECT (SELECT string_agg(i::text, '/' ORDER BY i) FROM public.crs_vb) AS b,
       (SELECT count(*) FROM public.crs_vw) AS v;

DROP VIEW public.crs_vw;
DROP TABLE public.crs_vb;

-- ============================================================================
-- Two schemas on the search path, both holding the name an action writes
-- ============================================================================
CREATE SCHEMA crs_p2;
CREATE TABLE crs_p2.crs_dp (i int);
CREATE TABLE public.crs_dp (i int);
CREATE TABLE public.crs_kp (i int);
SET search_path = crs_p2, public;
CREATE RULE crs_ru AS ON INSERT TO public.crs_kp DO ALSO INSERT INTO crs_dp VALUES (new.i);
INSERT INTO public.crs_kp VALUES (1);

-- the schema ahead on the path is the one the action names
-- begin-expected
-- columns: pub | s
-- row: 0, 1
-- end-expected
SELECT (SELECT count(*) FROM public.crs_dp) AS pub, (SELECT count(*) FROM crs_p2.crs_dp) AS s;

DROP SCHEMA crs_p2 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_rules WHERE rulename = 'crs_ru';

INSERT INTO public.crs_kp VALUES (2);

-- begin-expected
-- columns: k | pub
-- row: 2, 0
-- end-expected
SELECT (SELECT count(*) FROM public.crs_kp) AS k, (SELECT count(*) FROM public.crs_dp) AS pub;

RESET search_path;
DROP TABLE public.crs_kp CASCADE;
DROP TABLE public.crs_dp CASCADE;

-- and the other way round: the schema behind public on the path is nothing to it
CREATE SCHEMA crs_p3;
CREATE TABLE crs_p3.crs_qd (i int);
CREATE TABLE public.crs_qd (i int);
CREATE TABLE public.crs_qk (i int);
SET search_path = public, crs_p3;
CREATE RULE crs_rq3 AS ON INSERT TO public.crs_qk DO ALSO INSERT INTO crs_qd VALUES (new.i);
INSERT INTO public.crs_qk VALUES (1);

-- begin-expected
-- columns: pub | s
-- row: 1, 0
-- end-expected
SELECT (SELECT count(*) FROM public.crs_qd) AS pub, (SELECT count(*) FROM crs_p3.crs_qd) AS s;

DROP SCHEMA crs_p3 CASCADE;

-- begin-expected
-- columns: d
-- row: public/crs_qk/crs_rq3
-- end-expected
SELECT schemaname || '/' || tablename || '/' || rulename AS d FROM pg_rules WHERE rulename = 'crs_rq3';

INSERT INTO public.crs_qk VALUES (2);

-- begin-expected
-- columns: k | d
-- row: 2, 2
-- end-expected
SELECT (SELECT count(*) FROM public.crs_qk) AS k, (SELECT count(*) FROM public.crs_qd) AS d;

RESET search_path;
DROP TABLE public.crs_qk CASCADE;
DROP TABLE public.crs_qd CASCADE;

-- ============================================================================
-- Two schemas holding the same relation names each keep their own rule
-- ============================================================================
CREATE SCHEMA crs_ss;
CREATE SCHEMA crs_ss2;
CREATE TABLE crs_ss.t (i int);
CREATE TABLE crs_ss.l (i int);
CREATE TABLE crs_ss2.t (i int);
CREATE TABLE crs_ss2.l (i int);
CREATE RULE crs_rs AS ON INSERT TO crs_ss.t DO ALSO INSERT INTO crs_ss.l VALUES (new.i);
CREATE RULE crs_rs2 AS ON INSERT TO crs_ss2.t DO ALSO INSERT INTO crs_ss2.l VALUES (new.i + 20);

-- begin-expected
-- columns: d
-- row: crs_ss/t/crs_rs
-- row: crs_ss2/t/crs_rs2
-- end-expected
SELECT schemaname || '/' || tablename || '/' || rulename AS d FROM pg_rules
 WHERE rulename IN ('crs_rs', 'crs_rs2') ORDER BY 1;

DROP SCHEMA crs_ss CASCADE;

-- begin-expected
-- columns: d
-- row: crs_ss2/t/crs_rs2
-- end-expected
SELECT schemaname || '/' || tablename || '/' || rulename AS d FROM pg_rules
 WHERE rulename IN ('crs_rs', 'crs_rs2') ORDER BY 1;

INSERT INTO crs_ss2.t VALUES (1);

-- begin-expected
-- columns: t | l
-- row: 1, 21
-- end-expected
SELECT (SELECT count(*) FROM crs_ss2.t) AS t, (SELECT string_agg(i::text, '/') FROM crs_ss2.l) AS l;

DROP SCHEMA crs_ss2 CASCADE;

-- ============================================================================
-- The rules on the other write paths are left alone the same way
-- ============================================================================
CREATE SCHEMA crs_u;
CREATE TABLE public.crs_ud (i int);
CREATE TABLE public.crs_uk (i int);
CREATE TABLE crs_u.crs_ud (i int);
CREATE RULE crs_ru1 AS ON UPDATE TO public.crs_uk DO ALSO INSERT INTO public.crs_ud VALUES (new.i);
CREATE RULE crs_ru2 AS ON DELETE TO public.crs_uk DO ALSO INSERT INTO public.crs_ud VALUES (old.i * -1);
INSERT INTO public.crs_uk VALUES (5);
DROP SCHEMA crs_u CASCADE;

-- begin-expected
-- columns: r
-- row: crs_ru1
-- row: crs_ru2
-- end-expected
SELECT rulename AS r FROM pg_rules WHERE rulename IN ('crs_ru1', 'crs_ru2') ORDER BY 1;

UPDATE public.crs_uk SET i = 6;
DELETE FROM public.crs_uk;

-- begin-expected
-- columns: d | k
-- row: -6/6, 0
-- end-expected
SELECT (SELECT string_agg(i::text, '/' ORDER BY i) FROM public.crs_ud) AS d,
       (SELECT count(*) FROM public.crs_uk) AS k;

DROP TABLE public.crs_uk CASCADE;
DROP TABLE public.crs_ud CASCADE;

-- ============================================================================
-- A rule with no action to name a relation with is left alone as well
-- ============================================================================
CREATE SCHEMA crs_nn;
CREATE TABLE crs_nn.crs_nk (i int);
CREATE TABLE public.crs_nk (i int);
CREATE RULE crs_rn AS ON INSERT TO public.crs_nk DO INSTEAD NOTHING;
DROP SCHEMA crs_nn CASCADE;

-- begin-expected
-- columns: d
-- row: public/crs_nk/crs_rn
-- end-expected
SELECT schemaname || '/' || tablename || '/' || rulename AS d FROM pg_rules WHERE rulename = 'crs_rn';

INSERT INTO public.crs_nk VALUES (1);

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM public.crs_nk;

DROP RULE crs_rn ON public.crs_nk;
INSERT INTO public.crs_nk VALUES (2);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM public.crs_nk;

DROP TABLE public.crs_nk CASCADE;
