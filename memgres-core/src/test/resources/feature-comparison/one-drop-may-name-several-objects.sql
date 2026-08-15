-- One DROP may name several objects, and PostgreSQL takes them all down in the one statement: it
-- settles the whole set before it looks for what would be left pointing at any of it, so an
-- object the same statement drops is no reason to refuse, and a name written twice is dropped
-- once. Every name is looked for in the schema it was written with. A name that reaches nothing
-- takes the whole statement with it unless IF EXISTS was written. A statement that names several
-- says only that it cannot have what it asked for, where one that names a single object names it.
-- The kinds whose grammar names one object at a time have no list at all.

-- setup
CREATE TABLE odn_a (i int);
CREATE TABLE odn_b (i int);

-- stmt 1: every kind that takes a list drops every name in it
DROP TABLE public.odn_a, public.odn_b;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname IN ('odn_a','odn_b');
CREATE VIEW odn_v1 AS SELECT 1 AS x;
CREATE VIEW odn_v2 AS SELECT 2 AS x;
DROP VIEW odn_v1, odn_v2;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname IN ('odn_v1','odn_v2');
CREATE SEQUENCE odn_q1;
CREATE SEQUENCE odn_q2;
DROP SEQUENCE public.odn_q1, public.odn_q2;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname IN ('odn_q1','odn_q2');
CREATE TABLE odn_ix (i int, j int);
CREATE INDEX odn_x1 ON odn_ix (i);
CREATE INDEX odn_x2 ON odn_ix (j);
DROP INDEX public.odn_x1, public.odn_x2;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname IN ('odn_x1','odn_x2');
DROP TABLE odn_ix;
CREATE TYPE odn_e1 AS ENUM ('a');
CREATE TYPE odn_e2 AS ENUM ('b');
DROP TYPE public.odn_e1, public.odn_e2;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_type WHERE typname IN ('odn_e1','odn_e2');
CREATE DOMAIN odn_d1 AS int;
CREATE DOMAIN odn_d2 AS int;
DROP DOMAIN odn_d1, odn_d2;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_type WHERE typname IN ('odn_d1','odn_d2');
CREATE MATERIALIZED VIEW odn_m1 AS SELECT 1 AS x;
CREATE MATERIALIZED VIEW odn_m2 AS SELECT 2 AS x;
DROP MATERIALIZED VIEW public.odn_m1, public.odn_m2;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname IN ('odn_m1','odn_m2');
CREATE SCHEMA odn_s1;
CREATE SCHEMA odn_s2;
DROP SCHEMA odn_s1, odn_s2;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_namespace WHERE nspname IN ('odn_s1','odn_s2');
CREATE FUNCTION odn_f1() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE FUNCTION odn_f2(int) RETURNS int AS $$ SELECT 2 $$ LANGUAGE sql;
DROP FUNCTION odn_f1(), odn_f2(int);
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_proc WHERE proname IN ('odn_f1','odn_f2');

-- stmt 2: every name is looked for in the schema it was written with
CREATE SCHEMA odn_sq;
CREATE TABLE odn_ta (i int);
CREATE TABLE odn_sq.odn_tb (i int);
CREATE TABLE odn_tc (i int);
DROP TABLE public.odn_ta, odn_sq.odn_tb, odn_tc;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname IN ('odn_ta','odn_tb','odn_tc');
DROP SCHEMA odn_sq;

-- stmt 3: one object named twice is dropped once
CREATE VIEW odn_y1 AS SELECT 1 AS x;
DROP VIEW odn_y1, odn_y1;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname = 'odn_y1';
CREATE VIEW odn_y2 AS SELECT 1 AS x;
DROP VIEW odn_y2, public.odn_y2;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname = 'odn_y2';
CREATE TABLE odn_y3 (i int);
DROP TABLE odn_y3, odn_y3;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname = 'odn_y3';

-- stmt 4: a name that reaches nothing takes the whole statement with it
CREATE VIEW odn_k1 AS SELECT 1 AS x;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: view "odn_nosuch" does not exist
-- end-expected-error
DROP VIEW odn_k1, odn_nosuch;
-- begin-expected
-- columns: d
-- row: 1
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname = 'odn_k1';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: view "odn_nosuch" does not exist
-- end-expected-error
DROP VIEW odn_nosuch, odn_k1;
-- begin-expected
-- columns: d
-- row: 1
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname = 'odn_k1';
DROP VIEW IF EXISTS odn_nosuch, odn_k1;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname = 'odn_k1';

-- stmt 5: what the same DROP takes down is no dependency
CREATE TABLE odn_p (i int PRIMARY KEY);
CREATE TABLE odn_c (i int REFERENCES odn_p(i));
DROP TABLE odn_p, odn_c;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname IN ('odn_p','odn_c');
CREATE TABLE odn_rt (i int);
CREATE TABLE odn_rl (i int);
CREATE RULE odn_rr AS ON INSERT TO odn_rt DO ALSO INSERT INTO odn_rl VALUES (new.i);
DROP TABLE odn_rl, odn_rt;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname IN ('odn_rt','odn_rl');
CREATE VIEW odn_u1 AS SELECT 1 AS x;
CREATE VIEW odn_u2 AS SELECT x FROM odn_u1;
DROP VIEW odn_u1, odn_u2;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname IN ('odn_u1','odn_u2');

-- stmt 6: a dependent the list does not name refuses the whole set
CREATE TABLE odn_dp (i int PRIMARY KEY);
CREATE TABLE odn_dc (i int REFERENCES odn_dp(i));
CREATE TABLE odn_do (i int);
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop desired object(s) because other objects depend on them
-- end-expected-error
DROP TABLE odn_dp, odn_do;
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table odn_dp because other objects depend on it
-- end-expected-error
DROP TABLE odn_dp;
DROP TABLE odn_dc, odn_dp;
DROP TABLE odn_do;
CREATE VIEW odn_g1 AS SELECT 1 AS x;
CREATE VIEW odn_g2 AS SELECT x FROM odn_g1;
CREATE VIEW odn_g3 AS SELECT x FROM odn_g2;
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop desired object(s) because other objects depend on them
-- end-expected-error
DROP VIEW odn_g1, odn_g2;
DROP VIEW odn_g3, odn_g2, odn_g1;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_class WHERE relname IN ('odn_g1','odn_g2','odn_g3');

-- stmt 7: a comma where the grammar has no list is a syntax error
CREATE TABLE odn_on (i int);
CREATE TABLE odn_onl (i int);
CREATE RULE odn_o1 AS ON INSERT TO odn_on DO ALSO INSERT INTO odn_onl VALUES (new.i);
CREATE RULE odn_o2 AS ON UPDATE TO odn_on DO ALSO INSERT INTO odn_onl VALUES (new.i);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
DROP RULE odn_o1 ON odn_on, odn_o2 ON odn_on;
-- begin-expected
-- columns: d
-- row: 2
-- end-expected
SELECT count(*)::text AS d FROM pg_rules WHERE rulename IN ('odn_o1','odn_o2');
DROP TABLE odn_on CASCADE;
DROP TABLE odn_onl;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
DROP CAST (int AS text), (text AS int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
DROP OPERATOR CLASS odn_oc1 USING btree, odn_oc2 USING btree;

-- stmt 8: a rule depends on the relation in the schema its action named, and on no other
CREATE SCHEMA odn_rs;
CREATE TABLE odn_rst (i int);
CREATE TABLE odn_rs.odn_rsb (i int);
CREATE TABLE odn_rsb (i int);
CREATE RULE odn_rsr AS ON INSERT TO odn_rst DO ALSO INSERT INTO odn_rs.odn_rsb VALUES (new.i);
DROP TABLE odn_rsb;
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table odn_rs.odn_rsb because other objects depend on it
-- end-expected-error
DROP TABLE odn_rs.odn_rsb;
DROP TABLE odn_rst CASCADE;
DROP SCHEMA odn_rs CASCADE;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_rules WHERE rulename = 'odn_rsr';
