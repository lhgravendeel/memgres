-- source: review-2026-08.md
-- finding: Root cause 1: authorisation is decided by one function that only knows about table-level grants
-- area: System catalogs, information_schema and security
-- title: Root cause 1: authorisation is decided by one function that only knows about table-level grants
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c1 (a int, b text, c int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_c1 VALUES (1,'x',1),(2,'y',2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_cr;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT (a, b) ON zz_c1 TO zz_cr;
-- begin-expected
-- ok: 0
-- end-expected
GRANT USAGE ON SCHEMA public TO zz_cr;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_cr;
-- begin-expected
-- columns: a:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT a FROM zz_c1 ORDER BY a;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_sc;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_sc.u (i int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_sc.u VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_sr LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_sc.u TO zz_sr;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_sr;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: permission denied for schema zz_sc
-- end-expected-error
SELECT i FROM zz_sc.u;
-- PG: 42501 permission denied for schema zz_sc
-- begin-expected-error
-- sqlstate: 42501
-- message-like: permission denied for schema zz_sc
-- end-expected-error
CREATE TABLE zz_sc.v (i int);
-- PG: 42501 permission denied for schema zz_sc
-- begin-expected-error
-- sqlstate: 42501
-- message-like: permission denied for schema public
-- end-expected-error
CREATE TABLE zz_pubtab (i int);
-- PG: 42501 permission denied for schema public
-- begin-expected-error
-- sqlstate: 42501
-- message-like: permission denied for database memgrestest
-- end-expected-error
CREATE SCHEMA zz_sc2;
-- PG: 42501 permission denied for database
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_fn() RETURNS int LANGUAGE sql AS 'SELECT 42';
-- begin-expected
-- ok: 0
-- end-expected
REVOKE EXECUTE ON FUNCTION zz_fn() FROM PUBLIC;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_fr LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_fr;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: permission denied for function zz_fn
-- end-expected-error
SELECT zz_fn();
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_pt (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_pr LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
GRANT USAGE ON SCHEMA public TO zz_pr;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_pr;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: permission denied for table zz_pt
-- end-expected-error
SELECT a FROM zz_pt;
