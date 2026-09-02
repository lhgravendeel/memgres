-- source: investigation-2026-08.md
-- finding: 353
-- title: Definition-time query errors are re-thrown on two of the three CREATE VIEW paths: the WITH NO DATA branch catches Exception wholesale (MemgresException is a Run
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_u (a int);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_nosuchtable" does not exist
-- end-expected-error
CREATE MATERIALIZED VIEW zz_vf2_mv  AS SELECT * FROM zz_vf2_nosuchtable WITH NO DATA;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE MATERIALIZED VIEW zz_vf2_mv2 AS SELECT nosuchcol FROM zz_vf2_u WITH NO DATA;
-- begin-expected
-- columns: count:int4
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_class WHERE relname IN ('zz_vf2_mv','zz_vf2_mv2');
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf2_u" already exists
-- end-expected-error
CREATE TABLE zz_vf2_u (a int);
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: WITH CHECK OPTION is supported only on automatically updatable views
-- end-expected-error
CREATE VIEW zz_vf2_co AS WITH c AS (SELECT a FROM zz_vf2_u) SELECT a FROM c WITH CHECK OPTION;
