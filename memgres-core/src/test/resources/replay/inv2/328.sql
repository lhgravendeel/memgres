-- source: investigation-2026-08.md
-- finding: 328
-- title: Nothing assembles the dump: the catalog columns, the pg_get_*def strings and pg_depend are produced independently, each to answer a query rather than to reprodu
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_r2_pgdump_s;
-- begin-expected
-- ok: 0
-- end-expected
SET search_path = zz_r2_pgdump_s;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_r2_pgdump_gen (ser serial, w integer);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_r2_pgdump_parent (a integer, b text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_r2_pgdump_inh (c date) INHERITS (zz_r2_pgdump_parent);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_r2_pgdump_cons" does not exist
-- end-expected-error
CREATE VIEW zz_r2_pgdump_v1 AS SELECT a FROM zz_r2_pgdump_cons WHERE d > 0;
-- pg_dump -U memgres --schema=zz_r2_pgdump_s -f dump.sql ; psql -f dump.sql;
