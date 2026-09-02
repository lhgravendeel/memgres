-- source: investigation.md
-- finding: 16
-- title: Relations do not share one namespace
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE ix_ns_tab (a int);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ix_ns_t" does not exist
-- end-expected-error
CREATE INDEX ix_ns_tab ON ix_ns_t (a);
-- PG: 42P07 relation already exists | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ix_ns_t" does not exist
-- end-expected-error
CREATE INDEX ix_ns_seq ON ix_ns_t (a);
-- name of an existing sequence — PG: 42P07 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ix_ns_t" does not exist
-- end-expected-error
CREATE INDEX ix_ns_v   ON ix_ns_t (a);
-- name of an existing view     — PG: 42P07 | mg: OK
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE ix_ns_idx (x int);
-- name of an existing index    — PG: 42P07 | mg: OK
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE IF EXISTS ix_ns_idx;
-- it is an index — PG: 42809 not a table | mg: OK
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "ix_s2" does not exist
-- end-expected-error
CREATE INDEX ix_same_name ON ix_s2.ix_dt (a);
-- same index name in another schema
--   PG: OK | mg: 42P07 relation "ix_same_name" already exists
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "ix_case_t" does not exist
-- end-expected-error
CREATE INDEX "ix_mixedcase" ON ix_case_t (b);
-- quoted, differs only by case
--   PG: OK | mg: 42P07;
