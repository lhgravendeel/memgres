-- source: investigation.md
-- finding: 53
-- title: RLS PERMISSIVE policies are combined with AND, not OR ⚠️ high — rejects valid writes
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE t (id int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE t ENABLE ROW LEVEL SECURITY;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "r" does not exist
-- end-expected-error
GRANT INSERT, SELECT ON t TO r;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY a ON t AS PERMISSIVE FOR INSERT WITH CHECK (id > 0);
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY b ON t AS PERMISSIVE FOR INSERT WITH CHECK (id > 100);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: role "r" does not exist
-- end-expected-error
SET ROLE r;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO t VALUES (5);
--   PG: 1 row — PERMISSIVE policies are OR-ed, and the row satisfies policy a
--   mg: 42501 new row violates row-level security policy for table "t";
