-- source: review-2026-08.md
-- finding: Root cause 8: transaction-state gates are ad-hoc per-statement lists
-- area: Transactions, sessions, cursors and locks
-- title: Root cause 8: transaction-state gates are ad-hoc per-statement lists
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_t (i int);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN READ ONLY;
-- begin-expected-error
-- sqlstate: 25006
-- message-like: cannot execute COMMENT in a read-only transaction
-- end-expected-error
COMMENT ON TABLE zz_vf_t IS 'hi';
-- begin-expected-error
-- sqlstate: 22023
-- message-like: no security label providers have been loaded
-- end-expected-error
SECURITY LABEL ON TABLE zz_vf_t IS 'x';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ix (i int PRIMARY KEY, v int);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 25001
-- message-like: CREATE INDEX CONCURRENTLY cannot run inside a transaction block
-- end-expected-error
CREATE INDEX CONCURRENTLY zz_vf_xi ON zz_vf_ix (v);
-- begin-expected
-- ok: 0
-- end-expected
REINDEX TABLE CONCURRENTLY zz_vf_ix;
-- begin-expected
-- ok: 0
-- end-expected
CLUSTER;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_l (i int);
-- begin-expected
-- ok: 0
-- end-expected
DO $$ BEGIN LOCK TABLE zz_vf_l IN ACCESS EXCLUSIVE MODE; END $$;
