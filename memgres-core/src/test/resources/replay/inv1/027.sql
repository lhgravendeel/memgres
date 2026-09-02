-- source: investigation.md
-- finding: 27
-- title: A cursor survives a savepoint that should destroy it
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
SAVEPOINT s1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
DECLARE c CURSOR FOR SELECT i FROM t ORDER BY i;
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "c" does not exist
-- end-expected-error
FETCH 1 FROM c;
-- both: 1
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: ROLLBACK TO SAVEPOINT can only be used in transaction blocks
-- end-expected-error
ROLLBACK TO SAVEPOINT s1;
-- the cursor was declared after the savepoint
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "c" does not exist
-- end-expected-error
FETCH 1 FROM c;
-- PG: 34000 cursor "c" does not exist | mg: returns 2;
