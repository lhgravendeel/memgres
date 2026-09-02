-- source: review-2026-08.md
-- finding: Root cause 3: SELECT ... FOR UPDATE never re-reads the row it waited for
-- area: Isolation, deadlocks and features in combination
-- title: Root cause 3: SELECT ... FOR UPDATE never re-reads the row it waited for
-- session A                                   -- session B
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
UPDATE zz_t SET v = v + 1 WHERE i = 1;
-- v was 10
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
SELECT v FROM zz_t WHERE i = 1 FOR UPDATE;
-- blocks
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- B unblocks
-- A: BEGIN; UPDATE zz_t SET v = 42 WHERE i = 1;
-- B: SELECT v FROM zz_t WHERE i = 1 FOR UPDATE;   -- blocks
-- A: COMMIT;
-- A: BEGIN; DELETE FROM zz_t WHERE i = 1;
-- B: SELECT v FROM zz_t WHERE i = 1 FOR UPDATE;   -- blocks
-- A: COMMIT;
-- A: BEGIN; UPDATE zz_t SET v = 42 WHERE i = 1;
-- B: SELECT i FROM zz_t WHERE v = 10 FOR UPDATE;  -- blocks
-- A: COMMIT;
