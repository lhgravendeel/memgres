-- source: review-2026-08.md
-- finding: Root cause 2: a rolled-back transaction replays its undo entries over whatever the row holds now
-- area: Isolation, deadlocks and features in combination
-- title: Root cause 2: a rolled-back transaction replays its undo entries over whatever the row holds now
-- session A                                   -- session B
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
UPDATE zz_t SET v = 1 WHERE i = 1;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT 1/0;
-- A is now aborted, still open
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
UPDATE zz_t SET v = 2 WHERE i = 1;
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- B's write is committed
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
SELECT v FROM zz_t WHERE i = 1;
-- session A                                   -- session B
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
UPDATE zz_t SET v = 1 WHERE i = 1;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT 1/0;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
DELETE FROM zz_t WHERE i = 1;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
SELECT count(*) FROM zz_t WHERE i = 1;
