-- source: review-2026-08.md
-- finding: Root cause 12: SERIALIZABLE conflict detection records tables, not rows or predicates
-- area: Isolation, deadlocks and features in combination
-- title: Root cause 12: SERIALIZABLE conflict detection records tables, not rows or predicates
-- session A                                   -- session B
-- begin-expected
-- ok: 0
-- end-expected
BEGIN ISOLATION LEVEL SERIALIZABLE;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN ISOLATION LEVEL SERIALIZABLE;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
SELECT v FROM zz_t WHERE i = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
SELECT v FROM zz_t WHERE i = 2;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
UPDATE zz_t SET v = 111 WHERE i = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
UPDATE zz_t SET v = 222 WHERE i = 2;
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
