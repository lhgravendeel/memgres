-- source: review-2026-08.md
-- finding: Root cause 6: the deadlock victim is whoever arrives last, not whoever has waited longest
-- area: LISTEN/NOTIFY, advisory locks and DISCARD
-- title: Root cause 6: the deadlock victim is whoever arrives last, not whoever has waited longest
-- session A                                    -- session B
-- begin-expected
-- ok: 0
-- end-expected
SET lock_timeout = '6s';
-- begin-expected
-- ok: 0
-- end-expected
SET lock_timeout = '6s';
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
LOCK TABLE zz_t IN ACCESS EXCLUSIVE MODE;
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: LOCK TABLE can only be used in transaction blocks
-- end-expected-error
LOCK TABLE zz_u IN ACCESS EXCLUSIVE MODE;
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: LOCK TABLE can only be used in transaction blocks
-- end-expected-error
LOCK TABLE zz_u IN ACCESS EXCLUSIVE MODE;
-- waits
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: LOCK TABLE can only be used in transaction blocks
-- end-expected-error
LOCK TABLE zz_t IN ACCESS EXCLUSIVE MODE;
