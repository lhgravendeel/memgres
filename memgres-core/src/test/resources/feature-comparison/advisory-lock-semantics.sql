-- ============================================================================
-- Feature Comparison: Advisory lock semantics (single-connection)
-- Reference counting, mode separation, keyspace separation, xact-scoped locks,
-- and unlock-of-not-held behavior. Multi-session blocking semantics are covered
-- by unit tests (AdvisoryLockSemanticsTest); this file only exercises what a
-- single connection can observe.
-- ============================================================================

-- 1. try-lock on a free key returns true
-- begin-expected
-- columns: got
-- row: t
-- end-expected
SELECT pg_try_advisory_lock(801001)::text AS got;

-- 2. Re-acquiring in the same session succeeds (reference count -> 2)
-- begin-expected
-- columns: again
-- row: t
-- end-expected
SELECT pg_try_advisory_lock(801001)::text AS again;

-- 3. Each acquisition needs a matching unlock: first unlock -> t
-- begin-expected
-- columns: u1
-- row: t
-- end-expected
SELECT pg_advisory_unlock(801001)::text AS u1;

-- 4. Second unlock -> t (reference count reaches 0)
-- begin-expected
-- columns: u2
-- row: t
-- end-expected
SELECT pg_advisory_unlock(801001)::text AS u2;

-- 5. Third unlock -> f (not held; PG emits a WARNING, not part of the result set)
-- begin-expected
-- columns: u3
-- row: f
-- end-expected
SELECT pg_advisory_unlock(801001)::text AS u3;

-- 6. Unlock of a never-held key -> f
-- begin-expected
-- columns: notheld
-- row: f
-- end-expected
SELECT pg_advisory_unlock(801002)::text AS notheld;

-- ============================================================================
-- Keyspace separation: 1-arg (bigint) vs 2-arg (int, int) forms
-- ============================================================================

-- 7. Hold the 1-arg lock
-- begin-expected
-- columns: one_arg
-- row: t
-- end-expected
SELECT pg_try_advisory_lock(801003)::text AS one_arg;

-- 8. The 2-arg form (0, k) is a different lock: unlocking it returns f
-- begin-expected
-- columns: two_arg_unlock
-- row: f
-- end-expected
SELECT pg_advisory_unlock(0, 801003)::text AS two_arg_unlock;

-- 9. The 2-arg lock can be taken independently
-- begin-expected
-- columns: two_arg_lock
-- row: t
-- end-expected
SELECT pg_try_advisory_lock(0, 801003)::text AS two_arg_lock;

-- 10. Release both forms
-- begin-expected
-- columns: rel1
-- row: t
-- end-expected
SELECT pg_advisory_unlock(801003)::text AS rel1;

-- begin-expected
-- columns: rel2
-- row: t
-- end-expected
SELECT pg_advisory_unlock(0, 801003)::text AS rel2;

-- ============================================================================
-- Mode separation: shared vs exclusive
-- ============================================================================

-- 11. Exclusive hold cannot be released by the shared unlock
-- begin-expected
-- columns: ex
-- row: t
-- end-expected
SELECT pg_try_advisory_lock(801004)::text AS ex;

-- begin-expected
-- columns: wrong_mode
-- row: f
-- end-expected
SELECT pg_advisory_unlock_shared(801004)::text AS wrong_mode;

-- begin-expected
-- columns: right_mode
-- row: t
-- end-expected
SELECT pg_advisory_unlock(801004)::text AS right_mode;

-- 12. Shared hold cannot be released by the exclusive unlock
-- begin-expected
-- columns: sh
-- row: t
-- end-expected
SELECT pg_try_advisory_lock_shared(801005)::text AS sh;

-- begin-expected
-- columns: wrong_mode2
-- row: f
-- end-expected
SELECT pg_advisory_unlock(801005)::text AS wrong_mode2;

-- begin-expected
-- columns: right_mode2
-- row: t
-- end-expected
SELECT pg_advisory_unlock_shared(801005)::text AS right_mode2;

-- 13. Shared locks are reference-counted as well
-- begin-expected
-- columns: s1
-- row: t
-- end-expected
SELECT pg_try_advisory_lock_shared(801006)::text AS s1;

-- begin-expected
-- columns: s2
-- row: t
-- end-expected
SELECT pg_try_advisory_lock_shared(801006)::text AS s2;

-- begin-expected
-- columns: su1
-- row: t
-- end-expected
SELECT pg_advisory_unlock_shared(801006)::text AS su1;

-- begin-expected
-- columns: su2
-- row: t
-- end-expected
SELECT pg_advisory_unlock_shared(801006)::text AS su2;

-- begin-expected
-- columns: su3
-- row: f
-- end-expected
SELECT pg_advisory_unlock_shared(801006)::text AS su3;

-- 14. A session can hold both modes on the same key at once
-- begin-expected
-- columns: both_ex
-- row: t
-- end-expected
SELECT pg_try_advisory_lock(801007)::text AS both_ex;

-- begin-expected
-- columns: both_sh
-- row: t
-- end-expected
SELECT pg_try_advisory_lock_shared(801007)::text AS both_sh;

-- begin-expected
-- columns: both_rel_ex
-- row: t
-- end-expected
SELECT pg_advisory_unlock(801007)::text AS both_rel_ex;

-- begin-expected
-- columns: both_rel_sh
-- row: t
-- end-expected
SELECT pg_advisory_unlock_shared(801007)::text AS both_rel_sh;

-- ============================================================================
-- Transaction-scoped locks (xact variants)
-- ============================================================================

-- 15. xact lock inside a transaction is visible in pg_locks with the key split
--     into classid (high 32 bits) / objid (low 32 bits) and objsubid = 1
BEGIN;

-- begin-expected
-- columns: xact_got
-- row: t
-- end-expected
SELECT pg_try_advisory_xact_lock(801008)::text AS xact_got;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_locks
WHERE locktype = 'advisory' AND classid = 0 AND objid = 801008 AND objsubid = 1;

-- 16. pg_advisory_unlock cannot release a transaction-level hold
-- begin-expected
-- columns: manual_unlock
-- row: f
-- end-expected
SELECT pg_advisory_unlock(801008)::text AS manual_unlock;

COMMIT;

-- 17. After COMMIT the xact lock is gone
-- begin-expected
-- columns: after_commit
-- row: 0
-- end-expected
SELECT count(*)::int AS after_commit FROM pg_locks
WHERE locktype = 'advisory' AND objid = 801008;

-- 18. xact lock is also released by ROLLBACK
BEGIN;

-- begin-expected
-- columns: xact_got2
-- row: t
-- end-expected
SELECT pg_try_advisory_xact_lock(801009)::text AS xact_got2;

ROLLBACK;

-- begin-expected
-- columns: after_rollback
-- row: 0
-- end-expected
SELECT count(*)::int AS after_rollback FROM pg_locks
WHERE locktype = 'advisory' AND objid = 801009;

-- 19. Two-arg xact form appears with objsubid = 2 and both key halves
BEGIN;

-- begin-expected
-- columns: pair_got
-- row: t
-- end-expected
SELECT pg_try_advisory_xact_lock(7, 801010)::text AS pair_got;

-- begin-expected
-- columns: pair_row
-- row: 1
-- end-expected
SELECT count(*)::int AS pair_row FROM pg_locks
WHERE locktype = 'advisory' AND classid = 7 AND objid = 801010 AND objsubid = 2;

COMMIT;

-- ============================================================================
-- pg_advisory_unlock_all
-- ============================================================================

-- 20. unlock_all releases every session-level hold regardless of reference counts
SELECT pg_advisory_lock(801011);
SELECT pg_advisory_lock(801011);
SELECT pg_advisory_lock_shared(801012);
SELECT pg_advisory_unlock_all();

-- begin-expected
-- columns: gone1
-- row: f
-- end-expected
SELECT pg_advisory_unlock(801011)::text AS gone1;

-- begin-expected
-- columns: gone2
-- row: f
-- end-expected
SELECT pg_advisory_unlock_shared(801012)::text AS gone2;

-- begin-expected
-- columns: no_rows_left
-- row: 0
-- end-expected
SELECT count(*)::int AS no_rows_left FROM pg_locks
WHERE locktype = 'advisory' AND objid IN (801011, 801012);
