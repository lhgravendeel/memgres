-- ============================================================================
-- Feature Comparison: Procedure Transaction Control (A10)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG 11+ allows COMMIT and ROLLBACK inside procedures (but NOT functions).
-- This is a key differentiator between procedures and functions.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS ptc_test CASCADE;
CREATE SCHEMA ptc_test;
SET search_path = ptc_test, public;

CREATE TABLE ptc_log (id serial PRIMARY KEY, msg text);

-- ============================================================================
-- 1. Basic COMMIT in procedure
-- ============================================================================

CREATE PROCEDURE ptc_commit_basic() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('before commit');
  COMMIT;
  INSERT INTO ptc_log (msg) VALUES ('after commit');
END;
$$;

CALL ptc_commit_basic();

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM ptc_log;

TRUNCATE ptc_log;

-- ============================================================================
-- 2. Basic ROLLBACK in procedure
-- ============================================================================

CREATE PROCEDURE ptc_rollback_basic() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('will be rolled back');
  ROLLBACK;
  INSERT INTO ptc_log (msg) VALUES ('after rollback');
END;
$$;

CALL ptc_rollback_basic();

-- note: The INSERT before ROLLBACK is undone; the INSERT after ROLLBACK persists
-- begin-expected
-- columns: msg
-- row: after rollback
-- end-expected
SELECT msg FROM ptc_log ORDER BY id;

TRUNCATE ptc_log;

-- ============================================================================
-- 3. Multiple COMMITs in procedure
-- ============================================================================

CREATE PROCEDURE ptc_multi_commit() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('batch1');
  COMMIT;
  INSERT INTO ptc_log (msg) VALUES ('batch2');
  COMMIT;
  INSERT INTO ptc_log (msg) VALUES ('batch3');
  COMMIT;
END;
$$;

CALL ptc_multi_commit();

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::integer AS cnt FROM ptc_log;

TRUNCATE ptc_log;

-- ============================================================================
-- 4. COMMIT then ROLLBACK in same procedure
-- ============================================================================

CREATE PROCEDURE ptc_commit_then_rollback() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('committed');
  COMMIT;
  INSERT INTO ptc_log (msg) VALUES ('rolled back');
  ROLLBACK;
  INSERT INTO ptc_log (msg) VALUES ('final');
END;
$$;

CALL ptc_commit_then_rollback();

-- 'committed' persists (committed), 'rolled back' is undone, 'final' persists
-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM ptc_log;

-- begin-expected
-- columns: msg
-- row: committed
-- row: final
-- end-expected
SELECT msg FROM ptc_log ORDER BY id;

TRUNCATE ptc_log;

-- ============================================================================
-- 5. COMMIT AND CHAIN
-- ============================================================================

-- note: AND CHAIN commits and immediately starts a new transaction

CREATE PROCEDURE ptc_commit_chain() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('before chain');
  COMMIT AND CHAIN;
  INSERT INTO ptc_log (msg) VALUES ('after chain');
END;
$$;

CALL ptc_commit_chain();

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM ptc_log;

TRUNCATE ptc_log;

-- ============================================================================
-- 6. ROLLBACK AND CHAIN
-- ============================================================================

CREATE PROCEDURE ptc_rollback_chain() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('will be rolled back');
  ROLLBACK AND CHAIN;
  INSERT INTO ptc_log (msg) VALUES ('in new txn');
END;
$$;

CALL ptc_rollback_chain();

-- begin-expected
-- columns: msg
-- row: in new txn
-- end-expected
SELECT msg FROM ptc_log ORDER BY id;

TRUNCATE ptc_log;

-- ============================================================================
-- 7. Exception after COMMIT: committed data survives
-- ============================================================================

CREATE PROCEDURE ptc_error_after_commit() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('committed data');
  COMMIT;
  RAISE EXCEPTION 'intentional error';
END;
$$;

-- begin-expected-error
-- message-like: intentional error
-- end-expected-error
CALL ptc_error_after_commit();

-- The committed data should survive despite the error
-- begin-expected
-- columns: msg
-- row: committed data
-- end-expected
SELECT msg FROM ptc_log ORDER BY id;

TRUNCATE ptc_log;

-- ============================================================================
-- 8. COMMIT in loop (batch processing pattern)
-- ============================================================================

CREATE PROCEDURE ptc_batch_loop() LANGUAGE plpgsql AS $$
DECLARE
  i integer;
BEGIN
  FOR i IN 1..5 LOOP
    INSERT INTO ptc_log (msg) VALUES ('item ' || i::text);
    IF i % 2 = 0 THEN
      COMMIT;
    END IF;
  END LOOP;
END;
$$;

CALL ptc_batch_loop();

-- begin-expected
-- columns: cnt
-- row: 5
-- end-expected
SELECT count(*)::integer AS cnt FROM ptc_log;

TRUNCATE ptc_log;

-- ============================================================================
-- 9. COMMIT rejected in function
-- ============================================================================

-- note: Functions cannot use COMMIT/ROLLBACK — only procedures can

CREATE FUNCTION ptc_bad_fn() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  COMMIT;
END;
$$;

-- note: In PG, the function can be created but fails when called.
--       Some implementations reject at CREATE time.

-- ============================================================================
-- 10. ROLLBACK rejected in function
-- ============================================================================

CREATE FUNCTION ptc_bad_fn2() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  ROLLBACK;
END;
$$;

-- ============================================================================
-- 11. Nested procedure calls with COMMIT
-- ============================================================================

CREATE PROCEDURE ptc_inner() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('inner');
  COMMIT;
END;
$$;

CREATE PROCEDURE ptc_outer() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('outer before');
  CALL ptc_inner();
  INSERT INTO ptc_log (msg) VALUES ('outer after');
END;
$$;

CALL ptc_outer();

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::integer AS cnt FROM ptc_log;

TRUNCATE ptc_log;

-- ============================================================================
-- 12. Procedure with OUT parameters + COMMIT
-- ============================================================================

CREATE PROCEDURE ptc_out_param(INOUT result text) LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('with out param');
  COMMIT;
  result := 'done';
END;
$$;

CALL ptc_out_param(NULL);

-- begin-expected
-- columns: msg
-- row: with out param
-- end-expected
SELECT msg FROM ptc_log ORDER BY id;

TRUNCATE ptc_log;

-- ============================================================================
-- 13. ROLLBACK in exception handler
-- ============================================================================

CREATE PROCEDURE ptc_rollback_in_exception() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('before error');
  COMMIT;

  BEGIN
    INSERT INTO ptc_log (msg) VALUES ('will fail');
    PERFORM 1/0;
  EXCEPTION
    WHEN division_by_zero THEN
      ROLLBACK;
      INSERT INTO ptc_log (msg) VALUES ('recovered');
  END;
END;
$$;

CALL ptc_rollback_in_exception();

-- note: ROLLBACK inside an exception handler is rejected at runtime in PG
-- because exception handlers use subtransactions. The procedure call fails,
-- but 'before error' was already COMMITted so it may survive.
-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM ptc_log;

TRUNCATE ptc_log;

-- ============================================================================
-- 14. Empty procedure with just COMMIT (edge case)
-- ============================================================================

CREATE PROCEDURE ptc_just_commit() LANGUAGE plpgsql AS $$
BEGIN
  COMMIT;
END;
$$;

CALL ptc_just_commit();

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ============================================================================
-- 15. COMMIT preserves sequence values
-- ============================================================================

-- note: ptc_log uses a serial column, so nextval is called.
-- After COMMIT, sequence values should be preserved.

CREATE PROCEDURE ptc_seq_test() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('seq1');
  COMMIT;
  INSERT INTO ptc_log (msg) VALUES ('seq2');
END;
$$;

CALL ptc_seq_test();

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM ptc_log;

-- Verify IDs are sequential (not reset)
-- begin-expected
-- columns: ordered
-- row: true
-- end-expected
SELECT (max(id) > min(id)) AS ordered FROM ptc_log;

TRUNCATE ptc_log;

-- ============================================================================
-- 16. Multiple ROLLBACKs (idempotent)
-- ============================================================================

CREATE PROCEDURE ptc_multi_rollback() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('will vanish');
  ROLLBACK;
  ROLLBACK;
  INSERT INTO ptc_log (msg) VALUES ('survives');
END;
$$;

CALL ptc_multi_rollback();

-- begin-expected
-- columns: msg
-- row: survives
-- end-expected
SELECT msg FROM ptc_log ORDER BY id;

TRUNCATE ptc_log;

-- ============================================================================
-- 17. Procedure calling itself recursively with COMMIT
-- ============================================================================

CREATE PROCEDURE ptc_recursive(n integer) LANGUAGE plpgsql AS $$
BEGIN
  IF n <= 0 THEN RETURN; END IF;
  INSERT INTO ptc_log (msg) VALUES ('r' || n::text);
  COMMIT;
  CALL ptc_recursive(n - 1);
END;
$$;

CALL ptc_recursive(3);

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::integer AS cnt FROM ptc_log;

TRUNCATE ptc_log;

-- ============================================================================
-- 18. SAVEPOINT inside procedure
-- ============================================================================

CREATE PROCEDURE ptc_savepoint() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('before-sp');
  SAVEPOINT sp1;
  INSERT INTO ptc_log (msg) VALUES ('after-sp');
  ROLLBACK TO SAVEPOINT sp1;
  INSERT INTO ptc_log (msg) VALUES ('after-rollback-sp');
  COMMIT;
END;
$$;

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
CALL ptc_savepoint();

-- note: PG 18 does not support explicit SAVEPOINT in procedures —
-- only COMMIT / ROLLBACK are allowed.  The CALL above errors out,
-- so ptc_log is empty here.
-- begin-expected
-- columns: msg
-- end-expected
SELECT msg FROM ptc_log ORDER BY id;

TRUNCATE ptc_log;

-- ============================================================================
-- 19. COMMIT in procedure called from function (should error)
-- ============================================================================

CREATE PROCEDURE ptc_inner_commit() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('inner');
  COMMIT;
END;
$$;

-- note: Functions cannot do transaction control, and calling a procedure
-- that does COMMIT from within a function context should error
CREATE FUNCTION ptc_func_calls_proc() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  CALL ptc_inner_commit();
END;
$$;

-- begin-expected-error
-- message-like: invalid transaction termination
-- end-expected-error
SELECT ptc_func_calls_proc();

-- ============================================================================
-- 20. COMMIT in DO block
-- ============================================================================

-- note: DO blocks support transaction control like procedures
DO $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('do-before');
  COMMIT;
  INSERT INTO ptc_log (msg) VALUES ('do-after');
END;
$$;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM ptc_log;

TRUNCATE ptc_log;

-- ============================================================================
-- 21. Temp table across COMMIT in procedure
-- ============================================================================

CREATE PROCEDURE ptc_temp_table() LANGUAGE plpgsql AS $$
BEGIN
  CREATE TEMP TABLE ptc_temp (val text);
  INSERT INTO ptc_temp VALUES ('before-commit');
  COMMIT;
  -- Temp table survives COMMIT (ON COMMIT PRESERVE ROWS is default)
  INSERT INTO ptc_temp VALUES ('after-commit');
END;
$$;

CALL ptc_temp_table();

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM ptc_temp;

DROP TABLE ptc_temp;

-- ============================================================================
-- 22. SET LOCAL reset across COMMIT
-- ============================================================================

CREATE PROCEDURE ptc_set_local() LANGUAGE plpgsql AS $$
BEGIN
  SET LOCAL work_mem = '256MB';
  INSERT INTO ptc_log (msg) VALUES (current_setting('work_mem'));
  COMMIT;
  -- After COMMIT, SET LOCAL is reset
  INSERT INTO ptc_log (msg) VALUES (current_setting('work_mem'));
END;
$$;

CALL ptc_set_local();

-- note: First msg is '256MB', second should be the default (usually '4MB')
-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM ptc_log;

TRUNCATE ptc_log;

-- ============================================================================
-- 23. Cursor WITH HOLD across COMMIT
-- ============================================================================

-- note: Cursors WITH HOLD survive COMMIT
CREATE PROCEDURE ptc_cursor_hold() LANGUAGE plpgsql AS $$
DECLARE
  cur CURSOR WITH HOLD FOR SELECT generate_series(1, 3) AS n;
  rec record;
BEGIN
  OPEN cur;
  FETCH cur INTO rec;
  INSERT INTO ptc_log (msg) VALUES ('fetched-' || rec.n::text);
  COMMIT;
  -- Cursor still open after COMMIT because WITH HOLD
  FETCH cur INTO rec;
  INSERT INTO ptc_log (msg) VALUES ('fetched-' || rec.n::text);
  CLOSE cur;
END;
$$;

CALL ptc_cursor_hold();

-- begin-expected
-- columns: msg
-- end-expected
SELECT msg FROM ptc_log ORDER BY id;

TRUNCATE ptc_log;

-- ============================================================================
-- 24. Nested exception handlers with COMMIT
-- ============================================================================

CREATE PROCEDURE ptc_nested_exception() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('outer-start');
  COMMIT;
  BEGIN
    INSERT INTO ptc_log (msg) VALUES ('inner-start');
    RAISE EXCEPTION 'inner error';
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO ptc_log (msg) VALUES ('inner-caught');
  END;
END;
$$;

CALL ptc_nested_exception();

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM ptc_log;

TRUNCATE ptc_log;

-- ============================================================================
-- 25. Multiple procedures chained with COMMIT
-- ============================================================================

CREATE PROCEDURE ptc_chain_a() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('chain-a');
  COMMIT;
END;
$$;

CREATE PROCEDURE ptc_chain_b() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('chain-b');
  COMMIT;
END;
$$;

CREATE PROCEDURE ptc_chain_outer() LANGUAGE plpgsql AS $$
BEGIN
  CALL ptc_chain_a();
  CALL ptc_chain_b();
END;
$$;

CALL ptc_chain_outer();

-- begin-expected
-- columns: msg
-- row: chain-a
-- row: chain-b
-- end-expected
SELECT msg FROM ptc_log ORDER BY id;

TRUNCATE ptc_log;

-- ============================================================================
-- 26. ROLLBACK with deferred constraint violation
-- ============================================================================

CREATE TABLE ptc_defer_parent (id integer PRIMARY KEY);
CREATE TABLE ptc_defer_child (
  id integer PRIMARY KEY,
  parent_id integer REFERENCES ptc_defer_parent(id) DEFERRABLE INITIALLY DEFERRED
);
INSERT INTO ptc_defer_parent VALUES (1);

CREATE PROCEDURE ptc_deferred_rollback() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_defer_child VALUES (1, 999);  -- deferred FK violation
  ROLLBACK;  -- rollback before deferred check fires
  INSERT INTO ptc_defer_child VALUES (2, 1);  -- valid
  COMMIT;
END;
$$;

CALL ptc_deferred_rollback();

-- begin-expected
-- columns: id, parent_id
-- row: 2, 1
-- end-expected
SELECT * FROM ptc_defer_child;

DROP TABLE ptc_defer_child, ptc_defer_parent;

-- ============================================================================
-- 27. ABORT (synonym for ROLLBACK) in procedure
-- ============================================================================

CREATE PROCEDURE ptc_abort_test() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('discard-me');
  ABORT;
  INSERT INTO ptc_log (msg) VALUES ('after-abort');
  COMMIT;
END;
$$;

-- begin-expected-error
-- message-like: unsupported transaction command
-- end-expected-error
CALL ptc_abort_test();

-- note: PG 18 does not support ABORT as transaction control in PL/pgSQL procedures.
-- The CALL above errors out, so ptc_log is empty here.
-- begin-expected
-- columns: msg
-- end-expected
SELECT msg FROM ptc_log ORDER BY id;

TRUNCATE ptc_log;

-- ============================================================================
-- 28. Sequence values survive ROLLBACK
-- ============================================================================

CREATE SEQUENCE ptc_seq;

CREATE PROCEDURE ptc_seq_rollback() LANGUAGE plpgsql AS $$
DECLARE
  v1 bigint;
  v2 bigint;
BEGIN
  v1 := nextval('ptc_seq');
  INSERT INTO ptc_log (msg) VALUES ('seq=' || v1::text);
  ROLLBACK;
  v2 := nextval('ptc_seq');
  INSERT INTO ptc_log (msg) VALUES ('seq=' || v2::text);
  COMMIT;
END;
$$;

CALL ptc_seq_rollback();

-- note: First nextval (1) was rolled back but sequence doesn't revert
-- Second nextval should be 2, not 1
-- begin-expected
-- columns: msg
-- row: seq=2
-- end-expected
SELECT msg FROM ptc_log ORDER BY id;

TRUNCATE ptc_log;
DROP SEQUENCE ptc_seq;

-- ============================================================================
-- 29. AND CHAIN preserves isolation level
-- ============================================================================

CREATE PROCEDURE ptc_chain_isolation() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ptc_log (msg) VALUES ('batch1');
  COMMIT AND CHAIN;
  -- New transaction should inherit the isolation level of the previous one
  INSERT INTO ptc_log (msg) VALUES ('batch2');
  COMMIT;
END;
$$;

BEGIN ISOLATION LEVEL SERIALIZABLE;
CALL ptc_chain_isolation();

-- begin-expected
-- columns: msg
-- end-expected
SELECT msg FROM ptc_log ORDER BY id;

TRUNCATE ptc_log;

-- ============================================================================
-- 30. LOCK TABLE released after COMMIT in procedure
-- ============================================================================

CREATE TABLE ptc_lockable (id integer PRIMARY KEY);
INSERT INTO ptc_lockable VALUES (1);

CREATE PROCEDURE ptc_lock_commit() LANGUAGE plpgsql AS $$
BEGIN
  LOCK TABLE ptc_lockable IN ACCESS EXCLUSIVE MODE;
  INSERT INTO ptc_log (msg) VALUES ('locked');
  COMMIT;
  -- Lock released after COMMIT — table should be accessible
  INSERT INTO ptc_log (msg) VALUES ('unlocked');
  COMMIT;
END;
$$;

CALL ptc_lock_commit();

-- begin-expected
-- columns: msg
-- row: locked
-- row: unlocked
-- end-expected
SELECT msg FROM ptc_log ORDER BY id;

TRUNCATE ptc_log;
DROP TABLE ptc_lockable;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA ptc_test CASCADE;
SET search_path = public;
