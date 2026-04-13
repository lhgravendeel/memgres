-- ============================================================================
-- Feature Comparison: SECURITY DEFINER / SECURITY INVOKER
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS secdef_test CASCADE;
CREATE SCHEMA secdef_test;
SET search_path = secdef_test, public;

-- ============================================================================
-- 1. SECURITY INVOKER (default): current_user is the caller
-- ============================================================================

CREATE FUNCTION secdef_invoker_user() RETURNS text
LANGUAGE sql SECURITY INVOKER AS $$ SELECT current_user::text $$;

-- note: current_user should be the session user (the caller)
-- begin-expected
-- columns: match
-- row: true
-- end-expected
SELECT secdef_invoker_user() = current_user AS match;

-- ============================================================================
-- 2. SECURITY DEFINER: current_user switches to function owner
-- ============================================================================

CREATE FUNCTION secdef_definer_user() RETURNS text
LANGUAGE sql SECURITY DEFINER AS $$ SELECT current_user::text $$;

-- note: With SECURITY DEFINER, current_user is the function owner.
--       In a single-user setup, this is the same as the session user.
-- begin-expected
-- columns: match
-- row: true
-- end-expected
SELECT secdef_definer_user() = current_user AS match;

-- ============================================================================
-- 3. session_user vs current_user in SECURITY DEFINER
-- ============================================================================

CREATE FUNCTION secdef_both_users() RETURNS TABLE(sess text, curr text)
LANGUAGE sql SECURITY DEFINER AS $$
  SELECT session_user::text, current_user::text
$$;

-- note: session_user never changes; current_user changes to function owner
-- begin-expected
-- columns: session_matches
-- row: true
-- end-expected
SELECT (SELECT sess = curr FROM secdef_both_users()) AS session_matches;

-- ============================================================================
-- 4. SECURITY DEFINER with PL/pgSQL
-- ============================================================================

CREATE FUNCTION secdef_plpgsql() RETURNS text
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  u text;
BEGIN
  u := current_user;
  RETURN 'user:' || u;
END;
$$;

-- begin-expected
-- columns: starts_with_user
-- row: true
-- end-expected
SELECT secdef_plpgsql() LIKE 'user:%' AS starts_with_user;

-- ============================================================================
-- 5. SECURITY DEFINER reverts on error
-- ============================================================================

CREATE FUNCTION secdef_error() RETURNS text
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  RAISE EXCEPTION 'intentional error';
END;
$$;

-- begin-expected-error
-- message-like: intentional error
-- end-expected-error
SELECT secdef_error();

-- After error, current_user should be back to normal
-- begin-expected
-- columns: match
-- row: true
-- end-expected
SELECT current_user = session_user AS match;

-- ============================================================================
-- 6. SECURITY DEFINER reverts on exception in nested call
-- ============================================================================

CREATE FUNCTION secdef_inner_error() RETURNS text
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  PERFORM 1/0;
  RETURN 'ok';
END;
$$;

CREATE FUNCTION secdef_outer_catches() RETURNS text
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  BEGIN
    PERFORM secdef_inner_error();
  EXCEPTION WHEN OTHERS THEN
    RETURN 'caught:' || current_user;
  END;
END;
$$;

-- begin-expected
-- columns: starts_with_caught
-- row: true
-- end-expected
SELECT secdef_outer_catches() LIKE 'caught:%' AS starts_with_caught;

-- ============================================================================
-- 7. Nested SECURITY DEFINER calls
-- ============================================================================

CREATE FUNCTION secdef_level1() RETURNS text
LANGUAGE sql SECURITY DEFINER AS $$ SELECT 'L1:' || current_user::text $$;

CREATE FUNCTION secdef_level2() RETURNS text
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  RETURN 'L2:' || current_user || '/' || secdef_level1();
END;
$$;

-- note: Both functions owned by same user, so current_user is same at both levels
-- begin-expected
-- columns: starts_with_L2
-- row: true
-- end-expected
SELECT secdef_level2() LIKE 'L2:%' AS starts_with_L2;

-- ============================================================================
-- 8. SECURITY DEFINER + STRICT
-- ============================================================================

CREATE FUNCTION secdef_strict(x integer) RETURNS text
LANGUAGE sql STRICT SECURITY DEFINER AS $$
  SELECT 'user:' || current_user || ',val:' || x::text
$$;

-- Non-null input works
-- begin-expected
-- columns: has_val
-- row: true
-- end-expected
SELECT secdef_strict(42) LIKE '%val:42' AS has_val;

-- NULL input returns NULL (STRICT takes effect)
-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT secdef_strict(NULL) IS NULL AS is_null;

-- ============================================================================
-- 9. SECURITY DEFINER + SETOF
-- ============================================================================

CREATE FUNCTION secdef_setof() RETURNS SETOF text
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  RETURN NEXT 'row1:' || current_user;
  RETURN NEXT 'row2:' || current_user;
  RETURN;
END;
$$;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*) AS cnt FROM secdef_setof();

-- ============================================================================
-- 10. SECURITY DEFINER + SET clause
-- ============================================================================

-- note: Best practice is to SET search_path in SECURITY DEFINER functions
CREATE FUNCTION secdef_with_set() RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  sp text;
BEGIN
  SHOW search_path INTO sp;
  RETURN sp;
END;
$$;

-- begin-expected
-- columns: secdef_with_set
-- row: pg_catalog
-- end-expected
SELECT secdef_with_set();

-- Verify search_path is restored
-- begin-expected
-- columns: search_path
-- row: secdef_test, public
-- end-expected
SHOW search_path;

-- ============================================================================
-- 11. pg_proc catalog: prosecdef column
-- ============================================================================

-- begin-expected
-- columns: definer_fn, invoker_fn
-- row: true, false
-- end-expected
SELECT
  (SELECT prosecdef FROM pg_proc WHERE proname = 'secdef_definer_user' LIMIT 1) AS definer_fn,
  (SELECT prosecdef FROM pg_proc WHERE proname = 'secdef_invoker_user' LIMIT 1) AS invoker_fn;

-- ============================================================================
-- 12. ALTER FUNCTION: SECURITY INVOKER <-> SECURITY DEFINER
-- ============================================================================

CREATE FUNCTION secdef_toggle() RETURNS boolean
LANGUAGE sql SECURITY INVOKER AS $$ SELECT true $$;

-- Verify initially INVOKER
-- begin-expected
-- columns: is_definer
-- row: false
-- end-expected
SELECT prosecdef AS is_definer FROM pg_proc WHERE proname = 'secdef_toggle';

ALTER FUNCTION secdef_toggle() SECURITY DEFINER;

-- Now DEFINER
-- begin-expected
-- columns: is_definer
-- row: true
-- end-expected
SELECT prosecdef AS is_definer FROM pg_proc WHERE proname = 'secdef_toggle';

ALTER FUNCTION secdef_toggle() SECURITY INVOKER;

-- Back to INVOKER
-- begin-expected
-- columns: is_definer
-- row: false
-- end-expected
SELECT prosecdef AS is_definer FROM pg_proc WHERE proname = 'secdef_toggle';

-- ============================================================================
-- 13. SECURITY DEFINER function accessing table
-- ============================================================================

CREATE TABLE secdef_private (id integer PRIMARY KEY, secret text);
INSERT INTO secdef_private VALUES (1, 'top-secret');

CREATE FUNCTION secdef_read_secret(rid integer) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  s text;
BEGIN
  SELECT secret INTO s FROM secdef_private WHERE id = rid;
  RETURN s;
END;
$$;

-- begin-expected
-- columns: secdef_read_secret
-- row: top-secret
-- end-expected
SELECT secdef_read_secret(1);

-- ============================================================================
-- 14. SECURITY DEFINER with DML inside
-- ============================================================================

CREATE TABLE secdef_audit (ts timestamptz DEFAULT now(), msg text);

CREATE FUNCTION secdef_log(message text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  INSERT INTO secdef_audit (msg) VALUES (message);
END;
$$;

SELECT secdef_log('test message');

-- begin-expected
-- columns: msg
-- row: test message
-- end-expected
SELECT msg FROM secdef_audit;

-- ============================================================================
-- 15. SECURITY DEFINER + RETURNS TABLE
-- ============================================================================

CREATE FUNCTION secdef_list_secrets()
RETURNS TABLE(id integer, secret text)
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  RETURN QUERY SELECT p.id, p.secret FROM secdef_private p ORDER BY p.id;
END;
$$;

-- begin-expected
-- columns: id, secret
-- row: 1, top-secret
-- end-expected
SELECT * FROM secdef_list_secrets();

-- ============================================================================
-- 16. SECURITY DEFINER + exception handling
-- ============================================================================

CREATE FUNCTION secdef_safe_divide(a integer, b integer) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  RETURN (a / b)::text;
EXCEPTION
  WHEN division_by_zero THEN
    RETURN 'div/0 caught by ' || current_user;
END;
$$;

-- begin-expected
-- columns: result
-- row: 5
-- end-expected
SELECT secdef_safe_divide(10, 2) AS result;

-- begin-expected
-- columns: has_caught
-- row: true
-- end-expected
SELECT secdef_safe_divide(10, 0) LIKE 'div/0 caught by %' AS has_caught;

-- ============================================================================
-- 17. Multiple SECURITY DEFINER attributes combined
-- ============================================================================

CREATE FUNCTION secdef_combo(x integer)
RETURNS integer
LANGUAGE plpgsql
STRICT
SECURITY DEFINER
IMMUTABLE
SET search_path = pg_catalog
AS $$
BEGIN
  RETURN x * 2;
END;
$$;

-- begin-expected
-- columns: result
-- row: 84
-- end-expected
SELECT secdef_combo(42) AS result;

-- NULL returns NULL (STRICT)
-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT secdef_combo(NULL) IS NULL AS is_null;

-- Verify all attributes in pg_proc
-- begin-expected
-- columns: is_strict, is_definer, volatility
-- row: true, true, i
-- end-expected
SELECT proisstrict AS is_strict, prosecdef AS is_definer, provolatile AS volatility
FROM pg_proc WHERE proname = 'secdef_combo';

-- ============================================================================
-- 18. SECURITY DEFINER: current_user in different expression contexts
-- ============================================================================

CREATE FUNCTION secdef_user_contexts() RETURNS TABLE(ctx text, val text)
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  ctx := 'current_user'; val := current_user; RETURN NEXT;
  ctx := 'session_user'; val := session_user; RETURN NEXT;
  RETURN;
END;
$$;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*) AS cnt FROM secdef_user_contexts();

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA secdef_test CASCADE;
SET search_path = public;
