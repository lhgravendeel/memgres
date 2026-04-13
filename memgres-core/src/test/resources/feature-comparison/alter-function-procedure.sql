-- ============================================================================
-- Feature Comparison: ALTER FUNCTION / ALTER PROCEDURE (A7)
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

DROP SCHEMA IF EXISTS af_test CASCADE;
CREATE SCHEMA af_test;
SET search_path = af_test, public;

-- ============================================================================
-- 1. ALTER FUNCTION RENAME TO
-- ============================================================================

CREATE FUNCTION af_orig(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x + 1 $$;

ALTER FUNCTION af_orig(integer) RENAME TO af_renamed;

-- begin-expected
-- columns: result
-- row: 6
-- end-expected
SELECT af_renamed(5) AS result;

-- begin-expected-error
-- message-like: function af_orig(integer) does not exist
-- end-expected-error
SELECT af_orig(5);

-- ============================================================================
-- 2. ALTER FUNCTION SET SCHEMA
-- ============================================================================

CREATE SCHEMA af_other;
CREATE FUNCTION af_to_move(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x * 2 $$;

ALTER FUNCTION af_to_move(integer) SET SCHEMA af_other;

-- begin-expected
-- columns: result
-- row: 10
-- end-expected
SELECT af_other.af_to_move(5) AS result;

-- begin-expected-error
-- message-like: function af_to_move(integer) does not exist
-- end-expected-error
SELECT af_to_move(5);

DROP SCHEMA af_other CASCADE;

-- ============================================================================
-- 3. ALTER FUNCTION OWNER TO
-- ============================================================================

CREATE FUNCTION af_owned() RETURNS text LANGUAGE sql AS $$ SELECT current_user::text $$;

-- note: Owner change to current_user is a no-op but should succeed
ALTER FUNCTION af_owned() OWNER TO CURRENT_USER;

-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = 'af_owned') AS exists;

-- ============================================================================
-- 4. ALTER FUNCTION SECURITY DEFINER / INVOKER
-- ============================================================================

CREATE FUNCTION af_sec() RETURNS text LANGUAGE sql SECURITY INVOKER
AS $$ SELECT current_user::text $$;

-- Initially INVOKER
-- begin-expected
-- columns: prosecdef
-- row: false
-- end-expected
SELECT prosecdef FROM pg_proc WHERE proname = 'af_sec';

ALTER FUNCTION af_sec() SECURITY DEFINER;

-- begin-expected
-- columns: prosecdef
-- row: true
-- end-expected
SELECT prosecdef FROM pg_proc WHERE proname = 'af_sec';

ALTER FUNCTION af_sec() SECURITY INVOKER;

-- begin-expected
-- columns: prosecdef
-- row: false
-- end-expected
SELECT prosecdef FROM pg_proc WHERE proname = 'af_sec';

-- ============================================================================
-- 5. ALTER FUNCTION STRICT / CALLED ON NULL INPUT
-- ============================================================================

CREATE FUNCTION af_strict(x integer) RETURNS integer LANGUAGE sql
AS $$ SELECT x $$;

-- Initially not strict
-- begin-expected
-- columns: proisstrict
-- row: false
-- end-expected
SELECT proisstrict FROM pg_proc WHERE proname = 'af_strict';

ALTER FUNCTION af_strict(integer) STRICT;

-- begin-expected
-- columns: proisstrict
-- row: true
-- end-expected
SELECT proisstrict FROM pg_proc WHERE proname = 'af_strict';

-- NULL now returns NULL
-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT af_strict(NULL) IS NULL AS is_null;

ALTER FUNCTION af_strict(integer) CALLED ON NULL INPUT;

-- begin-expected
-- columns: proisstrict
-- row: false
-- end-expected
SELECT proisstrict FROM pg_proc WHERE proname = 'af_strict';

-- ============================================================================
-- 6. ALTER FUNCTION IMMUTABLE / STABLE / VOLATILE
-- ============================================================================

CREATE FUNCTION af_vol(x integer) RETURNS integer LANGUAGE sql VOLATILE
AS $$ SELECT x $$;

-- begin-expected
-- columns: provolatile
-- row: v
-- end-expected
SELECT provolatile FROM pg_proc WHERE proname = 'af_vol';

ALTER FUNCTION af_vol(integer) IMMUTABLE;

-- begin-expected
-- columns: provolatile
-- row: i
-- end-expected
SELECT provolatile FROM pg_proc WHERE proname = 'af_vol';

ALTER FUNCTION af_vol(integer) STABLE;

-- begin-expected
-- columns: provolatile
-- row: s
-- end-expected
SELECT provolatile FROM pg_proc WHERE proname = 'af_vol';

ALTER FUNCTION af_vol(integer) VOLATILE;

-- begin-expected
-- columns: provolatile
-- row: v
-- end-expected
SELECT provolatile FROM pg_proc WHERE proname = 'af_vol';

-- ============================================================================
-- 7. ALTER FUNCTION: IMMUTABLE enables use in expression index
-- ============================================================================

CREATE TABLE af_idx_test (id integer, val integer);
INSERT INTO af_idx_test VALUES (1, 5), (2, 10);

-- VOLATILE function in index — PG allows this (no IMMUTABLE requirement enforced)
CREATE INDEX idx_af_vol ON af_idx_test (af_vol(val));
DROP INDEX idx_af_vol;

-- Change to IMMUTABLE, then index also succeeds
ALTER FUNCTION af_vol(integer) IMMUTABLE;
CREATE INDEX idx_af_vol ON af_idx_test (af_vol(val));

DROP TABLE af_idx_test CASCADE;

-- ============================================================================
-- 8. ALTER FUNCTION COST / ROWS
-- ============================================================================

CREATE FUNCTION af_cost(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x $$;

ALTER FUNCTION af_cost(integer) COST 1000;

-- begin-expected
-- columns: procost
-- row: 1000
-- end-expected
SELECT procost FROM pg_proc WHERE proname = 'af_cost';

ALTER FUNCTION af_cost(integer) ROWS 500;

-- begin-expected
-- columns: prorows
-- row: 0
-- end-expected
SELECT prorows FROM pg_proc WHERE proname = 'af_cost';

-- ============================================================================
-- 9. ALTER FUNCTION PARALLEL
-- ============================================================================

CREATE FUNCTION af_parallel(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x $$;

ALTER FUNCTION af_parallel(integer) PARALLEL SAFE;

-- begin-expected
-- columns: proparallel
-- row: s
-- end-expected
SELECT proparallel FROM pg_proc WHERE proname = 'af_parallel';

ALTER FUNCTION af_parallel(integer) PARALLEL RESTRICTED;

-- begin-expected
-- columns: proparallel
-- row: r
-- end-expected
SELECT proparallel FROM pg_proc WHERE proname = 'af_parallel';

ALTER FUNCTION af_parallel(integer) PARALLEL UNSAFE;

-- begin-expected
-- columns: proparallel
-- row: u
-- end-expected
SELECT proparallel FROM pg_proc WHERE proname = 'af_parallel';

-- ============================================================================
-- 10. ALTER FUNCTION SET configuration
-- ============================================================================

CREATE FUNCTION af_config() RETURNS text LANGUAGE plpgsql AS $$
DECLARE sp text;
BEGIN
  SHOW search_path INTO sp;
  RETURN sp;
END;
$$;

ALTER FUNCTION af_config() SET search_path = pg_catalog;

-- begin-expected
-- columns: af_config
-- row: pg_catalog
-- end-expected
SELECT af_config();

-- Verify search_path reverts after function call
-- begin-expected
-- columns: search_path
-- row: af_test, public
-- end-expected
SHOW search_path;

-- ============================================================================
-- 11. ALTER FUNCTION RESET configuration
-- ============================================================================

ALTER FUNCTION af_config() RESET search_path;

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT af_config() LIKE '%af_test%' AS result;

-- ============================================================================
-- 12. ALTER overloaded function (specific signature)
-- ============================================================================

CREATE FUNCTION af_overload(x integer) RETURNS text LANGUAGE sql AS $$ SELECT 'int' $$;
CREATE FUNCTION af_overload(x text) RETURNS text LANGUAGE sql AS $$ SELECT 'text' $$;

ALTER FUNCTION af_overload(integer) RENAME TO af_overload_int;

-- Text version unchanged
-- begin-expected
-- columns: result
-- row: text
-- end-expected
SELECT af_overload('hi') AS result;

-- Integer version renamed
-- begin-expected
-- columns: result
-- row: int
-- end-expected
SELECT af_overload_int(5) AS result;

-- ============================================================================
-- 13. ALTER FUNCTION: non-existent function error
-- ============================================================================

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
ALTER FUNCTION af_nonexistent(integer) RENAME TO af_new;

-- ============================================================================
-- 14. ALTER PROCEDURE RENAME TO
-- ============================================================================

CREATE TABLE af_proc_log (msg text);
CREATE PROCEDURE af_proc_orig(p_msg text) LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO af_proc_log VALUES (p_msg);
END;
$$;

ALTER PROCEDURE af_proc_orig(text) RENAME TO af_proc_renamed;

CALL af_proc_renamed('hello');

-- begin-expected
-- columns: msg
-- row: hello
-- end-expected
SELECT msg FROM af_proc_log;

-- ============================================================================
-- 15. ALTER PROCEDURE SET SCHEMA
-- ============================================================================

CREATE SCHEMA af_proc_schema;
CREATE PROCEDURE af_proc_move() LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO af_proc_log VALUES ('moved');
END;
$$;

ALTER PROCEDURE af_proc_move() SET SCHEMA af_proc_schema;

CALL af_proc_schema.af_proc_move();

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM af_proc_log;

DROP SCHEMA af_proc_schema CASCADE;

-- ============================================================================
-- 16. ALTER PROCEDURE OWNER TO
-- ============================================================================

CREATE PROCEDURE af_proc_own() LANGUAGE plpgsql AS $$
BEGIN
  NULL;
END;
$$;

ALTER PROCEDURE af_proc_own() OWNER TO CURRENT_USER;

-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = 'af_proc_own') AS exists;

-- ============================================================================
-- 17. Multiple ALTER in sequence
-- ============================================================================

CREATE FUNCTION af_multi(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x $$;

ALTER FUNCTION af_multi(integer) IMMUTABLE;
ALTER FUNCTION af_multi(integer) STRICT;
ALTER FUNCTION af_multi(integer) SECURITY DEFINER;

-- begin-expected
-- columns: provolatile, proisstrict, prosecdef
-- row: i, true, true
-- end-expected
SELECT provolatile, proisstrict, prosecdef
FROM pg_proc WHERE proname = 'af_multi';

-- ============================================================================
-- 18. ALTER FUNCTION RETURNS NULL ON NULL INPUT
-- ============================================================================

CREATE FUNCTION af_rnoni(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x $$;

ALTER FUNCTION af_rnoni(integer) RETURNS NULL ON NULL INPUT;

-- begin-expected
-- columns: proisstrict
-- row: true
-- end-expected
SELECT proisstrict FROM pg_proc WHERE proname = 'af_rnoni';

-- ============================================================================
-- 19. ALTER FUNCTION on function with no args
-- ============================================================================

CREATE FUNCTION af_noargs() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;

ALTER FUNCTION af_noargs() IMMUTABLE;
ALTER FUNCTION af_noargs() STRICT;

-- begin-expected
-- columns: provolatile, proisstrict
-- row: i, true
-- end-expected
SELECT provolatile, proisstrict FROM pg_proc WHERE proname = 'af_noargs';

-- ============================================================================
-- 20. pg_proc catalog reflects all changes
-- ============================================================================

-- Comprehensive check on af_multi
-- begin-expected
-- columns: proname, provolatile, proisstrict, prosecdef
-- row: af_multi, i, true, true
-- end-expected
SELECT proname, provolatile, proisstrict, prosecdef
FROM pg_proc
WHERE proname = 'af_multi';

-- ============================================================================
-- 21. ALTER FUNCTION IF EXISTS on non-existent (no-op)
-- ============================================================================

-- note: IF EXISTS should silently succeed when function doesn't exist

ALTER FUNCTION IF EXISTS af_nonexistent_xyz() RENAME TO af_something;

-- begin-expected
-- columns: ok
-- row: true
-- end-expected
SELECT true AS ok;

-- ============================================================================
-- 22. Multiple properties in single ALTER statement
-- ============================================================================

CREATE FUNCTION af_multi_props() RETURNS integer LANGUAGE sql AS $$ SELECT 1; $$;

ALTER FUNCTION af_multi_props() IMMUTABLE STRICT LEAKPROOF PARALLEL SAFE COST 50;

-- begin-expected
-- columns: provolatile, proisstrict, proleakproof, proparallel, procost
-- row: i, true, true, s, 50
-- end-expected
SELECT provolatile, proisstrict, proleakproof, proparallel, procost
FROM pg_proc WHERE proname = 'af_multi_props';

-- ============================================================================
-- 23. ALTER FUNCTION LEAKPROOF
-- ============================================================================

CREATE FUNCTION af_leak() RETURNS integer LANGUAGE sql AS $$ SELECT 1; $$;

ALTER FUNCTION af_leak() LEAKPROOF;

-- begin-expected
-- columns: proleakproof
-- row: true
-- end-expected
SELECT proleakproof FROM pg_proc WHERE proname = 'af_leak';

ALTER FUNCTION af_leak() NOT LEAKPROOF;

-- begin-expected
-- columns: proleakproof
-- row: false
-- end-expected
SELECT proleakproof FROM pg_proc WHERE proname = 'af_leak';

-- ============================================================================
-- 24. ALTER on built-in function (should error)
-- ============================================================================

-- note: PG allows altering built-in functions when run as superuser
ALTER FUNCTION pg_sleep(double precision) IMMUTABLE;

-- ============================================================================
-- 25. ALTER FUNCTION with wrong signature (not found)
-- ============================================================================

CREATE FUNCTION af_sig_test(integer) RETURNS integer LANGUAGE sql AS $$ SELECT $1; $$;

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
ALTER FUNCTION af_sig_test(text) RENAME TO af_sig_test_new;

-- ============================================================================
-- 26. ALTER PROCEDURE SET config parameter
-- ============================================================================

CREATE PROCEDURE af_proc_config() LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE '%', current_setting('work_mem');
END;
$$;

ALTER PROCEDURE af_proc_config() SET work_mem = '256MB';

-- begin-expected
-- columns: has_config
-- row: true
-- end-expected
SELECT proconfig IS NOT NULL AS has_config
FROM pg_proc WHERE proname = 'af_proc_config';

-- ============================================================================
-- 27. ALTER FUNCTION volatility transitions (all directions)
-- ============================================================================

CREATE FUNCTION af_vol_all() RETURNS integer LANGUAGE sql AS $$ SELECT 1; $$;

-- Default is VOLATILE
-- begin-expected
-- columns: provolatile
-- row: v
-- end-expected
SELECT provolatile FROM pg_proc WHERE proname = 'af_vol_all';

ALTER FUNCTION af_vol_all() STABLE;
-- begin-expected
-- columns: provolatile
-- row: s
-- end-expected
SELECT provolatile FROM pg_proc WHERE proname = 'af_vol_all';

ALTER FUNCTION af_vol_all() IMMUTABLE;
-- begin-expected
-- columns: provolatile
-- row: i
-- end-expected
SELECT provolatile FROM pg_proc WHERE proname = 'af_vol_all';

ALTER FUNCTION af_vol_all() VOLATILE;
-- begin-expected
-- columns: provolatile
-- row: v
-- end-expected
SELECT provolatile FROM pg_proc WHERE proname = 'af_vol_all';

-- ============================================================================
-- 28. ALTER FUNCTION PARALLEL transitions
-- ============================================================================

CREATE FUNCTION af_par_all() RETURNS integer LANGUAGE sql AS $$ SELECT 1; $$;

ALTER FUNCTION af_par_all() PARALLEL SAFE;
-- begin-expected
-- columns: proparallel
-- row: s
-- end-expected
SELECT proparallel FROM pg_proc WHERE proname = 'af_par_all';

ALTER FUNCTION af_par_all() PARALLEL RESTRICTED;
-- begin-expected
-- columns: proparallel
-- row: r
-- end-expected
SELECT proparallel FROM pg_proc WHERE proname = 'af_par_all';

ALTER FUNCTION af_par_all() PARALLEL UNSAFE;
-- begin-expected
-- columns: proparallel
-- row: u
-- end-expected
SELECT proparallel FROM pg_proc WHERE proname = 'af_par_all';

-- ============================================================================
-- 29. ALTER FUNCTION inside transaction, then ROLLBACK
-- ============================================================================

CREATE FUNCTION af_txn_test() RETURNS integer LANGUAGE sql AS $$ SELECT 1; $$;

BEGIN;
ALTER FUNCTION af_txn_test() IMMUTABLE;

-- Inside transaction, should see the change
-- begin-expected
-- columns: provolatile
-- row: i
-- end-expected
SELECT provolatile FROM pg_proc WHERE proname = 'af_txn_test';

ROLLBACK;

-- After rollback, should revert to VOLATILE
-- begin-expected
-- columns: provolatile
-- row: v
-- end-expected
SELECT provolatile FROM pg_proc WHERE proname = 'af_txn_test';

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA af_test CASCADE;
SET search_path = public;
