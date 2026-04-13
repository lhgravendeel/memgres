-- ============================================================================
-- Feature Comparison: Custom Operators
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- command: TAG   -> expected command tag
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS op_test CASCADE;
CREATE SCHEMA op_test;
SET search_path = op_test, public;

CREATE TABLE op_data (id integer PRIMARY KEY, val integer, label text);
INSERT INTO op_data VALUES (1, 10, 'alpha'), (2, 20, 'beta'), (3, 30, 'gamma');

-- ============================================================================
-- 1. Basic binary operator
-- ============================================================================

CREATE FUNCTION op_int_add_10(a integer, b integer) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$ SELECT a + b + 10 $$;

-- command: CREATE OPERATOR
CREATE OPERATOR +++ (
  LEFTARG = integer,
  RIGHTARG = integer,
  FUNCTION = op_int_add_10
);

-- begin-expected
-- columns: result
-- row: 5
-- end-expected
SELECT 2 +++ 3 AS result;

-- ============================================================================
-- 2. Prefix (unary) operator
-- ============================================================================

CREATE FUNCTION op_negate_text(a text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT reverse(a) $$;

CREATE OPERATOR !!! (
  RIGHTARG = text,
  FUNCTION = op_negate_text
);

-- begin-expected
-- columns: result
-- row: olleh
-- end-expected
SELECT !!! 'hello' AS result;

-- ============================================================================
-- 3. Multi-character operator
-- ============================================================================

CREATE FUNCTION op_text_contains(haystack text, needle text) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$ SELECT position(needle in haystack) > 0 $$;

CREATE OPERATOR ~~> (
  LEFTARG = text,
  RIGHTARG = text,
  FUNCTION = op_text_contains
);

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT 'hello world' ~~> 'world' AS result;

-- begin-expected
-- columns: result
-- row: false
-- end-expected
SELECT 'hello world' ~~> 'xyz' AS result;

-- ============================================================================
-- 4. OPERATOR() qualified syntax
-- ============================================================================

-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT 2 OPERATOR(op_test.+++) 3 AS result;

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT 'hello' OPERATOR(op_test.~~>) 'ell' AS result;

-- ============================================================================
-- 5. Operator overloading: same symbol, different types
-- ============================================================================

CREATE FUNCTION op_text_concat_bang(a text, b text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT a || '!' || b $$;

-- command: CREATE OPERATOR
CREATE OPERATOR +++ (
  LEFTARG = text,
  RIGHTARG = text,
  FUNCTION = op_text_concat_bang
);

-- Integer version
-- begin-expected
-- columns: result
-- row: 5
-- end-expected
SELECT 2 +++ 3 AS result;

-- Text version (+++ tokenized as + + + which doesn't apply to text)
-- begin-expected-error
-- message-like: operator does not exist
-- end-expected-error
SELECT 'hello'::text +++ 'world'::text AS result;

-- ============================================================================
-- 6. Operator in WHERE clause
-- ============================================================================

-- begin-expected
-- columns: label
-- row: alpha
-- row: beta
-- row: gamma
-- end-expected
SELECT label FROM op_data
WHERE label ~~> 'a'
ORDER BY id;

-- ============================================================================
-- 7. Operator in SELECT list
-- ============================================================================

-- begin-expected
-- columns: id, boosted
-- row: 1, 11
-- row: 2, 21
-- row: 3, 31
-- end-expected
SELECT id, val +++ 1 AS boosted FROM op_data ORDER BY id;

-- ============================================================================
-- 8. Operator in ORDER BY
-- ============================================================================

-- begin-expected
-- columns: id
-- row: 3
-- row: 2
-- row: 1
-- end-expected
SELECT id FROM op_data ORDER BY val +++ 0 DESC;

-- ============================================================================
-- 9. Operator in JOIN ON
-- ============================================================================

CREATE TABLE op_patterns (pattern text);
INSERT INTO op_patterns VALUES ('alph'), ('gam');

-- begin-expected
-- columns: label, pattern
-- row: alpha, alph
-- row: gamma, gam
-- end-expected
SELECT d.label, p.pattern
FROM op_data d
JOIN op_patterns p ON d.label ~~> p.pattern
ORDER BY d.id;

DROP TABLE op_patterns;

-- ============================================================================
-- 10. Operator with NULL input
-- ============================================================================

-- note: Unless the backing function is STRICT, NULLs are passed through.
--       Our non-STRICT op_int_add_10 will produce NULL if either input is NULL.

-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT (NULL::integer +++ 5) IS NULL AS is_null;

-- ============================================================================
-- 11. Operator with STRICT backing function
-- ============================================================================

CREATE FUNCTION op_strict_multiply(a integer, b integer) RETURNS integer
LANGUAGE sql STRICT IMMUTABLE AS $$ SELECT a * b $$;

CREATE OPERATOR *** (
  LEFTARG = integer,
  RIGHTARG = integer,
  FUNCTION = op_strict_multiply
);

-- begin-expected
-- columns: r1, r2
-- row: 12, true
-- end-expected
SELECT 3 *** 4 AS r1, (NULL::integer *** 4) IS NULL AS r2;

-- ============================================================================
-- 12. COMMUTATOR hint
-- ============================================================================

CREATE FUNCTION op_is_less(a integer, b integer) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$ SELECT a < b $$;

CREATE FUNCTION op_is_greater(a integer, b integer) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$ SELECT a > b $$;

CREATE OPERATOR <<< (
  LEFTARG = integer,
  RIGHTARG = integer,
  FUNCTION = op_is_less,
  COMMUTATOR = >>>
);

CREATE OPERATOR >>> (
  LEFTARG = integer,
  RIGHTARG = integer,
  FUNCTION = op_is_greater,
  COMMUTATOR = <<<
);

-- begin-expected
-- columns: r1, r2
-- row: true, false
-- end-expected
SELECT 1 <<< 2 AS r1, 2 <<< 1 AS r2;

-- begin-expected
-- columns: r1, r2
-- row: true, false
-- end-expected
SELECT 2 >>> 1 AS r1, 1 >>> 2 AS r2;

-- ============================================================================
-- 13. NEGATOR hint
-- ============================================================================

CREATE FUNCTION op_eq_custom(a integer, b integer) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$ SELECT a = b $$;

CREATE FUNCTION op_neq_custom(a integer, b integer) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$ SELECT a <> b $$;

CREATE OPERATOR === (
  LEFTARG = integer,
  RIGHTARG = integer,
  FUNCTION = op_eq_custom,
  NEGATOR = !==,
  COMMUTATOR = ===
);

CREATE OPERATOR !== (
  LEFTARG = integer,
  RIGHTARG = integer,
  FUNCTION = op_neq_custom,
  NEGATOR = ===,
  COMMUTATOR = !==
);

-- begin-expected
-- columns: r1, r2, r3, r4
-- row: true, false, false, true
-- end-expected
SELECT 5 === 5 AS r1, 5 === 6 AS r2, 5 !== 5 AS r3, 5 !== 6 AS r4;

-- ============================================================================
-- 14. DROP OPERATOR
-- ============================================================================

CREATE FUNCTION op_drop_fn(a integer, b integer) RETURNS integer
LANGUAGE sql AS $$ SELECT a - b $$;

CREATE OPERATOR --- (
  LEFTARG = integer,
  RIGHTARG = integer,
  FUNCTION = op_drop_fn
);

-- Verify it works
-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT 5 --- 2 AS result;

DROP OPERATOR --- (integer, integer);

-- begin-expected-error
-- message-like: operator does not exist
-- end-expected-error
SELECT 5 --- 2;

-- ============================================================================
-- 15. DROP OPERATOR IF EXISTS
-- ============================================================================

-- command: DROP OPERATOR
DROP OPERATOR IF EXISTS --- (integer, integer);

-- ============================================================================
-- 16. Operator in aggregate expression
-- ============================================================================

-- begin-expected
-- columns: total
-- row: 63
-- end-expected
SELECT sum(val +++ 1) AS total FROM op_data;

-- ============================================================================
-- 17. Custom aggregate using custom operator's backing function
-- ============================================================================

CREATE AGGREGATE agg_custom_sum(integer) (
  SFUNC = op_int_add_10,
  STYPE = integer,
  INITCOND = '0'
);

-- note: Each step adds val + 10 (because op_int_add_10 adds 10)
-- 0 + 10 + 10 = 20, 20 + 20 + 10 = 50, 50 + 30 + 10 = 90
-- begin-expected
-- columns: result
-- row: 90
-- end-expected
SELECT agg_custom_sum(val) AS result FROM op_data;

-- ============================================================================
-- 18. Operator in CASE expression
-- ============================================================================

-- begin-expected
-- columns: id, category
-- row: 1, small
-- row: 2, small
-- row: 3, large
-- end-expected
SELECT id,
  CASE WHEN val +++ 0 < 25 THEN 'small' ELSE 'large' END AS category
FROM op_data ORDER BY id;

-- ============================================================================
-- 19. Operator in CHECK constraint
-- ============================================================================

-- command: CREATE TABLE
CREATE TABLE op_checked (
  id integer PRIMARY KEY,
  a integer,
  b integer,
  CHECK ((a +++ b) > 0)
);

-- command: INSERT 0 1
INSERT INTO op_checked VALUES (1, 5, 3);

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM op_checked;

-- begin-expected-error
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO op_checked VALUES (2, -10, -20);

-- command: DROP TABLE
DROP TABLE op_checked;

-- ============================================================================
-- 20. Operator in subquery
-- ============================================================================

-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM op_data
WHERE val +++ 0 > (SELECT min(val) +++ 5 FROM op_data)
ORDER BY id;

-- ============================================================================
-- 21. Operator in CTE
-- ============================================================================

-- begin-expected
-- columns: id, boosted
-- row: 1, 11
-- row: 2, 21
-- row: 3, 31
-- end-expected
WITH boosted AS (
  SELECT id, val +++ 1 AS boosted FROM op_data
)
SELECT * FROM boosted ORDER BY id;

-- ============================================================================
-- 22. Operator in HAVING
-- ============================================================================

-- begin-expected
-- columns: label, total
-- row: gamma, 31
-- end-expected
SELECT label, sum(val +++ 1) AS total
FROM op_data
GROUP BY id, label, val
HAVING sum(val +++ 1) > 25
ORDER BY id;

-- ============================================================================
-- 23. Operator inside PL/pgSQL function body
-- ============================================================================

CREATE FUNCTION op_in_plpgsql(a integer, b integer) RETURNS integer
LANGUAGE plpgsql AS $$
BEGIN
  RETURN a +++ b;
END;
$$;

-- begin-expected
-- columns: result
-- row: 5
-- end-expected
SELECT op_in_plpgsql(2, 3) AS result;

-- ============================================================================
-- 24. Operator with qualified syntax inside PL/pgSQL
-- ============================================================================

-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
CREATE FUNCTION op_qualified_in_plpgsql(a integer, b integer) RETURNS integer
LANGUAGE plpgsql AS $$
BEGIN
  RETURN a OPERATOR(op_test.+++) b;
END;
$$;

-- begin-expected-error
-- message-like: function op_qualified_in_plpgsql(integer, integer) does not exist
-- end-expected-error
SELECT op_qualified_in_plpgsql(2, 3) AS result;

-- ============================================================================
-- 25. pg_operator catalog
-- ============================================================================

-- +++ cannot be created as operator name (PG tokenizes as + + +)
-- begin-expected
-- columns: exists
-- row: f
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_operator WHERE oprname = '+++'
) AS exists;

-- Verify catalog fields: +++ was never created, expect 0 rows
-- begin-expected
-- columns: has_left, has_right
-- end-expected
SELECT
  oprleft <> 0 AS has_left,
  oprright <> 0 AS has_right
FROM pg_operator
WHERE oprname = '+++'
LIMIT 1;

-- ============================================================================
-- 26. CREATE OPERATOR CLASS (B-tree)
-- ============================================================================

-- note: Operator classes define the strategy for index operations.
-- We create a minimal B-tree operator class.

CREATE FUNCTION op_int_lt(a integer, b integer) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$ SELECT a < b $$;

CREATE FUNCTION op_int_le(a integer, b integer) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$ SELECT a <= b $$;

CREATE FUNCTION op_int_eq(a integer, b integer) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$ SELECT a = b $$;

CREATE FUNCTION op_int_ge(a integer, b integer) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$ SELECT a >= b $$;

CREATE FUNCTION op_int_gt(a integer, b integer) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$ SELECT a > b $$;

CREATE FUNCTION op_int_cmp(a integer, b integer) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$ SELECT CASE WHEN a < b THEN -1 WHEN a = b THEN 0 ELSE 1 END $$;

CREATE OPERATOR <~ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = op_int_lt, COMMUTATOR = >~);
CREATE OPERATOR <=~ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = op_int_le, COMMUTATOR = >=~);
CREATE OPERATOR =~ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = op_int_eq, COMMUTATOR = =~);
CREATE OPERATOR >=~ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = op_int_ge, COMMUTATOR = <=~);
CREATE OPERATOR >~ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = op_int_gt, COMMUTATOR = <~);

CREATE OPERATOR CLASS op_int_btree_ops FOR TYPE integer USING btree AS
  OPERATOR 1 <~,
  OPERATOR 2 <=~,
  OPERATOR 3 =~,
  OPERATOR 4 >=~,
  OPERATOR 5 >~,
  FUNCTION 1 op_int_cmp(integer, integer);

-- Verify in pg_opclass
-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_opclass WHERE opcname = 'op_int_btree_ops'
) AS exists;

-- ============================================================================
-- 27. CREATE OPERATOR FAMILY
-- ============================================================================

CREATE OPERATOR FAMILY op_int_fam USING btree;

-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_opfamily WHERE opfname = 'op_int_fam'
) AS exists;

DROP OPERATOR FAMILY op_int_fam USING btree;

-- ============================================================================
-- 28. DROP OPERATOR CLASS
-- ============================================================================

DROP OPERATOR CLASS op_int_btree_ops USING btree;

-- begin-expected
-- columns: exists
-- row: false
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_opclass WHERE opcname = 'op_int_btree_ops'
) AS exists;

-- ============================================================================
-- 29. Operator volatility in index expression
-- ============================================================================

-- Operator backed by VOLATILE function should be rejected in CREATE INDEX
CREATE FUNCTION op_volatile_fn(a integer, b integer) RETURNS integer
LANGUAGE sql VOLATILE AS $$ SELECT a + b $$;

CREATE OPERATOR +~+ (
  LEFTARG = integer,
  RIGHTARG = integer,
  FUNCTION = op_volatile_fn
);

CREATE TABLE op_vol_test (id integer, val integer);

-- command: CREATE INDEX
CREATE INDEX idx_vol_op ON op_vol_test ((id +~+ val));

DROP TABLE op_vol_test;

-- ============================================================================
-- 30. Operator backed by IMMUTABLE function in index expression (success)
-- ============================================================================

CREATE TABLE op_idx_test (id integer, val integer);
INSERT INTO op_idx_test VALUES (1, 5), (2, 10);

-- +++ is backed by IMMUTABLE op_int_add_10
-- command: CREATE INDEX
CREATE INDEX idx_immut_op ON op_idx_test ((id +++ val));

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM op_idx_test ORDER BY id;

DROP TABLE op_idx_test CASCADE;

-- ============================================================================
-- 31. Multiple operators in single expression
-- ============================================================================

-- begin-expected
-- columns: result
-- row: true
-- end-expected
SELECT (2 +++ 3) <<< (10 +++ 20) AS result;

-- ============================================================================
-- 32. Operator with boolean result in WHERE
-- ============================================================================

-- begin-expected
-- columns: id, label
-- row: 1, alpha
-- end-expected
SELECT id, label FROM op_data WHERE val <<< 15 ORDER BY id;

-- ============================================================================
-- 33. Qualified operator in WHERE
-- ============================================================================

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM op_data WHERE val OPERATOR(op_test.<<<) 15 ORDER BY id;

-- ============================================================================
-- 34. Operator in RETURNING clause
-- ============================================================================

CREATE TABLE op_ret_test (id integer PRIMARY KEY, val integer);
INSERT INTO op_ret_test VALUES (1, 5);

-- begin-expected
-- columns: boosted
-- row: 16
-- end-expected
UPDATE op_ret_test SET val = 6 WHERE id = 1 RETURNING val +++ 10 AS boosted;

DROP TABLE op_ret_test;

-- ============================================================================
-- 35. Operator error: missing backing function
-- ============================================================================

-- begin-expected-error
-- message-like: function op_nonexistent(integer, integer) does not exist
-- end-expected-error
CREATE OPERATOR @@@ (
  LEFTARG = integer,
  RIGHTARG = integer,
  FUNCTION = op_nonexistent
);

-- ============================================================================
-- 36. Operator error: CREATE without LEFTARG or RIGHTARG
-- ============================================================================

-- note: At least one of LEFTARG or RIGHTARG must be specified

-- begin-expected-error
-- message-like: operator argument types must be specified
-- end-expected-error
CREATE OPERATOR @@@ (
  FUNCTION = op_int_add_10
);

-- ============================================================================
-- 37. Operator in window function expression
-- ============================================================================

-- begin-expected
-- columns: id, val, running_boosted
-- row: 1, 10, 11
-- row: 2, 20, 32
-- row: 3, 30, 63
-- end-expected
SELECT id, val,
  sum(val +++ 1) OVER (ORDER BY id) AS running_boosted
FROM op_data
ORDER BY id;

-- ============================================================================
-- 38. Forward reference: operator in PL/pgSQL body before operator exists
-- ============================================================================

-- note: PL/pgSQL bodies are not validated at CREATE time,
--       so referencing a not-yet-existing operator succeeds at CREATE time.

CREATE FUNCTION op_forward_ref(a integer, b integer) RETURNS integer
LANGUAGE plpgsql AS $$
BEGIN
  RETURN a @#@ b;
END;
$$;

-- Calling before operator exists fails
-- begin-expected-error
-- message-like: operator does not exist
-- end-expected-error
SELECT op_forward_ref(1, 2);

-- Now create the operator
CREATE FUNCTION op_forward_fn(a integer, b integer) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$ SELECT a * b + 100 $$;

CREATE OPERATOR @#@ (
  LEFTARG = integer,
  RIGHTARG = integer,
  FUNCTION = op_forward_fn
);

-- Now it works
-- begin-expected
-- columns: result
-- row: 102
-- end-expected
SELECT op_forward_ref(1, 2) AS result;

-- ============================================================================
-- 39. Operator with text arguments
-- ============================================================================

CREATE FUNCTION op_text_distance(a text, b text) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$ SELECT abs(length(a) - length(b)) $$;

CREATE OPERATOR <-> (
  LEFTARG = text,
  RIGHTARG = text,
  FUNCTION = op_text_distance
);

-- begin-expected
-- columns: result
-- row: 3
-- end-expected
SELECT 'hello' <-> 'hi' AS result;

-- ============================================================================
-- 40. Operator used with table columns
-- ============================================================================

-- begin-expected
-- columns: id, dist
-- row: 1, 3
-- row: 2, 2
-- row: 3, 3
-- end-expected
SELECT id, label <-> 'ab' AS dist FROM op_data ORDER BY id;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA op_test CASCADE;
SET search_path = public;
