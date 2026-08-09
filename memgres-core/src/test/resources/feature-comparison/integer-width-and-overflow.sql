-- ============================================================================
-- Feature Comparison: how wide an integer expression is, and when it overflows
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Integer arithmetic is done in the wider of its two operand types and checked
-- against that type's own bounds: int2 with int4 is an int4, either with int8
-- is an int8. There was an arm for two smallints and an arm for two integers
-- and nothing for a mixed pair, so a smallint beside a wider integer fell
-- through to the float8 path and 5::int2 / 2::int4 answered 2.5.
--
-- A sign in front of a numeric literal is part of the literal, as it is in
-- PostgreSQL's own grammar. Negating the literal afterwards read 2147483648
-- first, which is a bigint, so the expression was one width too wide and
-- (-2147483648) - 1 answered a number where PostgreSQL raises.
-- ============================================================================

SET search_path = public;


-- ============================================================================
-- The wider operand decides the type
-- ============================================================================

-- begin-expected
-- columns: r
-- row: smallint
-- end-expected
SELECT pg_typeof(5::int2 + 2::int2)::text AS r;

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_typeof(5::int2 + 2::int4)::text AS r;

-- begin-expected
-- columns: r
-- row: bigint
-- end-expected
SELECT pg_typeof(5::int2 + 2::int8)::text AS r;

-- begin-expected
-- columns: r
-- row: bigint
-- end-expected
SELECT pg_typeof(5::int4 + 2::int8)::text AS r;

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_typeof(5::int4 + 2::int4)::text AS r;


-- Integer division stays integer whatever the widths are.
-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT 5::int2 / 2::int4 AS r;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT 7 / 2 AS r;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT 7::int8 / 2::int2 AS r;

-- begin-expected
-- columns: r
-- row: 15
-- end-expected
SELECT 5::int2 * 3::int2 AS r;

-- begin-expected
-- columns: r
-- row: -3
-- end-expected
SELECT (-7) / 2 AS r;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT 7 % 3 AS r;


-- ============================================================================
-- And the bounds checked are that type's own
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22003
-- message-like: ERROR: smallint out of range
-- end-expected-error
SELECT 32767::int2 + 1::int2;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: ERROR: smallint out of range
-- end-expected-error
SELECT (-32768)::int2 * (-1)::int2;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: ERROR: integer out of range
-- end-expected-error
SELECT 2147483647::int4 + 1::int2;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: ERROR: integer out of range
-- end-expected-error
SELECT 2147483647 + 1;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: ERROR: bigint out of range
-- end-expected-error
SELECT 9223372036854775807 + 1;

-- begin-expected
-- columns: r
-- row: 32768
-- end-expected
SELECT 32767::int2 + 1::int4 AS r;

-- begin-expected
-- columns: r
-- row: 2147483648
-- end-expected
SELECT 2147483647::int4 + 1::int8 AS r;


-- Division by zero keeps its own error.
-- begin-expected-error
-- sqlstate: 22012
-- message-like: ERROR: division by zero
-- end-expected-error
SELECT 1/0;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: ERROR: division by zero
-- end-expected-error
SELECT 1::int2/0::int2;


-- ============================================================================
-- A sign belongs to the literal
-- ============================================================================

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_typeof(-2147483648)::text AS r;

-- begin-expected
-- columns: r
-- row: bigint
-- end-expected
SELECT pg_typeof(2147483648)::text AS r;

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_typeof(-5)::text AS r;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: ERROR: integer out of range
-- end-expected-error
SELECT (-2147483648) - 1;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: ERROR: integer out of range
-- end-expected-error
SELECT (-2147483648) * (-1);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: ERROR: integer out of range
-- end-expected-error
SELECT abs(-2147483648);

-- begin-expected
-- columns: r
-- row: 2147483648
-- end-expected
SELECT -(-2147483648) AS r;

-- begin-expected
-- columns: r
-- row: -5
-- end-expected
SELECT -(2+3) AS r;

-- begin-expected
-- columns: r
-- row: -2147483648
-- end-expected
SELECT -2147483648 AS r;

