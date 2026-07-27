-- ============================================================================
-- Feature Comparison: SQLSTATE reporting for engine-level failures
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Covers:
--   1. RANGE window frames over a numeric ORDER BY column
--   2. Range and multirange "strictly left/right of" operators
--   3. NaN through numeric rounding and aggregation
--   4. Interval literals whose fields exceed the representable range
--   5. Nesting deeper than the parser will follow
-- ============================================================================

-- ============================================================================
-- 1. RANGE frame with a numeric offset over a numeric ORDER BY column
-- ============================================================================
DROP TABLE IF EXISTS esr_num CASCADE;
CREATE TABLE esr_num (n numeric);
INSERT INTO esr_num VALUES (1.5), (2.5), (10.25);

SELECT string_agg(s::text, ',' ORDER BY s) AS a FROM (
  SELECT sum(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s
  FROM esr_num) t;
SELECT string_agg(s::text, ',' ORDER BY s) AS a FROM (
  SELECT count(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s
  FROM esr_num) t;
SELECT string_agg(s::text, ',' ORDER BY s) AS a FROM (
  SELECT avg(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s
  FROM esr_num) t;
SELECT string_agg(s::text, ',' ORDER BY s) AS a FROM (
  SELECT first_value(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s
  FROM esr_num) t;
SELECT string_agg(s::text, ',' ORDER BY s) AS a FROM (
  SELECT last_value(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s
  FROM esr_num) t;
-- a numeric offset, not just a numeric ordering column
SELECT string_agg(s::text, ',' ORDER BY s) AS a FROM (
  SELECT sum(n) OVER (ORDER BY n RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING) AS s
  FROM esr_num) t;
-- descending order and a one-sided frame
SELECT string_agg(s::text, ',' ORDER BY s) AS a FROM (
  SELECT sum(n) OVER (ORDER BY n DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s
  FROM esr_num) t;
-- the same shape over integer and float8 columns still works
DROP TABLE IF EXISTS esr_int CASCADE;
CREATE TABLE esr_int (n int, f float8);
INSERT INTO esr_int VALUES (1, 1.5), (2, 2.5);
SELECT string_agg(s::text, ',' ORDER BY s) AS a FROM (
  SELECT sum(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s
  FROM esr_int) t;
SELECT string_agg(s::text, ',' ORDER BY s) AS a FROM (
  SELECT sum(f) OVER (ORDER BY f RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s
  FROM esr_int) t;

-- ============================================================================
-- 2. Range and multirange strictly-left / strictly-right
-- ============================================================================
SELECT ('[1,3)'::int4range << '[5,8)'::int4range)::text AS a;
SELECT ('[1,3)'::int4range << '[2,8)'::int4range)::text AS a;
SELECT ('[5,8)'::int4range >> '[1,3)'::int4range)::text AS a;
SELECT ('[1,3)'::int4range >> '[5,8)'::int4range)::text AS a;
-- touching endpoints are still disjoint when one side is exclusive
SELECT ('[1,5)'::int4range << '[5,8)'::int4range)::text AS a;
SELECT ('[1,5]'::int4range << '[5,8)'::int4range)::text AS a;
-- an empty range is strictly left of nothing
SELECT ('empty'::int4range << '[5,8)'::int4range)::text AS a;
SELECT ('[1,3)'::int4range << 'empty'::int4range)::text AS a;
-- only the facing bounds matter, so an unbounded far side is irrelevant
SELECT ('(,3)'::int4range << '[5,8)'::int4range)::text AS a;
SELECT ('[1,3)'::int4range << '[5,)'::int4range)::text AS a;
-- but an unbounded facing side reaches infinity and can never be to one side
SELECT ('(,3)'::int4range << '(,8)'::int4range)::text AS a;
SELECT ('[1,)'::int4range << '[5,8)'::int4range)::text AS a;
SELECT ('[1,3)'::int4range >> '(,0)'::int4range)::text AS a;
-- other range types
SELECT ('[1,3)'::int8range << '[5,8)'::int8range)::text AS a;
SELECT ('[1.5,3.5)'::numrange << '[5.5,8.5)'::numrange)::text AS a;
SELECT (daterange('2020-01-01','2020-02-01') << daterange('2020-03-01','2020-04-01'))::text AS a;
-- multiranges
SELECT ('{[1,3)}'::int4multirange << '{[5,8)}'::int4multirange)::text AS a;
SELECT ('{[5,8)}'::int4multirange >> '{[1,3)}'::int4multirange)::text AS a;
SELECT ('{[1,3),[4,6)}'::int4multirange << '{[8,9)}'::int4multirange)::text AS a;
SELECT ('{[1,3),[4,10)}'::int4multirange << '{[8,9)}'::int4multirange)::text AS a;
SELECT ('{}'::int4multirange << '{[5,8)}'::int4multirange)::text AS a;
-- the integer bit-shift meaning of the same operators is unaffected
SELECT (1 << 4)::text AS a;
SELECT (256 >> 4)::text AS a;
SELECT (B'1101' << 2)::text AS a;
SELECT (B'1101' >> 2)::text AS a;
SELECT ('192.168.1.5'::inet << '192.168.1.0/24'::inet)::text AS a;
SELECT ('192.168.1.0/24'::inet >> '192.168.1.5'::inet)::text AS a;

-- ============================================================================
-- 3. NaN through numeric rounding and aggregation
-- ============================================================================
SELECT round('NaN'::numeric, 2)::text AS a;
SELECT round('NaN'::numeric)::text AS a;
SELECT trunc('NaN'::numeric, 2)::text AS a;
SELECT trunc('NaN'::numeric)::text AS a;
SELECT ceil('NaN'::numeric)::text AS a;
SELECT floor('NaN'::numeric)::text AS a;
SELECT abs('NaN'::numeric)::text AS a;
DROP TABLE IF EXISTS esr_nan CASCADE;
CREATE TABLE esr_nan (v numeric);
INSERT INTO esr_nan VALUES ('NaN'), (1), (2);
SELECT sum(v)::text AS a FROM esr_nan;
SELECT avg(v)::text AS a FROM esr_nan;
SELECT max(v)::text AS a FROM esr_nan;
SELECT min(v)::text AS a FROM esr_nan;
SELECT sum(DISTINCT v)::text AS a FROM esr_nan;
SELECT avg(DISTINCT v)::text AS a FROM esr_nan;
-- NaN compares equal to itself for numeric, so this filter keeps every row
SELECT sum(v)::text AS a FROM esr_nan WHERE v = v;
-- a set with no NaN is unaffected
SELECT sum(v)::text AS a FROM esr_nan WHERE v <> 'NaN';
SELECT avg(v)::text AS a FROM esr_nan WHERE v <> 'NaN';
-- rounding still works for ordinary values
SELECT round(2.567::numeric, 2)::text AS a;
SELECT trunc(2.567::numeric, 2)::text AS a;
SELECT round(-2.5::numeric)::text AS a;

-- ============================================================================
-- 4. Interval fields beyond the representable range
-- ============================================================================
SELECT '100000000000 years'::interval::text AS a;
SELECT '100000000000000 days'::interval::text AS a;
SELECT '99999999999 months'::interval::text AS a;
-- ordinary intervals are unaffected
SELECT '1 year 2 months 3 days'::interval::text AS a;
SELECT '04:05:06'::interval::text AS a;
SELECT '-1 year'::interval::text AS a;
SELECT interval '1 day' + interval '2 hours' AS a;

-- ============================================================================
-- 5. Nesting deeper than the parser will follow
-- ============================================================================
SELECT ((repeat('[', 20000) || repeat(']', 20000))::jsonb IS NOT NULL)::text AS a;
SELECT ((repeat('[', 20000) || repeat(']', 20000))::json IS NOT NULL)::text AS a;
SELECT ((repeat('[', 100) || repeat(']', 100))::jsonb IS NOT NULL)::text AS a;
SELECT ((repeat('[', 100) || repeat(']', 100))::json IS NOT NULL)::text AS a;

DROP TABLE IF EXISTS esr_nan CASCADE;
DROP TABLE IF EXISTS esr_int CASCADE;
DROP TABLE IF EXISTS esr_num CASCADE;
