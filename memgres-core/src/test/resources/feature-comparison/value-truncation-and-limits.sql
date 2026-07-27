-- ============================================================================
-- Feature Comparison: silent truncation and value range limits
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Every case here is one where a value or a row count was quietly reduced
-- rather than reported. A short answer or a narrowed value looks legitimate,
-- so nothing downstream can notice it.
-- ============================================================================

-- ============================================================================
-- 1. Row counts that must not be capped
-- ============================================================================
SELECT count(*)::text AS a FROM generate_series(
  timestamp '2020-01-01', timestamp '2020-01-01' + interval '20000 hours', interval '1 hour');
SELECT count(*)::text AS a FROM generate_series(
  date '2020-01-01', date '2020-01-01' + 20000, interval '1 day');
SELECT count(*)::text AS a FROM generate_series(
  timestamptz '2020-01-01 00:00:00+00', timestamptz '2020-01-01 00:00:00+00' + interval '15000 hours', interval '1 hour');
-- the integer overload and short ranges are unaffected
SELECT count(*)::text AS a FROM generate_series(1, 20000);
SELECT count(*)::text AS a FROM generate_series(
  timestamp '2020-01-01', timestamp '2020-01-02', interval '1 hour');
SELECT count(*)::text AS a FROM generate_series(
  timestamp '2020-01-10', timestamp '2020-01-01', interval '-1 day');

WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 150000)
SELECT count(*)::text AS a FROM r;
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 50)
SELECT count(*)::text AS a FROM r;

-- ============================================================================
-- 2. numeric to integer must not wrap
-- ============================================================================
SELECT ('9223372036854775808'::numeric::int8)::text AS a;
SELECT ('-9223372036854775809'::numeric::int8)::text AS a;
SELECT ('99999999999999999999'::numeric::bigint)::text AS a;
SELECT ('2147483648'::numeric::int)::text AS a;
SELECT ('-2147483649'::numeric::int)::text AS a;
SELECT ('32768'::numeric::smallint)::text AS a;
-- values that do fit are unaffected
SELECT ('123'::numeric::int)::text AS a;
SELECT ('2.7'::numeric::int)::text AS a;
SELECT ('-2.7'::numeric::int)::text AS a;
SELECT ('9223372036854775807'::numeric::int8)::text AS a;
SELECT ('-9223372036854775808'::numeric::int8)::text AS a;

-- ============================================================================
-- 3. LIMIT and OFFSET are bigint
-- ============================================================================
SELECT count(*)::text AS a FROM (SELECT 1 LIMIT 2147483648) t;
SELECT count(*)::text AS a FROM (SELECT 1 LIMIT 9223372036854775807) t;
SELECT count(*)::text AS a FROM (SELECT 1 OFFSET 2147483648) t;
SELECT count(*)::text AS a FROM (SELECT 1 OFFSET 9223372036854775807) t;
DROP TABLE IF EXISTS vtl_t CASCADE;
CREATE TABLE vtl_t (i int);
INSERT INTO vtl_t SELECT generate_series(1,5);
SELECT count(*)::text AS a FROM (SELECT i FROM vtl_t LIMIT 2147483648) t;
SELECT count(*)::text AS a FROM (SELECT i FROM vtl_t OFFSET 2147483648) t;
SELECT count(*)::text AS a FROM (SELECT i FROM vtl_t ORDER BY i LIMIT 3) t;
SELECT count(*)::text AS a FROM (SELECT i FROM vtl_t ORDER BY i OFFSET 3) t;
-- a limit of zero returns nothing
SELECT count(*)::text AS a FROM (SELECT 1 LIMIT 0) t;
SELECT count(*)::text AS a FROM (SELECT i FROM vtl_t LIMIT 0) t;
-- and a negative one is still rejected
SELECT count(*)::text AS a FROM (SELECT 1 LIMIT -1) t;
SELECT count(*)::text AS a FROM (SELECT 1 OFFSET -1) t;
SELECT count(*)::text AS a FROM (SELECT i FROM vtl_t LIMIT NULL) t;

-- ============================================================================
-- 4. uuid input: the forms PG takes, and the ones it refuses
-- ============================================================================
SELECT ('a0eebc999c0b4ef8bb6d6bb9bd380a11'::uuid)::text AS a;
SELECT ('{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'::uuid)::text AS a;
SELECT ('a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11'::uuid)::text AS a;
SELECT ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid)::text AS a;
SELECT ('{a0eebc999c0b4ef8bb6d6bb9bd380a11}'::uuid)::text AS a;
-- a short value must not be padded into a different valid identifier
SELECT ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1'::uuid)::text AS a;
SELECT ('1-1-1-1-1'::uuid)::text AS a;
SELECT ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a111'::uuid)::text AS a;
SELECT ('not-a-uuid'::uuid)::text AS a;
SELECT (''::uuid)::text AS a;
SELECT ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid)::text AS a;

-- ============================================================================
-- 5. bit input radix prefixes
-- ============================================================================
SELECT ('b101'::varbit)::text AS a;
SELECT ('B101'::varbit)::text AS a;
SELECT ('x1f'::varbit)::text AS a;
SELECT ('X1F'::varbit)::text AS a;
SELECT ('B101'::bit(3))::text AS a;
SELECT ('xff'::varbit)::text AS a;
-- plain digits and genuinely invalid input are unaffected
SELECT ('101'::varbit)::text AS a;
SELECT ('102'::varbit)::text AS a;
SELECT ('xzz'::varbit)::text AS a;

-- ============================================================================
-- 6. Range bounds must fit the element type
-- ============================================================================
SELECT ('[1,99999999999999999999999)'::int4range)::text AS a;
SELECT ('[-99999999999999999999999,1)'::int4range)::text AS a;
SELECT ('[1,3000000000)'::int4range)::text AS a;
SELECT ('[1,3.5)'::int4range)::text AS a;
SELECT ('[1,99999999999999999999999)'::int8range)::text AS a;
SELECT ('[1,3.5)'::int8range)::text AS a;
-- bounds that do fit are unaffected
SELECT ('[1,10)'::int4range)::text AS a;
SELECT ('[1,2147483647)'::int4range)::text AS a;
SELECT ('[1,9223372036854775807)'::int8range)::text AS a;
SELECT ('empty'::int4range)::text AS a;
SELECT ('(,3)'::int4range)::text AS a;
-- and a numeric range still takes fractional bounds
SELECT ('[1.5,3.5)'::numrange)::text AS a;

-- ============================================================================
-- 7. A column default has to fit the column
-- ============================================================================
DROP TABLE IF EXISTS vtl_d CASCADE;
CREATE TABLE vtl_d (i int);
INSERT INTO vtl_d VALUES (1);
ALTER TABLE vtl_d ADD COLUMN c varchar(2) DEFAULT 'abcdef';
SELECT count(*)::text AS a FROM information_schema.columns WHERE table_name = 'vtl_d';
-- the same check applies with no rows to backfill
DROP TABLE IF EXISTS vtl_e CASCADE;
CREATE TABLE vtl_e (i int);
ALTER TABLE vtl_e ADD COLUMN c varchar(2) DEFAULT 'abcdef';
SELECT count(*)::text AS a FROM information_schema.columns WHERE table_name = 'vtl_e';
INSERT INTO vtl_e VALUES (1);
-- a default that fits is added and backfilled
ALTER TABLE vtl_d ADD COLUMN d varchar(10) DEFAULT 'ok';
SELECT d AS a FROM vtl_d;
ALTER TABLE vtl_d ADD COLUMN e char(3) DEFAULT 'abcdef';
ALTER TABLE vtl_d ADD COLUMN f numeric(3,0) DEFAULT 12345;

DROP TABLE IF EXISTS vtl_d CASCADE;
DROP TABLE IF EXISTS vtl_e CASCADE;
DROP TABLE IF EXISTS vtl_t CASCADE;
