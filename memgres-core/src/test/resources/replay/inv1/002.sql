-- source: investigation.md
-- finding: 2
-- title: Internal Java errors leak as `XX000` ⚠️
-- RANGE frame with any numeric (non-integer) offset:
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT sum(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t;
--   ERROR: Internal error: class java.math.BigDecimal cannot be cast to class java.lang.Double
--   Affects sum/count/avg/first_value/last_value alike — 5 confirmed.

-- Range strictly-left / strictly-right operators:
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '[1,3)'::int4range << '[5,8)'::int4range;
--   ERROR: Internal error: For input string: "[1,3)"     (also >>, and multirange forms)

-- NaN through numeric functions:
-- begin-expected
-- columns: round:numeric
-- row: NaN
-- rowcount: 1
-- end-expected
SELECT round('NaN'::numeric, 2);
-- Internal error: Character N is neither a decimal digit...
-- begin-expected
-- columns: trunc:numeric
-- row: NaN
-- rowcount: 1
-- end-expected
SELECT trunc('NaN'::numeric, 2);
-- same
-- begin-expected
-- columns: avg:numeric
-- row: NaN
-- rowcount: 1
-- end-expected
SELECT avg(v) FROM (VALUES ('NaN'::numeric),(1::numeric)) x(v);
-- same

-- begin-expected-error
-- sqlstate: 22015
-- message-like: interval field value out of range: "100000000000 years"
-- end-expected-error
SELECT '100000000000 years'::interval;
-- PG: 22015 interval out of range | mg: XX000;
