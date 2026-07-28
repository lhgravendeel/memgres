-- ============================================================================
-- Feature Comparison: numeric specials, float overflow, math domains, overloads
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- numeric's NaN and infinities have to survive being stored and aggregated, not
-- only computed. real and double have to report a value that leaves their range
-- on the way in, not store an infinity in its place. The inverse trigonometric
-- and hyperbolic functions have domains, and PostgreSQL raises outside them.
-- And an untyped literal handed to a math function resolves to double precision,
-- which decides the answer: round('2.5') is 2, round(2.5::numeric) is 3.
-- ============================================================================

-- ============================================================================
-- 1. numeric NaN and the infinities as stored values
-- ============================================================================

DROP TABLE IF EXISTS nsm_store;
CREATE TABLE nsm_store (v numeric);

INSERT INTO nsm_store VALUES ('Infinity');
INSERT INTO nsm_store VALUES ('-Infinity');
INSERT INTO nsm_store VALUES ('inf');
INSERT INTO nsm_store VALUES ('-inf');
INSERT INTO nsm_store VALUES ('Infinity'::numeric);
INSERT INTO nsm_store VALUES (CAST('NaN' AS numeric));
INSERT INTO nsm_store SELECT 'Infinity';
INSERT INTO nsm_store SELECT 'Infinity'::numeric;
INSERT INTO nsm_store SELECT 'NaN'::numeric;

-- begin-expected
-- columns: a | b | c
-- row: 5, 2, 2
-- end-expected
SELECT (count(*) FILTER (WHERE v = 'Infinity'))::text AS a,
       (count(*) FILTER (WHERE v = '-Infinity'))::text AS b,
       (count(*) FILTER (WHERE v = 'NaN'))::text AS c
FROM nsm_store;

-- A numeric column still refuses anything that is not a number
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type numeric
-- end-expected-error
INSERT INTO nsm_store VALUES ('infin');

DROP TABLE nsm_store;

DROP TABLE IF EXISTS nsm_upd;
CREATE TABLE nsm_upd (id int primary key, v numeric);
INSERT INTO nsm_upd VALUES (1, 1);
UPDATE nsm_upd SET v = 'Infinity' WHERE id = 1;

-- begin-expected
-- columns: a
-- row: Infinity
-- end-expected
SELECT v::text AS a FROM nsm_upd WHERE id = 1;

-- The row is not left locked by the update
UPDATE nsm_upd SET v = 2 WHERE id = 1;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT v::text AS a FROM nsm_upd WHERE id = 1;

DROP TABLE nsm_upd;

-- ============================================================================
-- 2. The numeric functions over the specials
-- ============================================================================

-- begin-expected
-- columns: a | b | c | d
-- row: t, t, t, t
-- end-expected
SELECT scale('NaN'::numeric) IS NULL AS a,
       scale('Infinity'::numeric) IS NULL AS b,
       min_scale('NaN'::numeric) IS NULL AS c,
       min_scale('Infinity'::numeric) IS NULL AS d;

-- begin-expected
-- columns: a | b | c
-- row: NaN, Infinity, -Infinity
-- end-expected
SELECT trim_scale('NaN'::numeric)::text AS a,
       trim_scale('Infinity'::numeric)::text AS b,
       trim_scale('-Infinity'::numeric)::text AS c;

-- Ordinary numbers are untouched
-- begin-expected
-- columns: a | b | c
-- row: 3, 2, 1.1
-- end-expected
SELECT scale(1.230)::text AS a, min_scale(1.230)::text AS b, trim_scale(1.230)::text AS c;

-- The variance of a set holding a special is not a number
-- begin-expected
-- columns: a | b | c | d
-- row: NaN, NaN, NaN, NaN
-- end-expected
SELECT var_pop(v)::text AS a, var_samp(v)::text AS b,
       stddev_pop(v)::text AS c, stddev(v)::text AS d
FROM (VALUES ('NaN'::numeric),(1::numeric),(2::numeric)) x(v);

-- begin-expected
-- columns: a | b
-- row: NaN, NaN
-- end-expected
SELECT var_pop(v)::text AS a, stddev_pop(v)::text AS b
FROM (VALUES ('Infinity'::numeric),(1::numeric),(2::numeric)) x(v);

-- stddev over numeric answers in numeric
-- begin-expected
-- columns: a
-- row: numeric
-- end-expected
SELECT pg_typeof(stddev(v))::text AS a
FROM (VALUES ('NaN'::numeric),(1::numeric),(2::numeric)) x(v);

-- Ordinary sets keep their answers
-- begin-expected
-- columns: a | b | c | d
-- row: 1.5555555555555556, 2.3333333333333333, 1.2472191289246471, 1.5275252316519467
-- end-expected
SELECT var_pop(x)::text AS a, var_samp(x)::text AS b,
       stddev_pop(x)::text AS c, stddev(x)::text AS d
FROM (VALUES (1::numeric),(2::numeric),(4::numeric)) t(x);

-- Integer division carries the specials through
-- begin-expected
-- columns: a | b | c | d | e
-- row: Infinity, -Infinity, NaN, 0, NaN
-- end-expected
SELECT div('Infinity'::numeric, 2::numeric)::text AS a,
       div('-Infinity'::numeric, 2::numeric)::text AS b,
       div('NaN'::numeric, 2::numeric)::text AS c,
       div(2::numeric, 'Infinity'::numeric)::text AS d,
       div('Infinity'::numeric, 'Infinity'::numeric)::text AS e;

-- begin-expected
-- columns: a | b | c | d
-- row: 2, -2, -2, 0
-- end-expected
SELECT div(9,4)::text AS a, div(-9,4)::text AS b, div(9,-4)::text AS c, div(0,5)::text AS d;

-- begin-expected
-- columns: a
-- row: numeric
-- end-expected
SELECT pg_typeof(div(9,4))::text AS a;

-- to_char writes the word for NaN and fills the field for an infinity
-- begin-expected
-- columns: a | b | c | d
-- row:  NaN,  NaN,   NaN, NaN
-- end-expected
SELECT to_char('NaN'::numeric, '999') AS a, to_char('NaN'::float8, '999') AS b,
       to_char('NaN'::numeric, '9999') AS c, to_char('NaN'::numeric, 'FM999') AS d;

-- begin-expected
-- columns: a | b | c | d
-- row:  ###,  ###, -###, ###
-- end-expected
SELECT to_char('Infinity'::numeric, '999') AS a, to_char('Infinity'::float8, '999') AS b,
       to_char('-Infinity'::numeric, '999') AS c, to_char('Infinity'::numeric, 'FM999') AS d;

-- begin-expected
-- columns: a | b
-- row:   42,  -1.5
-- end-expected
SELECT to_char(42::numeric, '999') AS a, to_char(-1.5, '99.9') AS b;

-- A NaN belongs in no bucket
-- begin-expected-error
-- sqlstate: 2201G
-- message-like: operand, lower bound, and upper bound cannot be NaN
-- end-expected-error
SELECT width_bucket('NaN'::numeric, 0, 10, 5);

-- begin-expected-error
-- sqlstate: 2201G
-- message-like: operand, lower bound, and upper bound cannot be NaN
-- end-expected-error
SELECT width_bucket(1, 'NaN'::numeric, 10, 5);

-- begin-expected
-- columns: a | b | c
-- row: 2, 0, 4
-- end-expected
SELECT width_bucket(5.0,1.0,10.0,3) AS a, width_bucket(0,1,10,3) AS b,
       width_bucket(11,1,10,3) AS c;

-- A special has no money to become
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot convert NaN to bigint
-- end-expected-error
SELECT 'NaN'::numeric::money;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot convert infinity to bigint
-- end-expected-error
SELECT 'Infinity'::numeric::money;

-- A declared numeric field has no room for an infinity
-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
SELECT 'Infinity'::numeric(10,2);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: NUMERIC precision -1 must be between 1 and 1000
-- end-expected-error
SELECT 1::numeric(-1);

-- ============================================================================
-- 3. real and double overflow on the paths that store rather than compute
-- ============================================================================

DROP TABLE IF EXISTS nsm_real;
CREATE TABLE nsm_real (id int primary key, r real);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: is out of range for type real
-- end-expected-error
INSERT INTO nsm_real VALUES (1, 1e39);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: is out of range for type real
-- end-expected-error
INSERT INTO nsm_real VALUES (2, 3.5e38);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: is out of range for type real
-- end-expected-error
INSERT INTO nsm_real VALUES (3, '1e39');

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
INSERT INTO nsm_real SELECT 4, 3.4e38::float8*2;

-- The values real can hold are stored as before
INSERT INTO nsm_real VALUES (5, 1.5), (6, 3.4e38), (7, 'Infinity'), (8, NULL);

-- begin-expected
-- columns: id | a
-- row: 5, 1.5
-- row: 6, 3.4e+38
-- row: 7, Infinity
-- row: 8, NULL
-- end-expected
SELECT id, r::text AS a FROM nsm_real ORDER BY id;

DROP TABLE nsm_real;

DROP TABLE IF EXISTS nsm_alter;
CREATE TABLE nsm_alter (d float8);
INSERT INTO nsm_alter VALUES (1e39);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
ALTER TABLE nsm_alter ALTER COLUMN d TYPE real;

-- The column keeps its type and its value
-- begin-expected
-- columns: a
-- row: 1e+39
-- end-expected
SELECT d::text AS a FROM nsm_alter;

DROP TABLE nsm_alter;

DROP TABLE IF EXISTS nsm_big;
CREATE TABLE nsm_big (d float8);
INSERT INTO nsm_big VALUES (1.5e308),(1.5e308);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT sum(d) FROM nsm_big;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT avg(d) FROM nsm_big;

DROP TABLE nsm_big;

DROP TABLE IF EXISTS nsm_sum;
CREATE TABLE nsm_sum (d float8, r real, i int, n numeric);
INSERT INTO nsm_sum VALUES (1.5, 1.5, 1, 1.5), (2.5, 2.5, 2, 2.5);

-- begin-expected
-- columns: a | b | c | d | e | f
-- row: double precision, double precision, real, double precision, bigint, numeric
-- end-expected
SELECT pg_typeof(sum(d))::text AS a, pg_typeof(avg(d))::text AS b,
       pg_typeof(sum(r))::text AS c, pg_typeof(avg(r))::text AS d,
       pg_typeof(sum(i))::text AS e, pg_typeof(avg(i))::text AS f
FROM nsm_sum;

-- begin-expected
-- columns: a | b
-- row: 4, 2
-- end-expected
SELECT sum(d)::text AS a, avg(d)::text AS b FROM nsm_sum;

DROP TABLE nsm_sum;

DROP TABLE IF EXISTS nsm_ravg;
CREATE TABLE nsm_ravg (r real);
INSERT INTO nsm_ravg VALUES (3.0e38),(3.0e38);

-- begin-expected
-- columns: a | b
-- row: 3.0000000054977558e+38, double precision
-- end-expected
SELECT avg(r)::text AS a, pg_typeof(avg(r))::text AS b FROM nsm_ravg;

DROP TABLE nsm_ravg;

-- A literal that underflows a float type is reported, not stored as zero
-- begin-expected-error
-- sqlstate: 22003
-- message-like: is out of range for type double precision
-- end-expected-error
SELECT 1e-400::float8;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: "1e-400" is out of range for type double precision
-- end-expected-error
SELECT '1e-400'::float8;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: is out of range for type real
-- end-expected-error
SELECT 1e-46::real;

-- The offending value is written in plain decimal
-- begin-expected-error
-- sqlstate: 22003
-- message-like: "1000000000000000000000000000000000000000" is out of range for type real
-- end-expected-error
SELECT 1e39::real;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: "1000000000000000000000000000000000000000" is out of range for type real
-- end-expected-error
SELECT 1e39::numeric::real;

-- A float8 narrowed to float4 is reported by the operation instead
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT 1e39::float8::real;

-- The values inside the range are untouched
-- begin-expected
-- columns: a | b | c | d
-- row: 1e-46, 1e-45, 3.4e+38, 1e-05
-- end-expected
SELECT 1e-46::float8::text AS a, 1e-45::float4::text AS b,
       3.4e38::real::text AS c, 0.00001::float8::text AS d;

-- ============================================================================
-- 4. The mathematical domain errors
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22003
-- message-like: input is out of range
-- end-expected-error
SELECT asind(2);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: input is out of range
-- end-expected-error
SELECT acosd(-2);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: input is out of range
-- end-expected-error
SELECT acosh(0.5);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: input is out of range
-- end-expected-error
SELECT atanh(2.0);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: input is out of range
-- end-expected-error
SELECT atanh(2.0::float8);

-- Inside the domain they still answer, and exactly at the notable points
-- begin-expected
-- columns: a | b | c | d | e
-- row: 30, 60, 90, 0, 45
-- end-expected
SELECT asind(0.5)::text AS a, acosd(0.5)::text AS b, asind(1)::text AS c,
       acosd(1)::text AS d, atand(1)::text AS e;

-- begin-expected
-- columns: a | b | c | d
-- row: -90, 180, -45, 45
-- end-expected
SELECT asind(-1)::text AS a, acosd(-1)::text AS b, atand(-1)::text AS c,
       atan2d(1,1)::text AS d;

-- atanh is infinite at the ends of its domain rather than undefined there
-- begin-expected
-- columns: a | b | c | d
-- row: Infinity, -Infinity, 0, 0.5493061443340549
-- end-expected
SELECT atanh(1.0)::text AS a, atanh(-1.0)::text AS b, acosh(1::float8)::text AS c,
       atanh(0.5)::text AS d;

-- cot and cotd exist and are infinite where the tangent is zero
-- begin-expected
-- columns: a | b | c | d | e
-- row: Infinity, Infinity, 0, 1, -Infinity
-- end-expected
SELECT cot(0)::text AS a, cotd(0)::text AS b, cotd(90)::text AS c,
       cotd(45)::text AS d, cotd(180)::text AS e;

-- begin-expected
-- columns: a | b
-- row: 0.6420926159343306, double precision
-- end-expected
SELECT cot(1.0)::text AS a, pg_typeof(cot(1.0))::text AS b;

-- The degree functions are exact at the quarter turns
-- begin-expected
-- columns: a | b | c | d | e
-- row: Infinity, -Infinity, 1, 0, 0
-- end-expected
SELECT tand(90)::text AS a, tand(270)::text AS b, tand(45)::text AS c,
       tand(180)::text AS d, tand(360)::text AS e;

-- begin-expected
-- columns: a | b | c | d | e
-- row: 0.5, 1, 0, 0.5, 0
-- end-expected
SELECT sind(30)::text AS a, sind(90)::text AS b, sind(180)::text AS c,
       cosd(60)::text AS d, cosd(90)::text AS e;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: input is out of range
-- end-expected-error
SELECT tand('Infinity'::float8);

-- float8 exp reports both ends of the range
-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT exp(1000::float8);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT exp(1000::real);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: underflow
-- end-expected-error
SELECT exp(-1000::float8);

-- begin-expected
-- columns: a | b
-- row: 8.218407461554972e+307, 2.718281828459045
-- end-expected
SELECT exp(709::float8)::text AS a, exp(1)::text AS b;

-- The numeric transcendentals are computed in numeric, with PG's own result scale
-- begin-expected
-- columns: a | b | c | d
-- row: 2.7182818284590452, 22026.465794806717, 1.414213562373095, 0.50000000000000000
-- end-expected
SELECT exp(1::numeric)::text AS a, exp(10::numeric)::text AS b,
       sqrt(2::numeric)::text AS c, sqrt(0.25::numeric)::text AS d;

-- begin-expected
-- columns: a | b | c | d
-- row: 1.4142135623730950, 10.0000000000000000, 1.0000000000000000, 100000000000000000000
-- end-expected
SELECT power(2::numeric,0.5)::text AS a, log(2.0,1024.0)::text AS b,
       power(0::numeric,0)::text AS c, power(10::numeric,20::numeric)::text AS d;

-- begin-expected
-- columns: a | b | c
-- row: 0.6931471805599453, 2.3025850929940457, 2.0000000000000000
-- end-expected
SELECT ln(2::numeric)::text AS a, ln(10::numeric)::text AS b, log(100::numeric)::text AS c;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value overflows numeric format
-- end-expected-error
SELECT exp(6000::numeric);

-- A numeric logarithm reaches below where double underflows to zero
-- begin-expected
-- columns: a | b | c
-- row: t, t, 1002
-- end-expected
SELECT (ln(1e-400::numeric)::text LIKE '-921.03403719761827%') AS a,
       (log(1e-400::numeric)::text = '-400.'||repeat('0',400)) AS b,
       length(exp(-6000::numeric)::text) AS c;

-- The logarithm domain errors are unchanged
-- begin-expected-error
-- sqlstate: 2201E
-- message-like: cannot take logarithm of zero
-- end-expected-error
SELECT ln(0::numeric);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT log(1.0, 10.0);

-- begin-expected-error
-- sqlstate: 2201F
-- message-like: cannot take square root of a negative number
-- end-expected-error
SELECT sqrt(-1::numeric);

-- The float8 specials pass straight through
-- begin-expected
-- columns: a | b | c | d
-- row: NaN, NaN, Infinity, Infinity
-- end-expected
SELECT ln('NaN'::float8)::text AS a, sqrt('NaN'::float8)::text AS b,
       sqrt('Infinity'::float8)::text AS c, ln('Infinity'::float8)::text AS d;

-- A float8 of that size prints in exponential notation
-- begin-expected
-- columns: a | b | c
-- row: 1.633123935319537e+16, 1e+16, 1.2345678901234568e+17
-- end-expected
SELECT tan(pi()/2)::text AS a, 1e16::float8::text AS b,
       123456789012345678::float8::text AS c;

-- The root operators answer in float8
-- begin-expected
-- columns: a | b | c | d
-- row: -2, double precision, 2, double precision
-- end-expected
SELECT (||/ -8.0)::text AS a, pg_typeof(||/ -8.0)::text AS b,
       (|/ 4.0)::text AS c, pg_typeof(|/ 4.0)::text AS d;

-- ============================================================================
-- 5. The overload an untyped literal resolves to
-- ============================================================================

-- round on an untyped literal is round(double precision), which rounds half to even
-- begin-expected
-- columns: a | b | c | d | e
-- row: 2, -2, 0, 2, double precision
-- end-expected
SELECT round('2.5')::text AS a, round('-2.5')::text AS b, round('0.5')::text AS c,
       round('1.5')::text AS d, pg_typeof(round('2.5'))::text AS e;

-- begin-expected
-- columns: a | b | c | d | e
-- row: 2, 4, -2, 0, double precision
-- end-expected
SELECT round(2.5::float8)::text AS a, round(3.5::float8)::text AS b,
       round(-2.5::float8)::text AS c, round(0.5::float8)::text AS d,
       pg_typeof(round(2.5::float8))::text AS e;

-- begin-expected
-- columns: a | b
-- row: 2, double precision
-- end-expected
SELECT round(2.5::float4)::text AS a, pg_typeof(round(2.5::float4))::text AS b;

-- round on a numeric still rounds half away from zero
-- begin-expected
-- columns: a | b | c | d
-- row: 3, 4, -3, numeric
-- end-expected
SELECT round(2.5::numeric)::text AS a, round(3.5::numeric)::text AS b,
       round(-2.5::numeric)::text AS c, pg_typeof(round(2.5))::text AS d;

-- The same shape across the rest of the family
-- begin-expected
-- columns: a | b | c | d | e | f
-- row: double precision, double precision, double precision, double precision, double precision, double precision
-- end-expected
SELECT pg_typeof(abs('-5'))::text AS a, pg_typeof(ceil('4.2'))::text AS b,
       pg_typeof(floor('4.2'))::text AS c, pg_typeof(sqrt('4'))::text AS d,
       pg_typeof(power('2','3'))::text AS e, pg_typeof(cbrt('8'))::text AS f;

-- begin-expected
-- columns: a | b | c | d | e
-- row: double precision, double precision, double precision, double precision, double precision
-- end-expected
SELECT pg_typeof(sign('-4.2'))::text AS a, pg_typeof(exp('1'))::text AS b,
       pg_typeof(ln('1'))::text AS c, pg_typeof(log('100'))::text AS d,
       pg_typeof(round('2.5'))::text AS e;

-- begin-expected
-- columns: a | b | c | d
-- row: 5, 5, 2, 8
-- end-expected
SELECT abs('-5')::text AS a, ceil('4.2')::text AS b, sqrt('4')::text AS c,
       power('2','3')::text AS d;

-- An integer argument resolves to float8 too; only a numeric one stays numeric
-- begin-expected
-- columns: a | b | c | d | e
-- row: double precision, double precision, numeric, double precision, numeric
-- end-expected
SELECT pg_typeof(round(5))::text AS a, pg_typeof(sqrt(4))::text AS b,
       pg_typeof(sqrt(4::numeric))::text AS c, pg_typeof(ceil(4))::text AS d,
       pg_typeof(ceil(4.2))::text AS e;

-- An all-untyped call with no single reading is refused
-- begin-expected-error
-- sqlstate: 42725
-- message-like: function trunc(unknown) is not unique
-- end-expected-error
SELECT trunc('2.9');

-- begin-expected-error
-- sqlstate: 42725
-- message-like: function mod(unknown, unknown) is not unique
-- end-expected-error
SELECT mod('5', '2');

-- begin-expected-error
-- sqlstate: 42725
-- message-like: function gcd(unknown, unknown) is not unique
-- end-expected-error
SELECT gcd('12', '8');

-- One typed argument is enough to settle it
-- begin-expected
-- columns: a | b | c | d
-- row: 1, 4, 2, integer
-- end-expected
SELECT mod('5', 2)::text AS a, gcd('12', 8)::text AS b,
       trunc('2.9'::numeric)::text AS c, pg_typeof(mod('5', 2))::text AS d;

-- begin-expected
-- columns: a | b | c | d
-- row: 2, -2, numeric, double precision
-- end-expected
SELECT trunc(2.9)::text AS a, trunc(-2.9::float8)::text AS b,
       pg_typeof(trunc(2.9))::text AS c, pg_typeof(trunc(2.9::float8))::text AS d;

-- abs answers in the argument's own type at every width
-- begin-expected
-- columns: a | b | c | d | e | f
-- row: bigint, integer, smallint, numeric, double precision, real
-- end-expected
SELECT pg_typeof(abs('-9223372036854775807'::int8))::text AS a,
       pg_typeof(abs('-2147483647'::int4))::text AS b,
       pg_typeof(abs('-32767'::int2))::text AS c,
       pg_typeof(abs(1.5::numeric))::text AS d,
       pg_typeof(abs(1.5::float8))::text AS e,
       pg_typeof(abs(1.5::float4))::text AS f;

-- begin-expected
-- columns: a | b | c
-- row: bigint, smallint, double precision
-- end-expected
SELECT pg_typeof(abs(NULL::int8))::text AS a, pg_typeof(abs(NULL::int2))::text AS b,
       pg_typeof(abs(NULL))::text AS c;

DROP TABLE IF EXISTS nsm_abs2;
DROP TABLE IF EXISTS nsm_abs;
CREATE TABLE nsm_abs (i int);
INSERT INTO nsm_abs VALUES (-5);
CREATE TABLE nsm_abs2 AS SELECT abs(i) AS x FROM nsm_abs;

-- The widened type is not persisted into the new table's column
-- begin-expected
-- columns: a
-- row: integer
-- end-expected
SELECT pg_typeof(x)::text AS a FROM nsm_abs2;

DROP TABLE nsm_abs2;
DROP TABLE nsm_abs;

-- numeric gcd has no int8 ceiling
-- begin-expected
-- columns: a | b | c
-- row: 9223372036854775808, 0.5, 12
-- end-expected
SELECT gcd('-9223372036854775808'::numeric, 0::numeric)::text AS a,
       gcd(1.5::numeric, 0.5::numeric)::text AS b,
       lcm(4::numeric, 6::numeric)::text AS c;

-- The integer forms keep their own ceilings and their own types
-- begin-expected
-- columns: a | b | c | d
-- row: 1, bigint, bigint, integer
-- end-expected
SELECT gcd('-9223372036854775808'::int8, 1::int8)::text AS a,
       pg_typeof(gcd('-9223372036854775808'::int8, 1::int8))::text AS b,
       pg_typeof(lcm(0::int8,0::int8))::text AS c,
       pg_typeof(gcd(12,8))::text AS d;

-- begin-expected
-- columns: a | b | c | d
-- row: 120, numeric, 2, integer
-- end-expected
SELECT factorial('5')::text AS a, pg_typeof(factorial('5'))::text AS b,
       width_bucket('5','1','10',3)::text AS c,
       pg_typeof(width_bucket('5','1','10',3))::text AS d;

-- ============================================================================
-- 6. Regression guard: the shapes around the new rules keep working
-- ============================================================================

DROP VIEW IF EXISTS nsm_view;
DROP TABLE IF EXISTS nsm_g;
CREATE TABLE nsm_g (id int, n numeric, d float8, r real, i int);
INSERT INTO nsm_g VALUES (1, 2.5, 2.5, 2.5, 3), (2, -2.5, -2.5, -2.5, -3), (3, NULL, NULL, NULL, NULL);

-- begin-expected
-- columns: id | a | b | c
-- row: 1, 3, 2, 2
-- row: 2, -3, -2, -2
-- row: 3, NULL, NULL, NULL
-- end-expected
SELECT id, round(n)::text AS a, round(d)::text AS b, round(r)::text AS c
FROM nsm_g ORDER BY id;

-- begin-expected
-- columns: a | b | c | d
-- row: numeric, double precision, double precision, integer
-- end-expected
SELECT pg_typeof(round(n))::text AS a, pg_typeof(round(d))::text AS b,
       pg_typeof(round(r))::text AS c, pg_typeof(abs(i))::text AS d
FROM nsm_g WHERE id = 1;

-- In WHERE, in ORDER BY and in GROUP BY
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM nsm_g WHERE round(n) = 3;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM nsm_g WHERE abs(i) >= 3 ORDER BY id;

-- begin-expected
-- columns: a
-- row: -3
-- row: 3
-- row: NULL
-- end-expected
SELECT round(n)::text AS a FROM nsm_g ORDER BY round(n) NULLS LAST;

-- begin-expected
-- columns: a
-- row: -2
-- row: 2
-- row: NULL
-- end-expected
SELECT round(d)::text AS a FROM nsm_g GROUP BY round(d) ORDER BY 1;

-- Through a derived table and through a view
-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT sub.rn::text AS a FROM (SELECT round(n) AS rn FROM nsm_g) sub WHERE sub.rn >= 3;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT sub.rn FROM (SELECT row_number() OVER (ORDER BY id) AS rn FROM nsm_g) sub
WHERE sub.rn >= 1 ORDER BY 1;

CREATE VIEW nsm_view AS SELECT id, round(d) AS rd, abs(i) AS ai FROM nsm_g;

-- begin-expected
-- columns: a | b
-- row: 2, 3
-- end-expected
SELECT rd::text AS a, ai::text AS b FROM nsm_view WHERE id = 1;

-- begin-expected
-- columns: a | b
-- row: double precision, integer
-- end-expected
SELECT pg_typeof(rd)::text AS a, pg_typeof(ai)::text AS b FROM nsm_view LIMIT 1;

-- Aggregates and plain arithmetic over the same columns
-- begin-expected
-- columns: a | b | c | d
-- row: 0.0, 0, 0, 0
-- end-expected
SELECT sum(n)::text AS a, sum(d)::text AS b, sum(i)::text AS c, avg(d)::text AS d FROM nsm_g;

-- begin-expected
-- columns: a | b | c | d
-- row: 3.5, 5, 1.25, 1
-- end-expected
SELECT (n + 1)::text AS a, (d * 2)::text AS b, (r / 2)::text AS c, (i % 2)::text AS d
FROM nsm_g WHERE id = 1;

-- begin-expected
-- columns: a | b
-- row: double precision, real
-- end-expected
SELECT pg_typeof(r * 2)::text AS a, pg_typeof(r * r)::text AS b FROM nsm_g WHERE id = 1;

DROP VIEW nsm_view;
DROP TABLE nsm_g;

-- Untouched neighbours
-- begin-expected
-- columns: a | b | c | d
-- row: 4, 4.000000000000000, 3, -2
-- end-expected
SELECT sqrt(16)::text AS a, sqrt(16.0)::text AS b, cbrt(27)::text AS c, cbrt(-8)::text AS d;

-- begin-expected
-- columns: a | b | c | d
-- row: -4, -5, -4, -5
-- end-expected
SELECT ceil(-4.2)::text AS a, floor(-4.2)::text AS b,
       ceil(-4.2::float8)::text AS c, floor(-4.2::float8)::text AS d;

-- begin-expected
-- columns: a | b | c | d
-- row: 2, 1, -2, 0
-- end-expected
SELECT round(1.5)::text AS a, round(0.5)::text AS b, round(-1.5)::text AS c,
       sign(0.0)::text AS d;

-- begin-expected
-- columns: a | b | c | d
-- row: NULL, NULL, NULL, NULL
-- end-expected
SELECT round(NULL::numeric) AS a, abs(NULL::numeric) AS b,
       trunc(NULL::numeric) AS c, width_bucket(NULL::float8, 0, 10, 5) AS d;

-- begin-expected
-- columns: a | b | c
-- row: NULL, NULL, NULL
-- end-expected
SELECT mod(NULL::int, 2) AS a, gcd(NULL::int, 2) AS b, lcm(NULL::int, 2) AS c;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT 1e308::float8 * 10;

-- begin-expected-error
-- sqlstate: 2201F
-- message-like: a negative number raised to a non-integer power
-- end-expected-error
SELECT (-1)::float8 ^ 0.5;

-- begin-expected
-- columns: a | b | c
-- row: 0.3333333333333333, 0.30000000000000004, 6
-- end-expected
SELECT (1::float8/3::float8)::text AS a, (0.1::float8 + 0.2::float8)::text AS b,
       ('5'::int + 1)::text AS c;
