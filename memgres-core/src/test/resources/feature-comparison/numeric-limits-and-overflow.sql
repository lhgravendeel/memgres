-- ============================================================================
-- Feature Comparison: numeric limits, overflow and the domain errors
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A value that leaves its type's range must be reported, not rounded into a
-- plausible wrong number. Covers numeric's NaN and infinities, the
-- two's-complement boundary where negation and abs have no result, real and
-- double arithmetic that overflows or underflows, numeric's typmod bounds,
-- the mathematical domain errors, and the declared-size limits on
-- char/varchar/bit and on an array's dimensions.
-- ============================================================================

-- ============================================================================
-- 1. numeric has NaN and, since PG 14, both infinities
-- ============================================================================

-- begin-expected
-- columns: a
-- row: Infinity
-- end-expected
SELECT 'Infinity'::numeric AS a;

-- begin-expected
-- columns: a
-- row: -Infinity
-- end-expected
SELECT '-Infinity'::numeric AS a;

-- The short spellings and the case-insensitive ones read the same way
-- begin-expected
-- columns: a
-- row: Infinity
-- end-expected
SELECT 'inf'::numeric AS a;

-- begin-expected
-- columns: a
-- row: -Infinity
-- end-expected
SELECT '-inf'::numeric AS a;

-- begin-expected
-- columns: a
-- row: Infinity
-- end-expected
SELECT 'INFINITY'::numeric AS a;

-- begin-expected
-- columns: a
-- row: NaN
-- end-expected
SELECT 'NaN'::numeric AS a;

-- Anything else is still rejected
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type numeric
-- end-expected-error
SELECT 'infin'::numeric;

-- Arithmetic follows the IEEE rules PG's numeric follows
-- begin-expected
-- columns: a
-- row: Infinity
-- end-expected
SELECT 'Infinity'::numeric + 1 AS a;

-- begin-expected
-- columns: a
-- row: NaN
-- end-expected
SELECT 'Infinity'::numeric - 'Infinity'::numeric AS a;

-- begin-expected
-- columns: a
-- row: NaN
-- end-expected
SELECT 'Infinity'::numeric * 0 AS a;

-- begin-expected
-- columns: a
-- row: NaN
-- end-expected
SELECT 'NaN'::numeric + 1 AS a;

-- begin-expected
-- columns: a
-- row: NaN
-- end-expected
SELECT 'Infinity'::numeric % 2 AS a;

-- begin-expected
-- columns: a
-- row: NaN
-- end-expected
SELECT mod('NaN'::numeric, 2) AS a;

-- begin-expected
-- columns: a
-- row: NaN
-- end-expected
SELECT sign('NaN'::numeric) AS a;

-- begin-expected
-- columns: a
-- row: -1
-- end-expected
SELECT sign('-Infinity'::numeric) AS a;

-- begin-expected
-- columns: a
-- row: Infinity
-- end-expected
SELECT abs('-Infinity'::numeric) AS a;

-- Comparison puts NaN above every other numeric, the infinities at the ends
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('-Infinity'::numeric < 'NaN'::numeric) AS a;

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('Infinity'::numeric < 'NaN'::numeric) AS a;

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('NaN'::numeric = 'NaN'::numeric) AS a;

-- begin-expected
-- columns: v
-- row: -Infinity
-- row: 1
-- row: Infinity
-- row: NaN
-- end-expected
SELECT v FROM (VALUES ('NaN'::numeric),(1::numeric),('-Infinity'::numeric),('Infinity'::numeric)) x(v) ORDER BY v;

-- Aggregation carries them through rather than losing the value's type
-- begin-expected
-- columns: a
-- row: NaN
-- end-expected
SELECT sum(v) AS a FROM (VALUES ('NaN'::numeric),(1::numeric)) x(v);

-- begin-expected
-- columns: a
-- row: Infinity
-- end-expected
SELECT sum(v) AS a FROM (VALUES ('Infinity'::numeric),(1::numeric)) x(v);

-- begin-expected
-- columns: a
-- row: NaN
-- end-expected
SELECT sum(v) AS a FROM (VALUES ('Infinity'::numeric),('-Infinity'::numeric)) x(v);

-- begin-expected
-- columns: a
-- row: Infinity
-- end-expected
SELECT avg(v) AS a FROM (VALUES ('Infinity'::numeric),(1::numeric)) x(v);

-- begin-expected
-- columns: a
-- row: NaN
-- end-expected
SELECT max(v) AS a FROM (VALUES ('NaN'::numeric),(1::numeric)) x(v);

-- NaN fits any numeric(p,s); an infinity is past every value one could round to
-- begin-expected
-- columns: a
-- row: NaN
-- end-expected
SELECT 'NaN'::numeric(10,2) AS a;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
SELECT 'Infinity'::numeric(10,2);

-- None of the three has an integer form, and numeric says so outright
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot convert NaN to integer
-- end-expected-error
SELECT 'NaN'::numeric::int;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot convert infinity to bigint
-- end-expected-error
SELECT '-Infinity'::numeric::bigint;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot convert infinity to integer
-- end-expected-error
SELECT ('Infinity'::numeric + 1)::int;

-- The same value arriving from float8 is a range error instead
-- begin-expected-error
-- sqlstate: 22003
-- end-expected-error
SELECT 'NaN'::float8::int;

-- Ordinary numeric casts to integer are untouched
-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT 1.9::numeric::int AS a;

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT 2.5::numeric::int AS a;

-- ============================================================================
-- 2. Negation and abs at a two's-complement minimum
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT abs('-9223372036854775808'::int8);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT -('-9223372036854775808'::int8);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT abs('-2147483648'::int4);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT -('-2147483648'::int4);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: smallint out of range
-- end-expected-error
SELECT abs('-32768'::int2);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: smallint out of range
-- end-expected-error
SELECT -('-32768'::int2);

-- The @ operator is the same absolute value
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT @ '-2147483648'::int4;

-- One step inside the boundary is unaffected
-- begin-expected
-- columns: a
-- row: 32767
-- end-expected
SELECT abs('-32767'::int2) AS a;

-- begin-expected
-- columns: a
-- row: 2147483647
-- end-expected
SELECT abs('-2147483647'::int4) AS a;

-- begin-expected
-- columns: a
-- row: 9223372036854775807
-- end-expected
SELECT abs(-9223372036854775807::int8) AS a;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT abs(-5) AS a;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT @ (-5) AS a;

-- begin-expected
-- columns: a
-- row: 2.5
-- end-expected
SELECT abs(-2.5::numeric) AS a;

-- gcd and lcm carry the same boundary
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT gcd('-9223372036854775808'::int8, 0::int8);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT gcd('-2147483648'::int4, 0::int4);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT lcm('-9223372036854775808'::int8, 1::int8);

-- begin-expected
-- columns: a
-- row: 4
-- end-expected
SELECT gcd(12, 8) AS a;

-- begin-expected
-- columns: a
-- row: 4
-- end-expected
SELECT gcd(-12, 8) AS a;

-- begin-expected
-- columns: a
-- row: 12
-- end-expected
SELECT lcm(4, 6) AS a;

-- begin-expected
-- columns: a
-- row: 12
-- end-expected
SELECT lcm(-4, 6) AS a;

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT lcm(0, 5) AS a;

-- ============================================================================
-- 3. real and double arithmetic that leaves the range
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT 3.4e38::real * 2::real;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT 3.0e38::real + 3.0e38::real;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT (-3.0e38)::real - 3.0e38::real;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT 3.4e38::real / 0.5::real;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: underflow
-- end-expected-error
SELECT 1.0e-38::real * 1.0e-38::real;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: underflow
-- end-expected-error
SELECT 1e-308::float8 * 1e-308::float8;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT 1e308::float8 * 10;

-- A real result keeps real's precision rather than double's
-- begin-expected
-- columns: a
-- row: 0.33333334
-- end-expected
SELECT (1.0::real / 3.0::real)::text AS a;

-- begin-expected
-- columns: a
-- row: 0.3
-- end-expected
SELECT (0.1::real + 0.2::real)::text AS a;

-- begin-expected
-- columns: a
-- row: 3.4e+38
-- end-expected
SELECT (3.4e38::real + 1::real)::text AS a;

-- An infinite operand is not an overflow
-- begin-expected
-- columns: a
-- row: Infinity
-- end-expected
SELECT 'Infinity'::real * 2::real AS a;

-- A difference or a product that is exactly zero has not underflowed
-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT 1.0::real - 1.0::real AS a;

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT 1.0::float8 - 1.0::float8 AS a;

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT 0.0::float8 * 5.0::float8 AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT 1.0::real * 2.0::real AS a;

-- begin-expected
-- columns: a
-- row: 2.5
-- end-expected
SELECT 5.0::float8 / 2.0::float8 AS a;

-- Float division reports a zero divisor rather than producing an infinity
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT 2::float8 / 0;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT 1.0::real / 0.0::real;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT 'Infinity'::float8 / 0;

-- A NaN dividend is the one case that still yields NaN
-- begin-expected
-- columns: a
-- row: NaN
-- end-expected
SELECT 'NaN'::float8 / 0 AS a;

-- The other types divide by zero the same way they always did
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT 2::numeric / 0;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT 2 / 0;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT 2 % 0;

-- A real total is a real, so it overflows where a double one would not
CREATE TABLE nlo_real_agg (r real);
INSERT INTO nlo_real_agg VALUES (3.0e38), (3.0e38);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT sum(r) FROM nlo_real_agg;

-- The average of the same column is a double and does not
-- begin-expected
-- columns: a
-- row: 3.0000000054977558e+38
-- end-expected
SELECT avg(r)::text AS a FROM nlo_real_agg;

DROP TABLE nlo_real_agg;

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT sum(x) AS a FROM (VALUES (1::real),(2::real)) t(x);

-- begin-expected
-- columns: a
-- row: 1.5
-- end-expected
SELECT avg(x) AS a FROM (VALUES (1::real),(2::real)) t(x);

-- ============================================================================
-- 4. numeric's typmod bounds
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22023
-- message-like: NUMERIC precision 1001 must be between 1 and 1000
-- end-expected-error
SELECT 1::numeric(1001,0);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: NUMERIC precision 0 must be between 1 and 1000
-- end-expected-error
SELECT 1::numeric(0,0);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: NUMERIC scale 1001 must be between -1000 and 1000
-- end-expected-error
SELECT 1::numeric(5,1001);

-- A scale wider than the precision leaves a fractional-only field
-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
SELECT 1::numeric(5,10);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
SELECT 0.00001::numeric(5,10);

-- begin-expected
-- columns: a
-- row: 0.0000010000
-- end-expected
SELECT 0.000001::numeric(5,10) AS a;

-- The bounds themselves are accepted
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT 1::numeric(1000,0) AS a;

-- begin-expected
-- columns: a
-- row: 1.00
-- end-expected
SELECT 1::numeric(5,2) AS a;

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT 1::numeric(5,-2) AS a;

-- begin-expected
-- columns: a
-- row: 12300
-- end-expected
SELECT 12345::numeric(5,-2) AS a;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
SELECT 12345.6::numeric(5,2);

-- ============================================================================
-- 5. Mathematical domain errors
-- ============================================================================

-- begin-expected-error
-- sqlstate: 2201F
-- message-like: zero raised to a negative power is undefined
-- end-expected-error
SELECT power(0::numeric, -1);

-- begin-expected-error
-- sqlstate: 2201F
-- message-like: zero raised to a negative power is undefined
-- end-expected-error
SELECT power(0::float8, -1);

-- begin-expected-error
-- sqlstate: 2201F
-- message-like: zero raised to a negative power is undefined
-- end-expected-error
SELECT 0::float8 ^ -1;

-- begin-expected-error
-- sqlstate: 2201F
-- message-like: a negative number raised to a non-integer power
-- end-expected-error
SELECT power(-1::numeric, 0.5);

-- begin-expected-error
-- sqlstate: 2201F
-- message-like: a negative number raised to a non-integer power
-- end-expected-error
SELECT power(-1::float8, 0.5);

-- begin-expected-error
-- sqlstate: 2201E
-- message-like: cannot take logarithm of zero
-- end-expected-error
SELECT ln(0::numeric);

-- begin-expected-error
-- sqlstate: 2201E
-- message-like: cannot take logarithm of a negative number
-- end-expected-error
SELECT ln(-1::numeric);

-- begin-expected-error
-- sqlstate: 2201E
-- message-like: cannot take logarithm of zero
-- end-expected-error
SELECT log(0::numeric);

-- begin-expected-error
-- sqlstate: 2201E
-- message-like: cannot take logarithm of a negative number
-- end-expected-error
SELECT log10(-1::numeric);

-- A base of one makes log(base, x) divide by zero
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT log(1.0, 10.0);

-- begin-expected-error
-- sqlstate: 2201E
-- message-like: cannot take logarithm of zero
-- end-expected-error
SELECT log(0.0, 10.0);

-- begin-expected-error
-- sqlstate: 2201E
-- message-like: cannot take logarithm of zero
-- end-expected-error
SELECT log(10.0, 0.0);

-- begin-expected-error
-- sqlstate: 2201F
-- message-like: cannot take square root of a negative number
-- end-expected-error
SELECT sqrt(-1::numeric);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: input is out of range
-- end-expected-error
SELECT asin(2::float8);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: input is out of range
-- end-expected-error
SELECT acos(2::float8);

-- Everything inside the domain is unchanged
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT power(0::numeric, 0) AS a;

-- begin-expected
-- columns: a
-- row: 8
-- end-expected
SELECT power(2::numeric, 3) AS a;

-- begin-expected
-- columns: a
-- row: -8
-- end-expected
SELECT power(-2::numeric, 3) AS a;

-- begin-expected
-- columns: a
-- row: 1024
-- end-expected
SELECT 2 ^ 10 AS a;

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT ln(1) AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT log(100) AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT log10(100) AS a;

-- begin-expected
-- columns: a
-- row: 10
-- end-expected
SELECT log(2, 1024) AS a;

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT sqrt(9) AS a;

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT asin(0) AS a;

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT acos(1) AS a;

-- ============================================================================
-- 6. Declared-size limits on char, varchar, bit and float
-- ============================================================================

-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type varchar cannot exceed 10485760
-- end-expected-error
CREATE TABLE nlo_limit (a varchar(10485761));

-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type varchar must be at least 1
-- end-expected-error
CREATE TABLE nlo_limit (a varchar(0));

-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type char must be at least 1
-- end-expected-error
CREATE TABLE nlo_limit (a char(0));

-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type char cannot exceed 10485760
-- end-expected-error
CREATE TABLE nlo_limit (a char(10485761));

-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type bit must be at least 1
-- end-expected-error
CREATE TABLE nlo_limit (a bit(0));

-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type varbit must be at least 1
-- end-expected-error
CREATE TABLE nlo_limit (a bit varying(0));

-- begin-expected-error
-- sqlstate: 22023
-- message-like: NUMERIC precision 1001 must be between 1 and 1000
-- end-expected-error
CREATE TABLE nlo_limit (a numeric(1001,2));

-- begin-expected-error
-- sqlstate: 22023
-- message-like: precision for type float must be less than 54 bits
-- end-expected-error
CREATE TABLE nlo_limit (a float(54));

-- begin-expected-error
-- sqlstate: 22023
-- message-like: precision for type float must be at least 1 bit
-- end-expected-error
CREATE TABLE nlo_limit (a float(0));

-- The same limits apply to a cast's type modifier
-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type varchar cannot exceed 10485760
-- end-expected-error
SELECT '1'::varchar(10485761);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type varchar must be at least 1
-- end-expected-error
SELECT '1'::varchar(0);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type char must be at least 1
-- end-expected-error
SELECT '1'::char(0);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type bit must be at least 1
-- end-expected-error
SELECT '1'::bit(0);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: precision for type float must be less than 54 bits
-- end-expected-error
SELECT 1::float(54);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: NUMERIC precision 1001 must be between 1 and 1000
-- end-expected-error
SELECT 1::numeric(1001,2);

-- Every modifier inside the limits still declares and casts
CREATE TABLE nlo_limit (a varchar(10), b char(5), c bit(4), d bit varying(8),
                        e numeric(1000,0), f numeric(5,2), g float(53), h float(24));
INSERT INTO nlo_limit VALUES ('abc', 'xy', '1010', '1100', 7, 1.25, 1.5, 2.5);

-- begin-expected
-- columns: a|b|c|d|e|f|g|h
-- row: abc|xy   |1010|1100|7|1.25|1.5|2.5
-- end-expected
SELECT a, b, c, d, e, f, g, h FROM nlo_limit;

DROP TABLE nlo_limit;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT '1'::varchar(5) AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT 1::float(53) AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT 1::float(24) AS a;

-- A value longer than its declared length is still rejected, the type is not
CREATE TABLE nlo_len (a varchar(3));

-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(3)
-- end-expected-error
INSERT INTO nlo_len VALUES ('abcd');

DROP TABLE nlo_len;

-- ============================================================================
-- 7. An array carries at most six dimensions
-- ============================================================================

-- begin-expected-error
-- sqlstate: 54000
-- message-like: number of array dimensions (7) exceeds the maximum allowed (6)
-- end-expected-error
SELECT array_ndims(ARRAY[[[[[[[1]]]]]]]);

-- begin-expected
-- columns: a
-- row: 6
-- end-expected
SELECT array_ndims(ARRAY[[[[[[1]]]]]]) AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT array_ndims(ARRAY[1,2,3]) AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT array_ndims(ARRAY[[1,2],[3,4]]) AS a;
