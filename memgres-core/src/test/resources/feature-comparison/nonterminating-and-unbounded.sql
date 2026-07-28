-- Operations that either never returned or allocated without bound.
--
-- rpad grew its result with a loop that an empty fill string never advanced, so any call whose
-- fill happened to evaluate to '' wedged the connection permanently. The pad and repeat family,
-- the bit length modifier, factorial and numeric casts all built results PostgreSQL refuses
-- outright, exhausting the heap instead of raising. generate_series over a wide integer range
-- filled its virtual table one row at a time, which is quadratic, so five million rows never
-- finished.
--
-- Covers: empty and computed pad fills, the single-allocation ceiling on lpad/rpad/repeat, the
-- bit and varbit length modifier bounds, numeric's digit limits reached through factorial and
-- through casts, and generate_series at scale and at the edge of bigint.

-- An empty fill has nothing to pad with, so the input comes back unchanged
SELECT rpad('abc', 10, '');
SELECT lpad('abc', 10, '');
SELECT rpad('abc', 2, '');
SELECT lpad('abc', 2, '');

-- The same holds for any expression that evaluates to an empty fill
SELECT rpad('abc', 10, substr('a', 2));
SELECT rpad('abc', 10, '' || '');
SELECT rpad('abc', 10, repeat('z', 0));
SELECT rpad('abc', 10, left('x', 0));
SELECT lpad('abc', 10, left('x', 0));

-- Nothing gets built, so an outsized length with an empty fill is not a length error
SELECT rpad('abc', 400000000, '');
SELECT lpad('abc', 400000000, '');

-- The realistic trigger: the separator arrives from a column
CREATE TABLE nto_pad (s text, n int, sep text);
INSERT INTO nto_pad VALUES ('abc', 10, ''), ('abc', 10, '-');
SELECT rpad(s, n, sep) FROM nto_pad ORDER BY sep;
SELECT lpad(s, n, sep) FROM nto_pad ORDER BY sep;
SELECT rpad(s, 400000000, sep) FROM nto_pad ORDER BY sep;
DROP TABLE nto_pad;

-- Ordinary padding is unaffected
SELECT rpad('abc', 6, 'xy');
SELECT lpad('abc', 6, 'xy');
SELECT rpad('abc', 2, 'x');
SELECT lpad('abc', 2, 'x');
SELECT lpad('abc', 3, 'x');
SELECT rpad('abc', 0, 'x');
SELECT rpad('abc', -1, 'x');
SELECT lpad('abc', -1, 'x');
SELECT rpad('abc', 8);
SELECT lpad('abc', 8);
SELECT rpad('', 5, 'ab');
SELECT lpad('', 5, 'ab');
SELECT rpad('ab', 6, 'cd');
SELECT rpad('abc', 10, NULL);

-- A result wider than one allocation is refused rather than attempted
SELECT length(lpad('abc', 400000000, 'x'));
SELECT length(rpad('abc', 400000000, 'x'));
SELECT length(lpad('abc', 1500000000, 'x'));
SELECT length(rpad('abc', 2000000000, 'x'));
SELECT lpad('abcdefghij', 400000000);
SELECT length(repeat('ab', 1500000000));
SELECT length(repeat('ab', 600000000));
SELECT length(repeat('abcdefghij', 200000000));
SELECT length(repeat(chr(233), 600000000));

-- Sizes that do fit still work
SELECT length(repeat('ab', 1000));
SELECT length(lpad('abc', 100000, 'x'));
SELECT repeat('ab', 1);
SELECT repeat('', 5);
SELECT translate('abc', 'abc', '');
SELECT regexp_replace('abc', '', 'X');

-- A bit string's length modifier is bounded before anything is built
SELECT '0'::bit(200000000);
SELECT '0'::bit(83886081);
SELECT '0'::varbit(200000000);
SELECT '0'::bit varying(200000000);
SELECT '10'::varbit(83886081);
SELECT 42::bit(200000000);
SELECT '0'::bit(99999999999);
SELECT '0'::bit(0);

-- Modifiers within range behave as before
SELECT '0'::bit(1);
SELECT '101'::bit(8);
SELECT '101'::varbit(2);
SELECT B'101'::bit(5);
SELECT 42::bit(8);

-- factorial stops at the widest result numeric can hold
SELECT factorial(50000);
SELECT factorial(32178);
SELECT factorial(-1);
SELECT factorial(20);
SELECT factorial(0);
SELECT factorial(5000) IS NOT NULL;
SELECT length(factorial(1000)::text);

-- A cast cannot produce a numeric wider than the format allows
SELECT '1e200000'::numeric;
SELECT '1e200000'::numeric + 1;
SELECT '1e131072'::numeric + 1;
SELECT (-1e200000)::numeric;
SELECT '1e200000'::decimal;
SELECT (1e-16384)::numeric;
SELECT '1e131071'::numeric IS NOT NULL;
SELECT (1e-16383)::numeric IS NOT NULL;

-- Ordinary numeric casts are untouched
SELECT '123.456'::numeric;
SELECT '1e-30'::numeric;
SELECT '  12  '::numeric;
SELECT 'NaN'::numeric;
SELECT 1e20::numeric;
SELECT 1.5::numeric;
SELECT (-0.5)::numeric;
SELECT 1000000::numeric(10,2);
SELECT 12345.678::numeric(8,3);
SELECT '1e100'::numeric + 1;
SELECT 2::numeric ^ 10;

-- A wide integer series finishes
SELECT count(*) FROM generate_series(1, 5000000);
SELECT max(g), min(g), count(*) FROM generate_series(1, 3000000) g;
SELECT max(g) FROM generate_series(1, 1000000) g;
SELECT count(*) FROM generate_series(1, 200000);
SELECT count(*) FROM generate_series(1.0, 200000.0, 1);

-- A series that runs to the edge of bigint terminates instead of wrapping
SELECT * FROM generate_series(9223372036854775805, 9223372036854775807);
SELECT * FROM generate_series(-9223372036854775807, -9223372036854775805);
SELECT * FROM generate_series(9223372036854775805, 9223372036854775807, 2);

-- Small series keep their existing shape
SELECT * FROM generate_series(1, 3);
SELECT * FROM generate_series(1, 3) WITH ORDINALITY;
SELECT * FROM generate_series(5, 1, -2);
SELECT count(*) FROM generate_series(1, 0);
SELECT sum(g) FROM generate_series(1, 100) g;
SELECT * FROM generate_series(1.0, 2.0, 0.5);
SELECT count(*) FROM generate_series('2020-01-01'::date, '2020-01-05'::date, '1 day');
SELECT count(*) FROM generate_series(1, 3, 0);
SELECT count(*) FROM unnest(ARRAY[1,2,3]);
SELECT count(*) FROM string_to_table('a,b,c', ',');
