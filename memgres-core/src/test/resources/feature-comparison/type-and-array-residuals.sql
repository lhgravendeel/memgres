-- The last of the type-checking residuals: a handful of places where memgres answered in the
-- wrong type, accepted a date no calendar has, or wrote a composite back in a form that read as a
-- different value.
--
-- Nine things measured against PostgreSQL 18 and closed here:
--
--  1. A range's strictly-left/strictly-right test answered in the range type rather than in
--     boolean, and the same spelling over a point or a box failed outright while trying to read
--     the boolean back as a shape. Beside a non-range it threw a NumberFormatException that
--     reached the client as an internal XX000 instead of PostgreSQL's 42883.
--  2. timestamp had no calendar bounds at all: 4714-11-23 BC and year zero were accepted,
--     294276-12-31 23:59:59 was refused as bad spelling, and every out-of-range field said
--     "invalid input syntax" where PostgreSQL says "date/time field value out of range". The two
--     readings PostgreSQL rolls over -- 24:00:00 and a sixtieth second -- were refused too.
--  3. Every composite in an array was double-quoted, so {(1),(2)} came out as {"(1)","(2)"} --
--     an array of two different strings -- and a field with a space in it was written back
--     unquoted, which loses the field boundary. The WITH RECURSIVE ... SEARCH SET column is an
--     array of composites, so it carried the same damage.
--  4. date_part answered in text; extract answered in text.
--  5. json_object and the rest of the JSON builders answered in text or text[], and a rejected
--     escape carried no DETAIL line.
--  6. The range constructors answered in text, int8range read its bounds as int (so
--     int8range(1, 99999999999) built [1,1215752191)), and int4range narrowed a bigint bound
--     instead of refusing the call.
--  7. COLLATE was accepted over any type at all, and erased the type it was written over.
--  8. The ordered-set aggregates answered in text.
--  9. The array containment operators were reported as missing a NULL check. They are not: the
--     22004 measured came from the intarray extension, which redeclares @>, <@ and && over
--     integer[]. Stock PostgreSQL answers f or t exactly as memgres does, so the operators are
--     asserted here over text[], which intarray does not touch, and the answers are the plain
--     ones. Nothing was changed for it.
--
-- Every statement that returns more than one row sorts them.

-- setup
DROP TABLE IF EXISTS tar_t CASCADE;

CREATE TABLE tar_t (id int PRIMARY KEY, s text, v varchar(10), n int);
INSERT INTO tar_t VALUES (1,'b','x',3),(2,'a','y',4);

-- ============================================================================
-- SECTION A: << and >> over a range or a shape ask a yes/no question
-- ============================================================================

-- 1: a range strictly left of another is a boolean, not a range

-- begin-expected
-- columns: a | b | t
-- row: t | t | boolean
-- end-expected
SELECT '[1,3)'::int4range << '[5,8)'::int4range AS a,
       '[5,8)'::int4range >> '[1,3)'::int4range AS b,
       pg_typeof('[1,3)'::int4range << '[5,8)'::int4range)::text AS t;

-- 2: false is a boolean too, and the other range types behave the same

-- begin-expected
-- columns: a | b | c
-- row: f | t | t
-- end-expected
SELECT '[1,3)'::int4range << '[2,8)'::int4range AS a,
       '[1,3)'::numrange << '[5,8)'::numrange AS b,
       '[1,3)'::int8range << '[5,8)'::int8range AS c;

-- 3: a multirange answers the same way

-- begin-expected
-- columns: a | b | t
-- row: t | t | boolean
-- end-expected
SELECT '{[1,3)}'::int4multirange << '{[5,8)}'::int4multirange AS a,
       '{[5,8)}'::int4multirange >> '{[1,3)}'::int4multirange AS b,
       pg_typeof('{[1,3)}'::int4multirange << '{[5,8)}'::int4multirange)::text AS t;

-- 4: the four shapes PostgreSQL declares << over

-- begin-expected
-- columns: a | b | c | d
-- row: t | t | t | t
-- end-expected
SELECT point '(1,1)' << point '(3,3)' AS a,
       box '((0,0),(1,1))' << box '((3,3),(4,4))' AS b,
       circle '<(0,0),1>' << circle '<(5,5),1>' AS c,
       polygon '((0,0),(1,1))' << polygon '((5,5),(6,6))' AS d;

-- 5: a range beside an integer has no such operator

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: int4range << integer
-- end-expected-error
SELECT '[1,3)'::int4range << 3;

-- 6: and neither has an integer beside a range

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer << int4range
-- end-expected-error
SELECT 3 << '[1,3)'::int4range;

-- 7: on integers, bit strings and networks << keeps the meaning it always had

-- begin-expected
-- columns: a | b | c | d
-- row: 8 | 2 | t | t
-- end-expected
SELECT 1 << 3 AS a, 8 >> 2 AS b,
       inet '192.168.1.5' << inet '192.168.1.0/24' AS c,
       inet '192.168.1.0/24' >> inet '192.168.1.5' AS d;

-- 8: the neighbouring range operators were already right and stay right

-- begin-expected
-- columns: a | b | c | d
-- row: f | t | f | t
-- end-expected
SELECT '[1,3)'::int4range && '[5,8)'::int4range AS a,
       '[1,3)'::int4range &< '[5,8)'::int4range AS b,
       '[1,3)'::int4range &> '[5,8)'::int4range AS c,
       '[1,3)'::int4range -|- '[3,8)'::int4range AS d;

-- ============================================================================
-- SECTION B: the calendar timestamp and date actually hold
-- ============================================================================

-- 9: the last moment timestamp can hold, and the first

-- begin-expected
-- columns: hi | lo
-- row: 294276-12-31 23:59:59 | 4714-11-24 00:00:00 BC
-- end-expected
SELECT '294276-12-31 23:59:59'::timestamp AS hi, timestamp '4714-11-24 BC' AS lo;

-- 10: one year past the end

-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT '294277-01-01 00:00:00'::timestamp;

-- 11: and one day before the start

-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT timestamp '4714-11-23 BC';

-- 12: date has the same first day and says so in its own name

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date out of range
-- end-expected-error
SELECT date '4714-11-23 BC';

-- 13: a date that far back is a date like any other

-- begin-expected
-- columns: result
-- row: 4714-11-24 BC
-- end-expected
SELECT date '4714-11-24 BC' AS result;

-- 14: there is no year zero

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range
-- end-expected-error
SELECT timestamp '0000-01-01';

-- 15: an hour past the end of the day is a field out of range, not bad spelling

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range
-- end-expected-error
SELECT '2000-01-01 25:00:00'::timestamp;

-- 16: so is a sixty-first second

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range
-- end-expected-error
SELECT '2000-01-01 00:00:61'::timestamp;

-- 17: and a thirteenth month

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range
-- end-expected-error
SELECT '2000-13-01 00:00:00'::timestamp;

-- 18: and a day the month does not have

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range
-- end-expected-error
SELECT '2000-02-30'::timestamp;

-- 19: 24:00:00 is the following midnight and a sixtieth second the next minute

-- begin-expected
-- columns: a | b | c
-- row: 2000-01-02 00:00:00 | 2000-01-01 00:01:00 | 2000-01-02 00:00:00
-- end-expected
SELECT '2000-01-01 24:00:00'::timestamp AS a,
       '2000-01-01 00:00:60'::timestamp AS b,
       '2000-01-01 23:59:60'::timestamp AS c;

-- 20: text that is not a date at all is still a syntax error

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type timestamp
-- end-expected-error
SELECT 'garbage'::timestamp;

-- 21: the ordinary readings are untouched

-- begin-expected
-- columns: a | b | c | d
-- row: 2000-01-01 12:34:56 | 2000-01-01 12:34:56.789 | 2000-02-29 00:00:00 | 2000-01-01 00:00:00
-- end-expected
SELECT '2000-01-01 12:34:56'::timestamp AS a,
       '2000-01-01 12:34:56.789'::timestamp AS b,
       '2000-02-29 00:00:00'::timestamp AS c,
       '2000-01-01'::timestamp AS d;

-- 22: a leap day that is not one, and a zoneless literal with a named zone after it

-- begin-expected
-- columns: a | b
-- row: 2000-01-01 12:34:56 | 2000-01-01 12:34:56
-- end-expected
SELECT '2000-01-01 12:34:56 CET'::timestamp AS a,
       '2000-01-01 12:34:56+02'::timestamp AS b;

-- ============================================================================
-- SECTION C: a composite in an array is quoted by the array's own rule
-- ============================================================================

-- 23: nothing in (1) needs quoting, so nothing is quoted

-- begin-expected
-- columns: result
-- row: {(1),(2)}
-- end-expected
SELECT ARRAY[ROW(1), ROW(2)] AS result;

-- 24: the same array read as text

-- begin-expected
-- columns: result
-- row: {(1),(2)}
-- end-expected
SELECT (ARRAY[ROW(1), ROW(2)])::text AS result;

-- 25: a comma inside the composite does need quoting, at both levels

-- begin-expected
-- columns: result
-- row: {"(1,a)","(2,b)"}
-- end-expected
SELECT ARRAY[ROW(1,'a'), ROW(2,'b')] AS result;

-- 26: a composite on its own quotes the fields that need it

-- begin-expected
-- columns: a | b | c | d
-- row: (1) | (1,"a b") | (1,,x) | (t,f)
-- end-expected
SELECT ROW(1) AS a, ROW(1,'a b') AS b, ROW(1,NULL,'x') AS c, ROW(true,false) AS d;

-- 27: the SEARCH SET column is an array of composites

-- begin-expected
-- columns: n | ord
-- row: 1 | {(1)}
-- row: 2 | {(1),(2)}
-- row: 3 | {(1),(2),(3)}
-- end-expected
WITH RECURSIVE r(n) AS (
    SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3
) SEARCH DEPTH FIRST BY n SET ord
SELECT n, ord FROM r ORDER BY n;

-- 28: plain text arrays are quoted exactly as before

-- begin-expected
-- columns: a | b
-- row: {a,b} | {"a b","c,d"}
-- end-expected
SELECT ARRAY['a','b'] AS a, ARRAY['a b','c,d'] AS b;

-- ============================================================================
-- SECTION D: array containment and overlap over text[], which no extension redeclares
-- ============================================================================

-- 29: a NULL element never matches, and never raises

-- begin-expected
-- columns: a | b | c
-- row: f | f | t
-- end-expected
SELECT ARRAY['a',NULL]::text[] @> ARRAY[NULL]::text[] AS a,
       ARRAY[NULL]::text[] <@ ARRAY['a',NULL]::text[] AS b,
       ARRAY['a',NULL]::text[] @> ARRAY['a']::text[] AS c;

-- 30: the same for overlap

-- begin-expected
-- columns: a | b | c
-- row: f | t | f
-- end-expected
SELECT ARRAY[NULL]::text[] && ARRAY[NULL]::text[] AS a,
       ARRAY['a',NULL]::text[] && ARRAY['a']::text[] AS b,
       ARRAY[NULL,'b']::text[] && ARRAY['a',NULL]::text[] AS c;

-- ============================================================================
-- SECTION E: the JSON builders answer in json or jsonb
-- ============================================================================

-- 31: json_object is a json document, not the array it was written from

-- begin-expected
-- columns: a | b
-- row: json | json
-- end-expected
SELECT pg_typeof(json_object('{a,1,b,2}'))::text AS a,
       pg_typeof(json_object('{a,b}', '{1,2}'))::text AS b;

-- 32: and the value it holds

-- begin-expected
-- columns: result
-- row: {"a" : "1", "b" : "2"}
-- end-expected
SELECT json_object('{a,1,b,2}')::text AS result;

-- 33: the builders and the converters answer in the flavour their name says

-- begin-expected
-- columns: a | b | c | d
-- row: json | jsonb | json | jsonb
-- end-expected
SELECT pg_typeof(json_build_object('a',1))::text AS a,
       pg_typeof(jsonb_build_object('a',1))::text AS b,
       pg_typeof(to_json(1))::text AS c,
       pg_typeof(to_jsonb(1))::text AS d;

-- 34: so do the aggregates and the array builder

-- begin-expected
-- columns: a | b | c
-- row: json | jsonb | json
-- end-expected
SELECT pg_typeof(json_agg(x))::text AS a,
       pg_typeof(jsonb_agg(x))::text AS b,
       pg_typeof(json_build_array(1))::text AS c
FROM (VALUES (1)) v(x);

-- 35: a rejected escape says which escape it was

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- detail-like: Escape sequence "\q" is invalid.
-- end-expected-error
SELECT ('"' || chr(92) || 'q"')::jsonb;

-- 36: a \u with too few hexadecimal digits after it

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- detail-like: must be followed by four hexadecimal digits.
-- end-expected-error
SELECT ('"' || chr(92) || 'u12"')::jsonb;

-- 37: half a surrogate pair

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- detail-like: Unicode low surrogate must follow a high surrogate.
-- end-expected-error
SELECT ('"' || chr(92) || 'ud834"')::jsonb;

-- 38: a NUL has no character to be converted to

-- begin-expected-error
-- sqlstate: 22P05
-- message-like: unsupported Unicode escape sequence
-- detail-like: cannot be converted to text.
-- end-expected-error
SELECT ('"' || chr(92) || 'u0000"')::jsonb;

-- 39: the escapes JSON does have still work

-- begin-expected
-- columns: a | b
-- row: {"a": "b\\c"} | {"a": "x"}
-- end-expected
SELECT ('{"a":"b' || chr(92) || chr(92) || 'c"}')::jsonb::text AS a,
       ('{"a":"' || chr(92) || 'u0078"}')::jsonb::text AS b;

-- ============================================================================
-- SECTION F: date_part and extract answer in numbers
-- ============================================================================

-- 40: the declared type of each

-- begin-expected
-- columns: a | b
-- row: double precision | numeric
-- end-expected
SELECT pg_typeof(date_part('year', timestamp '2020-05-17 11:22:33'))::text AS a,
       pg_typeof(EXTRACT(YEAR FROM timestamp '2020-05-17 11:22:33'))::text AS b;

-- 41: dividing one is a real division, not an integer one

-- begin-expected
-- columns: a | b
-- row: 288.57142857142856 | 2.5
-- end-expected
SELECT date_part('year', timestamp '2020-05-17 11:22:33') / 7 AS a,
       date_part('month', timestamp '2020-05-17 11:22:33') / 2 AS b;

-- 42: the units the finding named all answer with the same value on both

-- begin-expected
-- columns: a | b | c | d
-- row: 2020 | 1589714553 | 33000 | 20
-- end-expected
SELECT date_part('y', timestamp '2020-05-17 11:22:33') AS a,
       date_part('epoch', timestamp '2020-05-17 11:22:33') AS b,
       date_part('ms', timestamp '2020-05-17 11:22:33') AS c,
       date_part('weeks', timestamp '2020-05-17 11:22:33') AS d;

-- 43: over a date, a time and an interval too

-- begin-expected
-- columns: a | b | c
-- row: 2020 | 11 | 2
-- end-expected
SELECT date_part('year', date '2020-05-17') AS a,
       date_part('hour', time '11:22:33') AS b,
       date_part('year', interval '2 years 3 months') AS c;

-- ============================================================================
-- SECTION G: the range constructors answer in their own range type
-- ============================================================================

-- 44: each constructor's declared type

-- begin-expected
-- columns: a | b | c | d
-- row: int4range | int8range | numrange | daterange
-- end-expected
SELECT pg_typeof(int4range(1,2))::text AS a,
       pg_typeof(int8range(1,2))::text AS b,
       pg_typeof(numrange(1,2))::text AS c,
       pg_typeof(daterange(date '2020-01-01', date '2020-02-01'))::text AS d;

-- 45: int8range holds a bigint bound whole

-- begin-expected
-- columns: a | b | c
-- row: [1,99999999999) | [1,9223372036854775807) | [1,4)
-- end-expected
SELECT int8range(1, 99999999999) AS a,
       int8range(1, 9223372036854775807) AS b,
       int8range(1,3,'[]') AS c;

-- 46: int4range has no overload for a bigint bound

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function int4range(integer, bigint) does not exist
-- end-expected-error
SELECT int4range(1, 99999999999);

-- 47: nor for one just past the end of int

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function int4range(integer, bigint) does not exist
-- end-expected-error
SELECT int4range(1, 2147483648);

-- 48: nor for a numeric one

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function int4range(integer, numeric) does not exist
-- end-expected-error
SELECT int4range(1, 2.0);

-- 49: an integer bound is what it takes, and a smallint promotes to one

-- begin-expected
-- columns: a | b | c | d
-- row: [1,2) | [1,2) | [1,11) | (,10)
-- end-expected
SELECT int4range(1,2) AS a, int4range(1, 2::smallint) AS b,
       int4range(1,10,'[]') AS c, int4range(NULL,10) AS d;

-- 50: lower and upper answer in the type the range is built over

-- begin-expected
-- columns: a | b | c | d
-- row: integer | integer | numeric | date
-- end-expected
SELECT pg_typeof(lower('[1,5)'::int4range))::text AS a,
       pg_typeof(upper(int4range(1,5)))::text AS b,
       pg_typeof(lower('[1.5,5)'::numrange))::text AS c,
       pg_typeof(lower('[2020-01-01,2020-02-01)'::daterange))::text AS d;

-- 51: lower and upper over a string are the string functions they always were

-- begin-expected
-- columns: a | b | c
-- row: abcdef | ABCDEF | text
-- end-expected
SELECT lower('abcDEF') AS a, upper('abcDEF') AS b,
       pg_typeof(lower('abcDEF'))::text AS c;

-- 52: a constructed range is a range, so the range operators take it

-- begin-expected
-- columns: a | b | c
-- row: t | t | 1
-- end-expected
SELECT int4range(1,2) @> 1 AS a,
       int4range(1,2) = '[1,2)'::int4range AS b,
       lower(int4range(1,5)) AS c;

-- ============================================================================
-- SECTION H: COLLATE only over a type that carries a collation
-- ============================================================================

-- 53: an integer has none

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type integer
-- end-expected-error
SELECT 1 COLLATE "C";

-- 54: nor has the integer an expression computes

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type integer
-- end-expected-error
SELECT (1 + 1) COLLATE "C";

-- 55: nor a column of one

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type integer
-- end-expected-error
SELECT n COLLATE "C" FROM tar_t;

-- 56: nor a date

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type date
-- end-expected-error
SELECT current_date COLLATE "C";

-- 57: nor a numeric, and the type named is the written one

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type numeric
-- end-expected-error
SELECT 1.5 COLLATE "C";

-- 58: nor a boolean

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type boolean
-- end-expected-error
SELECT true COLLATE "C";

-- 59: nor a timestamp, named the way PostgreSQL spells it

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type timestamp without time zone
-- end-expected-error
SELECT '2020-01-01'::timestamp COLLATE "C";

-- 60: the string types carry one and are untouched

-- begin-expected
-- columns: a | b | c | d
-- row: a | a | ab | f
-- end-expected
SELECT 'a' COLLATE "C" AS a, 'a'::text COLLATE "C" AS b,
       ('a' || 'b') COLLATE "C" AS c, 'b' < 'a' COLLATE "C" AS d;

-- 61: so do a text column, a varchar one and what a string function returns

-- begin-expected
-- columns: s | v | u
-- row: a | y | A
-- row: b | x | B
-- end-expected
SELECT s COLLATE "C" AS s, v COLLATE "C" AS v, upper(s) COLLATE "C" AS u
FROM tar_t ORDER BY s;

-- 62: and an ORDER BY over one still sorts

-- begin-expected
-- columns: s
-- row: a
-- row: b
-- end-expected
SELECT s FROM tar_t ORDER BY s COLLATE "C";

-- 63: a NULL has no type to refuse

-- begin-expected
-- columns: result
-- row: NULL
-- end-expected
SELECT NULL COLLATE "C" AS result;

-- ============================================================================
-- SECTION I: the ordered-set aggregates
-- ============================================================================

-- 64: the value each answers with

-- begin-expected
-- columns: a | b | c
-- row: 1.5 | 1 | 1
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) AS a,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS b,
       mode() WITHIN GROUP (ORDER BY x) AS c
FROM (VALUES (1),(2)) v(x);

-- 65: the type each answers in

-- begin-expected
-- columns: a | b | c
-- row: double precision | integer | integer
-- end-expected
SELECT pg_typeof(percentile_cont(0.5) WITHIN GROUP (ORDER BY x))::text AS a,
       pg_typeof(percentile_disc(0.5) WITHIN GROUP (ORDER BY x))::text AS b,
       pg_typeof(mode() WITHIN GROUP (ORDER BY x))::text AS c
FROM (VALUES (1),(2)) v(x);

-- 66: the two that give back one of the values they sorted follow that value's type

-- begin-expected
-- columns: a | b | c
-- row: numeric | text | date
-- end-expected
SELECT pg_typeof(percentile_disc(0.5) WITHIN GROUP (ORDER BY x))::text AS a,
       pg_typeof(mode() WITHIN GROUP (ORDER BY y))::text AS b,
       pg_typeof(mode() WITHIN GROUP (ORDER BY z))::text AS c
FROM (VALUES (1.5,'a',date '2020-01-01')) v(x,y,z);

-- 67: the hypothetical-set four have types of their own

-- begin-expected
-- columns: a | b | c | d
-- row: bigint | bigint | double precision | double precision
-- end-expected
SELECT pg_typeof(rank(1) WITHIN GROUP (ORDER BY x))::text AS a,
       pg_typeof(dense_rank(1) WITHIN GROUP (ORDER BY x))::text AS b,
       pg_typeof(percent_rank(1) WITHIN GROUP (ORDER BY x))::text AS c,
       pg_typeof(cume_dist(1) WITHIN GROUP (ORDER BY x))::text AS d
FROM (VALUES (1),(2)) v(x);

-- 68: an array of fractions gives an array of results

-- begin-expected
-- columns: result
-- row: {1.25,1.5}
-- end-expected
SELECT percentile_cont(ARRAY[0.25,0.5]) WITHIN GROUP (ORDER BY x) AS result
FROM (VALUES (1),(2)) v(x);

-- cleanup
DROP TABLE IF EXISTS tar_t CASCADE;
