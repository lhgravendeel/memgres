-- Operator resolution from the declared types, and the declared type of the result.
--
-- Covers: the cross-family comparisons PostgreSQL has no operator for (text against date,
-- timestamp, time, interval, uuid, inet and bytea; date and timestamp against integer), reached
-- through BETWEEN, IN, = ANY and NULLIF as well as through a plain comparison, and for a real
-- table column as well as for a literal; the arithmetic text and booleans do not carry, including
-- %, ^ and @; the UNION/INTERSECT/EXCEPT type unification and the SQL type names its message
-- prints; the date and time arithmetic where one side is an untyped literal, in both directions;
-- the array mutators handed an unknown array literal; the range that has no || of its own; and the
-- declared result type of the date/time, network, geometric, array, range, jsonb, text-search,
-- regex and bit operators, of pg_typeof, and of GREATEST over an untyped literal.

-- An operator is resolved from the declared types, so text compares only with text
SELECT '2020-01-01'::text = date '2020-01-01';
SELECT date '2020-01-01' = '2020-01-01'::text;
SELECT '2020-01-01'::text < date '2020-01-02';
SELECT '2020-01-01'::varchar = date '2020-01-01';
SELECT '2020-01-01'::text = timestamp '2020-01-01';
SELECT '10:00'::text = time '10:00';
SELECT '1 day'::text = interval '1 day';
SELECT '00000000-0000-0000-0000-000000000000'::text = '00000000-0000-0000-0000-000000000000'::uuid;
SELECT '00000000-0000-0000-0000-000000000000'::uuid = '00000000-0000-0000-0000-000000000000'::text;
SELECT '00000000-0000-0000-0000-000000000000'::uuid < 'ffffffff-0000-0000-0000-000000000000'::text;
SELECT '10.0.0.1'::text = '10.0.0.1'::inet;
SELECT 'ab'::text = 'ab'::bytea;
SELECT date '2020-01-01' = 1;
SELECT date '2020-01-01' < 1;
SELECT timestamp '2020-01-01' = 1;

-- BETWEEN, IN, = ANY and NULLIF resolve the same operators
SELECT '5'::text BETWEEN 1 AND 9;
SELECT 5 BETWEEN '1'::text AND '9'::text;
SELECT '5'::text IN (1,2,5);
SELECT 5 IN ('1'::text,'5'::text);
SELECT '5'::text = ANY(ARRAY[1,2,5]);
SELECT NULLIF(1, '2'::text);
SELECT NULLIF(1, '1'::text);
SELECT NULLIF('1'::text, 1);
SELECT NULLIF(date '2020-01-01', '2020-01-01'::text);
SELECT NULLIF(1, 'a'::text);

-- Text carries no arithmetic of any kind
SELECT '10'::text % 3;
SELECT '10'::varchar % 3;
SELECT '10'::text % '3'::text;
SELECT 10 % '3'::text;
SELECT '10'::text ^ 2;
SELECT '10'::text - '5'::text;
SELECT @ '-10'::text;

-- The rule reads a real table column's declared type, not only a literal's
CREATE TABLE xta_w2(t text, i int);
INSERT INTO xta_w2 VALUES ('5', 5);
SELECT count(*) FROM xta_w2 WHERE t = i;
SELECT count(*) FROM xta_w2 WHERE t BETWEEN 1 AND 9;
SELECT t % i FROM xta_w2;
CREATE TABLE xta_w1(t text, d date);
INSERT INTO xta_w1 VALUES ('2020-01-01', date '2020-01-01');
SELECT count(*) FROM xta_w1 WHERE t = d;
CREATE TABLE xta_w4(u uuid, t text);
INSERT INTO xta_w4 VALUES ('00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000000');
SELECT count(*) FROM xta_w4 WHERE u = t;

-- What still resolves: an untyped literal takes the other side's type
SELECT '5' = 5;
SELECT 'a'::text = 'a'::text;
SELECT 1 = 1.0;
SELECT '2020-01-01'::date = '2020-01-01'::date;
SELECT 5 BETWEEN 1 AND 9;
SELECT 10 % 3;
SELECT @ -10;
SELECT NULLIF(1, 1);
SELECT NULLIF('a', 'a');
SELECT NULL::int = 1;
SELECT NULL::date = date '2020-01-01';
SELECT count(*)::int FROM xta_w2 WHERE t = '5';
SELECT count(*)::int FROM xta_w2 WHERE i BETWEEN 0 AND 9 AND i IN (5,6);
SELECT count(*)::int FROM (SELECT i AS x FROM xta_w2) s WHERE s.x = 5;
SELECT count(*)::int FROM (SELECT t AS x FROM xta_w2) s WHERE s.x = '5';
SELECT (SELECT rn FROM (SELECT 1 AS rn) sub) >= 1;
SELECT count(*)::int FROM (SELECT row_number() OVER () AS rn FROM (SELECT 1) t) sub WHERE sub.rn >= 1;
DROP TABLE xta_w4;
DROP TABLE xta_w1;
DROP TABLE xta_w2;

-- Two branches of a set operation have to settle on one type
SELECT 1 UNION SELECT 'a'::text;
SELECT 'a'::text UNION SELECT 1;
SELECT 1 INTERSECT SELECT 'a'::text;
SELECT 1 EXCEPT SELECT 'a'::text;
SELECT date '2020-01-01' UNION SELECT 1;
SELECT 1 UNION SELECT 2;
SELECT 1 UNION SELECT 1.5;
SELECT 'a' UNION SELECT 'b'::text;

-- Date and time arithmetic with an untyped literal, resolved in both directions
SELECT date '2020-01-01' + '1';
SELECT date '2020-01-01' + '1 day';
SELECT '1 day' + date '2020-01-01';
SELECT date '2020-01-01' - '1';
SELECT date '2020-01-02' - '2020-01-01';
SELECT timestamp '2020-01-02' + '1 day';
SELECT timestamp '2020-01-02' - '1 day';
SELECT time '10:00' + '1 hour';
SELECT '1 hour' + time '10:00';
SELECT time '10:00' - '1 hour';
SELECT time with time zone '10:00+02' + '1 hour';
SELECT interval '1 day' * '2';
SELECT interval '1 day' / '2';
SELECT time with time zone '10:00+02';
SELECT time without time zone '10:00';
CREATE TABLE xta_dtt(d date);
INSERT INTO xta_dtt VALUES (date '2020-01-01');
SELECT d + '1' FROM xta_dtt;
SELECT d - '1' FROM xta_dtt;
SELECT d + 1 FROM xta_dtt;
SELECT d - 1 FROM xta_dtt;
DROP TABLE xta_dtt;

-- The array mutators read an unknown array literal as an array
SELECT array_append('{1,2}', 3);
SELECT array_append('{a,b}', 'c');
SELECT array_append('{1,2}', NULL);
SELECT array_prepend(0, '{1,2}');
SELECT array_cat('{1,2}', '{3}');
SELECT array_cat('{1,2}'::int[], '{3}');
SELECT array_cat('{1,2}', '{3}'::int[]);
SELECT array_remove('{1,2,3}', 2);
SELECT array_remove('{a,b,c}', 'b');
SELECT array_replace('{1,2,3}', 2, 9);
SELECT array_positions('{1,2,3}', 2);
SELECT array_position('{1,2,3}', 2);
SELECT * FROM unnest('{1,2,3}');
SELECT array_append(ARRAY[1,2], 3);
SELECT array_cat(ARRAY[1,2], ARRAY[3]);
SELECT array_replace('{1,2,3}'::int[], 2, 9);
SELECT array_length(ARRAY[1,2,3], 1);
SELECT '{1,2}'::int[] || 3;

-- A range has no || of its own, so PostgreSQL resolves anynonarray || text
SELECT '[1.0,3.0)'::numrange || '[2.0,5.0)';
SELECT '[1,3)'::int4range || '[5,7)';
SELECT 'x' || '[1,3)'::int4range;
SELECT '[1,3)'::int4range || 'x';
SELECT '[1.0,3.0)'::numrange || '[2.0,5.0)'::numrange;
SELECT '[1,3)'::int4range || '[5,7)'::int4range;

-- A tsquery beside an untyped literal reads it as the document, never as another query
SELECT 'a'::tsquery @@ 'a b';
SELECT 'cat'::tsquery @@ 'cat dog';

-- Two paths are joined; a closed path has no free end and the join is NULL
SELECT '((0,0),(1,1))'::path + '((2,2))';
SELECT '[(0,0),(1,1)]'::path + '(1,1)'::point;

-- IS UNKNOWN takes a boolean; an untyped literal is coerced to one and fails on its input
SELECT 'a'::text IS UNKNOWN;
SELECT 'a'::text IS NOT UNKNOWN;
SELECT 'a' IS UNKNOWN;
SELECT true IS UNKNOWN;
SELECT NULL::boolean IS UNKNOWN;
SELECT (1 = 1) IS NOT UNKNOWN;

-- GREATEST and LEAST settle on one type before they compare anything
SELECT pg_typeof(GREATEST('10', 9));
SELECT GREATEST('10', 9);
SELECT LEAST('10', 9);

-- numeric and float8 read the non-decimal integer forms int4 already read
SELECT '0x2a'::numeric;
SELECT '0o52'::numeric;
SELECT '0b101010'::numeric;
SELECT '1_000'::numeric;
SELECT '0x2a'::float8;
SELECT '1.5'::numeric;
SELECT 'zz'::numeric;

-- The declared result type, which is what a client decodes the value by
SELECT date '2020-01-01' + 1;
SELECT date '2020-01-01' - 1;
SELECT 1 + date '2020-01-01';
SELECT date '2020-01-02' - date '2020-01-01';
SELECT date '2020-01-01' + interval '1 day';
SELECT timestamp '2020-01-01' + interval '1 day';
SELECT timestamp '2020-01-02' - timestamp '2020-01-01';
SELECT interval '1 day' + interval '1 hour';
SELECT time '10:00' + interval '1 hour';
SELECT interval '1 day' * 2;
SELECT interval '1 day' / 2;
SELECT interval '1 day' * 2.5;
SELECT pg_typeof(date '2020-01-01' + 1);
SELECT pg_typeof(timestamp '2020-01-01' + interval '1 day');
SELECT pg_typeof(interval '1 day' * 2);
SELECT pg_typeof('255.255.255.255'::inet - '0.0.0.0'::inet);
SELECT '255.255.255.255'::inet - '0.0.0.0'::inet;

-- The predicates that answer yes or no, and the operators that answer in their own type
SELECT pg_typeof('10.0.0.1'::inet & '255.0.0.0'::inet);
SELECT pg_typeof('192.168.1.1'::inet | '0.0.0.255');
SELECT pg_typeof(~ '192.168.1.1'::inet);
SELECT pg_typeof('10.0.0.1'::inet << '10.0.0.0/8'::inet);
SELECT pg_typeof('10.0.0.1'::inet <<= '10.0.0.0/8'::inet);
SELECT pg_typeof('10.0.0.0/8'::inet >> '10.0.0.1'::inet);
SELECT pg_typeof('10.0.0.0/8'::inet >>= '10.0.0.1'::inet);
SELECT pg_typeof('10.0.0.0/8'::inet && '10.0.0.1'::inet);
SELECT pg_typeof('((0,0),(2,2))'::box @> '(1,1)'::point);
SELECT pg_typeof('(0,0)'::point <-> '(3,4)'::point);
SELECT pg_typeof('(1,2)'::point ~= '(1,2)'::point);
SELECT pg_typeof('(1,2)'::point + '(3,4)');
SELECT pg_typeof('<(0,0),5>'::circle && '<(1,1),5>');
SELECT pg_typeof('[(0,0),(2,2)]'::lseg ?# '[(0,2),(2,0)]');
SELECT pg_typeof(ARRAY[1,2] || ARRAY[3]);
SELECT pg_typeof(ARRAY[1,2] @> ARRAY[1]);
SELECT pg_typeof(ARRAY[1,2] && ARRAY[1]);
SELECT pg_typeof(array_append(ARRAY[1,2],3));
SELECT pg_typeof(array_cat(ARRAY[1,2],ARRAY[3]));
SELECT pg_typeof(array_replace('{1,2,3}'::int[],2,9));
SELECT pg_typeof(array_fill(1, ARRAY[3]));
SELECT pg_typeof(string_to_array('a,b',','));
SELECT pg_typeof('[1,10)'::int4range @> 5);
SELECT pg_typeof('[1,3)'::int4range && '[2,5)'::int4range);
SELECT pg_typeof('[1,3)'::int4range + '[2,5)'::int4range);
SELECT pg_typeof('{"a":1}'::jsonb ?| ARRAY['a']);
SELECT pg_typeof('{"a":1}'::jsonb @> '{"a":1}');
SELECT pg_typeof('{"a":1}'::jsonb -> 'a');
SELECT pg_typeof('{"a":1}'::jsonb ->> 'a');
SELECT pg_typeof('{"a":1,"b":2}'::jsonb - 'a');
SELECT pg_typeof(to_tsvector('simple','a b') @@ 'a'::tsquery);
SELECT pg_typeof('a'::tsvector || 'b');
SELECT pg_typeof('abc' ~ 'a');
SELECT pg_typeof('abc' !~ 'z');
SELECT pg_typeof('abc' LIKE 'a%');
SELECT pg_typeof(B'101' & '111');
SELECT pg_typeof(2 ^ 10);
SELECT pg_typeof(@ -10);
SELECT pg_typeof(1);
SELECT pg_typeof('a');
SELECT * FROM unnest('{1,2,3}'::int[]);
