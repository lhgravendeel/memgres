-- ============================================================================
-- Feature Comparison: operators and casts resolve from declared types
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- An operator is chosen from the types the query declares, not from the shape
-- of the values that turn up at run time. Reading the values instead answered
-- '5'::text = 5 with true, gave '2.5'::int a value at all, and read the literal
-- beside an inet as a point -- so a query that is an error in PostgreSQL came
-- back with a plausible number here, which is the worst way to be wrong.
-- ============================================================================

-- ============================================================================
-- 1. There is no operator across two type families
-- ============================================================================
SELECT '5'::text = 5 AS a;
SELECT '5'::text > 4 AS a;
SELECT 1::int = true AS a;
SELECT '10'::text + 5 AS a;
SELECT '3'::varchar * 2 AS a;
SELECT '7'::text / 2 AS a;
SELECT 5 - '2'::text AS a;
SELECT '1'::json > '2'::json AS a;

-- ... but a literal written without a type is unknown, and resolves against the
-- other side, so these keep working
SELECT ('5' = 5)::text AS a;
SELECT ('5'::text = '5')::text AS a;
SELECT (1 = 1.0)::text AS a;
SELECT (1::int8 = 1::int4)::text AS a;
SELECT (1 + 1)::text AS a;
SELECT ('2024-01-01'::date + 1)::text AS a;
SELECT (interval '1 day' + interval '1 hour')::text AS a;
SELECT ('1'::jsonb = '1'::jsonb)::text AS a;

-- ============================================================================
-- 2. An untyped literal is read as the type on the other side
-- ============================================================================
SELECT ('192.168.1.1'::inet && '192.168.1.0/24')::text AS a;
SELECT ('192.168.1.1'::inet << '192.168.1.0/24')::text AS a;
SELECT ('10.0.0.0/8'::cidr >> '10.1.2.3')::text AS a;
SELECT ('10.1.2.5'::inet - '10.1.2.3')::text AS a;
SELECT (interval '1 day' + '1 hour')::text AS a;

-- reading it as that type is also what produces PostgreSQL's error, rather than
-- a quiet true
SELECT '((0,0),(2,2))'::box @> '(1,1)' AS a;
SELECT '<(0,0),5>'::circle @> '(1,1)' AS a;
SELECT '[2020-01-01,2020-06-01)'::daterange @> '2020-03-01' AS a;

-- ============================================================================
-- 3. Integer input is what PostgreSQL's input function accepts
-- ============================================================================
SELECT '2.5'::integer AS a;
SELECT '2.9'::bigint AS a;
SELECT 'null'::integer AS a;
SELECT ''::integer AS a;
SELECT '0x2a'::int::text AS a;
SELECT '0o52'::int::text AS a;
SELECT '0b101010'::int::text AS a;
SELECT '1_000'::int::text AS a;
SELECT '1_0_0'::int::text AS a;
SELECT '4_2'::smallint::text AS a;
SELECT '0X2A'::bigint::text AS a;
SELECT ' 42 '::integer::text AS a;
SELECT '+42'::integer::text AS a;

-- ============================================================================
-- 4. Boolean input takes any prefix that names exactly one word
-- ============================================================================
SELECT 't'::boolean::text AS a;
SELECT 'tr'::boolean::text AS a;
SELECT 'tru'::boolean::text AS a;
SELECT 'true'::boolean::text AS a;
SELECT 'y'::boolean::text AS a;
SELECT 'ye'::boolean::text AS a;
SELECT 'yes'::boolean::text AS a;
SELECT 'on'::boolean::text AS a;
SELECT 'f'::boolean::text AS a;
SELECT 'fa'::boolean::text AS a;
SELECT 'fals'::boolean::text AS a;
SELECT 'false'::boolean::text AS a;
SELECT 'n'::boolean::text AS a;
SELECT 'no'::boolean::text AS a;
SELECT 'of'::boolean::text AS a;
SELECT 'off'::boolean::text AS a;
SELECT '1'::boolean::text AS a;
SELECT '0'::boolean::text AS a;
SELECT ' TRUE '::boolean::text AS a;
-- "o" starts both "on" and "off", so it names neither
SELECT 'o'::boolean AS a;
SELECT 'tx'::boolean AS a;
SELECT 'null'::boolean AS a;

-- ============================================================================
-- 5. Only integer converts to boolean
-- ============================================================================
SELECT (0.5::numeric)::boolean AS a;
SELECT (1.5::float8)::boolean AS a;
SELECT (1::int8)::boolean AS a;
SELECT (1::int2)::boolean AS a;
SELECT (1::int)::boolean::text AS a;
SELECT (true)::int::text AS a;

-- ============================================================================
-- 6. A float rounds half to even; a numeric rounds away from zero
-- ============================================================================
SELECT (0.5::float8)::int::text AS a;
SELECT (1.5::float8)::int::text AS a;
SELECT (1.6::float8)::int::text AS a;
SELECT (2.5::float8)::int::text AS a;
SELECT (-1.5::float8)::int::text AS a;
SELECT (0.5::numeric)::int::text AS a;
SELECT (1.5::numeric)::int::text AS a;
SELECT (2.5::numeric)::int::text AS a;
SELECT (-0.5::numeric)::int::text AS a;
-- and a value with no integer to land on is out of range, not a saturated bound
SELECT (1e30::float8)::bigint AS a;
SELECT ('Infinity'::float8)::bigint AS a;
SELECT ('NaN'::float8)::bigint AS a;
SELECT (9.3e18::float8)::bigint AS a;
SELECT (2147483647.6::float8)::int AS a;
SELECT (2147483647.4::float8)::int::text AS a;

-- ============================================================================
-- 7. JSON input is parsed, not bracket-counted
-- ============================================================================
SELECT '{"a": 1} trailing'::json AS a;
SELECT '{a: 1}'::json AS a;
SELECT '"abc'::json AS a;
SELECT '007'::json AS a;
SELECT '+1'::json AS a;
SELECT '1.'::json AS a;
SELECT '.5'::json AS a;
SELECT '[1,2] [3]'::jsonb AS a;
SELECT '{"a":1,}'::json AS a;
-- the JSON it still accepts
SELECT '1e5'::json::text AS a;
SELECT '-0.5'::json::text AS a;
SELECT 'true'::json::text AS a;
SELECT 'null'::json::text AS a;
SELECT '[]'::json::text AS a;
SELECT '{"a": 1}'::json::text AS a;
SELECT '{"a":[1,2,{"b":null}]}'::jsonb::text AS a;

-- ============================================================================
-- 8. A three-valued test reads a boolean
-- ============================================================================
SELECT 1 IS UNKNOWN AS a;
SELECT 1 IS TRUE AS a;
SELECT 1 IS NOT FALSE AS a;
SELECT (true IS UNKNOWN)::text AS a;
SELECT (NULL::boolean IS UNKNOWN)::text AS a;
SELECT (true IS TRUE)::text AS a;
SELECT (NULL::boolean IS NOT TRUE)::text AS a;

-- ============================================================================
-- 9. A derived column's type does not decide the operator
-- ============================================================================
DROP TABLE IF EXISTS tres_t CASCADE;
CREATE TABLE tres_t (id int, a text);
INSERT INTO tres_t VALUES (1,'x'),(2,'y');
SELECT count(*)::text AS a FROM (
  SELECT t.id, row_number() OVER (ORDER BY t.id) AS rn FROM tres_t t
) sub WHERE sub.rn >= 1;
DROP TABLE tres_t CASCADE;

-- ============================================================================
-- 10. A domain's CHECK still applies on a cast
-- ============================================================================
DROP DOMAIN IF EXISTS tres_pos CASCADE;
CREATE DOMAIN tres_pos AS int CHECK (VALUE > 0);
SELECT (-1)::tres_pos AS a;
SELECT (1)::tres_pos::text AS a;
DROP DOMAIN tres_pos CASCADE;

-- ============================================================================
-- 11. IS DISTINCT FROM resolves the same equality a comparison does
-- ============================================================================
SELECT 1 IS DISTINCT FROM 'a'::text AS a;
SELECT 1 IS NOT DISTINCT FROM 'a'::text AS a;
SELECT (1 IS DISTINCT FROM 2)::text AS a;
SELECT (1 IS NOT DISTINCT FROM 1)::text AS a;
SELECT (NULL::int IS DISTINCT FROM 1)::text AS a;

-- a type with no "=" at all cannot be compared, even against a literal
SELECT '(1,2)' = '(1,2)'::point AS a;

-- ============================================================================
-- 12. A branch list settles on one type
-- ============================================================================
SELECT CASE WHEN true THEN 1 ELSE '2'::text END AS a;
SELECT GREATEST('10'::text, 9) AS a;
SELECT COALESCE(1::int, 'x') AS a;
SELECT (CASE WHEN true THEN 1 ELSE 2 END)::text AS a;
SELECT (CASE WHEN true THEN 'a' ELSE 'b' END) AS a;
SELECT GREATEST(1, 2, 3)::text AS a;
SELECT LEAST(1, 2, 3)::text AS a;
SELECT COALESCE(NULL::int, 7)::text AS a;

-- ============================================================================
-- 13. A row comparison needs equal arity
-- ============================================================================
SELECT ROW(1,2) < ROW(1,2,3) AS a;
SELECT ROW(1,2) = ROW(1,2,3) AS a;
SELECT (ROW(1,2) = ROW(1,2))::text AS a;
SELECT (ROW(1,2) < ROW(1,3))::text AS a;

-- ============================================================================
-- 14. NULLIF, ISNULL and NOTNULL
-- ============================================================================
SELECT NULLIF(1, NULL)::text AS a;
SELECT NULLIF(1.5, NULL)::text AS a;
SELECT (NULLIF(1, NULL) + 1)::text AS a;
SELECT NULLIF(1, 2)::text AS a;
SELECT NULLIF(1, 1)::text AS a;
SELECT (1 ISNULL)::text AS a;
SELECT (1 NOTNULL)::text AS a;
SELECT (NULL::int ISNULL)::text AS a;
SELECT (NULL::int NOTNULL)::text AS a;

-- ============================================================================
-- 15. An array literal has to be braced to be an array
-- ============================================================================
SELECT ARRAY[1,2] || '3' AS a;
SELECT ARRAY['a','b'] || 'c' AS a;
SELECT (ARRAY[[1,2],[3,4]] || '{5,6}')::text AS a;
SELECT (ARRAY[1,2] || 3)::text AS a;
SELECT (ARRAY[1,2] || ARRAY[3])::text AS a;
SELECT ('{1,2}'::int[] || '{3}')::text AS a;

-- ============================================================================
-- 16. A polymorphic argument cannot be resolved from an unknown literal
-- ============================================================================
SELECT array_to_string('{1,2,3}', '-') AS a;
SELECT array_length('{1,2,3}', 1) AS a;
SELECT cardinality('{1,2,3}') AS a;
SELECT array_to_string(ARRAY[1,2,3], '-') AS a;
SELECT array_length(ARRAY[1,2,3], 1)::text AS a;
SELECT cardinality(ARRAY[1,2,3])::text AS a;
SELECT array_to_string('{1,2,3}'::int[], '-') AS a;

-- ============================================================================
-- 17. A call is resolved from the types the query wrote
-- ============================================================================
DROP FUNCTION IF EXISTS tres_num(int);
DROP FUNCTION IF EXISTS tres_num(double precision);
CREATE FUNCTION tres_num(int) RETURNS text LANGUAGE sql AS $$ SELECT 'int' $$;
CREATE FUNCTION tres_num(double precision) RETURNS text LANGUAGE sql AS $$ SELECT 'float' $$;
SELECT tres_num('6') AS a;
SELECT tres_num(6.0) AS a;
SELECT tres_num(6) AS a;
-- IF EXISTS on the way out: memgres cannot yet resolve a DROP by a two-word parameter type,
-- which is a gap in DROP FUNCTION's overload lookup rather than in the resolution this file is
-- about, and the two engines agree on the IF EXISTS form
DROP FUNCTION IF EXISTS tres_num(int);
DROP FUNCTION IF EXISTS tres_num(double precision);

-- ============================================================================
-- 18. Range operators resolve from the declared range type
-- ============================================================================
SELECT ('[1.0,3.0)'::numrange * '[2.0,5.0)')::text AS a;
SELECT ('[1,3)'::int4range && '[2,5)')::text AS a;
SELECT '{[1,3),[5,7)}'::int4multirange @> '[1,2)' AS a;
SELECT ('{[1,3)}'::int4multirange @> '{[1,2)}')::text AS a;

-- ============================================================================
-- 19. tsquery matches a vector either way round
-- ============================================================================
SELECT ('a'::tsquery @@ to_tsvector('simple','a b'))::text AS a;
SELECT (to_tsvector('simple','a b') @@ 'a'::tsquery)::text AS a;
SELECT ('a & b'::tsquery @@ to_tsvector('simple','a b'))::text AS a;
SELECT jsonb_exists_any('{"a":1,"b":2}'::jsonb, '{a,z}')::text AS a;

-- ============================================================================
-- 20. A CHECK added by ALTER DOMAIN applies on a cast
-- ============================================================================
DROP DOMAIN IF EXISTS tres_d3 CASCADE;
CREATE DOMAIN tres_d3 AS int;
ALTER DOMAIN tres_d3 ADD CHECK (VALUE > 0);
SELECT (-5)::tres_d3 AS a;
SELECT (5)::tres_d3::text AS a;
DROP DOMAIN tres_d3 CASCADE;

-- ============================================================================
-- 21. Text that is no date at all is an input syntax error
-- ============================================================================
SELECT 'null'::date AS a;
SELECT '2023-02-29'::date AS a;
SELECT '2024-02-29'::date::text AS a;

-- ============================================================================
-- 22. IN and ANY/ALL resolve the same equality
-- ============================================================================
-- "= ANY(array)" is parsed as an IN, so both spellings look up the operand
-- type's "=" and a type without one cannot be written either way.
SELECT '(1,2)'::point = ANY(ARRAY['(1,2)'::point]) AS a;
SELECT '(1,2)'::point = ANY('{"(1,2)"}'::point[]) AS a;
SELECT '(1,2)'::point IN ('(1,2)'::point) AS a;
SELECT '(1,2)'::point = ALL(ARRAY['(1,2)'::point]) AS a;
SELECT polygon '((0,0),(1,1))' IN (polygon '((0,0),(1,1))') AS a;

-- NOT IN is not the negation of IN: PostgreSQL expands it to "<> ALL", which
-- resolves "<>" -- an operator a point does have.
SELECT ('(1,2)'::point NOT IN ('(1,2)'::point))::text AS a;
SELECT ('(1,2)'::point <> ANY(ARRAY['(1,2)'::point]))::text AS a;
SELECT ('(1,2)'::point ~= '(1,2)'::point)::text AS a;

-- ordinary IN and ANY are untouched
SELECT (1 = ANY(ARRAY[1,2]))::text AS a;
SELECT (1 IN (1,2))::text AS a;
SELECT (3 NOT IN (1,2))::text AS a;
SELECT ('a'::text IN ('a','b'))::text AS a;
SELECT (1 = ALL(ARRAY[1,1]))::text AS a;
SELECT ('2024-01-01'::date IN ('2024-01-01'::date))::text AS a;
SELECT count(*)::text AS a FROM (VALUES (1),(2)) v(x) WHERE v.x IN (1,2);
SELECT count(*)::text AS a FROM (VALUES (1),(2)) v(x) WHERE v.x = ANY(ARRAY[1]);
