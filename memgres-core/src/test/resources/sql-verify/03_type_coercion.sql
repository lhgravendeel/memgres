-- ============================================================
-- 03: Type Coercion, Casting, and Type Errors
-- ============================================================

-- === Valid casts: :: syntax ===
SELECT '42'::int;
SELECT '42'::bigint;
SELECT '42'::smallint;
SELECT '3.14'::numeric;
SELECT '3.14'::double precision;
SELECT '3.14'::real;
SELECT 42::text;
SELECT 42::varchar;
SELECT 3.14::int;
SELECT 3.99::int;
SELECT '2024-01-15'::date;
SELECT '2024-01-15 10:30:00'::timestamp;
SELECT '2024-01-15 10:30:00+02'::timestamptz;
SELECT '10:30:00'::time;
SELECT 'true'::boolean;
SELECT 'false'::boolean;
SELECT 't'::boolean;
SELECT 'f'::boolean;
SELECT 'yes'::boolean;
SELECT 'no'::boolean;
SELECT '1'::boolean;
SELECT '0'::boolean;
SELECT '{"a":1}'::jsonb;
SELECT '{"a":1}'::json;
SELECT '[1,2,3]'::jsonb;
SELECT 'hello'::bytea;
SELECT true::int;
SELECT false::int;
SELECT 1::boolean;
SELECT 0::boolean;
SELECT '1 day'::interval;
SELECT '2 hours 30 minutes'::interval;
SELECT '1 year 2 months'::interval;

-- === Valid casts: CAST syntax ===
SELECT CAST('42' AS int);
SELECT CAST('3.14' AS numeric);
SELECT CAST(42 AS text);
SELECT CAST('2024-01-15' AS date);
SELECT CAST('true' AS boolean);
SELECT CAST('{"a":1}' AS jsonb);

-- === Numeric edge cases ===
SELECT 1e3::int;
SELECT '1e3'::numeric;
SELECT 'Infinity'::double precision;
SELECT '-Infinity'::double precision;
SELECT 'NaN'::double precision;
SELECT 9999999999999999::bigint;
SELECT 2147483647::int;
SELECT -2147483648::int;
SELECT 0.1 + 0.2;
SELECT 0.1::numeric + 0.2::numeric;
SELECT 1.0 / 3.0;
SELECT 10 % 3;
SELECT 2 ^ 10;
SELECT |/ 144;
SELECT ||/ 27;
SELECT @ -42;

-- === String/text conversions ===
SELECT 42::text || ' items';
SELECT 'price: ' || 9.99::text;
SELECT length('hello'::varchar(3));
SELECT 'hello'::char(10);
SELECT 'hello'::varchar(3);

-- === Date/time conversions ===
SELECT '2024-01-15'::date + 30;
SELECT '2024-01-15'::date + '30 days'::interval;
SELECT '2024-01-15'::date - '2023-01-15'::date;
SELECT now()::date;
SELECT now()::time;
SELECT now()::timestamp;
SELECT EXTRACT(year FROM '2024-06-15'::date);
SELECT EXTRACT(month FROM '2024-06-15'::date);
SELECT EXTRACT(dow FROM '2024-06-15'::date);
SELECT DATE_TRUNC('month', '2024-06-15 10:30:00'::timestamp);

-- === NULL coercion ===
SELECT NULL::int;
SELECT NULL::text;
SELECT NULL::boolean;
SELECT NULL = NULL;
SELECT NULL IS NULL;
SELECT NULL IS NOT NULL;
SELECT COALESCE(NULL, 42);
SELECT COALESCE(NULL, NULL, 'fallback');
SELECT NULLIF(1, 1);
SELECT NULLIF(1, 2);

-- === Array casts ===
SELECT '{1,2,3}'::int[];
SELECT ARRAY[1,2,3]::text[];
SELECT '{hello,world}'::text[];

-- === Invalid casts ===
SELECT 'hello'::int;
SELECT 'not_a_number'::numeric;
SELECT 'not_a_date'::date;
SELECT '25:00:00'::time;
SELECT 'not_a_bool'::boolean;
SELECT '{'::jsonb;
SELECT 'invalid json'::jsonb;
SELECT ''::int;
SELECT 2147483648::int;
SELECT 'hello'::uuid;

-- === Implicit coercion in operations ===
CREATE TABLE coerce_test (id serial PRIMARY KEY, int_col int, text_col text, bool_col boolean, num_col numeric);
INSERT INTO coerce_test (int_col, text_col, bool_col, num_col) VALUES (42, 'hello', true, 3.14);
SELECT * FROM coerce_test WHERE int_col = '42';
SELECT * FROM coerce_test WHERE text_col = 42;
SELECT * FROM coerce_test WHERE bool_col = 'true';
SELECT * FROM coerce_test WHERE num_col = '3.14';
SELECT int_col + '10' FROM coerce_test;
SELECT text_col || 123 FROM coerce_test;
DROP TABLE coerce_test;
