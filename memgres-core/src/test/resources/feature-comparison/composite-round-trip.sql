DROP TYPE IF EXISTS zz_rt_c CASCADE;

DROP TYPE IF EXISTS zz_rt_ca CASCADE;

DROP TYPE IF EXISTS zz_rt_en CASCADE;

DROP TYPE IF EXISTS zz_rt_rg CASCADE;

CREATE TYPE zz_rt_c AS (a text, b text);

CREATE TYPE zz_rt_ca AS (arr text[]);

CREATE TYPE zz_rt_en AS ENUM ('e,f', 'z');

CREATE TYPE zz_rt_rg AS RANGE (subtype = text);

DROP TABLE IF EXISTS zz_rt_ct CASCADE;

DROP TABLE IF EXISTS zz_rt_cu CASCADE;

DROP TABLE IF EXISTS zz_rt_cx CASCADE;

DROP TABLE IF EXISTS zz_rt_iv CASCADE;

CREATE TABLE zz_rt_ct (c zz_rt_c);

CREATE TABLE zz_rt_cu (c zz_rt_c);

CREATE TABLE zz_rt_cx (c zz_rt_c UNIQUE);

CREATE TABLE zz_rt_iv (v interval UNIQUE);

INSERT INTO zz_rt_ct VALUES (ROW('a"b','c,d'));

INSERT INTO zz_rt_cu VALUES (ROW('a','b,c')), (ROW('a,b','c'));

-- begin-expected
-- columns: row
-- row: ("a""b","c,d")
-- end-expected
SELECT ROW('a"b','c,d')::text;

-- begin-expected
-- columns: row
-- row: ("a\\b",x)
-- end-expected
SELECT ROW('a\b','x')::text;

-- begin-expected
-- columns: row
-- row: ("",x)
-- end-expected
SELECT ROW('','x')::text;

-- begin-expected
-- columns: row
-- row: (,x)
-- end-expected
SELECT ROW(NULL::text,'x')::text;

-- begin-expected
-- columns: row
-- row: ("a b",x)
-- end-expected
SELECT ROW('a b','x')::text;

-- begin-expected
-- columns: row
-- row: ("(a)",x)
-- end-expected
SELECT ROW('(a)','x')::text;

-- begin-expected
-- columns: row
-- row: ("{a,b}")
-- end-expected
SELECT ROW(ARRAY['a','b'])::text;

-- begin-expected
-- columns: row
-- row: (t,f)
-- end-expected
SELECT ROW(true, false)::text;

-- begin-expected
-- columns: row
-- row: ("\\x0102")
-- end-expected
SELECT ROW('\x0102'::bytea)::text;

-- begin-expected
-- columns: row
-- row: (1.50,0.0000000000000001)
-- end-expected
SELECT ROW(1.50::numeric, 1e-16::numeric)::text;

-- begin-expected
-- columns: row
-- row: ("4713-01-01 BC",24:00:00,1e-300)
-- end-expected
SELECT ROW('4713-01-01 BC'::date, '24:00:00'::time, 1e-300::float8)::text;

-- begin-expected
-- columns: c
-- row: ("a""b","c,d")
-- end-expected
SELECT c::text FROM zz_rt_ct;

-- begin-expected
-- columns: a|b
-- row: <a"b>|<c,d>
-- end-expected
SELECT '<' || (c).a || '>' AS a, '<' || (c).b || '>' AS b FROM zz_rt_ct;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(DISTINCT c) FROM zz_rt_cu;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM (SELECT c FROM zz_rt_cu GROUP BY c) g;

-- begin-expected
-- columns: a1
-- row: a"b
-- end-expected
SELECT ('("a""b",z)'::zz_rt_c).a AS a1;

-- begin-expected
-- columns: a2
-- row: a\b
-- end-expected
SELECT ('("a\\b",z)'::zz_rt_c).a AS a2;

-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (ROW('a"b','z')::zz_rt_c::text::zz_rt_c).a = 'a"b' AS ok;

-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('("",z)'::zz_rt_c).a = '' AS ok;

-- begin-expected
-- columns: lit
-- row: ("{a,b}")
-- end-expected
SELECT ROW(ARRAY['a','b'])::zz_rt_ca::text AS lit;

-- begin-expected
-- columns: e
-- row: b
-- end-expected
SELECT ((ROW(ARRAY['a','b'])::zz_rt_ca::text::zz_rt_ca).arr)[2] AS e;

-- begin-expected
-- columns: enum_range
-- row: {"e,f",z}
-- end-expected
SELECT enum_range(NULL::zz_rt_en)::text;

-- begin-expected
-- columns: v
-- row: e,f
-- row: z
-- end-expected
SELECT x::text AS v FROM (SELECT unnest(enum_range(NULL::zz_rt_en)) AS x) t ORDER BY 1;

-- begin-expected
-- columns: zz_rt_rg
-- row: ["a,b",z)
-- end-expected
SELECT zz_rt_rg('a,b','z')::text;

-- begin-expected
-- columns: zz_rt_rg
-- row: ["",z)
-- end-expected
SELECT zz_rt_rg('','z')::text;

-- begin-expected
-- columns: zz_rt_rg
-- row: ["a b",z)
-- end-expected
SELECT zz_rt_rg('a b','z')::text;

-- begin-expected
-- columns: eq|n
-- row: t|1
-- end-expected
SELECT '1 day'::interval = '24 hours'::interval AS eq, count(DISTINCT x) AS n FROM (SELECT '1 day'::interval AS x UNION ALL SELECT '24 hours'::interval) t;

-- begin-expected
-- columns: eq|n
-- row: t|1
-- end-expected
SELECT '2020-01-01 12:00:00+00'::timestamptz = '2020-01-01 13:00:00+01'::timestamptz AS eq, count(DISTINCT x) AS n FROM (SELECT '2020-01-01 12:00:00+00'::timestamptz AS x UNION ALL SELECT '2020-01-01 13:00:00+01'::timestamptz) t;

-- begin-expected
-- columns: eq|gt
-- row: t|f
-- end-expected
SELECT 0.0::float8 = -0.0::float8 AS eq, 0.0::float8 > -0.0::float8 AS gt;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM (SELECT x FROM (SELECT 0.0::float8 AS x UNION ALL SELECT -0.0::float8) t GROUP BY x) g;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(DISTINCT x) FROM (SELECT 0.0::float8 AS x UNION ALL SELECT -0.0::float8) t;

INSERT INTO zz_rt_iv VALUES ('1 day');

-- begin-expected-error
-- sqlstate: 23505
-- message-like: ERROR: duplicate key value violates unique constraint "zz_rt_iv_v_key"
-- end-expected-error
INSERT INTO zz_rt_iv VALUES ('24 hours');

-- begin-expected
-- columns: a|b
-- row: a|b, c
-- row: a, b|c
-- end-expected
WITH RECURSIVE zz_rt_r(a, b) AS (SELECT 'a'::text, 'b, c'::text UNION SELECT 'a, b'::text, 'c'::text FROM zz_rt_r WHERE a = 'a') SELECT a, b FROM zz_rt_r ORDER BY a, b;

-- begin-expected
-- columns: q|f
-- row: E'a\\b'|E'a\\b'
-- end-expected
SELECT quote_literal('a\b') AS q, format('%L','a\b') AS f;

-- begin-expected
-- columns: quote_ident|quote_ident|quote_ident|quote_ident|quote_ident|quote_ident|quote_ident
-- row: "current_date"|"trim"|"coalesce"|"select"|"a b"|"Abc"|abc
-- end-expected
SELECT quote_ident('current_date'), quote_ident('trim'), quote_ident('coalesce'), quote_ident('select'), quote_ident('a b'), quote_ident('Abc'), quote_ident('abc');

-- begin-expected
-- columns: quote_nullable|quote_nullable
-- row: NULL|E'a\\b'
-- end-expected
SELECT quote_nullable(NULL), quote_nullable('a\b');

-- begin-expected
-- columns: format|format|format
-- row: \x61|{1,2}|t
-- end-expected
SELECT format('%s', '\x61'::bytea), format('%s', ARRAY[1,2]), format('%s', true);

-- begin-expected
-- columns: format|format
-- row: "a b"|abc
-- end-expected
SELECT format('%I', 'a b'), format('%I','abc');

-- begin-expected
-- columns: text
-- row: true
-- end-expected
SELECT ('\x61'::bytea LIKE 'a')::text;

-- begin-expected
-- columns: to_jsonb
-- row: "x"
-- end-expected
SELECT to_jsonb('"x"'::jsonb)::text;

-- begin-expected
-- columns: to_jsonb
-- row: "x"
-- end-expected
SELECT to_jsonb(to_jsonb('"x"'::jsonb))::text;

DROP TABLE zz_rt_ct;

DROP TABLE zz_rt_cu;

DROP TABLE zz_rt_cx;

DROP TABLE zz_rt_iv;

DROP TYPE zz_rt_c CASCADE;

DROP TYPE zz_rt_ca CASCADE;

DROP TYPE zz_rt_en CASCADE;

DROP TYPE zz_rt_rg CASCADE;

