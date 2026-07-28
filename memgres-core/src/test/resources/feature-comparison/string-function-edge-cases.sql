-- ============================================================================
-- Feature Comparison: string and pattern-matching edge cases
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- The degenerate corners of the string surface: a LIKE pattern that ends with
-- its escape character, the escape letters PostgreSQL spells inside a regular
-- expression, POSIX bracket classes, and the empty, zero and NULL arguments to
-- the functions that take a delimiter, a separator, a format or a dimension.
-- ============================================================================

DROP TABLE IF EXISTS sfe_t CASCADE;
CREATE TABLE sfe_t (v text);
INSERT INTO sfe_t VALUES ('a'), ('b');

-- ============================================================================
-- 1. A LIKE pattern must not end with the escape character
-- ============================================================================
SELECT 'abc' LIKE 'ab\' AS a;
SELECT 'abc' ILIKE 'ab\' AS a;
SELECT 'abc' NOT LIKE 'ab\' AS a;
SELECT 'abc' NOT ILIKE 'ab\' AS a;
SELECT 'abc' ~~ 'ab\' AS a;
SELECT 'abc' !~~ 'ab\' AS a;
SELECT 'abc' ~~* 'AB\' AS a;
SELECT 'abc' LIKE 'ab!' ESCAPE '!' AS a;
SELECT 'abc' ILIKE 'AB!' ESCAPE '!' AS a;
SELECT 'abc' LIKE 'ab\' ESCAPE '\' AS a;
SELECT 'abc' LIKE '\' AS a;
SELECT 'abc' LIKE '!' ESCAPE '!' AS a;
SELECT 'abc' LIKE '%\' AS a;
SELECT 'abc' ILIKE '%\' AS a;
SELECT 'abc' LIKE '_\' AS a;
SELECT 'abc' LIKE '__\' AS a;
SELECT 'abcd' LIKE '%c\' AS a;

-- ============================================================================
-- 2. The complaint only comes when matching reaches the escape
-- ============================================================================
SELECT 'ab' LIKE 'ab\' AS a;
SELECT 'a' LIKE 'ab\' AS a;
SELECT '' LIKE '\' AS a;
SELECT 'abc' LIKE 'x\' AS a;
SELECT 'abc' LIKE '%x\' AS a;
SELECT 'abc' LIKE 'abc\' AS a;
SELECT 'abc' LIKE 'ab\\\' AS a;
SELECT 'abc' LIKE 'ab\' ESCAPE '' AS a;
SELECT 'ab\' LIKE 'ab\' ESCAPE '' AS a;
SELECT 'abc' LIKE 'ab' ESCAPE 'xy' AS a;

-- ============================================================================
-- 3. Ordinary LIKE patterns are untouched
-- ============================================================================
SELECT 'abc' LIKE '' AS a;
SELECT '' LIKE '' AS a;
SELECT 'abc' LIKE '%' AS a;
SELECT 'abc' LIKE 'a%' AS a;
SELECT 'abc' LIKE '%c' AS a;
SELECT 'abc' LIKE '%b%' AS a;
SELECT 'abc' LIKE '%%%' AS a;
SELECT 'abc' LIKE 'a%b%c' AS a;
SELECT 'aaa' LIKE '%a%a%a%' AS a;
SELECT 'abc' LIKE '_%_' AS a;
SELECT 'a' LIKE '_%_' AS a;
SELECT 'a%c' LIKE 'a\%c' AS a;
SELECT 'a_c' LIKE 'a\_c' AS a;
SELECT 'abc' LIKE 'a\bc' AS a;
SELECT 'a\b' LIKE 'a\\b' AS a;
SELECT 'ab\' LIKE '%\\' AS a;
SELECT '100%' LIKE '100\%' AS a;
SELECT 'axc' LIKE 'a.c' AS a;
SELECT 'a.c' LIKE 'a.c' AS a;
SELECT 'a(c' LIKE 'a(c' AS a;
SELECT 'a*c' LIKE 'a*c' AS a;
SELECT 'abc' LIKE 'A%' AS a;
SELECT 'abc' ILIKE 'A%' AS a;
SELECT E'a\nb' LIKE 'a%b' AS a;
SELECT E'a\nb' LIKE 'a_b' AS a;
SELECT E'a\nb' ILIKE 'A%B' AS a;
SELECT 'abc' LIKE NULL AS a;

-- ============================================================================
-- 4. SIMILAR TO drops a trailing escape rather than complaining
-- ============================================================================
SELECT 'abc' SIMILAR TO 'abc!' ESCAPE '!' AS a;
SELECT 'ab\' SIMILAR TO 'ab\' AS a;
SELECT 'abc' SIMILAR TO 'ab\' AS a;
SELECT 'abc' NOT SIMILAR TO 'ab\' AS a;
SELECT 'abc' SIMILAR TO 'ab!' ESCAPE '!' AS a;
SELECT 'abc' SIMILAR TO '' AS a;
SELECT '' SIMILAR TO '' AS a;
SELECT 'abc' SIMILAR TO '%' AS a;
SELECT 'abc' SIMILAR TO 'a_c' AS a;
SELECT 'abc' SIMILAR TO '(a|b)bc' AS a;
SELECT 'a%c' SIMILAR TO 'a\%c' AS a;
SELECT 'abc' SIMILAR TO 'a\%c' AS a;
SELECT E'a\nb' SIMILAR TO 'a%b' AS a;
SELECT substring('abc' SIMILAR 'a#"b#"c' ESCAPE '#') AS a;

-- ============================================================================
-- 5. A regular expression is newline-insensitive unless told otherwise
-- ============================================================================
SELECT E'a\nb' ~ 'a.b' AS a;
SELECT E'a\nb' ~* 'A.B' AS a;
SELECT E'a\nb' !~ 'a.b' AS a;
SELECT E'a\nb' !~* 'A.B' AS a;
SELECT E'a\nb' ~ '^b' AS a;
SELECT E'a\nb' ~ 'a$' AS a;
SELECT E'a\nb' ~ 'b$' AS a;
SELECT E'a\nb\n' ~ 'b$' AS a;
SELECT regexp_replace(E'a\nb', 'a.b', 'X') AS a;
SELECT regexp_match(E'a\nb', 'a.b') AS a;
SELECT regexp_like(E'a\nb', 'a.b') AS a;
SELECT regexp_count(E'a\nb', 'a.b') AS a;
SELECT regexp_instr(E'a\nb', 'a.b') AS a;
SELECT regexp_split_to_array(E'a\nb', '.') AS a;

-- ============================================================================
-- 6. The newline-sensitivity option letters, embedded and as flags
-- ============================================================================
SELECT E'a\nb' ~ '(?p)a.b' AS a;
SELECT E'a\nb' ~ '(?w)a.b' AS a;
SELECT E'a\nb' ~ '(?n)a.b' AS a;
SELECT E'a\nb' ~ '(?m)a.b' AS a;
SELECT E'a\nb' ~ '(?s)a.b' AS a;
SELECT E'a\nb' ~ '(?p)^b' AS a;
SELECT E'a\nb' ~ '(?w)^b' AS a;
SELECT E'a\nb' ~ '(?n)^b' AS a;
SELECT E'a\nb' ~ '(?m)^b' AS a;
SELECT E'a\nb' ~ '(?w)b$' AS a;
SELECT E'a\nb' ~ '(?p)b$' AS a;
SELECT E'a\nb' ~ '(?n)b$' AS a;
SELECT E'a\nb' ~ '(?i)A.B' AS a;
SELECT E'a\nb' ~ '(?e)a.b' AS a;
SELECT E'a\nb' ~ '(?b)a.b' AS a;
SELECT 'ab' ~ '(?t)ab' AS a;
SELECT 'a b' ~ '(?t) a b ' AS a;
SELECT 'ab' ~ '(?x) a b ' AS a;
SELECT 'ab' ~ '(?ix) A B ' AS a;
SELECT 'AB' ~ '(?c)ab' AS a;
SELECT 'ab' ~ '(?q)ab' AS a;
SELECT 'a.b' ~ '(?q)a.b' AS a;
SELECT 'axb' ~ '(?q)a.b' AS a;
SELECT 'ab' ~ '(?w)' AS a;
SELECT 'ab' ~ '(?q)' AS a;
SELECT 'a.b' ~ '***=a.b' AS a;
SELECT 'axb' ~ '***=a.b' AS a;
SELECT 'axb' ~ '***:a.b' AS a;
SELECT 'abc' ~ '(?z)abc' AS a;
SELECT 'ab' ~ '(?g)ab' AS a;
SELECT regexp_replace(E'a\nb', '(?p)a.b', 'X') AS a;
SELECT regexp_replace(E'a\nb', '(?w)a.b', 'X') AS a;
SELECT regexp_replace(E'a\nb', 'a.b', 'X', 'n') AS a;
SELECT regexp_replace(E'a\nb', 'a.b', 'X', 'p') AS a;
SELECT regexp_replace(E'a\nb', 'a.b', 'X', 'w') AS a;
SELECT regexp_replace(E'a\nb', 'a.b', 'X', 's') AS a;
SELECT regexp_replace('a b', 'a b', 'X', 'x') AS a;
SELECT regexp_replace('a.b', 'a.b', 'X', 'q') AS a;
SELECT regexp_replace('ABC', 'b', 'X', 'ic') AS a;
SELECT regexp_replace('abcabc', 'b', 'X', 'g') AS a;
SELECT regexp_replace('abcABC', 'b', 'X', 'gi') AS a;

-- ============================================================================
-- 7. Bracket expressions and POSIX character classes
-- ============================================================================
SELECT 'a1' ~ '[[:alpha:]][[:digit:]]' AS a;
SELECT ' ' ~ '[[:space:]]' AS a;
SELECT 'A' ~ '[[:upper:]]' AS a;
SELECT '_' ~ '[[:punct:]]' AS a;
SELECT 'a' ~ '[[:alnum:]]' AS a;
SELECT 'f' ~ '[[:xdigit:]]' AS a;
SELECT 'a' ~ '[[:ascii:]]' AS a;
SELECT 'a' ~ '[[:word:]]' AS a;
SELECT '_' ~ '[[:word:]]' AS a;
SELECT E'\t' ~ '[[:cntrl:]]' AS a;
SELECT E'a\tb' ~ 'a[[:blank:]]b' AS a;
SELECT 'a' ~ '[^[:digit:]]' AS a;
SELECT 'a' ~ '[^[:alpha:]]' AS a;
SELECT 'a' ~ '[[:alpha:][:digit:]]' AS a;
SELECT 'a' ~ '[[:alpha:]]*' AS a;
SELECT 'x' ~ '[[:alpha:]-]' AS a;
SELECT 'ab cd' ~ 'ab[[:>:]]' AS a;
SELECT 'ab cd' ~ '[[:<:]]cd' AS a;
SELECT 'a' ~ '[[:foo:]]' AS a;
SELECT 'a' ~ '[[:ALPHA:]]' AS a;
SELECT 'a' ~ '[[:alpha]]' AS a;
SELECT 'a' ~ '[abc' AS a;
SELECT 'a' ~ '[[:alpha:]' AS a;
SELECT '[' ~ '[[]' AS a;
SELECT ']' ~ '[]]' AS a;
SELECT 'a' ~ '[^]]' AS a;
SELECT '-' ~ '[a-]' AS a;
SELECT 'a:' ~ '[:]' AS a;
SELECT 'a' ~ '[[.a.]]' AS a;
SELECT 'a' ~ '[[=a=]]' AS a;
SELECT 'a' ~ '[\w]' AS a;
SELECT 'a-b' ~ '[a\-b]' AS a;
SELECT '&' ~ '[&]' AS a;

-- ============================================================================
-- 8. An option letter PostgreSQL does not know is an error
-- ============================================================================
SELECT regexp_replace('abc', 'b', 'X', 'z') AS a;
SELECT regexp_replace('abc', 'b', 'X', 'G') AS a;
SELECT regexp_replace('abc', 'b', 'X', 'gz') AS a;
SELECT regexp_replace('abc', 'b', 'X', ' ') AS a;
SELECT regexp_matches('abc', 'b', 'z') AS a;
SELECT regexp_like('abc', 'b', 'z') AS a;
SELECT regexp_count('abc', 'b', 1, 'z') AS a;
SELECT regexp_substr('abc', 'b', 1, 1, 'z') AS a;
SELECT regexp_instr('abc', 'b', 1, 1, 0, 'z') AS a;
SELECT regexp_split_to_array('abc', 'b', 'z') AS a;
SELECT regexp_like('abc', 'b', 'g') AS a;
SELECT regexp_replace('abc', 'b', 'X', '') AS a;
SELECT regexp_replace('abc', 'b', 'X', 'b') AS a;
SELECT regexp_replace('abc', 'b', 'X', 't') AS a;
SELECT regexp_replace('abc', 'b', 'X', 'e') AS a;
SELECT regexp_replace('abc', 'b', 'X', 'c') AS a;

-- ============================================================================
-- 9. NULL arguments keep the regexp family strict
-- ============================================================================
SELECT regexp_replace('abc', NULL, 'X') AS a;
SELECT regexp_replace('abc', 'b', NULL) AS a;
SELECT regexp_replace('abc', 'b', 'X', NULL) AS a;
SELECT regexp_like('abc', NULL) AS a;
SELECT regexp_count('abc', NULL) AS a;
SELECT regexp_instr('abc', NULL) AS a;
SELECT regexp_substr('abc', NULL) AS a;
SELECT regexp_split_to_array('abc', NULL) AS a;
SELECT regexp_split_to_array('abc', 'b', NULL) AS a;
SELECT 'a' ~ NULL AS a;

-- ============================================================================
-- 10. A zero-length match splits the way PostgreSQL splits
-- ============================================================================
SELECT regexp_split_to_array('abc', '') AS a;
SELECT array_length(regexp_split_to_array('abc', ''), 1) AS a;
SELECT regexp_split_to_array('abc', 'x*') AS a;
SELECT regexp_split_to_array('abc', 'c*') AS a;
SELECT regexp_split_to_array('abcc', 'c*') AS a;
SELECT regexp_split_to_array('abc', 'a*') AS a;
SELECT regexp_split_to_array('abc', 'b') AS a;
SELECT regexp_split_to_array('abc', 'z') AS a;
SELECT regexp_split_to_array('aXbXc', 'X') AS a;
SELECT regexp_split_to_array('Xabc', 'X') AS a;
SELECT regexp_split_to_array('abcX', 'X') AS a;
SELECT regexp_split_to_array('a1b2c', '[0-9]') AS a;
SELECT regexp_split_to_array('the quick brown fox', '\s+') AS a;
SELECT count(*) AS a FROM regexp_split_to_table('abc', '');
SELECT count(*) AS a FROM regexp_split_to_table(E'a\nb', '.');
SELECT count(*) AS a FROM regexp_split_to_table('abc', 'b');
SELECT array_to_string(array(SELECT * FROM regexp_split_to_table('abcX','X')), '|') AS a;
SELECT array_to_string(array(SELECT * FROM regexp_split_to_table('abc','c*')), '|') AS a;

-- ============================================================================
-- 11. An empty match is still a match regexp_matches reports
-- ============================================================================
SELECT regexp_matches('abc', 'x*') AS a;
SELECT regexp_matches('abc', 'b*') AS a;
SELECT regexp_matches('abc', '') AS a;
SELECT array_length(regexp_matches('abc', 'x*'), 1) AS a;
SELECT array_length(regexp_matches('abc', 'x*', 'g'), 1) AS a;
SELECT count(*) AS a FROM regexp_matches('abc', 'x*', 'g');
SELECT regexp_matches('abc', '(a)(b)') AS a;
SELECT regexp_matches('a,b', '(a,b)') AS a;
SELECT regexp_matches('a b', '(a b)') AS a;
SELECT regexp_matches('abc', '(a)(x)?(c)?') AS a;
SELECT (regexp_matches('a b', '(a b)'))[1] AS a;
SELECT (regexp_match('a,b', '(a,b)'))[1] AS a;
SELECT regexp_matches('foobarbequebaz', '(bar)(beque)') AS a;

-- ============================================================================
-- 12. An empty delimiter is not an instruction to split
-- ============================================================================
SELECT string_to_array('abc', '') AS a;
SELECT array_length(string_to_array('abc', ''), 1) AS a;
SELECT string_to_array('abc', '', 'abc') AS a;
SELECT string_to_array('abc', '', 'x') AS a;
SELECT string_to_array('abc', NULL) AS a;
SELECT string_to_array('', '') AS a;
SELECT string_to_array('', ',') AS a;
SELECT string_to_array('a,b', ',') AS a;
SELECT string_to_array('abcabc', 'bc') AS a;
SELECT string_to_array('abc', 'b', 'a') AS a;
SELECT string_to_array(NULL, ',') AS a;

-- ============================================================================
-- 13. Nothing to find in an empty needle
-- ============================================================================
SELECT replace('abc', '', 'X') AS a;
SELECT replace('', '', 'X') AS a;
SELECT replace('abc', '', '') AS a;
SELECT replace('abc', 'b', '') AS a;
SELECT replace('aaa', 'a', 'bb') AS a;
SELECT replace('abc', NULL, 'X') AS a;
SELECT translate('abc', '', 'X') AS a;
SELECT split_part('abc', '', 1) AS a;
SELECT lpad('abc', 6, '') AS a;
SELECT rpad('abc', 6, '') AS a;
SELECT repeat('abc', 0) AS a;
SELECT repeat('abc', -1) AS a;
SELECT btrim('abc', '') AS a;
SELECT concat_ws('', 'a', 'b') AS a;
SELECT array_to_string(ARRAY['a','b'], '') AS a;

-- ============================================================================
-- 14. A NULL separator joins with nothing at all
-- ============================================================================
SELECT string_agg(v, NULL) AS a FROM sfe_t;
SELECT string_agg(v, NULL ORDER BY v) AS a FROM sfe_t;
SELECT string_agg(v, '') AS a FROM sfe_t;
SELECT string_agg(v, ',') AS a FROM sfe_t;
SELECT string_agg(v, '-') AS a FROM (VALUES ('a'),(NULL),('b')) t(v);

-- ============================================================================
-- 15. An empty format string
-- ============================================================================
SELECT to_char(1, '') AS a;
SELECT to_char(1.5, '') AS a;
SELECT to_char(1::int, '') AS a;
SELECT to_char(1::bigint, '') AS a;
SELECT to_char(1.5::float8, '') AS a;
SELECT to_char(now(), '') AS a;
SELECT to_char(now()::date, '') AS a;
SELECT to_char(1, NULL) AS a;
SELECT to_char(NULL::numeric, '') AS a;
SELECT to_char(1, '9') AS a;
SELECT to_char(1234.5, '9999.9') AS a;

-- ============================================================================
-- 16. A negative rounding scale prints in full
-- ============================================================================
SELECT round(1.5, -1)::text AS a;
SELECT trunc(1.5, -1)::text AS a;
SELECT round(15, -1)::text AS a;
SELECT round(1234.5, -2)::text AS a;
SELECT trunc(1234.5, -2)::text AS a;
SELECT round(-15.0, -1)::text AS a;
SELECT round(123.456, -1)::text AS a;
SELECT trunc(123.456, -1)::text AS a;
SELECT round(1.5::numeric, -5)::text AS a;
SELECT round(1.5, 0)::text AS a;
SELECT round(1.55, 1)::text AS a;
SELECT (2e1::numeric)::text AS a;
SELECT (1e-10::numeric)::text AS a;
SELECT (0.00001::numeric)::text AS a;
SELECT (1.50::numeric)::text AS a;

-- ============================================================================
-- 17. A dimension the array does not have yields no subscripts
-- ============================================================================
SELECT count(*) AS a FROM generate_subscripts(ARRAY[1,2], 5);
SELECT count(*) AS a FROM generate_subscripts(ARRAY[1,2], 2);
SELECT count(*) AS a FROM generate_subscripts(ARRAY[1,2], 0);
SELECT count(*) AS a FROM generate_subscripts(ARRAY[1,2], -1);
SELECT count(*) AS a FROM generate_subscripts(ARRAY[1,2], NULL);
SELECT count(*) AS a FROM generate_subscripts(ARRAY[1,2], 1);
SELECT count(*) AS a FROM generate_subscripts(ARRAY[[1,2],[3,4]], 2);
SELECT count(*) AS a FROM generate_subscripts(ARRAY[[1,2],[3,4]], 3);
SELECT count(*) AS a FROM generate_subscripts(ARRAY[1,2], 1, true);
SELECT count(*) AS a FROM generate_subscripts('{}'::int[], 1);

DROP TABLE IF EXISTS sfe_t CASCADE;
