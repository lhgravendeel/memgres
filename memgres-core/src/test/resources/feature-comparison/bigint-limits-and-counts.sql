-- ============================================================================
-- Feature Comparison: function existence and signatures
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL picks an overload from the argument's declared TYPE and never
-- looks at the value in it. It declares left/right/repeat/lpad/rpad/substr/
-- substring/split_part/overlay/chr and the regexp positional routines with an
-- integer count and no int8, numeric, real or float8 form, and none of those
-- types has an implicit cast down to int4 -- so a bigint column holding 4 is
-- just as much a missing function as a literal four billion is. int2 does cast
-- up to int4 and keeps working.
--
-- LIMIT and OFFSET are the other half of the same question and go the other
-- way: they are bigint there, so a value above 2^31 is simply a large limit.
-- ============================================================================

-- setup
DROP TABLE IF EXISTS fes_t CASCADE;
CREATE TABLE fes_t (n bigint, m int, sm smallint, nu numeric, d float8, s text);
INSERT INTO fes_t VALUES (4, 4, 4, 4, 4, 'abcde');

-- ============================================================================
-- 1. A count declared wider than integer names a function that does not exist
-- ============================================================================

-- stmt 1: a bigint column, the case an application actually hits
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(text, bigint) does not exist
-- end-expected-error
SELECT left(s, n) FROM fes_t;

-- stmt 2: an explicit cast on a value well inside the integer range
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, bigint) does not exist
-- end-expected-error
SELECT left('abcde', 4::bigint);

-- stmt 3
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function right(unknown, bigint) does not exist
-- end-expected-error
SELECT right('abcde', 4::bigint);

-- stmt 4
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function repeat(unknown, bigint) does not exist
-- end-expected-error
SELECT repeat('ab', 3::bigint);

-- stmt 5
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lpad(unknown, bigint, unknown) does not exist
-- end-expected-error
SELECT lpad('abc', 6::bigint, 'x');

-- stmt 6
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function rpad(unknown, bigint) does not exist
-- end-expected-error
SELECT rpad('abc', 6::bigint);

-- stmt 7
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function substr(unknown, bigint, bigint) does not exist
-- end-expected-error
SELECT substr('abcdef', 2::bigint, 3::bigint);

-- stmt 8
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function substring(unknown, bigint, integer) does not exist
-- end-expected-error
SELECT substring('abcdef', 2::bigint, 3);

-- stmt 9
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function split_part(unknown, unknown, bigint) does not exist
-- end-expected-error
SELECT split_part('a,b,c', ',', 2::bigint);

-- stmt 10: the syntax form is reported schema-qualified
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog.overlay(unknown, unknown, bigint, bigint) does not exist
-- end-expected-error
SELECT overlay('abcdef' placing 'XY' from 2::bigint for 3::bigint);

-- stmt 11: chr too
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function chr(bigint) does not exist
-- end-expected-error
SELECT chr(65::bigint);

-- stmt 12: the missing overload is decided before the value is, so a literal
-- four billion is the same error and not an empty string
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, bigint) does not exist
-- end-expected-error
SELECT left('abcde', 4294967296);

-- stmt 13: arithmetic answers in the wider of its operands
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, bigint) does not exist
-- end-expected-error
SELECT left('abcde', (4294967296 - 4294967292));

-- stmt 14: a literal past bigint is numeric, not bigint
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, numeric) does not exist
-- end-expected-error
SELECT left('abcde', 9223372036854775808);

-- stmt 15: a written constant with a point is numeric
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, numeric) does not exist
-- end-expected-error
SELECT left('abcde', 4.0);

-- stmt 16
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function repeat(unknown, numeric) does not exist
-- end-expected-error
SELECT repeat('ab', 3.7);

-- stmt 17
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, double precision) does not exist
-- end-expected-error
SELECT left('abcde', 4::float8);

-- stmt 18
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, real) does not exist
-- end-expected-error
SELECT left('abcde', 4::real);

-- stmt 19: a numeric column
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, numeric) does not exist
-- end-expected-error
SELECT left('abcde', nu) FROM fes_t;

-- stmt 20: a float8 column
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, double precision) does not exist
-- end-expected-error
SELECT left('abcde', d) FROM fes_t;

-- stmt 21: the sign has nothing to do with it -- a negative bigint is refused
-- for its type, while a negative integer is a legal count (stmt 33 below)
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, bigint) does not exist
-- end-expected-error
SELECT left('abcde', -2::bigint);

-- stmt 22: a scalar sub-query answering in bigint
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function repeat(unknown, bigint) does not exist
-- end-expected-error
SELECT repeat('ab', (SELECT count(*) FROM fes_t));

-- stmt 23: through a CTE
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, bigint) does not exist
-- end-expected-error
WITH c AS (SELECT n FROM fes_t) SELECT left('abcde', n) FROM c;

-- stmt 24: inside a derived table
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, bigint) does not exist
-- end-expected-error
SELECT x FROM (SELECT left('abcde', 4::bigint) AS x) q;

-- stmt 25: inside a set operation
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, bigint) does not exist
-- end-expected-error
SELECT left('abcde', 4::bigint) UNION ALL SELECT 'z';

-- stmt 26: resolution happens before execution, so a NULL string does not get
-- to answer NULL ahead of the error
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, bigint) does not exist
-- end-expected-error
SELECT left(NULL, 4::bigint);

-- stmt 27: nor does a NULL count
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, bigint) does not exist
-- end-expected-error
SELECT left('abcde', NULL::bigint);

-- stmt 28: the regexp routines take their positions as integer too
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function regexp_count(unknown, unknown, bigint) does not exist
-- end-expected-error
SELECT regexp_count('abcabc', 'b', 1::bigint);

-- stmt 29
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function regexp_substr(unknown, unknown, bigint) does not exist
-- end-expected-error
SELECT regexp_substr('abcabc', 'b', 1::bigint);

-- stmt 30
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function regexp_instr(unknown, unknown, bigint) does not exist
-- end-expected-error
SELECT regexp_instr('abcabc', 'b', 1::bigint);

-- stmt 31
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function regexp_replace(unknown, unknown, unknown, bigint, integer) does not exist
-- end-expected-error
SELECT regexp_replace('abcabc', 'b', 'X', 1::bigint, 0);

-- ============================================================================
-- 2. Everything an integer parameter does accept keeps working
-- ============================================================================

-- stmt 32: a plain integer literal
-- begin-expected
-- columns: a
-- row: abcd
-- end-expected
SELECT left('abcde', 4) AS a;

-- stmt 33: a negative count is legal -- the refusal above was about type only
-- begin-expected
-- columns: a
-- row: abc
-- end-expected
SELECT left('abcde', -2) AS a;

-- stmt 34: the bottom of the integer range, which PostgreSQL's lexer folds the
-- sign into and so keeps as an integer
-- begin-expected
-- columns: a
-- row:
-- end-expected
SELECT left('abcde', -2147483648) AS a;

-- stmt 35: one below it is a bigint again
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function left(unknown, bigint) does not exist
-- end-expected-error
SELECT left('abcde', -2147483649);

-- stmt 36: the top of the integer range
-- begin-expected
-- columns: a
-- row: abcde
-- end-expected
SELECT left('abcde', 2147483647) AS a;

-- stmt 37: int2 has an implicit cast up to int4
-- begin-expected
-- columns: a
-- row: abcd
-- end-expected
SELECT left('abcde', 4::smallint) AS a;

-- stmt 38: and so does a smallint column
-- begin-expected
-- columns: a
-- row: abcd
-- end-expected
SELECT left(s, sm) AS a FROM fes_t;

-- stmt 39: an integer column
-- begin-expected
-- columns: a
-- row: abcd
-- end-expected
SELECT left(s, m) AS a FROM fes_t;

-- stmt 40: a bare literal is unknown and takes the parameter's own type
-- begin-expected
-- columns: a
-- row: abcd
-- end-expected
SELECT left('abcde', '4') AS a;

-- stmt 41: length/char_length/strpos answer in integer
-- begin-expected
-- columns: a
-- row: abcd
-- end-expected
SELECT left('abcde', char_length('abcd')) AS a;

-- stmt 42
-- begin-expected
-- columns: a
-- row: abcd
-- end-expected
SELECT left('abcde', strpos('abcd', 'd')) AS a;

-- stmt 43: integer arithmetic stays integer
-- begin-expected
-- columns: a
-- row: abcd
-- end-expected
SELECT left('abcde', 2 + 2) AS a;

-- stmt 44: an integer sub-query
-- begin-expected
-- columns: a
-- row: abcd
-- end-expected
SELECT left('abcde', (SELECT m FROM fes_t)) AS a;

-- stmt 45: a CASE over integers
-- begin-expected
-- columns: a
-- row: abcd
-- end-expected
SELECT left('abcde', CASE WHEN true THEN 4 ELSE 2 END) AS a;

-- stmt 46: coalesce over integers
-- begin-expected
-- columns: a
-- row: abcd
-- end-expected
SELECT left('abcde', coalesce(m, 4)) AS a FROM fes_t;

-- stmt 47: an integer max()
-- begin-expected
-- columns: a
-- row: abcd
-- end-expected
SELECT left('abcde', max(m)) AS a FROM fes_t;

-- stmt 48: a bigint narrowed back to integer by an explicit cast
-- begin-expected
-- columns: a
-- row: ab
-- end-expected
SELECT left('abcde', (2::bigint)::int) AS a;

-- stmt 49: a NULL count with no type of its own is still NULL, not an error
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT coalesce(left('abcde', NULL), 'NULL') AS a;

-- stmt 50: every other routine's integer form
-- begin-expected
-- columns: a
-- row: ababab
-- end-expected
SELECT repeat('ab', 3) AS a;

-- stmt 51
-- begin-expected
-- columns: a
-- row:   abc
-- end-expected
SELECT lpad('abc', 6) AS a;

-- stmt 52
-- begin-expected
-- columns: a
-- row: xxxabc
-- end-expected
SELECT lpad('abc', 6, 'x') AS a;

-- stmt 53
-- begin-expected
-- columns: a
-- row: abcxxx
-- end-expected
SELECT rpad('abc', 6, 'x') AS a;

-- stmt 54
-- begin-expected
-- columns: a
-- row: bcd
-- end-expected
SELECT substr('abcdef', 2, 3) AS a;

-- stmt 55
-- begin-expected
-- columns: a
-- row: bcd
-- end-expected
SELECT substring('abcdef' from 2 for 3) AS a;

-- stmt 56
-- begin-expected
-- columns: a
-- row: b
-- end-expected
SELECT split_part('a,b,c', ',', 2) AS a;

-- stmt 57
-- begin-expected
-- columns: a
-- row: c
-- end-expected
SELECT split_part('a,b,c', ',', -1) AS a;

-- stmt 58
-- begin-expected
-- columns: a
-- row: aXYef
-- end-expected
SELECT overlay('abcdef' placing 'XY' from 2 for 3) AS a;

-- stmt 59: the SIMILAR-pattern form of substring is untouched
-- begin-expected
-- columns: a
-- row: cd
-- end-expected
SELECT substring('abcdef' from '%#"cd#"%' for '#') AS a;

-- stmt 60: and the regex form
-- begin-expected
-- columns: a
-- row: cd
-- end-expected
SELECT substring('abcdef' from 'c.') AS a;

-- stmt 61: the regexp routines with integer positions
-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT regexp_count('abcabc', 'b', 1) AS a;

-- stmt 62
-- begin-expected
-- columns: a
-- row: b
-- end-expected
SELECT regexp_substr('abcabc', 'b', 1, 1) AS a;

-- stmt 63
-- begin-expected
-- columns: a
-- row: aXcaXc
-- end-expected
SELECT regexp_replace('abcabc', 'b', 'X', 1, 0) AS a;

-- stmt 64: a text flags argument in the same position is not a count
-- begin-expected
-- columns: a
-- row: aXcaXc
-- end-expected
SELECT regexp_replace('abcabc', 'b', 'X', 'g') AS a;

-- ============================================================================
-- 3. The errors that are about the value keep being about the value
-- ============================================================================

-- stmt 65
-- begin-expected-error
-- sqlstate: 22011
-- message-like: negative substring length not allowed
-- end-expected-error
SELECT substr('abcdef', 2, -1);

-- stmt 66
-- begin-expected-error
-- sqlstate: 22023
-- message-like: field position must not be zero
-- end-expected-error
SELECT split_part('a,b,c', ',', 0);

-- stmt 67
-- begin-expected-error
-- sqlstate: 54000
-- message-like: null character not permitted
-- end-expected-error
SELECT chr(0);

-- stmt 68
-- begin-expected-error
-- sqlstate: 54000
-- message-like: requested character too large for encoding: 2147483647
-- end-expected-error
SELECT chr(2147483647);

-- stmt 69: a negative code point is not an oversized one
-- begin-expected-error
-- sqlstate: 22023
-- message-like: character number must be positive
-- end-expected-error
SELECT chr(-1);

-- stmt 70: a narrowing cast still overflows on the value
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT left('abcde', 4294967296::bigint::int);

-- ============================================================================
-- 4. LIMIT and OFFSET are bigint, so the same numbers are simply large
-- ============================================================================

-- stmt 71
-- begin-expected
-- columns: m
-- row: 4
-- end-expected
SELECT m FROM fes_t LIMIT 4294967296;

-- stmt 72
-- begin-expected
-- columns: m
-- row: 4
-- end-expected
SELECT m FROM fes_t LIMIT (2147483647::bigint + 1);

-- stmt 73
-- begin-expected
-- columns: m
-- row: 4
-- end-expected
SELECT m FROM fes_t FETCH FIRST 4294967296 ROWS ONLY;

-- stmt 74
-- begin-expected
-- columns: m
-- row: 4
-- end-expected
SELECT m FROM fes_t LIMIT ALL;

-- stmt 75
-- begin-expected
-- columns: m
-- end-expected
SELECT m FROM fes_t OFFSET 4294967296;

-- stmt 76: past bigint it is the value that is out of range, not the type
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT m FROM fes_t LIMIT 9223372036854775808;

-- stmt 77
-- begin-expected-error
-- sqlstate: 2201W
-- message-like: LIMIT must not be negative
-- end-expected-error
SELECT m FROM fes_t LIMIT -1;

-- stmt 78
-- begin-expected-error
-- sqlstate: 2201X
-- message-like: OFFSET must not be negative
-- end-expected-error
SELECT m FROM fes_t OFFSET -1;

-- stmt 79: inside a derived table
-- begin-expected
-- columns: m
-- row: 4
-- end-expected
SELECT * FROM (SELECT m FROM fes_t LIMIT 4294967296) q;

-- stmt 80: on a whole set operation
-- begin-expected
-- columns: m
-- row: 4
-- row: 4
-- end-expected
SELECT m FROM fes_t UNION ALL SELECT m FROM fes_t LIMIT 4294967296;

-- cleanup
DROP TABLE IF EXISTS fes_t CASCADE;
