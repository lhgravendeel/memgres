-- ============================================================================
-- -- A pattern is read in PostgreSQL's own language, and a character is a character.
-- --
-- -- Regular expressions here are PostgreSQL's advanced ones, not java.util.regex's: the class
-- -- names hold what its tables say they hold, an escape it does not define is an error rather
-- -- than something to guess at, and a brace that begins no repetition is a brace. SIMILAR TO is
-- -- a third language again -- a regular expression with a smaller vocabulary, in which a dot is
-- -- a dot -- and it is written out into the first one rather than assembled by quoting. And
-- -- everything that counts characters counts characters: a position, a length and an underscore
-- -- all mean the same thing above U+FFFF as below it, where a character takes two units to store
-- -- and cutting between them leaves half of one behind.
--
-- ============================================================================

-- ============================================================================
-- 1. What the named classes hold
-- ============================================================================
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('é' ~ '[[:alpha:]]')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('Ä' ~ '[[:upper:]]')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('é' ~ '\w')::text AS a;
-- begin-expected
-- columns: a
-- row: XXX
-- end-expected
SELECT regexp_replace('aéb','[[:alpha:]]','X','g') AS a;
-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT regexp_count('aéb','[[:alpha:]]') AS a;
-- begin-expected
-- columns: a
-- row: aéb
-- end-expected
SELECT substring('aéb' from '[[:alpha:]]+') AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('é' SIMILAR TO '[[:alpha:]]')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('5' ~ '[[:digit:]]')::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('٥' ~ '[[:digit:]]')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('٥' ~ '[[:alpha:]]')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (' ' ~ '[[:space:]]')::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('a' ~ '\d')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('a' ~ '\S')::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('a' ~ '\W')::text AS a;

-- ============================================================================
-- 2. The escapes PostgreSQL defines, and the ones it does not
-- ============================================================================
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('a' ~ '\141')::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT (E'ab\n' ~ 'b\Z')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('ab' ~ '(?# comment)ab')::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('a' ~ '[\b]')::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('abc' ~ 'a{,3}')::text AS a;
-- begin-expected-error
-- sqlstate: 2201B
-- message-like: invalid regular expression: quantifier operand invalid
-- end-expected-error
SELECT ('abc' ~ '(?<name>a)')::text AS a;
-- begin-expected-error
-- sqlstate: 2201B
-- message-like: invalid regular expression: invalid escape \ sequence
-- end-expected-error
SELECT ('abc' ~ '\Qa\E')::text AS a;
-- begin-expected-error
-- sqlstate: 2201B
-- message-like: invalid regular expression: invalid escape \ sequence
-- end-expected-error
SELECT ('abc' ~ '\')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('abc' ~ '\mabc')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('abc' ~ 'abc\M')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('abc' ~ '\Aabc')::text AS a;

-- ============================================================================
-- 3. What a replacement text says, and what a start and a count say
-- ============================================================================
-- begin-expected
-- columns: a
-- row: ax\nyc
-- end-expected
SELECT regexp_replace('abc', 'b', 'x\ny') AS a;
-- begin-expected
-- columns: a
-- row: a\0c
-- end-expected
SELECT regexp_replace('abc','b','\0') AS a;
-- begin-expected
-- columns: a
-- row: a[b]c
-- end-expected
SELECT regexp_replace('abc','(b)','[\1]') AS a;
-- begin-expected
-- columns: a
-- row: a&c
-- end-expected
SELECT regexp_replace('abc','b','&') AS a;
-- begin-expected
-- columns: a
-- row: abc
-- end-expected
SELECT regexp_replace('abc','b','\&') AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "start": 0
-- end-expected-error
SELECT regexp_substr('abc', 'a', 0) AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "start": 0
-- end-expected-error
SELECT regexp_instr('abc', 'a', 0) AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "start": 0
-- end-expected-error
SELECT regexp_count('abc','a',0) AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "n": -1
-- end-expected-error
SELECT regexp_replace('banana','a','X',1,-1) AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "n": 0
-- end-expected-error
SELECT regexp_substr('abc','a',1,0) AS a;
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM regexp_matches(E'a\nb', '.', 'g');
-- begin-expected
-- columns: count
-- row: 5
-- end-expected
SELECT count(*) FROM regexp_matches(E'a\nb\nc', '.', 'g');
-- begin-expected
-- columns: regexp_matches
-- row: {ab}
-- end-expected
SELECT * FROM regexp_matches('ab', 'a b', 'x');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid regular expression option: "z"
-- end-expected-error
SELECT * FROM regexp_matches('a', 'a', 'z');
-- begin-expected
-- columns: regexp_matches
-- row: {b}
-- end-expected
SELECT * FROM regexp_matches(E'a\nb', '^b', 'n');

-- ============================================================================
-- 4. SIMILAR TO is its own language
-- ============================================================================
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('a' SIMILAR TO '.' ESCAPE '!')::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('a1' SIMILAR TO 'a\d' ESCAPE '!')::text AS a;
-- begin-expected-error
-- sqlstate: 22025
-- message-like: invalid escape string
-- end-expected-error
SELECT ('abc' SIMILAR TO 'abc' ESCAPE 'xy')::text AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT ('abc' LIKE 'abc' ESCAPE NULL)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('a{b}' SIMILAR TO 'a{b}')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('aaa' SIMILAR TO 'a{2,3}')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('abc' SIMILAR TO '(a|b)%')::text AS a;
-- begin-expected
-- columns: a
-- row: abc
-- end-expected
SELECT substring('abc' SIMILAR 'abc' ESCAPE '#') AS a;
-- begin-expected
-- columns: a
-- row: abc
-- end-expected
SELECT substring('abc' from 'a%c' for '#') AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT substring('abc' from '(x)?b') AS a;
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT substring('abcdef', '2', '3') AS a;

-- ============================================================================
-- 5. A character is a character, however many units it takes to store
-- ============================================================================
-- begin-expected
-- columns: a
-- row: b
-- end-expected
SELECT substr(U&'a\+01F600b', 3, 1) AS a;
-- begin-expected
-- columns: a
-- row: a😀
-- end-expected
SELECT substr(U&'a\+01F600b', 1, 2) AS a;
-- begin-expected
-- columns: a
-- row: aXb
-- end-expected
SELECT overlay(U&'a\+01F600b' placing 'X' from 2 for 1) AS a;
-- begin-expected
-- columns: a
-- row: a😀
-- end-expected
SELECT left(U&'a\+01F600b', 2) AS a;
-- begin-expected
-- columns: a
-- row: 😀b
-- end-expected
SELECT right(U&'a\+01F600b', 2) AS a;
-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT length(lpad(U&'\+01F600',3,'x')) AS a;
-- begin-expected
-- columns: a
-- row: xx😀
-- end-expected
SELECT lpad(U&'\+01F600',3,'x') AS a;
-- begin-expected
-- columns: a
-- row: 😀xx
-- end-expected
SELECT rpad(U&'\+01F600',3,'x') AS a;
-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT strpos(U&'a\+01F600b', 'b') AS a;
-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT position('b' in U&'a\+01F600b') AS a;
-- begin-expected
-- columns: a
-- row: 128512
-- end-expected
SELECT ascii(U&'\+01F600') AS a;
-- begin-expected
-- columns: a
-- row: {a,😀,b}
-- end-expected
SELECT string_to_array(U&'a\+01F600b', NULL)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (U&'\+01F600' LIKE '_')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (U&'a\+01F600b' LIKE 'a_b')::text AS a;
-- begin-expected
-- columns: a
-- row: abc
-- end-expected
SELECT right('abc', -2147483648) AS a;
-- begin-expected
-- columns: a
-- row: 
-- end-expected
SELECT left('abc', -2147483648) AS a;

-- ============================================================================
-- 6. Case is folded one character at a time, so a fold changes no length
-- ============================================================================
-- begin-expected
-- columns: a
-- row: STRAßE
-- end-expected
SELECT upper('straße') AS a;
-- begin-expected
-- columns: a
-- row: 6
-- end-expected
SELECT length(upper('straße')) AS a;
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT length(lower(U&'\1E9E')) AS a;
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT length(casefold(U&'\0130')) AS a;

-- ============================================================================
-- 7. The singletons: what is trimmed, what is quoted, what is numbered
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT length(btrim(E' \tabc\n ')) AS a;
-- begin-expected
-- columns: a
-- row: [	abc	]
-- end-expected
SELECT '[' || trim(both from E'\tabc\t') || ']' AS a;
-- begin-expected
-- columns: a
-- row: abc
-- end-expected
SELECT trim('xabcx', 'x') AS a;
-- begin-expected
-- columns: a
-- row: a b
-- end-expected
SELECT format('%1$s %s', 'a', 'b') AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: format specifies argument 0, but arguments are numbered from 1
-- end-expected-error
SELECT format('%0$s','a') AS a;
-- begin-expected
-- columns: a
-- row: y
-- end-expected
SELECT format('%*s', NULL, 'y') AS a;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT format('%*s', 'abc', 'y') AS a;
-- begin-expected
-- columns: a
-- row: data
-- end-expected
SELECT unistr('dat\U00000061') AS a;
-- begin-expected
-- columns: a
-- row: data
-- end-expected
SELECT unistr('dat\0061') AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: invalid Unicode escape
-- end-expected-error
SELECT unistr('\wxyz') AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: invalid Unicode escape
-- end-expected-error
SELECT unistr('\12') AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT unicode_assigned(U&'a\0378')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT unicode_assigned('abc')::text AS a;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer ^@ integer
-- end-expected-error
SELECT (1 ^@ 1)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('abc' ^@ 'ab')::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('abc' LIKE ANY (ARRAY['x%','a%']))::text AS a;
-- begin-expected
-- columns: a
-- row: {a,NULL,c}
-- end-expected
SELECT string_to_array('abc', NULL, 'b')::text AS a;

-- ============================================================================
-- 8. What the reader makes of a written literal
-- ============================================================================
-- begin-expected
-- columns: a
-- row: data
-- end-expected
SELECT U&'d!0061t!+000061' UESCAPE '!' AS a;
-- begin-expected
-- columns: a
-- row: data
-- end-expected
SELECT U&'\0064\0061\0074\0061' AS a;
-- begin-expected
-- columns: a
-- row: a\b
-- end-expected
SELECT U&'a\\b' AS a;
SELECT 'foo'
-- begin-expected
-- columns: a
-- row: foobar
-- end-expected
'bar' AS a;
-- begin-expected
-- columns: a
-- row: 😀
-- end-expected
SELECT U&'\+01F600' AS a;
