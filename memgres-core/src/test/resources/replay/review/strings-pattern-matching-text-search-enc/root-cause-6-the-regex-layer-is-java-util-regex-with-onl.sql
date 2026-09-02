-- source: review-2026-08.md
-- finding: Root cause 6: the regex layer is java.util.regex with only a thin translation
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Root cause 6: the regex layer is java.util.regex with only a thin translation
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'é' ~ '[[:alpha:]]';
-- and [[:upper:]], [[:lower:]], [[:alnum:]], [[:digit:]], \w, \d
-- begin-expected
-- columns: regexp_replace:text
-- row: XXX
-- rowcount: 1
-- end-expected
SELECT regexp_replace('aéb','[[:alpha:]]','X','g');
-- begin-expected
-- columns: substring:text
-- row: aéb
-- rowcount: 1
-- end-expected
SELECT substring('aéb' from '[[:alpha:]]+');
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'a' ~ '\141';
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT E'ab\n' ~ 'b\Z';
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'ab' ~ '(?# comment)ab';
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'a' ~ '[\b]';
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'abc' ~ 'a{,3}';
-- begin-expected-error
-- sqlstate: 2201B
-- message-like: invalid regular expression: quantifier operand invalid
-- end-expected-error
SELECT 'abc' ~ '(?<name>a)';
-- begin-expected-error
-- sqlstate: 2201B
-- message-like: invalid regular expression: invalid escape \ sequence
-- end-expected-error
SELECT 'abc' ~ '\Qa\E';
-- begin-expected
-- columns: regexp_matches:_text
-- row: {ab}
-- rowcount: 1
-- end-expected
SELECT * FROM regexp_matches('ab', 'a b', 'x');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid regular expression option: "z"
-- end-expected-error
SELECT * FROM regexp_matches('a', 'a', 'z');
-- begin-expected
-- columns: regexp_matches:_text
-- row: {b}
-- rowcount: 1
-- end-expected
SELECT * FROM regexp_matches(E'a\nb', '^b', 'n');
-- begin-expected
-- columns: regexp_replace:text
-- row: ax\nyc
-- rowcount: 1
-- end-expected
SELECT regexp_replace('abc', 'b', 'x\ny');
-- begin-expected
-- columns: regexp_replace:text
-- row: a\0c
-- rowcount: 1
-- end-expected
SELECT regexp_replace('abc','b','\0');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "start": 0
-- end-expected-error
SELECT regexp_substr('abc', 'a', 0);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "start": 0
-- end-expected-error
SELECT regexp_count('abc','a',0);
-- begin-expected-error
-- sqlstate: 2201B
-- message-like: invalid regular expression: invalid escape \ sequence
-- end-expected-error
SELECT 'abc' ~ '\';
-- begin-expected-error
-- sqlstate: 2201B
-- message-like: invalid regular expression: parentheses () not balanced
-- end-expected-error
SELECT 'abc' ~ '(';
-- begin-expected-error
-- sqlstate: 2201B
-- message-like: invalid regular expression: invalid repetition count(s)
-- end-expected-error
SELECT 'abc' ~ 'a{2,1}';
-- begin-expected-error
-- sqlstate: 2201B
-- message-like: invalid regular expression: invalid character range
-- end-expected-error
SELECT 'abc' ~ '[z-a]';
