-- source: investigation-2026-08.md
-- finding: 61
-- title: The regex layer compiles PostgreSQL AREs with java.util.regex after only a thin textual translation: javaFlags() never sets Pattern.UNICODE_CHARACTER_CLASS, tra
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "start": 0
-- end-expected-error
SELECT regexp_substr('abc', 'a', 0);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "start": 0
-- end-expected-error
SELECT regexp_instr('abc', 'a', 0);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "start": 0
-- end-expected-error
SELECT regexp_count('abc','a',0);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'é' ~ '[[:alpha:]]';
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'Ä' ~ '[[:upper:]]';
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'é' ~ '\w';
-- begin-expected
-- columns: regexp_replace:text
-- row: XXX
-- rowcount: 1
-- end-expected
SELECT regexp_replace('aéb','[[:alpha:]]','X','g');
-- begin-expected
-- columns: regexp_count:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT regexp_count('aéb','[[:alpha:]]');
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
SELECT 'é' SIMILAR TO '[[:alpha:]]';
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
-- sqlstate: 2201B
-- message-like: invalid regular expression: invalid escape \ sequence
-- end-expected-error
SELECT 'abc' ~ '\';
