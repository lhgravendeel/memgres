-- source: investigation-2026-08.md
-- finding: 64
-- title: Position-taking string functions and matchLike walk Java chars (UTF-16 code units) while length() counts code points, so every function disagrees with length() 
-- begin-expected
-- columns: substr:text
-- row: b
-- rowcount: 1
-- end-expected
SELECT substr(U&'a\+01F600b', 3, 1);
-- begin-expected
-- columns: overlay:text
-- row: aXb
-- rowcount: 1
-- end-expected
SELECT overlay(U&'a\+01F600b' placing 'X' from 2 for 1);
-- begin-expected
-- columns: left:text
-- row: a😀
-- rowcount: 1
-- end-expected
SELECT left(U&'a\+01F600b', 2);
-- begin-expected
-- columns: length:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT length(lpad(U&'\+01F600',3,'x'));
-- begin-expected
-- columns: strpos:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT strpos(U&'a\+01F600b', 'b');
-- begin-expected
-- columns: ascii:int4
-- row: 128512
-- rowcount: 1
-- end-expected
SELECT ascii(U&'\+01F600');
-- begin-expected
-- columns: array_length:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT array_length(string_to_array(U&'a\+01F600b', NULL), 1);
-- begin-expected
-- columns: text:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT (U&'\+01F600' LIKE '_')::text;
-- begin-expected
-- columns: text:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT (U&'a\+01F600b' LIKE 'a_b')::text;
