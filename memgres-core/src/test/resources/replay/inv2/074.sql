-- source: investigation-2026-08.md
-- finding: 74
-- title: Unrelated singletons in this area
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'abc' LIKE ANY (ARRAY['x%','a%']);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'abc' ILIKE ANY (ARRAY['X%','A%']);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'abc' NOT LIKE ALL (ARRAY['x%','y%']);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'abc' ~ ANY (ARRAY['^x','^a']);
-- begin-expected
-- columns: length:int4
-- row: 5
-- rowcount: 1
-- end-expected
SELECT length(btrim(E' \tabc\n '));
-- begin-expected
-- columns: ?column?:text
-- row: [	abc	]
-- rowcount: 1
-- end-expected
SELECT '[' || trim(both from E'\tabc\t') || ']';
-- begin-expected
-- columns: length:int4
-- row: 32
-- rowcount: 1
-- end-expected
SELECT length(sha256('abc'::bytea));
-- begin-expected
-- columns: pg_typeof:regtype
-- row: bytea
-- rowcount: 1
-- end-expected
SELECT pg_typeof(sha256('abc'::bytea));
-- begin-expected
-- columns: encode:text
-- row: ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
-- rowcount: 1
-- end-expected
SELECT encode(sha256('abc'::bytea),'hex');
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(*) FROM regexp_matches(E'a\nb', '.', 'g');
-- begin-expected
-- columns: count:int8
-- row: 5
-- rowcount: 1
-- end-expected
SELECT count(*) FROM regexp_matches(E'a\nb\nc', '.', 'g');
-- begin-expected-error
-- sqlstate: 22025
-- message-like: invalid escape string
-- end-expected-error
SELECT 'abc' SIMILAR TO 'abc' ESCAPE 'xy';
-- begin-expected
-- columns: ?column?:bool
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT 'abc' LIKE 'abc' ESCAPE NULL;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'a' SIMILAR TO '.' ESCAPE '!';
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'a1' SIMILAR TO 'a\d' ESCAPE '!';
-- begin-expected
-- columns: overlay:text
-- row: aXYabcdef
-- rowcount: 1
-- end-expected
SELECT overlay('abcdef' placing 'XY' from 2 for -1);
-- begin-expected
-- columns: overlay:text
-- row: abXYabcdef
-- rowcount: 1
-- end-expected
SELECT overlay('abcdef' placing 'XY' from 3 for -2);
-- begin-expected
-- columns: overlay:bytea
-- row: \x01ff010203
-- rowcount: 1
-- end-expected
SELECT overlay('\x010203'::bytea placing '\xff'::bytea from 2 for -1);
-- begin-expected
-- columns: substring:text
-- row: abc
-- rowcount: 1
-- end-expected
SELECT substring('abc' SIMILAR 'abc' ESCAPE '#');
-- begin-expected
-- columns: substring:text
-- row: abc
-- rowcount: 1
-- end-expected
SELECT substring('abc' from 'a%c' for '#');
-- begin-expected
-- columns: substring:text
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT substring('abc' from '(x)?b');
