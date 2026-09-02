-- source: investigation-2026-08.md
-- finding: 82
-- title: The phrase operator's tree shape and distance are not part of a tsquery's identity: parsePhrase folds left, the printer emits no parentheses around a PHRASE chi
-- begin-expected
-- columns: text:text
-- row: 'a' <-> ( 'b' <-> 'c' )
-- rowcount: 1
-- end-expected
SELECT 'a <-> (b <-> c)'::tsquery::text;
-- begin-expected
-- columns: text:text
-- row: 'a' <-> ( 'b' <-> 'c' )
-- rowcount: 1
-- end-expected
SELECT ('a <-> (b <-> c)'::tsquery::text)::tsquery::text;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'a <-> (b <-> c)'::tsquery = 'a <-> b <-> c'::tsquery;
-- begin-expected
-- columns: text:text
-- row: 'a' <2> ( 'b' <-> 'c' )
-- rowcount: 1
-- end-expected
SELECT 'a <2> (b <-> c)'::tsquery::text;
-- begin-expected
-- columns: ts_rewrite:text
-- row: 'a' <2> 'b'
-- rowcount: 1
-- end-expected
SELECT ts_rewrite('a <2> b'::tsquery, 'a <-> b'::tsquery, 'zz'::tsquery)::text;
-- begin-expected
-- columns: ts_rewrite:text
-- row: 'a' <-> 'b'
-- rowcount: 1
-- end-expected
SELECT ts_rewrite('a <-> b'::tsquery, 'a <2> b'::tsquery, 'zz'::tsquery)::text;
-- begin-expected
-- columns: ts_rewrite:text
-- row: 'x' & 'a' <5> 'b'
-- rowcount: 1
-- end-expected
SELECT ts_rewrite('x & (a <5> b)'::tsquery, 'a <-> b'::tsquery, 'zz'::tsquery)::text;
