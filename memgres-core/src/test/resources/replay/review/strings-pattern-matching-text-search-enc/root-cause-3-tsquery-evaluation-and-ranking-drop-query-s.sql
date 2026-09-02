-- source: review-2026-08.md
-- finding: Root cause 3: tsquery evaluation and ranking drop query structure
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Root cause 3: tsquery evaluation and ranking drop query structure
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT to_tsvector('simple','a c b') @@ to_tsquery('simple','a <-> (b & c)');
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'a'::tsquery @> 'a'::tsquery;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'a'::tsquery <@ 'a & b'::tsquery;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'cat dog'::tsvector @@ 'cat:A'::tsquery;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT strip('a:1A'::tsvector) @@ 'a:A'::tsquery;
-- begin-expected
-- columns: ts_rank:float4
-- row: 1e-20
-- rowcount: 1
-- end-expected
SELECT ts_rank('a:1'::tsvector, 'a & !b'::tsquery);
-- begin-expected
-- columns: ts_rank:float4
-- row: 0.09910322
-- rowcount: 1
-- end-expected
SELECT ts_rank('a:1 b:2'::tsvector, 'a & !b'::tsquery);
-- begin-expected
-- columns: ts_rank:float4
-- row: 1e-16
-- rowcount: 1
-- end-expected
SELECT ts_rank(strip('a:1 b:2'::tsvector), 'a & b'::tsquery);
-- begin-expected
-- columns: ts_rank_cd:float4
-- row: 0.2
-- rowcount: 1
-- end-expected
SELECT ts_rank_cd('a:1,10,20 b:2,11'::tsvector, 'a <-> b'::tsquery, 0);
-- begin-expected
-- columns: to_tsquery:text
-- row: 'cat' <2> 'dog'
-- rowcount: 1
-- end-expected
SELECT to_tsquery('english', 'the <-> cat <-> the <-> dog')::text;
-- begin-expected
-- columns: to_tsquery:text
-- row: 'cat' <-> 'dog'
-- rowcount: 1
-- end-expected
SELECT to_tsquery('english', '''cat dog''')::text;
-- begin-expected
-- columns: ts_delete:text
-- row: 'cat':1 'dog':3
-- rowcount: 1
-- end-expected
SELECT ts_delete(to_tsvector('english','cats and dogs'), 'cats')::text;
-- begin-expected
-- columns: querytree:text
-- row: 'a' <-> 'b'
-- rowcount: 1
-- end-expected
SELECT querytree('a <-> b'::tsquery);
-- begin-expected
-- columns: querytree:text
-- row: 'a':*
-- rowcount: 1
-- end-expected
SELECT querytree('a:*'::tsquery);
-- begin-expected
-- columns: querytree:text
-- row: 
-- rowcount: 1
-- end-expected
SELECT querytree(''::tsquery);
-- begin-expected
-- columns: querytree:text
-- row: T
-- rowcount: 1
-- end-expected
SELECT querytree('a | !b'::tsquery);
-- begin-expected
-- columns: ts_rewrite:text
-- row: 'x' <-> 'y' & 'a'
-- rowcount: 1
-- end-expected
SELECT ts_rewrite('a & b'::tsquery, 'b'::tsquery, 'x <-> y'::tsquery)::text;
