-- source: investigation-2026-08.md
-- finding: 58
-- title: tsquery evaluation and ranking drop query structure: PHRASE falls back to `left.matches(v) && right.matches(v)` when either side is not a bare TERM, phrase() di
-- begin-expected
-- columns: ts_delete:text
-- row: 'cat':1 'dog':3
-- rowcount: 1
-- end-expected
SELECT ts_delete(to_tsvector('english','cats and dogs'), 'cats')::text;
-- begin-expected
-- columns: text:text
-- row: false
-- rowcount: 1
-- end-expected
SELECT (to_tsvector('simple','a c b') @@ to_tsquery('simple','a <-> (b & c)'))::text;
-- begin-expected
-- columns: to_tsquery:text
-- row: 'cat' <2> 'dog'
-- rowcount: 1
-- end-expected
SELECT to_tsquery('english', 'the <-> cat <-> the <-> dog')::text;
-- begin-expected
-- columns: text:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT ('a'::tsquery @> 'a'::tsquery)::text;
-- begin-expected
-- columns: text:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT ('a & b'::tsquery @> 'a'::tsquery)::text;
-- begin-expected
-- columns: text:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT ('a'::tsquery <@ 'a & b'::tsquery)::text;
-- begin-expected
-- columns: text:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT ('cat dog'::tsvector @@ 'cat:A'::tsquery)::text;
-- begin-expected
-- columns: text:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT (strip('a:1A'::tsvector) @@ 'a:A'::tsquery)::text;
-- begin-expected
-- columns: ts_rank:text
-- row: 1e-20
-- rowcount: 1
-- end-expected
SELECT ts_rank('a:1'::tsvector, 'a & !b'::tsquery)::text;
-- begin-expected
-- columns: ts_rank:text
-- row: 0.09910322
-- rowcount: 1
-- end-expected
SELECT ts_rank('a:1 b:2'::tsvector, 'a & !b'::tsquery)::text;
-- begin-expected
-- columns: ts_rank:text
-- row: 0.06079271
-- rowcount: 1
-- end-expected
SELECT ts_rank(to_tsvector('simple','a b c d e'), to_tsquery('simple','!a'))::text;
-- begin-expected
-- columns: ts_rank:text
-- row: 1e-16
-- rowcount: 1
-- end-expected
SELECT ts_rank(strip('a:1 b:2'::tsvector), 'a & b'::tsquery)::text;
-- begin-expected
-- columns: ts_rank_cd:text
-- row: 0.2
-- rowcount: 1
-- end-expected
SELECT ts_rank_cd('a:1,10,20 b:2,11'::tsvector, 'a <-> b'::tsquery, 0)::text;
