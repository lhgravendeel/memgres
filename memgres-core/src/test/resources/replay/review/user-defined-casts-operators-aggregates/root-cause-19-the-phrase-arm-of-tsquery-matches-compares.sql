-- source: review-2026-08.md
-- finding: Root cause 19: the PHRASE arm of TsQuery.matches compares positions and nothing else
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 19: the PHRASE arm of TsQuery.matches compares positions and nothing else
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT to_tsvector('simple','supernova star')  @@ to_tsquery('simple','supern:* <-> star');
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT to_tsvector('simple','star supernova')  @@ to_tsquery('simple','star <-> supern:*');
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT to_tsvector('english','supernovae stars') @@ to_tsquery('english','sup:* <-> star');
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT to_tsvector('simple','alpha beta gamma') @@ to_tsquery('simple','alpha <2> gam:*');
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT to_tsvector('simple','alpha beta')       @@ 'alp:* <-> beta'::tsquery;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'a:1A b:2B'::tsvector @@ 'a:B <-> b:B'::tsquery;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'a:1A b:2B'::tsvector @@ 'a:A <-> b:A'::tsquery;
