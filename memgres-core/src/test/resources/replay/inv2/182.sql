-- source: investigation-2026-08.md
-- finding: 182
-- title: the PHRASE arm of TsQuery.matches looks the bare lexeme up in the position map and never reads the operand's prefix flag or weight set, both of which the TERM a
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
