-- source: review-2026-08.md
-- finding: Root cause 2: the composite text format is read by a hand-rolled splitter that is not `record_in`
-- area: The parsers
-- title: Root cause 2: the composite text format is read by a hand-rolled splitter that is not `record_in`
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf2_ct AS (b boolean, s text, n int);
-- begin-expected
-- columns: b:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT ('(t,hi,1)'::zz_vf2_ct).b;
-- begin-expected
-- columns: b:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT ((ROW(true,'hi',1)::zz_vf2_ct)::text::zz_vf2_ct).b;
-- begin-expected
-- columns: s:text
-- row: a"b
-- rowcount: 1
-- end-expected
SELECT ('(t,"a""b",1)'::zz_vf2_ct).s;
-- begin-expected
-- columns: s:text
-- row: a"b
-- rowcount: 1
-- end-expected
SELECT ((ROW(true, 'a"b', 1)::zz_vf2_ct)::text::zz_vf2_ct).s;
-- begin-expected
-- columns: s:text
-- row: a,b
-- rowcount: 1
-- end-expected
SELECT ('(t,a\,b,1)'::zz_vf2_ct).s;
