-- source: investigation-2026-08.md
-- finding: 344
-- title: The composite text format is read by a hand-rolled splitter that is not record_in: splitCompositeString toggles on '"' with no backslash case and no doubled-quo
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
