-- source: investigation-2026-08.md
-- finding: 370
-- title: Unrelated singletons in this area
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_rt_ch (v "char");
-- begin-expected
-- columns: to_jsonb:text
-- row: "x"
-- rowcount: 1
-- end-expected
SELECT to_jsonb('"x"'::jsonb)::text;
-- begin-expected
-- columns: to_jsonb:text
-- row: "x"
-- rowcount: 1
-- end-expected
SELECT to_jsonb(to_jsonb('"x"'::jsonb))::text;
