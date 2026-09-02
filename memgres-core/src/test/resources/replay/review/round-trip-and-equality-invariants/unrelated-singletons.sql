-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: Round-trip and equality invariants
-- title: Unrelated singletons
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
