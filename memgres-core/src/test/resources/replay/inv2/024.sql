-- source: investigation-2026-08.md
-- finding: 24
-- title: array_agg's result type is hardcoded, so the value is advertised (and downstream treated) as text[] whatever the element type; unnest, to_json, to_jsonb and = A
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ag (v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_ag VALUES (10),(20),(30);
-- begin-expected
-- columns: u:int4
-- row: 10
-- row: 20
-- row: 30
-- rowcount: 3
-- end-expected
SELECT unnest(array_agg(v)) AS u FROM zz_vf_ag ORDER BY u;
-- begin-expected
-- columns: to_json:json
-- row: [10,20,30]
-- rowcount: 1
-- end-expected
SELECT to_json(array_agg(v)) FROM zz_vf_ag;
-- begin-expected
-- columns: to_jsonb:jsonb
-- row: [10, 20, 30]
-- rowcount: 1
-- end-expected
SELECT to_jsonb(array_agg(v)) FROM zz_vf_ag;
