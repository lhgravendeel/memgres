-- source: investigation-2026-08.md
-- finding: 99
-- title: The DEFAULT marker is only special-cased in a plain INSERT VALUES list and a plain UPDATE SET; everywhere else it reaches the generic literal evaluator, and whe
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_fb (i int PRIMARY KEY, v int DEFAULT 7);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_fb VALUES (1, 1);
-- begin-expected
-- columns: v:int4
-- row: 7
-- rowcount: 1
-- end-expected
INSERT INTO zz_fb VALUES (1, 4) ON CONFLICT (i) DO UPDATE SET v = DEFAULT RETURNING v;
-- begin-expected
-- ok: 1
-- end-expected
MERGE INTO zz_fb t USING (VALUES (1)) s(i) ON t.i = s.i
  WHEN MATCHED THEN UPDATE SET v = DEFAULT;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_id (i int GENERATED ALWAYS AS IDENTITY, j int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_id (j) VALUES (1);
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_id SET i = DEFAULT;
