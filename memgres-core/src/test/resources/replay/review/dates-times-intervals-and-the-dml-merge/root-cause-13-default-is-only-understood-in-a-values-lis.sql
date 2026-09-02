-- source: review-2026-08.md
-- finding: Root cause 13: DEFAULT is only understood in a VALUES list
-- area: Dates, times, intervals — and the DML/MERGE/COPY findings filed with them
-- title: Root cause 13: DEFAULT is only understood in a VALUES list
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
