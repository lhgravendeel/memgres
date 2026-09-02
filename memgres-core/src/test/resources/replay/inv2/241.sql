-- source: investigation-2026-08.md
-- finding: 241
-- title: Write-side row selection through an auto-updatable view never looks at the view's WHERE: resolveViewToBaseTable builds a column mapping from sel.targets() and i
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vb (id int PRIMARY KEY, a int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vb VALUES (1,5),(2,50);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_vv AS SELECT id, a FROM zz_vb WHERE a < 10;
-- begin-expected
-- ok: 0
-- end-expected
UPDATE zz_vv SET a = 6 WHERE id = 2;
-- begin-expected
-- columns: id:int4 | a:int4
-- row: 1 | 5
-- row: 2 | 50
-- rowcount: 2
-- end-expected
SELECT id, a FROM zz_vb ORDER BY id;
-- begin-expected
-- ok: 1
-- end-expected
DELETE FROM zz_vv;
-- begin-expected
-- columns: id:int4 | a:int4
-- row: 2 | 50
-- rowcount: 1
-- end-expected
SELECT id, a FROM zz_vb ORDER BY id;
