-- source: investigation.md
-- finding: 100
-- title: Four array types are still named by OID
-- begin-expected
-- columns: case:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT CASE WHEN 1=0 THEN 1/0 WHEN 1=1 THEN 1 ELSE 2/0 END;
-- 1, no error
-- begin-expected
-- columns: case:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT CASE 1 WHEN 0 THEN 1/0 WHEN 1 THEN 1 ELSE 2/0 END;
-- 1
-- begin-expected
-- columns: case:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT CASE WHEN true THEN 1 ELSE 1/0 END;
-- 1
-- begin-expected
-- columns: coalesce:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT COALESCE(1, 1/0);
-- 1
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT false AND 1/0 = 1;
-- false
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT true OR 1/0 = 1;
-- true;
