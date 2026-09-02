-- source: investigation-2026-08.md
-- finding: 278
-- title: The wire layer decides what a statement returns by scanning its raw text: isSelectInto walks the uppercased SQL for " INTO " tracking only single quotes and par
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_t1 (a int, b text);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf2_t1 VALUES (1,'x'),(2,'y');
-- begin-expected
-- columns: a:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT a /* copy into archive */ FROM zz_vf2_t1 ORDER BY a;
-- begin-expected
-- columns: ?column?:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT 1 /* insert into log */;
-- begin-expected
-- columns: total into sum:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT 1 AS "total into sum";
-- begin-expected
-- columns: s:text
-- row:  into 
-- rowcount: 1
-- end-expected
SELECT $$ into $$ AS s;
-- begin-expected
-- columns: a:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT a FROM zz_vf2_t1 -- pulled into the report
ORDER BY a;
