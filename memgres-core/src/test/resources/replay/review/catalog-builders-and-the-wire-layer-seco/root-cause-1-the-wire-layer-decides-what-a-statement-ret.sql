-- source: review-2026-08.md
-- finding: Root cause 1: the wire layer decides what a statement returns by scanning its text, and discards the rows when it guesses wrong
-- area: Catalog builders and the wire layer, second pass
-- title: Root cause 1: the wire layer decides what a statement returns by scanning its text, and discards the rows when it guesses wrong
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
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) /* into */ FROM zz_vf2_t1;
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
