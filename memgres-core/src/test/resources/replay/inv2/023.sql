-- source: investigation-2026-08.md
-- finding: 23
-- title: SelectExecutor.containsAggregate() enumerates AST node types by hand and has no branch for BetweenExpr, ArrayExpr, AnyAllArrayExpr and friends, so an aggregate 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_ag (v int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_ag VALUES (10),(20),(30);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT max(v) BETWEEN 1 AND 100 FROM zz_vf_ag;
-- begin-expected
-- columns: array:_int4
-- row: {30}
-- rowcount: 1
-- end-expected
SELECT ARRAY[max(v)] FROM zz_vf_ag;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 30 = ANY (ARRAY[max(v)]) FROM zz_vf_ag;
