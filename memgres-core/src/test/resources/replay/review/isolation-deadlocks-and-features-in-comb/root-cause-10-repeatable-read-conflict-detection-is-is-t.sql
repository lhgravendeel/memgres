-- source: review-2026-08.md
-- finding: Root cause 10: REPEATABLE READ conflict detection is "is this exact row array in my snapshot list"
-- area: Isolation, deadlocks and features in combination
-- title: Root cause 10: REPEATABLE READ conflict detection is "is this exact row array in my snapshot list"
-- session A                                   -- session B
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (i int PRIMARY KEY, v int, s text);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c');
-- begin-expected
-- ok: 0
-- end-expected
BEGIN ISOLATION LEVEL REPEATABLE READ;
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_t;
-- 3, snapshot taken
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t VALUES (4,40,'d');
-- begin-expected
-- ok: 4
-- end-expected
UPDATE zz_t SET s = 'x';
-- begin-expected
-- columns: count:int8
-- row: 4
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_t;
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected
-- columns: i:int4 | s:text
-- row: 1 | x
-- row: 2 | x
-- row: 3 | x
-- row: 4 | x
-- rowcount: 4
-- end-expected
SELECT i, s FROM zz_t ORDER BY i;
-- session A                                   -- session B
-- begin-expected
-- ok: 0
-- end-expected
BEGIN ISOLATION LEVEL REPEATABLE READ;
-- begin-expected
-- columns: count:int8
-- row: 4
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_t;
-- begin-expected
-- ok: 1
-- end-expected
DELETE FROM zz_t WHERE i = 2;
-- begin-expected
-- ok: 0
-- end-expected
DELETE FROM zz_t WHERE i = 2;
-- begin-expected
-- columns: ?column?:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT 1;
-- session A                                   -- session B
-- begin-expected
-- ok: 0
-- end-expected
BEGIN ISOLATION LEVEL REPEATABLE READ;
-- begin-expected
-- columns: ?column?:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT 1;
-- snapshot taken
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_u (i int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_u VALUES (1),(2);
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_class WHERE relname = 'zz_u';
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_u;
