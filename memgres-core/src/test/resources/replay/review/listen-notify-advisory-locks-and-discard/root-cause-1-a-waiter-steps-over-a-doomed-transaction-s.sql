-- source: review-2026-08.md
-- finding: Root cause 1: a waiter steps over a doomed transaction's row lock, and that transaction's rollback then replays its undo log over the waiter's write
-- area: LISTEN/NOTIFY, advisory locks and DISCARD
-- title: Root cause 1: a waiter steps over a doomed transaction's row lock, and that transaction's rollback then replays its undo log over the waiter's write
-- session A                                    -- session B
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_dl (i int PRIMARY KEY, v int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_dl VALUES (1,10),(2,20);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_dl SET v = 100 WHERE i = 1;
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_dl SET v = 200 WHERE i = 2;
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_dl SET v = 300 WHERE i = 2;
-- waits
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_dl SET v = 400 WHERE i = 1;
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: i:int4 | v:int4
-- row: 1 | 400
-- row: 2 | 300
-- rowcount: 2
-- end-expected
SELECT i, v FROM zz_dl ORDER BY i;
