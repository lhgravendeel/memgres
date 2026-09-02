-- source: review-2026-08.md
-- finding: Root cause 6: ON CONFLICT bypasses the duplicate-key wait that the plain INSERT path performs
-- area: Isolation, deadlocks and features in combination
-- title: Root cause 6: ON CONFLICT bypasses the duplicate-key wait that the plain INSERT path performs
-- session A                                   -- session B
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (i int PRIMARY KEY, v int);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t VALUES (7,70);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t VALUES (7,71)
                                                ON CONFLICT (i) DO UPDATE SET v = 999;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: count:int8 | string_agg:text
-- row: 0 | NULL
-- rowcount: 1
-- end-expected
SELECT count(*), string_agg(v::text,',') FROM zz_t WHERE i = 7;
