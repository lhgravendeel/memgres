-- source: review-2026-08.md
-- finding: Root cause 13: each DML write path re-implements the SET loop, so the generated-column guard is on one of four
-- area: Isolation, deadlocks and features in combination
-- title: Root cause 13: each DML write path re-implements the SET loop, so the generated-column guard is on one of four
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (id int PRIMARY KEY, a int, g int GENERATED ALWAYS AS (a*2) STORED);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t (id,a) VALUES (1,1);
-- begin-expected-error
-- sqlstate: 428C9
-- message-like: column "g" can only be updated to DEFAULT
-- end-expected-error
UPDATE zz_t SET g = 3 WHERE id = 1;
-- begin-expected-error
-- sqlstate: 428C9
-- message-like: column "g" can only be updated to DEFAULT
-- end-expected-error
INSERT INTO zz_t (id,a) VALUES (1,7) ON CONFLICT (id) DO UPDATE SET g = 3;
-- begin-expected-error
-- sqlstate: 428C9
-- message-like: column "g" can only be updated to DEFAULT
-- end-expected-error
MERGE INTO zz_t t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET g = 99;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_s (id int, nv int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_s VALUES (1, 55);
-- begin-expected-error
-- sqlstate: 428C9
-- message-like: column "g" can only be updated to DEFAULT
-- end-expected-error
UPDATE zz_t t SET g = s.nv FROM zz_s s WHERE t.id = s.id;
