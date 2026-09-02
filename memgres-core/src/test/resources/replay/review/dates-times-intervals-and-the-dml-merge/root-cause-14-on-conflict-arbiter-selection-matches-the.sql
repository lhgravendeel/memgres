-- source: review-2026-08.md
-- finding: Root cause 14: ON CONFLICT arbiter selection matches the column list only
-- area: Dates, times, intervals — and the DML/MERGE/COPY findings filed with them
-- title: Root cause 14: ON CONFLICT arbiter selection matches the column list only
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_f6 (i int, m int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE UNIQUE INDEX zz_f6_p ON zz_f6 (m) WHERE m > 100;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_f6 VALUES (1, 200);
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zz_f6 VALUES (2, 200) ON CONFLICT (m) DO UPDATE SET i = 9;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_f8 (i int, j text, UNIQUE NULLS NOT DISTINCT (i));
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_f8 VALUES (NULL, 'a');
-- begin-expected
-- columns: j:text
-- row: c
-- rowcount: 1
-- end-expected
INSERT INTO zz_f8 VALUES (NULL, 'b') ON CONFLICT (i) DO UPDATE SET j = 'c' RETURNING j;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_f9 (i int, r int4range, EXCLUDE USING gist (r WITH &&));
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_f9 VALUES (1, '[1,5)');
-- begin-expected
-- ok: 0
-- end-expected
INSERT INTO zz_f9 VALUES (2, '[3,7)') ON CONFLICT DO NOTHING;
