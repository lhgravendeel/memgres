-- source: investigation-2026-08.md
-- finding: 102
-- title: ON CONFLICT arbiter selection matches on the conflict target's column list alone: the index predicate, the DEFERRABLE flag and NULLS NOT DISTINCT are never cons
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
INSERT INTO zz_f8 VALUES (NULL, 'd') ON CONFLICT DO NOTHING;
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
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_fa (i int, t text, v int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE UNIQUE INDEX zz_fa_low ON zz_fa (lower(t));
-- begin-expected
-- ok: 0
-- end-expected
CREATE UNIQUE INDEX zz_fa_i ON zz_fa (i);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_fa VALUES (1, 'Abc', 1);
-- begin-expected
-- columns: i:int4 | v:int4
-- row: 1 | 2
-- rowcount: 1
-- end-expected
INSERT INTO zz_fa VALUES (2, 'ABC', 2) ON CONFLICT (lower(t)) DO UPDATE SET v = EXCLUDED.v RETURNING i, v;
-- begin-expected
-- columns: i:int4
-- row: 3
-- rowcount: 1
-- end-expected
INSERT INTO zz_fa VALUES (3, 'q', 3) ON CONFLICT (i int4_ops) DO NOTHING RETURNING i;
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
CREATE TABLE zz_f7 (i int, CONSTRAINT zz_f7_u UNIQUE (i) DEFERRABLE);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_f7 VALUES (1);
-- begin-expected-error
-- sqlstate: 55000
-- message-like: ON CONFLICT does not support deferrable unique constraints/exclusion constraints as arbiters
-- end-expected-error
INSERT INTO zz_f7 VALUES (1) ON CONFLICT (i) DO NOTHING;
