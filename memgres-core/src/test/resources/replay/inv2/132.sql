-- source: investigation-2026-08.md
-- finding: 132
-- title: Referential actions (ON DELETE / ON UPDATE SET NULL and SET DEFAULT) rewrite the child row in place and never re-validate it, so NOT NULL, CHECK and domain cons
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_p (id int PRIMARY KEY);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_c (p int NOT NULL REFERENCES zz_vf_p(id) ON DELETE SET NULL);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_p VALUES (1);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_c VALUES (1);
-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "p" of relation "zz_vf_c" violates not-null constraint
-- end-expected-error
DELETE FROM zz_vf_p WHERE id = 1;
-- begin-expected
-- columns: pnull:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT p IS NULL AS pnull FROM zz_vf_c;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_p3 (id int PRIMARY KEY);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_c3 (p int REFERENCES zz_vf_p3(id) ON DELETE SET DEFAULT DEFAULT 99, CHECK (p < 50));
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_p3 VALUES (1),(99);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_c3 VALUES (1);
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zz_vf_c3" violates check constraint "zz_vf_c3_p_check"
-- end-expected-error
DELETE FROM zz_vf_p3 WHERE id = 1;
-- begin-expected
-- columns: p:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT p FROM zz_vf_c3;
