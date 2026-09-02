-- source: investigation-2026-08.md
-- finding: 154
-- title: ALTER VIEW actions and view-dependency updates rewrite the view rather than preserving its published contract: a base-column rename is substituted into the SELE
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_v2t (i int, v text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_v2v AS SELECT i, v FROM zz_v2t;
-- begin-expected
-- ok: 0
-- end-expected
ALTER VIEW zz_v2v ALTER COLUMN v SET DEFAULT 'zz';
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_v2v (i) VALUES (1);
-- begin-expected
-- columns: i:int4 | v:text
-- row: 1 | zz
-- rowcount: 1
-- end-expected
SELECT i, v FROM zz_v2t;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_v1t (i int, v text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_v1t VALUES (1,'a');
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_v1v AS SELECT i, v FROM zz_v1t;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_v1t RENAME COLUMN v TO v2;
-- begin-expected
-- columns: v:text
-- row: a
-- rowcount: 1
-- end-expected
SELECT v FROM zz_v1v;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_v2t" already exists
-- end-expected-error
CREATE TABLE zz_v2t (i int, v text);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_v2v" already exists
-- end-expected-error
CREATE VIEW zz_v2v AS SELECT i, v FROM zz_v2t;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: ALTER action ADD COLUMN cannot be performed on relation "zz_v2v"
-- end-expected-error
ALTER VIEW zz_v2v ADD COLUMN q int;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: ALTER action ALTER COLUMN ... SET NOT NULL cannot be performed on relation "zz_v2v"
-- end-expected-error
ALTER VIEW zz_v2v ALTER COLUMN v SET NOT NULL;
