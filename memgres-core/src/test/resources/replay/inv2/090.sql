-- source: investigation-2026-08.md
-- finding: 90
-- title: The two DO INSTEAD rule paths never consult the rule's stored qualification, so a conditional rule fires for every row and suppresses every row
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_r2 (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE RULE zz_r2_r AS ON INSERT TO zz_r2 WHERE NEW.i > 100 DO INSTEAD NOTHING;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_r2 VALUES (1);
-- begin-expected
-- columns: count:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM zz_r2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_r4 (i int, v text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_r4 VALUES (1,'a');
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_r4v AS SELECT i, v FROM zz_r4;
-- begin-expected
-- ok: 0
-- end-expected
CREATE RULE zz_r4_r AS ON UPDATE TO zz_r4v WHERE NEW.i > 5 DO INSTEAD UPDATE zz_r4 SET v = NEW.v WHERE i = OLD.i;
-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot update view "zz_r4v"
-- end-expected-error
UPDATE zz_r4v SET v='q' WHERE i=1;
