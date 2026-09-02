-- source: investigation.md
-- finding: 119
-- title: No cycle detection in foreign-key cascades ⚠️ high
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE fa (id int PRIMARY KEY, b int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE fb (id int PRIMARY KEY, a int REFERENCES fa(id) ON DELETE CASCADE);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE fa ADD FOREIGN KEY (b) REFERENCES fb(id) ON DELETE CASCADE;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO fa VALUES (1, NULL);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO fb VALUES (1, 1);
-- begin-expected
-- ok: 1
-- end-expected
UPDATE fa SET b = 1 WHERE id = 1;
-- close the reference cycle
-- begin-expected
-- ok: 1
-- end-expected
DELETE FROM fa WHERE id = 1;
--   PG: 0 rows remain — the cascade visits each row once
--   mg: XX000 Internal error: StackOverflowError
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE t (a int PRIMARY KEY, b int, FOREIGN KEY (b) REFERENCES t(a) ON DELETE CASCADE);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO t VALUES (0, 0);
-- the row references itself
-- begin-expected
-- ok: 1
-- end-expected
DELETE FROM t WHERE a = 0;
--   PG: succeeds, 0 rows remain
--   mg: 23503 insert or update on table "t" violates foreign key constraint;
