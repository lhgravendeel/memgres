-- source: investigation.md
-- finding: 32
-- title: Transaction read-only precedence is inverted ⚠️ high — rejects valid SQL
-- begin-expected
-- ok: 0
-- end-expected
SET default_transaction_read_only = on;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN READ WRITE;
-- explicitly asks for a writable transaction
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE t (a int);
-- PG: OK | mg: 25006 cannot execute CREATE TABLE in a read-only transaction
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO t VALUES (1);
-- PG: OK | mg: 25006;
