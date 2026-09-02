-- source: review-2026-08.md
-- finding: Root cause 11: the COLLATION arm of DROP is an empty block
-- area: SQL-level cursors
-- title: Root cause 11: the COLLATION arm of DROP is an empty block
-- begin-expected
-- ok: 0
-- end-expected
CREATE COLLATION zz_coll (LOCALE = 'C');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a text COLLATE zz_coll);
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop collation zz_coll because other objects depend on it
-- end-expected-error
DROP COLLATION zz_coll;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_t;
-- begin-expected
-- ok: 0
-- end-expected
DROP COLLATION zz_coll;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_collation WHERE collname = 'zz_coll';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation "zz_coll" for encoding "UTF8" does not exist
-- end-expected-error
SELECT 'a' COLLATE zz_coll;
