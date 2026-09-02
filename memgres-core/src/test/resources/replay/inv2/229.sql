-- source: investigation-2026-08.md
-- finding: 229
-- title: the COLLATION arm of the DROP switch is an empty block that only emits the IF EXISTS notice
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
