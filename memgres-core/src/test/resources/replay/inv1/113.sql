-- source: investigation.md
-- finding: 113
-- title: `ALTER TABLE` on the wrong relation kind, and `DROP` kind confusion
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "a_view" does not exist
-- end-expected-error
ALTER TABLE a_view ALTER COLUMN i SET NOT NULL;
--   PG: 42809 ALTER action cannot be performed on relation "a_view" | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "a_sequence" does not exist
-- end-expected-error
ALTER TABLE a_sequence ADD COLUMN z int;
-- PG: 42809 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: view "v2" does not exist
-- end-expected-error
DROP VIEW v2;
-- v2 IS a view
--   PG: succeeds | mg: 42809 "v2" is not a view          ← rejects valid SQL
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: table "t" does not exist
-- end-expected-error
DROP TABLE t;
-- dependents already dropped
--   PG: succeeds | mg: 2BP01 cannot drop because other objects depend on it;
