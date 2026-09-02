-- source: investigation.md
-- finding: 107
-- title: `TRUNCATE` of a foreign-key pair together is rejected ⚠️ rejects valid SQL
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "fk_parent" does not exist
-- end-expected-error
TRUNCATE fk_parent, fk_child;
--   PG: succeeds — truncating both together leaves no dangling reference
--   mg: 0A000 cannot truncate a table referenced in a foreign key constraint;
