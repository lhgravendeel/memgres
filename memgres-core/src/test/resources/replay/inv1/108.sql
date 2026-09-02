-- source: investigation.md
-- finding: 108
-- title: Inheritance conflict detection is absent (3 cases)
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "p1" does not exist
-- end-expected-error
CREATE TABLE c () INHERITS (p1, p2);
-- p1.shared int, p2.shared bigint
--   PG: 42804 inherited column "shared" has a type conflict | mg: created
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "p3" does not exist
-- end-expected-error
CREATE TABLE c () INHERITS (p3, p4);
-- same column, different DEFAULT on each
--   PG: 42611 column "tomorrow" inherits conflicting default values | mg: created
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "p1" does not exist
-- end-expected-error
CREATE TABLE c (shared bigint) INHERITS (p1);
-- child redeclares with a wider type
--   PG: 42804 column "shared" has a type conflict | mg: created;
