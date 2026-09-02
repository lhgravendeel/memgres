-- source: investigation.md
-- finding: 115
-- title: Domain constraint validation
-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting NULL/NOT NULL constraints
-- end-expected-error
CREATE DOMAIN d AS int4 NOT NULL NULL;
-- PG: 42601 conflicting NULL/NOT NULL | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: multiple default expressions
-- end-expected-error
CREATE DOMAIN d AS int4 DEFAULT 3 DEFAULT 3;
-- PG: 42601 multiple default expressions | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unique constraints not possible for domains
-- end-expected-error
CREATE DOMAIN d AS int4 UNIQUE;
-- PG: 42601 unique constraints not possible for domains | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: primary key constraints not possible for domains
-- end-expected-error
CREATE DOMAIN d AS int4 PRIMARY KEY;
-- PG: 42601 | mg: OK
-- begin-expected-error
-- sqlstate: 42601
-- message-like: foreign key constraints not possible for domains
-- end-expected-error
CREATE DOMAIN d AS int4 CONSTRAINT c REFERENCES t(i);
-- PG: 42601 foreign key not possible | mg: OK;
