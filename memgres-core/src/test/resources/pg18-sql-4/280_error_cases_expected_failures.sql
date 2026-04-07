DROP SCHEMA IF EXISTS test_280 CASCADE;
CREATE SCHEMA test_280;
SET search_path TO test_280;

CREATE TABLE t (
    id integer PRIMARY KEY,
    n integer NOT NULL
);

INSERT INTO t VALUES (1, 10);

-- begin-expected-error
-- message-like: relation "missing_table" does not exist
-- end-expected-error
SELECT * FROM missing_table;

-- begin-expected-error
-- message-like: column "missing_col" does not exist
-- end-expected-error
SELECT missing_col FROM t;

-- begin-expected-error
-- message-like: invalid input syntax for type integer
-- end-expected-error
SELECT 'abc'::integer;

-- begin-expected-error
-- message-like: division by zero
-- end-expected-error
SELECT 10 / 0;

-- begin-expected-error
-- message-like: duplicate key value
-- end-expected-error
INSERT INTO t VALUES (1, 99);

DROP SCHEMA test_280 CASCADE;
