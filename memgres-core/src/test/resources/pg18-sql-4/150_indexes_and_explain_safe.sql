DROP SCHEMA IF EXISTS test_150 CASCADE;
CREATE SCHEMA test_150;
SET search_path TO test_150;

CREATE TABLE idx_test (
    id integer PRIMARY KEY,
    email text NOT NULL,
    amount integer NOT NULL,
    active boolean NOT NULL
);

CREATE UNIQUE INDEX uq_idx_test_email ON idx_test(email);
CREATE INDEX idx_idx_test_lower_email ON idx_test((lower(email)));
CREATE INDEX idx_idx_test_active_partial ON idx_test(id) WHERE active = true;

INSERT INTO idx_test VALUES
    (1, 'A@example.com', 10, true),
    (2, 'B@example.com', 20, false),
    (3, 'C@example.com', 30, true);

-- begin-expected
-- columns: id|email
-- row: 1|A@example.com
-- end-expected
SELECT id, email
FROM idx_test
WHERE lower(email) = 'a@example.com';

-- begin-expected
-- columns: id
-- row: 1
-- row: 3
-- end-expected
SELECT id
FROM idx_test
WHERE active = true
ORDER BY id;

-- begin-expected-error
-- message-like: duplicate key value
-- end-expected-error
INSERT INTO idx_test VALUES
    (4, 'A@example.com', 40, true);

DROP SCHEMA test_150 CASCADE;
