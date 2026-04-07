DROP SCHEMA IF EXISTS test_130 CASCADE;
CREATE SCHEMA test_130;
SET search_path TO test_130;

CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

CREATE DOMAIN positive_int AS integer
    CHECK (VALUE > 0);

CREATE TABLE typed_test (
    id integer PRIMARY KEY,
    current_mood mood NOT NULL,
    qty positive_int NOT NULL,
    validity daterange NOT NULL
);

INSERT INTO typed_test VALUES
    (1, 'ok', 2, daterange('2024-01-01', '2024-02-01', '[)')),
    (2, 'happy', 5, daterange('2024-01-15', '2024-03-01', '[)'));

-- begin-expected
-- columns: id|current_mood|mood_ge_ok
-- row: 1|ok|true
-- row: 2|happy|true
-- end-expected
SELECT id, current_mood, (current_mood >= 'ok') AS mood_ge_ok
FROM typed_test
ORDER BY id;

-- begin-expected
-- columns: id|contains_day|overlaps_other
-- row: 1|true|true
-- row: 2|true|true
-- end-expected
SELECT
    id,
    validity @> DATE '2024-01-20' AS contains_day,
    validity && daterange('2024-01-20', '2024-01-25', '[)') AS overlaps_other
FROM typed_test
ORDER BY id;

-- begin-expected-error
-- message-like: value for domain positive_int violates check constraint
-- end-expected-error
INSERT INTO typed_test VALUES
    (3, 'sad', 0, daterange('2024-01-01', '2024-01-02', '[)'));

DROP SCHEMA test_130 CASCADE;
