DROP SCHEMA IF EXISTS test_630 CASCADE;
CREATE SCHEMA test_630;
SET search_path TO test_630;

CREATE TABLE imported_users (
    row_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email_raw text NOT NULL,
    full_name text NOT NULL
);

INSERT INTO imported_users(email_raw, full_name) VALUES
(' Alice@example.com ','Alice A'),
('alice@example.com','Alice Alt'),
('BOB@example.com','Bob B'),
('bob@example.com ','Bobby'),
('carol@example.com','Carol C');

-- begin-expected
-- columns: normalized_email,count
-- row: alice@example.com|2
-- row: bob@example.com|2
-- end-expected
SELECT lower(trim(email_raw)) AS normalized_email, COUNT(*)
FROM imported_users
GROUP BY lower(trim(email_raw))
HAVING COUNT(*) > 1
ORDER BY normalized_email;

-- begin-expected
-- columns: row_id,normalized_email
-- row: 1|alice@example.com
-- row: 2|alice@example.com
-- row: 3|bob@example.com
-- row: 4|bob@example.com
-- row: 5|carol@example.com
-- end-expected
SELECT row_id, lower(trim(email_raw)) AS normalized_email
FROM imported_users
ORDER BY row_id;

-- begin-expected
-- columns: canonical_row_id,normalized_email
-- row: 1|alice@example.com
-- row: 3|bob@example.com
-- row: 5|carol@example.com
-- end-expected
SELECT MIN(row_id) AS canonical_row_id, lower(trim(email_raw)) AS normalized_email
FROM imported_users
GROUP BY lower(trim(email_raw))
ORDER BY canonical_row_id;

