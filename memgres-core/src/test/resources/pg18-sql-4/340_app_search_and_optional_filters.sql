DROP SCHEMA IF EXISTS test_340 CASCADE;
CREATE SCHEMA test_340;
SET search_path TO test_340;

CREATE TABLE users (
    user_id integer PRIMARY KEY,
    name text NOT NULL,
    email text NOT NULL,
    status text NOT NULL,
    city text,
    created_at date NOT NULL
);

INSERT INTO users(user_id, name, email, status, city, created_at) VALUES
    (1, 'Alice Jones', 'alice@example.com', 'active',   'Amsterdam', DATE '2024-01-01'),
    (2, 'Bob Smith',   'bob@sample.org',    'invited',  'Rotterdam', DATE '2024-01-05'),
    (3, 'Alicia Reed', 'reed@example.com',  'active',   'Utrecht',   DATE '2024-02-01'),
    (4, 'Carol White', 'cw@example.net',    'disabled', 'Amsterdam', DATE '2024-02-15'),
    (5, 'Dave Brown',  'dave@sample.org',   'active',   NULL,        DATE '2024-03-01');

-- begin-expected
-- columns: user_id|name
-- row: 1|Alice Jones
-- row: 3|Alicia Reed
-- end-expected
SELECT user_id, name
FROM users
WHERE name ILIKE 'ali%'
ORDER BY user_id;

-- begin-expected
-- columns: user_id|name|email
-- row: 1|Alice Jones|alice@example.com
-- row: 3|Alicia Reed|reed@example.com
-- row: 4|Carol White|cw@example.net
-- end-expected
SELECT user_id, name, email
FROM users
WHERE lower(name) LIKE '%ali%'
   OR lower(email) LIKE '%example%'
ORDER BY user_id;

-- begin-expected
-- columns: user_id|name|status|city
-- row: 1|Alice Jones|active|Amsterdam
-- end-expected
SELECT user_id, name, status, city
FROM users
WHERE ('active' IS NULL OR status = 'active')
  AND ('Amsterdam' IS NULL OR city = 'Amsterdam')
  AND (DATE '2024-01-01' IS NULL OR created_at >= DATE '2024-01-01')
ORDER BY user_id;

-- begin-expected
-- columns: user_id|display_name
-- row: 1|Alice Jones <alice@example.com>
-- row: 2|Bob Smith <bob@sample.org>
-- row: 3|Alicia Reed <reed@example.com>
-- row: 4|Carol White <cw@example.net>
-- row: 5|Dave Brown <dave@sample.org>
-- end-expected
SELECT user_id, name || ' <' || email || '>' AS display_name
FROM users
ORDER BY user_id;

-- begin-expected
-- columns: user_id|name|city_label
-- row: 5|Dave Brown|(unknown)
-- row: 3|Alicia Reed|Utrecht
-- row: 2|Bob Smith|Rotterdam
-- row: 1|Alice Jones|Amsterdam
-- row: 4|Carol White|Amsterdam
-- end-expected
SELECT user_id, name, COALESCE(city, '(unknown)') AS city_label
FROM users
ORDER BY COALESCE(city, '(unknown)') DESC, user_id;

DROP SCHEMA test_340 CASCADE;
