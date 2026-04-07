DROP SCHEMA IF EXISTS test_880 CASCADE;
CREATE SCHEMA test_880;
SET search_path TO test_880;

CREATE TABLE plan_history (
    account_id integer NOT NULL,
    valid_from date NOT NULL,
    valid_to date,
    plan_name text NOT NULL,
    PRIMARY KEY (account_id, valid_from)
);

INSERT INTO plan_history VALUES
(1, '2024-01-01', '2024-02-01', 'free'),
(1, '2024-02-01', NULL, 'pro'),
(2, '2024-01-10', NULL, 'team');

-- begin-expected
-- columns: account_id,plan_name
-- row: 1|free
-- row: 2|team
-- end-expected
SELECT account_id, plan_name
FROM plan_history
WHERE DATE '2024-01-20' >= valid_from
  AND (valid_to IS NULL OR DATE '2024-01-20' < valid_to)
ORDER BY account_id;

-- begin-expected
-- columns: account_id,plan_name
-- row: 1|pro
-- row: 2|team
-- end-expected
SELECT account_id, plan_name
FROM plan_history
WHERE DATE '2024-02-20' >= valid_from
  AND (valid_to IS NULL OR DATE '2024-02-20' < valid_to)
ORDER BY account_id;

CREATE TABLE events (
    event_id integer PRIMARY KEY,
    account_id integer NOT NULL,
    happened_on date NOT NULL
);

INSERT INTO events VALUES
(100, 1, '2024-01-15'),
(101, 1, '2024-02-15'),
(102, 2, '2024-02-15');

-- begin-expected
-- columns: event_id,plan_name
-- row: 100|free
-- row: 101|pro
-- row: 102|team
-- end-expected
SELECT e.event_id, p.plan_name
FROM events e
JOIN plan_history p
  ON p.account_id = e.account_id
 AND e.happened_on >= p.valid_from
 AND (p.valid_to IS NULL OR e.happened_on < p.valid_to)
ORDER BY e.event_id;

