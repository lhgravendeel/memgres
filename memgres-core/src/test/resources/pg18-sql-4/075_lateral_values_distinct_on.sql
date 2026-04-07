DROP SCHEMA IF EXISTS test_075 CASCADE;
CREATE SCHEMA test_075;
SET search_path TO test_075;

CREATE TABLE accounts (
    account_id integer PRIMARY KEY,
    owner_name text NOT NULL,
    labels text[] NOT NULL,
    scores integer[] NOT NULL,
    city text NOT NULL
);

INSERT INTO accounts(account_id, owner_name, labels, scores, city) VALUES
    (1, 'anna', ARRAY['red','blue'],   ARRAY[5,10], 'ams'),
    (2, 'bert', ARRAY['green'],        ARRAY[7],    'ams'),
    (3, 'cara', ARRAY['red','yellow'], ARRAY[9,3],  'ut'),
    (4, 'dina', ARRAY['blue'],         ARRAY[9],    'ut'),
    (5, 'erik', ARRAY['red','green'],  ARRAY[4,8],  'rdm');

-- begin-expected
-- columns: column1|column2
-- row: 1|one
-- row: 2|two
-- row: 3|three
-- end-expected
VALUES (1, 'one'), (2, 'two'), (3, 'three');

-- begin-expected
-- columns: account_id|owner_name|labels|scores|city
-- row: 1|anna|{red,blue}|{5,10}|ams
-- row: 2|bert|{green}|{7}|ams
-- row: 3|cara|{red,yellow}|{9,3}|ut
-- row: 4|dina|{blue}|{9}|ut
-- row: 5|erik|{red,green}|{4,8}|rdm
-- end-expected
TABLE accounts;

-- begin-expected
-- columns: account_id|ord|label
-- row: 1|1|red
-- row: 1|2|blue
-- row: 2|1|green
-- row: 3|1|red
-- row: 3|2|yellow
-- row: 4|1|blue
-- row: 5|1|red
-- row: 5|2|green
-- end-expected
SELECT a.account_id, x.ord, x.label
FROM accounts AS a
CROSS JOIN LATERAL unnest(a.labels) WITH ORDINALITY AS x(label, ord)
ORDER BY a.account_id, x.ord;

-- begin-expected
-- columns: account_id|pair_no|label|score
-- row: 1|1|red|5
-- row: 1|2|blue|10
-- row: 2|1|green|7
-- row: 3|1|red|9
-- row: 3|2|yellow|3
-- row: 4|1|blue|9
-- row: 5|1|red|4
-- row: 5|2|green|8
-- end-expected
SELECT a.account_id, z.pair_no, z.label, z.score
FROM accounts AS a
CROSS JOIN LATERAL ROWS FROM(
    unnest(a.labels),
    unnest(a.scores)
) WITH ORDINALITY AS z(label, score, pair_no)
ORDER BY a.account_id, z.pair_no;

-- begin-expected
-- columns: city|account_id|best_score
-- row: ams|1|10
-- row: rdm|5|8
-- row: ut|3|9
-- end-expected
SELECT DISTINCT ON (city) city, account_id, best_score
FROM (
    SELECT a.city, a.account_id, max(s.score) AS best_score
    FROM accounts AS a
    CROSS JOIN LATERAL unnest(a.scores) AS s(score)
    GROUP BY a.city, a.account_id
) AS ranked
ORDER BY city, best_score DESC, account_id;

-- begin-expected
-- columns: account_id|owner_name|picked_label
-- row: 1|anna|red
-- row: 2|bert|green
-- row: 3|cara|yellow
-- row: 4|dina|blue
-- row: 5|erik|red
-- end-expected
SELECT a.account_id, a.owner_name, p.picked_label
FROM accounts AS a
CROSS JOIN LATERAL (
    SELECT label AS picked_label
    FROM unnest(a.labels) AS u(label)
    ORDER BY label DESC
    LIMIT 1
) AS p
ORDER BY a.account_id;

DROP SCHEMA test_075 CASCADE;
