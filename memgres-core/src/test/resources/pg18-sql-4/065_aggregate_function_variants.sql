DROP SCHEMA IF EXISTS test_065 CASCADE;
CREATE SCHEMA test_065;
SET search_path TO test_065;

CREATE TABLE sales (
    id integer PRIMARY KEY,
    region text NOT NULL,
    seller text NOT NULL,
    amount numeric(10,2) NOT NULL,
    qty integer NOT NULL,
    category text NOT NULL,
    flag boolean NOT NULL,
    payload jsonb NOT NULL
);

INSERT INTO sales(id, region, seller, amount, qty, category, flag, payload) VALUES
    (1, 'north', 'alice', 10.00, 1, 'hardware', true,  '{"code":"A1"}'),
    (2, 'north', 'bob',   25.00, 2, 'hardware', false, '{"code":"A2"}'),
    (3, 'south', 'alice',  8.00, 1, 'software', true,  '{"code":"B1"}'),
    (4, 'south', 'cara',  17.50, 3, 'hardware', true,  '{"code":"B2"}'),
    (5, 'south', 'dave',  17.50, 2, 'software', false, '{"code":"B3"}'),
    (6, 'west',  'erin',   5.00, 1, 'hardware', true,  '{"code":"C1"}');

-- begin-expected
-- columns: region|row_count|sum_amount|avg_amount|min_amount|max_amount|sum_flagged
-- row: north|2|35.00|17.5000000000000000|10.00|25.00|10.00
-- row: south|3|43.00|14.3333333333333333|8.00|17.50|25.50
-- row: west|1|5.00|5.0000000000000000|5.00|5.00|5.00
-- end-expected
SELECT region,
       count(*) AS row_count,
       sum(amount) AS sum_amount,
       avg(amount) AS avg_amount,
       min(amount) AS min_amount,
       max(amount) AS max_amount,
       sum(amount) FILTER (WHERE flag) AS sum_flagged
FROM sales
GROUP BY region
ORDER BY region;

-- begin-expected
-- columns: region|seller_list|category_list|qtys
-- row: north|alice,bob|hardware,hardware|1,2
-- row: south|alice,cara,dave|software,hardware,software|1,3,2
-- row: west|erin|hardware|1
-- end-expected
SELECT region,
       string_agg(seller, ',' ORDER BY seller) AS seller_list,
       string_agg(category, ',' ORDER BY id) AS category_list,
       string_agg(qty::text, ',' ORDER BY id) AS qtys
FROM sales
GROUP BY region
ORDER BY region;

-- begin-expected
-- columns: region|seller_array|code_array|seller_json|amount_object
-- row: north|{alice,bob}|{A1,A2}|["alice", "bob"]|{"bob": 25.00, "alice": 10.00}
-- row: south|{alice,cara,dave}|{B1,B2,B3}|["alice", "cara", "dave"]|{"cara": 17.50, "dave": 17.50, "alice": 8.00}
-- row: west|{erin}|{C1}|["erin"]|{"erin": 5.00}
-- end-expected
SELECT region,
       array_agg(seller ORDER BY seller) AS seller_array,
       array_agg(payload->>'code' ORDER BY id) AS code_array,
       json_agg(seller ORDER BY seller) AS seller_json,
       jsonb_object_agg(seller, amount ORDER BY seller DESC) AS amount_object
FROM sales
GROUP BY region
ORDER BY region;

-- begin-expected
-- columns: region|var_pop_amt|stddev_pop_amt
-- row: north|56.2500000000000000|7.5000000000000000
-- row: south|20.0555555555555556|4.4783421596208701
-- row: west|0|0
-- end-expected
SELECT region,
       var_pop(amount) AS var_pop_amt,
       stddev_pop(amount) AS stddev_pop_amt
FROM sales
GROUP BY region
ORDER BY region;

-- begin-expected
-- columns: region|pct50_disc|pct50_cont
-- row: north|10.00|17.50
-- row: south|17.50|17.50
-- row: west|5.00|5.00
-- end-expected
SELECT region,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY amount) AS pct50_disc,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY amount) AS pct50_cont
FROM sales
GROUP BY region
ORDER BY region;

DROP SCHEMA test_065 CASCADE;
