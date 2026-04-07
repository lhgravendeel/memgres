DROP SCHEMA IF EXISTS test_580 CASCADE;
CREATE SCHEMA test_580;
SET search_path TO test_580;

CREATE TABLE users (
    user_id integer PRIMARY KEY,
    username text NOT NULL
);

CREATE TABLE likes (
    user_id integer NOT NULL,
    item_id integer NOT NULL,
    PRIMARY KEY (user_id, item_id)
);

INSERT INTO users(user_id, username) VALUES
(1,'alice'),(2,'bob'),(3,'carol'),(4,'dave');

INSERT INTO likes(user_id, item_id) VALUES
(1,101),(1,102),(1,103),
(2,101),(2,103),(2,104),
(3,101),(3,104),(3,105),
(4,102),(4,103),(4,105);

-- begin-expected
-- columns: item_id,shared_likes
-- row: 103|2
-- row: 104|2
-- row: 102|1
-- row: 105|1
-- end-expected
SELECT l2.item_id, COUNT(*) AS shared_likes
FROM likes l1
JOIN likes l2
  ON l1.user_id = l2.user_id
 AND l2.item_id <> l1.item_id
WHERE l1.item_id = 101
GROUP BY l2.item_id
ORDER BY shared_likes DESC, l2.item_id;

-- begin-expected
-- columns: user_a,user_b,shared_items
-- row: alice|bob|2
-- row: alice|dave|2
-- row: bob|carol|2
-- end-expected
SELECT ua.username AS user_a, ub.username AS user_b, COUNT(*) AS shared_items
FROM likes la
JOIN likes lb
  ON la.item_id = lb.item_id
 AND la.user_id < lb.user_id
JOIN users ua ON ua.user_id = la.user_id
JOIN users ub ON ub.user_id = lb.user_id
GROUP BY ua.username, ub.username
HAVING COUNT(*) >= 2
ORDER BY user_a, user_b;

-- begin-expected
-- columns: deterministic_sample_count
-- row: 2
-- end-expected
SELECT COUNT(*) AS deterministic_sample_count
FROM (
  SELECT item_id
  FROM (VALUES (101),(102),(103),(104),(105)) AS v(item_id)
  ORDER BY item_id
  LIMIT 2
) s;

