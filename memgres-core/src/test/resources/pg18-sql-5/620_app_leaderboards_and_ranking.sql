DROP SCHEMA IF EXISTS test_620 CASCADE;
CREATE SCHEMA test_620;
SET search_path TO test_620;

CREATE TABLE scores (
    user_id integer PRIMARY KEY,
    username text NOT NULL,
    points integer NOT NULL
);

INSERT INTO scores(user_id, username, points) VALUES
(1,'alice',100),
(2,'bob',150),
(3,'carol',150),
(4,'dave',90),
(5,'erin',120);

-- begin-expected
-- columns: username,points,rnk,dense_rnk
-- row: bob|150|1|1
-- row: carol|150|2|1
-- row: erin|120|3|2
-- row: alice|100|4|3
-- row: dave|90|5|4
-- end-expected
SELECT username, points,
       RANK() OVER (ORDER BY points DESC, username) AS rnk,
       DENSE_RANK() OVER (ORDER BY points DESC) AS dense_rnk
FROM scores
ORDER BY points DESC, username;

-- begin-expected
-- columns: username,points
-- row: bob|150
-- row: carol|150
-- row: erin|120
-- end-expected
SELECT username, points
FROM scores
ORDER BY points DESC, username
LIMIT 3;

-- begin-expected
-- columns: username,percentile_bucket
-- row: alice|2
-- row: bob|1
-- row: carol|1
-- row: dave|3
-- row: erin|2
-- end-expected
SELECT username,
       NTILE(3) OVER (ORDER BY points DESC, username) AS percentile_bucket
FROM scores
ORDER BY username;

