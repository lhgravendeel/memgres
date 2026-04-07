DROP SCHEMA IF EXISTS test_900 CASCADE;
CREATE SCHEMA test_900;
SET search_path TO test_900;

CREATE TABLE events (
    event_id integer PRIMARY KEY,
    user_id integer NOT NULL,
    event_type text NOT NULL,
    tag text,
    score integer NOT NULL
);

INSERT INTO events VALUES
(1, 1, 'click', 'a', 10),
(2, 1, 'view',  'b', 20),
(3, 1, 'click', 'a', 30),
(4, 2, 'view',  'c', 15),
(5, 2, 'click', 'd', 25);

-- begin-expected
-- columns: user_id,ordered_tags
-- row: 1|a,b,a
-- row: 2|c,d
-- end-expected
SELECT user_id, string_agg(tag, ',' ORDER BY event_id) AS ordered_tags
FROM events
GROUP BY user_id
ORDER BY user_id;

-- begin-expected
-- columns: user_id,distinct_tags
-- row: 1|a,b
-- row: 2|c,d
-- end-expected
SELECT user_id, string_agg(tag, ',' ORDER BY tag) AS distinct_tags
FROM (
    SELECT DISTINCT user_id, tag
    FROM events
) d
GROUP BY user_id
ORDER BY user_id;

-- begin-expected
-- columns: user_id,click_count,view_count
-- row: 1|2|1
-- row: 2|1|1
-- end-expected
SELECT user_id,
       COUNT(*) FILTER (WHERE event_type = 'click') AS click_count,
       COUNT(*) FILTER (WHERE event_type = 'view') AS view_count
FROM events
GROUP BY user_id
ORDER BY user_id;

-- begin-expected
-- columns: user_id,any_big_score,all_positive
-- row: 1|t|t
-- row: 2|f|t
-- end-expected
SELECT user_id,
       bool_or(score >= 30) AS any_big_score,
       bool_and(score > 0) AS all_positive
FROM events
GROUP BY user_id
ORDER BY user_id;

