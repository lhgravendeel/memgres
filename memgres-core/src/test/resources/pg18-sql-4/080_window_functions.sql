DROP SCHEMA IF EXISTS test_080 CASCADE;
CREATE SCHEMA test_080;
SET search_path TO test_080;

CREATE TABLE scores (
    grp text,
    player text,
    score integer
);

INSERT INTO scores VALUES
    ('A', 'p1', 10),
    ('A', 'p2', 20),
    ('A', 'p3', 20),
    ('B', 'q1', 5),
    ('B', 'q2', 15);

-- begin-expected
-- columns: grp|player|score|rn|rnk|drnk|running_sum|prev_score|next_score
-- row: A|p2|20|1|1|1|20||20
-- row: A|p3|20|2|1|1|40|20|10
-- row: A|p1|10|3|3|2|50|20|
-- row: B|q2|15|1|1|1|15||5
-- row: B|q1|5|2|2|2|20|15|
-- end-expected
SELECT
    grp,
    player,
    score,
    ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC, player) AS rn,
    RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS rnk,
    DENSE_RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS drnk,
    SUM(score) OVER (
        PARTITION BY grp
        ORDER BY score DESC, player
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_sum,
    LAG(score) OVER (PARTITION BY grp ORDER BY score DESC, player) AS prev_score,
    LEAD(score) OVER (PARTITION BY grp ORDER BY score DESC, player) AS next_score
FROM scores
ORDER BY grp, rn;

-- begin-expected
-- columns: grp|player|score|grp_total|grp_avg
-- row: A|p1|10|50|16.6666666666666667
-- row: A|p2|20|50|16.6666666666666667
-- row: A|p3|20|50|16.6666666666666667
-- row: B|q1|5|20|10.0000000000000000
-- row: B|q2|15|20|10.0000000000000000
-- end-expected
SELECT
    grp,
    player,
    score,
    SUM(score) OVER (PARTITION BY grp) AS grp_total,
    AVG(score) OVER (PARTITION BY grp) AS grp_avg
FROM scores
ORDER BY grp, player;

DROP SCHEMA test_080 CASCADE;
