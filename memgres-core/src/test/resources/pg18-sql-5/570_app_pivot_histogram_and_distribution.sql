DROP SCHEMA IF EXISTS test_570 CASCADE;
CREATE SCHEMA test_570;
SET search_path TO test_570;

CREATE TABLE issues (
    issue_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    team text NOT NULL,
    priority integer NOT NULL,
    status text NOT NULL,
    estimate_hours integer NOT NULL
);

INSERT INTO issues(team, priority, status, estimate_hours) VALUES
('api', 1, 'open', 3),
('api', 2, 'open', 8),
('api', 1, 'closed', 5),
('web', 3, 'open', 1),
('web', 2, 'closed', 13),
('web', 1, 'closed', 2),
('ops', 2, 'open', 21),
('ops', 3, 'open', 34),
('ops', 1, 'closed', 55);

-- begin-expected
-- columns: team,open_count,closed_count
-- row: api|2|1
-- row: ops|2|1
-- row: web|1|2
-- end-expected
SELECT team,
       SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_count,
       SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) AS closed_count
FROM issues
GROUP BY team
ORDER BY team;

-- begin-expected
-- columns: bucket,count
-- row: 1|6
-- row: 2|1
-- row: 3|1
-- row: 4|1
-- end-expected
SELECT width_bucket(estimate_hours, 0, 60, 4) AS bucket, COUNT(*)
FROM issues
GROUP BY bucket
ORDER BY bucket;

-- begin-expected
-- columns: priority,count
-- row: 1|4
-- row: 2|3
-- row: 3|2
-- end-expected
SELECT priority, COUNT(*)
FROM issues
GROUP BY priority
ORDER BY priority;

-- begin-expected
-- columns: team,small_estimates,medium_estimates,large_estimates
-- row: api|2|1|0
-- row: ops|0|0|3
-- row: web|2|1|0
-- end-expected
SELECT team,
       SUM(CASE WHEN estimate_hours <= 5 THEN 1 ELSE 0 END) AS small_estimates,
       SUM(CASE WHEN estimate_hours BETWEEN 6 AND 20 THEN 1 ELSE 0 END) AS medium_estimates,
       SUM(CASE WHEN estimate_hours > 20 THEN 1 ELSE 0 END) AS large_estimates
FROM issues
GROUP BY team
ORDER BY team;

