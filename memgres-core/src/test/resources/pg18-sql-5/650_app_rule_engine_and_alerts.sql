DROP SCHEMA IF EXISTS test_650 CASCADE;
CREATE SCHEMA test_650;
SET search_path TO test_650;

CREATE TABLE rules (
    rule_id integer PRIMARY KEY,
    rule_name text NOT NULL,
    metric_name text NOT NULL,
    threshold numeric(10,2) NOT NULL,
    enabled boolean NOT NULL
);

CREATE TABLE metric_events (
    event_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    metric_name text NOT NULL,
    observed_value numeric(10,2) NOT NULL,
    observed_at timestamp NOT NULL
);

INSERT INTO rules(rule_id, rule_name, metric_name, threshold, enabled) VALUES
(1, 'cpu warn', 'cpu', 80.00, true),
(2, 'mem warn', 'mem', 70.00, true),
(3, 'disk warn', 'disk', 90.00, false);

INSERT INTO metric_events(metric_name, observed_value, observed_at) VALUES
('cpu', 75.00, '2024-01-01 10:00'),
('cpu', 85.00, '2024-01-01 10:05'),
('mem', 72.00, '2024-01-01 10:06'),
('disk', 95.00, '2024-01-01 10:07');

-- begin-expected
-- columns: rule_name,event_id,observed_value
-- row: cpu warn|2|85.00
-- row: mem warn|3|72.00
-- end-expected
SELECT r.rule_name, e.event_id, e.observed_value
FROM rules r
JOIN metric_events e
  ON e.metric_name = r.metric_name
WHERE r.enabled
  AND e.observed_value > r.threshold
ORDER BY e.event_id;

-- begin-expected
-- columns: metric_name,latest_value,alerting
-- row: cpu|85.00|t
-- row: disk|95.00|f
-- row: mem|72.00|t
-- end-expected
WITH latest AS (
    SELECT DISTINCT ON (metric_name) metric_name, observed_value
    FROM metric_events
    ORDER BY metric_name, observed_at DESC
)
SELECT l.metric_name,
       l.observed_value AS latest_value,
       EXISTS (
           SELECT 1
           FROM rules r
           WHERE r.metric_name = l.metric_name
             AND r.enabled
             AND l.observed_value > r.threshold
       ) AS alerting
FROM latest l
ORDER BY l.metric_name;

