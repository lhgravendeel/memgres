DROP SCHEMA IF EXISTS test_560 CASCADE;
CREATE SCHEMA test_560;
SET search_path TO test_560;

CREATE TABLE metrics (
    metric_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    host text NOT NULL,
    metric_name text NOT NULL,
    ts timestamp NOT NULL,
    value numeric(10,2) NOT NULL,
    tags jsonb NOT NULL DEFAULT '{}'::jsonb
);

INSERT INTO metrics(host, metric_name, ts, value, tags) VALUES
('web-1','cpu','2024-01-01 10:00', 20.00, '{"env":"prod","region":"eu"}'),
('web-1','cpu','2024-01-01 10:05', 25.00, '{"env":"prod","region":"eu"}'),
('web-1','cpu','2024-01-01 10:10', 30.00, '{"env":"prod","region":"eu"}'),
('web-2','cpu','2024-01-01 10:00', 50.00, '{"env":"prod","region":"us"}'),
('web-2','cpu','2024-01-01 10:05', 55.00, '{"env":"prod","region":"us"}'),
('web-2','cpu','2024-01-01 10:10', 40.00, '{"env":"prod","region":"us"}'),
('db-1','cpu','2024-01-01 10:00', 70.00, '{"env":"prod","region":"eu"}'),
('db-1','cpu','2024-01-01 10:05', 65.00, '{"env":"prod","region":"eu"}'),
('db-1','cpu','2024-01-01 10:10', 75.00, '{"env":"prod","region":"eu"}'),
('web-1','mem','2024-01-01 10:00', 60.00, '{"env":"prod","region":"eu"}'),
('web-1','mem','2024-01-01 10:05', 61.00, '{"env":"prod","region":"eu"}'),
('web-2','mem','2024-01-01 10:00', 63.00, '{"env":"prod","region":"us"}');

-- begin-expected
-- columns: host,avg_cpu
-- row: db-1|70.0000000000000000
-- row: web-1|25.0000000000000000
-- row: web-2|48.3333333333333333
-- end-expected
SELECT host, AVG(value) AS avg_cpu
FROM metrics
WHERE metric_name = 'cpu'
GROUP BY host
ORDER BY host;

-- begin-expected
-- columns: bucket,count
-- row: 2024-01-01 10:00:00|3
-- row: 2024-01-01 10:05:00|3
-- row: 2024-01-01 10:10:00|3
-- end-expected
SELECT date_trunc('minute', ts) AS bucket, COUNT(*)
FROM metrics
WHERE metric_name = 'cpu'
GROUP BY 1
ORDER BY 1;

-- begin-expected
-- columns: host,ts,value
-- row: db-1|2024-01-01 10:10:00|75.00
-- row: web-1|2024-01-01 10:10:00|30.00
-- row: web-2|2024-01-01 10:10:00|40.00
-- end-expected
SELECT DISTINCT ON (host) host, ts, value
FROM metrics
WHERE metric_name = 'cpu'
ORDER BY host, ts DESC;

-- begin-expected
-- columns: host,ts,value,moving_avg_2
-- row: db-1|2024-01-01 10:00:00|70.00|70.0000000000000000
-- row: db-1|2024-01-01 10:05:00|65.00|67.5000000000000000
-- row: db-1|2024-01-01 10:10:00|75.00|70.0000000000000000
-- row: web-1|2024-01-01 10:00:00|20.00|20.0000000000000000
-- row: web-1|2024-01-01 10:05:00|25.00|22.5000000000000000
-- row: web-1|2024-01-01 10:10:00|30.00|27.5000000000000000
-- row: web-2|2024-01-01 10:00:00|50.00|50.0000000000000000
-- row: web-2|2024-01-01 10:05:00|55.00|52.5000000000000000
-- row: web-2|2024-01-01 10:10:00|40.00|47.5000000000000000
-- end-expected
SELECT host, ts, value,
       AVG(value) OVER (
           PARTITION BY host
           ORDER BY ts
           ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
       ) AS moving_avg_2
FROM metrics
WHERE metric_name = 'cpu'
ORDER BY host, ts;

-- begin-expected
-- columns: host,p95
-- row: db-1|74.5000
-- row: web-1|29.5000
-- row: web-2|54.5000
-- end-expected
SELECT host,
       ROUND((percentile_cont(0.95) WITHIN GROUP (ORDER BY value))::numeric, 4) AS p95
FROM metrics
WHERE metric_name = 'cpu'
GROUP BY host
ORDER BY host;

-- begin-expected
-- columns: host,region,latest_cpu
-- row: db-1|eu|75.00
-- row: web-1|eu|30.00
-- row: web-2|us|40.00
-- end-expected
SELECT DISTINCT ON (host)
       host,
       tags->>'region' AS region,
       value AS latest_cpu
FROM metrics
WHERE metric_name = 'cpu'
ORDER BY host, ts DESC;

