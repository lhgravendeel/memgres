DROP SCHEMA IF EXISTS test_1260 CASCADE;
CREATE SCHEMA test_1260;
SET search_path TO test_1260;

CREATE TABLE logs (
    log_id integer PRIMARY KEY,
    line text NOT NULL
);

INSERT INTO logs VALUES
(1, 'user=alice code=E100 path=/api/v1'),
(2, 'user=bob code=E200 path=/api/v2'),
(3, 'user=carol code=OK path=/health');

-- begin-expected
-- columns: log_id,code
-- row: 1|code=E100
-- row: 2|code=E200
-- row: 3|code=OK
-- end-expected
SELECT log_id, substring(line from 'code=([A-Z0-9]+)') AS code
FROM logs
ORDER BY log_id;

-- begin-expected
-- columns: log_id,masked
-- row: 1|user=*** code=E100 path=/api/v1
-- row: 2|user=*** code=E200 path=/api/v2
-- row: 3|user=*** code=OK path=/health
-- end-expected
SELECT log_id,
       regexp_replace(line, 'user=[^ ]+', 'user=***') AS masked
FROM logs
ORDER BY log_id;

-- begin-expected
-- columns: token
-- row: alice
-- row: bob
-- row: carol
-- end-expected
SELECT (regexp_matches(line, 'user=([^ ]+)'))[1] AS token
FROM logs
ORDER BY token;

-- begin-expected
-- columns: log_id,is_api
-- row: 1|t
-- row: 2|t
-- row: 3|f
-- end-expected
SELECT log_id, line ~ 'path=/api/' AS is_api
FROM logs
ORDER BY log_id;

