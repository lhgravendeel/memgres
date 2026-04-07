DROP SCHEMA IF EXISTS test_070 CASCADE;
CREATE SCHEMA test_070;
SET search_path TO test_070;

CREATE TABLE employees (
    emp_id integer PRIMARY KEY,
    name text NOT NULL,
    manager_id integer
);

INSERT INTO employees VALUES
    (1, 'CEO', NULL),
    (2, 'CTO', 1),
    (3, 'CFO', 1),
    (4, 'Dev1', 2),
    (5, 'Dev2', 2);

-- begin-expected
-- columns: name
-- row: CTO
-- row: Dev1
-- row: Dev2
-- end-expected
SELECT name
FROM employees e
WHERE manager_id = (
    SELECT emp_id FROM employees WHERE name = 'CTO'
)
OR emp_id = (
    SELECT emp_id FROM employees WHERE name = 'CTO'
)
ORDER BY name;

-- begin-expected
-- columns: name
-- row: CTO
-- end-expected
SELECT e.name
FROM employees e
WHERE EXISTS (
    SELECT 1
    FROM employees r
    WHERE r.manager_id = e.emp_id
      AND r.name LIKE 'D%'
)
ORDER BY e.name;

-- begin-expected
-- columns: emp_id|name|level|path
-- row: 1|CEO|1|CEO
-- row: 2|CTO|2|CEO > CTO
-- row: 3|CFO|2|CEO > CFO
-- row: 4|Dev1|3|CEO > CTO > Dev1
-- row: 5|Dev2|3|CEO > CTO > Dev2
-- end-expected
WITH RECURSIVE org AS (
    SELECT emp_id, name, manager_id, 1 AS level, name::text AS path
    FROM employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.emp_id, e.name, e.manager_id, o.level + 1, (o.path || ' > ' || e.name)::text
    FROM employees e
    JOIN org o ON e.manager_id = o.emp_id
)
SELECT emp_id, name, level, path
FROM org
ORDER BY emp_id;

WITH top_mgr AS (
    SELECT emp_id
    FROM employees
    WHERE name = 'CEO'
),
new_people AS (
    SELECT 6 AS emp_id, 'Ops'::text AS name, emp_id AS manager_id
    FROM top_mgr
)
INSERT INTO employees(emp_id, name, manager_id)
SELECT emp_id, name, manager_id
FROM new_people;

-- begin-expected
-- columns: emp_id|name|manager_id
-- row: 6|Ops|1
-- end-expected
SELECT emp_id, name, manager_id
FROM employees
WHERE emp_id = 6;

DROP SCHEMA test_070 CASCADE;
