-- ============================================================
-- 05: Subqueries, CTEs, and Set Operations
-- ============================================================

-- Setup
CREATE TABLE departments (id serial PRIMARY KEY, name text UNIQUE NOT NULL);
CREATE TABLE employees (id serial PRIMARY KEY, name text NOT NULL, dept_id int REFERENCES departments(id), salary numeric(10,2), active boolean DEFAULT true);
INSERT INTO departments (name) VALUES ('Engineering'), ('Sales'), ('Marketing'), ('HR');
INSERT INTO employees (name, dept_id, salary) VALUES
  ('Alice', 1, 95000), ('Bob', 1, 85000), ('Charlie', 2, 70000),
  ('Diana', 2, 75000), ('Eve', 3, 65000), ('Frank', 3, 60000),
  ('Grace', 4, 80000), ('Hank', 1, 110000), ('Ivy', NULL, 50000);

-- === Scalar subquery in SELECT ===
SELECT name, salary, (SELECT AVG(salary) FROM employees) AS avg_sal FROM employees ORDER BY name;
SELECT name, salary - (SELECT AVG(salary) FROM employees) AS diff FROM employees ORDER BY name;

-- === Subquery in WHERE: comparison ===
SELECT name FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) ORDER BY name;
SELECT name FROM employees WHERE salary = (SELECT MAX(salary) FROM employees);
SELECT name FROM employees WHERE dept_id = (SELECT id FROM departments WHERE name = 'Engineering');

-- === Subquery in WHERE: IN ===
SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name IN ('Engineering', 'Sales')) ORDER BY name;
SELECT name FROM employees WHERE dept_id NOT IN (SELECT id FROM departments WHERE name = 'HR') AND dept_id IS NOT NULL ORDER BY name;

-- === Subquery in WHERE: EXISTS ===
SELECT d.name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.salary > 90000) ORDER BY d.name;
SELECT d.name FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id) ORDER BY d.name;

-- === Subquery in WHERE: ANY/ALL ===
SELECT name FROM employees WHERE salary > ANY (SELECT salary FROM employees WHERE dept_id = 2) ORDER BY name;
SELECT name FROM employees WHERE salary > ALL (SELECT salary FROM employees WHERE dept_id = 3) ORDER BY name;

-- === Subquery in FROM ===
SELECT sub.dept_name, sub.emp_count FROM (SELECT d.name AS dept_name, COUNT(e.id) AS emp_count FROM departments d LEFT JOIN employees e ON e.dept_id = d.id GROUP BY d.name) sub ORDER BY sub.emp_count DESC;
SELECT * FROM (SELECT name, salary, RANK() OVER (ORDER BY salary DESC) AS rnk FROM employees) ranked WHERE rnk <= 3;

-- === Correlated subquery ===
SELECT e.name, e.salary, (SELECT d.name FROM departments d WHERE d.id = e.dept_id) AS dept FROM employees e ORDER BY e.name;
SELECT e.name FROM employees e WHERE e.salary > (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.dept_id = e.dept_id) ORDER BY e.name;

-- === CTE: basic ===
WITH active_emps AS (SELECT * FROM employees WHERE active = true)
SELECT COUNT(*) FROM active_emps;

WITH dept_stats AS (
  SELECT dept_id, COUNT(*) AS cnt, AVG(salary) AS avg_sal
  FROM employees WHERE dept_id IS NOT NULL GROUP BY dept_id
)
SELECT d.name, ds.cnt, ds.avg_sal::numeric(10,2)
FROM dept_stats ds JOIN departments d ON d.id = ds.dept_id
ORDER BY d.name;

-- === CTE: multiple CTEs ===
WITH eng AS (SELECT * FROM employees WHERE dept_id = 1),
     sales AS (SELECT * FROM employees WHERE dept_id = 2)
SELECT 'Engineering' AS dept, COUNT(*) AS cnt, AVG(salary)::numeric(10,2) AS avg FROM eng
UNION ALL
SELECT 'Sales', COUNT(*), AVG(salary)::numeric(10,2) FROM sales;

-- === CTE with DML ===
CREATE TABLE archive (id int, name text, archived_at timestamp DEFAULT now());
WITH removed AS (
  DELETE FROM employees WHERE active = false RETURNING id, name
)
INSERT INTO archive (id, name) SELECT id, name FROM removed;
SELECT * FROM archive;

-- === Recursive CTE ===
CREATE TABLE tree (id serial PRIMARY KEY, parent_id int REFERENCES tree(id), label text);
INSERT INTO tree (parent_id, label) VALUES (NULL, 'root'), (1, 'child1'), (1, 'child2'), (2, 'grandchild1'), (2, 'grandchild2'), (3, 'grandchild3');

WITH RECURSIVE descendants AS (
  SELECT id, parent_id, label, 0 AS depth FROM tree WHERE parent_id IS NULL
  UNION ALL
  SELECT t.id, t.parent_id, t.label, d.depth + 1 FROM tree t JOIN descendants d ON t.parent_id = d.id
)
SELECT * FROM descendants ORDER BY depth, id;

-- === Set operations ===
SELECT name FROM employees WHERE dept_id = 1
UNION
SELECT name FROM employees WHERE salary > 80000
ORDER BY name;

SELECT name FROM employees WHERE dept_id = 1
UNION ALL
SELECT name FROM employees WHERE salary > 80000
ORDER BY name;

SELECT name FROM employees WHERE dept_id = 1
INTERSECT
SELECT name FROM employees WHERE salary > 80000
ORDER BY name;

SELECT name FROM employees WHERE dept_id = 1
EXCEPT
SELECT name FROM employees WHERE salary > 100000
ORDER BY name;

-- === UNION ALL inside EXISTS ===
SELECT EXISTS (
  SELECT 1 FROM employees WHERE dept_id = 1
  UNION ALL
  SELECT 1 FROM employees WHERE dept_id = 2
);

-- === UNION ALL inside IN ===
SELECT name FROM departments WHERE id IN (
  SELECT dept_id FROM employees WHERE salary > 90000
  UNION
  SELECT dept_id FROM employees WHERE active = false
) ORDER BY name;

-- === INVALID: Subquery errors ===
-- Subquery returns multiple rows where scalar expected
SELECT name FROM employees WHERE salary = (SELECT salary FROM employees);
-- Column count mismatch in UNION
SELECT name, salary FROM employees UNION SELECT name FROM employees;
-- Recursive CTE without RECURSIVE keyword
WITH bad_recursive AS (
  SELECT 1 AS n UNION ALL SELECT n+1 FROM bad_recursive WHERE n < 5
) SELECT * FROM bad_recursive;

-- Cleanup
DROP TABLE archive;
DROP TABLE tree;
DROP TABLE employees;
DROP TABLE departments;
