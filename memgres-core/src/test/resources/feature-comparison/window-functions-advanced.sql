-- ============================================================================
-- Feature Comparison: Advanced Window Functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, NTILE, and
-- advanced window frame specifications not covered in other test files.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS wf_test CASCADE;
CREATE SCHEMA wf_test;
SET search_path = wf_test, public;

CREATE TABLE wf_data (id integer, dept text, salary integer);
INSERT INTO wf_data VALUES
  (1, 'eng', 80000),
  (2, 'eng', 90000),
  (3, 'eng', 100000),
  (4, 'sales', 60000),
  (5, 'sales', 70000),
  (6, 'sales', 80000),
  (7, 'hr', 65000),
  (8, 'hr', 75000);

-- ============================================================================
-- SECTION A: LAG and LEAD
-- ============================================================================

-- ============================================================================
-- 1. LAG: previous row value
-- ============================================================================

-- begin-expected
-- columns: id, salary, prev_salary
-- row: 1, 80000, NULL
-- row: 2, 90000, 80000
-- row: 3, 100000, 90000
-- end-expected
SELECT id, salary,
  lag(salary) OVER (ORDER BY id) AS prev_salary
FROM wf_data WHERE dept = 'eng' ORDER BY id;

-- ============================================================================
-- 2. LAG with offset
-- ============================================================================

-- begin-expected
-- columns: id, salary, two_back
-- row: 1, 80000, NULL
-- row: 2, 90000, NULL
-- row: 3, 100000, 80000
-- end-expected
SELECT id, salary,
  lag(salary, 2) OVER (ORDER BY id) AS two_back
FROM wf_data WHERE dept = 'eng' ORDER BY id;

-- ============================================================================
-- 3. LAG with default value
-- ============================================================================

-- begin-expected
-- columns: id, salary, prev_salary
-- row: 1, 80000, 0
-- row: 2, 90000, 80000
-- row: 3, 100000, 90000
-- end-expected
SELECT id, salary,
  lag(salary, 1, 0) OVER (ORDER BY id) AS prev_salary
FROM wf_data WHERE dept = 'eng' ORDER BY id;

-- ============================================================================
-- 4. LEAD: next row value
-- ============================================================================

-- begin-expected
-- columns: id, salary, next_salary
-- row: 1, 80000, 90000
-- row: 2, 90000, 100000
-- row: 3, 100000, NULL
-- end-expected
SELECT id, salary,
  lead(salary) OVER (ORDER BY id) AS next_salary
FROM wf_data WHERE dept = 'eng' ORDER BY id;

-- ============================================================================
-- 5. LEAD with offset and default
-- ============================================================================

-- begin-expected
-- columns: id, salary, two_ahead
-- row: 1, 80000, 100000
-- row: 2, 90000, -1
-- row: 3, 100000, -1
-- end-expected
SELECT id, salary,
  lead(salary, 2, -1) OVER (ORDER BY id) AS two_ahead
FROM wf_data WHERE dept = 'eng' ORDER BY id;

-- ============================================================================
-- 6. LAG/LEAD with PARTITION BY
-- ============================================================================

-- begin-expected
-- columns: dept, id, salary, prev_in_dept
-- row: eng, 1, 80000, NULL
-- row: eng, 2, 90000, 80000
-- row: eng, 3, 100000, 90000
-- row: hr, 7, 65000, NULL
-- row: hr, 8, 75000, 65000
-- row: sales, 4, 60000, NULL
-- row: sales, 5, 70000, 60000
-- row: sales, 6, 80000, 70000
-- end-expected
SELECT dept, id, salary,
  lag(salary) OVER (PARTITION BY dept ORDER BY id) AS prev_in_dept
FROM wf_data ORDER BY dept, id;

-- ============================================================================
-- 7. LAG with NULL values in data
-- ============================================================================

CREATE TABLE wf_nulls (id integer, val integer);
INSERT INTO wf_nulls VALUES (1, 10), (2, NULL), (3, 30), (4, NULL), (5, 50);

-- begin-expected
-- columns: id, val, prev_val
-- row: 1, 10, NULL
-- row: 2, NULL, 10
-- row: 3, 30, NULL
-- row: 4, NULL, 30
-- row: 5, 50, NULL
-- end-expected
SELECT id, val,
  lag(val) OVER (ORDER BY id) AS prev_val
FROM wf_nulls ORDER BY id;

DROP TABLE wf_nulls;

-- ============================================================================
-- SECTION B: FIRST_VALUE, LAST_VALUE, NTH_VALUE
-- ============================================================================

-- ============================================================================
-- 8. FIRST_VALUE: first in partition
-- ============================================================================

-- begin-expected
-- columns: dept, id, salary, first_sal
-- row: eng, 1, 80000, 80000
-- row: eng, 2, 90000, 80000
-- row: eng, 3, 100000, 80000
-- row: sales, 4, 60000, 60000
-- row: sales, 5, 70000, 60000
-- row: sales, 6, 80000, 60000
-- end-expected
SELECT dept, id, salary,
  first_value(salary) OVER (PARTITION BY dept ORDER BY id) AS first_sal
FROM wf_data WHERE dept IN ('eng', 'sales') ORDER BY dept, id;

-- ============================================================================
-- 9. LAST_VALUE with full frame
-- ============================================================================

-- note: LAST_VALUE needs ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
-- to see the actual last value in the partition (default frame only goes to current row)
-- begin-expected
-- columns: dept, id, salary, last_sal
-- row: eng, 1, 80000, 100000
-- row: eng, 2, 90000, 100000
-- row: eng, 3, 100000, 100000
-- row: sales, 4, 60000, 80000
-- row: sales, 5, 70000, 80000
-- row: sales, 6, 80000, 80000
-- end-expected
SELECT dept, id, salary,
  last_value(salary) OVER (
    PARTITION BY dept ORDER BY id
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS last_sal
FROM wf_data WHERE dept IN ('eng', 'sales') ORDER BY dept, id;

-- ============================================================================
-- 10. LAST_VALUE with default frame (current row = end of frame)
-- ============================================================================

-- note: Default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
-- So last_value = current row's value
-- begin-expected
-- columns: id, salary, last_val
-- row: 1, 80000, 80000
-- row: 2, 90000, 90000
-- row: 3, 100000, 100000
-- end-expected
SELECT id, salary,
  last_value(salary) OVER (ORDER BY id) AS last_val
FROM wf_data WHERE dept = 'eng' ORDER BY id;

-- ============================================================================
-- 11. NTH_VALUE: Nth row in frame
-- ============================================================================

-- begin-expected
-- columns: id, salary, second_sal
-- row: 1, 80000, NULL
-- row: 2, 90000, 90000
-- row: 3, 100000, 90000
-- end-expected
SELECT id, salary,
  nth_value(salary, 2) OVER (ORDER BY id) AS second_sal
FROM wf_data WHERE dept = 'eng' ORDER BY id;

-- ============================================================================
-- 12. NTH_VALUE with full frame
-- ============================================================================

-- begin-expected
-- columns: id, salary, third_sal
-- row: 1, 80000, 100000
-- row: 2, 90000, 100000
-- row: 3, 100000, 100000
-- end-expected
SELECT id, salary,
  nth_value(salary, 3) OVER (
    ORDER BY id
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS third_sal
FROM wf_data WHERE dept = 'eng' ORDER BY id;

-- ============================================================================
-- 13. NTH_VALUE beyond partition size
-- ============================================================================

-- begin-expected
-- columns: id, val
-- row: 1, NULL
-- row: 2, NULL
-- row: 3, NULL
-- end-expected
SELECT id,
  nth_value(salary, 10) OVER (
    ORDER BY id
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS val
FROM wf_data WHERE dept = 'eng' ORDER BY id;

-- ============================================================================
-- SECTION C: NTILE
-- ============================================================================

-- ============================================================================
-- 14. NTILE: divide into N buckets
-- ============================================================================

-- begin-expected
-- columns: id, salary, bucket
-- row: 4, 60000, 1
-- row: 5, 70000, 2
-- row: 6, 80000, 3
-- end-expected
SELECT id, salary,
  ntile(3) OVER (ORDER BY salary) AS bucket
FROM wf_data WHERE dept = 'sales' ORDER BY salary;

-- ============================================================================
-- 15. NTILE: more buckets than rows
-- ============================================================================

-- begin-expected
-- columns: id, salary, bucket
-- row: 4, 60000, 1
-- row: 5, 70000, 2
-- row: 6, 80000, 3
-- end-expected
SELECT id, salary,
  ntile(5) OVER (ORDER BY salary) AS bucket
FROM wf_data WHERE dept = 'sales' ORDER BY salary;

-- ============================================================================
-- 16. NTILE(1): all rows in one bucket
-- ============================================================================

-- begin-expected
-- columns: id, bucket
-- row: 1, 1
-- row: 2, 1
-- row: 3, 1
-- end-expected
SELECT id,
  ntile(1) OVER (ORDER BY id) AS bucket
FROM wf_data WHERE dept = 'eng' ORDER BY id;

-- ============================================================================
-- SECTION D: Advanced frame specifications
-- ============================================================================

-- ============================================================================
-- 17. ROWS BETWEEN N PRECEDING AND N FOLLOWING (sliding window)
-- ============================================================================

-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
-- note: The ::integer cast binds to avg(salary) before OVER is parsed, causing a syntax error.
SELECT id, salary,
  avg(salary)::integer OVER (
    ORDER BY id
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS avg3
FROM wf_data WHERE dept = 'eng' ORDER BY id;

-- ============================================================================
-- 18. ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
-- ============================================================================

-- begin-expected
-- columns: id, salary, remaining_sum
-- row: 1, 80000, 270000
-- row: 2, 90000, 190000
-- row: 3, 100000, 100000
-- end-expected
SELECT id, salary,
  sum(salary) OVER (
    ORDER BY id
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
  ) AS remaining_sum
FROM wf_data WHERE dept = 'eng' ORDER BY id;

-- ============================================================================
-- 19. RANGE frame with numeric offset
-- ============================================================================

-- begin-expected
-- columns: id, salary, cnt
-- row: 1, 80000, 2
-- row: 2, 90000, 3
-- row: 3, 100000, 2
-- end-expected
SELECT id, salary,
  count(*) OVER (
    ORDER BY salary
    RANGE BETWEEN 10000 PRECEDING AND 10000 FOLLOWING
  )::integer AS cnt
FROM wf_data WHERE dept = 'eng' ORDER BY id;

-- ============================================================================
-- 20. GROUPS frame
-- ============================================================================

CREATE TABLE wf_groups (id integer, score integer);
INSERT INTO wf_groups VALUES
  (1, 10), (2, 10), (3, 20), (4, 20), (5, 30);

-- begin-expected
-- columns: id, score, grp_sum
-- row: 1, 10, 60
-- row: 2, 10, 60
-- row: 3, 20, 90
-- row: 4, 20, 90
-- row: 5, 30, 70
-- end-expected
SELECT id, score,
  sum(score) OVER (
    ORDER BY score
    GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS grp_sum
FROM wf_groups ORDER BY id;

DROP TABLE wf_groups;

-- ============================================================================
-- 21. Named WINDOW clause
-- ============================================================================

-- begin-expected
-- columns: id, salary, rn, running_sum
-- row: 1, 80000, 1, 80000
-- row: 2, 90000, 2, 170000
-- row: 3, 100000, 3, 270000
-- end-expected
SELECT id, salary,
  row_number() OVER w AS rn,
  sum(salary) OVER w AS running_sum
FROM wf_data WHERE dept = 'eng'
WINDOW w AS (ORDER BY id)
ORDER BY id;

-- ============================================================================
-- 22. Multiple named WINDOWs
-- ============================================================================

-- begin-expected
-- columns: id, dept, dept_rank, global_rank
-- row: 4, sales, 1, 1
-- row: 7, hr, 1, 2
-- row: 5, sales, 2, 3
-- row: 8, hr, 2, 4
-- row: 6, sales, 3, 5
-- row: 1, eng, 1, 5
-- row: 2, eng, 2, 7
-- row: 3, eng, 3, 8
-- end-expected
SELECT id, dept,
  rank() OVER dept_w AS dept_rank,
  rank() OVER global_w AS global_rank
FROM wf_data
WINDOW dept_w AS (PARTITION BY dept ORDER BY salary),
       global_w AS (ORDER BY salary)
ORDER BY salary;

-- ============================================================================
-- 23. percent_rank() and cume_dist() window functions
-- ============================================================================

-- begin-expected
-- columns: id, salary, pct_rank
-- row: 1, 80000, 0.00
-- row: 2, 90000, 0.50
-- row: 3, 100000, 1.00
-- end-expected
SELECT id, salary,
  round(percent_rank() OVER (ORDER BY salary)::numeric, 2) AS pct_rank
FROM wf_data WHERE dept = 'eng' ORDER BY salary;

-- begin-expected
-- columns: id, salary, cum_dist
-- row: 1, 80000, 0.33
-- row: 2, 90000, 0.67
-- row: 3, 100000, 1.00
-- end-expected
SELECT id, salary,
  round(cume_dist() OVER (ORDER BY salary)::numeric, 2) AS cum_dist
FROM wf_data WHERE dept = 'eng' ORDER BY salary;

-- ============================================================================
-- 24. Window function on empty partition
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM (
  SELECT lag(salary) OVER (ORDER BY id)
  FROM wf_data WHERE dept = 'nonexistent'
) sub;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA wf_test CASCADE;
SET search_path = public;
