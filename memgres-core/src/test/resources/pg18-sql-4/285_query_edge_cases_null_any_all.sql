DROP SCHEMA IF EXISTS test_285 CASCADE;
CREATE SCHEMA test_285;
SET search_path TO test_285;

CREATE TABLE qvals (
    id integer PRIMARY KEY,
    val integer,
    arr integer[],
    note text
);

INSERT INTO qvals(id, val, arr, note) VALUES
    (1, 1, ARRAY[1,2,NULL], 'one'),
    (2, 2, ARRAY[3,4], 'two'),
    (3, NULL, ARRAY[NULL,5], 'null-val'),
    (4, 5, ARRAY[]::integer[], 'empty'),
    (5, 7, NULL, 'null-arr');

-- begin-expected
-- columns: id|not_in_simple|not_in_with_null_rhs|in_with_null_rhs
-- row: 1|f|f|t
-- row: 2|t||
-- row: 3|||
-- row: 4|t||
-- row: 5|t||
-- end-expected
SELECT id,
       val NOT IN (1, 3) AS not_in_simple,
       val NOT IN (1, NULL, 3) AS not_in_with_null_rhs,
       val IN (1, NULL, 3) AS in_with_null_rhs
FROM qvals
ORDER BY id;

-- begin-expected
-- columns: id|eq_any_arr|gt_all_arr|lt_all_arr|eq_any_subq
-- row: 1|t|f|f|t
-- row: 2|f|f|t|f
-- row: 3||||
-- row: 4|f|t|t|t
-- row: 5||||f
-- end-expected
SELECT id,
       val = ANY(arr) AS eq_any_arr,
       val > ALL(arr) AS gt_all_arr,
       val < ALL(arr) AS lt_all_arr,
       val = ANY(SELECT x FROM (VALUES (1), (5), (9)) AS v(x)) AS eq_any_subq
FROM qvals
ORDER BY id;

-- begin-expected
-- columns: id|exists_nonnull_arr|exists_matching_arr|scalar_subq_max|coalesced_max
-- row: 1|t|t|2|2
-- row: 2|t|f|4|4
-- row: 3|t|f|5|5
-- row: 4|f|f||-1
-- row: 5|f|f||-1
-- end-expected
SELECT q.id,
       EXISTS (
           SELECT 1
           FROM unnest(COALESCE(q.arr, ARRAY[]::integer[])) AS u(x)
       ) AS exists_nonnull_arr,
       EXISTS (
           SELECT 1
           FROM unnest(COALESCE(q.arr, ARRAY[]::integer[])) AS u(x)
           WHERE x = q.val
       ) AS exists_matching_arr,
       (
           SELECT max(x)
           FROM unnest(q.arr) AS u(x)
       ) AS scalar_subq_max,
       COALESCE((
           SELECT max(x)
           FROM unnest(q.arr) AS u(x)
       ), -1) AS coalesced_max
FROM qvals AS q
ORDER BY q.id;

-- begin-expected
-- columns: id|arr_state|all_null_elements|cardinality_arr
-- row: 1|present|f|3
-- row: 2|present|f|2
-- row: 3|present|f|2
-- row: 4|present|t|0
-- row: 5|null||
-- end-expected
SELECT id,
       CASE WHEN arr IS NULL THEN 'null' ELSE 'present' END AS arr_state,
       CASE
           WHEN arr IS NULL THEN NULL
           ELSE COALESCE((SELECT bool_and(x IS NULL) FROM unnest(arr) AS u(x)), true)
       END AS all_null_elements,
       cardinality(arr) AS cardinality_arr
FROM qvals
ORDER BY id;

DROP SCHEMA test_285 CASCADE;
