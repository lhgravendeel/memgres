DROP SCHEMA IF EXISTS test_045 CASCADE;
CREATE SCHEMA test_045;
SET search_path TO test_045;

CREATE TABLE expr_data (
    id integer PRIMARY KEY,
    a integer,
    b integer,
    c text,
    d boolean
);

INSERT INTO expr_data(id, a, b, c, d) VALUES
    (1, 10, 3, 'Alpha', true),
    (2, 7,  2, 'beta', false),
    (3, NULL, 5, 'Gamma', NULL),
    (4, -4, 6, 'alphabet', true),
    (5, 10, NULL, 'delta', false);

-- begin-expected
-- columns: id|sum_ab|diff_ab|mul_ab|int_div_ab|mod_ab|pow_ab
-- row: 1|13|7|30|3|1|1000
-- row: 2|9|5|14|3|1|49
-- row: 4|2|-10|-24|0|-4|4096
-- end-expected
SELECT id,
       a + b AS sum_ab,
       a - b AS diff_ab,
       a * b AS mul_ab,
       a / b AS int_div_ab,
       a % b AS mod_ab,
       a ^ b AS pow_ab
FROM expr_data
WHERE id IN (1,2,4)
ORDER BY id;

-- begin-expected
-- columns: id|simple_case|searched_case|coalesced|nullif_demo|greatest_ab|least_ab
-- row: 1|ten|big|10|3|10|3
-- row: 2|other|small|7|2|7|2
-- row: 3|other|missing a|999|5|5|5
-- row: 4|other|small|-4|6|6|-4
-- row: 5|ten|big|10||10|10
-- end-expected
SELECT id,
       CASE a WHEN 10 THEN 'ten' ELSE 'other' END AS simple_case,
       CASE
           WHEN a IS NULL THEN 'missing a'
           WHEN a >= 10 THEN 'big'
           ELSE 'small'
       END AS searched_case,
       COALESCE(a, 999) AS coalesced,
       NULLIF(b, 0) AS nullif_demo,
       GREATEST(COALESCE(a, b), COALESCE(b, a)) AS greatest_ab,
       LEAST(COALESCE(a, b), COALESCE(b, a)) AS least_ab
FROM expr_data
ORDER BY id;

-- begin-expected
-- columns: id|comparison_bucket|not_equal_ten|between_check|in_check|not_in_check|distinct_from_ten
-- row: 1|ge|f|t|t|f|f
-- row: 2|ge|t|t|t|t|t
-- row: 3|unknown||||t|t
-- row: 4|lt|t|f|f|t|t
-- row: 5|unknown|f|t|t|f|f
-- end-expected
SELECT id,
       CASE
           WHEN a >= b THEN 'ge'
           WHEN a < b THEN 'lt'
           ELSE 'unknown'
       END AS comparison_bucket,
       a <> 10 AS not_equal_ten,
       a BETWEEN 0 AND 10 AS between_check,
       a IN (7, 10, 12) AS in_check,
       COALESCE(a, -999) NOT IN (10, -9999) AS not_in_check,
       a IS DISTINCT FROM 10 AS distinct_from_ten
FROM expr_data
ORDER BY id;

-- begin-expected
-- columns: id|normalized|prefix3|contains_alpha_ci|similar_match|regex_match_ci|position_of_a
-- row: 1|alpha|Alp|t|t|t|1
-- row: 2|beta|bet|f|t|t|4
-- row: 3|gamma|Gam|f|t|t|2
-- row: 4|alphabet|alp|t|f|t|1
-- row: 5|delta|del|f|t|t|5
-- end-expected
SELECT id,
       lower(c) AS normalized,
       substring(c FROM 1 FOR 3) AS prefix3,
       c ILIKE '%alpha%' AS contains_alpha_ci,
       c SIMILAR TO '(Alpha|beta|Gamma|delta)' AS similar_match,
       c ~* 'a.*' AS regex_match_ci,
       position('a' IN lower(c)) AS position_of_a
FROM expr_data
ORDER BY id;

-- begin-expected
-- columns: id|bool_state|not_d|is_true|is_not_false|is_unknown
-- row: 1|true|f|t|t|f
-- row: 2|false|t|f|f|f
-- row: 3|null||f|t|t
-- row: 4|true|f|t|t|f
-- row: 5|false|t|f|f|f
-- end-expected
SELECT id,
       CASE WHEN d IS TRUE THEN 'true' WHEN d IS FALSE THEN 'false' ELSE 'null' END AS bool_state,
       NOT d AS not_d,
       d IS TRUE AS is_true,
       d IS NOT FALSE AS is_not_false,
       d IS UNKNOWN AS is_unknown
FROM expr_data
ORDER BY id;

-- begin-expected
-- columns: id|row_eq_target|row_lt_target|row_ge_target
-- row: 1|t|f|t
-- row: 2|f|t|f
-- row: 4|f|t|f
-- row: 5|||
-- end-expected
SELECT id,
       (a, b) = (10, 3) AS row_eq_target,
       (a, b) < (10, 3) AS row_lt_target,
       (a, b) >= (10, 3) AS row_ge_target
FROM expr_data
WHERE id IN (1,2,4,5)
ORDER BY id;

DROP SCHEMA test_045 CASCADE;
