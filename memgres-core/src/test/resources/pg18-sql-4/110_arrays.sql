DROP SCHEMA IF EXISTS test_110 CASCADE;
CREATE SCHEMA test_110;
SET search_path TO test_110;

CREATE TABLE arr_test (
    id integer PRIMARY KEY,
    nums integer[],
    tags text[]
);

INSERT INTO arr_test VALUES
    (1, ARRAY[1,2,3], ARRAY['red','blue']),
    (2, ARRAY[2,4], ARRAY['green']),
    (3, ARRAY[]::integer[], ARRAY[]::text[]);

-- begin-expected
-- columns: id|second_num|slice_text|num_len|card
-- row: 1|2|{1,2}|3|3
-- row: 2|4|{2,4}|2|2
-- row: 3||||0
-- end-expected
SELECT
    id,
    nums[2] AS second_num,
    nums[1:2]::text AS slice_text,
    array_length(nums, 1) AS num_len,
    cardinality(nums) AS card
FROM arr_test
ORDER BY id;

-- begin-expected
-- columns: id|has_two|overlap_tags|contains_pair
-- row: 1|true|true|true
-- row: 2|true|false|false
-- row: 3|false|false|false
-- end-expected
SELECT
    id,
    2 = ANY(nums) AS has_two,
    tags && ARRAY['blue','black'] AS overlap_tags,
    nums @> ARRAY[1,2] AS contains_pair
FROM arr_test
ORDER BY id;

-- begin-expected
-- columns: id|n
-- row: 1|1
-- row: 1|2
-- row: 1|3
-- row: 2|2
-- row: 2|4
-- end-expected
SELECT a.id, u.n
FROM arr_test a
CROSS JOIN LATERAL unnest(a.nums) AS u(n)
ORDER BY a.id, u.n;

DROP SCHEMA test_110 CASCADE;
