DROP SCHEMA IF EXISTS test_290 CASCADE;
CREATE SCHEMA test_290;
SET search_path TO test_290;

CREATE TABLE weird (
    id integer PRIMARY KEY,
    a integer,
    b double precision,
    txt text
);

INSERT INTO weird VALUES
    (1, NULL, 'NaN', ''),
    (2, 0, 'Infinity', NULL),
    (3, 5, '-Infinity', 'x');

-- begin-expected
-- columns: id|same_as_null|coalesced|txt_kind
-- row: 1|true|-1|empty
-- row: 2|false|0|null
-- row: 3|false|5|value
-- end-expected
SELECT
    id,
    (a IS NOT DISTINCT FROM NULL) AS same_as_null,
    COALESCE(a, -1) AS coalesced,
    CASE
        WHEN txt IS NULL THEN 'null'
        WHEN txt = '' THEN 'empty'
        ELSE 'value'
    END AS txt_kind
FROM weird
ORDER BY id;

-- begin-expected
-- columns: id|b_txt
-- row: 3|-Infinity
-- row: 2|Infinity
-- row: 1|NaN
-- end-expected
SELECT id, b::text AS b_txt
FROM weird
ORDER BY b NULLS LAST;

DROP SCHEMA test_290 CASCADE;
