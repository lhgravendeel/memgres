DROP SCHEMA IF EXISTS test_810 CASCADE;
CREATE SCHEMA test_810;
SET search_path TO test_810;

CREATE TABLE t1 (
    id integer PRIMARY KEY,
    name text
);

INSERT INTO t1 VALUES (1, 'x'), (2, 'y');

-- begin-expected
-- columns: has_stat_row
-- row: t
-- end-expected
SELECT EXISTS (
    SELECT 1
    FROM pg_stat_user_tables
    WHERE schemaname = 'test_810'
      AND relname = 't1'
) AS has_stat_row;

-- begin-expected
-- columns: has_pk_index
-- row: t
-- end-expected
SELECT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'test_810'
      AND tablename = 't1'
      AND indexname = 't1_pkey'
) AS has_pk_index;

-- begin-expected
-- columns: relation_size_positive
-- row: t
-- end-expected
SELECT pg_relation_size('test_810.t1'::regclass) > 0 AS relation_size_positive;

