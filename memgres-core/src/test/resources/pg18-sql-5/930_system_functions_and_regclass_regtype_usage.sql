DROP SCHEMA IF EXISTS test_930 CASCADE;
CREATE SCHEMA test_930;
SET search_path TO test_930;

CREATE TABLE demo (
    demo_id integer PRIMARY KEY,
    payload text
);

INSERT INTO demo VALUES (1, 'x');

-- begin-expected
-- columns: inferred_type
-- row: integer
-- end-expected
SELECT pg_typeof(demo_id)::text AS inferred_type
FROM demo
WHERE demo_id = 1;

-- begin-expected
-- columns: formatted
-- row: table demo exists
-- end-expected
SELECT format('table %s exists', to_regclass('test_930.demo')) AS formatted;

-- begin-expected
-- columns: regclass_text
-- row: demo
-- end-expected
SELECT 'test_930.demo'::regclass::text AS regclass_text;

-- begin-expected
-- columns: regtype_text
-- row: integer
-- end-expected
SELECT 'integer'::regtype::text AS regtype_text;

-- begin-expected
-- columns: constraint_def_contains_pkey
-- row: t
-- end-expected
SELECT position('PRIMARY KEY' in pg_get_constraintdef(oid)) > 0 AS constraint_def_contains_pkey
FROM pg_constraint
WHERE conrelid = 'test_930.demo'::regclass
  AND contype = 'p';

