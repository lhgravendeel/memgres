DROP SCHEMA IF EXISTS test_740 CASCADE;
CREATE SCHEMA test_740;
SET search_path TO test_740;

CREATE TABLE base_table (
    id integer PRIMARY KEY,
    amount integer NOT NULL
);

CREATE VIEW base_view AS
SELECT id, amount, amount * 2 AS doubled
FROM base_table;

CREATE FUNCTION plus_one(x integer)
RETURNS integer
LANGUAGE SQL
AS $$
  SELECT x + 1
$$;

-- begin-expected
-- columns: view_definition_contains_doubled
-- row: t
-- end-expected
SELECT position('doubled' in pg_get_viewdef('test_740.base_view'::regclass, true)) > 0 AS view_definition_contains_doubled;

-- begin-expected
-- columns: function_definition_contains_select
-- row: t
-- end-expected
SELECT position('SELECT x + 1' in pg_get_functiondef('test_740.plus_one(integer)'::regprocedure)) > 0 AS function_definition_contains_select;

-- begin-expected
-- columns: has_view_dependency
-- row: t
-- end-expected
SELECT EXISTS (
    SELECT 1
    FROM pg_depend d
    JOIN pg_rewrite r ON r.oid = d.objid
    JOIN pg_class vc ON vc.oid = r.ev_class
    JOIN pg_namespace vn ON vn.oid = vc.relnamespace
    JOIN pg_class tc ON tc.oid = d.refobjid
    JOIN pg_namespace tn ON tn.oid = tc.relnamespace
    WHERE vn.nspname = 'test_740'
      AND vc.relname = 'base_view'
      AND tn.nspname = 'test_740'
      AND tc.relname = 'base_table'
) AS has_view_dependency;

