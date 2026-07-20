-- Wire protocol behavior tests (H7, H12, M25, L5, L9, L10)

-- L5: Syntax error position should be 1-based (PG convention)
-- begin-expected-error
-- error: 42601
-- end-expected
SELECT FROM WHERE;

-- L9: RAISE WARNING should have SQLState 01000, not 00000
CREATE OR REPLACE FUNCTION wpb_warn() RETURNS void AS $$
BEGIN
  RAISE WARNING 'test warning message';
END;
$$ LANGUAGE plpgsql;

SELECT wpb_warn();

-- L10: COLUMN_DEF formatting
CREATE TABLE wpb_defaults(
  id int PRIMARY KEY,
  ts timestamp DEFAULT CURRENT_TIMESTAMP,
  name text DEFAULT 'hello',
  active boolean DEFAULT true
);

-- begin-expected
-- columns: column_default
-- row: CURRENT_TIMESTAMP
-- end-expected
SELECT column_default FROM information_schema.columns
WHERE table_name='wpb_defaults' AND column_name='ts';

-- L10: Composite index should report all columns
CREATE TABLE wpb_comp(a int, b int, c text);
CREATE INDEX wpb_comp_idx ON wpb_comp(a, b);

-- begin-expected
-- columns: indexdef
-- row: CREATE INDEX wpb_comp_idx ON public.wpb_comp USING btree (a, b)
-- end-expected
SELECT pg_get_indexdef(c.oid) AS indexdef
FROM pg_class c WHERE c.relname = 'wpb_comp_idx';

-- L10: pg_get_indexdef with column number
-- begin-expected
-- columns: col1, col2
-- row: a, b
-- end-expected
SELECT pg_get_indexdef(c.oid, 1, true) AS col1, pg_get_indexdef(c.oid, 2, true) AS col2
FROM pg_class c WHERE c.relname = 'wpb_comp_idx';

-- L10: getFunctionColumns should report params for SQL functions
CREATE FUNCTION wpb_add(a int, b int) RETURNS int AS $$
  SELECT a + b;
$$ LANGUAGE sql;

-- begin-expected
-- columns: proargnames
-- row: {a,b}
-- end-expected
SELECT proargnames FROM pg_proc WHERE proname = 'wpb_add';

-- M25: Procedures should not be callable as functions
CREATE PROCEDURE wpb_noop() AS $$
BEGIN
  -- nothing
END;
$$ LANGUAGE plpgsql;

-- begin-expected-error
-- error: 42809
-- end-expected
SELECT wpb_noop();

-- Cleanup
DROP FUNCTION IF EXISTS wpb_warn();
DROP TABLE IF EXISTS wpb_defaults;
DROP TABLE IF EXISTS wpb_comp;
DROP FUNCTION IF EXISTS wpb_add(int, int);
DROP PROCEDURE IF EXISTS wpb_noop();
