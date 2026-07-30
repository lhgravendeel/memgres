-- ============================================================================
-- Feature Comparison: rules in the catalogs, and a composite's array type
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Two ways the catalogs described less than the engine actually holds.
--
-- A rule written with CREATE RULE worked, but nothing in the catalogs said so:
-- pg_rewrite held only the implicit _RETURN rules of views, pg_class.relhasrules
-- stayed false, and pg_get_ruledef had no row to be reached through. A tool
-- asking what rules a relation carries was told none.
--
-- And every composite type reported typarray = 0. In PostgreSQL a composite
-- always has its matching _name array type, so a client following typarray to
-- describe an array of a row type found nothing at the far end.
--
-- Counts over the whole of pg_type or pg_opclass are not compared here: the
-- reference server carries contrib extensions CI does not have. Named rows and
-- the invariant "no composite has typarray = 0" are what is pinned.
-- ============================================================================

-- ============================================================================
-- 1. A rule is a pg_rewrite row, with its event and whether it replaces
-- ============================================================================
DROP TABLE IF EXISTS crt_r CASCADE;
CREATE TABLE crt_r (i int PRIMARY KEY, j int);
CREATE RULE crt_del AS ON DELETE TO crt_r DO INSTEAD NOTHING;
CREATE RULE crt_upd AS ON UPDATE TO crt_r DO INSTEAD NOTHING;

-- begin-expected
-- columns: rulename, ev_type, ev_enabled, is_instead
-- row: crt_del, 4, O, true
-- row: crt_upd, 2, O, true
-- end-expected
SELECT rulename::text AS rulename, ev_type::text AS ev_type,
       ev_enabled::text AS ev_enabled, is_instead::text AS is_instead
  FROM pg_rewrite WHERE ev_class = 'crt_r'::regclass ORDER BY rulename;

-- ============================================================================
-- 2. relhasrules says the relation carries one
-- ============================================================================
-- begin-expected
-- columns: relhasrules
-- row: true
-- end-expected
SELECT relhasrules::text AS relhasrules FROM pg_class WHERE relname = 'crt_r';

-- ============================================================================
-- 3. pg_rules writes the definition the way pg_get_ruledef does
-- ============================================================================
-- The newline is folded away so the value fits one annotation line.
-- begin-expected
-- columns: rulename, definition
-- row: crt_del, CREATE RULE crt_del AS~    ON DELETE TO public.crt_r DO INSTEAD NOTHING;
-- end-expected
SELECT rulename::text AS rulename,
       replace(definition, chr(10), '~')::text AS definition
  FROM pg_rules WHERE tablename = 'crt_r' AND rulename = 'crt_del';

-- ============================================================================
-- 4. pg_get_ruledef is reachable through the pg_rewrite row
-- ============================================================================
-- begin-expected
-- columns: rulename, d
-- row: crt_upd, CREATE RULE crt_upd AS~    ON UPDATE TO public.crt_r DO INSTEAD NOTHING;
-- end-expected
SELECT r.rulename::text AS rulename,
       replace(pg_get_ruledef(r.oid), chr(10), '~')::text AS d
  FROM pg_rewrite r
 WHERE r.ev_class = 'crt_r'::regclass AND r.rulename = 'crt_upd';

-- ============================================================================
-- 5. Dropping the rule takes the catalog rows with it
-- ============================================================================
DROP RULE crt_del ON crt_r;
DROP RULE crt_upd ON crt_r;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM pg_rewrite WHERE ev_class = 'crt_r'::regclass;

-- relhasrules is documented as "has (or once had) rules" and PostgreSQL only
-- clears it at VACUUM, so it stands after the rules are gone.
-- begin-expected
-- columns: relhasrules
-- row: true
-- end-expected
SELECT relhasrules::text AS relhasrules FROM pg_class WHERE relname = 'crt_r';

DROP TABLE IF EXISTS crt_r CASCADE;

-- ============================================================================
-- 6. No composite type is left without its array type
-- ============================================================================
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM pg_type WHERE typtype = 'c' AND typarray = 0;

-- the catalog relations' own row types are composites too
-- begin-expected
-- columns: typname
-- row: _pg_class
-- row: _pg_index
-- row: _pg_proc
-- row: _pg_type
-- end-expected
SELECT typname::text AS typname FROM pg_type
 WHERE typname IN ('_pg_class','_pg_proc','_pg_type','_pg_index') ORDER BY typname;

-- and the link runs both ways
-- begin-expected
-- columns: arr, elem
-- row: _pg_class, pg_class
-- end-expected
SELECT t.typname::text AS arr, e.typname::text AS elem
  FROM pg_type t JOIN pg_type e ON e.oid = t.typelem WHERE t.typname = '_pg_class';

-- ============================================================================
-- 7. A table's row type and a user composite get one the same way
-- ============================================================================
DROP TABLE IF EXISTS crt_t CASCADE;
CREATE TABLE crt_t (a int, b text);

-- begin-expected
-- columns: linked
-- row: true
-- end-expected
SELECT (typarray <> 0)::text AS linked FROM pg_type WHERE typname = 'crt_t';

-- begin-expected
-- columns: typname, typtype
-- row: _crt_t, b
-- end-expected
SELECT typname::text AS typname, typtype::text AS typtype
  FROM pg_type WHERE typname = '_crt_t';

DROP TABLE IF EXISTS crt_t CASCADE;

DROP TYPE IF EXISTS crt_ct CASCADE;
CREATE TYPE crt_ct AS (x int, y text);

-- begin-expected
-- columns: linked
-- row: true
-- end-expected
SELECT (typarray <> 0)::text AS linked FROM pg_type WHERE typname = 'crt_ct';

-- begin-expected
-- columns: arr, elem
-- row: _crt_ct, crt_ct
-- end-expected
SELECT t.typname::text AS arr, e.typname::text AS elem
  FROM pg_type t JOIN pg_type e ON e.oid = t.typelem WHERE t.typname = '_crt_ct';

DROP TYPE IF EXISTS crt_ct CASCADE;
