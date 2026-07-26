-- What the catalog tells a tool about the server: which functions exist, which array types are
-- registered, and how ranges and row types are classified.

DROP TABLE IF EXISTS cpt_t CASCADE;
CREATE TABLE cpt_t (id int PRIMARY KEY, name text);

-- A function memgres evaluates has to be a function the catalog knows about.
-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS a FROM pg_proc WHERE proname = 'upper';

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS a FROM pg_proc WHERE proname = 'abs';

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS a FROM pg_proc WHERE proname = 'to_char';

-- begin-expected
-- columns: nspname
-- row: pg_catalog
-- end-expected
SELECT n.nspname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE p.proname = 'upper' LIMIT 1;

-- A name carried only once resolves as a regproc.
-- begin-expected
-- columns: a
-- row: pg_backend_pid
-- end-expected
SELECT 'pg_backend_pid'::regproc::text AS a;

-- The common array types are registered, and each names what it holds.
-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (count(*) = 11)::text AS a FROM pg_type WHERE typname IN
  ('_int4','_text','_bool','_numeric','_uuid','_jsonb','_bit','_box','_oid','_xml','_macaddr');

-- begin-expected
-- columns: typname
-- row: int4
-- end-expected
SELECT e.typname FROM pg_type a JOIN pg_type e ON e.oid = a.typelem WHERE a.typname = '_int4';

-- A range is its own kind of type, and a multirange another.
-- begin-expected
-- columns: t
-- row: r
-- end-expected
SELECT typtype::text AS t FROM pg_type WHERE typname = 'int4range';

-- begin-expected
-- columns: t
-- row: r
-- end-expected
SELECT typtype::text AS t FROM pg_type WHERE typname = 'numrange';

-- begin-expected
-- columns: t
-- row: m
-- end-expected
SELECT typtype::text AS t FROM pg_type WHERE typname = 'int4multirange';

-- begin-expected
-- columns: t
-- row: m
-- end-expected
SELECT typtype::text AS t FROM pg_type WHERE typname = 'datemultirange';

-- Ordinary types are still base types.
-- begin-expected
-- columns: t
-- row: b
-- end-expected
SELECT typtype::text AS t FROM pg_type WHERE typname = 'int4';

-- begin-expected
-- columns: t
-- row: b
-- end-expected
SELECT typtype::text AS t FROM pg_type WHERE typname = 'text';

-- A table is also a row type.
-- begin-expected
-- columns: t
-- row: c
-- end-expected
SELECT typtype::text AS t FROM pg_type WHERE typname = 'cpt_t';

-- So is a composite type, and only once.
DROP TYPE IF EXISTS cpt_c CASCADE;
CREATE TYPE cpt_c AS (a int, b text);

-- begin-expected
-- columns: t
-- row: c
-- end-expected
SELECT typtype::text AS t FROM pg_type WHERE typname = 'cpt_c';

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_type WHERE typname = 'cpt_c';

DROP TYPE cpt_c;
DROP TABLE cpt_t;
