-- ---------------------------------------------------------------------------
-- A composite attribute keeps the modifier it was declared with, and one that
-- is dropped keeps its number: PostgreSQL leaves the row behind under a name
-- nobody could have written, so the next attribute added takes a number of its
-- own rather than the one just freed.
-- ---------------------------------------------------------------------------

DROP TYPE IF EXISTS zzw5e_ha CASCADE;
CREATE TYPE zzw5e_ha AS (x int);
ALTER TYPE zzw5e_ha ADD ATTRIBUTE y text;
ALTER TYPE zzw5e_ha RENAME ATTRIBUTE y TO y2;
ALTER TYPE zzw5e_ha ALTER ATTRIBUTE y2 TYPE varchar(5);

-- begin-expected
-- columns: a
-- row: x:integer,y2:character varying(5)
-- end-expected
SELECT string_agg(a.attname || ':' || format_type(a.atttypid, a.atttypmod),
                  ',' ORDER BY a.attnum) AS a
  FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
  WHERE c.relname = 'zzw5e_ha' AND a.attnum > 0;

ALTER TYPE zzw5e_ha DROP ATTRIBUTE y2;
ALTER TYPE zzw5e_ha ADD ATTRIBUTE z int;

-- begin-expected
-- columns: a
-- row: x,........pg.dropped.2........,z
-- end-expected
SELECT string_agg(a.attname, ',' ORDER BY a.attnum) AS a
  FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
  WHERE c.relname = 'zzw5e_ha' AND a.attnum > 0;

-- begin-expected
-- columns: a
-- row: x,z
-- end-expected
SELECT string_agg(a.attname, ',' ORDER BY a.attnum) AS a
  FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
  WHERE c.relname = 'zzw5e_ha' AND a.attnum > 0 AND NOT a.attisdropped;

-- The gap counts towards the relation's attribute count.
-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT c.relnatts::text AS n FROM pg_class c WHERE c.relname = 'zzw5e_ha';

-- information_schema leaves the hole in ordinal_position rather than
-- renumbering the attributes that outlived the dropped one.
-- begin-expected
-- columns: a
-- row: x:1,z:3
-- end-expected
SELECT string_agg(a.attribute_name || ':' || a.ordinal_position::text,
                  ',' ORDER BY a.ordinal_position) AS a
  FROM information_schema.attributes a WHERE a.udt_name = 'zzw5e_ha';

-- The type itself is made of what is left, so a value of it has two fields.
-- begin-expected
-- columns: x
-- row: 7
-- end-expected
SELECT (ROW(7, 8)::zzw5e_ha).x::text AS x;

-- begin-expected
-- columns: z
-- row: 8
-- end-expected
SELECT (ROW(7, 8)::zzw5e_ha).z::text AS z;

DROP TYPE zzw5e_ha;

-- A modifier written on CREATE TYPE is reported the same way.
DROP TYPE IF EXISTS zzw5e_hc CASCADE;
CREATE TYPE zzw5e_hc AS (x int, y varchar(5), w numeric(8,2), u char(4));

-- begin-expected
-- columns: a
-- row: x|integer|-1,y|character varying(5)|9,w|numeric(8,2)|524294,u|character(4)|8
-- end-expected
SELECT string_agg(a.attname || '|' || format_type(a.atttypid, a.atttypmod)
                  || '|' || a.atttypmod::text, ',' ORDER BY a.attnum) AS a
  FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
  WHERE c.relname = 'zzw5e_hc' AND a.attnum > 0;

DROP TYPE zzw5e_hc;

-- ---------------------------------------------------------------------------
-- SET SCHEMA moves a type out of the schema its bare name was answered from.
-- An unqualified type name is the search path's to answer and PostgreSQL looks
-- nowhere else, so the old word reaches nothing at all -- for a domain, an enum
-- and a composite alike -- while the type is reachable where it was moved to.
-- ---------------------------------------------------------------------------

DROP SCHEMA IF EXISTS zzw5e_s2 CASCADE;
DROP DOMAIN IF EXISTS zzw5e_kr CASCADE;
DROP TYPE IF EXISTS zzw5e_ks CASCADE;
DROP TYPE IF EXISTS zzw5e_kc CASCADE;
CREATE SCHEMA zzw5e_s2;
CREATE DOMAIN zzw5e_kr AS int;
CREATE TYPE zzw5e_ks AS ENUM ('a');
CREATE TYPE zzw5e_kc AS (q int);
ALTER DOMAIN zzw5e_kr SET SCHEMA zzw5e_s2;
ALTER TYPE zzw5e_ks SET SCHEMA zzw5e_s2;
ALTER TYPE zzw5e_kc SET SCHEMA zzw5e_s2;

-- begin-expected
-- columns: s
-- row: zzw5e_s2
-- end-expected
SELECT n.nspname AS s FROM pg_type t
  JOIN pg_namespace n ON n.oid = t.typnamespace WHERE t.typname = 'zzw5e_kr';

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zzw5e_kr" does not exist
-- end-expected-error
CREATE TABLE zzw5e_t9 (c zzw5e_kr);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zzw5e_ks" does not exist
-- end-expected-error
CREATE TABLE zzw5e_ta (c zzw5e_ks);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zzw5e_kc" does not exist
-- end-expected-error
CREATE TABLE zzw5e_tb (c zzw5e_kc);

-- Written where it now lives, all three are found.
CREATE TABLE zzw5e_tc (c zzw5e_s2.zzw5e_kc, d zzw5e_s2.zzw5e_kr,
                       e zzw5e_s2.zzw5e_ks);

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*)::text AS n FROM information_schema.columns
  WHERE table_name = 'zzw5e_tc';

DROP TABLE zzw5e_tc;
DROP SCHEMA zzw5e_s2 CASCADE;