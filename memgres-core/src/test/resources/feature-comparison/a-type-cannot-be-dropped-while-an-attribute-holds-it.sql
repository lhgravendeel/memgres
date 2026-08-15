-- ============================================================================
-- An attribute of a composite type depends on the type it was declared with
--
-- A composite type is a relation of attributes, and every attribute holds the
-- type it was declared with as firmly as a table column does. So a DROP of
-- that type is refused while the attribute stands, under RESTRICT as under no
-- keyword at all, and the type is still there afterwards -- a refused drop
-- takes nothing.
--
-- CASCADE takes the attribute rather than the type that holds it: the
-- composite survives with one attribute fewer, and the attribute it lost is
-- left behind in pg_attribute under a placeholder name with attisdropped set,
-- so the attributes after it keep the numbers they had and the next attribute
-- added takes the number past them. It reads the same whether the attribute
-- was declared with an enum, a domain or another composite, and a table
-- column written with the type is refused and cascaded beside the attribute.
-- Every answer here was read off PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TYPE zzt4d_qe AS ENUM ('a','b');
CREATE TYPE zzt4d_qc AS (x zzt4d_qe, y int);

-- ----------------------------------------------------------------------------
-- The drop is refused, and takes nothing
-- ----------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzt4d_qe because other objects depend on it
-- end-expected-error
DROP TYPE zzt4d_qe;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzt4d_qe because other objects depend on it
-- end-expected-error
DROP TYPE zzt4d_qe RESTRICT;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM pg_type WHERE typname = 'zzt4d_qe';

-- ----------------------------------------------------------------------------
-- CASCADE takes the attribute and leaves the composite standing
-- ----------------------------------------------------------------------------
DROP TYPE zzt4d_qe CASCADE;

-- begin-expected
-- columns: typtype
-- row: c
-- end-expected
SELECT typtype::text AS typtype FROM pg_type WHERE typname = 'zzt4d_qc';

-- the attribute it lost is still numbered, so the one after it keeps its number
-- begin-expected
-- columns: atts
-- row: ........pg.dropped.1......../1/true,y/2/false
-- end-expected
SELECT string_agg(attname || '/' || attnum::text || '/' || attisdropped::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = (SELECT typrelid FROM pg_type WHERE typname = 'zzt4d_qc') AND attnum > 0;

-- and an attribute added later takes the number past it
ALTER TYPE zzt4d_qc ADD ATTRIBUTE z int;

-- begin-expected
-- columns: atts
-- row: ........pg.dropped.1......../1/true,y/2/false,z/3/false
-- end-expected
SELECT string_agg(attname || '/' || attnum::text || '/' || attisdropped::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = (SELECT typrelid FROM pg_type WHERE typname = 'zzt4d_qc') AND attnum > 0;

DROP TYPE zzt4d_qc;

-- ----------------------------------------------------------------------------
-- A domain and a composite hold an attribute the same way
-- ----------------------------------------------------------------------------
CREATE DOMAIN zzt4d_qd AS int;
CREATE TYPE zzt4d_qk AS (x zzt4d_qd);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzt4d_qd because other objects depend on it
-- end-expected-error
DROP DOMAIN zzt4d_qd;

DROP TYPE zzt4d_qk;
DROP DOMAIN zzt4d_qd;

CREATE TYPE zzt4d_qk1 AS (x int);
CREATE TYPE zzt4d_qk2 AS (y zzt4d_qk1);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzt4d_qk1 because other objects depend on it
-- end-expected-error
DROP TYPE zzt4d_qk1;

DROP TYPE zzt4d_qk2;
DROP TYPE zzt4d_qk1;

-- ----------------------------------------------------------------------------
-- A table column and an attribute are refused together and cascaded together
-- ----------------------------------------------------------------------------
CREATE TYPE zzt4d_qe2 AS ENUM ('a','b');
CREATE TYPE zzt4d_qc2 AS (x zzt4d_qe2, y int);
CREATE TABLE zzt4d_qt (c zzt4d_qe2, d int);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzt4d_qe2 because other objects depend on it
-- end-expected-error
DROP TYPE zzt4d_qe2;

DROP TYPE zzt4d_qe2 CASCADE;

-- the relation stands, holding the columns the type did not reach
-- begin-expected
-- columns: cols
-- row: d
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'zzt4d_qt';

-- and its own dropped column keeps its number too
-- begin-expected
-- columns: atts
-- row: ........pg.dropped.1......../1/true,d/2/false
-- end-expected
SELECT string_agg(attname || '/' || attnum::text || '/' || attisdropped::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zzt4d_qt'::regclass AND attnum > 0;

-- begin-expected
-- columns: typtype
-- row: c
-- end-expected
SELECT typtype::text AS typtype FROM pg_type WHERE typname = 'zzt4d_qc2';

-- cleanup
DROP TABLE zzt4d_qt;
DROP TYPE zzt4d_qc2;
