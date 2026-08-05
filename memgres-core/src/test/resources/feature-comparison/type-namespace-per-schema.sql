-- Types live in schemas, and a schema holds one name across types and relations alike.
--
-- A composite type owns a pg_class row as well as a pg_type row, so its name is taken for
-- tables, views, sequences and indexes too. A table, a view and a materialized view own a row
-- type, so their names are taken for types. A sequence and an index own no row type, so a type
-- may share a name with either. Which check a statement reports first decides the SQLSTATE:
-- a statement making a type looks at the type name first (42710) and at the relation name after
-- (42P07); a statement making a relation looks the other way round.

DROP SCHEMA IF EXISTS tnp_a CASCADE;
DROP SCHEMA IF EXISTS tnp_b CASCADE;
CREATE SCHEMA tnp_a;
CREATE SCHEMA tnp_b;
SET search_path = public;

-- ---------------------------------------------------------------------------
-- 1. CREATE TYPE lands in the schema it names
-- ---------------------------------------------------------------------------

CREATE TYPE tnp_a.tnp_comp AS (a int, b text);

-- begin-expected
-- columns: nspname,typtype
-- row: tnp_a|c
-- end-expected
SELECT n.nspname, t.typtype::text AS typtype FROM pg_type t
  JOIN pg_namespace n ON n.oid = t.typnamespace WHERE t.typname = 'tnp_comp';

-- A composite type owns a relation of its own, and that relation is in the same schema.
-- begin-expected
-- columns: nspname,relkind
-- row: tnp_a|c
-- end-expected
SELECT n.nspname, c.relkind::text AS relkind FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'tnp_comp';

-- pg_type.typrelid names that very relation.
-- begin-expected
-- columns: linked
-- row: true
-- end-expected
SELECT (t.typrelid = c.oid) AS linked FROM pg_type t
  JOIN pg_class c ON c.relname = t.typname WHERE t.typname = 'tnp_comp';

-- Its attributes are described where the type is, not where the search path points.
-- begin-expected
-- columns: udt_schema,attribute_name
-- row: tnp_a|a
-- row: tnp_a|b
-- end-expected
SELECT udt_schema, attribute_name FROM information_schema.attributes
  WHERE udt_name = 'tnp_comp' ORDER BY ordinal_position;

-- A composite type is not a table, so information_schema.tables says nothing about it.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM information_schema.tables WHERE table_name = 'tnp_comp';

-- ---------------------------------------------------------------------------
-- 2. CREATE TYPE with no qualifier lands where the search path puts a CREATE
-- ---------------------------------------------------------------------------

SET search_path = tnp_a, public;

CREATE TYPE tnp_sp_comp AS (a int);
CREATE TYPE tnp_sp_enum AS ENUM ('x', 'y');
CREATE DOMAIN tnp_sp_dom AS int;
CREATE TYPE tnp_sp_range AS RANGE (subtype = int4);

-- begin-expected
-- columns: typname,nspname
-- row: tnp_sp_comp|tnp_a
-- row: tnp_sp_dom|tnp_a
-- row: tnp_sp_enum|tnp_a
-- row: tnp_sp_range|tnp_a
-- end-expected
SELECT t.typname::text AS typname, n.nspname FROM pg_type t
  JOIN pg_namespace n ON n.oid = t.typnamespace
  WHERE t.typname IN ('tnp_sp_comp', 'tnp_sp_enum', 'tnp_sp_dom', 'tnp_sp_range')
  ORDER BY t.typname;

SET search_path = public;

-- ---------------------------------------------------------------------------
-- 3. A composite type's row type is in the relation namespace
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "tnp_comp" already exists
-- end-expected-error
CREATE TABLE tnp_a.tnp_comp (a int);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "tnp_comp" already exists
-- end-expected-error
CREATE SEQUENCE tnp_a.tnp_comp;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "tnp_comp" already exists
-- end-expected-error
CREATE VIEW tnp_a.tnp_comp AS SELECT 1;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "tnp_comp" already exists
-- end-expected-error
CREATE MATERIALIZED VIEW tnp_a.tnp_comp AS SELECT 1;

CREATE TABLE tnp_a.tnp_base (a int);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "tnp_comp" already exists
-- end-expected-error
CREATE INDEX tnp_comp ON tnp_a.tnp_base (a);

-- Renaming a table onto a composite type's name collides just as onto a table's.
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "tnp_comp" already exists
-- end-expected-error
ALTER TABLE tnp_a.tnp_base RENAME TO tnp_comp;

-- The other schema is free: a name is taken in one schema only.
CREATE TABLE tnp_b.tnp_comp (a int);

-- begin-expected
-- columns: nspname,relkind
-- row: tnp_a|c
-- row: tnp_b|r
-- end-expected
SELECT n.nspname, c.relkind::text AS relkind FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relname = 'tnp_comp' ORDER BY n.nspname;

DROP TABLE tnp_b.tnp_comp;

-- ---------------------------------------------------------------------------
-- 4. Dropping a composite type with the wrong DROP
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "tnp_comp" is not a table
-- end-expected-error
DROP TABLE tnp_a.tnp_comp;

-- IF EXISTS does not turn the wrong kind into nothing to do.
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "tnp_comp" is not a table
-- end-expected-error
DROP TABLE IF EXISTS tnp_a.tnp_comp;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "tnp_comp" is not a view
-- end-expected-error
DROP VIEW tnp_a.tnp_comp;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "tnp_comp" is not an index
-- end-expected-error
DROP INDEX tnp_a.tnp_comp;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "tnp_a.tnp_comp" is not a domain
-- end-expected-error
DROP DOMAIN tnp_a.tnp_comp;

-- ---------------------------------------------------------------------------
-- 5. A table's row type takes the name for types
-- ---------------------------------------------------------------------------

CREATE TABLE tnp_a.tnp_tab (a int);

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "tnp_tab" already exists
-- end-expected-error
CREATE TYPE tnp_a.tnp_tab AS (a int);

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "tnp_tab" already exists
-- end-expected-error
CREATE TYPE tnp_a.tnp_tab AS ENUM ('x');

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "tnp_tab" already exists
-- end-expected-error
CREATE DOMAIN tnp_a.tnp_tab AS int;

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "tnp_tab" already exists
-- end-expected-error
CREATE TYPE tnp_a.tnp_tab AS RANGE (subtype = int4);

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "tnp_tab" already exists
-- end-expected-error
CREATE TYPE tnp_a.tnp_tab;

-- ... and DROP TYPE may not take that row type away on its own.
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type tnp_a.tnp_tab because table tnp_a.tnp_tab requires it
-- end-expected-error
DROP TYPE tnp_a.tnp_tab;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type tnp_a.tnp_tab because table tnp_a.tnp_tab requires it
-- end-expected-error
DROP TYPE IF EXISTS tnp_a.tnp_tab;

-- A view has a row type too.
CREATE VIEW tnp_a.tnp_vw AS SELECT 1 AS a;

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "tnp_vw" already exists
-- end-expected-error
CREATE TYPE tnp_a.tnp_vw AS ENUM ('x');

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type tnp_a.tnp_vw because view tnp_a.tnp_vw requires it
-- end-expected-error
DROP TYPE tnp_a.tnp_vw;

DROP VIEW tnp_a.tnp_vw;

-- ---------------------------------------------------------------------------
-- 6. A sequence and an index own no row type, so a type may share their name
-- ---------------------------------------------------------------------------

CREATE SEQUENCE tnp_a.tnp_seq;
CREATE TYPE tnp_a.tnp_seq AS ENUM ('x');

-- begin-expected
-- columns: nspname,typtype
-- row: tnp_a|e
-- end-expected
SELECT n.nspname, t.typtype::text AS typtype FROM pg_type t
  JOIN pg_namespace n ON n.oid = t.typnamespace WHERE t.typname = 'tnp_seq';

DROP TYPE tnp_a.tnp_seq;

CREATE INDEX tnp_idx ON tnp_a.tnp_base (a);
CREATE DOMAIN tnp_a.tnp_idx AS int;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
  WHERE t.typname = 'tnp_idx' AND n.nspname = 'tnp_a';

DROP DOMAIN tnp_a.tnp_idx;

-- A composite type does own a relation, so the sequence's name is not free for one.
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "tnp_seq" already exists
-- end-expected-error
CREATE TYPE tnp_a.tnp_seq AS (a int);

DROP SEQUENCE tnp_a.tnp_seq;

-- ---------------------------------------------------------------------------
-- 6b. A label belongs to an enum and to nothing else
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42809
-- message-like: tnp_a.tnp_comp is not an enum
-- end-expected-error
ALTER TYPE tnp_a.tnp_comp ADD VALUE 'q';

-- begin-expected-error
-- sqlstate: 42809
-- message-like: tnp_a.tnp_comp is not an enum
-- end-expected-error
ALTER TYPE tnp_a.tnp_comp RENAME VALUE 'q' TO 'r';

-- ---------------------------------------------------------------------------
-- 7. A qualifier names one schema and reaches no other
-- ---------------------------------------------------------------------------

CREATE TYPE tnp_a.tnp_moved AS ENUM ('x');

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "tnp_b.tnp_moved" does not exist
-- end-expected-error
DROP TYPE tnp_b.tnp_moved;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "tnp_b.tnp_moved" does not exist
-- end-expected-error
ALTER TYPE tnp_b.tnp_moved RENAME TO tnp_moved2;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "tnp_b.tnp_moved" does not exist
-- end-expected-error
ALTER TYPE tnp_b.tnp_moved ADD VALUE 'q';

-- The type the statement did not name is untouched.
-- begin-expected
-- columns: nspname
-- row: tnp_a
-- end-expected
SELECT n.nspname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
  WHERE t.typname = 'tnp_moved';

-- An unqualified DROP TYPE reaches only what the search path exposes.
SET search_path = tnp_b;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "tnp_moved" does not exist
-- end-expected-error
DROP TYPE tnp_moved;

DROP TYPE IF EXISTS tnp_moved;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_type WHERE typname = 'tnp_moved';

-- With the owning schema on the path it is reached.
SET search_path = tnp_b, tnp_a;
DROP TYPE tnp_moved;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_type WHERE typname = 'tnp_moved';

SET search_path = public;

-- ---------------------------------------------------------------------------
-- 8. A rename keeps the type where it was; SET SCHEMA moves it
-- ---------------------------------------------------------------------------

CREATE TYPE tnp_a.tnp_ren AS ENUM ('x');
ALTER TYPE tnp_a.tnp_ren RENAME TO tnp_ren2;

-- begin-expected
-- columns: nspname
-- row: tnp_a
-- end-expected
SELECT n.nspname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
  WHERE t.typname = 'tnp_ren2';

ALTER TYPE tnp_a.tnp_ren2 SET SCHEMA tnp_b;

-- begin-expected
-- columns: nspname
-- row: tnp_b
-- end-expected
SELECT n.nspname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
  WHERE t.typname = 'tnp_ren2';

DROP TYPE tnp_b.tnp_ren2;

CREATE TYPE tnp_a.tnp_cren AS (a int);
ALTER TYPE tnp_a.tnp_cren RENAME TO tnp_cren2;

-- The composite's relation moves with it.
-- begin-expected
-- columns: nspname,relkind
-- row: tnp_a|c
-- end-expected
SELECT n.nspname, c.relkind::text AS relkind FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'tnp_cren2';

ALTER TYPE tnp_a.tnp_cren2 SET SCHEMA tnp_b;

-- begin-expected
-- columns: nspname
-- row: tnp_b
-- end-expected
SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relname = 'tnp_cren2';

DROP TYPE tnp_b.tnp_cren2;

-- ---------------------------------------------------------------------------
-- 9. DROP TYPE names a domain too, and a column declared as one blocks it
-- ---------------------------------------------------------------------------

CREATE DOMAIN tnp_a.tnp_dom AS int CHECK (VALUE > 0);
CREATE TABLE tnp_a.tnp_uses (d tnp_a.tnp_dom);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type tnp_a.tnp_dom because other objects depend on it
-- end-expected-error
DROP DOMAIN tnp_a.tnp_dom;

DROP TABLE tnp_a.tnp_uses;
DROP TYPE tnp_a.tnp_dom;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_type WHERE typname = 'tnp_dom';

-- ---------------------------------------------------------------------------
-- 10. A composite type used as a column type, by its qualified name
-- ---------------------------------------------------------------------------

CREATE TABLE tnp_a.tnp_holder (v tnp_a.tnp_comp);
INSERT INTO tnp_a.tnp_holder VALUES (ROW(4, 'four'));

-- begin-expected
-- columns: a,b
-- row: 4|four
-- end-expected
SELECT (v).a AS a, (v).b AS b FROM tnp_a.tnp_holder;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type tnp_a.tnp_comp because other objects depend on it
-- end-expected-error
DROP TYPE tnp_a.tnp_comp;

DROP TABLE tnp_a.tnp_holder;

-- ---------------------------------------------------------------------------
-- 11. Dropping the schema takes its types with it
-- ---------------------------------------------------------------------------

DROP SCHEMA tnp_a CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_type
  WHERE typname IN ('tnp_comp', 'tnp_sp_comp', 'tnp_sp_enum', 'tnp_sp_dom', 'tnp_sp_range');

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'tnp_comp';

-- The name is free again in a schema of the same name.
CREATE SCHEMA tnp_a;
CREATE TYPE tnp_a.tnp_comp AS (a int);

-- begin-expected
-- columns: nspname
-- row: tnp_a
-- end-expected
SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relname = 'tnp_comp';

DROP SCHEMA tnp_a CASCADE;
DROP SCHEMA tnp_b CASCADE;

-- ---------------------------------------------------------------------------
-- 12. Unqualified names in public still behave
-- ---------------------------------------------------------------------------

CREATE TYPE tnp_pub AS ENUM ('a', 'b');

-- begin-expected
-- columns: nspname
-- row: public
-- end-expected
SELECT n.nspname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
  WHERE t.typname = 'tnp_pub';

CREATE TABLE tnp_pub_t (c tnp_pub);
INSERT INTO tnp_pub_t VALUES ('a');

-- begin-expected
-- columns: c
-- row: a
-- end-expected
SELECT c::text AS c FROM tnp_pub_t;

DROP TABLE tnp_pub_t;
DROP TYPE tnp_pub;

-- ---------------------------------------------------------------------------
-- 13. A relation's row type has to find the type name free
-- ---------------------------------------------------------------------------

CREATE SCHEMA tnp_a;
CREATE SCHEMA tnp_b;

CREATE TYPE tnp_a.tnp_enum AS ENUM ('x');
CREATE DOMAIN tnp_a.tnp_dom AS int;
CREATE TYPE tnp_a.tnp_rng AS RANGE (subtype = int4);
CREATE TYPE tnp_a.tnp_shell;

-- A table carries a row type of its own name, so a name an enum, a domain or a range already
-- answers to is taken for it, even though no relation holds it.
-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "tnp_enum" already exists
-- end-expected-error
CREATE TABLE tnp_a.tnp_enum (i int);

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "tnp_dom" already exists
-- end-expected-error
CREATE TABLE tnp_a.tnp_dom (i int);

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "tnp_rng" already exists
-- end-expected-error
CREATE TABLE tnp_a.tnp_rng (i int);

-- The name is free in another schema: a type takes a name in its own schema only.
CREATE TABLE tnp_b.tnp_enum (i int);

-- begin-expected
-- columns: nspname
-- row: tnp_b
-- end-expected
SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relname = 'tnp_enum' AND c.relkind = 'r';

-- An index carries no row type, so it may share a name with an enum.
CREATE TABLE tnp_a.tnp_base (i int);
CREATE INDEX tnp_enum ON tnp_a.tnp_base (i);

-- A shell type is a reservation the new relation's row type fills in, not a collision.
CREATE TABLE tnp_a.tnp_shell (i int);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'tnp_a' AND c.relname = 'tnp_shell' AND c.relkind = 'r';

DROP SCHEMA tnp_a CASCADE;
DROP SCHEMA tnp_b CASCADE;

-- ---------------------------------------------------------------------------
-- 14. Renaming a relation onto a type name
-- ---------------------------------------------------------------------------

CREATE SCHEMA tnp_a;
CREATE TYPE tnp_a.tnp_enum AS ENUM ('x');
CREATE DOMAIN tnp_a.tnp_dom AS int;
CREATE TYPE tnp_a.tnp_shell;
CREATE TYPE tnp_a.tnp_comp AS (a int);
CREATE TABLE tnp_a.tnp_other (i int);
CREATE TABLE tnp_a.tnp_t (i int);

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "tnp_enum" already exists
-- end-expected-error
ALTER TABLE tnp_a.tnp_t RENAME TO tnp_enum;

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "tnp_dom" already exists
-- end-expected-error
ALTER TABLE tnp_a.tnp_t RENAME TO tnp_dom;

-- A rename has no new type to write into a reserved name, so a shell blocks it here although
-- it did not block the CREATE above.
-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "tnp_shell" already exists
-- end-expected-error
ALTER TABLE tnp_a.tnp_t RENAME TO tnp_shell;

-- A composite and another table both own a relation, so the relation check reports first.
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "tnp_comp" already exists
-- end-expected-error
ALTER TABLE tnp_a.tnp_t RENAME TO tnp_comp;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "tnp_other" already exists
-- end-expected-error
ALTER TABLE tnp_a.tnp_t RENAME TO tnp_other;

-- Nothing was renamed by any of them.
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'tnp_a' AND c.relname = 'tnp_t';

DROP SCHEMA tnp_a CASCADE;

-- ---------------------------------------------------------------------------
-- 15. A composite type is reached by a query, and refused for what it is
-- ---------------------------------------------------------------------------

CREATE SCHEMA tnp_a;
CREATE TYPE tnp_a.tnp_comp AS (a int);
SET search_path = tnp_a, public;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot open relation "tnp_comp"
-- end-expected-error
SELECT * FROM tnp_comp;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot open relation "tnp_comp"
-- end-expected-error
SELECT count(*) FROM tnp_comp;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot open relation "tnp_comp"
-- end-expected-error
INSERT INTO tnp_comp VALUES (1);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot open relation "tnp_comp"
-- end-expected-error
UPDATE tnp_comp SET a = 1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot open relation "tnp_comp"
-- end-expected-error
DELETE FROM tnp_comp;

SET search_path = public;

-- PostgreSQL names the bare relation even when the statement qualified it.
-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot open relation "tnp_comp"
-- end-expected-error
SELECT * FROM tnp_a.tnp_comp;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "tnp_comp" is not a table
-- end-expected-error
TRUNCATE tnp_a.tnp_comp;

-- What a composite is made of is ALTER TYPE's business, and ALTER TABLE says so.
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "tnp_comp" is a composite type
-- end-expected-error
ALTER TABLE tnp_a.tnp_comp ADD COLUMN y int;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "tnp_comp" is a composite type
-- end-expected-error
ALTER TABLE tnp_a.tnp_comp DROP COLUMN a;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "tnp_comp" is a composite type
-- end-expected-error
ALTER TABLE tnp_a.tnp_comp RENAME TO tnp_comp2;

-- The type still has exactly the attribute it was created with.
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM information_schema.attributes
  WHERE udt_schema = 'tnp_a' AND udt_name = 'tnp_comp';

DROP SCHEMA tnp_a CASCADE;

-- ---------------------------------------------------------------------------
-- 16. A message names a type the way a reader could have written it
-- ---------------------------------------------------------------------------

CREATE SCHEMA tnp_a;
CREATE SCHEMA tnp_b;
CREATE TYPE tnp_a.tnp_enum AS ENUM ('x');
CREATE DOMAIN tnp_a.tnp_dom AS int CHECK (VALUE > 0);
CREATE DOMAIN tnp_a.tnp_nn AS int NOT NULL;
SET search_path = public;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input value for enum tnp_a.tnp_enum: "q"
-- end-expected-error
SELECT 'q'::tnp_a.tnp_enum;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain tnp_a.tnp_dom violates check constraint "tnp_dom_check"
-- end-expected-error
SELECT (-1)::tnp_a.tnp_dom;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: domain tnp_a.tnp_nn does not allow null values
-- end-expected-error
SELECT NULL::tnp_a.tnp_nn;

CREATE TABLE tnp_b.tnp_holder (e tnp_a.tnp_enum, d tnp_a.tnp_dom);

-- A value written into a column of that type is blamed the same way.
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input value for enum tnp_a.tnp_enum: "q"
-- end-expected-error
INSERT INTO tnp_b.tnp_holder VALUES ('q', 1);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain tnp_a.tnp_dom violates check constraint "tnp_dom_check"
-- end-expected-error
INSERT INTO tnp_b.tnp_holder VALUES ('x', -1);

-- Put the schema on the path and the qualifier goes away, however the cast was spelled.
SET search_path = tnp_a, public;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input value for enum tnp_enum: "q"
-- end-expected-error
SELECT 'q'::tnp_enum;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input value for enum tnp_enum: "q"
-- end-expected-error
SELECT 'q'::tnp_a.tnp_enum;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain tnp_dom violates check constraint "tnp_dom_check"
-- end-expected-error
SELECT (-1)::tnp_a.tnp_dom;

SET search_path = public;

-- ---------------------------------------------------------------------------
-- 17. udt_schema names the type's schema, not the table's
-- ---------------------------------------------------------------------------

CREATE TYPE tnp_a.tnp_comp AS (a int);
CREATE TABLE tnp_b.tnp_cols (e tnp_a.tnp_enum, c tnp_a.tnp_comp, i int);

-- begin-expected
-- columns: column_name|udt_schema|udt_name
-- row: c|tnp_a|tnp_comp
-- row: e|tnp_a|tnp_enum
-- row: i|pg_catalog|int4
-- end-expected
SELECT column_name, udt_schema, udt_name FROM information_schema.columns
  WHERE table_schema = 'tnp_b' AND table_name = 'tnp_cols' ORDER BY column_name;

-- begin-expected
-- columns: column_name|udt_schema|udt_name
-- row: c|tnp_a|tnp_comp
-- row: e|tnp_a|tnp_enum
-- row: i|pg_catalog|int4
-- end-expected
SELECT column_name, udt_schema, udt_name FROM information_schema.column_udt_usage
  WHERE table_schema = 'tnp_b' AND table_name = 'tnp_cols' ORDER BY column_name;

-- The other way round: a table in tnp_a with a column of a type in public.
CREATE TYPE public.tnp_pubenum AS ENUM ('y');
CREATE TABLE tnp_a.tnp_cols2 (e tnp_pubenum);

-- begin-expected
-- columns: udt_schema|udt_name
-- row: public|tnp_pubenum
-- end-expected
SELECT udt_schema, udt_name FROM information_schema.columns
  WHERE table_schema = 'tnp_a' AND table_name = 'tnp_cols2' AND column_name = 'e';

-- An attribute's type has a schema of its own, which need not be the composite's.
CREATE TYPE tnp_b.tnp_comp2 AS (a tnp_a.tnp_enum, b int);

-- begin-expected
-- columns: attribute_name|attribute_udt_schema|attribute_udt_name
-- row: a|tnp_a|tnp_enum
-- row: b|pg_catalog|int4
-- end-expected
SELECT attribute_name, attribute_udt_schema, attribute_udt_name
  FROM information_schema.attributes
  WHERE udt_schema = 'tnp_b' AND udt_name = 'tnp_comp2' ORDER BY attribute_name;

DROP TABLE tnp_a.tnp_cols2;
DROP TYPE public.tnp_pubenum;
DROP SCHEMA tnp_b CASCADE;
DROP SCHEMA tnp_a CASCADE;

-- ---------------------------------------------------------------------------
-- 18. A quoted type name with a dot in it is one name
-- ---------------------------------------------------------------------------

CREATE DOMAIN "tnp_a.dotted" AS int;

-- begin-expected
-- columns: typname|nspname
-- row: tnp_a.dotted|public
-- end-expected
SELECT t.typname, n.nspname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
  WHERE t.typname = 'tnp_a.dotted';

-- Reading its own name as a qualifier made the type undroppable by any statement.
DROP DOMAIN "tnp_a.dotted";

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_type WHERE typname = 'tnp_a.dotted';
