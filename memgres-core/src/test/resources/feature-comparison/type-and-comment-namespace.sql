-- ============================================================================
-- Feature Comparison: one type namespace per schema, one comment per object
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Enums, composites, ranges, domains and shells share one namespace per schema,
-- and each schema has its own. CREATE TYPE a.e and CREATE TYPE b.e both succeed
-- and pg_type carries two rows called e; a column declared a.e keeps reading
-- a.e's labels however many other schemas later hold an e. A bare name is the
-- search path's to answer, so the same statement text reads a's type under one
-- search path and b's under another.
--
-- A comment belongs to the object it was made on. a.t and b.t are two tables and
-- obj_description answers for each separately. A rename or a SET SCHEMA carries
-- the comment along, because PostgreSQL keys a comment by an OID and a rename
-- does not change one.
--
-- pg_description covers every kind that can carry a comment — table, column,
-- view, materialized view, index, sequence, constraint, function, type, domain
-- and schema — each under its own OID, and obj_description and col_description
-- read that same table.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS tcns_a CASCADE;
DROP SCHEMA IF EXISTS tcns_b CASCADE;
CREATE SCHEMA tcns_a;
CREATE SCHEMA tcns_b;

CREATE TYPE tcns_a.e AS ENUM ('x');
CREATE TYPE tcns_b.e AS ENUM ('p');
CREATE DOMAIN tcns_a.d AS int CHECK (VALUE > 0);
CREATE DOMAIN tcns_b.d AS text;
CREATE TYPE tcns_a.ct AS (x int);
CREATE TYPE tcns_b.ct AS (y text);
CREATE TYPE tcns_a.rg AS RANGE (SUBTYPE = int4);
CREATE TYPE tcns_b.rg AS RANGE (SUBTYPE = date);

CREATE TABLE tcns_a.t (c tcns_a.e);
CREATE TABLE tcns_b.t (c tcns_b.e);
INSERT INTO tcns_a.t VALUES ('x');
INSERT INTO tcns_b.t VALUES ('p');

-- ============================================================================
-- A type belongs to one schema, and each schema has its own
-- ============================================================================

-- stmt 1: both schemas hold an enum called e
-- begin-expected
-- columns: nspname | typname
-- row: tcns_a | e
-- row: tcns_b | e
-- end-expected
SELECT n.nspname::text, t.typname::text FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE t.typname = 'e' AND n.nspname LIKE 'tcns_%' ORDER BY 1;

-- stmt 2: and a domain, a composite and a range of the same name each
-- begin-expected
-- columns: typname | n
-- row: ct | 2
-- row: d | 2
-- row: e | 2
-- row: rg | 2
-- end-expected
SELECT t.typname::text, count(*)::int AS n FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname IN ('tcns_a', 'tcns_b') AND t.typname IN ('e', 'd', 'ct', 'rg')
GROUP BY 1 ORDER BY 1;

-- stmt 3: each enum keeps its own labels
-- begin-expected
-- columns: a | b
-- row: {x} | {p}
-- end-expected
SELECT enum_range(NULL::tcns_a.e)::text AS a, enum_range(NULL::tcns_b.e)::text AS b;

-- stmt 4: each domain keeps its own base type
-- begin-expected
-- columns: domain_schema | data_type
-- row: tcns_a | integer
-- row: tcns_b | text
-- end-expected
SELECT domain_schema::text, data_type::text FROM information_schema.domains
WHERE domain_name = 'd' AND domain_schema LIKE 'tcns_%' ORDER BY 1;

-- stmt 5: each composite keeps its own attribute
-- begin-expected
-- columns: udt_schema | attribute_name
-- row: tcns_a | x
-- row: tcns_b | y
-- end-expected
SELECT udt_schema::text, attribute_name::text FROM information_schema.attributes
WHERE udt_name = 'ct' AND udt_schema LIKE 'tcns_%' ORDER BY 1;

-- stmt 6: a column reads the type it was declared with, not the other schema's
-- begin-expected
-- columns: a | b
-- row: x | p
-- end-expected
SELECT (SELECT c::text FROM tcns_a.t) AS a, (SELECT c::text FROM tcns_b.t) AS b;

-- stmt 7: and refuses a label that type has not got, however many the other has
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input value for enum tcns_a.e: "p"
-- end-expected-error
INSERT INTO tcns_a.t VALUES ('p');

-- stmt 8: the catalogs name the column's own type and its own schema
-- begin-expected
-- columns: table_schema | udt_schema | udt_name
-- row: tcns_a | tcns_a | e
-- row: tcns_b | tcns_b | e
-- end-expected
SELECT table_schema::text, udt_schema::text, udt_name::text
FROM information_schema.columns
WHERE table_name = 't' AND table_schema LIKE 'tcns_%' AND column_name = 'c' ORDER BY 1;

-- stmt 9: a qualified name names only that schema's type
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "tcns_b.nosuch" does not exist
-- end-expected-error
SELECT 'x'::tcns_b.nosuch;

-- stmt 10: a schema that is not there is reported before the type
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "tcns_nosuch" does not exist
-- end-expected-error
CREATE TYPE tcns_nosuch.q AS ENUM ('x');

-- stmt 11: one namespace per schema holds all five kinds, so a domain
-- cannot take the name an enum has in that schema
-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "e" already exists
-- end-expected-error
CREATE DOMAIN tcns_a.e AS int;

-- stmt 12: and a composite cannot either
-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "d" already exists
-- end-expected-error
CREATE TYPE tcns_a.d AS (z int);

-- ============================================================================
-- A bare name is resolved along the search path
-- ============================================================================

SET search_path TO tcns_a;

-- stmt 13: e is a's under this search path
-- begin-expected
-- columns: r
-- row: {x}
-- end-expected
SELECT enum_range(NULL::e)::text AS r;

SET search_path TO tcns_b;

-- stmt 14: and b's under this one
-- begin-expected
-- columns: r
-- row: {p}
-- end-expected
SELECT enum_range(NULL::e)::text AS r;

-- stmt 15: so a label only a's e has is not input for this one
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input value for enum e: "x"
-- end-expected-error
SELECT 'x'::e;

SET search_path TO public;

-- ============================================================================
-- ALTER and DROP reach the type they name
-- ============================================================================

ALTER TYPE tcns_a.e ADD VALUE 'y';

-- stmt 16: the labels went on a's e and nowhere else
-- begin-expected
-- columns: a | b
-- row: {x,y} | {p}
-- end-expected
SELECT enum_range(NULL::tcns_a.e)::text AS a, enum_range(NULL::tcns_b.e)::text AS b;

DROP TYPE tcns_b.rg;

-- stmt 17: dropping b's range leaves a's
-- begin-expected
-- columns: nspname
-- row: tcns_a
-- end-expected
SELECT n.nspname::text FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE t.typname = 'rg' AND n.nspname LIKE 'tcns_%' ORDER BY 1;

-- stmt 18: a type that is not in the schema written is not there to drop
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "tcns_b.rg" does not exist
-- end-expected-error
DROP TYPE tcns_b.rg;

-- ============================================================================
-- A renamed type keeps the columns declared with it
-- ============================================================================

CREATE TYPE tcns_a.rn AS ENUM ('k');
CREATE TABLE tcns_a.rt (c tcns_a.rn);
INSERT INTO tcns_a.rt VALUES ('k');
ALTER TYPE tcns_a.rn RENAME TO rn2;

-- stmt 19: the value is still readable and the catalog names the new name
-- begin-expected
-- columns: v | udt_name
-- row: k | rn2
-- end-expected
SELECT (SELECT c::text FROM tcns_a.rt) AS v,
       (SELECT udt_name::text FROM information_schema.columns
        WHERE table_schema = 'tcns_a' AND table_name = 'rt' AND column_name = 'c') AS udt_name;

-- stmt 20: and the column still refuses a label the type has not got
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input value for enum tcns_a.rn2: "nope"
-- end-expected-error
INSERT INTO tcns_a.rt VALUES ('nope');

CREATE DOMAIN tcns_a.dn AS int CHECK (VALUE > 0);
CREATE TABLE tcns_a.dtab (c tcns_a.dn);
ALTER DOMAIN tcns_a.dn RENAME TO dn2;

-- stmt 21: a renamed domain is still the column's type
-- begin-expected
-- columns: domain_schema | domain_name
-- row: tcns_a | dn2
-- end-expected
SELECT domain_schema::text, domain_name::text FROM information_schema.columns
WHERE table_schema = 'tcns_a' AND table_name = 'dtab' AND column_name = 'c';

-- stmt 22: and still carries its CHECK
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain tcns_a.dn2 violates check constraint
-- end-expected-error
INSERT INTO tcns_a.dtab VALUES (-1);

-- ============================================================================
-- A comment belongs to the object it was made on
-- ============================================================================

COMMENT ON TABLE tcns_a.t IS 'acom';
COMMENT ON TABLE tcns_b.t IS 'bcom';
COMMENT ON COLUMN tcns_a.t.c IS 'acol';
COMMENT ON COLUMN tcns_b.t.c IS 'bcol';

-- stmt 23: two tables of the same name carry their own comments
-- begin-expected
-- columns: a | b
-- row: acom | bcom
-- end-expected
SELECT obj_description('tcns_a.t'::regclass) AS a, obj_description('tcns_b.t'::regclass) AS b;

-- stmt 24: and so do their columns
-- begin-expected
-- columns: a | b
-- row: acol | bcol
-- end-expected
SELECT col_description('tcns_a.t'::regclass, 1) AS a,
       col_description('tcns_b.t'::regclass, 1) AS b;

ALTER TABLE tcns_a.t RENAME TO t2;

-- stmt 25: the rename carries the comment of the object being renamed
-- begin-expected
-- columns: a | acol | b
-- row: acom | acol | bcom
-- end-expected
SELECT obj_description('tcns_a.t2'::regclass) AS a,
       col_description('tcns_a.t2'::regclass, 1) AS acol,
       obj_description('tcns_b.t'::regclass) AS b;

ALTER TABLE tcns_a.t2 SET SCHEMA tcns_b;

-- stmt 26: so does a move to another schema
-- begin-expected
-- columns: a | acol
-- row: acom | acol
-- end-expected
SELECT obj_description('tcns_b.t2'::regclass) AS a,
       col_description('tcns_b.t2'::regclass, 1) AS acol;

ALTER TABLE tcns_b.t2 SET SCHEMA tcns_a;

COMMENT ON TYPE tcns_a.ct IS 'ctcom';
ALTER TYPE tcns_a.ct RENAME TO ct2;

-- stmt 27: a renamed type keeps its comment too
-- begin-expected
-- columns: c
-- row: ctcom
-- end-expected
SELECT obj_description('tcns_a.ct2'::regtype) AS c;

-- ============================================================================
-- pg_description covers every kind that can carry a comment
-- ============================================================================

CREATE TABLE tcns_a.k (id int PRIMARY KEY, v int);
CREATE INDEX tcns_ki ON tcns_a.k (v);
CREATE VIEW tcns_a.kv AS SELECT id FROM tcns_a.k;
CREATE MATERIALIZED VIEW tcns_a.km AS SELECT id FROM tcns_a.k;
CREATE SEQUENCE tcns_a.ks;
ALTER TABLE tcns_a.k ADD CONSTRAINT tcns_kchk CHECK (v > 0);
CREATE FUNCTION tcns_a.kfn(int) RETURNS int AS $$ SELECT $1 $$ LANGUAGE sql;

COMMENT ON TABLE tcns_a.k IS 'k-table';
COMMENT ON COLUMN tcns_a.k.v IS 'k-column';
COMMENT ON INDEX tcns_a.tcns_ki IS 'k-index';
COMMENT ON VIEW tcns_a.kv IS 'k-view';
COMMENT ON MATERIALIZED VIEW tcns_a.km IS 'k-matview';
COMMENT ON SEQUENCE tcns_a.ks IS 'k-sequence';
COMMENT ON CONSTRAINT tcns_kchk ON tcns_a.k IS 'k-constraint';
COMMENT ON FUNCTION tcns_a.kfn(int) IS 'k-function';
COMMENT ON TYPE tcns_a.e IS 'k-type';
COMMENT ON DOMAIN tcns_a.d IS 'k-domain';
COMMENT ON SCHEMA tcns_a IS 'k-schema';

-- stmt 28: every one of the eleven reaches pg_description
-- begin-expected
-- columns: n
-- row: 11
-- end-expected
SELECT count(*)::int AS n FROM pg_description WHERE description LIKE 'k-%';

-- stmt 29: each relation kind answers under its own OID
-- begin-expected
-- columns: t | c | i | v | m | s
-- row: k-table | k-column | k-index | k-view | k-matview | k-sequence
-- end-expected
SELECT obj_description('tcns_a.k'::regclass) AS t,
       col_description('tcns_a.k'::regclass, 2) AS c,
       obj_description('tcns_a.tcns_ki'::regclass) AS i,
       obj_description('tcns_a.kv'::regclass) AS v,
       obj_description('tcns_a.km'::regclass) AS m,
       obj_description('tcns_a.ks'::regclass) AS s;

-- stmt 30: and so does each of the kinds that live outside pg_class
-- begin-expected
-- columns: con | fn | ty | dom | sch
-- row: k-constraint | k-function | k-type | k-domain | k-schema
-- end-expected
SELECT (SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint
        WHERE conname = 'tcns_kchk') AS con,
       (SELECT obj_description(oid, 'pg_proc') FROM pg_proc
        WHERE proname = 'kfn') AS fn,
       obj_description('tcns_a.e'::regtype) AS ty,
       obj_description('tcns_a.d'::regtype) AS dom,
       (SELECT obj_description(oid, 'pg_namespace') FROM pg_namespace
        WHERE nspname = 'tcns_a') AS sch;

-- stmt 31: every row names the catalog its object lives in
-- begin-expected
-- columns: relname | n
-- row: pg_class | 6
-- row: pg_constraint | 1
-- row: pg_namespace | 1
-- row: pg_proc | 1
-- row: pg_type | 2
-- end-expected
SELECT c.relname::text, count(*)::int AS n FROM pg_description d
JOIN pg_class c ON c.oid = d.classoid
WHERE d.description LIKE 'k-%' GROUP BY 1 ORDER BY 1;

-- stmt 32: a column comment is the only one with a non-zero objsubid
-- begin-expected
-- columns: description | objsubid
-- row: k-column | 2
-- end-expected
SELECT d.description::text, d.objsubid::int FROM pg_description d
WHERE d.description LIKE 'k-%' AND d.objsubid <> 0;

-- stmt 33: obj_description answers only for the catalog it is asked about
-- begin-expected
-- columns: a | b
-- row: k-table | NULL
-- end-expected
SELECT obj_description('tcns_a.k'::regclass, 'pg_class') AS a,
       obj_description('tcns_a.k'::regclass, 'pg_proc') AS b;

COMMENT ON COLUMN tcns_a.kv.id IS 'view column';

-- stmt 34: a view carries a column comment like any other relation
-- begin-expected
-- columns: c
-- row: view column
-- end-expected
SELECT col_description('tcns_a.kv'::regclass, 1) AS c;

COMMENT ON TABLE tcns_a.k IS NULL;

-- stmt 35: a comment set to NULL is removed rather than emptied
-- begin-expected
-- columns: c | n
-- row: NULL | 0
-- end-expected
SELECT obj_description('tcns_a.k'::regclass) AS c,
       (SELECT count(*)::int FROM pg_description WHERE description = 'k-table') AS n;

DROP TYPE tcns_a.ct2;

-- stmt 36: dropping the object takes its comment with it
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_description WHERE description = 'ctcom';

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA tcns_a CASCADE;
DROP SCHEMA tcns_b CASCADE;
