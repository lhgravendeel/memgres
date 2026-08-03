-- ============================================================================
-- Qualified names, and which query level a name belongs to
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Two lookups that read a written name in two steps.
--
-- A qualified name says where to look as well as what to look for, and both
-- halves are read: a schema that is not there is 3F000, a schema that is there
-- and does not hold the object is the object's own complaint. IF EXISTS says
-- not to mind either.
--
-- A bare relation name in a sub-select is read against the levels it is written
-- inside. The relations of one FROM clause are computed side by side, so a
-- sub-select cannot read its neighbour without LATERAL -- but it can always
-- read the levels the whole query is nested inside, and the names its own FROM
-- brings in are its own however they were brought in.
-- ============================================================================

DROP TABLE IF EXISTS qs_u CASCADE;
DROP TABLE IF EXISTS qs_t CASCADE;
DROP DOMAIN IF EXISTS qs_dom CASCADE;
CREATE TABLE qs_t (id int PRIMARY KEY, v int);
CREATE TABLE qs_u (id int PRIMARY KEY, v int);
INSERT INTO qs_t VALUES (1,1),(2,2);
INSERT INTO qs_u VALUES (1,1),(2,2);
CREATE DOMAIN qs_dom AS int;

-- ============================================================================
-- A. IF EXISTS over a schema that is not there
-- ============================================================================
-- A relation in a schema that does not exist is a relation that does not
-- exist, so IF EXISTS skips it rather than reporting the schema.

ALTER TABLE IF EXISTS qs_noschema.t ADD COLUMN j int;
ALTER TABLE IF EXISTS qs_noschema.t DROP COLUMN j;
ALTER TABLE IF EXISTS qs_noschema.t RENAME COLUMN i TO j;
ALTER TABLE IF EXISTS qs_noschema.t RENAME TO qs_x;
ALTER TABLE IF EXISTS qs_noschema.t ALTER COLUMN i SET DEFAULT 1;
ALTER TABLE IF EXISTS qs_noschema.t ALTER COLUMN i TYPE bigint;
DROP TABLE IF EXISTS qs_noschema.t;
DROP INDEX IF EXISTS qs_noschema.ix;

-- without IF EXISTS the schema is what is reported
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "qs_noschema" does not exist
-- end-expected-error
ALTER TABLE qs_noschema.t ADD COLUMN j int;

-- ============================================================================
-- B. A statement that opens a relation by a written qualifier
-- ============================================================================
-- COMMENT and GRANT resolve the schema before they look for the relation, so
-- a schema that is not there is what they report.

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "qs_noschema" does not exist
-- end-expected-error
COMMENT ON TABLE qs_noschema.t IS 'x';

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "qs_noschema" does not exist
-- end-expected-error
COMMENT ON COLUMN qs_noschema.t.c IS 'x';

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "qs_noschema" does not exist
-- end-expected-error
GRANT SELECT ON qs_noschema.t TO PUBLIC;

-- a schema that is there and holds no such relation is the relation's own
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
COMMENT ON TABLE public.qs_nosuchtable IS 'x';

-- ============================================================================
-- C. A type name written under a schema
-- ============================================================================
-- The built-in types are pg_catalog's and nothing else's. A schema that is
-- there and does not hold the type is 42704, not 3F000.

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT 1::pg_catalog.int4 AS c;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "public.int4" does not exist
-- end-expected-error
SELECT 1::public.int4;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_toast.int4" does not exist
-- end-expected-error
SELECT 1::pg_toast.int4;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "information_schema.int4" does not exist
-- end-expected-error
SELECT 1::information_schema.int4;

-- the SQL spellings the grammar rewrites are not pg_catalog entries
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_catalog.integer" does not exist
-- end-expected-error
SELECT 1::pg_catalog.integer;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_catalog.serial" does not exist
-- end-expected-error
SELECT 1::pg_catalog.serial;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_catalog.bigint" does not exist
-- end-expected-error
SELECT 1::pg_catalog.bigint;

-- a schema that is not there outranks the type, wherever the type is written
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "qs_noschema" does not exist
-- end-expected-error
SELECT 1::qs_noschema.int4;

-- a type this database was told about answers under the schema it was made in
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT 1::public.qs_dom AS c;

-- but not under pg_catalog, which holds no such thing
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_catalog.qs_dom" does not exist
-- end-expected-error
SELECT 1::pg_catalog.qs_dom;

-- a relation mints a type of its own name in its own schema
-- begin-expected
-- columns: c
-- row: NULL
-- end-expected
SELECT NULL::public.qs_t AS c;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_catalog.qs_t" does not exist
-- end-expected-error
SELECT NULL::pg_catalog.qs_t;

-- the same reading in a column definition and in the function form of a cast
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_toast.int4" does not exist
-- end-expected-error
CREATE TABLE qs_q1 (i pg_toast.int4);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_toast.int4" does not exist
-- end-expected-error
SELECT CAST(1 AS pg_toast.int4);

-- ============================================================================
-- D. A sub-select beside a relation
-- ============================================================================
-- It cannot read its neighbour, however the neighbour was written.

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "qs_t"
-- end-expected-error
SELECT * FROM qs_t, (SELECT qs_t.id) s;

-- an alias hides the relation's name for resolution, and the entry is still
-- the one PostgreSQL finds when it says what went wrong
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "qs_t"
-- end-expected-error
SELECT * FROM qs_t x, (SELECT qs_t.id) s;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "a"
-- end-expected-error
SELECT * FROM qs_t a, (SELECT a.v) b;

-- read from a level below the sub-select, and still its neighbour
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "a"
-- end-expected-error
SELECT * FROM qs_t a, (SELECT (SELECT a.v)) s;

-- a relation not entered yet is simply missing
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "qs_t"
-- end-expected-error
SELECT * FROM (SELECT qs_t.id) s, qs_t;

-- LATERAL is the word that brings the neighbour into reach
-- begin-expected
-- columns: id,v,v
-- row: 1|1|1
-- row: 2|2|2
-- end-expected
SELECT a.id, a.v, b.v FROM qs_t a, LATERAL (SELECT a.v) b ORDER BY 1;

-- ============================================================================
-- E. The names a sub-select brings in for itself
-- ============================================================================
-- However they were brought in: through a join, through a WITH item, or by a
-- level below it. None of them reaches the neighbour.

-- begin-expected
-- columns: n
-- row: 8
-- end-expected
SELECT count(*) AS n
  FROM qs_t x, (SELECT qs_t.id FROM qs_t JOIN qs_u ON true) s;

-- begin-expected
-- columns: n
-- row: 8
-- end-expected
SELECT count(*) AS n
  FROM qs_t JOIN (SELECT qs_t.id FROM qs_t JOIN qs_u ON true) s ON true;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n
  FROM qs_t x, (WITH qs_t AS (SELECT 1 AS id) SELECT qs_t.id FROM qs_t) s;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n
  FROM qs_t x, (SELECT (SELECT qs_t.id FROM qs_t LIMIT 1)) s;

-- begin-expected
-- columns: n
-- row: 6
-- end-expected
SELECT count(*) AS n
  FROM qs_t x, (SELECT qs_t.id FROM qs_t UNION SELECT 9) s;

-- ============================================================================
-- F. A level above the whole query
-- ============================================================================
-- An ordinary correlated reference, which needs no LATERAL. When a neighbour
-- of the same name is there too, the outer one is what the name means, because
-- the neighbour is not in scope at all.

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n
  FROM qs_t WHERE EXISTS (SELECT 1 FROM qs_t z, (SELECT qs_t.v) q);

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n
  FROM qs_t WHERE EXISTS (SELECT 1 FROM qs_t, (SELECT qs_t.v) q);

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT (SELECT count(*) FROM qs_t z, (SELECT qs_t.v) q) AS n FROM qs_t LIMIT 1;

-- ============================================================================
-- G. A variadic signature
-- ============================================================================
-- It records its last parameter as the array PostgreSQL collects the arguments
-- into, and a call writes the elements.

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT jsonb_extract_path('{"a":1}'::jsonb, 'a'::text) AS c;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT jsonb_extract_path_text('{"a":1}'::jsonb, 'a'::text) AS c;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT jsonb_extract_path('{"a":{"b":1}}'::jsonb, 'a'::text, 'b'::text) AS c;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT json_extract_path_text('{"a":1}'::json, 'a'::text) AS c;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT jsonb_extract_path('{"a":1}'::jsonb, 'a'::varchar) AS c;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT jsonb_extract_path('{"a":1}'::jsonb, 'a'::name) AS c;

-- a call whose count matches no signature of the name still resolves to nothing
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function multirange(int4range, int4range) does not exist
-- end-expected-error
SELECT multirange(int4range(1,5), int4range(10,15));

-- ============================================================================
-- H. A catalog column the two servers spell differently
-- ============================================================================
-- A stored expression is pg_node_tree where PostgreSQL declares it, and the
-- functions that read one are declared over that type.

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n
  FROM pg_catalog.pg_publication_rel pr
 WHERE pg_catalog.pg_get_expr(pr.prqual, pr.prrelid) IS NOT NULL;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n
  FROM pg_catalog.pg_attrdef ad
 WHERE pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) IS NULL;

-- ============================================================================
-- I. A name the schema written does not hold at all
-- ============================================================================
-- Reported as it was written, which is what distinguishes it from the same
-- complaint made about a bare name.

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_catalog.qs_nosuchtype" does not exist
-- end-expected-error
SELECT 1::pg_catalog.qs_nosuchtype;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "public.qs_nosuchtype" does not exist
-- end-expected-error
SELECT 1::public.qs_nosuchtype;

-- unqualified, there is no schema to name
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "qs_nosuchtype" does not exist
-- end-expected-error
SELECT 1::qs_nosuchtype;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_catalog.qs_nosuchtype" does not exist
-- end-expected-error
CREATE TABLE qs_q2 (i pg_catalog.qs_nosuchtype);

-- a relation's type belongs to the schema the relation is in
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "public.pg_class" does not exist
-- end-expected-error
SELECT NULL::public.pg_class;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "public.pg_tables" does not exist
-- end-expected-error
SELECT NULL::public.pg_tables;

-- ============================================================================
-- J. The domains information_schema is written in terms of
-- ============================================================================
-- The standard describes its own catalog in five named domains, and they answer
-- under that schema and nowhere else.

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT 1::information_schema.cardinal_number AS c;

-- begin-expected
-- columns: c
-- row: x
-- end-expected
SELECT 'x'::information_schema.character_data AS c;

-- begin-expected
-- columns: c
-- row: x
-- end-expected
SELECT 'x'::information_schema.sql_identifier AS c;

-- begin-expected
-- columns: c
-- row: YES
-- end-expected
SELECT 'YES'::information_schema.yes_or_no AS c;

-- begin-expected
-- columns: c
-- row: t
-- end-expected
SELECT now()::information_schema.time_stamp IS NOT NULL AS c;

-- named as the qualified type it is
-- begin-expected
-- columns: c
-- row: information_schema.cardinal_number
-- end-expected
SELECT pg_typeof(1::information_schema.cardinal_number)::text AS c;

-- the value is the type underneath, and is read as one
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
SELECT 'x'::information_schema.cardinal_number;

-- and then judged by the domain's own constraint
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain information_schema.yes_or_no violates check constraint "yes_or_no_check"
-- end-expected-error
SELECT 'MAYBE'::information_schema.yes_or_no;

-- they answer to nothing else
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cardinal_number" does not exist
-- end-expected-error
SELECT 1::cardinal_number;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "public.cardinal_number" does not exist
-- end-expected-error
SELECT 1::public.cardinal_number;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_catalog.cardinal_number" does not exist
-- end-expected-error
SELECT 1::pg_catalog.cardinal_number;

-- ============================================================================
-- K. A multi-word type name written under a schema
-- ============================================================================
-- The grammar reads a multi-word spelling only where no schema was written:
-- after a qualifier it takes a single name, and the next word is unexpected.

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "varying"
-- end-expected-error
SELECT NULL::pg_catalog.character varying;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "precision"
-- end-expected-error
SELECT NULL::pg_catalog.double precision;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "with"
-- end-expected-error
SELECT NULL::pg_catalog.timestamp with time zone;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "without"
-- end-expected-error
SELECT NULL::pg_catalog.time without time zone;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "varying"
-- end-expected-error
CREATE TABLE qs_q3 (i pg_catalog.character varying);

-- half of a multi-word spelling is no type of its own
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_catalog.character" does not exist
-- end-expected-error
SELECT NULL::pg_catalog.character;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_catalog.varying" does not exist
-- end-expected-error
SELECT NULL::pg_catalog.varying;

-- a precision and an array suffix are carried however the name was written
-- begin-expected
-- columns: c
-- row: ab
-- end-expected
SELECT 'abc'::pg_catalog.varchar(2) AS c;

-- begin-expected
-- columns: c
-- row: 1.20
-- end-expected
SELECT 1.2::pg_catalog.numeric(10,2) AS c;

-- ============================================================================
-- L. Which words may stand as a label, and which as a relation's alias
-- ============================================================================
-- A label written without AS may not be a word the grammar is still expecting
-- to continue the expression before it. A relation's alias is a plain name, and
-- takes any word the grammar does not keep for itself.

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "varying"
-- end-expected-error
SELECT 1 varying;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "day"
-- end-expected-error
SELECT 1 day;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "character"
-- end-expected-error
SELECT 1 character;

-- written with AS, every one of them is a label
-- begin-expected
-- columns: varying
-- row: 1
-- end-expected
SELECT 1 AS varying;

-- begin-expected
-- columns: day
-- row: 1
-- end-expected
SELECT 1 AS day;

-- and a word the grammar does not keep is a label without AS
-- begin-expected
-- columns: value
-- row: 1
-- end-expected
SELECT 1 value;

-- begin-expected
-- columns: name
-- row: 1
-- end-expected
SELECT 1 name;

-- a relation takes the wider set
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT varying.id FROM qs_t varying ORDER BY 1;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT "day".id FROM qs_t day ORDER BY 1;

-- but not a word it keeps for itself
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "overlaps"
-- end-expected-error
SELECT * FROM qs_t overlaps;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "isnull"
-- end-expected-error
SELECT * FROM qs_t isnull;

-- FOR opens a locking clause and nothing else
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
SELECT * FROM qs_t for;

DROP TABLE IF EXISTS qs_q2 CASCADE;
DROP TABLE IF EXISTS qs_q3 CASCADE;
-- ============================================================================
-- M. An integer's shift and bitwise operators
-- ============================================================================
-- PostgreSQL spells these the same way for other types: >> also asks whether
-- one network address contains another and whether one range lies wholly to the
-- right of another, and those answer with a boolean. Over two integers it is a
-- shift, and a shift is no condition.

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT * FROM qs_t WHERE v >> 1;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT * FROM qs_t WHERE v << 1;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT * FROM qs_t WHERE v & 1;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT * FROM qs_t WHERE 4 >> 1;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of AND must be type boolean, not type integer
-- end-expected-error
SELECT * FROM qs_t WHERE v >> 1 AND true;

-- a shift keeps the width of the value being shifted
-- begin-expected
-- columns: c
-- row: integer
-- end-expected
SELECT pg_typeof(v >> 1)::text AS c FROM qs_t LIMIT 1;

-- begin-expected
-- columns: c
-- row: integer
-- end-expected
SELECT pg_typeof(4 >> 1)::text AS c;

-- begin-expected
-- columns: c
-- row: integer
-- end-expected
SELECT pg_typeof(v & 1)::text AS c FROM qs_t LIMIT 1;

-- begin-expected
-- columns: c
-- row: bigint
-- end-expected
SELECT pg_typeof(9000000000::bigint >> 1)::text AS c;

-- begin-expected
-- columns: c
-- row: 5
-- end-expected
SELECT (10 >> 1) AS c;

-- over a network address the same word is a containment test
-- begin-expected
-- columns: c
-- row: t
-- end-expected
SELECT ('10.0.0.0/8'::inet >> '10.1.2.3'::inet) AS c;

DROP TABLE IF EXISTS qs_q1 CASCADE;
DROP TABLE IF EXISTS qs_u CASCADE;
DROP TABLE IF EXISTS qs_t CASCADE;
DROP DOMAIN IF EXISTS qs_dom CASCADE;
