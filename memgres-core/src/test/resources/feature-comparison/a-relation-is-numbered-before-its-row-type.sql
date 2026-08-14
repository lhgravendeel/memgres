-- An object is numbered in the order PostgreSQL writes its catalogue rows in. A relation takes
-- its number first, then the array of the type describing its rows, then that row type itself;
-- a type that has no relation -- an enum, a domain, a range -- still takes its number after its
-- own array. A sequence and an index describe no row, so neither carries a row type at all. And
-- the numbering runs forward: everything one statement creates is numbered below everything the
-- next statement creates, so a relation's row type never lands behind a later relation.
-- Nothing below reads an OID as a value. Every answer is one object's OID held against another's,
-- which is all PostgreSQL promises. Every answer was read off PostgreSQL 18.

-- stmt 1: a composite type writes its relation, then its array, then the type
CREATE TYPE zzt4e_comp AS (x int, y int);

-- begin-expected
-- columns: rel_before_array | array_before_type | rel_before_type
-- row: t | t | t
-- end-expected
SELECT c.oid < a.oid AS rel_before_array, a.oid < t.oid AS array_before_type, c.oid < t.oid AS rel_before_type FROM pg_type t JOIN pg_class c ON c.oid = t.typrelid JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'zzt4e_comp';

-- stmt 2: a table's row type is numbered the same way, and after its array
CREATE TABLE zzt4e_plain (i int);

-- begin-expected
-- columns: rel_before_array | array_before_type | rel_before_type
-- row: t | t | t
-- end-expected
SELECT c.oid < a.oid AS rel_before_array, a.oid < t.oid AS array_before_type, c.oid < t.oid AS rel_before_type FROM pg_type t JOIN pg_class c ON c.oid = t.typrelid JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'zzt4e_plain';

-- stmt 3: an enum and a domain are each numbered after their own array
CREATE TYPE zzt4e_enum AS ENUM ('a', 'b');
CREATE DOMAIN zzt4e_dom AS int;

-- begin-expected
-- columns: array_before_type
-- row: t
-- end-expected
SELECT a.oid < t.oid AS array_before_type FROM pg_type t JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'zzt4e_enum';

-- begin-expected
-- columns: array_before_type
-- row: t
-- end-expected
SELECT a.oid < t.oid AS array_before_type FROM pg_type t JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'zzt4e_dom';

-- stmt 4: a range writes four rows, and the range type itself is the last of them
CREATE TYPE zzt4e_rng AS RANGE (subtype = int4);

-- begin-expected
-- columns: rangearray_first | multirange_second | multirangearray_third
-- row: t | t | t
-- end-expected
SELECT (SELECT oid FROM pg_type WHERE typname = '_zzt4e_rng') < (SELECT oid FROM pg_type WHERE typname = 'zzt4e_rng_multirange') AS rangearray_first, (SELECT oid FROM pg_type WHERE typname = 'zzt4e_rng_multirange') < (SELECT oid FROM pg_type WHERE typname = '_zzt4e_rng_multirange') AS multirange_second, (SELECT oid FROM pg_type WHERE typname = '_zzt4e_rng_multirange') < (SELECT oid FROM pg_type WHERE typname = 'zzt4e_rng') AS multirangearray_third;

-- stmt 5: a view, a materialized view and a table written by a query all carry a row type of
-- their own, numbered after their relation and after its array
CREATE VIEW zzt4e_v AS SELECT 1 AS a;
CREATE MATERIALIZED VIEW zzt4e_mv AS SELECT 1 AS a;
CREATE TABLE zzt4e_ctas AS SELECT 1 AS a;

-- begin-expected
-- columns: rel_before_array | array_before_type
-- row: t | t
-- end-expected
SELECT c.oid < a.oid AS rel_before_array, a.oid < t.oid AS array_before_type FROM pg_type t JOIN pg_class c ON c.oid = t.typrelid JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'zzt4e_v';

-- begin-expected
-- columns: rel_before_array | array_before_type
-- row: t | t
-- end-expected
SELECT c.oid < a.oid AS rel_before_array, a.oid < t.oid AS array_before_type FROM pg_type t JOIN pg_class c ON c.oid = t.typrelid JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'zzt4e_mv';

-- begin-expected
-- columns: rel_before_array | array_before_type
-- row: t | t
-- end-expected
SELECT c.oid < a.oid AS rel_before_array, a.oid < t.oid AS array_before_type FROM pg_type t JOIN pg_class c ON c.oid = t.typrelid JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'zzt4e_ctas';

-- stmt 6: a partitioned table is numbered like any other relation, and its partition after it
CREATE TABLE zzt4e_part (i int, j int) PARTITION BY RANGE (i);
CREATE TABLE zzt4e_p1 PARTITION OF zzt4e_part FOR VALUES FROM (0) TO (10);

-- begin-expected
-- columns: rel_before_array | array_before_type
-- row: t | t
-- end-expected
SELECT c.oid < a.oid AS rel_before_array, a.oid < t.oid AS array_before_type FROM pg_type t JOIN pg_class c ON c.oid = t.typrelid JOIN pg_type a ON a.oid = t.typarray WHERE t.typname = 'zzt4e_part';

-- begin-expected
-- columns: parent_rowtype_before_partition
-- row: t
-- end-expected
SELECT (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_part') < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_p1') AS parent_rowtype_before_partition;

-- stmt 7: a sequence and an index describe no row, so neither has a row type
CREATE SEQUENCE zzt4e_seq;
CREATE INDEX zzt4e_ix_plain ON zzt4e_plain (i);

-- begin-expected
-- columns: relname | relkind | reltype
-- row: zzt4e_ix_plain | i | 0
-- row: zzt4e_seq | S | 0
-- end-expected
SELECT relname, relkind, reltype FROM pg_class WHERE relname IN ('zzt4e_seq', 'zzt4e_ix_plain') ORDER BY relname;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_type WHERE typname IN ('zzt4e_seq', 'zzt4e_ix_plain');

-- stmt 8: the numbering runs forward, so a row type is never numbered after a later relation
CREATE TABLE zzt4e_a (i int);
CREATE TABLE zzt4e_b (i int);

-- begin-expected
-- columns: first_relation_first | first_rowtype_before_second_relation
-- row: t | t
-- end-expected
SELECT (SELECT oid FROM pg_class WHERE relname = 'zzt4e_a') < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_b') AS first_relation_first, (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_a') < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_b') AS first_rowtype_before_second_relation;

-- begin-expected
-- columns: rowtype_before_later_index
-- row: t
-- end-expected
SELECT (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_plain') < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ix_plain') AS rowtype_before_later_index;

-- stmt 9: a change to a table leaves its three numbers alone, and a table created again takes
-- numbers above the ones it had
CREATE TABLE zzt4e_cap (k text, relid bigint, rowtype bigint, arrtype bigint);
INSERT INTO zzt4e_cap SELECT 'a', c.oid, t.oid, a.oid FROM pg_class c JOIN pg_type t ON t.oid = c.reltype JOIN pg_type a ON a.oid = t.typarray WHERE c.relname = 'zzt4e_a';
ALTER TABLE zzt4e_a ADD COLUMN j text;

-- begin-expected
-- columns: relation_oid_kept | rowtype_oid_kept | array_oid_kept
-- row: t | t | t
-- end-expected
SELECT (SELECT relid FROM zzt4e_cap WHERE k = 'a') = (SELECT oid FROM pg_class WHERE relname = 'zzt4e_a') AS relation_oid_kept, (SELECT rowtype FROM zzt4e_cap WHERE k = 'a') = (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_a') AS rowtype_oid_kept, (SELECT arrtype FROM zzt4e_cap WHERE k = 'a') = (SELECT typarray FROM pg_type WHERE typname = 'zzt4e_a') AS array_oid_kept;

DROP TABLE zzt4e_a;
CREATE TABLE zzt4e_a (i int);

-- begin-expected
-- columns: recreated_relation_is_later | recreated_rowtype_is_later
-- row: t | t
-- end-expected
SELECT (SELECT relid FROM zzt4e_cap WHERE k = 'a') < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_a') AS recreated_relation_is_later, (SELECT rowtype FROM zzt4e_cap WHERE k = 'a') < (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_a') AS recreated_rowtype_is_later;

-- cleanup
DROP TABLE zzt4e_cap;
DROP TABLE zzt4e_a;
DROP TABLE zzt4e_b;
DROP TABLE zzt4e_p1;
DROP TABLE zzt4e_part;
DROP TABLE zzt4e_ctas;
DROP MATERIALIZED VIEW zzt4e_mv;
DROP VIEW zzt4e_v;
DROP SEQUENCE zzt4e_seq;
DROP TABLE zzt4e_plain;
DROP TYPE zzt4e_rng;
DROP DOMAIN zzt4e_dom;
DROP TYPE zzt4e_enum;
DROP TYPE zzt4e_comp;
