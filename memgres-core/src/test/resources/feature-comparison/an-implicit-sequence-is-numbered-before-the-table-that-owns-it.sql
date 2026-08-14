-- A sequence a table declaration asks for is written before the table itself: PostgreSQL numbers
-- the sequence's pg_class row, then the table's, then the array of the table's row type, then the
-- row type. Two serial columns give two sequences, both numbered before the table, in the order
-- the columns were declared. An identity column is numbered exactly as a serial one is, and a
-- table in a schema of its own is numbered the same way, with its row type and that type's array
-- filed in the schema the table lives in.
-- Nothing below reads an OID as a value. Every answer is one object's OID held against another's.
-- Every answer was read off PostgreSQL 18.

-- stmt 1: a serial column's sequence comes before the table, and the row type last of all
CREATE TABLE zzt4e_ser (i serial, j int);

-- begin-expected
-- columns: seq_before_table | table_before_array | array_before_rowtype
-- row: t | t | t
-- end-expected
SELECT (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ser_i_seq') < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ser') AS seq_before_table, (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ser') < (SELECT oid FROM pg_type WHERE typname = '_zzt4e_ser') AS table_before_array, (SELECT oid FROM pg_type WHERE typname = '_zzt4e_ser') < (SELECT oid FROM pg_type WHERE typname = 'zzt4e_ser') AS array_before_rowtype;

-- the sequence itself describes no row
-- begin-expected
-- columns: relkind | reltype
-- row: S | 0
-- end-expected
SELECT relkind, reltype FROM pg_class WHERE relname = 'zzt4e_ser_i_seq';

-- stmt 2: an identity column is numbered the same way
CREATE TABLE zzt4e_ident (i int GENERATED ALWAYS AS IDENTITY, j int);

-- begin-expected
-- columns: seq_before_table | table_before_array | array_before_rowtype
-- row: t | t | t
-- end-expected
SELECT (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ident_i_seq') < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ident') AS seq_before_table, (SELECT oid FROM pg_class WHERE relname = 'zzt4e_ident') < (SELECT oid FROM pg_type WHERE typname = '_zzt4e_ident') AS table_before_array, (SELECT oid FROM pg_type WHERE typname = '_zzt4e_ident') < (SELECT oid FROM pg_type WHERE typname = 'zzt4e_ident') AS array_before_rowtype;

-- stmt 3: two serial columns give two sequences, both before the table and in column order
CREATE TYPE zzt4e_early AS (q int);
CREATE TABLE zzt4e_two (i serial, j serial);

-- begin-expected
-- columns: earlier_type_before_later_sequence | first_sequence_first | both_sequences_before_table
-- row: t | t | t
-- end-expected
SELECT (SELECT oid FROM pg_type WHERE typname = 'zzt4e_early') < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_two_i_seq') AS earlier_type_before_later_sequence, (SELECT oid FROM pg_class WHERE relname = 'zzt4e_two_i_seq') < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_two_j_seq') AS first_sequence_first, (SELECT oid FROM pg_class WHERE relname = 'zzt4e_two_j_seq') < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_two') AS both_sequences_before_table;

-- stmt 4: a table in a schema of its own is numbered the same way, and its row type and array
-- are filed in that schema
CREATE SCHEMA zzt4e_sc;
CREATE TABLE zzt4e_sc.zzt4e_in (i serial);

-- begin-expected
-- columns: seq_before_table | table_before_array | array_before_rowtype
-- row: t | t | t
-- end-expected
SELECT (SELECT oid FROM pg_class WHERE relname = 'zzt4e_in_i_seq') < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_in') AS seq_before_table, (SELECT oid FROM pg_class WHERE relname = 'zzt4e_in') < (SELECT oid FROM pg_type WHERE typname = '_zzt4e_in') AS table_before_array, (SELECT oid FROM pg_type WHERE typname = '_zzt4e_in') < (SELECT oid FROM pg_type WHERE typname = 'zzt4e_in') AS array_before_rowtype;

-- begin-expected
-- columns: rowtype_in_the_schema | array_in_the_schema
-- row: t | t
-- end-expected
SELECT t.typnamespace = n.oid AS rowtype_in_the_schema, a.typnamespace = n.oid AS array_in_the_schema FROM pg_type t JOIN pg_type a ON a.oid = t.typarray JOIN pg_namespace n ON n.nspname = 'zzt4e_sc' WHERE t.typname = 'zzt4e_in';

-- stmt 5: an index a column constraint asks for is written after the table's row type, and
-- describes no row either
CREATE TABLE zzt4e_uq (i int UNIQUE);

-- begin-expected
-- columns: unique_index_after_rowtype | unique_index_reltype
-- row: t | 0
-- end-expected
SELECT (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_uq') < (SELECT oid FROM pg_class WHERE relname = 'zzt4e_uq_i_key') AS unique_index_after_rowtype, (SELECT reltype FROM pg_class WHERE relname = 'zzt4e_uq_i_key') AS unique_index_reltype;

-- cleanup
DROP TABLE zzt4e_uq;
DROP SCHEMA zzt4e_sc CASCADE;
DROP TABLE zzt4e_two;
DROP TYPE zzt4e_early;
DROP TABLE zzt4e_ident;
DROP TABLE zzt4e_ser;
