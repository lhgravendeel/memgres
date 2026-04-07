\echo '=== 04_ddl_schema.sql ==='
\set VERBOSITY verbose
\set SHOW_CONTEXT never
\set ON_ERROR_STOP off
SET search_path = pg_catalog, public;
DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
CREATE SCHEMA compat_extra;
SET search_path = compat, pg_catalog;

CREATE TABLE base_tbl(
    id int,
    name text,
    created_at timestamp default current_timestamp
);
CREATE VIEW base_v AS SELECT id, name FROM base_tbl;
CREATE SEQUENCE seq1 START 10 INCREMENT 5;
CREATE TABLE seq_tbl(id int DEFAULT nextval('seq1'), payload text);
CREATE INDEX idx_base_name ON base_tbl(name);
CREATE UNIQUE INDEX idx_base_id_uq ON base_tbl(id);
CREATE TABLE child_tbl(
    child_id int,
    base_id int
);
ALTER TABLE child_tbl ADD COLUMN note text;
ALTER TABLE child_tbl RENAME COLUMN note TO remarks;
ALTER TABLE child_tbl RENAME TO child_tbl_renamed;
ALTER TABLE base_tbl ADD COLUMN flag boolean DEFAULT false;
ALTER TABLE base_tbl ALTER COLUMN name TYPE varchar(20);
ALTER TABLE base_tbl ALTER COLUMN flag SET DEFAULT true;
ALTER TABLE base_tbl ALTER COLUMN flag DROP DEFAULT;

INSERT INTO base_tbl(id, name) VALUES (1, 'one'), (2, 'two');
SELECT * FROM base_v ORDER BY id;
SELECT nextval('seq1') AS seq_a, currval('seq1') AS seq_b;

CREATE TABLE compat_extra.qtab(x int);
SELECT * FROM compat_extra.qtab;

-- DDL semantic errors
CREATE SCHEMA compat;
CREATE TABLE base_tbl(id int);
CREATE VIEW base_v AS SELECT 1 AS x;
CREATE SEQUENCE seq1;
CREATE INDEX idx_base_name ON base_tbl(name);
CREATE TABLE dup_cols(a int, a text);
CREATE TABLE bad_fk(a int REFERENCES no_such_table(id));
CREATE TABLE bad_default(a int DEFAULT now());
CREATE TABLE bad_generated(a int, b timestamp GENERATED ALWAYS AS (now()) STORED);
ALTER TABLE no_such_table ADD COLUMN x int;
ALTER TABLE base_tbl ADD COLUMN name text;
ALTER TABLE base_tbl ALTER COLUMN no_such_column TYPE text;
ALTER TABLE base_tbl DROP COLUMN no_such_column;
ALTER TABLE base_tbl RENAME COLUMN no_such_column TO x;
ALTER TABLE base_tbl RENAME TO child_tbl_renamed;
ALTER TABLE base_tbl ALTER COLUMN id TYPE timestamp;
DROP TABLE no_such_table;
DROP VIEW base_tbl;
DROP SEQUENCE base_tbl;
DROP INDEX no_such_index;
CREATE INDEX idx_expr_bad ON base_tbl ((unknown_func(name)));
CREATE INDEX idx_bad_col ON base_tbl(no_such_column);

-- dependency behavior
DROP TABLE base_tbl;
DROP TABLE base_tbl CASCADE;

-- recreate after cascade to continue
CREATE TABLE base_tbl(id int, name text);
CREATE VIEW base_v AS SELECT * FROM base_tbl;

-- syntax/DDL edge cases that parse differently from semantic failures
CREATE TABLE ddl_bad_01 id int);
CREATE TABLE ddl_bad_02 (id int
CREATE INDEX ddl_bad_03 base_tbl(name);
ALTER TABLE base_tbl ADD;
DROP VIEW IF EXISTS;

DROP SCHEMA compat CASCADE;
DROP SCHEMA compat_extra CASCADE;
