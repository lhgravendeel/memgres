\pset pager off
\pset format unaligned
\pset tuples_only off
\pset null <NULL>
\set VERBOSITY verbose
\set SHOW_CONTEXT always
\set ON_ERROR_STOP off

DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;
SET client_min_messages = notice;
SET extra_float_digits = 0;
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC';

SELECT current_schema() AS current_schema,
       current_setting('TimeZone') AS timezone,
       current_setting('DateStyle') AS datestyle,
       current_setting('IntervalStyle') AS intervalstyle;

CREATE SCHEMA extra;
CREATE TABLE t1(id int PRIMARY KEY, v text DEFAULT 'x');
CREATE TABLE t2(id int REFERENCES t1(id), note text);
CREATE VIEW v1 AS SELECT * FROM t1;
CREATE SEQUENCE s1 START 10 INCREMENT 2;
CREATE INDEX t1_v_idx ON t1(v);
CREATE TYPE status AS ENUM ('new','done');
CREATE DOMAIN shorttxt AS varchar(3);
CREATE TEMP TABLE tt(a int);

ALTER TABLE t1 ADD COLUMN created_at timestamptz DEFAULT now();
ALTER TABLE t1 ALTER COLUMN v TYPE varchar(10);
ALTER TABLE t1 RENAME COLUMN v TO val;
ALTER TABLE t1 RENAME TO t1_renamed;
ALTER VIEW v1 RENAME TO v1_renamed;
ALTER SEQUENCE s1 RESTART WITH 100;
ALTER DOMAIN shorttxt ADD CONSTRAINT shorttxt_nn CHECK (VALUE IS NOT NULL);

DROP VIEW v1_renamed;
DROP TABLE t2;
DROP TABLE t1_renamed;
DROP SEQUENCE s1;
DROP TYPE status;
DROP DOMAIN shorttxt;

-- failure cases
CREATE TABLE tbad(a int, a int);
CREATE TABLE tbad2(a int DEFAULT 'x');
CREATE TABLE no_schema.t(a int);
ALTER TABLE no_such ADD COLUMN x int;
CREATE VIEW vbad AS SELECT * FROM no_such;
CREATE INDEX idx_bad ON no_such(a);
DROP TABLE no_such;
DROP SCHEMA compat;
CREATE TYPE bad_enum AS ENUM ();
CREATE DOMAIN bad_domain AS int CHECK ();
ALTER TABLE tt ADD COLUMN a text;
CREATE TEMP TABLE tt(a int);
CREATE TABLE dep1(a int PRIMARY KEY);
CREATE TABLE dep2(a int REFERENCES dep1(a));
DROP TABLE dep1;
DROP TABLE dep1 CASCADE;
DROP TABLE dep2;
DROP TABLE dep1;

DROP SCHEMA compat CASCADE;



-- more DDL object kinds and alter variants
CREATE TABLE part_t(id int, v text) PARTITION BY RANGE (id);
CREATE TABLE part_t_p1 PARTITION OF part_t FOR VALUES FROM (1) TO (100);
CREATE TABLE parent_inh(id int, note text);
CREATE TABLE child_inh(extra int) INHERITS (parent_inh);
CREATE UNLOGGED TABLE unlogged_t(a int);
CREATE MATERIALIZED VIEW mv1 AS SELECT 1 AS x;
CREATE INDEX t_part_expr_idx ON part_t ((lower(v)));
CREATE UNIQUE INDEX t_part_unique_idx ON part_t (id);
CREATE INDEX t_part_partial_idx ON part_t (id) WHERE id > 10;

ALTER TABLE child_inh ADD COLUMN z int DEFAULT 9;
ALTER TABLE child_inh ALTER COLUMN z DROP DEFAULT;
ALTER TABLE child_inh ALTER COLUMN z SET DEFAULT 10;
ALTER TABLE child_inh ALTER COLUMN note SET NOT NULL;
ALTER TABLE child_inh ALTER COLUMN note DROP NOT NULL;
ALTER TABLE child_inh ADD CONSTRAINT child_inh_extra_ck CHECK (extra >= 0);
ALTER TABLE child_inh DROP CONSTRAINT child_inh_extra_ck;
COMMENT ON TABLE child_inh IS 'inheritance child';
TRUNCATE TABLE child_inh;
REFRESH MATERIALIZED VIEW mv1;

-- more DDL failure cases
CREATE TABLE part_bad PARTITION OF part_t FOR VALUES FROM (100) TO (1);
CREATE TABLE bad_inh() INHERITS ();
CREATE MATERIALIZED VIEW mv_bad AS;
ALTER TABLE child_inh ALTER COLUMN nope TYPE text;
ALTER TABLE child_inh ADD CONSTRAINT child_inh_extra_ck CHECK (extra >= 0);
ALTER TABLE child_inh DROP CONSTRAINT no_such_constraint;
COMMENT ON TABLE no_such IS 'x';
TRUNCATE TABLE no_such;
REFRESH MATERIALIZED VIEW no_such;
REINDEX TABLE no_such;

