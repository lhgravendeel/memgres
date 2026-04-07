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

-- partitioning
CREATE TABLE p_range(
  id int,
  region text,
  created date
) PARTITION BY RANGE (id);

CREATE TABLE p_range_1_100 PARTITION OF p_range
FOR VALUES FROM (1) TO (100);

CREATE TABLE p_range_100_200 PARTITION OF p_range
FOR VALUES FROM (100) TO (200);

CREATE TABLE p_list(
  code text,
  qty int
) PARTITION BY LIST (code);

CREATE TABLE p_list_a PARTITION OF p_list
FOR VALUES IN ('A', 'B');

CREATE TABLE p_list_default PARTITION OF p_list
DEFAULT;

CREATE TABLE p_hash(
  id int,
  note text
) PARTITION BY HASH (id);

CREATE TABLE p_hash_0 PARTITION OF p_hash FOR VALUES WITH (modulus 2, remainder 0);
CREATE TABLE p_hash_1 PARTITION OF p_hash FOR VALUES WITH (modulus 2, remainder 1);

INSERT INTO p_range VALUES (1, 'EU', DATE '2024-01-01'), (150, 'US', DATE '2024-01-02');
INSERT INTO p_list VALUES ('A', 10), ('Z', 99);
INSERT INTO p_hash VALUES (1, 'odd'), (2, 'even');

SELECT tableoid::regclass, * FROM p_range ORDER BY id;
SELECT tableoid::regclass, * FROM p_list ORDER BY code;
SELECT tableoid::regclass, * FROM p_hash ORDER BY id;

CREATE INDEX p_range_idx ON p_range (id);
ALTER TABLE p_range ATTACH PARTITION p_range_100_200 FOR VALUES FROM (100) TO (200);
ALTER TABLE p_list DETACH PARTITION p_list_default;
ALTER TABLE p_list ATTACH PARTITION p_list_default DEFAULT;

-- inheritance
CREATE TABLE inh_parent(
  id int,
  note text
);

CREATE TABLE inh_child(
  extra int
) INHERITS (inh_parent);

INSERT INTO inh_parent VALUES (1, 'parent');
INSERT INTO inh_child VALUES (2, 'child', 99);

SELECT * FROM inh_parent ORDER BY id;
SELECT * FROM ONLY inh_parent ORDER BY id;
SELECT * FROM inh_child ORDER BY id;

ALTER TABLE inh_child NO INHERIT inh_parent;
ALTER TABLE inh_child INHERIT inh_parent;

-- bad partitioning / inheritance cases
CREATE TABLE p_range_bad PARTITION OF p_range FOR VALUES FROM (50) TO (120);
INSERT INTO p_range VALUES (999, 'XX', DATE '2024-01-03');
CREATE TABLE p_list_bad PARTITION OF p_list FOR VALUES IN ('A');
CREATE TABLE p_hash_bad PARTITION OF p_hash FOR VALUES WITH (modulus 2, remainder 2);
ALTER TABLE p_list DETACH PARTITION no_such_partition;
ALTER TABLE p_range ATTACH PARTITION p_range_1_100 FOR VALUES FROM (1) TO (100);
CREATE TABLE inh_bad() INHERITS ();
ALTER TABLE inh_parent INHERIT no_such_parent;
ALTER TABLE inh_parent NO INHERIT no_such_parent;

DROP SCHEMA compat CASCADE;
