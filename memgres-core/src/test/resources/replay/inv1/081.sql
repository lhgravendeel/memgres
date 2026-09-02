-- source: investigation.md
-- finding: 81
-- title: Declared-size limits are not enforced (18 cases)
-- unrunnable: the report wrote this reproducer abbreviated
CREATE TABLE t (a varchar(10485761));
-- PG: 22023 length cannot exceed 10485760 | mg: OK
CREATE TABLE t (a char(0));
-- PG: 22023 length must be at least 1     | mg: OK
CREATE TABLE t (a bit(0));
-- PG: 22023 length must be at least 1     | mg: OK
CREATE TABLE t (a numeric(1001,2));
-- PG: 22023 precision must be 1..1000     | mg: OK
CREATE TABLE t (a float(54));
-- PG: 22023 precision must be < 54 bits   | mg: OK
SELECT '1'::varchar(10485761);
-- PG: 22023 | mg: OK
SELECT array_ndims(ARRAY[[[[[[[1]]]]]]]);
--   PG: 54000 number of array dimensions (7) exceeds the maximum allowed (6) | mg: 7
SELECT concat(… 101 arguments …);
--   PG: 54023 cannot pass more than 100 arguments to a function | mg: OK
CREATE INDEX i ON t (c1, …, c33);
--   PG: 54011 cannot use more than 32 columns in an index | mg: OK
ALTER TABLE t ADD PRIMARY KEY (c1, …, c33);
-- PG: 54011 | mg: OK;;
