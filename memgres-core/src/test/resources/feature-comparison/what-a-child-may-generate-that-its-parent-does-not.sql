-- ============================================================================
-- What a child may generate that its parent does not
--
-- A child holds its parent's rows, so a column of it is filled the way the
-- parent's is: a value the writer supplies where the parent takes one, and an
-- expression the relation works out where the parent works one out. Two
-- generated columns may disagree about the expression, because each relation
-- computes its own rows, but not about whether the column is computed at all,
-- nor about the kind of generated column it is.
--
-- PostgreSQL reads both links out of the one rule, so ALTER TABLE ... INHERIT
-- says what ATTACH PARTITION says. A DEFAULT is not a generation expression, so
-- a parent that has one still refuses a child that computes the column, and two
-- DEFAULTs may disagree freely.
--
-- Every value below was read off PostgreSQL 18 before it was written down.
-- ============================================================================

-- ============================================================================
-- A generated column on one side of the link and not on the other
-- ============================================================================

CREATE TABLE zzm3sd_gp (i int, k int, s text) PARTITION BY LIST (s);
CREATE TABLE zzm3sd_gc (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "k" in child table must not be a generated column
-- end-expected-error
ALTER TABLE zzm3sd_gp ATTACH PARTITION zzm3sd_gc FOR VALUES IN ('a');

DROP TABLE zzm3sd_gc;
DROP TABLE zzm3sd_gp;

CREATE TABLE zzm3sd_gp (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text) PARTITION BY LIST (s);
CREATE TABLE zzm3sd_gc (i int, k int, s text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "k" in child table must be a generated column
-- end-expected-error
ALTER TABLE zzm3sd_gp ATTACH PARTITION zzm3sd_gc FOR VALUES IN ('a');

DROP TABLE zzm3sd_gc;
DROP TABLE zzm3sd_gp;

-- a default is not a generation expression
CREATE TABLE zzm3sd_gp (i int, k int DEFAULT 7, s text) PARTITION BY LIST (s);
CREATE TABLE zzm3sd_gc (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "k" in child table must not be a generated column
-- end-expected-error
ALTER TABLE zzm3sd_gp ATTACH PARTITION zzm3sd_gc FOR VALUES IN ('a');

DROP TABLE zzm3sd_gc;
DROP TABLE zzm3sd_gp;

-- two generated columns of different kinds are refused for the kind
CREATE TABLE zzm3sd_gp (i int, k int GENERATED ALWAYS AS (i * 2) VIRTUAL, s text) PARTITION BY LIST (s);
CREATE TABLE zzm3sd_gc (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "k" inherits from generated column of different kind
-- end-expected-error
ALTER TABLE zzm3sd_gp ATTACH PARTITION zzm3sd_gc FOR VALUES IN ('a');

DROP TABLE zzm3sd_gc;
DROP TABLE zzm3sd_gp;

-- ============================================================================
-- What the link does take
-- ============================================================================

-- two of the same kind are attached whatever they compute, and the partition
-- goes on holding its own expression
CREATE TABLE zzm3sd_gp (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text) PARTITION BY LIST (s);
CREATE TABLE zzm3sd_gc (i int, k int GENERATED ALWAYS AS (i * 3) STORED, s text);
ALTER TABLE zzm3sd_gp ATTACH PARTITION zzm3sd_gc FOR VALUES IN ('a');

-- begin-expected
-- columns: gen
-- row: (i * 3)
-- end-expected
SELECT pg_get_expr(adbin, adrelid) AS gen FROM pg_attrdef WHERE adrelid = 'zzm3sd_gc'::regclass;

DROP TABLE zzm3sd_gp CASCADE;

CREATE TABLE zzm3sd_vp (i int, k int GENERATED ALWAYS AS (i * 2) VIRTUAL, s text) PARTITION BY LIST (s);
CREATE TABLE zzm3sd_vc (i int, k int GENERATED ALWAYS AS (i * 2) VIRTUAL, s text);
ALTER TABLE zzm3sd_vp ATTACH PARTITION zzm3sd_vc FOR VALUES IN ('a');

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_inherits WHERE inhrelid = 'zzm3sd_vc'::regclass;

DROP TABLE zzm3sd_vp CASCADE;

CREATE TABLE zzm3sd_np (i int, k int, s text) PARTITION BY LIST (s);
CREATE TABLE zzm3sd_nc (i int, k int, s text);
ALTER TABLE zzm3sd_np ATTACH PARTITION zzm3sd_nc FOR VALUES IN ('a');
INSERT INTO zzm3sd_np VALUES (1, 2, 'a');

-- begin-expected
-- columns: i | k | s
-- row: 1 | 2 | a
-- end-expected
SELECT i, k, s FROM zzm3sd_nc;

DROP TABLE zzm3sd_np CASCADE;

-- two defaults may disagree freely
CREATE TABLE zzm3sd_dp (i int, k int DEFAULT 7, s text) PARTITION BY LIST (s);
CREATE TABLE zzm3sd_dc2 (i int, k int DEFAULT 9, s text);
ALTER TABLE zzm3sd_dp ATTACH PARTITION zzm3sd_dc2 FOR VALUES IN ('a');

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_inherits WHERE inhrelid = 'zzm3sd_dc2'::regclass;

DROP TABLE zzm3sd_dp CASCADE;

-- ============================================================================
-- The same rule for a table joining an inheritance hierarchy
-- ============================================================================

CREATE TABLE zzm3sd_ip (i int, k int, s text);
CREATE TABLE zzm3sd_ic (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "k" in child table must not be a generated column
-- end-expected-error
ALTER TABLE zzm3sd_ic INHERIT zzm3sd_ip;

DROP TABLE zzm3sd_ic;
DROP TABLE zzm3sd_ip;

CREATE TABLE zzm3sd_ip (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text);
CREATE TABLE zzm3sd_ic (i int, k int, s text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "k" in child table must be a generated column
-- end-expected-error
ALTER TABLE zzm3sd_ic INHERIT zzm3sd_ip;

DROP TABLE zzm3sd_ic;
DROP TABLE zzm3sd_ip;

CREATE TABLE zzm3sd_ip (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text);
CREATE TABLE zzm3sd_ic (i int, k int GENERATED ALWAYS AS (i * 2) STORED, s text);
ALTER TABLE zzm3sd_ic INHERIT zzm3sd_ip;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_inherits WHERE inhrelid = 'zzm3sd_ic'::regclass;

ALTER TABLE zzm3sd_ic NO INHERIT zzm3sd_ip;
DROP TABLE zzm3sd_ic;
DROP TABLE zzm3sd_ip;
