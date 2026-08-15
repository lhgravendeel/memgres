-- One DROP settles every name it was given before it takes any of them.
--
-- A name whose relation another name in the same list carries off -- a partition of a partitioned
-- table, an inheritance child, a table a foreign key points at -- is dropped once, and is not
-- reported missing when its own turn in the list comes round. That holds in either order, with
-- and without CASCADE, and with and without IF EXISTS: both names were there when the list was
-- read, so IF EXISTS has nothing to skip.
--
-- A name of the wrong kind is still refused, and refused before anything goes; a name that never
-- existed takes the whole statement with it and leaves every other name where it was.
--
-- Every answer below was read off PostgreSQL 18.

-- ============================================================================
-- A partitioned table and its partition, in each order
-- ============================================================================

-- stmt 1: the partitioned table named first, with CASCADE
CREATE TABLE zzr7gn_hq (i int) PARTITION BY RANGE (i);
CREATE TABLE zzr7gn_h1 PARTITION OF zzr7gn_hq FOR VALUES FROM (0) TO (10);
DROP TABLE zzr7gn_hq, zzr7gn_h1 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname LIKE 'zzr7gn!_h%' ESCAPE '!';

-- stmt 2: the partition named first, without CASCADE
CREATE TABLE zzr7gn_hq (i int) PARTITION BY RANGE (i);
CREATE TABLE zzr7gn_h1 PARTITION OF zzr7gn_hq FOR VALUES FROM (0) TO (10);
DROP TABLE zzr7gn_h1, zzr7gn_hq;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname LIKE 'zzr7gn!_h%' ESCAPE '!';

-- stmt 3: IF EXISTS has nothing to skip -- both names were there when the list was read
CREATE TABLE zzr7gn_hq (i int) PARTITION BY RANGE (i);
CREATE TABLE zzr7gn_h1 PARTITION OF zzr7gn_hq FOR VALUES FROM (0) TO (10);
DROP TABLE IF EXISTS zzr7gn_hq, zzr7gn_h1;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname LIKE 'zzr7gn!_h%' ESCAPE '!';

-- stmt 4: a sub-partition named ahead of both the tables it hangs from
CREATE TABLE zzr7gn_hq (i int) PARTITION BY RANGE (i);
CREATE TABLE zzr7gn_h1 PARTITION OF zzr7gn_hq FOR VALUES FROM (0) TO (10) PARTITION BY RANGE (i);
CREATE TABLE zzr7gn_h2 PARTITION OF zzr7gn_h1 FOR VALUES FROM (0) TO (5);
DROP TABLE zzr7gn_h2, zzr7gn_hq, zzr7gn_h1;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname LIKE 'zzr7gn!_h%' ESCAPE '!';

-- stmt 5: a reader of the partition the list does not name refuses the whole set
CREATE TABLE zzr7gn_bq (i int) PARTITION BY RANGE (i);
CREATE TABLE zzr7gn_b1 PARTITION OF zzr7gn_bq FOR VALUES FROM (0) TO (10);
CREATE VIEW zzr7gn_bv AS SELECT * FROM zzr7gn_b1;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table zzr7gn_bq because other objects depend on it
-- end-expected-error
DROP TABLE zzr7gn_bq;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop desired object(s) because other objects depend on them
-- end-expected-error
DROP TABLE zzr7gn_bq, zzr7gn_b1;

DROP TABLE zzr7gn_bq, zzr7gn_b1 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_bq','zzr7gn_b1','zzr7gn_bv');

-- stmt 6: a reader of the partitioned table, with the partition named first
CREATE TABLE zzr7gn_pq (i int) PARTITION BY RANGE (i);
CREATE TABLE zzr7gn_p1 PARTITION OF zzr7gn_pq FOR VALUES FROM (0) TO (10);
CREATE VIEW zzr7gn_pv AS SELECT * FROM zzr7gn_pq;
DROP TABLE zzr7gn_p1, zzr7gn_pq CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_pq','zzr7gn_p1','zzr7gn_pv');

-- ============================================================================
-- An inheritance parent and its child, in each order
-- ============================================================================

-- stmt 7: the parent named first
CREATE TABLE zzr7gn_pa (i int);
CREATE TABLE zzr7gn_ch (j int) INHERITS (zzr7gn_pa);
DROP TABLE zzr7gn_pa, zzr7gn_ch CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_pa','zzr7gn_ch');

-- stmt 8: the child named first, without CASCADE
CREATE TABLE zzr7gn_pa (i int);
CREATE TABLE zzr7gn_ch (j int) INHERITS (zzr7gn_pa);
DROP TABLE zzr7gn_ch, zzr7gn_pa;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_pa','zzr7gn_ch');

-- ============================================================================
-- Two tables joined by a foreign key, with and without CASCADE
-- ============================================================================

-- stmt 9: the referenced table named first, with CASCADE
CREATE TABLE zzr7gn_fp (i int PRIMARY KEY);
CREATE TABLE zzr7gn_fc (i int REFERENCES zzr7gn_fp(i));
DROP TABLE zzr7gn_fp, zzr7gn_fc CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_fp','zzr7gn_fc');

-- stmt 10: the referring table named first, without CASCADE
CREATE TABLE zzr7gn_fp (i int PRIMARY KEY);
CREATE TABLE zzr7gn_fc (i int REFERENCES zzr7gn_fp(i));
DROP TABLE zzr7gn_fc, zzr7gn_fp;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_fp','zzr7gn_fc');

-- stmt 11: a referring table the list does not name refuses the whole set, and leaves it all
CREATE TABLE zzr7gn_ep (i int PRIMARY KEY);
CREATE TABLE zzr7gn_ec (i int REFERENCES zzr7gn_ep(i));
CREATE TABLE zzr7gn_eo (i int);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop desired object(s) because other objects depend on them
-- end-expected-error
DROP TABLE zzr7gn_ep, zzr7gn_eo;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table zzr7gn_ep because other objects depend on it
-- end-expected-error
DROP TABLE zzr7gn_ep;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_ep','zzr7gn_ec','zzr7gn_eo');

DROP TABLE zzr7gn_ec, zzr7gn_ep, zzr7gn_eo;

-- ============================================================================
-- A table beside a view, an index and a sequence: the kind is settled first
-- ============================================================================

-- stmt 12: a view over the table, named beside it
CREATE TABLE zzr7gn_t (i int);
CREATE VIEW zzr7gn_v1 AS SELECT * FROM zzr7gn_t;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzr7gn_v1" is not a table
-- end-expected-error
DROP TABLE zzr7gn_t, zzr7gn_v1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzr7gn_t" is not a view
-- end-expected-error
DROP VIEW zzr7gn_v1, zzr7gn_t;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table zzr7gn_t because other objects depend on it
-- end-expected-error
DROP TABLE zzr7gn_t;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_t','zzr7gn_v1');

DROP TABLE zzr7gn_t CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_t','zzr7gn_v1');

-- stmt 13: an index on the table, named beside it
CREATE TABLE zzr7gn_ixt (i int);
CREATE INDEX zzr7gn_ix ON zzr7gn_ixt (i);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzr7gn_ix" is not a table
-- end-expected-error
DROP TABLE zzr7gn_ixt, zzr7gn_ix;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzr7gn_ixt" is not an index
-- end-expected-error
DROP INDEX zzr7gn_ix, zzr7gn_ixt;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_ixt','zzr7gn_ix');

DROP TABLE zzr7gn_ixt;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_ixt','zzr7gn_ix');

-- stmt 14: a sequence the table's column owns, named beside it
CREATE TABLE zzr7gn_sq (i serial, j int);

-- begin-expected
-- columns: n
-- row: zzr7gn_sq
-- row: zzr7gn_sq_i_seq
-- end-expected
SELECT relname AS n FROM pg_class WHERE relname LIKE 'zzr7gn!_sq%' ESCAPE '!' ORDER BY 1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzr7gn_sq_i_seq" is not a table
-- end-expected-error
DROP TABLE zzr7gn_sq, zzr7gn_sq_i_seq;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop sequence zzr7gn_sq_i_seq because other objects depend on it
-- end-expected-error
DROP SEQUENCE zzr7gn_sq_i_seq;

DROP TABLE zzr7gn_sq;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname LIKE 'zzr7gn!_sq%' ESCAPE '!';

-- ============================================================================
-- A name that never existed, beside one that does
-- ============================================================================

-- stmt 15: named second, and named first: the statement goes either way
CREATE TABLE zzr7gn_a (i int);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: table "zzr7gn_nosuch" does not exist
-- end-expected-error
DROP TABLE zzr7gn_a, zzr7gn_nosuch;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: table "zzr7gn_nosuch" does not exist
-- end-expected-error
DROP TABLE zzr7gn_nosuch, zzr7gn_a;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'zzr7gn_a';

-- stmt 16: IF EXISTS passes over the name that was never there and takes the other
DROP TABLE IF EXISTS zzr7gn_nosuch, zzr7gn_a;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname = 'zzr7gn_a';

-- stmt 17: one relation written twice is one name settled twice, not a name that went missing
CREATE TABLE zzr7gn_da (i int);
CREATE VIEW zzr7gn_dv AS SELECT * FROM zzr7gn_da;
DROP TABLE public.zzr7gn_da, zzr7gn_da CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname IN ('zzr7gn_da','zzr7gn_dv');

-- cleanup
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_class WHERE relname LIKE 'zzr7gn!_%' ESCAPE '!';
