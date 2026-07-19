-- Partitioned table lifecycle: TRUNCATE, DROP, DETACH/ATTACH, bound typing, NULL routing

-- stmt 1: TRUNCATE on a partitioned parent empties all partitions
CREATE TABLE ptl_tr (id int, v text) PARTITION BY RANGE (id);
CREATE TABLE ptl_tr_p1 PARTITION OF ptl_tr FOR VALUES FROM (0) TO (100);
CREATE TABLE ptl_tr_p2 PARTITION OF ptl_tr FOR VALUES FROM (100) TO (200);
INSERT INTO ptl_tr VALUES (10, 'a'), (110, 'b'), (150, 'c');
TRUNCATE ptl_tr;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM ptl_tr;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM ptl_tr_p1;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM ptl_tr_p2;

-- stmt 2: TRUNCATE ONLY on a partitioned parent is an error
-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot truncate only a partitioned table
-- end-expected-error
TRUNCATE ONLY ptl_tr;

-- stmt 3: TRUNCATE ONLY on a leaf partition is allowed
INSERT INTO ptl_tr VALUES (10, 'a'), (110, 'b');
TRUNCATE ONLY ptl_tr_p1;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM ptl_tr;

DROP TABLE ptl_tr;

-- stmt 4: DROP TABLE of a partitioned parent drops its partitions too (no CASCADE needed)
CREATE TABLE ptl_dp (id int) PARTITION BY RANGE (id);
CREATE TABLE ptl_dp_p1 PARTITION OF ptl_dp FOR VALUES FROM (0) TO (100);
INSERT INTO ptl_dp VALUES (10);
DROP TABLE ptl_dp;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: does not exist
-- end-expected-error
SELECT * FROM ptl_dp_p1;

-- stmt 5: DROP of one partition removes it from routing; inserts into its range then fail
CREATE TABLE ptl_dg (id int) PARTITION BY RANGE (id);
CREATE TABLE ptl_dg_p1 PARTITION OF ptl_dg FOR VALUES FROM (0) TO (100);
CREATE TABLE ptl_dg_p2 PARTITION OF ptl_dg FOR VALUES FROM (100) TO (200);
INSERT INTO ptl_dg VALUES (10), (110);
DROP TABLE ptl_dg_p1;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM ptl_dg;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: no partition of relation
-- end-expected-error
INSERT INTO ptl_dg VALUES (20);

DROP TABLE ptl_dg;

-- stmt 6: text-keyed RANGE partition with MAXVALUE accepts high values
CREATE TABLE ptl_txt (name text) PARTITION BY RANGE (name);
CREATE TABLE ptl_txt_am PARTITION OF ptl_txt FOR VALUES FROM (MINVALUE) TO ('m');
CREATE TABLE ptl_txt_mz PARTITION OF ptl_txt FOR VALUES FROM ('m') TO (MAXVALUE);
INSERT INTO ptl_txt VALUES ('apple'), ('zebra'), ('mango');

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM ptl_txt_mz;

-- begin-expected
-- columns: name
-- row: apple
-- row: mango
-- row: zebra
-- end-expected
SELECT name FROM ptl_txt ORDER BY name;

DROP TABLE ptl_txt;

-- stmt 7: numeric RANGE partitioning with sentinels still routes correctly
CREATE TABLE ptl_num (n int) PARTITION BY RANGE (n);
CREATE TABLE ptl_num_low PARTITION OF ptl_num FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE ptl_num_high PARTITION OF ptl_num FOR VALUES FROM (0) TO (MAXVALUE);
INSERT INTO ptl_num VALUES (-999), (0), (999);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM ptl_num_high;

DROP TABLE ptl_num;

-- stmt 8: LIST partition declaring NULL receives SQL NULL rows
CREATE TABLE ptl_ln (v text) PARTITION BY LIST (v);
CREATE TABLE ptl_ln_ab PARTITION OF ptl_ln FOR VALUES IN ('a', 'b');
CREATE TABLE ptl_ln_null PARTITION OF ptl_ln FOR VALUES IN (NULL);
INSERT INTO ptl_ln VALUES ('a'), (NULL);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM ptl_ln_null;

DROP TABLE ptl_ln;

-- stmt 9: NULL without a NULL list partition goes to the DEFAULT partition
CREATE TABLE ptl_ld (v text) PARTITION BY LIST (v);
CREATE TABLE ptl_ld_a PARTITION OF ptl_ld FOR VALUES IN ('a');
CREATE TABLE ptl_ld_def PARTITION OF ptl_ld DEFAULT;
INSERT INTO ptl_ld VALUES (NULL);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM ptl_ld_def;

DROP TABLE ptl_ld;

-- stmt 10: NULL with neither NULL list partition nor DEFAULT partition is an error
CREATE TABLE ptl_le (v text) PARTITION BY LIST (v);
CREATE TABLE ptl_le_a PARTITION OF ptl_le FOR VALUES IN ('a');

-- begin-expected-error
-- sqlstate: 23514
-- message-like: no partition of relation
-- end-expected-error
INSERT INTO ptl_le VALUES (NULL);

DROP TABLE ptl_le;

-- stmt 11: DETACH PARTITION removes routing; detached table keeps its rows
CREATE TABLE ptl_de (id int) PARTITION BY RANGE (id);
CREATE TABLE ptl_de_p1 PARTITION OF ptl_de FOR VALUES FROM (0) TO (100);
INSERT INTO ptl_de VALUES (10);
ALTER TABLE ptl_de DETACH PARTITION ptl_de_p1;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM ptl_de;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM ptl_de_p1;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: no partition of relation
-- end-expected-error
INSERT INTO ptl_de VALUES (10);

DROP TABLE ptl_de;
DROP TABLE ptl_de_p1;

-- stmt 12: ATTACH PARTITION with overlapping bounds is rejected
CREATE TABLE ptl_at (id int) PARTITION BY RANGE (id);
CREATE TABLE ptl_at_p1 PARTITION OF ptl_at FOR VALUES FROM (0) TO (100);
CREATE TABLE ptl_at_new (id int);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: would overlap partition
-- end-expected-error
ALTER TABLE ptl_at ATTACH PARTITION ptl_at_new FOR VALUES FROM (50) TO (150);

-- stmt 13: after the rejected ATTACH the table is not half-attached
-- begin-expected-error
-- sqlstate: 23514
-- message-like: no partition of relation
-- end-expected-error
INSERT INTO ptl_at VALUES (120);

-- stmt 14: a non-overlapping ATTACH then succeeds and routes
ALTER TABLE ptl_at ATTACH PARTITION ptl_at_new FOR VALUES FROM (100) TO (200);
INSERT INTO ptl_at VALUES (120);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM ptl_at_new;

DROP TABLE ptl_at;

-- stmt 15: multi-level partitioning — TRUNCATE of the top parent empties leaf sub-partitions
CREATE TABLE ptl_ml (region text, id int) PARTITION BY LIST (region);
CREATE TABLE ptl_ml_us PARTITION OF ptl_ml FOR VALUES IN ('us') PARTITION BY RANGE (id);
CREATE TABLE ptl_ml_us_1 PARTITION OF ptl_ml_us FOR VALUES FROM (0) TO (100);
CREATE TABLE ptl_ml_eu PARTITION OF ptl_ml FOR VALUES IN ('eu');
INSERT INTO ptl_ml VALUES ('us', 5), ('eu', 7);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM ptl_ml;

TRUNCATE ptl_ml;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM ptl_ml_us_1;

-- stmt 16: multi-level DROP of the top parent drops leaf sub-partitions
DROP TABLE ptl_ml;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: does not exist
-- end-expected-error
SELECT * FROM ptl_ml_us_1;

-- stmt 17: multi-column RANGE key routing with MAXVALUE element
CREATE TABLE ptl_mc (a int, b int) PARTITION BY RANGE (a, b);
CREATE TABLE ptl_mc_p1 PARTITION OF ptl_mc FOR VALUES FROM (0, 0) TO (10, MAXVALUE);
CREATE TABLE ptl_mc_p2 PARTITION OF ptl_mc FOR VALUES FROM (10, MAXVALUE) TO (20, MAXVALUE);
INSERT INTO ptl_mc VALUES (5, 500), (10, 3), (15, 1);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM ptl_mc_p1;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM ptl_mc_p2;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: no partition of relation
-- end-expected-error
INSERT INTO ptl_mc VALUES (25, 0);

DROP TABLE ptl_mc;
