-- Range/List partition bound parsing with typed literals, cast syntax, signed numbers

-- stmt 1: plain string bounds (baseline)
CREATE TABLE rp_plain (d date) PARTITION BY RANGE (d);
CREATE TABLE rp_plain_2026 PARTITION OF rp_plain FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
INSERT INTO rp_plain VALUES ('2026-06-15');

-- begin-expected
-- columns: d
-- row: 2026-06-15
-- end-expected
SELECT d FROM rp_plain;

DROP TABLE rp_plain CASCADE;

-- stmt 2: typed DATE bounds
CREATE TABLE rp_typed (d date) PARTITION BY RANGE (d);
CREATE TABLE rp_typed_apr PARTITION OF rp_typed FOR VALUES FROM (DATE '2026-04-01') TO (DATE '2026-05-01');
INSERT INTO rp_typed VALUES ('2026-04-15');

-- begin-expected
-- columns: d
-- row: 2026-04-15
-- end-expected
SELECT d FROM rp_typed;

DROP TABLE rp_typed CASCADE;

-- stmt 3: typed TIMESTAMP bounds
CREATE TABLE rp_ts (ts timestamp) PARTITION BY RANGE (ts);
CREATE TABLE rp_ts_2026 PARTITION OF rp_ts FOR VALUES FROM (TIMESTAMP '2026-01-01 00:00:00') TO (TIMESTAMP '2027-01-01 00:00:00');
INSERT INTO rp_ts VALUES ('2026-06-15 12:00:00');

-- begin-expected
-- columns: ts
-- row: 2026-06-15 12:00:00
-- end-expected
SELECT ts FROM rp_ts;

DROP TABLE rp_ts CASCADE;

-- stmt 4: cast syntax bounds
CREATE TABLE rp_cast (d date) PARTITION BY RANGE (d);
CREATE TABLE rp_cast_apr PARTITION OF rp_cast FOR VALUES FROM ('2026-04-01'::date) TO ('2026-05-01'::date);
INSERT INTO rp_cast VALUES ('2026-04-15');

-- begin-expected
-- columns: d
-- row: 2026-04-15
-- end-expected
SELECT d FROM rp_cast;

DROP TABLE rp_cast CASCADE;

-- stmt 5: signed negative numeric bounds
CREATE TABLE rp_neg (n int) PARTITION BY RANGE (n);
CREATE TABLE rp_neg_low PARTITION OF rp_neg FOR VALUES FROM (-100) TO (0);
CREATE TABLE rp_neg_high PARTITION OF rp_neg FOR VALUES FROM (0) TO (100);
INSERT INTO rp_neg VALUES (-50);
INSERT INTO rp_neg VALUES (50);

-- begin-expected
-- columns: n
-- row: -50
-- row: 50
-- end-expected
SELECT n FROM rp_neg ORDER BY n;

DROP TABLE rp_neg CASCADE;

-- stmt 6: MINVALUE / MAXVALUE
CREATE TABLE rp_mm (n int) PARTITION BY RANGE (n);
CREATE TABLE rp_mm_low PARTITION OF rp_mm FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE rp_mm_high PARTITION OF rp_mm FOR VALUES FROM (0) TO (MAXVALUE);
INSERT INTO rp_mm VALUES (-999);
INSERT INTO rp_mm VALUES (999);

-- begin-expected
-- columns: n
-- row: -999
-- row: 999
-- end-expected
SELECT n FROM rp_mm ORDER BY n;

DROP TABLE rp_mm CASCADE;

-- stmt 7: multi-column range
CREATE TABLE rp_mc (d date, n int) PARTITION BY RANGE (d, n);
CREATE TABLE rp_mc_p1 PARTITION OF rp_mc FOR VALUES FROM ('2026-01-01', 0) TO ('2026-12-31', 100);
INSERT INTO rp_mc VALUES ('2026-06-15', 50);

-- begin-expected
-- columns: d | n
-- row: 2026-06-15, 50
-- end-expected
SELECT d, n FROM rp_mc;

DROP TABLE rp_mc CASCADE;

-- stmt 8: LIST partition with typed DATE values
CREATE TABLE lp_typed (d date) PARTITION BY LIST (d);
CREATE TABLE lp_typed_a PARTITION OF lp_typed FOR VALUES IN (DATE '2026-04-01', DATE '2026-04-02');
INSERT INTO lp_typed VALUES ('2026-04-01');

-- begin-expected
-- columns: d
-- row: 2026-04-01
-- end-expected
SELECT d FROM lp_typed;

DROP TABLE lp_typed CASCADE;

-- stmt 9: DEFAULT partition
CREATE TABLE rp_def (n int) PARTITION BY RANGE (n);
CREATE TABLE rp_def_p1 PARTITION OF rp_def FOR VALUES FROM (0) TO (100);
CREATE TABLE rp_def_default PARTITION OF rp_def DEFAULT;
INSERT INTO rp_def VALUES (50);
INSERT INTO rp_def VALUES (200);

-- begin-expected
-- columns: n
-- row: 50
-- row: 200
-- end-expected
SELECT n FROM rp_def ORDER BY n;

DROP TABLE rp_def CASCADE;

-- stmt 10: positive sign
CREATE TABLE rp_pos (n int) PARTITION BY RANGE (n);
CREATE TABLE rp_pos_p1 PARTITION OF rp_pos FOR VALUES FROM (+0) TO (+100);
INSERT INTO rp_pos VALUES (50);

-- begin-expected
-- columns: n
-- row: 50
-- end-expected
SELECT n FROM rp_pos;

DROP TABLE rp_pos CASCADE;

-- stmt 11: hash partitioning
CREATE TABLE hp (id int) PARTITION BY HASH (id);
CREATE TABLE hp_0 PARTITION OF hp FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE hp_1 PARTITION OF hp FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE hp_2 PARTITION OF hp FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE hp_3 PARTITION OF hp FOR VALUES WITH (MODULUS 4, REMAINDER 3);
INSERT INTO hp VALUES (1);
INSERT INTO hp VALUES (2);
INSERT INTO hp VALUES (3);
INSERT INTO hp VALUES (4);

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT id FROM hp ORDER BY id;

DROP TABLE hp CASCADE;

-- stmt 12: LIST partition with plain text strings
CREATE TABLE lp_text (status text) PARTITION BY LIST (status);
CREATE TABLE lp_text_active PARTITION OF lp_text FOR VALUES IN ('active', 'pending');
CREATE TABLE lp_text_done PARTITION OF lp_text FOR VALUES IN ('done', 'archived');
INSERT INTO lp_text VALUES ('active');
INSERT INTO lp_text VALUES ('pending');
INSERT INTO lp_text VALUES ('done');
INSERT INTO lp_text VALUES ('archived');

-- begin-expected
-- columns: status
-- row: active
-- row: archived
-- row: done
-- row: pending
-- end-expected
SELECT status FROM lp_text ORDER BY status;

DROP TABLE lp_text CASCADE;
