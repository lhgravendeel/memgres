-- source: investigation-2026-08.md
-- finding: 363
-- title: The RETURNING path is built against the statement's target relation, not the partition the row was routed to. DmlExecutor.java:890 computes targetTable, the lea
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_p (id int) PARTITION BY RANGE (id);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_p1 PARTITION OF zz_vf2_p FOR VALUES FROM (0) TO (10);
-- begin-expected
-- columns: tableoid:text
-- row: zz_vf2_p1
-- rowcount: 1
-- end-expected
INSERT INTO zz_vf2_p VALUES (1) RETURNING tableoid::regclass::text;
