-- source: review-2026-08.md
-- finding: A leaf partition is stored as an ordinary table, so nothing enforces its own bound
-- area: DML, MERGE, partitioning, rules and the COPY/extended-protocol surface
-- title: A leaf partition is stored as an ordinary table, so nothing enforces its own bound
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_f1 (i int) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_f1_1 PARTITION OF zz_vf_f1 FOR VALUES FROM (1) TO (10);
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zz_vf_f1_1" violates partition constraint
-- end-expected-error
INSERT INTO zz_vf_f1_1 VALUES (99);
-- begin-expected
-- columns: i:int4
-- rowcount: 0
-- end-expected
SELECT i FROM zz_vf_f1 ORDER BY i;
