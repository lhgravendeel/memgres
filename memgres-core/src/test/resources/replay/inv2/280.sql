-- source: investigation-2026-08.md
-- finding: 280
-- title: The binary encoder computes timestamps in nanoseconds, so Duration.toNanos() throws for any span over ~292 years, and the catch silently substitutes the value's
-- JDBC url ...?prepareThreshold=1 so pgjdbc asks for binary on the second execution
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_b (ts timestamp);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf2_b VALUES ('1600-06-01 12:00:00'), ('2020-01-01 00:00:00');
-- begin-expected
-- columns: ts:timestamp
-- row: 1600-06-01 12:00:00
-- row: 2020-01-01 00:00:00
-- rowcount: 2
-- end-expected
SELECT ts FROM zz_vf2_b ORDER BY ts;
-- executed three times on one PreparedStatement;
