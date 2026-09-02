-- source: review-2026-08.md
-- finding: Root cause 3: the binary encoder computes timestamps in nanoseconds and silently answers with text when that overflows
-- area: Catalog builders and the wire layer, second pass
-- title: Root cause 3: the binary encoder computes timestamps in nanoseconds and silently answers with text when that overflows
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
