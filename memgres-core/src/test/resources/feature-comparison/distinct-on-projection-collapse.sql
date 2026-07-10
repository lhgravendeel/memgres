-- DISTINCT ON must dedupe only on its key expressions, never additionally apply a plain
-- full-projection DISTINCT pass.
--
-- Regression coverage for mtask-8 Group 4: the parser sets stmt.distinct() = true for
-- DISTINCT ON too, and SelectExecutor.applyDistinct ran an unconditional full-projection
-- DISTINCT whenever stmt.distinct(), merging rows that have distinct DISTINCT ON keys but an
-- incidentally-equal projection (when the key isn't part of the projection).

-- setup
CREATE TABLE don_collapse_t (d date, typ text, charged numeric);
INSERT INTO don_collapse_t VALUES ('2026-01-01', 'auto', 1);
INSERT INTO don_collapse_t VALUES ('2026-01-01', 'manual', 5);
INSERT INTO don_collapse_t VALUES ('2026-01-02', 'auto', 2);
INSERT INTO don_collapse_t VALUES ('2026-01-02', 'manual', 5);
INSERT INTO don_collapse_t VALUES ('2026-01-03', 'manual', 10);

-- stmt 1: DISTINCT ON (d) projects only "charged" -- two different dates project the same
-- value (5); PostgreSQL keeps both rows (3 total), it does not collapse them to 2.
-- begin-expected
-- columns: recent_count
-- row: 3
-- end-expected
SELECT COUNT(*) AS recent_count FROM (
  SELECT DISTINCT ON (d) charged FROM don_collapse_t
  ORDER BY d, CASE typ WHEN 'manual' THEN 0 ELSE 1 END
) daily;

-- cleanup
DROP TABLE don_collapse_t;
