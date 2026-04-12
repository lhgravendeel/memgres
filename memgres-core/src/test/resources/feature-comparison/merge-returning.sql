-- ============================================================================
-- Feature Comparison: MERGE RETURNING & WHEN NOT MATCHED BY SOURCE (A2)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG 17 added MERGE ... RETURNING and WHEN NOT MATCHED BY SOURCE.
-- PG 18 added merge_action() and RETURNING OLD/NEW for MERGE.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS mr_test CASCADE;
CREATE SCHEMA mr_test;
SET search_path = mr_test, public;

CREATE TABLE mr_target (id integer PRIMARY KEY, val text, score integer);
CREATE TABLE mr_source (id integer PRIMARY KEY, val text, score integer);

-- ============================================================================
-- 1. Basic MERGE: INSERT, UPDATE, DELETE actions
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'old', 10), (2, 'delete-me', 20);
INSERT INTO mr_source VALUES (1, 'updated', 15), (3, 'new', 30);

MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED AND s.val = 'updated' THEN
  UPDATE SET val = s.val, score = s.score
WHEN NOT MATCHED THEN
  INSERT (id, val, score) VALUES (s.id, s.val, s.score);

-- begin-expected
-- columns: id, val, score
-- row: 1, updated, 15
-- row: 2, delete-me, 20
-- row: 3, new, 30
-- end-expected
SELECT * FROM mr_target ORDER BY id;

-- ============================================================================
-- 2. MERGE ... RETURNING *
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'old', 10);
INSERT INTO mr_source VALUES (1, 'updated', 15), (2, 'new', 20);

-- begin-expected
-- columns: id, val, score, id, val, score
-- row: 1, updated, 15, 1, updated, 15
-- row: 2, new, 20, 2, new, 20
-- end-expected
MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val, score = s.score
WHEN NOT MATCHED THEN
  INSERT (id, val, score) VALUES (s.id, s.val, s.score)
RETURNING *;

-- ============================================================================
-- 3. MERGE RETURNING specific columns
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'old', 10);
INSERT INTO mr_source VALUES (1, 'new', 99);

-- begin-expected
-- columns: id, val
-- row: 1, new
-- end-expected
MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val, score = s.score
RETURNING t.id, t.val;

-- ============================================================================
-- 4. MERGE RETURNING with expression
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'a', 10);
INSERT INTO mr_source VALUES (1, 'b', 20);

-- begin-expected
-- columns: id, doubled
-- row: 1, 40
-- end-expected
MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val, score = s.score
RETURNING t.id, t.score * 2 AS doubled;

-- ============================================================================
-- 5. WHEN NOT MATCHED BY SOURCE — UPDATE
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'match', 10), (2, 'orphan', 20), (3, 'orphan2', 30);
INSERT INTO mr_source VALUES (1, 'match-src', 15);

MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val, score = s.score
WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET val = 'no-source';

-- begin-expected
-- columns: id, val, score
-- row: 1, match-src, 15
-- row: 2, no-source, 20
-- row: 3, no-source, 30
-- end-expected
SELECT * FROM mr_target ORDER BY id;

-- ============================================================================
-- 6. WHEN NOT MATCHED BY SOURCE — DELETE
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'match', 10), (2, 'orphan', 20);
INSERT INTO mr_source VALUES (1, 'src', 15);

MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val
WHEN NOT MATCHED BY SOURCE THEN
  DELETE;

-- begin-expected
-- columns: id, val
-- row: 1, src
-- end-expected
SELECT id, val FROM mr_target ORDER BY id;

-- ============================================================================
-- 7. WHEN NOT MATCHED BY SOURCE — DO NOTHING
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'match', 10), (2, 'orphan', 20);
INSERT INTO mr_source VALUES (1, 'src', 15);

MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val
WHEN NOT MATCHED BY SOURCE THEN
  DO NOTHING;

-- begin-expected
-- columns: id, val
-- row: 1, src
-- row: 2, orphan
-- end-expected
SELECT id, val FROM mr_target ORDER BY id;

-- ============================================================================
-- 8. WHEN NOT MATCHED BY TARGET (explicit form)
-- ============================================================================

-- note: WHEN NOT MATCHED BY TARGET is equivalent to WHEN NOT MATCHED
TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_source VALUES (1, 'new', 10);

MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN NOT MATCHED BY TARGET THEN
  INSERT (id, val, score) VALUES (s.id, s.val, s.score);

-- begin-expected
-- columns: id, val, score
-- row: 1, new, 10
-- end-expected
SELECT * FROM mr_target;

-- ============================================================================
-- 9. merge_action() function
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'old', 10);
INSERT INTO mr_source VALUES (1, 'updated', 15), (2, 'new', 20);

-- begin-expected
-- columns: id, action
-- row: 1, UPDATE
-- row: 2, INSERT
-- end-expected
MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val, score = s.score
WHEN NOT MATCHED THEN
  INSERT (id, val, score) VALUES (s.id, s.val, s.score)
RETURNING t.id, merge_action() AS action;

-- ============================================================================
-- 10. merge_action() with DELETE
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'keep', 10), (2, 'remove', 20);
INSERT INTO mr_source VALUES (1, 'keep-src', 15);

-- begin-expected
-- columns: id, action
-- row: 1, UPDATE
-- row: 2, DELETE
-- end-expected
MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val
WHEN NOT MATCHED BY SOURCE THEN
  DELETE
RETURNING t.id, merge_action() AS action;

-- ============================================================================
-- 11. DO NOTHING produces no RETURNING rows
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'a', 10);
INSERT INTO mr_source VALUES (1, 'b', 20);

-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT count(*) AS cnt FROM (
  MERGE INTO mr_target t
  USING mr_source s ON t.id = s.id
  WHEN MATCHED THEN
    DO NOTHING
  RETURNING *
) sub;

-- ============================================================================
-- 12. WITH (CTE) as MERGE source
-- ============================================================================

TRUNCATE mr_target;
INSERT INTO mr_target VALUES (1, 'old', 10);

-- begin-expected
-- columns: id, val
-- row: 1, from-cte
-- end-expected
WITH src AS (SELECT 1 AS id, 'from-cte' AS val, 99 AS score)
MERGE INTO mr_target t
USING src s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val, score = s.score
RETURNING t.id, t.val;

-- ============================================================================
-- 13. MERGE with VALUES as source
-- ============================================================================

TRUNCATE mr_target;

MERGE INTO mr_target t
USING (VALUES (1, 'a', 10), (2, 'b', 20)) AS s(id, val, score) ON t.id = s.id
WHEN NOT MATCHED THEN
  INSERT (id, val, score) VALUES (s.id, s.val, s.score);

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM mr_target;

-- ============================================================================
-- 14. MERGE with multiple WHEN MATCHED clauses
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'low', 5), (2, 'high', 50);
INSERT INTO mr_source VALUES (1, 'src1', 100), (2, 'src2', 200);

MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED AND t.score < 10 THEN
  UPDATE SET val = 'was-low', score = s.score
WHEN MATCHED THEN
  UPDATE SET val = 'was-high', score = s.score;

-- begin-expected
-- columns: id, val
-- row: 1, was-low
-- row: 2, was-high
-- end-expected
SELECT id, val FROM mr_target ORDER BY id;

-- ============================================================================
-- 15. MERGE: no rows affected
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;

-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT count(*) AS cnt FROM (
  MERGE INTO mr_target t
  USING mr_source s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET val = s.val
  RETURNING *
) sub;

-- ============================================================================
-- 16. MERGE RETURNING with NULL values
-- ============================================================================

TRUNCATE mr_target;
INSERT INTO mr_source VALUES (10, NULL, NULL);

-- begin-expected
-- columns: id, val_is_null
-- row: 10, true
-- end-expected
MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN NOT MATCHED THEN
  INSERT (id, val, score) VALUES (s.id, s.val, s.score)
RETURNING t.id, t.val IS NULL AS val_is_null;

-- ============================================================================
-- 17. MERGE with subquery source
-- ============================================================================

TRUNCATE mr_target;

MERGE INTO mr_target t
USING (SELECT id, val, score FROM mr_source WHERE score IS NULL) s ON t.id = s.id
WHEN NOT MATCHED THEN
  INSERT VALUES (s.id, s.val, s.score);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::integer AS cnt FROM mr_target;

-- ============================================================================
-- 18. MERGE RETURNING with COALESCE
-- ============================================================================

TRUNCATE mr_target;
INSERT INTO mr_source VALUES (20, NULL, 50);

-- begin-expected
-- columns: id, safe_val
-- row: 10, unknown
-- row: 20, unknown
-- end-expected
MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN NOT MATCHED THEN
  INSERT VALUES (s.id, s.val, s.score)
RETURNING t.id, COALESCE(t.val, 'unknown') AS safe_val;

-- ============================================================================
-- 19. MERGE as CTE source (writable CTE)
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'old', 10);
INSERT INTO mr_source VALUES (1, 'updated', 99), (2, 'new', 50);

-- begin-expected
-- columns: id, val
-- row: 1, updated
-- row: 2, new
-- end-expected
WITH merged AS (
  MERGE INTO mr_target t
  USING mr_source s ON t.id = s.id
  WHEN MATCHED THEN
    UPDATE SET val = s.val, score = s.score
  WHEN NOT MATCHED THEN
    INSERT (id, val, score) VALUES (s.id, s.val, s.score)
  RETURNING t.id, t.val
)
SELECT id, val FROM merged ORDER BY id;

-- ============================================================================
-- 20. Conditional WHEN NOT MATCHED BY SOURCE
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'active', 10), (2, 'inactive', 20), (3, 'inactive', 30);
INSERT INTO mr_source VALUES (1, 'src', 15);

MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val
WHEN NOT MATCHED BY SOURCE AND t.val = 'inactive' THEN
  DELETE;

-- begin-expected
-- columns: id, val
-- row: 1, src
-- end-expected
SELECT id, val FROM mr_target ORDER BY id;

-- ============================================================================
-- 21. Multiple WHEN NOT MATCHED BY SOURCE clauses
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'match', 10), (2, 'low', 5), (3, 'high', 50);
INSERT INTO mr_source VALUES (1, 'src', 15);

MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val
WHEN NOT MATCHED BY SOURCE AND t.score < 10 THEN
  DELETE
WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET val = 'orphan';

-- begin-expected
-- columns: id, val, score
-- row: 1, src, 10
-- row: 3, orphan, 50
-- end-expected
SELECT * FROM mr_target ORDER BY id;

-- ============================================================================
-- 22. MERGE self-join (source = target)
-- ============================================================================

TRUNCATE mr_target;
INSERT INTO mr_target VALUES (1, 'a', 10), (2, 'b', 20);

MERGE INTO mr_target t
USING (SELECT id, val, score FROM mr_target WHERE score > 10) s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = 'self-updated';

-- begin-expected
-- columns: id, val
-- row: 1, a
-- row: 2, self-updated
-- end-expected
SELECT id, val FROM mr_target ORDER BY id;

-- ============================================================================
-- 23. MERGE RETURNING with type cast
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'old', 10);
INSERT INTO mr_source VALUES (1, 'new', 99);

-- begin-expected
-- columns: id_text, score_float
-- row: 1, 99.0
-- end-expected
MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val, score = s.score
RETURNING t.id::text AS id_text, t.score::float AS score_float;

-- ============================================================================
-- 24. MERGE RETURNING with aggregate via CTE
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_source VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30);

-- begin-expected
-- columns: cnt, total
-- row: 3, 60
-- end-expected
WITH inserted AS (
  MERGE INTO mr_target t
  USING mr_source s ON t.id = s.id
  WHEN NOT MATCHED THEN
    INSERT (id, val, score) VALUES (s.id, s.val, s.score)
  RETURNING t.score
)
SELECT count(*)::integer AS cnt, sum(score)::integer AS total FROM inserted;

-- ============================================================================
-- 25. MERGE with merge_action() and WHEN NOT MATCHED BY SOURCE
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'keep', 10), (2, 'update-me', 20), (3, 'orphan', 30);
INSERT INTO mr_source VALUES (2, 'updated', 25);

-- begin-expected
-- columns: id, action
-- row: 2, UPDATE
-- row: 3, DELETE
-- end-expected
MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val, score = s.score
WHEN NOT MATCHED BY SOURCE AND t.id > 1 THEN
  DELETE
RETURNING t.id, merge_action() AS action;

-- ============================================================================
-- 26. Error: MERGE RETURNING non-existent column
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_source VALUES (1, 'a', 10);

-- begin-expected-error
-- message-like: column
-- end-expected-error
MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN NOT MATCHED THEN
  INSERT (id, val, score) VALUES (s.id, s.val, s.score)
RETURNING t.nonexistent;

-- ============================================================================
-- 27. MERGE with all three clause types + RETURNING
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'match', 10), (2, 'orphan', 20);
INSERT INTO mr_source VALUES (1, 'updated', 15), (3, 'brand-new', 30);

-- begin-expected
-- columns: id, action
-- row: 1, UPDATE
-- row: 2, DELETE
-- row: 3, INSERT
-- end-expected
MERGE INTO mr_target t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val, score = s.score
WHEN NOT MATCHED BY TARGET THEN
  INSERT (id, val, score) VALUES (s.id, s.val, s.score)
WHEN NOT MATCHED BY SOURCE THEN
  DELETE
RETURNING t.id, merge_action() AS action;

-- ============================================================================
-- 28. MERGE with multi-table USING (JOIN source)
-- ============================================================================

CREATE TABLE mr_extra (id integer PRIMARY KEY, bonus integer);
INSERT INTO mr_extra VALUES (1, 100), (3, 300);

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'old', 10);
INSERT INTO mr_source VALUES (1, 'new', 20), (3, 'new3', 30);

-- begin-expected
-- columns: id, val, score
-- row: 1, new, 120
-- row: 3, new3, 330
-- end-expected
MERGE INTO mr_target t
USING (SELECT s.id, s.val, s.score + e.bonus AS score
       FROM mr_source s JOIN mr_extra e ON s.id = e.id) src
ON t.id = src.id
WHEN MATCHED THEN
  UPDATE SET val = src.val, score = src.score
WHEN NOT MATCHED THEN
  INSERT (id, val, score) VALUES (src.id, src.val, src.score)
RETURNING t.*;

DROP TABLE mr_extra;

-- ============================================================================
-- 29. MERGE with CHECK constraint on target
-- ============================================================================

CREATE TABLE mr_checked (
  id integer PRIMARY KEY,
  val text,
  score integer CHECK (score >= 0)
);
INSERT INTO mr_checked VALUES (1, 'old', 10);

-- Valid update: respects CHECK
TRUNCATE mr_source;
INSERT INTO mr_source VALUES (1, 'updated', 50);

MERGE INTO mr_checked t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val, score = s.score;

-- begin-expected
-- columns: score
-- row: 50
-- end-expected
SELECT score FROM mr_checked WHERE id = 1;

-- Invalid update: violates CHECK
TRUNCATE mr_source;
INSERT INTO mr_source VALUES (1, 'bad', -10);

-- begin-expected-error
-- message-like: violates check constraint
-- end-expected-error
MERGE INTO mr_checked t
USING mr_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val, score = s.score;

DROP TABLE mr_checked;

-- ============================================================================
-- 30. MERGE with expression-based ON condition
-- ============================================================================

TRUNCATE mr_target;
TRUNCATE mr_source;
INSERT INTO mr_target VALUES (1, 'a', 10), (2, 'b', 20);
INSERT INTO mr_source VALUES (2, 'src-for-1', 99), (3, 'src-for-2', 88);

-- note: ON condition uses expression (s.id - 1) rather than simple equality
-- begin-expected
-- columns: id, val
-- row: 1, src-for-1
-- row: 2, src-for-2
-- end-expected
MERGE INTO mr_target t
USING mr_source s ON t.id = s.id - 1
WHEN MATCHED THEN
  UPDATE SET val = s.val, score = s.score
RETURNING t.id, t.val;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA mr_test CASCADE;
SET search_path = public;
