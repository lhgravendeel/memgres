-- ============================================================================
-- Feature Comparison: RETURNING OLD / RETURNING NEW (PG 18, A5)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG 18 enhances RETURNING for INSERT/UPDATE/DELETE to support:
--   RETURNING OLD.*     — pre-modification values
--   RETURNING NEW.*     — post-modification values
--   RETURNING OLD.col, NEW.col  — mixed
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS ron_test CASCADE;
CREATE SCHEMA ron_test;
SET search_path = ron_test, public;

CREATE TABLE ron_data (id integer PRIMARY KEY, val text, score integer);
INSERT INTO ron_data VALUES (1, 'alpha', 10), (2, 'beta', 20), (3, 'gamma', 30);

-- ============================================================================
-- 1. INSERT RETURNING NEW.*
-- ============================================================================

-- note: For INSERT, NEW.* = the inserted row

-- begin-expected
-- columns: id, val, score
-- row: 4, delta, 40
-- end-expected
INSERT INTO ron_data VALUES (4, 'delta', 40) RETURNING NEW.*;

-- ============================================================================
-- 2. INSERT RETURNING OLD.*
-- ============================================================================

-- note: For INSERT, OLD.* = all NULLs (no previous row)

-- begin-expected
-- columns: id, val, score
-- row:  |  |
-- end-expected
INSERT INTO ron_data VALUES (5, 'epsilon', 50) RETURNING OLD.*;

-- ============================================================================
-- 3. UPDATE RETURNING OLD.*
-- ============================================================================

-- begin-expected
-- columns: id, val, score
-- row: 1, alpha, 10
-- end-expected
UPDATE ron_data SET val = 'ALPHA', score = 100 WHERE id = 1 RETURNING OLD.*;

-- ============================================================================
-- 4. UPDATE RETURNING NEW.*
-- ============================================================================

-- begin-expected
-- columns: id, val, score
-- row: 2, BETA, 200
-- end-expected
UPDATE ron_data SET val = 'BETA', score = 200 WHERE id = 2 RETURNING NEW.*;

-- ============================================================================
-- 5. UPDATE RETURNING mixed OLD.col, NEW.col
-- ============================================================================

-- begin-expected
-- columns: old_val, new_val, old_score, new_score
-- row: gamma, GAMMA, 30, 300
-- end-expected
UPDATE ron_data SET val = 'GAMMA', score = 300 WHERE id = 3
RETURNING OLD.val AS old_val, NEW.val AS new_val, OLD.score AS old_score, NEW.score AS new_score;

-- ============================================================================
-- 6. DELETE RETURNING OLD.*
-- ============================================================================

-- begin-expected
-- columns: id, val, score
-- row: 5, epsilon, 50
-- end-expected
DELETE FROM ron_data WHERE id = 5 RETURNING OLD.*;

-- ============================================================================
-- 7. DELETE RETURNING NEW.*
-- ============================================================================

-- note: For DELETE, NEW.* = all NULLs (row no longer exists)

-- begin-expected
-- columns: id, val, score
-- row:  |  |
-- end-expected
DELETE FROM ron_data WHERE id = 4 RETURNING NEW.*;

-- ============================================================================
-- 8. Bare RETURNING * (backward compatible)
-- ============================================================================

-- note: Bare RETURNING * should work as before (returns the current/affected row)

-- begin-expected
-- columns: id, val, score
-- row: 6, zeta, 60
-- end-expected
INSERT INTO ron_data VALUES (6, 'zeta', 60) RETURNING *;

-- begin-expected
-- columns: id, val, score
-- row: 6, ZETA, 600
-- end-expected
UPDATE ron_data SET val = 'ZETA', score = 600 WHERE id = 6 RETURNING *;

-- begin-expected
-- columns: id, val, score
-- row: 6, ZETA, 600
-- end-expected
DELETE FROM ron_data WHERE id = 6 RETURNING *;

-- ============================================================================
-- 9. UPDATE RETURNING expression with OLD and NEW
-- ============================================================================

INSERT INTO ron_data VALUES (7, 'seven', 70);

-- begin-expected
-- columns: id, score_diff
-- row: 7, 630
-- end-expected
UPDATE ron_data SET score = 700 WHERE id = 7
RETURNING id, NEW.score - OLD.score AS score_diff;

-- ============================================================================
-- 10. UPDATE multiple rows RETURNING OLD/NEW
-- ============================================================================

INSERT INTO ron_data VALUES (8, 'eight', 80), (9, 'nine', 90);

-- begin-expected
-- columns: id, old_score, new_score
-- row: 8, 80, 0
-- row: 9, 90, 0
-- end-expected
UPDATE ron_data SET score = 0 WHERE id IN (8, 9)
RETURNING id, OLD.score AS old_score, NEW.score AS new_score;

-- ============================================================================
-- 11. INSERT RETURNING OLD.col (should be NULL)
-- ============================================================================

-- begin-expected
-- columns: new_val, old_val_is_null
-- row: test, true
-- end-expected
INSERT INTO ron_data VALUES (10, 'test', 100)
RETURNING NEW.val AS new_val, OLD.val IS NULL AS old_val_is_null;

-- ============================================================================
-- 12. DELETE RETURNING NEW.col (should be NULL)
-- ============================================================================

-- begin-expected
-- columns: old_val, new_val_is_null
-- row: test, true
-- end-expected
DELETE FROM ron_data WHERE id = 10
RETURNING OLD.val AS old_val, NEW.val IS NULL AS new_val_is_null;

-- ============================================================================
-- 13. ON CONFLICT DO UPDATE with RETURNING OLD/NEW
-- ============================================================================

CREATE TABLE ron_upsert (id integer PRIMARY KEY, val text, score integer);
INSERT INTO ron_upsert VALUES (1, 'original', 10);

-- begin-expected
-- columns: old_val, new_val
-- row: original, replaced
-- end-expected
INSERT INTO ron_upsert VALUES (1, 'replaced', 99)
ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val, score = EXCLUDED.score
RETURNING OLD.val AS old_val, NEW.val AS new_val;

-- ============================================================================
-- 14. ON CONFLICT DO UPDATE: INSERT path (no conflict)
-- ============================================================================

-- begin-expected
-- columns: new_val, old_is_null
-- row: brand-new, true
-- end-expected
INSERT INTO ron_upsert VALUES (2, 'brand-new', 50)
ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val
RETURNING NEW.val AS new_val, OLD.val IS NULL AS old_is_null;

DROP TABLE ron_upsert;

-- ============================================================================
-- 15. RETURNING OLD/NEW with generated column
-- ============================================================================

CREATE TABLE ron_gen (
  id integer PRIMARY KEY,
  val integer,
  doubled integer GENERATED ALWAYS AS (val * 2) STORED
);

INSERT INTO ron_gen (id, val) VALUES (1, 5);

-- begin-expected
-- columns: old_doubled, new_doubled
-- row: 10, 20
-- end-expected
UPDATE ron_gen SET val = 10 WHERE id = 1
RETURNING OLD.doubled AS old_doubled, NEW.doubled AS new_doubled;

DROP TABLE ron_gen;

-- ============================================================================
-- 16. RETURNING OLD/NEW with DEFAULT values
-- ============================================================================

CREATE TABLE ron_defaults (
  id integer PRIMARY KEY,
  val text DEFAULT 'default_val',
  created_at timestamp DEFAULT now()
);

-- begin-expected
-- columns: new_val, old_val_is_null
-- row: default_val, true
-- end-expected
INSERT INTO ron_defaults (id) VALUES (1)
RETURNING NEW.val AS new_val, OLD.val IS NULL AS old_val_is_null;

DROP TABLE ron_defaults;

-- ============================================================================
-- 17. RETURNING OLD/NEW with NULL update
-- ============================================================================

INSERT INTO ron_data VALUES (11, 'eleven', 110);

-- begin-expected
-- columns: old_val, new_val
-- row: eleven |
-- end-expected
UPDATE ron_data SET val = NULL WHERE id = 11
RETURNING OLD.val AS old_val, NEW.val AS new_val;

-- ============================================================================
-- 18. RETURNING with COALESCE on OLD/NEW
-- ============================================================================

-- begin-expected
-- columns: safe_old, safe_new
-- row: was-null, new-val
-- end-expected
UPDATE ron_data SET val = 'new-val' WHERE id = 11
RETURNING COALESCE(OLD.val, 'was-null') AS safe_old, COALESCE(NEW.val, 'was-null') AS safe_new;

-- ============================================================================
-- 19. MERGE RETURNING OLD/NEW
-- ============================================================================

CREATE TABLE ron_merge_target (id integer PRIMARY KEY, val text);
CREATE TABLE ron_merge_source (id integer PRIMARY KEY, val text);

INSERT INTO ron_merge_target VALUES (1, 'old-val');
INSERT INTO ron_merge_source VALUES (1, 'new-val'), (2, 'inserted');

-- begin-expected
-- columns: id, action, old_val, new_val
-- row: 1 | UPDATE | old-val | new-val
-- row: 2 | INSERT |  | inserted
-- end-expected
MERGE INTO ron_merge_target t
USING ron_merge_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val
WHEN NOT MATCHED THEN
  INSERT VALUES (s.id, s.val)
RETURNING t.id, merge_action() AS action, OLD.val AS old_val, NEW.val AS new_val;

DROP TABLE ron_merge_target, ron_merge_source;

-- ============================================================================
-- 20. INSERT multiple rows RETURNING NEW.*
-- ============================================================================

-- begin-expected
-- columns: id, val, score
-- row: 20, twenty, 200
-- row: 21, twenty-one, 210
-- end-expected
INSERT INTO ron_data VALUES (20, 'twenty', 200), (21, 'twenty-one', 210)
RETURNING NEW.*;

-- ============================================================================
-- 21. DELETE multiple rows RETURNING OLD.*
-- ============================================================================

-- begin-expected
-- columns: id, val, score
-- row: 20, twenty, 200
-- row: 21, twenty-one, 210
-- end-expected
DELETE FROM ron_data WHERE id IN (20, 21) RETURNING OLD.*;

-- ============================================================================
-- 22. RETURNING only OLD columns (no NEW)
-- ============================================================================

INSERT INTO ron_data VALUES (22, 'temp', 220);

-- begin-expected
-- columns: old_id, old_val
-- row: 22, temp
-- end-expected
DELETE FROM ron_data WHERE id = 22 RETURNING OLD.id AS old_id, OLD.val AS old_val;

-- ============================================================================
-- 23. RETURNING only NEW columns (no OLD)
-- ============================================================================

-- begin-expected
-- columns: new_id, new_val
-- row: 23, fresh
-- end-expected
INSERT INTO ron_data VALUES (23, 'fresh', 230)
RETURNING NEW.id AS new_id, NEW.val AS new_val;

DELETE FROM ron_data WHERE id = 23;

-- ============================================================================
-- 24. OLD/NEW with IDENTITY column
-- ============================================================================

CREATE TABLE ron_identity (
  id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  val text
);

INSERT INTO ron_identity (val) VALUES ('first');

-- begin-expected
-- columns: old_id, new_id, old_val, new_val
-- row: 1, 1, first, updated
-- end-expected
UPDATE ron_identity SET val = 'updated' WHERE id = 1
RETURNING OLD.id AS old_id, NEW.id AS new_id, OLD.val AS old_val, NEW.val AS new_val;

-- begin-expected
-- columns: new_id, old_id_is_null
-- row: 2, true
-- end-expected
INSERT INTO ron_identity (val) VALUES ('second')
RETURNING NEW.id AS new_id, OLD.id IS NULL AS old_id_is_null;

DROP TABLE ron_identity;

-- ============================================================================
-- 25. RETURNING OLD.* and NEW.* in same UPDATE
-- ============================================================================

INSERT INTO ron_data VALUES (25, 'before', 250);

-- note: OLD.* and NEW.* together — column names may collide, aliases recommended
-- begin-expected
-- columns: id, val, score, id, val, score
-- row: 25, before, 250, 25, after, 999
-- end-expected
UPDATE ron_data SET val = 'after', score = 999 WHERE id = 25
RETURNING OLD.*, NEW.*;

DELETE FROM ron_data WHERE id = 25;

-- ============================================================================
-- 26. RETURNING OLD/NEW in a CTE
-- ============================================================================

INSERT INTO ron_data VALUES (26, 'orig', 260);

-- begin-expected
-- columns: old_val, new_val, diff
-- row: orig, changed, -160
-- end-expected
WITH changes AS (
  UPDATE ron_data SET val = 'changed', score = 100 WHERE id = 26
  RETURNING OLD.val AS old_val, NEW.val AS new_val, NEW.score - OLD.score AS diff
)
SELECT old_val, new_val, diff FROM changes;

DELETE FROM ron_data WHERE id = 26;

-- ============================================================================
-- 27. ON CONFLICT DO NOTHING with RETURNING OLD/NEW
-- ============================================================================

CREATE TABLE ron_upsert2 (id integer PRIMARY KEY, val text);
INSERT INTO ron_upsert2 VALUES (1, 'existing');

-- note: DO NOTHING produces no RETURNING rows when conflict occurs
-- begin-expected-error
-- message-like: syntax error
-- end-expected-error
SELECT count(*)::integer AS cnt FROM (
  INSERT INTO ron_upsert2 VALUES (1, 'conflict')
  ON CONFLICT (id) DO NOTHING
  RETURNING NEW.*
) sub;

-- Non-conflict path still returns
-- begin-expected
-- columns: id, val
-- row: 2, no-conflict
-- end-expected
INSERT INTO ron_upsert2 VALUES (2, 'no-conflict')
ON CONFLICT (id) DO NOTHING
RETURNING NEW.*;

DROP TABLE ron_upsert2;

-- ============================================================================
-- 28. OLD/NEW with array column
-- ============================================================================

CREATE TABLE ron_arrays (id integer PRIMARY KEY, tags text[]);
INSERT INTO ron_arrays VALUES (1, ARRAY['a','b']);

-- begin-expected
-- columns: old_tags, new_tags
-- row: {a,b}, {x,y,z}
-- end-expected
UPDATE ron_arrays SET tags = ARRAY['x','y','z'] WHERE id = 1
RETURNING OLD.tags AS old_tags, NEW.tags AS new_tags;

DROP TABLE ron_arrays;

-- ============================================================================
-- 29. OLD/NEW with SERIAL column
-- ============================================================================

CREATE TABLE ron_serial (id serial PRIMARY KEY, val text);

-- begin-expected
-- columns: new_id, old_is_null
-- row: 1, true
-- end-expected
INSERT INTO ron_serial (val) VALUES ('auto')
RETURNING NEW.id AS new_id, OLD.id IS NULL AS old_is_null;

DROP TABLE ron_serial;

-- ============================================================================
-- 30. MERGE RETURNING OLD/NEW + WHEN NOT MATCHED BY SOURCE
-- ============================================================================

CREATE TABLE ron_merge2_target (id integer PRIMARY KEY, val text);
CREATE TABLE ron_merge2_source (id integer PRIMARY KEY, val text);
INSERT INTO ron_merge2_target VALUES (1, 'match'), (2, 'orphan');
INSERT INTO ron_merge2_source VALUES (1, 'updated'), (3, 'new');

-- begin-expected
-- columns: id, action, old_val, new_val
-- row: 1 | UPDATE | match | updated
-- row: 2 | DELETE | orphan |
-- row: 3 | INSERT |  | new
-- end-expected
MERGE INTO ron_merge2_target t
USING ron_merge2_source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val
WHEN NOT MATCHED BY TARGET THEN
  INSERT VALUES (s.id, s.val)
WHEN NOT MATCHED BY SOURCE THEN
  DELETE
RETURNING t.id, merge_action() AS action, OLD.val AS old_val, NEW.val AS new_val;

DROP TABLE ron_merge2_target, ron_merge2_source;

-- ============================================================================
-- 31. DELETE RETURNING OLD with expression
-- ============================================================================

INSERT INTO ron_data VALUES (31, 'goodbye', 310);

-- begin-expected
-- columns: msg
-- row: deleted goodbye with score 310
-- end-expected
DELETE FROM ron_data WHERE id = 31
RETURNING 'deleted ' || OLD.val || ' with score ' || OLD.score::text AS msg;

-- ============================================================================
-- 32. UPDATE RETURNING CASE on OLD vs NEW
-- ============================================================================

INSERT INTO ron_data VALUES (32, 'test', 100);

-- begin-expected
-- columns: change_type
-- row: increased
-- end-expected
UPDATE ron_data SET score = 200 WHERE id = 32
RETURNING CASE WHEN NEW.score > OLD.score THEN 'increased'
               WHEN NEW.score < OLD.score THEN 'decreased'
               ELSE 'same' END AS change_type;

DELETE FROM ron_data WHERE id = 32;

-- ============================================================================
-- 33. INSERT RETURNING NEW with DEFAULT column values
-- ============================================================================

CREATE TABLE ron_def2 (
  id integer PRIMARY KEY,
  val text DEFAULT 'hello',
  ts timestamp DEFAULT '2026-01-01 00:00:00'
);

-- begin-expected
-- columns: new_val, new_ts
-- row: hello, 2026-01-01 00:00:00
-- end-expected
INSERT INTO ron_def2 (id) VALUES (1)
RETURNING NEW.val AS new_val, NEW.ts AS new_ts;

DROP TABLE ron_def2;

-- ============================================================================
-- 34. UPDATE ... FROM with RETURNING OLD/NEW
-- ============================================================================

CREATE TABLE ron_prices (id integer PRIMARY KEY, product text, price integer);
CREATE TABLE ron_discounts (product text, discount integer);
INSERT INTO ron_prices VALUES (1, 'apple', 100), (2, 'banana', 200);
INSERT INTO ron_discounts VALUES ('apple', 10), ('banana', 50);

-- begin-expected
-- columns: product, old_price, new_price
-- row: apple, 100, 90
-- row: banana, 200, 150
-- end-expected
UPDATE ron_prices p
SET price = p.price - d.discount
FROM ron_discounts d
WHERE p.product = d.product
RETURNING p.product, OLD.price AS old_price, NEW.price AS new_price;

DROP TABLE ron_prices, ron_discounts;

-- ============================================================================
-- 35. DELETE ... USING with RETURNING OLD.*
-- ============================================================================

CREATE TABLE ron_items (id integer PRIMARY KEY, val text);
CREATE TABLE ron_blacklist (val text);
INSERT INTO ron_items VALUES (1, 'keep'), (2, 'remove'), (3, 'also-remove');
INSERT INTO ron_blacklist VALUES ('remove'), ('also-remove');

-- begin-expected
-- columns: id, val
-- row: 3, also-remove
-- row: 2, remove
-- end-expected
DELETE FROM ron_items i
USING ron_blacklist b
WHERE i.val = b.val
RETURNING OLD.*;

DROP TABLE ron_items, ron_blacklist;

-- ============================================================================
-- 36. UPDATE SET col = DEFAULT RETURNING OLD/NEW
-- ============================================================================

CREATE TABLE ron_defaults3 (
  id integer PRIMARY KEY,
  val text DEFAULT 'reset'
);
INSERT INTO ron_defaults3 VALUES (1, 'custom');

-- begin-expected
-- columns: old_val, new_val
-- row: custom, reset
-- end-expected
UPDATE ron_defaults3 SET val = DEFAULT WHERE id = 1
RETURNING OLD.val AS old_val, NEW.val AS new_val;

DROP TABLE ron_defaults3;

-- ============================================================================
-- 37. RETURNING OLD/NEW with jsonb column
-- ============================================================================

CREATE TABLE ron_jsonb (id integer PRIMARY KEY, data jsonb);
INSERT INTO ron_jsonb VALUES (1, '{"key": "old_value"}');

-- begin-expected
-- columns: old_data, new_data
-- row: {"key": "old_value"}, {"key": "new_value"}
-- end-expected
UPDATE ron_jsonb SET data = '{"key": "new_value"}' WHERE id = 1
RETURNING OLD.data AS old_data, NEW.data AS new_data;

DROP TABLE ron_jsonb;

-- ============================================================================
-- 38. RETURNING OLD/NEW with partitioned table
-- ============================================================================

CREATE TABLE ron_partitioned (id integer, val text, score integer)
  PARTITION BY RANGE (id);

CREATE TABLE ron_part_low PARTITION OF ron_partitioned FOR VALUES FROM (1) TO (100);
CREATE TABLE ron_part_high PARTITION OF ron_partitioned FOR VALUES FROM (100) TO (1000);

INSERT INTO ron_partitioned VALUES (1, 'low', 10), (200, 'high', 20);

-- INSERT into partition via parent, RETURNING NEW.*
-- begin-expected
-- columns: id, val, score
-- row: 50, mid, 50
-- end-expected
INSERT INTO ron_partitioned VALUES (50, 'mid', 50) RETURNING NEW.*;

-- UPDATE across partition boundary concept: update within same partition
-- begin-expected-error
-- message-like: replica identity
-- end-expected-error
UPDATE ron_partitioned SET val = 'LOW-UPDATED' WHERE id = 1
RETURNING OLD.val AS old_val, NEW.val AS new_val;

-- DELETE from high partition
-- begin-expected-error
-- message-like: replica identity
-- end-expected-error
DELETE FROM ron_partitioned WHERE id = 200
RETURNING OLD.val AS old_val;

DROP TABLE ron_partitioned;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA ron_test CASCADE;
SET search_path = public;
