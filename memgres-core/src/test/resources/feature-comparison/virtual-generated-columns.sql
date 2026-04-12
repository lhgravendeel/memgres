-- ============================================================================
-- Feature Comparison: Virtual Generated Columns (PG 18)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS vg_test CASCADE;
CREATE SCHEMA vg_test;
SET search_path = vg_test, public;

-- ============================================================================
-- 1. Basic VIRTUAL generated column
-- ============================================================================

CREATE TABLE vg_basic (
  id integer PRIMARY KEY,
  a integer,
  b integer,
  total integer GENERATED ALWAYS AS (a + b) VIRTUAL
);

INSERT INTO vg_basic (id, a, b) VALUES (1, 10, 20), (2, 30, 40);

-- begin-expected
-- columns: id, a, b, total
-- row: 1, 10, 20, 30
-- row: 2, 30, 40, 70
-- end-expected
SELECT * FROM vg_basic ORDER BY id;

-- ============================================================================
-- 2. Default is VIRTUAL in PG 18 (omit STORED/VIRTUAL keyword)
-- ============================================================================

-- note: In PG 18, omitting STORED/VIRTUAL defaults to VIRTUAL
CREATE TABLE vg_default (
  id integer PRIMARY KEY,
  val integer,
  doubled integer GENERATED ALWAYS AS (val * 2)
);

INSERT INTO vg_default (id, val) VALUES (1, 5);

-- begin-expected
-- columns: id, val, doubled
-- row: 1, 5, 10
-- end-expected
SELECT * FROM vg_default;

-- ============================================================================
-- 3. STORED vs VIRTUAL on same table
-- ============================================================================

CREATE TABLE vg_mixed (
  id integer PRIMARY KEY,
  a integer,
  stored_sum integer GENERATED ALWAYS AS (a + 100) STORED,
  virtual_sum integer GENERATED ALWAYS AS (a + 200) VIRTUAL
);

INSERT INTO vg_mixed (id, a) VALUES (1, 10);

-- begin-expected
-- columns: id, a, stored_sum, virtual_sum
-- row: 1, 10, 110, 210
-- end-expected
SELECT * FROM vg_mixed;

-- ============================================================================
-- 4. INSERT: reject explicit value for virtual column
-- ============================================================================

-- begin-expected-error
-- message-like: cannot insert a non-DEFAULT value into column "total"
-- end-expected-error
INSERT INTO vg_basic (id, a, b, total) VALUES (3, 1, 2, 3);

-- ============================================================================
-- 5. INSERT: allow DEFAULT for virtual column
-- ============================================================================

INSERT INTO vg_basic (id, a, b, total) VALUES (3, 1, 2, DEFAULT);

-- begin-expected
-- columns: id, total
-- row: 3, 3
-- end-expected
SELECT id, total FROM vg_basic WHERE id = 3;

-- ============================================================================
-- 6. UPDATE: reject explicit value for virtual column
-- ============================================================================

-- begin-expected-error
-- message-like: column "total" can only be updated to DEFAULT
-- end-expected-error
UPDATE vg_basic SET total = 999 WHERE id = 1;

-- ============================================================================
-- 7. UPDATE: SET virtual = DEFAULT is OK
-- ============================================================================

UPDATE vg_basic SET a = 100, total = DEFAULT WHERE id = 1;

-- begin-expected
-- columns: id, a, total
-- row: 1, 100, 120
-- end-expected
SELECT id, a, total FROM vg_basic WHERE id = 1;

-- ============================================================================
-- 8. UPDATE source column: virtual recomputes
-- ============================================================================

UPDATE vg_basic SET a = 50 WHERE id = 1;

-- begin-expected
-- columns: total
-- row: 70
-- end-expected
SELECT total FROM vg_basic WHERE id = 1;

-- ============================================================================
-- 9. WHERE on virtual column
-- ============================================================================

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM vg_basic WHERE total > 50 ORDER BY id;

-- ============================================================================
-- 10. ORDER BY virtual column
-- ============================================================================

-- begin-expected
-- columns: id, total
-- row: 3, 3
-- row: 1, 70
-- row: 2, 70
-- end-expected
SELECT id, total FROM vg_basic ORDER BY total, id;

-- ============================================================================
-- 11. Virtual column with text expression
-- ============================================================================

CREATE TABLE vg_text (
  id integer PRIMARY KEY,
  first_name text,
  last_name text,
  full_name text GENERATED ALWAYS AS (first_name || ' ' || last_name) VIRTUAL
);

INSERT INTO vg_text (id, first_name, last_name) VALUES (1, 'John', 'Doe');

-- begin-expected
-- columns: full_name
-- row: John Doe
-- end-expected
SELECT full_name FROM vg_text WHERE id = 1;

-- ============================================================================
-- 12. Virtual column with CASE expression
-- ============================================================================

CREATE TABLE vg_case (
  id integer PRIMARY KEY,
  val integer,
  category text GENERATED ALWAYS AS (
    CASE WHEN val < 0 THEN 'negative' WHEN val = 0 THEN 'zero' ELSE 'positive' END
  ) VIRTUAL
);

INSERT INTO vg_case (id, val) VALUES (1, -5), (2, 0), (3, 10);

-- begin-expected
-- columns: id, category
-- row: 1, negative
-- row: 2, zero
-- row: 3, positive
-- end-expected
SELECT id, category FROM vg_case ORDER BY id;

-- ============================================================================
-- 13. Virtual column NULL propagation
-- ============================================================================

INSERT INTO vg_basic (id, a, b) VALUES (4, NULL, 10);

-- begin-expected
-- columns: id, total
-- row: 4 |
-- end-expected
SELECT id, total FROM vg_basic WHERE id = 4;

-- ============================================================================
-- 14. CREATE INDEX on virtual column
-- ============================================================================

CREATE INDEX idx_vg_total ON vg_basic (total);

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
SELECT id FROM vg_basic WHERE total = 70 AND id = 2;

DROP INDEX idx_vg_total;

-- ============================================================================
-- 15. UNIQUE index on virtual column
-- ============================================================================

CREATE TABLE vg_unique (
  id integer PRIMARY KEY,
  a integer,
  b integer,
  ab_sum integer GENERATED ALWAYS AS (a + b) VIRTUAL
);

CREATE UNIQUE INDEX idx_vg_unique ON vg_unique (ab_sum);

INSERT INTO vg_unique (id, a, b) VALUES (1, 10, 20);

-- note: PG does not enforce unique indexes on virtual generated columns
INSERT INTO vg_unique (id, a, b) VALUES (2, 15, 15);

DROP TABLE vg_unique;

-- ============================================================================
-- 16. CHECK constraint referencing virtual column
-- ============================================================================

CREATE TABLE vg_check (
  id integer PRIMARY KEY,
  a integer,
  b integer,
  total integer GENERATED ALWAYS AS (a + b) VIRTUAL,
  CHECK (total >= 0)
);

INSERT INTO vg_check (id, a, b) VALUES (1, 10, 20);

-- begin-expected-error
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO vg_check (id, a, b) VALUES (2, -50, 10);

DROP TABLE vg_check;

-- ============================================================================
-- 17. ALTER TABLE ADD COLUMN ... VIRTUAL
-- ============================================================================

CREATE TABLE vg_alter (id integer PRIMARY KEY, val integer);
INSERT INTO vg_alter VALUES (1, 5), (2, 10);

ALTER TABLE vg_alter ADD COLUMN doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL;

-- begin-expected
-- columns: id, val, doubled
-- row: 1, 5, 10
-- row: 2, 10, 20
-- end-expected
SELECT * FROM vg_alter ORDER BY id;

-- ============================================================================
-- 18. RETURNING with virtual column
-- ============================================================================

-- begin-expected
-- columns: id, val, doubled
-- row: 3, 15, 30
-- end-expected
INSERT INTO vg_alter (id, val) VALUES (3, 15) RETURNING *;

-- begin-expected
-- columns: id, doubled
-- row: 1, 100
-- end-expected
UPDATE vg_alter SET val = 50 WHERE id = 1 RETURNING id, doubled;

-- begin-expected
-- columns: id, doubled
-- row: 3, 30
-- end-expected
DELETE FROM vg_alter WHERE id = 3 RETURNING id, doubled;

-- ============================================================================
-- 19. Virtual column in CTE
-- ============================================================================

-- begin-expected
-- columns: id, doubled
-- row: 1, 100
-- row: 2, 20
-- end-expected
WITH d AS (SELECT id, doubled FROM vg_alter)
SELECT * FROM d ORDER BY id;

-- ============================================================================
-- 20. Virtual column in subquery
-- ============================================================================

-- begin-expected
-- columns: max_doubled
-- row: 100
-- end-expected
SELECT max(doubled) AS max_doubled FROM (SELECT doubled FROM vg_alter) sub;

-- ============================================================================
-- 21. Virtual column with aggregate
-- ============================================================================

-- begin-expected
-- columns: sum_doubled
-- row: 120
-- end-expected
SELECT sum(doubled) AS sum_doubled FROM vg_alter;

-- ============================================================================
-- 22. Virtual column in HAVING
-- ============================================================================

-- begin-expected
-- columns: id, doubled
-- row: 1, 100
-- end-expected
SELECT id, doubled
FROM vg_alter
GROUP BY id, doubled
HAVING doubled > 50
ORDER BY id;

-- ============================================================================
-- 23. Virtual column in window function
-- ============================================================================

-- begin-expected
-- columns: id, doubled, running
-- row: 2, 20, 20
-- row: 1, 100, 120
-- end-expected
SELECT id, doubled, sum(doubled) OVER (ORDER BY doubled) AS running
FROM vg_alter
ORDER BY doubled;

-- ============================================================================
-- 24. Volatile function rejection in virtual column
-- ============================================================================

-- begin-expected-error
-- message-like: generation expression is not immutable
-- end-expected-error
CREATE TABLE vg_volatile (
  id integer PRIMARY KEY,
  r double precision GENERATED ALWAYS AS (random()) VIRTUAL
);

-- ============================================================================
-- 25. IMMUTABLE function in virtual column (OK)
-- ============================================================================

CREATE FUNCTION vg_double(x integer) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$ SELECT x * 2 $$;

CREATE TABLE vg_immut (
  id integer PRIMARY KEY,
  val integer,
  doubled integer GENERATED ALWAYS AS (vg_double(val)) VIRTUAL
);

INSERT INTO vg_immut (id, val) VALUES (1, 7);

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT doubled FROM vg_immut;

DROP TABLE vg_immut;

-- ============================================================================
-- 26. STABLE function rejection in virtual column
-- ============================================================================

CREATE FUNCTION vg_stable_fn(x integer) RETURNS integer
LANGUAGE sql STABLE AS $$ SELECT x $$;

-- begin-expected-error
-- message-like: generation expression
-- end-expected-error
CREATE TABLE vg_stable_fail (
  id integer PRIMARY KEY,
  val integer,
  computed integer GENERATED ALWAYS AS (vg_stable_fn(val)) VIRTUAL
);

-- ============================================================================
-- 27. pg_attribute.attgenerated column
-- ============================================================================

-- begin-expected
-- columns: attname, attgenerated
-- row: total, v
-- end-expected
SELECT attname, attgenerated
FROM pg_attribute
WHERE attrelid = 'vg_basic'::regclass AND attname = 'total';

-- Stored column should have 's'
-- begin-expected
-- columns: attname, attgenerated
-- row: stored_sum, s
-- end-expected
SELECT attname, attgenerated
FROM pg_attribute
WHERE attrelid = 'vg_mixed'::regclass AND attname = 'stored_sum';

-- ============================================================================
-- 28. Expression index on virtual column
-- ============================================================================

CREATE TABLE vg_expr_idx (
  id integer PRIMARY KEY,
  val integer,
  doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL
);

CREATE INDEX idx_vg_expr ON vg_expr_idx (doubled);

INSERT INTO vg_expr_idx (id, val) VALUES (1, 5), (2, 10);

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
SELECT id FROM vg_expr_idx WHERE doubled = 20;

DROP TABLE vg_expr_idx CASCADE;

-- ============================================================================
-- 29. Virtual column with mathematical expression
-- ============================================================================

CREATE TABLE vg_math (
  id integer PRIMARY KEY,
  radius numeric(10,2),
  area numeric(20,4) GENERATED ALWAYS AS (3.14159 * radius * radius) VIRTUAL
);

INSERT INTO vg_math (id, radius) VALUES (1, 5.0);

-- begin-expected
-- columns: area
-- row: 78.5398
-- end-expected
SELECT area FROM vg_math;

DROP TABLE vg_math;

-- ============================================================================
-- 30. Virtual column: DEFAULT + GENERATED is invalid
-- ============================================================================

-- begin-expected-error
-- message-like: both default and generation expression specified
-- end-expected-error
CREATE TABLE vg_bad (
  id integer PRIMARY KEY,
  val integer DEFAULT 0 GENERATED ALWAYS AS (1) VIRTUAL
);

-- ============================================================================
-- 31. DELETE WHERE on virtual column
-- ============================================================================

DELETE FROM vg_basic WHERE total < 10;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM vg_basic WHERE total IS NOT NULL;

-- ============================================================================
-- 32. DISTINCT on virtual column
-- ============================================================================

INSERT INTO vg_basic (id, a, b) VALUES (10, 35, 35), (11, 34, 36);

-- begin-expected
-- columns: total
-- row: 70
-- end-expected
SELECT DISTINCT total FROM vg_basic WHERE total = 70;

-- ============================================================================
-- 33. GROUP BY virtual column
-- ============================================================================

-- begin-expected
-- columns: total, cnt
-- row: 70, 4
-- end-expected
SELECT total, count(*) AS cnt
FROM vg_basic
WHERE total = 70
GROUP BY total;

-- ============================================================================
-- 34. ON CONFLICT DO UPDATE with virtual column in WHERE
-- ============================================================================

CREATE TABLE vg_upsert (
  id integer PRIMARY KEY,
  val integer,
  doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL
);

INSERT INTO vg_upsert (id, val) VALUES (1, 5);
INSERT INTO vg_upsert (id, val) VALUES (1, 10)
  ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;

-- begin-expected
-- columns: id, val, doubled
-- row: 1, 10, 20
-- end-expected
SELECT * FROM vg_upsert;

-- ============================================================================
-- 35. Virtual column with type cast
-- ============================================================================

CREATE TABLE vg_cast (
  id integer PRIMARY KEY,
  price numeric(10,2),
  price_text text GENERATED ALWAYS AS (price::text || ' USD') VIRTUAL
);

INSERT INTO vg_cast (id, price) VALUES (1, 19.99);

-- begin-expected
-- columns: price_text
-- row: 19.99 USD
-- end-expected
SELECT price_text FROM vg_cast;

DROP TABLE vg_cast;

-- ============================================================================
-- 36. VIRTUAL referencing another VIRTUAL column (should error)
-- ============================================================================

-- note: A generated column cannot reference another generated column
-- begin-expected-error
-- message-like: generated column
-- end-expected-error
CREATE TABLE vg_chain (
  id integer PRIMARY KEY,
  a integer,
  b integer GENERATED ALWAYS AS (a * 2) VIRTUAL,
  c integer GENERATED ALWAYS AS (b + 1) VIRTUAL
);

-- ============================================================================
-- 37. VIRTUAL with subquery (should error)
-- ============================================================================

-- begin-expected-error
-- message-like: cannot use subquery
-- end-expected-error
CREATE TABLE vg_subquery (
  id integer PRIMARY KEY,
  val integer GENERATED ALWAYS AS ((SELECT 1)) VIRTUAL
);

-- ============================================================================
-- 38. DROP source column that VIRTUAL depends on
-- ============================================================================

CREATE TABLE vg_drop_dep (
  id integer PRIMARY KEY,
  a integer,
  b integer GENERATED ALWAYS AS (a * 2) VIRTUAL
);

-- begin-expected-error
-- message-like: other objects depend on it
-- end-expected-error
ALTER TABLE vg_drop_dep DROP COLUMN a;

-- CASCADE should drop both
ALTER TABLE vg_drop_dep DROP COLUMN a CASCADE;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM pg_attribute
WHERE attrelid = 'vg_drop_dep'::regclass AND attname = 'b' AND NOT attisdropped;

DROP TABLE vg_drop_dep;

-- ============================================================================
-- 39. VIRTUAL column in JOIN condition
-- ============================================================================

CREATE TABLE vg_join_a (id integer PRIMARY KEY, val integer,
  doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL);
CREATE TABLE vg_join_b (id integer PRIMARY KEY, score integer);

INSERT INTO vg_join_a (id, val) VALUES (1, 5), (2, 10);
INSERT INTO vg_join_b VALUES (1, 10), (2, 20), (3, 30);

-- begin-expected
-- columns: a_id, b_id
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT a.id AS a_id, b.id AS b_id
FROM vg_join_a a JOIN vg_join_b b ON a.doubled = b.score
ORDER BY a.id;

DROP TABLE vg_join_a, vg_join_b;

-- ============================================================================
-- 40. INSERT ... SELECT with VIRTUAL column target
-- ============================================================================

CREATE TABLE vg_ins_src (id integer, val integer);
INSERT INTO vg_ins_src VALUES (1, 5), (2, 10);

CREATE TABLE vg_ins_tgt (
  id integer PRIMARY KEY,
  val integer,
  doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL
);

-- note: Must select only non-generated columns
INSERT INTO vg_ins_tgt (id, val) SELECT id, val FROM vg_ins_src;

-- begin-expected
-- columns: id, val, doubled
-- row: 1, 5, 10
-- row: 2, 10, 20
-- end-expected
SELECT * FROM vg_ins_tgt ORDER BY id;

DROP TABLE vg_ins_src, vg_ins_tgt;

-- ============================================================================
-- 41. Division by zero in VIRTUAL expression (runtime error)
-- ============================================================================

CREATE TABLE vg_divzero (
  id integer PRIMARY KEY,
  a integer,
  b integer,
  ratio integer GENERATED ALWAYS AS (a / b) VIRTUAL
);

INSERT INTO vg_divzero (id, a, b) VALUES (1, 10, 2);

-- begin-expected
-- columns: ratio
-- row: 5
-- end-expected
SELECT ratio FROM vg_divzero WHERE id = 1;

INSERT INTO vg_divzero (id, a, b) VALUES (2, 10, 0);

-- note: Division by zero occurs at read time for VIRTUAL columns
-- begin-expected-error
-- message-like: division by zero
-- end-expected-error
SELECT ratio FROM vg_divzero WHERE id = 2;

DROP TABLE vg_divzero;

-- ============================================================================
-- 42. VIRTUAL column in DISTINCT ON
-- ============================================================================

CREATE TABLE vg_distinct (
  id integer PRIMARY KEY,
  category text,
  val integer,
  doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL
);

INSERT INTO vg_distinct (id, category, val) VALUES
  (1, 'A', 10), (2, 'A', 20), (3, 'B', 30);

-- begin-expected
-- columns: category, doubled
-- row: A, 20
-- row: B, 60
-- end-expected
SELECT DISTINCT ON (category) category, doubled
FROM vg_distinct ORDER BY category, id;

DROP TABLE vg_distinct;

-- ============================================================================
-- 43. VIRTUAL column in partial index WHERE clause
-- ============================================================================

CREATE TABLE vg_partial (
  id integer PRIMARY KEY,
  val integer,
  doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL
);

CREATE INDEX idx_vg_partial ON vg_partial (id) WHERE doubled > 20;

INSERT INTO vg_partial (id, val) VALUES (1, 5), (2, 15), (3, 25);

-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM vg_partial WHERE doubled > 20 ORDER BY id;

DROP TABLE vg_partial;

-- ============================================================================
-- 44. VIRTUAL column with array type expression
-- ============================================================================

CREATE TABLE vg_array (
  id integer PRIMARY KEY,
  a integer,
  b integer,
  pair integer[] GENERATED ALWAYS AS (ARRAY[a, b]) VIRTUAL
);

INSERT INTO vg_array (id, a, b) VALUES (1, 10, 20);

-- begin-expected
-- columns: pair
-- row: {10,20}
-- end-expected
SELECT pair FROM vg_array;

DROP TABLE vg_array;

-- ============================================================================
-- 45. VIRTUAL in UNION query
-- ============================================================================

CREATE TABLE vg_union1 (id integer PRIMARY KEY, val integer,
  doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL);
CREATE TABLE vg_union2 (id integer PRIMARY KEY, val integer,
  doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL);

INSERT INTO vg_union1 (id, val) VALUES (1, 5);
INSERT INTO vg_union2 (id, val) VALUES (2, 10);

-- begin-expected
-- columns: id, doubled
-- row: 1, 10
-- row: 2, 20
-- end-expected
SELECT id, doubled FROM vg_union1
UNION ALL
SELECT id, doubled FROM vg_union2
ORDER BY id;

DROP TABLE vg_union1, vg_union2;

-- ============================================================================
-- 46. VIRTUAL column NOT NULL constraint
-- ============================================================================

CREATE TABLE vg_notnull (
  id integer PRIMARY KEY,
  val integer NOT NULL,
  doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL
);

-- val NOT NULL ensures doubled is never null
INSERT INTO vg_notnull (id, val) VALUES (1, 5);

-- begin-expected
-- columns: doubled
-- row: 10
-- end-expected
SELECT doubled FROM vg_notnull;

DROP TABLE vg_notnull;

-- ============================================================================
-- 47. VIRTUAL column with boolean expression
-- ============================================================================

CREATE TABLE vg_bool (
  id integer PRIMARY KEY,
  score integer,
  passed boolean GENERATED ALWAYS AS (score >= 60) VIRTUAL
);

INSERT INTO vg_bool (id, score) VALUES (1, 80), (2, 40);

-- begin-expected
-- columns: id, passed
-- row: 1, true
-- row: 2, false
-- end-expected
SELECT id, passed FROM vg_bool ORDER BY id;

DROP TABLE vg_bool;

-- ============================================================================
-- 48. VIRTUAL column in MERGE target
-- ============================================================================

CREATE TABLE vg_merge_tgt (
  id integer PRIMARY KEY,
  val integer,
  doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL
);
CREATE TABLE vg_merge_src (id integer PRIMARY KEY, val integer);

INSERT INTO vg_merge_tgt (id, val) VALUES (1, 5);
INSERT INTO vg_merge_src VALUES (1, 10), (2, 20);

MERGE INTO vg_merge_tgt t
USING vg_merge_src s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET val = s.val
WHEN NOT MATCHED THEN
  INSERT (id, val) VALUES (s.id, s.val);

-- begin-expected
-- columns: id, val, doubled
-- row: 1, 10, 20
-- row: 2, 20, 40
-- end-expected
SELECT * FROM vg_merge_tgt ORDER BY id;

DROP TABLE vg_merge_tgt, vg_merge_src;

-- ============================================================================
-- 49. ALTER source column TYPE affecting VIRTUAL dependency
-- ============================================================================

CREATE TABLE vg_alter_type (
  id integer PRIMARY KEY,
  a integer,
  doubled integer GENERATED ALWAYS AS (a * 2) VIRTUAL
);
INSERT INTO vg_alter_type (id, a) VALUES (1, 5);

-- Changing the source column type should fail if VIRTUAL depends on it
-- begin-expected-error
-- message-like: cannot alter type
-- end-expected-error
ALTER TABLE vg_alter_type ALTER COLUMN a TYPE text;

-- Verify table still works
-- begin-expected
-- columns: doubled
-- row: 10
-- end-expected
SELECT doubled FROM vg_alter_type WHERE id = 1;

DROP TABLE vg_alter_type;

-- ============================================================================
-- 50. VIRTUAL column with COALESCE and NULLIF
-- ============================================================================

CREATE TABLE vg_coalesce (
  id integer PRIMARY KEY,
  a integer,
  b integer,
  safe_ratio numeric GENERATED ALWAYS AS (a::numeric / NULLIF(b, 0)) VIRTUAL,
  val_or_default integer GENERATED ALWAYS AS (COALESCE(a, 0) + COALESCE(b, 0)) VIRTUAL
);

INSERT INTO vg_coalesce (id, a, b) VALUES (1, 10, 2), (2, 10, 0), (3, NULL, 5);

-- begin-expected
-- columns: id, safe_ratio, val_or_default
-- row: 1, 5, 12
-- row: 2 |  | 10
-- row: 3 |  | 5
-- end-expected
SELECT id, safe_ratio, val_or_default FROM vg_coalesce ORDER BY id;

DROP TABLE vg_coalesce;

-- ============================================================================
-- 51. FK referencing VIRTUAL column (should error)
-- ============================================================================

CREATE TABLE vg_fk_parent (
  id integer PRIMARY KEY,
  val integer,
  doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL
);

-- note: Cannot create FK referencing a virtual generated column
-- begin-expected-error
-- message-like: no unique constraint
-- end-expected-error
CREATE TABLE vg_fk_child (
  id integer PRIMARY KEY,
  ref integer REFERENCES vg_fk_parent(doubled)
);

DROP TABLE vg_fk_parent;

-- ============================================================================
-- 52. VIRTUAL column in CREATE TABLE AS / SELECT INTO
-- ============================================================================

CREATE TABLE vg_ctas_src (
  id integer PRIMARY KEY,
  val integer,
  doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL
);
INSERT INTO vg_ctas_src (id, val) VALUES (1, 5), (2, 10);

-- note: CREATE TABLE AS materializes the virtual column as a regular column
CREATE TABLE vg_ctas_copy AS SELECT * FROM vg_ctas_src;

-- begin-expected
-- columns: id, val, doubled
-- row: 1, 5, 10
-- row: 2, 10, 20
-- end-expected
SELECT * FROM vg_ctas_copy ORDER BY id;

-- The new table's "doubled" should be a regular column, not generated
-- begin-expected
-- columns: attgenerated
-- row:
-- end-expected
SELECT attgenerated FROM pg_attribute
WHERE attrelid = 'vg_ctas_copy'::regclass AND attname = 'doubled';

DROP TABLE vg_ctas_src, vg_ctas_copy;

-- ============================================================================
-- 53. VIRTUAL column in COPY TO
-- ============================================================================

CREATE TABLE vg_copy (
  id integer PRIMARY KEY,
  val integer,
  doubled integer GENERATED ALWAYS AS (val * 2) VIRTUAL
);
INSERT INTO vg_copy (id, val) VALUES (1, 5), (2, 10);

-- note: COPY TO should include the computed virtual column values
-- command: COPY 2
COPY vg_copy TO STDOUT;

-- Verify the data is correct via SELECT (COPY output goes to stdout)
-- begin-expected
-- columns: id, doubled
-- row: 1, 10
-- row: 2, 20
-- end-expected
SELECT id, doubled FROM vg_copy ORDER BY id;

DROP TABLE vg_copy;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA vg_test CASCADE;
SET search_path = public;
