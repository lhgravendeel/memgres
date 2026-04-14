-- ============================================================================
-- Feature Comparison: Recursive CTE SEARCH and CYCLE Clauses
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests SEARCH BREADTH FIRST BY, SEARCH DEPTH FIRST BY, and CYCLE detection
-- clauses for recursive CTEs (PG 14+).
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS rcte_test CASCADE;
CREATE SCHEMA rcte_test;
SET search_path = rcte_test, public;

-- Tree structure: 1 -> {2, 3}, 2 -> {4, 5}, 3 -> {6}
CREATE TABLE rcte_tree (parent_id integer, child_id integer, label text);
INSERT INTO rcte_tree VALUES
  (NULL, 1, 'root'),
  (1, 2, 'left'),
  (1, 3, 'right'),
  (2, 4, 'left-left'),
  (2, 5, 'left-right'),
  (3, 6, 'right-left');

-- Graph with cycle: 1->2->3->1, 3->4->5
CREATE TABLE rcte_graph (src integer, dst integer);
INSERT INTO rcte_graph VALUES
  (1, 2), (2, 3), (3, 1),  -- cycle
  (3, 4), (4, 5);           -- branch

-- ============================================================================
-- SECTION A: Basic recursive CTE (baseline)
-- ============================================================================

-- ============================================================================
-- 1. Basic tree traversal without SEARCH/CYCLE
-- ============================================================================

-- begin-expected
-- columns: id, label, depth
-- row: 1, root, 0
-- row: 2, left, 1
-- row: 3, right, 1
-- row: 4, left-left, 2
-- row: 5, left-right, 2
-- row: 6, right-left, 2
-- end-expected
WITH RECURSIVE tree(id, label, depth) AS (
  SELECT child_id, label, 0 FROM rcte_tree WHERE parent_id IS NULL
  UNION ALL
  SELECT t.child_id, t.label, tr.depth + 1
  FROM rcte_tree t JOIN tree tr ON t.parent_id = tr.id
)
SELECT id, label, depth FROM tree ORDER BY depth, id;

-- ============================================================================
-- SECTION B: SEARCH BREADTH FIRST
-- ============================================================================

-- ============================================================================
-- 2. SEARCH BREADTH FIRST BY single column
-- ============================================================================

-- note: Breadth-first ordering visits all nodes at depth N before depth N+1
-- begin-expected
-- columns: id, label, depth
-- row: 1, root, 0
-- row: 2, left, 1
-- row: 3, right, 1
-- row: 4, left-left, 2
-- row: 5, left-right, 2
-- row: 6, right-left, 2
-- end-expected
WITH RECURSIVE tree(id, label, depth) AS (
  SELECT child_id, label, 0 FROM rcte_tree WHERE parent_id IS NULL
  UNION ALL
  SELECT t.child_id, t.label, tr.depth + 1
  FROM rcte_tree t JOIN tree tr ON t.parent_id = tr.id
)
SEARCH BREADTH FIRST BY id SET ordcol
SELECT id, label, depth FROM tree ORDER BY ordcol;

-- ============================================================================
-- 3. SEARCH BREADTH FIRST: ordcol is accessible
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 6
-- end-expected
WITH RECURSIVE tree(id, label, depth) AS (
  SELECT child_id, label, 0 FROM rcte_tree WHERE parent_id IS NULL
  UNION ALL
  SELECT t.child_id, t.label, tr.depth + 1
  FROM rcte_tree t JOIN tree tr ON t.parent_id = tr.id
)
SEARCH BREADTH FIRST BY id SET ordcol
SELECT count(*)::integer AS cnt FROM tree;

-- ============================================================================
-- SECTION C: SEARCH DEPTH FIRST
-- ============================================================================

-- ============================================================================
-- 4. SEARCH DEPTH FIRST BY single column
-- ============================================================================

-- note: Depth-first follows each branch fully before backtracking
-- begin-expected
-- columns: id, label
-- row: 1, root
-- row: 2, left
-- row: 4, left-left
-- row: 5, left-right
-- row: 3, right
-- row: 6, right-left
-- end-expected
WITH RECURSIVE tree(id, label, depth) AS (
  SELECT child_id, label, 0 FROM rcte_tree WHERE parent_id IS NULL
  UNION ALL
  SELECT t.child_id, t.label, tr.depth + 1
  FROM rcte_tree t JOIN tree tr ON t.parent_id = tr.id
)
SEARCH DEPTH FIRST BY id SET ordcol
SELECT id, label FROM tree ORDER BY ordcol;

-- ============================================================================
-- 5. SEARCH DEPTH FIRST: verify ordering is different from breadth-first
-- ============================================================================

-- begin-expected
-- columns: bf_order, df_order
-- row: false, false
-- end-expected
SELECT
  (SELECT string_agg(id::text, ',' ORDER BY ordcol) FROM (
    WITH RECURSIVE tree(id, label) AS (
      SELECT child_id, label FROM rcte_tree WHERE parent_id IS NULL
      UNION ALL
      SELECT t.child_id, t.label FROM rcte_tree t JOIN tree tr ON t.parent_id = tr.id
    ) SEARCH BREADTH FIRST BY id SET ordcol
    SELECT id, ordcol FROM tree
  ) bf) =
  (SELECT string_agg(id::text, ',' ORDER BY ordcol) FROM (
    WITH RECURSIVE tree(id, label) AS (
      SELECT child_id, label FROM rcte_tree WHERE parent_id IS NULL
      UNION ALL
      SELECT t.child_id, t.label FROM rcte_tree t JOIN tree tr ON t.parent_id = tr.id
    ) SEARCH DEPTH FIRST BY id SET ordcol
    SELECT id, ordcol FROM tree
  ) df) AS bf_order,
  false AS df_order;

-- ============================================================================
-- SECTION D: CYCLE Detection
-- ============================================================================

-- ============================================================================
-- 6. CYCLE: detect cycles in graph
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 5
-- end-expected
WITH RECURSIVE traverse(node) AS (
  SELECT 1
  UNION ALL
  SELECT g.dst FROM rcte_graph g JOIN traverse t ON g.src = t.node
)
CYCLE node SET is_cycle USING path
SELECT count(*)::integer AS cnt FROM traverse WHERE NOT is_cycle;

-- ============================================================================
-- 7. CYCLE: is_cycle flag is correct
-- ============================================================================

-- note: Starting from node 1: 1->2->3->1(cycle), 3->4->5
-- The cycle row (back to 1) should have is_cycle = true
-- begin-expected
-- columns: has_cycle_rows
-- row: true
-- end-expected
WITH RECURSIVE traverse(node) AS (
  SELECT 1
  UNION ALL
  SELECT g.dst FROM rcte_graph g JOIN traverse t ON g.src = t.node
)
CYCLE node SET is_cycle USING path
SELECT bool_or(is_cycle) AS has_cycle_rows FROM traverse;

-- ============================================================================
-- 8. CYCLE: no cycle in tree (acyclic graph)
-- ============================================================================

-- begin-expected
-- columns: any_cycles
-- row: false
-- end-expected
WITH RECURSIVE tree(id) AS (
  SELECT child_id FROM rcte_tree WHERE parent_id IS NULL
  UNION ALL
  SELECT t.child_id FROM rcte_tree t JOIN tree tr ON t.parent_id = tr.id
)
CYCLE id SET is_cycle USING path
SELECT bool_or(is_cycle) AS any_cycles FROM tree;

-- ============================================================================
-- 9. CYCLE with custom column names
-- ============================================================================

-- begin-expected
-- columns: has_loop
-- row: true
-- end-expected
WITH RECURSIVE traverse(node) AS (
  SELECT 1
  UNION ALL
  SELECT g.dst FROM rcte_graph g JOIN traverse t ON g.src = t.node
)
CYCLE node SET found_loop USING visited_path
SELECT bool_or(found_loop) AS has_loop FROM traverse;

-- ============================================================================
-- SECTION E: Combined SEARCH + CYCLE
-- ============================================================================

-- ============================================================================
-- 10. SEARCH BREADTH FIRST with CYCLE detection
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 6
-- end-expected
WITH RECURSIVE traverse(node) AS (
  SELECT 1
  UNION ALL
  SELECT g.dst FROM rcte_graph g JOIN traverse t ON g.src = t.node
)
SEARCH BREADTH FIRST BY node SET ordcol
CYCLE node SET is_cycle USING path
SELECT count(*)::integer AS cnt FROM traverse;

-- ============================================================================
-- 11. SEARCH DEPTH FIRST with CYCLE detection
-- ============================================================================

-- begin-expected
-- columns: non_cycle_cnt
-- row: 5
-- end-expected
WITH RECURSIVE traverse(node) AS (
  SELECT 1
  UNION ALL
  SELECT g.dst FROM rcte_graph g JOIN traverse t ON g.src = t.node
)
SEARCH DEPTH FIRST BY node SET ordcol
CYCLE node SET is_cycle USING path
SELECT count(*)::integer AS non_cycle_cnt FROM traverse WHERE NOT is_cycle;

-- ============================================================================
-- 12. Recursive CTE with multiple seed rows
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 5
-- end-expected
WITH RECURSIVE tree(id) AS (
  SELECT child_id FROM rcte_tree WHERE parent_id = 1  -- start from nodes 2 and 3
  UNION ALL
  SELECT t.child_id FROM rcte_tree t JOIN tree tr ON t.parent_id = tr.id
)
SELECT count(*)::integer AS cnt FROM tree;

-- ============================================================================
-- 13. Recursive CTE with depth limit
-- ============================================================================

-- begin-expected
-- columns: id, depth
-- row: 1, 0
-- row: 2, 1
-- row: 3, 1
-- end-expected
WITH RECURSIVE tree(id, depth) AS (
  SELECT child_id, 0 FROM rcte_tree WHERE parent_id IS NULL
  UNION ALL
  SELECT t.child_id, tr.depth + 1
  FROM rcte_tree t JOIN tree tr ON t.parent_id = tr.id
  WHERE tr.depth < 1
)
SELECT id, depth FROM tree ORDER BY depth, id;

-- ============================================================================
-- 14. Recursive CTE path accumulation
-- ============================================================================

-- begin-expected
-- columns: id, path
-- row: 1, 1
-- row: 2, 1->2
-- row: 3, 1->3
-- row: 4, 1->2->4
-- row: 5, 1->2->5
-- row: 6, 1->3->6
-- end-expected
WITH RECURSIVE tree(id, path) AS (
  SELECT child_id, child_id::text
  FROM rcte_tree WHERE parent_id IS NULL
  UNION ALL
  SELECT t.child_id, tr.path || '->' || t.child_id::text
  FROM rcte_tree t JOIN tree tr ON t.parent_id = tr.id
)
SELECT id, path FROM tree ORDER BY id;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA rcte_test CASCADE;
SET search_path = public;
