DROP SCHEMA IF EXISTS test_1200 CASCADE;
CREATE SCHEMA test_1200;
SET search_path TO test_1200;

CREATE TABLE edges (
    src text NOT NULL,
    dst text NOT NULL,
    PRIMARY KEY (src, dst)
);

INSERT INTO edges VALUES
('a','b'),
('a','c'),
('b','d'),
('c','d'),
('d','e'),
('e','b'),
('f','g');

-- begin-expected
-- columns: node,depth,path
-- row: a|0|{a}
-- row: b|1|{a,b}
-- row: c|1|{a,c}
-- row: d|2|{a,b,d}
-- row: d|2|{a,c,d}
-- row: e|3|{a,b,d,e}
-- row: e|3|{a,c,d,e}
-- row: b|4|{a,c,d,e,b}
-- end-expected
WITH RECURSIVE walk(node, depth, path) AS (
  SELECT 'a'::text, 0, ARRAY['a']::text[]
  UNION ALL
  SELECT e.dst, w.depth + 1, w.path || e.dst
  FROM walk w
  JOIN edges e ON e.src = w.node
  WHERE NOT e.dst = ANY(w.path)
)
SELECT node, depth, path::text AS path
FROM walk
ORDER BY depth, path::text;

-- begin-expected
-- columns: root,reachable_count
-- row: a|8
-- row: f|2
-- end-expected
WITH RECURSIVE roots AS (
  SELECT DISTINCT src AS root
  FROM edges
  WHERE src NOT IN (SELECT dst FROM edges)
), walk(root, node, path) AS (
  SELECT root, root, ARRAY[root]::text[]
  FROM roots
  UNION ALL
  SELECT w.root, e.dst, w.path || e.dst
  FROM walk w
  JOIN edges e ON e.src = w.node
  WHERE NOT e.dst = ANY(w.path)
)
SELECT root, COUNT(*) AS reachable_count
FROM walk
GROUP BY root
ORDER BY root;

-- begin-expected
-- columns: ancestor,descendant
-- row: a|b
-- row: a|d
-- row: a|e
-- row: b|d
-- row: b|e
-- row: c|b
-- row: c|d
-- row: c|e
-- row: d|b
-- row: d|e
-- row: e|b
-- row: e|d
-- end-expected
WITH RECURSIVE closure(ancestor, descendant, path) AS (
  SELECT src, dst, ARRAY[src, dst]::text[]
  FROM edges
  UNION ALL
  SELECT c.ancestor, e.dst, c.path || e.dst
  FROM closure c
  JOIN edges e ON e.src = c.descendant
  WHERE NOT e.dst = ANY(c.path)
)
SELECT ancestor, descendant
FROM closure
WHERE descendant IN ('b','d','e')
GROUP BY ancestor, descendant
ORDER BY ancestor, descendant;

