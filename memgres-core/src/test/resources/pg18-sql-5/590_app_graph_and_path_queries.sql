DROP SCHEMA IF EXISTS test_590 CASCADE;
CREATE SCHEMA test_590;
SET search_path TO test_590;

CREATE TABLE edges (
    src text NOT NULL,
    dst text NOT NULL,
    PRIMARY KEY (src, dst)
);

INSERT INTO edges(src, dst) VALUES
('a','b'),
('a','c'),
('b','d'),
('c','d'),
('d','e'),
('e','f');

-- begin-expected
-- columns: node
-- row: b
-- row: c
-- end-expected
SELECT dst AS node
FROM edges
WHERE src = 'a'
ORDER BY dst;

-- begin-expected
-- columns: node
-- row: d
-- end-expected
SELECT DISTINCT e2.dst AS node
FROM edges e1
JOIN edges e2 ON e1.dst = e2.src
WHERE e1.src = 'a'
ORDER BY node;

-- begin-expected
-- columns: node,depth,path
-- row: a|0|a
-- row: b|1|a>b
-- row: c|1|a>c
-- row: d|2|a>b>d
-- row: d|2|a>c>d
-- row: e|3|a>b>d>e
-- row: e|3|a>c>d>e
-- row: f|4|a>b>d>e>f
-- row: f|4|a>c>d>e>f
-- end-expected
WITH RECURSIVE walk(node, depth, path) AS (
  SELECT 'a'::text, 0, 'a'::text
  UNION ALL
  SELECT e.dst, w.depth + 1, w.path || '>' || e.dst
  FROM walk w
  JOIN edges e ON e.src = w.node
)
SELECT node, depth, path
FROM walk
ORDER BY depth, path;

-- begin-expected
-- columns: mutual_via
-- row: d
-- end-expected
SELECT e1.dst AS mutual_via
FROM edges e1
JOIN edges e2 ON e1.dst = e2.src
WHERE e1.src = 'b' AND e2.dst = 'e'
ORDER BY mutual_via;

