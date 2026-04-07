DROP SCHEMA IF EXISTS test_640 CASCADE;
CREATE SCHEMA test_640;
SET search_path TO test_640;

CREATE TABLE nodes (
    node_id integer PRIMARY KEY,
    parent_id integer,
    name text NOT NULL,
    kind text NOT NULL,
    size_bytes integer NOT NULL DEFAULT 0
);

INSERT INTO nodes(node_id, parent_id, name, kind, size_bytes) VALUES
(1, NULL, '/', 'dir', 0),
(2, 1, 'docs', 'dir', 0),
(3, 1, 'src', 'dir', 0),
(4, 2, 'readme.md', 'file', 10),
(5, 2, 'guide.md', 'file', 20),
(6, 3, 'main.py', 'file', 30),
(7, 3, 'lib', 'dir', 0),
(8, 7, 'util.py', 'file', 40);

-- begin-expected
-- columns: name,kind
-- row: docs|dir
-- row: src|dir
-- end-expected
SELECT name, kind
FROM nodes
WHERE parent_id = 1
ORDER BY name;

-- begin-expected
-- columns: node_id,path,kind,size_bytes
-- row: 1|/|dir|0
-- row: 2|//docs|dir|0
-- row: 3|//src|dir|0
-- row: 4|//docs/readme.md|file|10
-- row: 5|//docs/guide.md|file|20
-- row: 6|//src/main.py|file|30
-- row: 7|//src/lib|dir|0
-- row: 8|//src/lib/util.py|file|40
-- end-expected
WITH RECURSIVE tree AS (
    SELECT node_id, parent_id, name, kind, size_bytes, name::text AS path
    FROM nodes
    WHERE parent_id IS NULL
    UNION ALL
    SELECT n.node_id, n.parent_id, n.name, n.kind, n.size_bytes, t.path || '/' || n.name
    FROM tree t
    JOIN nodes n ON n.parent_id = t.node_id
)
SELECT node_id, path, kind, size_bytes
FROM tree
ORDER BY node_id;

-- begin-expected
-- columns: dir_name,total_file_size
-- row: docs|30
-- row: lib|40
-- row: src|30
-- end-expected
SELECT p.name AS dir_name, SUM(c.size_bytes) AS total_file_size
FROM nodes p
JOIN nodes c ON c.parent_id = p.node_id
WHERE p.kind = 'dir'
  AND c.kind = 'file'
GROUP BY p.name
ORDER BY p.name;

