-- M20: ALTER SEQUENCE OWNED BY tracking
-- L13: Partition NOT NULL conparentid

-- Setup
CREATE TABLE sob_t(id int, name text);
CREATE SEQUENCE sob_seq;

-- M20: ALTER SEQUENCE OWNED BY links sequence to table.column
ALTER SEQUENCE sob_seq OWNED BY sob_t.id;

-- begin-expected
-- columns: result
-- row: public.sob_seq
-- end-expected
SELECT pg_get_serial_sequence('sob_t', 'id') AS result;

-- M20: pg_depend row for OWNED BY
-- begin-expected
-- columns: deptype
-- row: a
-- end-expected
SELECT d.deptype FROM pg_depend d
JOIN pg_class sc ON sc.oid = d.objid
JOIN pg_class tc ON tc.oid = d.refobjid
WHERE sc.relname = 'sob_seq' AND tc.relname = 'sob_t';

-- M20: OWNED BY NONE clears
ALTER SEQUENCE sob_seq OWNED BY NONE;

-- begin-expected
-- columns: result
-- row: (null)
-- end-expected
SELECT pg_get_serial_sequence('sob_t', 'id') AS result;

-- M20: identity deptype is 'i'
CREATE TABLE sob_id(id int GENERATED ALWAYS AS IDENTITY);

-- begin-expected
-- columns: deptype
-- row: i
-- end-expected
SELECT d.deptype FROM pg_depend d
JOIN pg_class sc ON sc.oid = d.objid
JOIN pg_class tc ON tc.oid = d.refobjid
WHERE sc.relname = 'sob_id_id_seq' AND tc.relname = 'sob_id';

-- L13: partition child NOT NULL conparentid
CREATE TABLE sob_parent(id int NOT NULL, val text NOT NULL) PARTITION BY RANGE(id);
CREATE TABLE sob_child PARTITION OF sob_parent FOR VALUES FROM (1) TO (100);

-- begin-expected
-- columns: has_parent
-- row: t
-- end-expected
SELECT (child.conparentid > 0)::text AS has_parent
FROM pg_constraint child
JOIN pg_class cc ON cc.oid = child.conrelid
WHERE cc.relname = 'sob_child' AND child.contype = 'n' AND child.conname LIKE '%id%';

-- begin-expected
-- columns: coninhcount
-- row: 1
-- end-expected
SELECT child.coninhcount
FROM pg_constraint child
JOIN pg_class cc ON cc.oid = child.conrelid
WHERE cc.relname = 'sob_child' AND child.contype = 'n' AND child.conname LIKE '%id%';

-- Cleanup
DROP TABLE IF EXISTS sob_child;
DROP TABLE IF EXISTS sob_parent CASCADE;
DROP TABLE IF EXISTS sob_id;
DROP TABLE IF EXISTS sob_t CASCADE;
DROP SEQUENCE IF EXISTS sob_seq;
