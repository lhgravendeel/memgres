DROP SCHEMA IF EXISTS test_790 CASCADE;
CREATE SCHEMA test_790;
SET search_path TO test_790;

CREATE TABLE parent (
    parent_id integer PRIMARY KEY
);

CREATE TABLE child (
    child_id integer PRIMARY KEY,
    parent_id integer NOT NULL
);

INSERT INTO parent VALUES (1), (2);
INSERT INTO child VALUES (10, 1), (20, 2);

ALTER TABLE child
ADD CONSTRAINT child_parent_fk
FOREIGN KEY (parent_id) REFERENCES parent(parent_id) NOT VALID;

-- begin-expected
-- columns: conname,convalidated
-- row: child_parent_fk|t
-- end-expected
SELECT conname, convalidated
FROM pg_constraint
WHERE conname = 'child_parent_fk';

ALTER TABLE child VALIDATE CONSTRAINT child_parent_fk;

-- begin-expected
-- columns: conname,convalidated
-- row: child_parent_fk|t
-- end-expected
SELECT conname, convalidated
FROM pg_constraint
WHERE conname = 'child_parent_fk';

CREATE UNIQUE INDEX uq_child_parent_id ON child(parent_id);

-- begin-expected
-- columns: indexname
-- row: uq_child_parent_id
-- end-expected
SELECT indexname
FROM pg_indexes
WHERE schemaname = 'test_790'
  AND indexname = 'uq_child_parent_id';

