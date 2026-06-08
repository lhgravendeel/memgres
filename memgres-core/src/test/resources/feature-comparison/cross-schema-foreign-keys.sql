-- ============================================================================
-- Feature Comparison: Cross-Schema Foreign Keys
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests that foreign key constraints work correctly across schemas:
--   - CREATE TABLE with FK referencing another schema
--   - ALTER TABLE ADD FK referencing another schema
--   - Data validation (INSERT, UPDATE, DELETE) across schemas
--   - CASCADE actions across schemas
--   - Schema-qualified vs unqualified REFERENCES resolution
--   - DROP TABLE / DROP SCHEMA with cross-schema FK deps
--   - TRUNCATE with cross-schema FK deps
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS csfk_a CASCADE;
DROP SCHEMA IF EXISTS csfk_b CASCADE;
CREATE SCHEMA csfk_a;
CREATE SCHEMA csfk_b;

-- ============================================================================
-- A. CREATE TABLE with inline FK referencing another schema
-- ============================================================================

CREATE TABLE csfk_a.parent (id int PRIMARY KEY, val text);
CREATE TABLE csfk_b.child (id int PRIMARY KEY, parent_id int REFERENCES csfk_a.parent(id));

INSERT INTO csfk_a.parent VALUES (1, 'one'), (2, 'two'), (3, 'three');
INSERT INTO csfk_b.child VALUES (10, 1), (20, 2);

-- begin-expected
-- columns: id|parent_id
-- row: 10|1
-- row: 20|2
-- end-expected
SELECT id, parent_id FROM csfk_b.child ORDER BY id;

-- B. INSERT with invalid FK — should fail
-- begin-expected-error
-- sqlstate: 23503
-- message-like: foreign key
-- end-expected-error
INSERT INTO csfk_b.child VALUES (30, 99);

-- C. INSERT with NULL FK — allowed
INSERT INTO csfk_b.child VALUES (40, NULL);

-- begin-expected
-- columns: id|parent_id
-- row: 10|1
-- row: 20|2
-- row: 40|null
-- end-expected
SELECT id, parent_id FROM csfk_b.child ORDER BY id;

-- ============================================================================
-- D. ALTER TABLE ADD FK referencing another schema
-- ============================================================================

CREATE TABLE csfk_a.products (id int PRIMARY KEY);
CREATE TABLE csfk_b.orders (id int PRIMARY KEY, product_id int);
INSERT INTO csfk_a.products VALUES (100), (200);

ALTER TABLE csfk_b.orders ADD CONSTRAINT fk_product
    FOREIGN KEY (product_id) REFERENCES csfk_a.products(id);

-- Valid insert
INSERT INTO csfk_b.orders VALUES (1, 100);

-- begin-expected
-- columns: id|product_id
-- row: 1|100
-- end-expected
SELECT id, product_id FROM csfk_b.orders ORDER BY id;

-- Invalid insert — FK violation
-- begin-expected-error
-- sqlstate: 23503
-- message-like: foreign key
-- end-expected-error
INSERT INTO csfk_b.orders VALUES (2, 999);

-- ============================================================================
-- E. ALTER TABLE ADD FK rejects existing invalid data
-- ============================================================================

CREATE TABLE csfk_a.ref_tbl (id int PRIMARY KEY);
CREATE TABLE csfk_b.bad_data (id int PRIMARY KEY, ref_id int);
INSERT INTO csfk_a.ref_tbl VALUES (1);
INSERT INTO csfk_b.bad_data VALUES (10, 1), (20, 99);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: foreign key
-- end-expected-error
ALTER TABLE csfk_b.bad_data ADD CONSTRAINT fk_ref
    FOREIGN KEY (ref_id) REFERENCES csfk_a.ref_tbl(id);

-- ============================================================================
-- F. DELETE parent blocked by cross-schema child (NO ACTION)
-- ============================================================================

-- begin-expected-error
-- sqlstate: 23503
-- message-like: foreign key
-- end-expected-error
DELETE FROM csfk_a.parent WHERE id = 1;

-- ============================================================================
-- G. ON DELETE CASCADE across schemas
-- ============================================================================

CREATE TABLE csfk_a.cascade_parent (id int PRIMARY KEY);
CREATE TABLE csfk_b.cascade_child (id int PRIMARY KEY, pid int REFERENCES csfk_a.cascade_parent(id) ON DELETE CASCADE);

INSERT INTO csfk_a.cascade_parent VALUES (1), (2), (3);
INSERT INTO csfk_b.cascade_child VALUES (10, 1), (20, 1), (30, 2);

DELETE FROM csfk_a.cascade_parent WHERE id = 1;

-- Children referencing parent 1 should be gone
-- begin-expected
-- columns: id|pid
-- row: 30|2
-- end-expected
SELECT id, pid FROM csfk_b.cascade_child ORDER BY id;

-- ============================================================================
-- H. ON DELETE SET NULL across schemas
-- ============================================================================

CREATE TABLE csfk_a.setnull_parent (id int PRIMARY KEY);
CREATE TABLE csfk_b.setnull_child (id int PRIMARY KEY, pid int REFERENCES csfk_a.setnull_parent(id) ON DELETE SET NULL);

INSERT INTO csfk_a.setnull_parent VALUES (1);
INSERT INTO csfk_b.setnull_child VALUES (10, 1);

DELETE FROM csfk_a.setnull_parent WHERE id = 1;

-- begin-expected
-- columns: id|pid
-- row: 10|null
-- end-expected
SELECT id, pid FROM csfk_b.setnull_child ORDER BY id;

-- ============================================================================
-- I. ON UPDATE CASCADE across schemas
-- ============================================================================

CREATE TABLE csfk_a.upd_parent (id int PRIMARY KEY);
CREATE TABLE csfk_b.upd_child (id int PRIMARY KEY, pid int REFERENCES csfk_a.upd_parent(id) ON UPDATE CASCADE);

INSERT INTO csfk_a.upd_parent VALUES (1);
INSERT INTO csfk_b.upd_child VALUES (10, 1);

UPDATE csfk_a.upd_parent SET id = 99 WHERE id = 1;

-- begin-expected
-- columns: id|pid
-- row: 10|99
-- end-expected
SELECT id, pid FROM csfk_b.upd_child ORDER BY id;

-- ============================================================================
-- J. UPDATE parent PK blocked by child (NO ACTION)
-- ============================================================================

-- begin-expected-error
-- sqlstate: 23503
-- message-like: foreign key
-- end-expected-error
UPDATE csfk_a.parent SET id = 999 WHERE id = 2;

-- ============================================================================
-- K. Schema-qualified REFERENCES resolves correctly despite search_path
-- ============================================================================

-- Create "shared" table in both schemas with different data
CREATE TABLE csfk_a.shared (id int PRIMARY KEY);
CREATE TABLE csfk_b.shared (id int PRIMARY KEY);
INSERT INTO csfk_a.shared VALUES (1), (2);
INSERT INTO csfk_b.shared VALUES (100), (200);

-- FK explicitly references csfk_a.shared
SET search_path = csfk_b, csfk_a, public;
CREATE TABLE csfk_b.ref_test (id int PRIMARY KEY, sid int REFERENCES csfk_a.shared(id));

-- Insert referencing value in csfk_a.shared — should succeed
INSERT INTO csfk_b.ref_test VALUES (1, 1);

-- begin-expected
-- columns: id|sid
-- row: 1|1
-- end-expected
SELECT id, sid FROM csfk_b.ref_test ORDER BY id;

-- Insert referencing value only in csfk_b.shared — should fail (FK points to csfk_a.shared)
-- begin-expected-error
-- sqlstate: 23503
-- message-like: foreign key
-- end-expected-error
INSERT INTO csfk_b.ref_test VALUES (2, 100);

SET search_path = public;

-- ============================================================================
-- L. Composite FK across schemas
-- ============================================================================

CREATE TABLE csfk_a.comp_parent (a int, b int, PRIMARY KEY (a, b));
CREATE TABLE csfk_b.comp_child (id int PRIMARY KEY, pa int, pb int,
    FOREIGN KEY (pa, pb) REFERENCES csfk_a.comp_parent(a, b));

INSERT INTO csfk_a.comp_parent VALUES (1, 2), (3, 4);
INSERT INTO csfk_b.comp_child VALUES (10, 1, 2);

-- begin-expected
-- columns: id|pa|pb
-- row: 10|1|2
-- end-expected
SELECT id, pa, pb FROM csfk_b.comp_child ORDER BY id;

-- Invalid composite ref
-- begin-expected-error
-- sqlstate: 23503
-- message-like: foreign key
-- end-expected-error
INSERT INTO csfk_b.comp_child VALUES (20, 1, 4);

-- ============================================================================
-- M. DROP TABLE blocked by cross-schema FK (no CASCADE)
-- ============================================================================

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: depend
-- end-expected-error
DROP TABLE csfk_a.parent;

-- ============================================================================
-- N. DROP TABLE CASCADE removes FK on child
-- ============================================================================

CREATE TABLE csfk_a.drop_parent (id int PRIMARY KEY);
CREATE TABLE csfk_b.drop_child (id int PRIMARY KEY, pid int REFERENCES csfk_a.drop_parent(id));

INSERT INTO csfk_a.drop_parent VALUES (1);
INSERT INTO csfk_b.drop_child VALUES (10, 1);

DROP TABLE csfk_a.drop_parent CASCADE;

-- Child table still exists, FK gone — any value accepted
INSERT INTO csfk_b.drop_child VALUES (20, 999);

-- begin-expected
-- columns: id|pid
-- row: 10|1
-- row: 20|999
-- end-expected
SELECT id, pid FROM csfk_b.drop_child ORDER BY id;

-- ============================================================================
-- O. TRUNCATE parent blocked by cross-schema child
-- ============================================================================

CREATE TABLE csfk_a.trunc_parent (id int PRIMARY KEY);
CREATE TABLE csfk_b.trunc_child (id int PRIMARY KEY, pid int REFERENCES csfk_a.trunc_parent(id));

INSERT INTO csfk_a.trunc_parent VALUES (1);
INSERT INTO csfk_b.trunc_child VALUES (10, 1);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: truncate
-- end-expected-error
TRUNCATE csfk_a.trunc_parent;

-- ============================================================================
-- P. TRUNCATE CASCADE across schemas
-- ============================================================================

TRUNCATE csfk_a.trunc_parent CASCADE;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM csfk_a.trunc_parent;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM csfk_b.trunc_child;

-- ============================================================================
-- Q. DROP SCHEMA CASCADE cleans up cross-schema FK
-- ============================================================================

-- Recreate a simple cross-schema FK for this test
CREATE TABLE csfk_a.schema_parent (id int PRIMARY KEY);
CREATE TABLE csfk_b.schema_child (id int PRIMARY KEY, pid int REFERENCES csfk_a.schema_parent(id));

INSERT INTO csfk_a.schema_parent VALUES (1);
INSERT INTO csfk_b.schema_child VALUES (10, 1);

DROP SCHEMA csfk_a CASCADE;

-- Child table survives, FK gone — any value accepted
INSERT INTO csfk_b.schema_child VALUES (20, 999);

-- begin-expected
-- columns: id|pid
-- row: 10|1
-- row: 20|999
-- end-expected
SELECT id, pid FROM csfk_b.schema_child ORDER BY id;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA IF EXISTS csfk_a CASCADE;
DROP SCHEMA IF EXISTS csfk_b CASCADE;
