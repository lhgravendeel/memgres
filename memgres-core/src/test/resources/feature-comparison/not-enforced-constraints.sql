-- ============================================================================
-- Feature Comparison: NOT ENFORCED Constraints (PG 18)
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

DROP SCHEMA IF EXISTS ne_test CASCADE;
CREATE SCHEMA ne_test;
SET search_path = ne_test, public;

-- ============================================================================
-- 1. CHECK NOT ENFORCED — column-level inline
-- ============================================================================

CREATE TABLE ne_check_inline (
  id integer PRIMARY KEY,
  val integer CHECK (val > 0) NOT ENFORCED
);

-- Violating value succeeds (constraint not enforced)
INSERT INTO ne_check_inline VALUES (1, -5);

-- begin-expected
-- columns: id, val
-- row: 1, -5
-- end-expected
SELECT * FROM ne_check_inline;

-- ============================================================================
-- 2. CHECK NOT ENFORCED — table-level named constraint
-- ============================================================================

CREATE TABLE ne_check_named (
  id integer PRIMARY KEY,
  val integer,
  CONSTRAINT chk_positive CHECK (val > 0) NOT ENFORCED
);

INSERT INTO ne_check_named VALUES (1, -10);

-- begin-expected
-- columns: id, val
-- row: 1, -10
-- end-expected
SELECT * FROM ne_check_named;

-- ============================================================================
-- 3. CHECK ENFORCED (default) — still validates
-- ============================================================================

CREATE TABLE ne_check_enforced (
  id integer PRIMARY KEY,
  val integer CHECK (val > 0)
);

-- begin-expected-error
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO ne_check_enforced VALUES (1, -5);

-- ============================================================================
-- 4. FK NOT ENFORCED
-- ============================================================================

CREATE TABLE ne_parent (id integer PRIMARY KEY);
INSERT INTO ne_parent VALUES (1), (2), (3);

CREATE TABLE ne_child_ne (
  id integer PRIMARY KEY,
  parent_id integer REFERENCES ne_parent(id) NOT ENFORCED
);

-- Violating FK succeeds (parent_id=999 doesn't exist in parent)
INSERT INTO ne_child_ne VALUES (1, 999);

-- begin-expected
-- columns: id, parent_id
-- row: 1, 999
-- end-expected
SELECT * FROM ne_child_ne;

-- ============================================================================
-- 5. FK ENFORCED (default) — still validates
-- ============================================================================

CREATE TABLE ne_child_enforced (
  id integer PRIMARY KEY,
  parent_id integer REFERENCES ne_parent(id)
);

-- begin-expected-error
-- message-like: violates foreign key constraint
-- end-expected-error
INSERT INTO ne_child_enforced VALUES (1, 999);

-- ============================================================================
-- 6. UPDATE violating NOT ENFORCED CHECK
-- ============================================================================

CREATE TABLE ne_update_check (
  id integer PRIMARY KEY,
  val integer,
  CONSTRAINT chk_upd CHECK (val >= 0) NOT ENFORCED
);

INSERT INTO ne_update_check VALUES (1, 10);
UPDATE ne_update_check SET val = -1 WHERE id = 1;

-- begin-expected
-- columns: val
-- row: -1
-- end-expected
SELECT val FROM ne_update_check WHERE id = 1;

-- ============================================================================
-- 7. UPDATE violating NOT ENFORCED FK
-- ============================================================================

UPDATE ne_child_ne SET parent_id = 888 WHERE id = 1;

-- begin-expected
-- columns: parent_id
-- row: 888
-- end-expected
SELECT parent_id FROM ne_child_ne WHERE id = 1;

-- ============================================================================
-- 8. Mixed enforced + not-enforced on same table
-- ============================================================================

CREATE TABLE ne_mixed (
  id integer PRIMARY KEY,
  a integer CHECK (a > 0) NOT ENFORCED,
  b integer CHECK (b > 0)
);

-- a violates (not enforced) — OK; b satisfies (enforced) — OK
INSERT INTO ne_mixed VALUES (1, -5, 10);

-- begin-expected
-- columns: id, a, b
-- row: 1, -5, 10
-- end-expected
SELECT * FROM ne_mixed;

-- a OK, b violates (enforced) — FAIL
-- begin-expected-error
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO ne_mixed VALUES (2, 5, -10);

-- ============================================================================
-- 9. ALTER TABLE ALTER CONSTRAINT ... NOT ENFORCED
-- ============================================================================

CREATE TABLE ne_toggle (
  id integer PRIMARY KEY,
  val integer,
  CONSTRAINT chk_toggle CHECK (val > 0)
);

-- Enforced: rejects negative
-- begin-expected-error
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO ne_toggle VALUES (1, -5);

-- Disable enforcement
ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle NOT ENFORCED;

-- Now accepts negative
INSERT INTO ne_toggle VALUES (1, -5);

-- begin-expected
-- columns: id, val
-- end-expected
SELECT * FROM ne_toggle;

-- ============================================================================
-- 10. ALTER TABLE ALTER CONSTRAINT ... ENFORCED
-- ============================================================================

-- Re-enable enforcement
ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle ENFORCED;

-- Now rejects negative again
-- begin-expected-error
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO ne_toggle VALUES (2, -10);

-- ============================================================================
-- 11. ADD CONSTRAINT ... NOT ENFORCED (skips existing data validation)
-- ============================================================================

CREATE TABLE ne_add_late (
  id integer PRIMARY KEY,
  val integer
);

INSERT INTO ne_add_late VALUES (1, -5), (2, 10), (3, -20);

-- Adding NOT ENFORCED constraint succeeds despite existing violating data
ALTER TABLE ne_add_late ADD CONSTRAINT chk_late CHECK (val > 0) NOT ENFORCED;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::integer AS cnt FROM ne_add_late;

-- ============================================================================
-- 12. pg_constraint.conenforced column
-- ============================================================================

-- begin-expected
-- columns: conname, conenforced
-- row: chk_positive, false
-- end-expected
SELECT conname, conenforced
FROM pg_constraint
WHERE conname = 'chk_positive';

-- begin-expected
-- columns: conenforced
-- row: true
-- end-expected
SELECT conenforced
FROM pg_constraint
WHERE conrelid = 'ne_check_enforced'::regclass
AND contype = 'c'
LIMIT 1;

-- ============================================================================
-- 13. information_schema.table_constraints.enforced
-- ============================================================================

-- begin-expected
-- columns: enforced
-- row: NO
-- end-expected
SELECT enforced
FROM information_schema.table_constraints
WHERE constraint_name = 'chk_positive'
AND table_schema = 'ne_test';

-- ============================================================================
-- 14. NOT ENFORCED FK: no cascade actions
-- ============================================================================

-- note: NOT ENFORCED FK should not trigger ON DELETE CASCADE / ON UPDATE CASCADE

-- Delete parent — child row with dangling FK stays
DELETE FROM ne_parent WHERE id = 1;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::integer AS cnt FROM ne_child_ne;

-- ============================================================================
-- 15. Multiple NOT ENFORCED constraints
-- ============================================================================

CREATE TABLE ne_multi (
  id integer PRIMARY KEY,
  a integer,
  b integer,
  CONSTRAINT chk_a CHECK (a > 100) NOT ENFORCED,
  CONSTRAINT chk_b CHECK (b > 100) NOT ENFORCED
);

-- Both violated — both accepted
INSERT INTO ne_multi VALUES (1, 0, 0);

-- begin-expected
-- columns: id, a, b
-- row: 1, 0, 0
-- end-expected
SELECT * FROM ne_multi;

-- ============================================================================
-- 16. DROP NOT ENFORCED constraint
-- ============================================================================

ALTER TABLE ne_multi DROP CONSTRAINT chk_a;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_constraint
WHERE conrelid = 'ne_multi'::regclass AND conname LIKE 'chk_%';

-- ============================================================================
-- 17. NOT ENFORCED CHECK with complex expression
-- ============================================================================

CREATE TABLE ne_complex_check (
  id integer PRIMARY KEY,
  start_date date,
  end_date date,
  CONSTRAINT chk_dates CHECK (end_date > start_date) NOT ENFORCED
);

-- Violating dates — accepted
INSERT INTO ne_complex_check VALUES (1, '2025-12-31', '2025-01-01');

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM ne_complex_check;

-- ============================================================================
-- 18. NOT ENFORCED FK — table-level syntax
-- ============================================================================

CREATE TABLE ne_child_tbl (
  id integer PRIMARY KEY,
  parent_id integer,
  CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES ne_parent(id) NOT ENFORCED
);

INSERT INTO ne_child_tbl VALUES (1, 99999);

-- begin-expected
-- columns: id, parent_id
-- row: 1, 99999
-- end-expected
SELECT * FROM ne_child_tbl;

-- ============================================================================
-- 19. NOT ENFORCED with ON CONFLICT DO UPDATE
-- ============================================================================

CREATE TABLE ne_upsert (
  id integer PRIMARY KEY,
  val integer CHECK (val > 0) NOT ENFORCED
);

INSERT INTO ne_upsert VALUES (1, 10);
INSERT INTO ne_upsert VALUES (1, -5) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;

-- begin-expected
-- columns: val
-- row: -5
-- end-expected
SELECT val FROM ne_upsert WHERE id = 1;

-- ============================================================================
-- 20. NOT ENFORCED with DELETE
-- ============================================================================

DELETE FROM ne_upsert WHERE id = 1;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM ne_upsert;

-- ============================================================================
-- 21. NOT ENFORCED with RETURNING
-- ============================================================================

INSERT INTO ne_upsert VALUES (1, -99) RETURNING id, val;

-- begin-expected
-- columns: id, val
-- row: 1, -99
-- end-expected
SELECT * FROM ne_upsert;

-- ============================================================================
-- 22. Self-referencing FK NOT ENFORCED
-- ============================================================================

CREATE TABLE ne_self_ref (
  id integer PRIMARY KEY,
  parent_id integer,
  CONSTRAINT fk_self FOREIGN KEY (parent_id) REFERENCES ne_self_ref(id) NOT ENFORCED
);

INSERT INTO ne_self_ref VALUES (1, 999);  -- parent doesn't exist, accepted

-- begin-expected
-- columns: id, parent_id
-- row: 1, 999
-- end-expected
SELECT * FROM ne_self_ref;

-- ============================================================================
-- 23. Multi-column FK NOT ENFORCED
-- ============================================================================

CREATE TABLE ne_parent_mc (a integer, b integer, PRIMARY KEY (a, b));
INSERT INTO ne_parent_mc VALUES (1, 1), (2, 2);

CREATE TABLE ne_child_mc (
  id integer PRIMARY KEY,
  a integer,
  b integer,
  CONSTRAINT fk_mc FOREIGN KEY (a, b) REFERENCES ne_parent_mc(a, b) NOT ENFORCED
);

INSERT INTO ne_child_mc VALUES (1, 99, 99);  -- doesn't exist, accepted

-- begin-expected
-- columns: id, a, b
-- row: 1, 99, 99
-- end-expected
SELECT * FROM ne_child_mc;

-- ============================================================================
-- 24. UNIQUE NOT ENFORCED
-- ============================================================================

-- note: PG 18 allows UNIQUE NOT ENFORCED — duplicate values accepted

-- begin-expected-error
-- message-like: NOT ENFORCED
-- end-expected-error
CREATE TABLE ne_unique (
  id integer PRIMARY KEY,
  code text,
  CONSTRAINT uq_code UNIQUE (code) NOT ENFORCED
);

INSERT INTO ne_unique VALUES (1, 'A'), (2, 'A');  -- duplicate accepted

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT count(*)::integer AS cnt FROM ne_unique WHERE code = 'A';

-- ============================================================================
-- 25. PRIMARY KEY NOT ENFORCED (should error)
-- ============================================================================

-- note: NOT ENFORCED is not allowed on PRIMARY KEY constraints
-- begin-expected-error
-- message-like: primary key
-- end-expected-error
CREATE TABLE ne_pk_fail (
  id integer,
  CONSTRAINT pk_fail PRIMARY KEY (id) NOT ENFORCED
);

-- ============================================================================
-- 26. NOT VALID vs NOT ENFORCED distinction
-- ============================================================================

-- NOT VALID: skips existing data validation but ENFORCES for future inserts
-- NOT ENFORCED: never enforces, neither existing nor future data

CREATE TABLE ne_valid_test (id integer PRIMARY KEY, val integer);
INSERT INTO ne_valid_test VALUES (1, -5);  -- negative value

-- NOT VALID: future inserts must still satisfy the constraint
ALTER TABLE ne_valid_test ADD CONSTRAINT chk_positive CHECK (val > 0) NOT VALID;

-- begin-expected-error
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO ne_valid_test VALUES (2, -10);

DROP TABLE ne_valid_test;

-- NOT ENFORCED: future inserts are NOT checked
CREATE TABLE ne_enforced_test (id integer PRIMARY KEY, val integer);
INSERT INTO ne_enforced_test VALUES (1, -5);

ALTER TABLE ne_enforced_test ADD CONSTRAINT chk_positive2 CHECK (val > 0) NOT ENFORCED;

-- This succeeds because NOT ENFORCED skips validation
INSERT INTO ne_enforced_test VALUES (2, -10);

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM ne_enforced_test;

DROP TABLE ne_enforced_test;

-- ============================================================================
-- 27. NOT ENFORCED with DEFERRABLE
-- ============================================================================

CREATE TABLE ne_defer_parent (id integer PRIMARY KEY);
INSERT INTO ne_defer_parent VALUES (1);

CREATE TABLE ne_defer_child (
  id integer PRIMARY KEY,
  parent_id integer,
  CONSTRAINT fk_defer FOREIGN KEY (parent_id) REFERENCES ne_defer_parent(id)
    DEFERRABLE INITIALLY DEFERRED NOT ENFORCED
);

-- Not enforced, so orphan reference allowed regardless of deferral
INSERT INTO ne_defer_child VALUES (1, 999);

-- begin-expected
-- columns: parent_id
-- row: 999
-- end-expected
SELECT parent_id FROM ne_defer_child;

DROP TABLE ne_defer_child, ne_defer_parent;

-- ============================================================================
-- 28. NOT ENFORCED FK with CASCADE action
-- ============================================================================

CREATE TABLE ne_cascade_parent (id integer PRIMARY KEY, val text);
INSERT INTO ne_cascade_parent VALUES (1, 'parent1'), (2, 'parent2');

CREATE TABLE ne_cascade_child (
  id integer PRIMARY KEY,
  parent_id integer,
  CONSTRAINT fk_cascade FOREIGN KEY (parent_id) REFERENCES ne_cascade_parent(id)
    ON DELETE CASCADE NOT ENFORCED
);

INSERT INTO ne_cascade_child VALUES (1, 1), (2, 999);  -- 999 is orphan

-- note: Deleting parent with NOT ENFORCED FK — cascade may or may not fire
-- Inserting orphan reference was allowed. Let's see cascade behavior for valid refs.
DELETE FROM ne_cascade_parent WHERE id = 1;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::integer AS cnt FROM ne_cascade_child WHERE parent_id = 999;

DROP TABLE ne_cascade_child, ne_cascade_parent;

-- ============================================================================
-- 29. DROP CONSTRAINT that was NOT ENFORCED
-- ============================================================================

CREATE TABLE ne_drop_test (id integer PRIMARY KEY, val integer,
  CONSTRAINT chk_drop CHECK (val > 0) NOT ENFORCED);
INSERT INTO ne_drop_test VALUES (1, -5);

ALTER TABLE ne_drop_test DROP CONSTRAINT chk_drop;

-- Constraint gone — confirm pg_constraint
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM pg_constraint
WHERE conname = 'chk_drop' AND conrelid = 'ne_drop_test'::regclass;

DROP TABLE ne_drop_test;

-- ============================================================================
-- 30. NOT ENFORCED with NULL FK column
-- ============================================================================

CREATE TABLE ne_null_parent (id integer PRIMARY KEY);
INSERT INTO ne_null_parent VALUES (1);

CREATE TABLE ne_null_child (
  id integer PRIMARY KEY,
  parent_id integer,
  CONSTRAINT fk_null_ne FOREIGN KEY (parent_id) REFERENCES ne_null_parent(id) NOT ENFORCED
);

-- NULL FK is always allowed (even with enforced FK), but verify with NOT ENFORCED
INSERT INTO ne_null_child VALUES (1, NULL);

-- begin-expected
-- columns: id, parent_id_is_null
-- row: 1, true
-- end-expected
SELECT id, parent_id IS NULL AS parent_id_is_null FROM ne_null_child;

DROP TABLE ne_null_child, ne_null_parent;

-- ============================================================================
-- 31. Toggling ENFORCED validates existing data
-- ============================================================================

CREATE TABLE ne_toggle_validate (id integer PRIMARY KEY, val integer,
  CONSTRAINT chk_toggle CHECK (val > 0) NOT ENFORCED);

INSERT INTO ne_toggle_validate VALUES (1, -5);  -- violating data

-- Switching to ENFORCED should fail because existing data violates constraint
-- begin-expected-error
-- message-like: cannot alter enforceability
-- end-expected-error
ALTER TABLE ne_toggle_validate ALTER CONSTRAINT chk_toggle ENFORCED;

DROP TABLE ne_toggle_validate;

-- ============================================================================
-- 32. NOT ENFORCED CHECK with complex expression
-- ============================================================================

CREATE TABLE ne_complex_check (
  id integer PRIMARY KEY,
  start_date date,
  end_date date,
  CONSTRAINT chk_dates CHECK (end_date > start_date) NOT ENFORCED
);

-- Violating: end_date before start_date
INSERT INTO ne_complex_check VALUES (1, '2026-12-31', '2026-01-01');

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM ne_complex_check;

DROP TABLE ne_complex_check;

-- ============================================================================
-- 33. EXCLUDE constraint NOT ENFORCED
-- ============================================================================

-- note: EXCLUDE constraints with NOT ENFORCED (requires btree_gist for integer ranges)

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE ne_exclude (
  id integer PRIMARY KEY,
  room integer,
  during tsrange,
  CONSTRAINT excl_room EXCLUDE USING gist (room WITH =, during WITH &&) NOT ENFORCED
);

-- Overlapping bookings accepted because NOT ENFORCED
INSERT INTO ne_exclude VALUES
  (1, 101, '[2026-01-01, 2026-01-05)'),
  (2, 101, '[2026-01-03, 2026-01-07)');

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT count(*)::integer AS cnt FROM ne_exclude WHERE room = 101;

DROP TABLE ne_exclude;

-- ============================================================================
-- 34. NOT ENFORCED FK with ON UPDATE CASCADE
-- ============================================================================

CREATE TABLE ne_upd_parent (id integer PRIMARY KEY, val text);
INSERT INTO ne_upd_parent VALUES (1, 'p1'), (2, 'p2');

CREATE TABLE ne_upd_child (
  id integer PRIMARY KEY,
  parent_id integer,
  CONSTRAINT fk_upd_cascade FOREIGN KEY (parent_id) REFERENCES ne_upd_parent(id)
    ON UPDATE CASCADE NOT ENFORCED
);

INSERT INTO ne_upd_child VALUES (1, 1), (2, 999);  -- 999 is orphan, accepted

-- Update parent key — cascade may or may not propagate to valid child refs
UPDATE ne_upd_parent SET id = 10 WHERE id = 1;

-- begin-expected
-- columns: orphan_val
-- row: 999
-- end-expected
SELECT parent_id AS orphan_val FROM ne_upd_child WHERE id = 2;

DROP TABLE ne_upd_child, ne_upd_parent;

-- ============================================================================
-- 35. information_schema after toggling enforcement
-- ============================================================================

CREATE TABLE ne_info_toggle (id integer PRIMARY KEY, val integer,
  CONSTRAINT chk_info CHECK (val > 0) NOT ENFORCED);

-- begin-expected
-- columns: enforced
-- row: NO
-- end-expected
SELECT enforced FROM information_schema.table_constraints
WHERE constraint_name = 'chk_info' AND table_schema = 'ne_test';

-- Remove violating data, then toggle to ENFORCED
INSERT INTO ne_info_toggle VALUES (1, 5);
ALTER TABLE ne_info_toggle ALTER CONSTRAINT chk_info ENFORCED;

-- begin-expected
-- columns: enforced
-- row: NO
-- end-expected
SELECT enforced FROM information_schema.table_constraints
WHERE constraint_name = 'chk_info' AND table_schema = 'ne_test';

DROP TABLE ne_info_toggle;

-- ============================================================================
-- 36. NOT ENFORCED with COPY FROM
-- ============================================================================

CREATE TABLE ne_copy_parent (id integer PRIMARY KEY);
INSERT INTO ne_copy_parent VALUES (1), (2);

CREATE TABLE ne_copy_child (
  id integer PRIMARY KEY,
  parent_id integer,
  CONSTRAINT fk_copy FOREIGN KEY (parent_id) REFERENCES ne_copy_parent(id) NOT ENFORCED
);

-- COPY with orphan reference — should succeed because NOT ENFORCED
COPY ne_copy_child FROM STDIN;
1	1
2	999
\.

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::integer AS cnt FROM ne_copy_child;

-- Verify orphan reference exists
-- begin-expected
-- columns: parent_id
-- end-expected
SELECT parent_id FROM ne_copy_child WHERE id = 2;

DROP TABLE ne_copy_child, ne_copy_parent;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA ne_test CASCADE;
SET search_path = public;
