-- ============================================================================
-- Feature Comparison: a failed statement leaves nothing behind
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A statement either happens or it does not. A multi-row INSERT refused on its
-- last row had been keeping every row before it, which is worse than the error
-- itself: the caller sees a failure and the table holds half the data.
-- ============================================================================

DROP TABLE IF EXISTS fsa_t CASCADE;
CREATE TABLE fsa_t (i int PRIMARY KEY, j text NOT NULL);
INSERT INTO fsa_t VALUES (1,'a');

-- ============================================================================
-- 1. A duplicate key in a later row
-- ============================================================================
INSERT INTO fsa_t VALUES (5,'e'),(5,'f');
SELECT count(*)::text AS a FROM fsa_t;
SELECT string_agg(i::text, ',' ORDER BY i) AS a FROM fsa_t;

-- ============================================================================
-- 2. A later row failing a NOT NULL constraint
-- ============================================================================
INSERT INTO fsa_t VALUES (6,'g'),(7,NULL);
SELECT count(*)::text AS a FROM fsa_t;
SELECT string_agg(i::text, ',' ORDER BY i) AS a FROM fsa_t;

-- ============================================================================
-- 3. A later row failing a CHECK constraint
-- ============================================================================
ALTER TABLE fsa_t ADD CONSTRAINT fsa_ck CHECK (i < 100);
INSERT INTO fsa_t VALUES (8,'h'),(200,'i');
SELECT count(*)::text AS a FROM fsa_t;
SELECT string_agg(i::text, ',' ORDER BY i) AS a FROM fsa_t;

-- ============================================================================
-- 4. ON CONFLICT DO UPDATE reaching the same row twice
-- ============================================================================
INSERT INTO fsa_t VALUES (9,'p'),(9,'q') ON CONFLICT (i) DO UPDATE SET j = EXCLUDED.j;
SELECT count(*)::text AS a FROM fsa_t;
SELECT string_agg(i::text, ',' ORDER BY i) AS a FROM fsa_t;

-- an update applied before the failure is rolled back with it
INSERT INTO fsa_t VALUES (10,'ten');
INSERT INTO fsa_t VALUES (10,'changed'),(10,'again')
  ON CONFLICT (i) DO UPDATE SET j = EXCLUDED.j;
SELECT j AS a FROM fsa_t WHERE i = 10;

-- ============================================================================
-- 5. INSERT ... SELECT is equally all-or-nothing
-- ============================================================================
DROP TABLE IF EXISTS fsa_src CASCADE;
CREATE TABLE fsa_src (i int, j text);
INSERT INTO fsa_src VALUES (20,'t'),(21,'u'),(1,'dup');
INSERT INTO fsa_t SELECT i, j FROM fsa_src ORDER BY i DESC;
SELECT count(*)::text AS a FROM fsa_t WHERE i IN (20,21);

-- ============================================================================
-- 6. A statement that succeeds keeps every row
-- ============================================================================
INSERT INTO fsa_t VALUES (30,'x'),(31,'y'),(32,'z');
SELECT count(*)::text AS a FROM fsa_t WHERE i IN (30,31,32);
INSERT INTO fsa_t VALUES (30,'skip'),(33,'w') ON CONFLICT (i) DO NOTHING;
SELECT string_agg(i::text, ',' ORDER BY i) AS a FROM fsa_t WHERE i IN (30,33);
SELECT j AS a FROM fsa_t WHERE i = 30;

-- ============================================================================
-- 7. Inside an explicit transaction the same rule holds
-- ============================================================================
BEGIN;
INSERT INTO fsa_t VALUES (40,'m'),(40,'n');
ROLLBACK;
SELECT count(*)::text AS a FROM fsa_t WHERE i = 40;
BEGIN;
INSERT INTO fsa_t VALUES (41,'m');
COMMIT;
SELECT count(*)::text AS a FROM fsa_t WHERE i = 41;

DROP TABLE IF EXISTS fsa_src CASCADE;
DROP TABLE IF EXISTS fsa_t CASCADE;
