-- ============================================================================
-- Feature Comparison: ON CONFLICT DO UPDATE — failed update must not corrupt row
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- When an ON CONFLICT DO UPDATE triggers a constraint violation (e.g. UNIQUE
-- on a non-conflict column), the row must be left unchanged. The original
-- values and index entries must remain intact.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS ocu_main CASCADE;
CREATE TABLE ocu_main (
    id    int PRIMARY KEY,
    email text UNIQUE,
    val   text
);
INSERT INTO ocu_main VALUES (1, 'a@test.com', 'first');
INSERT INTO ocu_main VALUES (2, 'b@test.com', 'second');

-- ============================================================================
-- 1. Failed upsert (UNIQUE violation on email) — row unchanged
-- ============================================================================

-- begin-expected-error
-- message-like: duplicate key
-- end-expected-error
INSERT INTO ocu_main VALUES (1, 'a@test.com', 'first')
    ON CONFLICT (id) DO UPDATE SET email = 'b@test.com';

-- begin-expected
-- columns: id|email|val
-- row: 1|a@test.com|first
-- end-expected
SELECT id, email, val FROM ocu_main WHERE id = 1;

-- ============================================================================
-- 2. Other row untouched
-- ============================================================================

-- begin-expected
-- columns: id|email
-- row: 2|b@test.com
-- end-expected
SELECT id, email FROM ocu_main WHERE id = 2;

-- ============================================================================
-- 3. Row count unchanged
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::text AS cnt FROM ocu_main;

-- ============================================================================
-- 4. Index still consistent — original email still reserved
-- ============================================================================

-- begin-expected-error
-- message-like: duplicate key
-- end-expected-error
INSERT INTO ocu_main VALUES (3, 'a@test.com', 'third');

-- ============================================================================
-- 5. Successful upsert still works after a failed one
-- ============================================================================

INSERT INTO ocu_main VALUES (1, 'a@test.com', 'first')
    ON CONFLICT (id) DO UPDATE SET val = 'updated';

-- begin-expected
-- columns: id|email|val
-- row: 1|a@test.com|updated
-- end-expected
SELECT id, email, val FROM ocu_main WHERE id = 1;

-- ============================================================================
-- 6. CHECK constraint violation — row unchanged
-- ============================================================================

DROP TABLE IF EXISTS ocu_check CASCADE;
CREATE TABLE ocu_check (id int PRIMARY KEY, score int CHECK (score >= 0));
INSERT INTO ocu_check VALUES (1, 50);

-- begin-expected-error
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO ocu_check VALUES (1, 50)
    ON CONFLICT (id) DO UPDATE SET score = -1;

-- begin-expected
-- columns: score
-- row: 50
-- end-expected
SELECT score FROM ocu_check WHERE id = 1;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS ocu_main CASCADE;
DROP TABLE IF EXISTS ocu_check CASCADE;
