-- ============================================================================
-- Feature Comparison: ON CONFLICT target validation
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL settles the ON CONFLICT clause while planning, so a column or
-- constraint that does not exist is reported before any row is looked at, and
-- a constraint with no unique index has nothing to arbitrate on. A statement
-- may also not update the same row twice, because the outcome would depend on
-- the order the rows happened to be processed.
--
-- Row counts after a failed statement are deliberately not asserted here: a
-- failed multi-row INSERT is not yet undone, which is tracked separately.
-- ============================================================================

DROP TABLE IF EXISTS oct_t CASCADE;
CREATE TABLE oct_t (i int PRIMARY KEY, j text, k int UNIQUE);
INSERT INTO oct_t VALUES (1,'a',10),(2,'b',20);
ALTER TABLE oct_t ADD CONSTRAINT oct_ck CHECK (i > 0);

-- ============================================================================
-- 1. The assignment list must name real columns
-- ============================================================================
INSERT INTO oct_t VALUES (9,'x',90) ON CONFLICT (i) DO UPDATE SET nosuchcol = 'x';
-- reported even when the action would never run
INSERT INTO oct_t VALUES (9,'x',90) ON CONFLICT (i) DO UPDATE SET nosuchcol = 'x' WHERE false;
-- and even when there is no conflicting row at all
INSERT INTO oct_t VALUES (99,'x',990) ON CONFLICT (i) DO UPDATE SET nosuchcol = 'x';

-- ============================================================================
-- 2. A named constraint must exist and be backed by a unique index
-- ============================================================================
INSERT INTO oct_t VALUES (9,'x',90) ON CONFLICT ON CONSTRAINT oct_no_such DO NOTHING;
INSERT INTO oct_t VALUES (9,'x',90) ON CONFLICT ON CONSTRAINT oct_ck DO NOTHING;
INSERT INTO oct_t VALUES (9,'x',90) ON CONFLICT ON CONSTRAINT oct_ck DO UPDATE SET j = 'y';

-- ============================================================================
-- 3. One statement may not update the same row twice
-- ============================================================================
INSERT INTO oct_t VALUES (20,'p',200),(20,'q',201) ON CONFLICT (i) DO UPDATE SET j = EXCLUDED.j;
-- the same through a unique constraint rather than the primary key
INSERT INTO oct_t VALUES (30,'p',300),(31,'q',300) ON CONFLICT (k) DO UPDATE SET j = EXCLUDED.j;
-- DO NOTHING may meet the same key repeatedly
INSERT INTO oct_t VALUES (21,'p',210),(21,'q',211) ON CONFLICT (i) DO NOTHING;
SELECT string_agg(j, ',' ORDER BY j) AS a FROM oct_t WHERE i = 21;

-- ============================================================================
-- 4. The working forms are unaffected
-- ============================================================================
INSERT INTO oct_t VALUES (1,'upd',11) ON CONFLICT (i) DO UPDATE SET j = EXCLUDED.j;
SELECT j AS a FROM oct_t WHERE i = 1;
INSERT INTO oct_t VALUES (1,'skipped',12) ON CONFLICT (i) DO NOTHING;
SELECT j AS a FROM oct_t WHERE i = 1;
INSERT INTO oct_t VALUES (1,'byconstraint',13)
  ON CONFLICT ON CONSTRAINT oct_t_pkey DO UPDATE SET j = EXCLUDED.j;
SELECT j AS a FROM oct_t WHERE i = 1;
-- a conflict on the secondary unique key
INSERT INTO oct_t VALUES (50,'bykey',20) ON CONFLICT (k) DO UPDATE SET j = EXCLUDED.j;
SELECT j AS a FROM oct_t WHERE k = 20;
-- a WHERE clause on the action still filters
INSERT INTO oct_t VALUES (1,'nope',14) ON CONFLICT (i) DO UPDATE SET j = 'nope' WHERE oct_t.i > 100;
SELECT j AS a FROM oct_t WHERE i = 1;
-- EXCLUDED and the target may both be referenced
INSERT INTO oct_t VALUES (1,'tail',15)
  ON CONFLICT (i) DO UPDATE SET j = oct_t.j || '/' || EXCLUDED.j;
SELECT j AS a FROM oct_t WHERE i = 1;
-- a target with no unique constraint is still rejected as before
INSERT INTO oct_t VALUES (60,'x',600) ON CONFLICT (j) DO NOTHING;
-- and a nonexistent column as the target
INSERT INTO oct_t VALUES (61,'x',610) ON CONFLICT (nosuchcol) DO NOTHING;
-- DO UPDATE with no target at all
INSERT INTO oct_t VALUES (62,'x',620) ON CONFLICT DO UPDATE SET j = 'x';
-- targetless DO NOTHING still absorbs any unique violation
INSERT INTO oct_t VALUES (1,'zzz',10) ON CONFLICT DO NOTHING;
SELECT j AS a FROM oct_t WHERE i = 1;

-- ============================================================================
-- 5. A single-row statement is unaffected by the affect-once rule
-- ============================================================================
INSERT INTO oct_t VALUES (2,'one',21) ON CONFLICT (i) DO UPDATE SET j = EXCLUDED.j;
SELECT j AS a FROM oct_t WHERE i = 2;
INSERT INTO oct_t VALUES (70,'p',700),(71,'q',710) ON CONFLICT (i) DO UPDATE SET j = EXCLUDED.j;
SELECT string_agg(j, ',' ORDER BY j) AS a FROM oct_t WHERE i IN (70,71);

DROP TABLE IF EXISTS oct_t CASCADE;
