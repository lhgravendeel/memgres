-- ============================================================================
-- A domain may stand over a composite type
-- ============================================================================

DROP DOMAIN IF EXISTS zzw4f_dc CASCADE;
DROP TYPE IF EXISTS zzw4f_c9 CASCADE;
CREATE TYPE zzw4f_c9 AS (a int, b text);

CREATE DOMAIN zzw4f_dc AS zzw4f_c9 CHECK ((VALUE).a > 3);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM pg_type WHERE typname = 'zzw4f_dc';

DROP DOMAIN zzw4f_dc;
DROP TYPE zzw4f_c9;

-- ============================================================================
-- Two data-modifying WITH items writing one row: only one write takes effect
-- ============================================================================

DROP TABLE IF EXISTS zzw4f_cte CASCADE;
CREATE TABLE zzw4f_cte (id int PRIMARY KEY, v int);
INSERT INTO zzw4f_cte VALUES (1,0);

-- begin-expected
-- columns: ca|cb
-- row: 1|0
-- end-expected
WITH a AS (UPDATE zzw4f_cte SET v = 100 WHERE id = 1 RETURNING id),
     b AS (UPDATE zzw4f_cte SET v = 200 WHERE id = 1 RETURNING id)
SELECT (SELECT count(*) FROM a)::text AS ca, (SELECT count(*) FROM b)::text AS cb;

-- begin-expected
-- columns: v
-- row: 100
-- end-expected
SELECT v::text AS v FROM zzw4f_cte WHERE id = 1;

-- A DELETE item and an UPDATE item on the same row: the update finds nothing left to write.
-- begin-expected
-- columns: cd|cu
-- row: 1|0
-- end-expected
WITH d AS (DELETE FROM zzw4f_cte WHERE id = 1 RETURNING id),
     u AS (UPDATE zzw4f_cte SET v = 7 WHERE id = 1 RETURNING id)
SELECT (SELECT count(*) FROM d)::text AS cd, (SELECT count(*) FROM u)::text AS cu;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM zzw4f_cte;

DROP TABLE zzw4f_cte;

-- ============================================================================
-- An aborted UPDATE renumbers nothing: the row keeps the tuple id it had
-- ============================================================================

DROP TABLE IF EXISTS zzw4f_c8 CASCADE;
CREATE TABLE zzw4f_c8 (id int PRIMARY KEY, v int);
INSERT INTO zzw4f_c8 VALUES (1,1);

-- begin-expected
-- columns: t
-- row: (0,1)
-- end-expected
SELECT ctid::text AS t FROM zzw4f_c8 WHERE id = 1;

BEGIN;
UPDATE zzw4f_c8 SET v = v + 1 WHERE id = 1;

-- the writing transaction sees the version it wrote
-- begin-expected
-- columns: t
-- row: (0,2)
-- end-expected
SELECT ctid::text AS t FROM zzw4f_c8 WHERE id = 1;

ROLLBACK;

-- begin-expected
-- columns: t|v
-- row: (0,1)|1
-- end-expected
SELECT ctid::text AS t, v::text AS v FROM zzw4f_c8 WHERE id = 1;

DROP TABLE zzw4f_c8;