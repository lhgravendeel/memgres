-- ============================================================================
-- Feature Comparison: cursors over VALUES and refcursors returned from PL/pgSQL
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A cursor can be declared for any query, VALUES included. A refcursor holds
-- a portal name, so a function that opens one and returns it leaves a cursor
-- the caller can FETCH from -- which is the whole point of the type.
-- ============================================================================

DROP TABLE IF EXISTS cvr_t CASCADE;
CREATE TABLE cvr_t (id int PRIMARY KEY);
INSERT INTO cvr_t VALUES (1),(2);

-- ============================================================================
-- 1. A refcursor returned from a function is usable by the caller
-- ============================================================================

CREATE FUNCTION cvr_open() RETURNS refcursor AS $$
DECLARE c refcursor := 'cvrcur';
BEGIN
  OPEN c FOR SELECT id FROM cvr_t ORDER BY id;
  RETURN c;
END $$ LANGUAGE plpgsql;

BEGIN;

-- begin-expected
-- columns: a
-- row: cvrcur
-- end-expected
SELECT cvr_open()::text AS a;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
FETCH ALL FROM cvrcur;

COMMIT;

-- ============================================================================
-- 2. A cursor may be declared for VALUES
-- ============================================================================

BEGIN;

DECLARE cvr_c CURSOR FOR VALUES (1),(2);

-- begin-expected
-- columns: column1
-- row: 1
-- row: 2
-- end-expected
FETCH ALL FROM cvr_c;

COMMIT;

DROP FUNCTION cvr_open();
DROP TABLE cvr_t;
