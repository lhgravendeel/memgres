-- ============================================================================
-- Feature Comparison: trigger firing residuals (bugs-review.md H6)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- 1. INSERT/UPDATE/DELETE are atomic: a row trigger raising mid-statement rolls
--    back every side effect the statement already applied.
-- 2. MERGE fires BEFORE row triggers (UPDATE and INSERT), honoring a BEFORE
--    trigger that modifies NEW or returns NULL to skip the row.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Setup: a trigger function that raises on id = 2
-- ----------------------------------------------------------------------------
CREATE TABLE tfr_u (id int PRIMARY KEY, v int);
INSERT INTO tfr_u VALUES (1, 10), (2, 20);
CREATE FUNCTION tfr_raise_on2() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.id = 2 THEN RAISE EXCEPTION 'boom on 2'; END IF; RETURN NEW; END;
$$;
CREATE TRIGGER tfr_bu BEFORE UPDATE ON tfr_u FOR EACH ROW EXECUTE FUNCTION tfr_raise_on2();

-- 1a. Plain UPDATE aborts when the BEFORE trigger raises on the 2nd row
-- begin-expected-error
-- sqlstate: P0001
-- message-like: boom on 2
-- end-expected-error
UPDATE tfr_u SET v = v + 1;

-- 1b. Whole statement rolled back: row 1 is NOT left updated to 11
-- begin-expected
-- columns: id | v
-- row: 1, 10
-- row: 2, 20
-- end-expected
SELECT id, v FROM tfr_u ORDER BY id;

DROP TABLE tfr_u;
DROP FUNCTION tfr_raise_on2();

-- ----------------------------------------------------------------------------
-- 2. Multi-row INSERT is atomic on a BEFORE INSERT trigger error
-- ----------------------------------------------------------------------------
CREATE TABLE tfr_i (id int PRIMARY KEY, v int);
CREATE FUNCTION tfr_ins_raise_on2() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.id = 2 THEN RAISE EXCEPTION 'ins boom'; END IF; RETURN NEW; END;
$$;
CREATE TRIGGER tfr_bi BEFORE INSERT ON tfr_i FOR EACH ROW EXECUTE FUNCTION tfr_ins_raise_on2();

-- begin-expected-error
-- sqlstate: P0001
-- message-like: ins boom
-- end-expected-error
INSERT INTO tfr_i VALUES (1, 10), (2, 20), (3, 30);

-- Whole multi-row INSERT rolled back: table is empty
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM tfr_i;

DROP TABLE tfr_i;
DROP FUNCTION tfr_ins_raise_on2();

-- ----------------------------------------------------------------------------
-- 3. Multi-row DELETE is atomic on an AFTER DELETE trigger error
-- ----------------------------------------------------------------------------
CREATE TABLE tfr_d (id int PRIMARY KEY, v int);
INSERT INTO tfr_d VALUES (1, 10), (2, 20), (3, 30);
CREATE FUNCTION tfr_del_raise_on2() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF OLD.id = 2 THEN RAISE EXCEPTION 'del boom'; END IF; RETURN OLD; END;
$$;
CREATE TRIGGER tfr_ad AFTER DELETE ON tfr_d FOR EACH ROW EXECUTE FUNCTION tfr_del_raise_on2();

-- begin-expected-error
-- sqlstate: P0001
-- message-like: del boom
-- end-expected-error
DELETE FROM tfr_d WHERE v > 0;

-- Whole DELETE rolled back: all three rows remain
-- begin-expected
-- columns: id | v
-- row: 1, 10
-- row: 2, 20
-- row: 3, 30
-- end-expected
SELECT id, v FROM tfr_d ORDER BY id;

DROP TABLE tfr_d;
DROP FUNCTION tfr_del_raise_on2();

-- ----------------------------------------------------------------------------
-- 4. MERGE fires BEFORE UPDATE and BEFORE INSERT triggers, which modify NEW
-- ----------------------------------------------------------------------------
CREATE TABLE tfr_mt (id int PRIMARY KEY, v int);
INSERT INTO tfr_mt VALUES (1, 10);
CREATE TABLE tfr_ms (id int, v int);
INSERT INTO tfr_ms VALUES (1, 100), (2, 200);
CREATE FUNCTION tfr_bump() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.v := NEW.v + 1; RETURN NEW; END;
$$;
CREATE TRIGGER tfr_mbu BEFORE UPDATE ON tfr_mt FOR EACH ROW EXECUTE FUNCTION tfr_bump();
CREATE TRIGGER tfr_mbi BEFORE INSERT ON tfr_mt FOR EACH ROW EXECUTE FUNCTION tfr_bump();

MERGE INTO tfr_mt t USING tfr_ms s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET v = s.v
WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);

-- BEFORE triggers ran and each added 1: id1 -> 101, id2 -> 201
-- begin-expected
-- columns: id | v
-- row: 1, 101
-- row: 2, 201
-- end-expected
SELECT id, v FROM tfr_mt ORDER BY id;

DROP TABLE tfr_mt;
DROP TABLE tfr_ms;
DROP FUNCTION tfr_bump();

-- ----------------------------------------------------------------------------
-- 5. MERGE BEFORE UPDATE trigger raising aborts and rolls back the statement
-- ----------------------------------------------------------------------------
CREATE TABLE tfr_mt2 (id int PRIMARY KEY, v int);
INSERT INTO tfr_mt2 VALUES (1, 10), (2, 20);
CREATE TABLE tfr_ms2 (id int, v int);
INSERT INTO tfr_ms2 VALUES (1, 100), (2, 200);
CREATE FUNCTION tfr_raise_on2b() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.id = 2 THEN RAISE EXCEPTION 'merge boom'; END IF; RETURN NEW; END;
$$;
CREATE TRIGGER tfr_mbu2 BEFORE UPDATE ON tfr_mt2 FOR EACH ROW EXECUTE FUNCTION tfr_raise_on2b();

-- begin-expected-error
-- sqlstate: P0001
-- message-like: merge boom
-- end-expected-error
MERGE INTO tfr_mt2 t USING tfr_ms2 s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v;

-- MERGE is atomic: no row updated
-- begin-expected
-- columns: id | v
-- row: 1, 10
-- row: 2, 20
-- end-expected
SELECT id, v FROM tfr_mt2 ORDER BY id;

DROP TABLE tfr_mt2;
DROP TABLE tfr_ms2;
DROP FUNCTION tfr_raise_on2b();

-- ----------------------------------------------------------------------------
-- 6. MERGE BEFORE UPDATE trigger returning NULL skips just that row
-- ----------------------------------------------------------------------------
CREATE TABLE tfr_mt3 (id int PRIMARY KEY, v int);
INSERT INTO tfr_mt3 VALUES (1, 10), (3, 30);
CREATE TABLE tfr_ms3 (id int, v int);
INSERT INTO tfr_ms3 VALUES (1, 100), (3, 300);
CREATE FUNCTION tfr_skip3() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.id = 3 THEN RETURN NULL; END IF; RETURN NEW; END;
$$;
CREATE TRIGGER tfr_mbu3 BEFORE UPDATE ON tfr_mt3 FOR EACH ROW EXECUTE FUNCTION tfr_skip3();

MERGE INTO tfr_mt3 t USING tfr_ms3 s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v;

-- id=3 skipped by trigger (still 30); id=1 updated to 100
-- begin-expected
-- columns: id | v
-- row: 1, 100
-- row: 3, 30
-- end-expected
SELECT id, v FROM tfr_mt3 ORDER BY id;

DROP TABLE tfr_mt3;
DROP TABLE tfr_ms3;
DROP FUNCTION tfr_skip3();
