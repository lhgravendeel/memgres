-- ============================================================================
-- Feature Comparison: BEFORE UPDATE trigger sees post-SET NEW values
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG: BEFORE UPDATE triggers fire with NEW already containing the proposed
-- SET-clause results. The trigger can further modify NEW.
-- ============================================================================

-- Setup
CREATE TABLE bt_test (id int PRIMARY KEY, val int, audit text);
INSERT INTO bt_test VALUES (1, 10, NULL);
CREATE OR REPLACE FUNCTION bt_fn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.audit := 'saw_val=' || NEW.val; RETURN NEW; END;
$$;
CREATE TRIGGER bt_trg BEFORE UPDATE ON bt_test FOR EACH ROW EXECUTE FUNCTION bt_fn();

-- ============================================================================
-- 1. BEFORE trigger sees NEW.val = 99 (post-SET), not 10 (pre-SET)
-- ============================================================================

UPDATE bt_test SET val = 99 WHERE id = 1;

-- begin-expected
-- columns: val|audit
-- row: 99|saw_val=99
-- end-expected
SELECT val, audit FROM bt_test WHERE id = 1;

-- Cleanup
DROP TABLE bt_test;
