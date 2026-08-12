-- ============================================================================
-- A partition fires its own triggers
-- ============================================================================
-- A FOR EACH ROW trigger belongs to the relation the row is really stored in.
-- PostgreSQL gives a partition a copy of every row trigger written on the
-- partitioned table above it, so a write that names the parent runs those
-- copies beside whatever was written on the partition itself, in trigger-name
-- order, and reports the partition in TG_TABLE_NAME and TG_RELNAME. A FOR EACH
-- STATEMENT trigger is not copied: it belongs to the relation the statement
-- named. An inheritance child is given no copy and fires only its own.
-- ============================================================================

CREATE TABLE zzy8ct_log (n serial PRIMARY KEY, m text);
CREATE FUNCTION zzy8ct_t() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zzy8ct_log(m) VALUES (TG_NAME || '/' || TG_WHEN || '/' || TG_OP || '/' || TG_LEVEL || '/' || TG_TABLE_NAME || '/' || TG_RELNAME || '/a=' || coalesce(TG_ARGV[0],'-') || '/n=' || TG_NARGS); IF TG_LEVEL = 'STATEMENT' THEN RETURN NULL; END IF; IF TG_OP = 'DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$;
CREATE FUNCTION zzy8ct_veto() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zzy8ct_log(m) VALUES ('VETO/' || TG_NAME || '/' || TG_WHEN || '/' || TG_OP || '/' || TG_TABLE_NAME); RETURN NULL; END $$;
CREATE FUNCTION zzy8ct_mark() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.s := NEW.s || '+m'; INSERT INTO zzy8ct_log(m) VALUES ('MARK/' || TG_NAME || '/' || TG_WHEN || '/' || TG_OP || '/' || TG_TABLE_NAME || '/new=' || NEW.i || ':' || NEW.s); RETURN NEW; END $$;

-- ----------------------------------------------------------------------------
-- 1. Every timing, event and level at once, for a write naming the parent and
--    a write naming the partition
-- ----------------------------------------------------------------------------
CREATE TABLE zzy8ct_p (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzy8ct_p1 PARTITION OF zzy8ct_p FOR VALUES FROM (0) TO (10);
CREATE TABLE zzy8ct_p2 PARTITION OF zzy8ct_p FOR VALUES FROM (10) TO (20);
CREATE TRIGGER zzy8ct_a_par BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_p FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('par');
CREATE TRIGGER zzy8ct_b_prt BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_p1 FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('prt');
CREATE TRIGGER zzy8ct_c_par AFTER INSERT OR UPDATE OR DELETE ON zzy8ct_p FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('apar');
CREATE TRIGGER zzy8ct_d_prt AFTER INSERT OR UPDATE OR DELETE ON zzy8ct_p1 FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('aprt');
CREATE TRIGGER zzy8ct_e_pars BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_p FOR EACH STATEMENT EXECUTE FUNCTION zzy8ct_t('spar');
CREATE TRIGGER zzy8ct_f_prts BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_p1 FOR EACH STATEMENT EXECUTE FUNCTION zzy8ct_t('sprt');
CREATE TRIGGER zzy8ct_g_pars AFTER INSERT OR UPDATE OR DELETE ON zzy8ct_p FOR EACH STATEMENT EXECUTE FUNCTION zzy8ct_t('aspar');
CREATE TRIGGER zzy8ct_h_prts AFTER INSERT OR UPDATE OR DELETE ON zzy8ct_p1 FOR EACH STATEMENT EXECUTE FUNCTION zzy8ct_t('asprt');

-- 1a. INSERT naming the parent: the parent's statement triggers around the
--     partition's row triggers, its own beside its copy of the parent's, in
--     trigger-name order
INSERT INTO zzy8ct_p VALUES (1,'a');

-- begin-expected
-- columns: m
-- row: zzy8ct_e_pars/BEFORE/INSERT/STATEMENT/zzy8ct_p/zzy8ct_p/a=spar/n=1
-- row: zzy8ct_a_par/BEFORE/INSERT/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/INSERT/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_c_par/AFTER/INSERT/ROW/zzy8ct_p1/zzy8ct_p1/a=apar/n=1
-- row: zzy8ct_d_prt/AFTER/INSERT/ROW/zzy8ct_p1/zzy8ct_p1/a=aprt/n=1
-- row: zzy8ct_g_pars/AFTER/INSERT/STATEMENT/zzy8ct_p/zzy8ct_p/a=aspar/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DELETE FROM zzy8ct_log;

-- 1b. UPDATE naming the parent
UPDATE zzy8ct_p SET s='b' WHERE i=1;

-- begin-expected
-- columns: m
-- row: zzy8ct_e_pars/BEFORE/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=spar/n=1
-- row: zzy8ct_a_par/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_c_par/AFTER/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=apar/n=1
-- row: zzy8ct_d_prt/AFTER/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=aprt/n=1
-- row: zzy8ct_g_pars/AFTER/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=aspar/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DELETE FROM zzy8ct_log;

-- 1c. DELETE naming the parent
DELETE FROM zzy8ct_p WHERE i=1;

-- begin-expected
-- columns: m
-- row: zzy8ct_e_pars/BEFORE/DELETE/STATEMENT/zzy8ct_p/zzy8ct_p/a=spar/n=1
-- row: zzy8ct_a_par/BEFORE/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_c_par/AFTER/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=apar/n=1
-- row: zzy8ct_d_prt/AFTER/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=aprt/n=1
-- row: zzy8ct_g_pars/AFTER/DELETE/STATEMENT/zzy8ct_p/zzy8ct_p/a=aspar/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DELETE FROM zzy8ct_log;

-- 1d. INSERT naming the partition: the same row triggers, the partition's own
--     statement triggers instead of the parent's
INSERT INTO zzy8ct_p1 VALUES (2,'a');

-- begin-expected
-- columns: m
-- row: zzy8ct_f_prts/BEFORE/INSERT/STATEMENT/zzy8ct_p1/zzy8ct_p1/a=sprt/n=1
-- row: zzy8ct_a_par/BEFORE/INSERT/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/INSERT/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_c_par/AFTER/INSERT/ROW/zzy8ct_p1/zzy8ct_p1/a=apar/n=1
-- row: zzy8ct_d_prt/AFTER/INSERT/ROW/zzy8ct_p1/zzy8ct_p1/a=aprt/n=1
-- row: zzy8ct_h_prts/AFTER/INSERT/STATEMENT/zzy8ct_p1/zzy8ct_p1/a=asprt/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DELETE FROM zzy8ct_log;

-- 1e. UPDATE naming the partition
UPDATE zzy8ct_p1 SET s='b' WHERE i=2;

-- begin-expected
-- columns: m
-- row: zzy8ct_f_prts/BEFORE/UPDATE/STATEMENT/zzy8ct_p1/zzy8ct_p1/a=sprt/n=1
-- row: zzy8ct_a_par/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_c_par/AFTER/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=apar/n=1
-- row: zzy8ct_d_prt/AFTER/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=aprt/n=1
-- row: zzy8ct_h_prts/AFTER/UPDATE/STATEMENT/zzy8ct_p1/zzy8ct_p1/a=asprt/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DELETE FROM zzy8ct_log;

-- 1f. DELETE naming the partition
DELETE FROM zzy8ct_p1 WHERE i=2;

-- begin-expected
-- columns: m
-- row: zzy8ct_f_prts/BEFORE/DELETE/STATEMENT/zzy8ct_p1/zzy8ct_p1/a=sprt/n=1
-- row: zzy8ct_a_par/BEFORE/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_c_par/AFTER/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=apar/n=1
-- row: zzy8ct_d_prt/AFTER/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=aprt/n=1
-- row: zzy8ct_h_prts/AFTER/DELETE/STATEMENT/zzy8ct_p1/zzy8ct_p1/a=asprt/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DELETE FROM zzy8ct_log;

-- 1g. A row landing in the partition that carries no triggers of its own runs
--     only its copies of the parent's
INSERT INTO zzy8ct_p VALUES (12,'a');

-- begin-expected
-- columns: m
-- row: zzy8ct_e_pars/BEFORE/INSERT/STATEMENT/zzy8ct_p/zzy8ct_p/a=spar/n=1
-- row: zzy8ct_a_par/BEFORE/INSERT/ROW/zzy8ct_p2/zzy8ct_p2/a=par/n=1
-- row: zzy8ct_c_par/AFTER/INSERT/ROW/zzy8ct_p2/zzy8ct_p2/a=apar/n=1
-- row: zzy8ct_g_pars/AFTER/INSERT/STATEMENT/zzy8ct_p/zzy8ct_p/a=aspar/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DELETE FROM zzy8ct_log;

UPDATE zzy8ct_p SET s='b' WHERE i=12;

-- begin-expected
-- columns: m
-- row: zzy8ct_e_pars/BEFORE/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=spar/n=1
-- row: zzy8ct_a_par/BEFORE/UPDATE/ROW/zzy8ct_p2/zzy8ct_p2/a=par/n=1
-- row: zzy8ct_c_par/AFTER/UPDATE/ROW/zzy8ct_p2/zzy8ct_p2/a=apar/n=1
-- row: zzy8ct_g_pars/AFTER/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=aspar/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DELETE FROM zzy8ct_p;
DELETE FROM zzy8ct_log;

-- ----------------------------------------------------------------------------
-- 2. A row that changes partition is carried out as a delete and an insert
-- ----------------------------------------------------------------------------
INSERT INTO zzy8ct_p VALUES (1,'a');
DELETE FROM zzy8ct_log;
UPDATE zzy8ct_p SET i=12 WHERE i=1;

-- The source's BEFORE UPDATE runs, then the source's BEFORE DELETE and the
-- destination's BEFORE INSERT. No AFTER UPDATE fires for the row at all.
-- begin-expected
-- columns: m
-- row: zzy8ct_e_pars/BEFORE/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=spar/n=1
-- row: zzy8ct_a_par/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_a_par/BEFORE/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_a_par/BEFORE/INSERT/ROW/zzy8ct_p2/zzy8ct_p2/a=par/n=1
-- row: zzy8ct_c_par/AFTER/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=apar/n=1
-- row: zzy8ct_d_prt/AFTER/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=aprt/n=1
-- row: zzy8ct_c_par/AFTER/INSERT/ROW/zzy8ct_p2/zzy8ct_p2/a=apar/n=1
-- row: zzy8ct_g_pars/AFTER/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=aspar/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;

-- begin-expected
-- columns: w, i, s
-- row: p2, 12, a
-- end-expected
SELECT 'p1' AS w, i, s FROM zzy8ct_p1 UNION ALL SELECT 'p2', i, s FROM zzy8ct_p2 ORDER BY w, i;
DELETE FROM zzy8ct_p;

-- 2b. What the source's BEFORE UPDATE leaves in NEW is what the destination is
--     offered, and what is stored
INSERT INTO zzy8ct_p VALUES (1,'a');
CREATE TRIGGER zzy8ct_z_mark BEFORE UPDATE ON zzy8ct_p1 FOR EACH ROW EXECUTE FUNCTION zzy8ct_mark();
DELETE FROM zzy8ct_log;
UPDATE zzy8ct_p SET i=12 WHERE i=1;

-- begin-expected
-- columns: m
-- row: zzy8ct_e_pars/BEFORE/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=spar/n=1
-- row: zzy8ct_a_par/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: MARK/zzy8ct_z_mark/BEFORE/UPDATE/zzy8ct_p1/new=12:a+m
-- row: zzy8ct_a_par/BEFORE/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_a_par/BEFORE/INSERT/ROW/zzy8ct_p2/zzy8ct_p2/a=par/n=1
-- row: zzy8ct_c_par/AFTER/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=apar/n=1
-- row: zzy8ct_d_prt/AFTER/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=aprt/n=1
-- row: zzy8ct_c_par/AFTER/INSERT/ROW/zzy8ct_p2/zzy8ct_p2/a=apar/n=1
-- row: zzy8ct_g_pars/AFTER/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=aspar/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;

-- begin-expected
-- columns: w, i, s
-- row: p2, 12, a+m
-- end-expected
SELECT 'p1' AS w, i, s FROM zzy8ct_p1 UNION ALL SELECT 'p2', i, s FROM zzy8ct_p2 ORDER BY w, i;
DROP TRIGGER zzy8ct_z_mark ON zzy8ct_p1;
DELETE FROM zzy8ct_p;

-- 2c. A BEFORE DELETE on the source that returns NULL keeps the row where it was
INSERT INTO zzy8ct_p VALUES (1,'a');
CREATE TRIGGER zzy8ct_z_veto BEFORE DELETE ON zzy8ct_p1 FOR EACH ROW EXECUTE FUNCTION zzy8ct_veto();
DELETE FROM zzy8ct_log;
UPDATE zzy8ct_p SET i=12 WHERE i=1;

-- begin-expected
-- columns: m
-- row: zzy8ct_e_pars/BEFORE/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=spar/n=1
-- row: zzy8ct_a_par/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_a_par/BEFORE/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: VETO/zzy8ct_z_veto/BEFORE/DELETE/zzy8ct_p1
-- row: zzy8ct_g_pars/AFTER/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=aspar/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;

-- begin-expected
-- columns: w, i, s
-- row: p1, 1, a
-- end-expected
SELECT 'p1' AS w, i, s FROM zzy8ct_p1 UNION ALL SELECT 'p2', i, s FROM zzy8ct_p2 ORDER BY w, i;
DROP TRIGGER zzy8ct_z_veto ON zzy8ct_p1;

-- 2d. A BEFORE INSERT on the destination that returns NULL leaves the row
--     stored nowhere: the delete has already been carried out
CREATE TRIGGER zzy8ct_z_veto2 BEFORE INSERT ON zzy8ct_p2 FOR EACH ROW EXECUTE FUNCTION zzy8ct_veto();
DELETE FROM zzy8ct_log;
UPDATE zzy8ct_p SET i=12 WHERE i=1;

-- begin-expected
-- columns: m
-- row: zzy8ct_e_pars/BEFORE/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=spar/n=1
-- row: zzy8ct_a_par/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_a_par/BEFORE/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_a_par/BEFORE/INSERT/ROW/zzy8ct_p2/zzy8ct_p2/a=par/n=1
-- row: VETO/zzy8ct_z_veto2/BEFORE/INSERT/zzy8ct_p2
-- row: zzy8ct_c_par/AFTER/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=apar/n=1
-- row: zzy8ct_d_prt/AFTER/DELETE/ROW/zzy8ct_p1/zzy8ct_p1/a=aprt/n=1
-- row: zzy8ct_g_pars/AFTER/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=aspar/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*) AS cnt FROM zzy8ct_p;
DROP TRIGGER zzy8ct_z_veto2 ON zzy8ct_p2;

-- 2e. A BEFORE UPDATE that returns NULL stops the move before either half
INSERT INTO zzy8ct_p VALUES (1,'a');
CREATE TRIGGER zzy8ct_z_veto3 BEFORE UPDATE ON zzy8ct_p1 FOR EACH ROW EXECUTE FUNCTION zzy8ct_veto();
DELETE FROM zzy8ct_log;
UPDATE zzy8ct_p SET i=12 WHERE i=1;

-- begin-expected
-- columns: m
-- row: zzy8ct_e_pars/BEFORE/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=spar/n=1
-- row: zzy8ct_a_par/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=par/n=1
-- row: zzy8ct_b_prt/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: VETO/zzy8ct_z_veto3/BEFORE/UPDATE/zzy8ct_p1
-- row: zzy8ct_g_pars/AFTER/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=aspar/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;

-- begin-expected
-- columns: w, i, s
-- row: p1, 1, a
-- end-expected
SELECT 'p1' AS w, i, s FROM zzy8ct_p1 UNION ALL SELECT 'p2', i, s FROM zzy8ct_p2 ORDER BY w, i;
DROP TRIGGER zzy8ct_z_veto3 ON zzy8ct_p1;
DELETE FROM zzy8ct_p;

-- ----------------------------------------------------------------------------
-- 3. Turning a trigger off on the partitioned table reaches every copy
-- ----------------------------------------------------------------------------
INSERT INTO zzy8ct_p VALUES (1,'a');
ALTER TABLE zzy8ct_p DISABLE TRIGGER zzy8ct_a_par;

-- begin-expected
-- columns: rel, tgenabled
-- row: zzy8ct_p, D
-- row: zzy8ct_p1, D
-- row: zzy8ct_p2, D
-- end-expected
SELECT tgrelid::regclass::text AS rel, tgenabled FROM pg_trigger WHERE tgname = 'zzy8ct_a_par' ORDER BY rel;
DELETE FROM zzy8ct_log;
UPDATE zzy8ct_p SET s='c' WHERE i=1;

-- begin-expected
-- columns: m
-- row: zzy8ct_e_pars/BEFORE/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=spar/n=1
-- row: zzy8ct_b_prt/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_c_par/AFTER/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=apar/n=1
-- row: zzy8ct_d_prt/AFTER/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=aprt/n=1
-- row: zzy8ct_g_pars/AFTER/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=aspar/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
ALTER TABLE zzy8ct_p ENABLE TRIGGER zzy8ct_a_par;

-- 3b. A partition may turn its own copy off on its own
ALTER TABLE zzy8ct_p1 DISABLE TRIGGER zzy8ct_a_par;

-- begin-expected
-- columns: rel, tgenabled
-- row: zzy8ct_p, O
-- row: zzy8ct_p1, D
-- row: zzy8ct_p2, O
-- end-expected
SELECT tgrelid::regclass::text AS rel, tgenabled FROM pg_trigger WHERE tgname = 'zzy8ct_a_par' ORDER BY rel;
DELETE FROM zzy8ct_log;
UPDATE zzy8ct_p SET s='d' WHERE i=1;

-- begin-expected
-- columns: m
-- row: zzy8ct_e_pars/BEFORE/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=spar/n=1
-- row: zzy8ct_b_prt/BEFORE/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=prt/n=1
-- row: zzy8ct_c_par/AFTER/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=apar/n=1
-- row: zzy8ct_d_prt/AFTER/UPDATE/ROW/zzy8ct_p1/zzy8ct_p1/a=aprt/n=1
-- row: zzy8ct_g_pars/AFTER/UPDATE/STATEMENT/zzy8ct_p/zzy8ct_p/a=aspar/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;

-- 3c. ENABLE ALWAYS reaches the copies too; ONLY leaves them where they were
ALTER TABLE zzy8ct_p ENABLE ALWAYS TRIGGER zzy8ct_a_par;

-- begin-expected
-- columns: rel, tgenabled
-- row: zzy8ct_p, A
-- row: zzy8ct_p1, A
-- row: zzy8ct_p2, A
-- end-expected
SELECT tgrelid::regclass::text AS rel, tgenabled FROM pg_trigger WHERE tgname = 'zzy8ct_a_par' ORDER BY rel;
ALTER TABLE ONLY zzy8ct_p DISABLE TRIGGER zzy8ct_a_par;

-- begin-expected
-- columns: rel, tgenabled
-- row: zzy8ct_p, D
-- row: zzy8ct_p1, A
-- row: zzy8ct_p2, A
-- end-expected
SELECT tgrelid::regclass::text AS rel, tgenabled FROM pg_trigger WHERE tgname = 'zzy8ct_a_par' ORDER BY rel;
ALTER TABLE zzy8ct_p ENABLE TRIGGER zzy8ct_a_par;
DROP TABLE zzy8ct_p CASCADE;

-- ----------------------------------------------------------------------------
-- 4. A partition of a partition: the copies go all the way down, and it is the
--    leaf that fires them
-- ----------------------------------------------------------------------------
CREATE TABLE zzy8ct_q (i int, j int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzy8ct_q1 PARTITION OF zzy8ct_q FOR VALUES FROM (0) TO (10) PARTITION BY RANGE (j);
CREATE TABLE zzy8ct_q1a PARTITION OF zzy8ct_q1 FOR VALUES FROM (0) TO (10);
CREATE TRIGGER zzy8ct_a_top BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_q FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('top');
CREATE TRIGGER zzy8ct_b_mid BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_q1 FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('mid');
CREATE TRIGGER zzy8ct_c_leaf BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_q1a FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('leaf');
CREATE TRIGGER zzy8ct_d_top AFTER INSERT OR UPDATE OR DELETE ON zzy8ct_q FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('atop');
CREATE TRIGGER zzy8ct_e_tops BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_q FOR EACH STATEMENT EXECUTE FUNCTION zzy8ct_t('stop');
CREATE TRIGGER zzy8ct_f_mids BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_q1 FOR EACH STATEMENT EXECUTE FUNCTION zzy8ct_t('smid');
DELETE FROM zzy8ct_log;
INSERT INTO zzy8ct_q VALUES (1,1,'a');

-- begin-expected
-- columns: m
-- row: zzy8ct_e_tops/BEFORE/INSERT/STATEMENT/zzy8ct_q/zzy8ct_q/a=stop/n=1
-- row: zzy8ct_a_top/BEFORE/INSERT/ROW/zzy8ct_q1a/zzy8ct_q1a/a=top/n=1
-- row: zzy8ct_b_mid/BEFORE/INSERT/ROW/zzy8ct_q1a/zzy8ct_q1a/a=mid/n=1
-- row: zzy8ct_c_leaf/BEFORE/INSERT/ROW/zzy8ct_q1a/zzy8ct_q1a/a=leaf/n=1
-- row: zzy8ct_d_top/AFTER/INSERT/ROW/zzy8ct_q1a/zzy8ct_q1a/a=atop/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DELETE FROM zzy8ct_log;

-- 4b. Naming the middle relation fires its statement triggers and the leaf's
--     row triggers
UPDATE zzy8ct_q1 SET s='c' WHERE i=1;

-- begin-expected
-- columns: m
-- row: zzy8ct_f_mids/BEFORE/UPDATE/STATEMENT/zzy8ct_q1/zzy8ct_q1/a=smid/n=1
-- row: zzy8ct_a_top/BEFORE/UPDATE/ROW/zzy8ct_q1a/zzy8ct_q1a/a=top/n=1
-- row: zzy8ct_b_mid/BEFORE/UPDATE/ROW/zzy8ct_q1a/zzy8ct_q1a/a=mid/n=1
-- row: zzy8ct_c_leaf/BEFORE/UPDATE/ROW/zzy8ct_q1a/zzy8ct_q1a/a=leaf/n=1
-- row: zzy8ct_d_top/AFTER/UPDATE/ROW/zzy8ct_q1a/zzy8ct_q1a/a=atop/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DELETE FROM zzy8ct_log;

DELETE FROM zzy8ct_q WHERE i=1;

-- begin-expected
-- columns: m
-- row: zzy8ct_e_tops/BEFORE/DELETE/STATEMENT/zzy8ct_q/zzy8ct_q/a=stop/n=1
-- row: zzy8ct_a_top/BEFORE/DELETE/ROW/zzy8ct_q1a/zzy8ct_q1a/a=top/n=1
-- row: zzy8ct_b_mid/BEFORE/DELETE/ROW/zzy8ct_q1a/zzy8ct_q1a/a=mid/n=1
-- row: zzy8ct_c_leaf/BEFORE/DELETE/ROW/zzy8ct_q1a/zzy8ct_q1a/a=leaf/n=1
-- row: zzy8ct_d_top/AFTER/DELETE/ROW/zzy8ct_q1a/zzy8ct_q1a/a=atop/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DROP TABLE zzy8ct_q CASCADE;

-- ----------------------------------------------------------------------------
-- 5. An inheritance child is given no copy: only its own row triggers fire
-- ----------------------------------------------------------------------------
CREATE TABLE zzy8ct_ip (i int, s text);
CREATE TABLE zzy8ct_ic (i int, s text) INHERITS (zzy8ct_ip);
CREATE TRIGGER zzy8ct_a_ipar BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_ip FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('ipar');
CREATE TRIGGER zzy8ct_b_ichd BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_ic FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('ichd');
CREATE TRIGGER zzy8ct_e_ipars BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_ip FOR EACH STATEMENT EXECUTE FUNCTION zzy8ct_t('sipar');
CREATE TRIGGER zzy8ct_f_ichds BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_ic FOR EACH STATEMENT EXECUTE FUNCTION zzy8ct_t('sichd');
INSERT INTO zzy8ct_ic VALUES (1,'a');
DELETE FROM zzy8ct_log;
UPDATE zzy8ct_ip SET s='b' WHERE i=1;

-- begin-expected
-- columns: m
-- row: zzy8ct_e_ipars/BEFORE/UPDATE/STATEMENT/zzy8ct_ip/zzy8ct_ip/a=sipar/n=1
-- row: zzy8ct_b_ichd/BEFORE/UPDATE/ROW/zzy8ct_ic/zzy8ct_ic/a=ichd/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DELETE FROM zzy8ct_log;

DELETE FROM zzy8ct_ip WHERE i=1;

-- begin-expected
-- columns: m
-- row: zzy8ct_e_ipars/BEFORE/DELETE/STATEMENT/zzy8ct_ip/zzy8ct_ip/a=sipar/n=1
-- row: zzy8ct_b_ichd/BEFORE/DELETE/ROW/zzy8ct_ic/zzy8ct_ic/a=ichd/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DROP TABLE zzy8ct_ip CASCADE;

-- ----------------------------------------------------------------------------
-- 6. MERGE against the parent, and INSERT ... ON CONFLICT DO UPDATE on it
-- ----------------------------------------------------------------------------
CREATE TABLE zzy8ct_m (i int PRIMARY KEY, s text) PARTITION BY RANGE (i);
CREATE TABLE zzy8ct_m1 PARTITION OF zzy8ct_m FOR VALUES FROM (0) TO (10);
CREATE TABLE zzy8ct_m2 PARTITION OF zzy8ct_m FOR VALUES FROM (10) TO (20);
CREATE TABLE zzy8ct_ms (i int, s text);
INSERT INTO zzy8ct_ms VALUES (1,'m1'),(3,'m3'),(13,'m13');
CREATE TRIGGER zzy8ct_a_mpar BEFORE INSERT OR UPDATE ON zzy8ct_m FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('mpar');
CREATE TRIGGER zzy8ct_b_m1 BEFORE INSERT OR UPDATE ON zzy8ct_m1 FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('m1');
INSERT INTO zzy8ct_m VALUES (1,'old');
DELETE FROM zzy8ct_log;
MERGE INTO zzy8ct_m t USING zzy8ct_ms u ON t.i = u.i WHEN MATCHED THEN UPDATE SET s = u.s WHEN NOT MATCHED THEN INSERT (i,s) VALUES (u.i,u.s);

-- begin-expected
-- columns: m
-- row: zzy8ct_a_mpar/BEFORE/UPDATE/ROW/zzy8ct_m1/zzy8ct_m1/a=mpar/n=1
-- row: zzy8ct_b_m1/BEFORE/UPDATE/ROW/zzy8ct_m1/zzy8ct_m1/a=m1/n=1
-- row: zzy8ct_a_mpar/BEFORE/INSERT/ROW/zzy8ct_m1/zzy8ct_m1/a=mpar/n=1
-- row: zzy8ct_b_m1/BEFORE/INSERT/ROW/zzy8ct_m1/zzy8ct_m1/a=m1/n=1
-- row: zzy8ct_a_mpar/BEFORE/INSERT/ROW/zzy8ct_m2/zzy8ct_m2/a=mpar/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;

-- begin-expected
-- columns: i, s
-- row: 1, m1
-- row: 3, m3
-- row: 13, m13
-- end-expected
SELECT i, s FROM zzy8ct_m ORDER BY i;
DELETE FROM zzy8ct_log;

INSERT INTO zzy8ct_m VALUES (1,'again') ON CONFLICT (i) DO UPDATE SET s = 'conflicted';

-- begin-expected
-- columns: m
-- row: zzy8ct_a_mpar/BEFORE/INSERT/ROW/zzy8ct_m1/zzy8ct_m1/a=mpar/n=1
-- row: zzy8ct_b_m1/BEFORE/INSERT/ROW/zzy8ct_m1/zzy8ct_m1/a=m1/n=1
-- row: zzy8ct_a_mpar/BEFORE/UPDATE/ROW/zzy8ct_m1/zzy8ct_m1/a=mpar/n=1
-- row: zzy8ct_b_m1/BEFORE/UPDATE/ROW/zzy8ct_m1/zzy8ct_m1/a=m1/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;

-- begin-expected
-- columns: i, s
-- row: 1, conflicted
-- row: 3, m3
-- row: 13, m13
-- end-expected
SELECT i, s FROM zzy8ct_m ORDER BY i;
DROP TABLE zzy8ct_m CASCADE;
DROP TABLE zzy8ct_ms;

-- ----------------------------------------------------------------------------
-- 7. A partition attached later is given the copies; a detached one gives them
--    up; and the copy cannot be dropped on its own
-- ----------------------------------------------------------------------------
CREATE TABLE zzy8ct_at (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzy8ct_at1 PARTITION OF zzy8ct_at FOR VALUES FROM (0) TO (10);
CREATE TRIGGER zzy8ct_a_at BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_at FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('at');
CREATE TABLE zzy8ct_at2 (i int, s text);
ALTER TABLE zzy8ct_at ATTACH PARTITION zzy8ct_at2 FOR VALUES FROM (10) TO (20);

-- begin-expected
-- columns: rel
-- row: zzy8ct_at
-- row: zzy8ct_at1
-- row: zzy8ct_at2
-- end-expected
SELECT tgrelid::regclass::text AS rel FROM pg_trigger WHERE tgname = 'zzy8ct_a_at' ORDER BY rel;
DELETE FROM zzy8ct_log;
INSERT INTO zzy8ct_at VALUES (12,'a');

-- begin-expected
-- columns: m
-- row: zzy8ct_a_at/BEFORE/INSERT/ROW/zzy8ct_at2/zzy8ct_at2/a=at/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
ALTER TABLE zzy8ct_at DETACH PARTITION zzy8ct_at2;

-- begin-expected
-- columns: rel
-- row: zzy8ct_at
-- row: zzy8ct_at1
-- end-expected
SELECT tgrelid::regclass::text AS rel FROM pg_trigger WHERE tgname = 'zzy8ct_a_at' ORDER BY rel;
DELETE FROM zzy8ct_log;
INSERT INTO zzy8ct_at2 VALUES (13,'b');

-- begin-expected
-- columns: m
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DROP TABLE zzy8ct_at2;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop trigger zzy8ct_a_at on table zzy8ct_at1 because trigger zzy8ct_a_at on table zzy8ct_at requires it
-- end-expected-error
DROP TRIGGER zzy8ct_a_at ON zzy8ct_at1;
DROP TRIGGER zzy8ct_a_at ON zzy8ct_at;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*) AS cnt FROM pg_trigger WHERE tgname = 'zzy8ct_a_at';
DROP TABLE zzy8ct_at CASCADE;

-- ----------------------------------------------------------------------------
-- 8. A WHEN condition travels with the copy and is read against the row the
--    partition is being given
-- ----------------------------------------------------------------------------
CREATE TABLE zzy8ct_wn (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzy8ct_wn1 PARTITION OF zzy8ct_wn FOR VALUES FROM (0) TO (10);
CREATE TABLE zzy8ct_wn2 PARTITION OF zzy8ct_wn FOR VALUES FROM (10) TO (20);
CREATE TRIGGER zzy8ct_a_wn BEFORE INSERT ON zzy8ct_wn FOR EACH ROW WHEN (NEW.s = 'yes') EXECUTE FUNCTION zzy8ct_t('wn');
DELETE FROM zzy8ct_log;
INSERT INTO zzy8ct_wn VALUES (1,'yes'),(2,'no'),(12,'yes'),(13,'no');

-- begin-expected
-- columns: m
-- row: zzy8ct_a_wn/BEFORE/INSERT/ROW/zzy8ct_wn1/zzy8ct_wn1/a=wn/n=1
-- row: zzy8ct_a_wn/BEFORE/INSERT/ROW/zzy8ct_wn2/zzy8ct_wn2/a=wn/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DROP TABLE zzy8ct_wn CASCADE;

-- ----------------------------------------------------------------------------
-- 9. A BEFORE INSERT copy that returns NULL drops the row in whichever
--    partition it was headed for
-- ----------------------------------------------------------------------------
CREATE TABLE zzy8ct_sk (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzy8ct_sk1 PARTITION OF zzy8ct_sk FOR VALUES FROM (0) TO (10);
CREATE TABLE zzy8ct_sk2 PARTITION OF zzy8ct_sk FOR VALUES FROM (10) TO (20);
CREATE TRIGGER zzy8ct_z_skip BEFORE INSERT ON zzy8ct_sk FOR EACH ROW EXECUTE FUNCTION zzy8ct_veto();
DELETE FROM zzy8ct_log;
INSERT INTO zzy8ct_sk VALUES (1,'a'),(12,'b');

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*) AS cnt FROM zzy8ct_sk;

-- begin-expected
-- columns: m
-- row: VETO/zzy8ct_z_skip/BEFORE/INSERT/zzy8ct_sk1
-- row: VETO/zzy8ct_z_skip/BEFORE/INSERT/zzy8ct_sk2
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;
DROP TABLE zzy8ct_sk CASCADE;

-- ----------------------------------------------------------------------------
-- 10. One statement over two partitions: each row runs the triggers of the
--     partition it is stored in, and no others. Which partition the statement
--     reaches first is the plan's business, so each is read on its own.
-- ----------------------------------------------------------------------------
CREATE TABLE zzy8ct_tw (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzy8ct_tw1 PARTITION OF zzy8ct_tw FOR VALUES FROM (0) TO (10);
CREATE TABLE zzy8ct_tw2 PARTITION OF zzy8ct_tw FOR VALUES FROM (10) TO (20);
CREATE TRIGGER zzy8ct_a_tw BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_tw FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('tw');
CREATE TRIGGER zzy8ct_b_tw1 BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_tw1 FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('one');
CREATE TRIGGER zzy8ct_c_tw2 BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_tw2 FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('two');
INSERT INTO zzy8ct_tw VALUES (1,'a'),(12,'b');
DELETE FROM zzy8ct_log;
UPDATE zzy8ct_tw SET s = s || '!';

-- begin-expected
-- columns: m
-- row: zzy8ct_a_tw/BEFORE/UPDATE/ROW/zzy8ct_tw1/zzy8ct_tw1/a=tw/n=1
-- row: zzy8ct_b_tw1/BEFORE/UPDATE/ROW/zzy8ct_tw1/zzy8ct_tw1/a=one/n=1
-- end-expected
SELECT m FROM zzy8ct_log WHERE m LIKE '%/zzy8ct_tw1/%' ORDER BY n;

-- begin-expected
-- columns: m
-- row: zzy8ct_a_tw/BEFORE/UPDATE/ROW/zzy8ct_tw2/zzy8ct_tw2/a=tw/n=1
-- row: zzy8ct_c_tw2/BEFORE/UPDATE/ROW/zzy8ct_tw2/zzy8ct_tw2/a=two/n=1
-- end-expected
SELECT m FROM zzy8ct_log WHERE m LIKE '%/zzy8ct_tw2/%' ORDER BY n;
DELETE FROM zzy8ct_log;
DELETE FROM zzy8ct_tw;

-- begin-expected
-- columns: m
-- row: zzy8ct_a_tw/BEFORE/DELETE/ROW/zzy8ct_tw1/zzy8ct_tw1/a=tw/n=1
-- row: zzy8ct_b_tw1/BEFORE/DELETE/ROW/zzy8ct_tw1/zzy8ct_tw1/a=one/n=1
-- end-expected
SELECT m FROM zzy8ct_log WHERE m LIKE '%/zzy8ct_tw1/%' ORDER BY n;

-- begin-expected
-- columns: m
-- row: zzy8ct_a_tw/BEFORE/DELETE/ROW/zzy8ct_tw2/zzy8ct_tw2/a=tw/n=1
-- row: zzy8ct_c_tw2/BEFORE/DELETE/ROW/zzy8ct_tw2/zzy8ct_tw2/a=two/n=1
-- end-expected
SELECT m FROM zzy8ct_log WHERE m LIKE '%/zzy8ct_tw2/%' ORDER BY n;
DROP TABLE zzy8ct_tw CASCADE;

-- ----------------------------------------------------------------------------
-- 11. A move between two leaves of the same sub-partitioned relation is the
--     same delete and insert, and each leaf runs its own
-- ----------------------------------------------------------------------------
CREATE TABLE zzy8ct_at3 (i int, j int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzy8ct_at3a PARTITION OF zzy8ct_at3 FOR VALUES FROM (0) TO (10) PARTITION BY RANGE (j);
CREATE TABLE zzy8ct_at3a1 PARTITION OF zzy8ct_at3a FOR VALUES FROM (0) TO (10);
CREATE TABLE zzy8ct_at3a2 PARTITION OF zzy8ct_at3a FOR VALUES FROM (10) TO (20);
CREATE TRIGGER zzy8ct_a_top BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_at3 FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('top');
CREATE TRIGGER zzy8ct_b_a1 BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_at3a1 FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('a1');
CREATE TRIGGER zzy8ct_c_a2 BEFORE INSERT OR UPDATE OR DELETE ON zzy8ct_at3a2 FOR EACH ROW EXECUTE FUNCTION zzy8ct_t('a2');
INSERT INTO zzy8ct_at3 VALUES (1,1,'a');
DELETE FROM zzy8ct_log;
UPDATE zzy8ct_at3 SET j = 12 WHERE i = 1;

-- begin-expected
-- columns: m
-- row: zzy8ct_a_top/BEFORE/UPDATE/ROW/zzy8ct_at3a1/zzy8ct_at3a1/a=top/n=1
-- row: zzy8ct_b_a1/BEFORE/UPDATE/ROW/zzy8ct_at3a1/zzy8ct_at3a1/a=a1/n=1
-- row: zzy8ct_a_top/BEFORE/DELETE/ROW/zzy8ct_at3a1/zzy8ct_at3a1/a=top/n=1
-- row: zzy8ct_b_a1/BEFORE/DELETE/ROW/zzy8ct_at3a1/zzy8ct_at3a1/a=a1/n=1
-- row: zzy8ct_a_top/BEFORE/INSERT/ROW/zzy8ct_at3a2/zzy8ct_at3a2/a=top/n=1
-- row: zzy8ct_c_a2/BEFORE/INSERT/ROW/zzy8ct_at3a2/zzy8ct_at3a2/a=a2/n=1
-- end-expected
SELECT m FROM zzy8ct_log ORDER BY n;

-- begin-expected
-- columns: w, i, j, s
-- row: a2, 1, 12, a
-- end-expected
SELECT 'a1' AS w, i, j, s FROM zzy8ct_at3a1 UNION ALL SELECT 'a2', i, j, s FROM zzy8ct_at3a2 ORDER BY w;
DROP TABLE zzy8ct_at3 CASCADE;

DROP FUNCTION zzy8ct_t() CASCADE;
DROP FUNCTION zzy8ct_veto() CASCADE;
DROP FUNCTION zzy8ct_mark() CASCADE;
DROP TABLE zzy8ct_log;
