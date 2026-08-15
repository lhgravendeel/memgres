-- ============================================================================
-- ON DELETE CASCADE is a DELETE on the referencing table, so that table's FOR EACH STATEMENT
-- triggers fire for it: once for the whole statement however many parent rows the action was
-- reached for, and even when the action matches no row of the referencing table.
-- ============================================================================

DROP TABLE IF EXISTS rsq_child CASCADE;
DROP TABLE IF EXISTS rsq_parent CASCADE;
DROP TABLE IF EXISTS rsq_log CASCADE;
DROP FUNCTION IF EXISTS rsq_note() CASCADE;
CREATE TABLE rsq_log (seq serial, t text);
CREATE FUNCTION rsq_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO rsq_log (t) VALUES (TG_TABLE_NAME || '/' || TG_WHEN || '/' || TG_OP || '/' || TG_LEVEL); RETURN NULL; END $$;
CREATE TABLE rsq_parent (id int PRIMARY KEY);
CREATE TABLE rsq_child (id int PRIMARY KEY, p int REFERENCES rsq_parent(id) ON DELETE CASCADE);
INSERT INTO rsq_parent VALUES (1),(2),(3);
INSERT INTO rsq_child VALUES (10,1),(11,1),(20,2);
CREATE TRIGGER rsq_bs BEFORE DELETE ON rsq_child FOR EACH STATEMENT EXECUTE FUNCTION rsq_note();
CREATE TRIGGER rsq_as AFTER DELETE ON rsq_child FOR EACH STATEMENT EXECUTE FUNCTION rsq_note();
CREATE TRIGGER rsq_ar AFTER DELETE ON rsq_child FOR EACH ROW EXECUTE FUNCTION rsq_note();

DELETE FROM rsq_parent WHERE id IN (1,2);

-- begin-expected
-- columns: t
-- row: rsq_child/BEFORE/DELETE/STATEMENT
-- row: rsq_child/AFTER/DELETE/ROW
-- row: rsq_child/AFTER/DELETE/ROW
-- row: rsq_child/AFTER/DELETE/ROW
-- row: rsq_child/AFTER/DELETE/STATEMENT
-- end-expected
SELECT t FROM rsq_log ORDER BY seq;

DELETE FROM rsq_log;
DELETE FROM rsq_parent WHERE id = 3;

-- a parent row with no children still runs the action, so the statement triggers still fire
-- begin-expected
-- columns: t
-- row: rsq_child/BEFORE/DELETE/STATEMENT
-- row: rsq_child/AFTER/DELETE/STATEMENT
-- end-expected
SELECT t FROM rsq_log ORDER BY seq;

DELETE FROM rsq_log;
DELETE FROM rsq_parent WHERE id = 99;

-- no parent row means no action and nothing to fire
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM rsq_log;

DROP TABLE rsq_child;
DROP TABLE rsq_parent;
DROP TABLE rsq_log;
DROP FUNCTION rsq_note();

-- ============================================================================
-- ON UPDATE CASCADE and ON DELETE SET NULL are an UPDATE of the referencing table, and fire its
-- statement-level UPDATE triggers as an UPDATE a client wrote would.
-- ============================================================================

DROP TABLE IF EXISTS rsu_child CASCADE;
DROP TABLE IF EXISTS rsu_parent CASCADE;
DROP TABLE IF EXISTS rsu_log CASCADE;
DROP FUNCTION IF EXISTS rsu_note() CASCADE;
CREATE TABLE rsu_log (seq serial, t text);
CREATE FUNCTION rsu_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO rsu_log (t) VALUES (TG_TABLE_NAME || '/' || TG_WHEN || '/' || TG_OP || '/' || TG_LEVEL); RETURN NULL; END $$;
CREATE TABLE rsu_parent (id int PRIMARY KEY);
CREATE TABLE rsu_child (id int PRIMARY KEY, p int REFERENCES rsu_parent(id) ON UPDATE CASCADE ON DELETE SET NULL);
INSERT INTO rsu_parent VALUES (1),(2);
INSERT INTO rsu_child VALUES (10,1),(20,2);
CREATE TRIGGER rsu_bs BEFORE UPDATE ON rsu_child FOR EACH STATEMENT EXECUTE FUNCTION rsu_note();
CREATE TRIGGER rsu_as AFTER UPDATE ON rsu_child FOR EACH STATEMENT EXECUTE FUNCTION rsu_note();

UPDATE rsu_parent SET id = id + 100 WHERE id IN (1,2);

-- begin-expected
-- columns: t
-- row: rsu_child/BEFORE/UPDATE/STATEMENT
-- row: rsu_child/AFTER/UPDATE/STATEMENT
-- end-expected
SELECT t FROM rsu_log ORDER BY seq;

-- begin-expected
-- columns: r
-- row: 10=101
-- row: 20=102
-- end-expected
SELECT id::text || '=' || coalesce(p::text,'<null>') AS r FROM rsu_child ORDER BY id;

DELETE FROM rsu_log;
DELETE FROM rsu_parent;

-- begin-expected
-- columns: t
-- row: rsu_child/BEFORE/UPDATE/STATEMENT
-- row: rsu_child/AFTER/UPDATE/STATEMENT
-- end-expected
SELECT t FROM rsu_log ORDER BY seq;

-- begin-expected
-- columns: r
-- row: 10=<null>
-- row: 20=<null>
-- end-expected
SELECT id::text || '=' || coalesce(p::text,'<null>') AS r FROM rsu_child ORDER BY id;

DROP TABLE rsu_child;
DROP TABLE rsu_parent;
DROP TABLE rsu_log;
DROP FUNCTION rsu_note();

-- ============================================================================
-- A relation's statement-level triggers fire once for the statement, so a table that references
-- itself with ON DELETE CASCADE fires its own once and not twice.
-- ============================================================================

DROP TABLE IF EXISTS rss_t CASCADE;
DROP TABLE IF EXISTS rss_log CASCADE;
DROP FUNCTION IF EXISTS rss_note() CASCADE;
CREATE TABLE rss_log (seq serial, t text);
CREATE FUNCTION rss_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO rss_log (t) VALUES (TG_TABLE_NAME || '/' || TG_WHEN || '/' || TG_OP || '/' || TG_LEVEL); RETURN NULL; END $$;
CREATE TABLE rss_t (id int PRIMARY KEY, parent int REFERENCES rss_t(id) ON DELETE CASCADE);
INSERT INTO rss_t VALUES (1,NULL),(2,1),(3,2);
CREATE TRIGGER rss_bs BEFORE DELETE ON rss_t FOR EACH STATEMENT EXECUTE FUNCTION rss_note();
CREATE TRIGGER rss_as AFTER DELETE ON rss_t FOR EACH STATEMENT EXECUTE FUNCTION rss_note();

DELETE FROM rss_t WHERE id = 1;

-- begin-expected
-- columns: t
-- row: rss_t/BEFORE/DELETE/STATEMENT
-- row: rss_t/AFTER/DELETE/STATEMENT
-- end-expected
SELECT t FROM rss_log ORDER BY seq;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM rss_t;

DROP TABLE rss_t;
DROP TABLE rss_log;
DROP FUNCTION rss_note();

-- ============================================================================
-- One AFTER STATEMENT firing, and its OLD TABLE holds every row the action took -- from all three
-- parent rows at once, not one transition table per parent row.
-- ============================================================================

DROP TABLE IF EXISTS rsw_child CASCADE;
DROP TABLE IF EXISTS rsw_parent CASCADE;
DROP TABLE IF EXISTS rsw_log CASCADE;
DROP FUNCTION IF EXISTS rsw_note() CASCADE;
CREATE TABLE rsw_log (seq serial, t text);
CREATE FUNCTION rsw_note() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE n int; BEGIN SELECT count(*) INTO n FROM rsw_gone; INSERT INTO rsw_log (t) VALUES (TG_TABLE_NAME || '/oldtable=' || n); RETURN NULL; END $$;
CREATE TABLE rsw_parent (id int PRIMARY KEY);
CREATE TABLE rsw_child (id int PRIMARY KEY, p int REFERENCES rsw_parent(id) ON DELETE CASCADE);
INSERT INTO rsw_parent VALUES (1),(2),(3);
INSERT INTO rsw_child VALUES (10,1),(11,1),(20,2);
CREATE TRIGGER rsw_as AFTER DELETE ON rsw_child REFERENCING OLD TABLE AS rsw_gone FOR EACH STATEMENT EXECUTE FUNCTION rsw_note();

DELETE FROM rsw_parent WHERE id IN (1,2,3);

-- begin-expected
-- columns: t
-- row: rsw_child/oldtable=3
-- end-expected
SELECT t FROM rsw_log ORDER BY seq;

DROP TABLE rsw_child;
DROP TABLE rsw_parent;
DROP TABLE rsw_log;
DROP FUNCTION rsw_note();

-- ============================================================================
-- NO ACTION writes nothing to the referencing table, so nothing of its fires.
-- ============================================================================

DROP TABLE IF EXISTS rsn_child CASCADE;
DROP TABLE IF EXISTS rsn_parent CASCADE;
DROP TABLE IF EXISTS rsn_log CASCADE;
DROP FUNCTION IF EXISTS rsn_note() CASCADE;
CREATE TABLE rsn_log (seq serial, t text);
CREATE FUNCTION rsn_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO rsn_log (t) VALUES (TG_TABLE_NAME || '/' || TG_WHEN || '/' || TG_OP); RETURN NULL; END $$;
CREATE TABLE rsn_parent (id int PRIMARY KEY);
CREATE TABLE rsn_child (id int PRIMARY KEY, p int REFERENCES rsn_parent(id) ON DELETE NO ACTION);
INSERT INTO rsn_parent VALUES (1),(2);
INSERT INTO rsn_child VALUES (10,1);
CREATE TRIGGER rsn_bs BEFORE DELETE ON rsn_child FOR EACH STATEMENT EXECUTE FUNCTION rsn_note();
CREATE TRIGGER rsn_as AFTER DELETE ON rsn_child FOR EACH STATEMENT EXECUTE FUNCTION rsn_note();

DELETE FROM rsn_parent WHERE id = 2;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM rsn_log;

DROP TABLE rsn_child;
DROP TABLE rsn_parent;
DROP TABLE rsn_log;
DROP FUNCTION rsn_note();