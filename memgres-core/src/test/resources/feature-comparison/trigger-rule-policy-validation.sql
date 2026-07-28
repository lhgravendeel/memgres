-- ============================================================================
-- Feature Comparison: definition-time validation of triggers, rules, policies
--                     and views
-- Target: PostgreSQL 18 vs Memgres
--
-- Three kinds of stored logic are written once and evaluated much later, so a
-- definition that cannot work has to be refused while it is being written.
-- This file covers what PostgreSQL refuses — a trigger whose WHEN condition
-- names a row that event has no version of, a rule that would give a table two
-- definitions of its own contents, a policy whose expression is not a boolean —
-- alongside the neighbouring definitions that must keep being accepted.
-- ============================================================================

DROP VIEW IF EXISTS trp_v2 CASCADE;
DROP VIEW IF EXISTS trp_v CASCADE;
DROP TABLE IF EXISTS trp_t CASCADE;
DROP TABLE IF EXISTS trp_t2 CASCADE;
DROP TABLE IF EXISTS trp_log CASCADE;
DROP FUNCTION IF EXISTS trp_tf() CASCADE;
DROP FUNCTION IF EXISTS trp_notrig() CASCADE;

CREATE TABLE trp_t (i int, j text);
CREATE TABLE trp_t2 (i int, j text);
CREATE TABLE trp_log (m text);
CREATE VIEW trp_v AS SELECT i, j FROM trp_t;
CREATE FUNCTION trp_tf() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;
CREATE FUNCTION trp_notrig() RETURNS int AS $$ BEGIN RETURN 1; END $$ LANGUAGE plpgsql;

-- ============================================================================
-- SECTION A: CREATE TRIGGER — definitions that cannot fire correctly
-- ============================================================================

-- The function must exist and must return trigger.
CREATE TRIGGER trp_g1 BEFORE INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_nosuch();
CREATE TRIGGER trp_g2 BEFORE INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_notrig();

-- A statement trigger has no row for a WHEN condition to look at.
CREATE TRIGGER trp_g3 AFTER INSERT ON trp_t FOR EACH STATEMENT WHEN (NEW.i > 0) EXECUTE FUNCTION trp_tf();

-- An INSERT has no OLD row and a DELETE has no NEW row.
CREATE TRIGGER trp_g4 BEFORE INSERT ON trp_t FOR EACH ROW WHEN (OLD.i > 0) EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g5 BEFORE DELETE ON trp_t FOR EACH ROW WHEN (NEW.i > 0) EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g6 AFTER INSERT ON trp_t FOR EACH ROW WHEN (NEW.i > 0 AND OLD.i > 0) EXECUTE FUNCTION trp_tf();

-- Relation kind: INSTEAD OF belongs to a view, BEFORE/AFTER row triggers to a table.
CREATE TRIGGER trp_g7 INSTEAD OF INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g8 BEFORE INSERT ON trp_v FOR EACH ROW EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g9 INSTEAD OF INSERT ON trp_v FOR EACH STATEMENT EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g10 INSTEAD OF INSERT ON trp_v FOR EACH ROW WHEN (true) EXECUTE FUNCTION trp_tf();

-- UPDATE OF names columns of the relation.
CREATE TRIGGER trp_g11 BEFORE UPDATE OF nosuch ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g12 BEFORE UPDATE OF i, nosuch ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g13 BEFORE INSERT OF i ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf();

-- TRUNCATE removes every row at once; there is no per-row firing to do.
CREATE TRIGGER trp_g14 BEFORE TRUNCATE ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf();

-- A WHEN condition is resolved against the trigger's own OLD/NEW rows.
CREATE TRIGGER trp_g15 BEFORE INSERT ON trp_t FOR EACH ROW WHEN (NEW.nosuch > 0) EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g16 BEFORE INSERT ON trp_t FOR EACH ROW WHEN (NEW.i) EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g17 BEFORE INSERT ON trp_t FOR EACH ROW WHEN (i > 0) EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g18 BEFORE INSERT ON trp_t FOR EACH ROW WHEN (zz.i > 0) EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g19 BEFORE INSERT ON trp_t FOR EACH ROW WHEN (count(*) > 0) EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g20 BEFORE INSERT ON trp_t FOR EACH ROW WHEN ((SELECT count(*) FROM trp_t) > 0) EXECUTE FUNCTION trp_tf();

-- A transition table is built from a statement that has already run.
CREATE TRIGGER trp_g21 BEFORE INSERT ON trp_t REFERENCING NEW TABLE AS nt FOR EACH STATEMENT EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g22 AFTER INSERT ON trp_t REFERENCING OLD TABLE AS ot FOR EACH STATEMENT EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_g23 AFTER DELETE ON trp_t REFERENCING NEW TABLE AS nt FOR EACH STATEMENT EXECUTE FUNCTION trp_tf();

-- A constraint trigger is only ever AFTER ... FOR EACH ROW.
CREATE CONSTRAINT TRIGGER trp_g24 BEFORE INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf();
CREATE CONSTRAINT TRIGGER trp_g25 AFTER INSERT ON trp_t FOR EACH STATEMENT EXECUTE FUNCTION trp_tf();

-- The relation itself must exist.
CREATE TRIGGER trp_g26 BEFORE INSERT ON trp_nosuchtbl FOR EACH ROW EXECUTE FUNCTION trp_tf();

-- ============================================================================
-- SECTION B: CREATE TRIGGER — definitions that must keep being accepted
-- ============================================================================

CREATE TRIGGER trp_ok1 BEFORE INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_ok2 AFTER INSERT ON trp_t FOR EACH STATEMENT EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_ok3 BEFORE UPDATE ON trp_t FOR EACH ROW WHEN (OLD.i IS DISTINCT FROM NEW.i) EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_ok4 BEFORE TRUNCATE ON trp_t FOR EACH STATEMENT EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_ok5 AFTER INSERT ON trp_t REFERENCING NEW TABLE AS nt FOR EACH STATEMENT EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_ok6 AFTER DELETE ON trp_t REFERENCING OLD TABLE AS ot FOR EACH STATEMENT EXECUTE FUNCTION trp_tf();
CREATE TRIGGER trp_ok7 INSTEAD OF INSERT ON trp_v FOR EACH ROW EXECUTE FUNCTION trp_tf();
CREATE CONSTRAINT TRIGGER trp_ok8 AFTER INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf();

-- A second trigger of the same name on the same relation.
CREATE TRIGGER trp_ok1 BEFORE INSERT ON trp_t FOR EACH ROW EXECUTE FUNCTION trp_tf();
-- ... but the same name on a different relation is a different trigger.
CREATE TRIGGER trp_ok1 BEFORE INSERT ON trp_log FOR EACH ROW EXECUTE FUNCTION trp_tf();

-- ============================================================================
-- SECTION C: ENABLE / DISABLE TRIGGER
-- ============================================================================

-- Quietly doing nothing here reads as success to whoever asked for it off.
ALTER TABLE trp_t DISABLE TRIGGER trp_no_such;
ALTER TABLE trp_t ENABLE TRIGGER trp_no_such;
ALTER TABLE trp_t DISABLE TRIGGER trp_ok1;
ALTER TABLE trp_t ENABLE TRIGGER trp_ok1;
ALTER TABLE trp_t DISABLE TRIGGER ALL;
ALTER TABLE trp_t ENABLE TRIGGER ALL;
ALTER TABLE trp_t DISABLE TRIGGER USER;
ALTER TABLE trp_t ENABLE TRIGGER USER;

-- ============================================================================
-- SECTION D: CREATE RULE
-- ============================================================================

-- An ON SELECT rule is how a view is represented; a table may not carry one.
CREATE RULE trp_r1 AS ON SELECT TO trp_t DO INSTEAD NOTHING;
CREATE RULE trp_r2 AS ON SELECT TO trp_t DO INSTEAD SELECT * FROM trp_log;
CREATE RULE trp_r3 AS ON SELECT TO trp_t DO ALSO SELECT * FROM trp_log;
CREATE RULE "_RETURN" AS ON SELECT TO trp_t DO INSTEAD SELECT * FROM trp_log;

-- The event decides which of OLD and NEW the action may name.
CREATE RULE trp_r4 AS ON INSERT TO trp_t DO ALSO INSERT INTO trp_log VALUES (OLD.j);
CREATE RULE trp_r5 AS ON DELETE TO trp_t DO ALSO INSERT INTO trp_log VALUES (NEW.j);
CREATE RULE trp_r6 AS ON INSERT TO trp_t DO ALSO INSERT INTO trp_log VALUES (NEW.j);
CREATE RULE trp_r7 AS ON DELETE TO trp_t DO ALSO INSERT INTO trp_log VALUES (OLD.j);
CREATE RULE trp_r8 AS ON UPDATE TO trp_t DO ALSO INSERT INTO trp_log VALUES (OLD.j);
CREATE RULE trp_r9 AS ON UPDATE TO trp_t DO ALSO INSERT INTO trp_log VALUES (NEW.j);

-- Unknown event, unknown relation, duplicate name.
CREATE RULE trp_r10 AS ON nonsense TO trp_t DO INSTEAD NOTHING;
CREATE RULE trp_r11 AS ON INSERT TO trp_nosuchtbl DO INSTEAD NOTHING;
CREATE RULE trp_r12 AS ON INSERT TO trp_t2 DO INSTEAD NOTHING;
CREATE RULE trp_r12 AS ON INSERT TO trp_t2 DO INSTEAD NOTHING;
CREATE RULE trp_r12 AS ON INSERT TO trp_log DO INSTEAD NOTHING;

-- An action list that is never closed runs off the end of the statement.
CREATE RULE trp_r13 AS ON UPDATE TO trp_t DO ALSO ( INSERT INTO trp_log VALUES ('u1');
CREATE RULE trp_r14 AS ON INSERT TO trp_t DO INSTEAD (NOTHING);

-- A single action wrapped in parentheses is still one action.
CREATE RULE trp_r15 AS ON INSERT TO trp_log DO ALSO ( INSERT INTO trp_log VALUES ('c') );

-- ============================================================================
-- SECTION E: ENABLE / DISABLE RULE, SET WITHOUT CLUSTER
-- ============================================================================

CREATE RULE trp_r16 AS ON UPDATE TO trp_log DO ALSO INSERT INTO trp_log VALUES ('y');
ALTER TABLE trp_log DISABLE RULE trp_r16;
ALTER TABLE trp_log ENABLE RULE trp_r16;
ALTER TABLE trp_log DISABLE RULE trp_no_such_rule;
ALTER TABLE trp_t SET WITHOUT CLUSTER;

-- ============================================================================
-- SECTION F: CREATE POLICY
-- ============================================================================

-- The relation must exist and must be a table: a view has no row security.
CREATE POLICY trp_p1 ON trp_nosuchtbl FOR SELECT USING (true);
CREATE POLICY trp_p2 ON trp_v FOR SELECT USING (true);

-- Which clause a policy may carry follows from the command it guards.
CREATE POLICY trp_p3 ON trp_t FOR SELECT WITH CHECK (true);
CREATE POLICY trp_p4 ON trp_t FOR DELETE WITH CHECK (true);
CREATE POLICY trp_p5 ON trp_t FOR INSERT USING (true);

-- The expression is resolved against the table and must be a boolean.
CREATE POLICY trp_p6 ON trp_t FOR SELECT USING (i);
CREATE POLICY trp_p7 ON trp_t FOR SELECT USING (j);
CREATE POLICY trp_p8 ON trp_t FOR INSERT WITH CHECK (i);
CREATE POLICY trp_p9 ON trp_t FOR SELECT USING (nosuchcol = 1);
CREATE POLICY trp_p10 ON trp_t FOR SELECT USING (trp_t.nosuch = 1);
CREATE POLICY trp_p11 ON trp_t FOR UPDATE USING (true) WITH CHECK (nosuch > 0);
CREATE POLICY trp_p12 ON trp_t FOR SELECT USING (count(*) > 0);
CREATE POLICY trp_p13 ON trp_t FOR INSERT WITH CHECK (count(*) > 0);

-- Roles named must exist; the built-in role words do not need one.
CREATE POLICY trp_p14 ON trp_t TO trp_no_such_role USING (true);

-- Unrecognized options.
CREATE POLICY trp_p15 ON trp_t AS nonsense FOR SELECT USING (true);

-- Accepted policies, and a duplicate name.
CREATE POLICY trp_p16 ON trp_t FOR SELECT USING (i > 0);
CREATE POLICY trp_p16 ON trp_t FOR SELECT USING (i > 0);
CREATE POLICY trp_p16 ON trp_log FOR SELECT USING (true);
CREATE POLICY trp_p17 ON trp_t FOR INSERT WITH CHECK (i > 0);
CREATE POLICY trp_p18 ON trp_t USING (i > 0) WITH CHECK (i > 0);
CREATE POLICY trp_p19 ON trp_t AS RESTRICTIVE FOR ALL TO PUBLIC USING (true);
CREATE POLICY trp_p20 ON trp_t FOR UPDATE USING (true) WITH CHECK (true);
CREATE POLICY trp_p21 ON trp_t FOR SELECT USING ((SELECT count(*) FROM trp_log) > 0);
CREATE POLICY trp_p22 ON trp_t TO CURRENT_USER USING (true);

-- The command a policy guards is fixed when it is created.
ALTER POLICY trp_p16 ON trp_t FOR UPDATE USING (true);
ALTER POLICY trp_p16 ON trp_t USING (i > 1);
ALTER POLICY trp_p16 ON trp_t RENAME TO trp_p16b;
ALTER POLICY trp_no_such_policy ON trp_t USING (true);

-- ============================================================================
-- SECTION G: CREATE VIEW
-- ============================================================================

-- A view whose output repeats a column name cannot be read unambiguously.
CREATE VIEW trp_v2 AS SELECT i, i FROM trp_t;
CREATE VIEW trp_v2 AS SELECT i, i, i FROM trp_t;
CREATE VIEW trp_v2 AS SELECT i AS a, j AS a FROM trp_t;
CREATE VIEW trp_v2 (a, a) AS SELECT i, j FROM trp_t;
CREATE VIEW trp_v2 AS SELECT a.i, b.i FROM trp_t a, trp_t b;

-- A CHECK OPTION on a view no INSERT can reach can never be applied.
CREATE VIEW trp_v2 AS SELECT DISTINCT i FROM trp_t WITH CHECK OPTION;
CREATE VIEW trp_v2 AS SELECT DISTINCT i FROM trp_t WITH LOCAL CHECK OPTION;
CREATE VIEW trp_v2 AS SELECT count(*) AS c FROM trp_t WITH CHECK OPTION;
CREATE VIEW trp_v2 AS SELECT i FROM trp_t GROUP BY i WITH CHECK OPTION;
CREATE VIEW trp_v2 AS SELECT i FROM trp_t UNION SELECT i FROM trp_t WITH CHECK OPTION;
CREATE VIEW trp_v2 AS SELECT i FROM trp_t LIMIT 5 WITH CHECK OPTION;
CREATE VIEW trp_v2 AS SELECT a.i FROM trp_t a, trp_t b WITH CHECK OPTION;

-- Views that must keep being accepted.
CREATE VIEW trp_v2 AS SELECT i, i AS i2 FROM trp_t;
DROP VIEW trp_v2;
CREATE VIEW trp_v2 AS SELECT count(*) AS c FROM trp_t;
DROP VIEW trp_v2;
CREATE VIEW trp_v2 AS SELECT i FROM trp_t WHERE i > 0 WITH CHECK OPTION;
INSERT INTO trp_v2 VALUES (5);
SELECT i FROM trp_t ORDER BY i;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP VIEW IF EXISTS trp_v2 CASCADE;
DROP VIEW IF EXISTS trp_v CASCADE;
DROP TABLE IF EXISTS trp_t CASCADE;
DROP TABLE IF EXISTS trp_t2 CASCADE;
DROP TABLE IF EXISTS trp_log CASCADE;
DROP FUNCTION IF EXISTS trp_tf() CASCADE;
DROP FUNCTION IF EXISTS trp_notrig() CASCADE;
