-- ============================================================================
-- DEFAULT assigned through a view is the view's own column default, because
-- PostgreSQL substitutes it while it rewrites the write and the relation the
-- statement named is the one that answers. A rule stands in place of that
-- rewrite, so NEW carries what the view declares and nothing at all where the
-- view declares none: the relation underneath is never asked. It is asked when
-- the write does reach it.
-- ============================================================================

-- ============================================================================
-- A view carrying an INSTEAD rule on UPDATE
-- ============================================================================
CREATE TABLE vrdf_t1 (id int, a text DEFAULT 'BASE');
INSERT INTO vrdf_t1 VALUES (1,'o');
CREATE VIEW vrdf_v1 AS SELECT * FROM vrdf_t1;
ALTER VIEW vrdf_v1 ALTER COLUMN a SET DEFAULT 'VIEWD';
CREATE RULE vrdf_r1 AS ON UPDATE TO vrdf_v1 DO INSTEAD UPDATE vrdf_t1 SET a = NEW.a WHERE id = OLD.id;
UPDATE vrdf_v1 SET a = DEFAULT;

-- begin-expected
-- columns: id | a
-- row: 1 | VIEWD
-- end-expected
SELECT id, a FROM vrdf_t1;

DROP VIEW vrdf_v1;
DROP TABLE vrdf_t1;

-- where the view declares no default of its own, nothing does
CREATE TABLE vrdf_t2 (id int, a text DEFAULT 'BASE');
INSERT INTO vrdf_t2 VALUES (1,'o');
CREATE VIEW vrdf_v2 AS SELECT * FROM vrdf_t2;
CREATE RULE vrdf_r2 AS ON UPDATE TO vrdf_v2 DO INSTEAD UPDATE vrdf_t2 SET a = NEW.a WHERE id = OLD.id;
UPDATE vrdf_v2 SET a = DEFAULT;

-- begin-expected
-- columns: id | a
-- row: 1 | NULL
-- end-expected
SELECT id, a FROM vrdf_t2;

DROP VIEW vrdf_v2;
DROP TABLE vrdf_t2;

-- ============================================================================
-- An INSERT rule reads the same default, for the keyword and for a column left
-- out alike
-- ============================================================================
CREATE TABLE vrdf_t3 (id int, a text DEFAULT 'BASE');
CREATE VIEW vrdf_v3 AS SELECT * FROM vrdf_t3;
ALTER VIEW vrdf_v3 ALTER COLUMN a SET DEFAULT 'VIEWD';
CREATE RULE vrdf_r3 AS ON INSERT TO vrdf_v3 DO INSTEAD INSERT INTO vrdf_t3 VALUES (NEW.id, NEW.a);
INSERT INTO vrdf_v3 (id, a) VALUES (1, DEFAULT);
INSERT INTO vrdf_v3 (id) VALUES (2);

-- begin-expected
-- columns: id | a
-- row: 1 | VIEWD
-- row: 2 | VIEWD
-- end-expected
SELECT id, a FROM vrdf_t3 ORDER BY id;

-- a row of a many-row VALUES reads it the same way, and a literal stands
INSERT INTO vrdf_v3 (id, a) VALUES (3, DEFAULT), (4, 'lit');

-- begin-expected
-- columns: id | a
-- row: 3 | VIEWD
-- row: 4 | lit
-- end-expected
SELECT id, a FROM vrdf_t3 WHERE id > 2 ORDER BY id;

DROP VIEW vrdf_v3;
DROP TABLE vrdf_t3;

CREATE TABLE vrdf_t4 (id int, a text DEFAULT 'BASE');
CREATE VIEW vrdf_v4 AS SELECT * FROM vrdf_t4;
CREATE RULE vrdf_r4 AS ON INSERT TO vrdf_v4 DO INSTEAD INSERT INTO vrdf_t4 VALUES (NEW.id, NEW.a);
INSERT INTO vrdf_v4 (id) VALUES (1);

-- begin-expected
-- columns: id | a
-- row: 1 | NULL
-- end-expected
SELECT id, a FROM vrdf_t4;

DROP VIEW vrdf_v4;
DROP TABLE vrdf_t4;

-- ============================================================================
-- A column the view declares a default for and one it does not, in one write
-- ============================================================================
CREATE TABLE vrdf_t5 (id int, a text DEFAULT 'BASE', b text DEFAULT 'BB');
INSERT INTO vrdf_t5 VALUES (1,'o','p');
CREATE VIEW vrdf_v5 AS SELECT * FROM vrdf_t5;
ALTER VIEW vrdf_v5 ALTER COLUMN a SET DEFAULT 'VA';
CREATE RULE vrdf_r5 AS ON UPDATE TO vrdf_v5 DO INSTEAD UPDATE vrdf_t5 SET a = NEW.a, b = NEW.b WHERE id = OLD.id;
UPDATE vrdf_v5 SET a = DEFAULT, b = DEFAULT;

-- begin-expected
-- columns: id | a | b
-- row: 1 | VA | NULL
-- end-expected
SELECT id, a, b FROM vrdf_t5;

DROP VIEW vrdf_v5;
DROP TABLE vrdf_t5;

-- the view's default is whatever expression it was given, and DROP DEFAULT
-- leaves it with none
CREATE TABLE vrdf_t6 (id int, a text DEFAULT 'BASE');
CREATE TABLE vrdf_l6 (id int, a text);
CREATE VIEW vrdf_v6 AS SELECT * FROM vrdf_t6;
ALTER VIEW vrdf_v6 ALTER COLUMN a SET DEFAULT 'x' || 'y';
CREATE RULE vrdf_r6 AS ON INSERT TO vrdf_v6 DO INSTEAD INSERT INTO vrdf_l6 VALUES (NEW.id, NEW.a);
INSERT INTO vrdf_v6 (id) VALUES (1);
ALTER VIEW vrdf_v6 ALTER COLUMN a DROP DEFAULT;
INSERT INTO vrdf_v6 (id) VALUES (2);

-- begin-expected
-- columns: id | a
-- row: 1 | xy
-- row: 2 | NULL
-- end-expected
SELECT id, a FROM vrdf_l6 ORDER BY id;

DROP VIEW vrdf_v6;
DROP TABLE vrdf_t6;
DROP TABLE vrdf_l6;

-- DEFAULT VALUES asks the view for every column, and the relation's own
-- defaults are not a fallback for any of them
CREATE TABLE vrdf_t7 (id int DEFAULT 9, a text DEFAULT 'BASE');
CREATE TABLE vrdf_l7 (id int, a text);
CREATE VIEW vrdf_v7 AS SELECT * FROM vrdf_t7;
ALTER VIEW vrdf_v7 ALTER COLUMN a SET DEFAULT 'VA';
CREATE RULE vrdf_r7 AS ON INSERT TO vrdf_v7 DO INSTEAD INSERT INTO vrdf_l7 VALUES (NEW.id, NEW.a);
INSERT INTO vrdf_v7 DEFAULT VALUES;

-- begin-expected
-- columns: id | a
-- row: NULL | VA
-- end-expected
SELECT id, a FROM vrdf_l7;

DROP VIEW vrdf_v7;
DROP TABLE vrdf_t7;
DROP TABLE vrdf_l7;

-- ============================================================================
-- A DO ALSO rule reads the same NEW, and the write it did not replace goes on
-- to the relation underneath and takes whatever default that write finds
-- ============================================================================
CREATE TABLE vrdf_t8 (id int, a text DEFAULT 'BASE');
CREATE TABLE vrdf_l8 (id int, a text);
INSERT INTO vrdf_t8 VALUES (1,'o');
CREATE VIEW vrdf_v8 AS SELECT * FROM vrdf_t8;
ALTER VIEW vrdf_v8 ALTER COLUMN a SET DEFAULT 'VIEWD';
CREATE RULE vrdf_r8 AS ON UPDATE TO vrdf_v8 DO ALSO INSERT INTO vrdf_l8 VALUES (OLD.id, NEW.a);
UPDATE vrdf_v8 SET a = DEFAULT;

-- begin-expected
-- columns: id | a
-- row: 1 | VIEWD
-- end-expected
SELECT id, a FROM vrdf_t8;

-- begin-expected
-- columns: id | a
-- row: 1 | VIEWD
-- end-expected
SELECT id, a FROM vrdf_l8;

DROP VIEW vrdf_v8;
DROP TABLE vrdf_t8;
DROP TABLE vrdf_l8;

CREATE TABLE vrdf_t9 (id int, a text DEFAULT 'BASE');
CREATE TABLE vrdf_l9 (id int, a text);
CREATE VIEW vrdf_v9 AS SELECT * FROM vrdf_t9;
CREATE RULE vrdf_r9 AS ON INSERT TO vrdf_v9 DO ALSO INSERT INTO vrdf_l9 VALUES (NEW.id, NEW.a);
INSERT INTO vrdf_v9 (id) VALUES (1);

-- the statement that went on to the relation took that relation's default
-- begin-expected
-- columns: id | a
-- row: 1 | BASE
-- end-expected
SELECT id, a FROM vrdf_t9;

-- the rule read what the view had to give, which was nothing
-- begin-expected
-- columns: id | a
-- row: 1 | NULL
-- end-expected
SELECT id, a FROM vrdf_l9;

DROP VIEW vrdf_v9;
DROP TABLE vrdf_t9;
DROP TABLE vrdf_l9;

-- ============================================================================
-- A rule of the relation the view was rewritten onto reads the default that
-- write filled in
-- ============================================================================
CREATE TABLE vrdf_ta (id int, a text DEFAULT 'BASE');
CREATE TABLE vrdf_la (id int, a text);
CREATE VIEW vrdf_va AS SELECT * FROM vrdf_ta;
CREATE RULE vrdf_ra AS ON INSERT TO vrdf_ta DO ALSO INSERT INTO vrdf_la VALUES (NEW.id, NEW.a);
INSERT INTO vrdf_va (id) VALUES (1);

-- begin-expected
-- columns: id | a
-- row: 1 | BASE
-- end-expected
SELECT id, a FROM vrdf_ta;

-- begin-expected
-- columns: id | a
-- row: 1 | BASE
-- end-expected
SELECT id, a FROM vrdf_la;

DROP VIEW vrdf_va;
DROP TABLE vrdf_ta;
DROP TABLE vrdf_la;

-- and the view's own default was substituted before that write was made, so it
-- reaches the relation's rule too
CREATE TABLE vrdf_tb (id int, a text DEFAULT 'BASE');
CREATE TABLE vrdf_lb (id int, a text);
CREATE VIEW vrdf_vb AS SELECT * FROM vrdf_tb;
ALTER VIEW vrdf_vb ALTER COLUMN a SET DEFAULT 'VIEWD';
CREATE RULE vrdf_rb AS ON INSERT TO vrdf_tb DO ALSO INSERT INTO vrdf_lb VALUES (NEW.id, NEW.a);
INSERT INTO vrdf_vb (id) VALUES (1);

-- begin-expected
-- columns: id | a
-- row: 1 | VIEWD
-- end-expected
SELECT id, a FROM vrdf_tb;

-- begin-expected
-- columns: id | a
-- row: 1 | VIEWD
-- end-expected
SELECT id, a FROM vrdf_lb;

DROP VIEW vrdf_vb;
DROP TABLE vrdf_tb;
DROP TABLE vrdf_lb;

-- ============================================================================
-- An INSTEAD OF trigger takes the write in place of the rewrite and reads the
-- same NEW
-- ============================================================================
CREATE TABLE vrdf_tc (id int, a text DEFAULT 'BASE');
INSERT INTO vrdf_tc VALUES (1,'o');
CREATE VIEW vrdf_vc AS SELECT * FROM vrdf_tc;
ALTER VIEW vrdf_vc ALTER COLUMN a SET DEFAULT 'VIEWD';
CREATE FUNCTION vrdf_fc() RETURNS trigger AS $$ BEGIN UPDATE vrdf_tc SET a = NEW.a WHERE id = OLD.id; RETURN NEW; END $$ LANGUAGE plpgsql;
CREATE TRIGGER vrdf_trc INSTEAD OF UPDATE ON vrdf_vc FOR EACH ROW EXECUTE FUNCTION vrdf_fc();
UPDATE vrdf_vc SET a = DEFAULT;

-- begin-expected
-- columns: id | a
-- row: 1 | VIEWD
-- end-expected
SELECT id, a FROM vrdf_tc;

DROP TRIGGER vrdf_trc ON vrdf_vc;
DROP VIEW vrdf_vc;
DROP FUNCTION vrdf_fc();
DROP TABLE vrdf_tc;

-- ============================================================================
-- A DELETE has no assignment to make, so no default reaches anything through
-- one, and DO INSTEAD NOTHING makes no write at all
-- ============================================================================
CREATE TABLE vrdf_td (id int, a text DEFAULT 'BASE');
CREATE TABLE vrdf_ld (id int, a text);
INSERT INTO vrdf_td VALUES (1,'o');
CREATE VIEW vrdf_vd AS SELECT * FROM vrdf_td;
ALTER VIEW vrdf_vd ALTER COLUMN a SET DEFAULT 'VIEWD';
CREATE RULE vrdf_rd AS ON DELETE TO vrdf_vd DO INSTEAD INSERT INTO vrdf_ld VALUES (OLD.id, OLD.a);
DELETE FROM vrdf_vd WHERE id = 1;

-- begin-expected
-- columns: id | a
-- row: 1 | o
-- end-expected
SELECT id, a FROM vrdf_td;

-- begin-expected
-- columns: id | a
-- row: 1 | o
-- end-expected
SELECT id, a FROM vrdf_ld;

DROP VIEW vrdf_vd;
DROP TABLE vrdf_td;
DROP TABLE vrdf_ld;

CREATE TABLE vrdf_te (id int, a text DEFAULT 'BASE');
CREATE VIEW vrdf_ve AS SELECT * FROM vrdf_te;
ALTER VIEW vrdf_ve ALTER COLUMN a SET DEFAULT 'VA';
CREATE RULE vrdf_re AS ON INSERT TO vrdf_ve DO INSTEAD NOTHING;
INSERT INTO vrdf_ve (id) VALUES (1);

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM vrdf_te;

DROP VIEW vrdf_ve;
DROP TABLE vrdf_te;

-- ============================================================================
-- A write that names the relation itself never goes near the view, whatever
-- the view declares
-- ============================================================================
CREATE TABLE vrdf_tf (id int, a text DEFAULT 'BASE');
CREATE TABLE vrdf_lf (id int, a text);
INSERT INTO vrdf_tf VALUES (1,'o');
CREATE VIEW vrdf_vf AS SELECT * FROM vrdf_tf;
ALTER VIEW vrdf_vf ALTER COLUMN a SET DEFAULT 'VIEWD';
CREATE RULE vrdf_rf AS ON UPDATE TO vrdf_tf DO ALSO INSERT INTO vrdf_lf VALUES (OLD.id, NEW.a);
UPDATE vrdf_tf SET a = DEFAULT;

-- begin-expected
-- columns: id | a
-- row: 1 | BASE
-- end-expected
SELECT id, a FROM vrdf_tf;

-- begin-expected
-- columns: id | a
-- row: 1 | BASE
-- end-expected
SELECT id, a FROM vrdf_lf;

DROP VIEW vrdf_vf;
DROP TABLE vrdf_tf;
DROP TABLE vrdf_lf;

-- ============================================================================
-- The relation that answers is the one the statement named, however many views
-- stand between it and the rule
-- ============================================================================
CREATE TABLE vrdf_tg (id int, a text DEFAULT 'BASE');
INSERT INTO vrdf_tg VALUES (1,'o');
CREATE VIEW vrdf_vg AS SELECT * FROM vrdf_tg;
ALTER VIEW vrdf_vg ALTER COLUMN a SET DEFAULT 'VIEWD';
CREATE VIEW vrdf_vg2 AS SELECT * FROM vrdf_vg;
CREATE RULE vrdf_rg AS ON UPDATE TO vrdf_vg DO INSTEAD UPDATE vrdf_tg SET a = NEW.a WHERE id = OLD.id;
UPDATE vrdf_vg2 SET a = DEFAULT;

-- the statement named vrdf_vg2, which declares no default of its own
-- begin-expected
-- columns: id | a
-- row: 1 | NULL
-- end-expected
SELECT id, a FROM vrdf_tg;

DROP VIEW vrdf_vg2;
DROP VIEW vrdf_vg;
DROP TABLE vrdf_tg;

-- ============================================================================
-- A rule's own RETURNING answers for the write the rule made
-- ============================================================================
CREATE TABLE vrdf_th (id int, a text DEFAULT 'BASE');
INSERT INTO vrdf_th VALUES (1,'o');
CREATE VIEW vrdf_vh AS SELECT * FROM vrdf_th;
ALTER VIEW vrdf_vh ALTER COLUMN a SET DEFAULT 'VIEWD';
CREATE RULE vrdf_rh AS ON UPDATE TO vrdf_vh DO INSTEAD UPDATE vrdf_th SET a = NEW.a WHERE id = OLD.id RETURNING vrdf_th.id, vrdf_th.a;

-- begin-expected
-- columns: id | a
-- row: 1 | VIEWD
-- end-expected
UPDATE vrdf_vh SET a = DEFAULT RETURNING id, a;

-- begin-expected
-- columns: id | a
-- row: 1 | VIEWD
-- end-expected
SELECT id, a FROM vrdf_th;

DROP VIEW vrdf_vh;
DROP TABLE vrdf_th;

-- ============================================================================
-- A conflict clause is not rewritten onto anything a rule stands for
-- ============================================================================
CREATE TABLE vrdf_ti (id int PRIMARY KEY, a text DEFAULT 'BASE');
CREATE TABLE vrdf_li (id int, a text);
CREATE VIEW vrdf_vi AS SELECT * FROM vrdf_ti;
ALTER VIEW vrdf_vi ALTER COLUMN a SET DEFAULT 'VA';
CREATE RULE vrdf_ri AS ON INSERT TO vrdf_vi DO INSTEAD INSERT INTO vrdf_li VALUES (NEW.id, NEW.a);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: INSERT with ON CONFLICT clause cannot be used with table that has INSERT or UPDATE rules
-- end-expected-error
INSERT INTO vrdf_vi (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM vrdf_li;

DROP VIEW vrdf_vi;
DROP TABLE vrdf_ti;
DROP TABLE vrdf_li;
