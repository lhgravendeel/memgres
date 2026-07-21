-- Feature comparison: residual DML-through-view bugs (H3 / H4)
-- Verified against PostgreSQL 18.

-- ============================================================
-- H3 part 1: positional INSERT through a reordered/renamed view
-- View columns are (num -> n, id): VALUES map in VIEW-column order.
-- ============================================================

-- setup
CREATE TABLE vdr_ins (id INT PRIMARY KEY, n INT);
CREATE VIEW vdr_ins_v AS SELECT n AS num, id FROM vdr_ins;

-- stmt: positional VALUES are (num=80, id=8) -> base row id=8, n=80
INSERT INTO vdr_ins_v VALUES (80, 8);

-- begin-expected
-- columns: id | n
-- row: 8, 80
-- end-expected
SELECT id, n FROM vdr_ins;

-- cleanup
DROP VIEW vdr_ins_v;
DROP TABLE vdr_ins;

-- ============================================================
-- H3 part 1b: positional INSERT through a single renamed subset view
-- ============================================================

-- setup
CREATE TABLE vdr_sub (id INT, n INT);
CREATE VIEW vdr_sub_v AS SELECT n AS num FROM vdr_sub;

-- stmt: the value lands in n, id stays NULL
INSERT INTO vdr_sub_v VALUES (77);

-- begin-expected
-- columns: id | n
-- row: NULL, 77
-- end-expected
SELECT id, n FROM vdr_sub;

-- cleanup
DROP VIEW vdr_sub_v;
DROP TABLE vdr_sub;

-- ============================================================
-- H3 part 2: UPDATE / DELETE WHERE referencing a renamed view column
-- ============================================================

-- setup
CREATE TABLE vdr_ren (id INT PRIMARY KEY, n INT);
INSERT INTO vdr_ren VALUES (5, 50), (6, 60);
CREATE VIEW vdr_ren_v AS SELECT n AS num, id FROM vdr_ren;

-- stmt: UPDATE resolves the renamed column in both SET and WHERE
UPDATE vdr_ren_v SET num = 51 WHERE num = 50;

-- begin-expected
-- columns: id | n
-- row: 5, 51
-- row: 6, 60
-- end-expected
SELECT id, n FROM vdr_ren ORDER BY id;

-- stmt: DELETE resolves the renamed column in WHERE
DELETE FROM vdr_ren_v WHERE num = 60;

-- begin-expected
-- columns: id | n
-- row: 5, 51
-- end-expected
SELECT id, n FROM vdr_ren ORDER BY id;

-- cleanup
DROP VIEW vdr_ren_v;
DROP TABLE vdr_ren;

-- ============================================================
-- H4: INSTEAD OF UPDATE / DELETE triggers on a join view actually fire
-- ============================================================

-- setup
CREATE TABLE vdr_a (id INT PRIMARY KEY, av TEXT);
CREATE TABLE vdr_b (id INT PRIMARY KEY, bv TEXT);
INSERT INTO vdr_a VALUES (1, 'a1'), (2, 'a2');
INSERT INTO vdr_b VALUES (1, 'b1'), (2, 'b2');
CREATE VIEW vdr_jv AS SELECT a.id, a.av, b.bv FROM vdr_a a JOIN vdr_b b ON a.id = b.id;
CREATE FUNCTION vdr_upd() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE vdr_a SET av = NEW.av WHERE id = OLD.id; RETURN NEW; END; $$;
CREATE FUNCTION vdr_del() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN DELETE FROM vdr_a WHERE id = OLD.id; RETURN OLD; END; $$;
CREATE TRIGGER vdr_tu INSTEAD OF UPDATE ON vdr_jv FOR EACH ROW EXECUTE FUNCTION vdr_upd();
CREATE TRIGGER vdr_td INSTEAD OF DELETE ON vdr_jv FOR EACH ROW EXECUTE FUNCTION vdr_del();

-- stmt: INSTEAD OF UPDATE fires and rewrites base table zza
UPDATE vdr_jv SET av = 'updated' WHERE id = 1;

-- begin-expected
-- columns: id | av
-- row: 1, updated
-- row: 2, a2
-- end-expected
SELECT id, av FROM vdr_a ORDER BY id;

-- stmt: INSTEAD OF DELETE fires and deletes from base table zza
DELETE FROM vdr_jv WHERE id = 2;

-- begin-expected
-- columns: id | av
-- row: 1, updated
-- end-expected
SELECT id, av FROM vdr_a ORDER BY id;

-- cleanup
DROP VIEW vdr_jv CASCADE;
DROP FUNCTION vdr_upd();
DROP FUNCTION vdr_del();
DROP TABLE vdr_a;
DROP TABLE vdr_b;
