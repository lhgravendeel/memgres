CREATE TABLE zzw2d_c1r (i int);
CREATE TABLE zzw2d_c1l (seq serial, m text);
CREATE RULE zzw2d_c1z AS ON INSERT TO zzw2d_c1r DO ALSO INSERT INTO zzw2d_c1l (m) VALUES ('z');
CREATE RULE zzw2d_c1m AS ON INSERT TO zzw2d_c1r DO ALSO INSERT INTO zzw2d_c1l (m) VALUES ('m');
CREATE RULE zzw2d_c1a AS ON INSERT TO zzw2d_c1r DO ALSO INSERT INTO zzw2d_c1l (m) VALUES ('a');
INSERT INTO zzw2d_c1r VALUES (1);
-- begin-expected
-- columns: m
-- row: a
-- row: m
-- row: z
-- end-expected
SELECT m FROM zzw2d_c1l ORDER BY seq;
DROP TABLE zzw2d_c1r;
DROP TABLE zzw2d_c1l;

CREATE TABLE zzw2d_c2t (i int, v text);
CREATE TABLE zzw2d_c2l (i int, v text);
CREATE RULE zzw2d_c2r AS ON INSERT TO zzw2d_c2t WHERE NEW.i > 10 DO INSTEAD INSERT INTO zzw2d_c2l VALUES (NEW.i, NEW.v);
INSERT INTO zzw2d_c2t VALUES (5,'small');
INSERT INTO zzw2d_c2t VALUES (50,'big');
-- begin-expected
-- columns: i,v
-- row: 5, small
-- end-expected
SELECT i,v FROM zzw2d_c2t ORDER BY i;
-- begin-expected
-- columns: i,v
-- row: 50, big
-- end-expected
SELECT i,v FROM zzw2d_c2l ORDER BY i;
DROP TABLE zzw2d_c2t;
DROP TABLE zzw2d_c2l;

CREATE TABLE zzw2d_c3t (i int);
CREATE RULE zzw2d_c3r AS ON INSERT TO zzw2d_c3t WHERE NEW.i < 0 DO INSTEAD NOTHING;
INSERT INTO zzw2d_c3t VALUES (5);
INSERT INTO zzw2d_c3t VALUES (-5);
-- begin-expected
-- columns: i
-- row: 5
-- end-expected
SELECT i FROM zzw2d_c3t ORDER BY i;
DROP TABLE zzw2d_c3t;

CREATE TABLE zzw2d_c4t (i int primary key, v int);
INSERT INTO zzw2d_c4t VALUES (1,10),(2,20);
CREATE TABLE zzw2d_c4l (i int);
CREATE RULE zzw2d_c4r AS ON UPDATE TO zzw2d_c4t WHERE OLD.i = 1 DO INSTEAD INSERT INTO zzw2d_c4l VALUES (OLD.i);
UPDATE zzw2d_c4t SET v = 99;
-- begin-expected
-- columns: i,v
-- row: 1, 10
-- row: 2, 99
-- end-expected
SELECT i,v FROM zzw2d_c4t ORDER BY i;
-- begin-expected
-- columns: i
-- row: 1
-- end-expected
SELECT i FROM zzw2d_c4l ORDER BY i;
DROP TABLE zzw2d_c4t;
DROP TABLE zzw2d_c4l;

CREATE TABLE zzw2d_c5q (i int);
CREATE TABLE zzw2d_c5l (i int);
CREATE RULE zzw2d_c5r AS ON INSERT TO zzw2d_c5q WHERE NEW.i > 10 DO ALSO INSERT INTO zzw2d_c5l VALUES (NEW.i);
INSERT INTO zzw2d_c5q VALUES (5);
INSERT INTO zzw2d_c5q VALUES (50);
-- begin-expected
-- columns: i
-- row: 50
-- end-expected
SELECT i FROM zzw2d_c5l ORDER BY i;
DROP TABLE zzw2d_c5q;
DROP TABLE zzw2d_c5l;

CREATE TABLE zzw2d_c6l (seq serial, t text);
CREATE FUNCTION zzw2d_c6f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zzw2d_c6l (t) VALUES (TG_WHEN||'/'||TG_OP||'/'||OLD.id); RETURN OLD; END $$;
CREATE TABLE zzw2d_c6t (id int PRIMARY KEY);
INSERT INTO zzw2d_c6t VALUES (1),(2);
CREATE TRIGGER zzw2d_c6b BEFORE DELETE ON zzw2d_c6t FOR EACH ROW EXECUTE FUNCTION zzw2d_c6f();
CREATE TRIGGER zzw2d_c6a AFTER DELETE ON zzw2d_c6t FOR EACH ROW EXECUTE FUNCTION zzw2d_c6f();
DELETE FROM zzw2d_c6t;
-- begin-expected
-- columns: t,count
-- row: AFTER/DELETE/1, 1
-- row: AFTER/DELETE/2, 1
-- row: BEFORE/DELETE/1, 1
-- row: BEFORE/DELETE/2, 1
-- end-expected
SELECT t, count(*) FROM zzw2d_c6l GROUP BY t ORDER BY t;
DROP TABLE zzw2d_c6t;
DROP TABLE zzw2d_c6l;
DROP FUNCTION zzw2d_c6f();

CREATE TABLE zzw2d_c7t (id int primary key, n int);
INSERT INTO zzw2d_c7t VALUES (1,10),(2,20);
CREATE FUNCTION zzw2d_c7f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER zzw2d_c7b BEFORE DELETE ON zzw2d_c7t FOR EACH ROW EXECUTE FUNCTION zzw2d_c7f();
DELETE FROM zzw2d_c7t WHERE id = 1;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM zzw2d_c7t ORDER BY id;
DROP TABLE zzw2d_c7t;
DROP FUNCTION zzw2d_c7f();

CREATE TABLE zzw2d_c8b (id int, note text);
INSERT INTO zzw2d_c8b VALUES (1,'a');
CREATE VIEW zzw2d_c8v AS SELECT id, note FROM zzw2d_c8b;
CREATE FUNCTION zzw2d_c8f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE zzw2d_c8b SET note = 'deleted-by-trigger' WHERE id = OLD.id; RETURN OLD; END $$;
CREATE TRIGGER zzw2d_c8t INSTEAD OF DELETE ON zzw2d_c8v FOR EACH ROW EXECUTE FUNCTION zzw2d_c8f();
DELETE FROM zzw2d_c8v WHERE id = 1;
-- begin-expected
-- columns: id,note
-- row: 1, deleted-by-trigger
-- end-expected
SELECT id, note FROM zzw2d_c8b ORDER BY id;
DROP VIEW zzw2d_c8v;
DROP TABLE zzw2d_c8b;
DROP FUNCTION zzw2d_c8f();