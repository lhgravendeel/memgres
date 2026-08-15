-- A VIRTUAL generated column is worked out for the rows a query returns, once the qualification
-- has settled which those are: PostgreSQL puts the generation expression where the reference to
-- the column stood, so a row the WHERE discards is a row it is never evaluated for.
DROP TABLE IF EXISTS zzw5a_vg1 CASCADE;
CREATE TABLE zzw5a_vg1 (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO zzw5a_vg1 (a,k) VALUES (5,'five');
INSERT INTO zzw5a_vg1 (a,k) VALUES (0,'zero');

-- begin-expected
-- columns: g
-- row: 2
-- end-expected
SELECT g FROM zzw5a_vg1 WHERE a = 5;

-- begin-expected
-- columns: g
-- end-expected
SELECT g FROM zzw5a_vg1 WHERE false;

-- begin-expected
-- columns: k
-- row: zero
-- end-expected
SELECT k FROM zzw5a_vg1 WHERE a = 0;

-- Reading it of the row it raises for is the one case that raises.
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT * FROM zzw5a_vg1 ORDER BY a;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT k FROM zzw5a_vg1 ORDER BY g;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT k FROM zzw5a_vg1 WHERE g > 1;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT max(k) FROM zzw5a_vg1 GROUP BY g;

DROP TABLE zzw5a_vg1;

-- A statement that never names a virtual column never evaluates it, so a write to a relation
-- whose generation expression raises for one of its rows is a write that goes through.
DROP TABLE IF EXISTS zzw5a_vg2 CASCADE;
CREATE TABLE zzw5a_vg2 (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO zzw5a_vg2 (a,k) VALUES (5,'five');
INSERT INTO zzw5a_vg2 (a,k) VALUES (0,'zero');
UPDATE zzw5a_vg2 SET k = 'x' WHERE a = 5;

-- begin-expected
-- columns: a, k
-- row: 0, zero
-- row: 5, x
-- end-expected
SELECT a, k FROM zzw5a_vg2 ORDER BY a;

-- begin-expected
-- columns: k
-- row: zero
-- end-expected
DELETE FROM zzw5a_vg2 WHERE k = 'zero' RETURNING k;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) AS count FROM zzw5a_vg2;

-- begin-expected
-- columns: k
-- row: r1
-- end-expected
INSERT INTO zzw5a_vg2 (a,k) VALUES (0,'r1') RETURNING k;

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
UPDATE zzw5a_vg2 SET k = k || '!' WHERE a = 0 RETURNING a;

-- An assignment that does read one reads it of the rows the qualification kept.
UPDATE zzw5a_vg2 SET k = 'z' || g WHERE a = 5;

-- begin-expected
-- columns: a, k
-- row: 0, r1!
-- row: 5, z2
-- end-expected
SELECT a, k FROM zzw5a_vg2 ORDER BY a;

DROP TABLE zzw5a_vg2;

-- A derived table reads of the relation under it what the query around it reads of the derived
-- table: PostgreSQL pulls the derived table up, so the * inside one is not a * over the query.
DROP TABLE IF EXISTS zzw5a_vg3 CASCADE;
CREATE TABLE zzw5a_vg3 (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO zzw5a_vg3 (a,k) VALUES (5,'five');
INSERT INTO zzw5a_vg3 (a,k) VALUES (0,'zero');

-- begin-expected
-- columns: k
-- row: five
-- row: zero
-- end-expected
SELECT k FROM (SELECT * FROM zzw5a_vg3) s ORDER BY k;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) AS count FROM (SELECT * FROM zzw5a_vg3) s;

-- begin-expected
-- columns: k
-- row: five
-- row: zero
-- end-expected
WITH c AS (SELECT * FROM zzw5a_vg3) SELECT k FROM c ORDER BY k;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
WITH c AS (SELECT * FROM zzw5a_vg3) SELECT count(*) AS count FROM c;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT * FROM (SELECT * FROM zzw5a_vg3) s ORDER BY a;

DROP TABLE zzw5a_vg3;

-- PostgreSQL fills in the default of a column the statement left out while it rewrites the
-- statement, which is before the rules are applied, so NEW carries the default and not a null.
DROP TABLE IF EXISTS zzw5a_rd1 CASCADE;
DROP TABLE IF EXISTS zzw5a_rd1log CASCADE;
CREATE TABLE zzw5a_rd1 (a text DEFAULT 'dflt', b int);
CREATE TABLE zzw5a_rd1log (m text);
CREATE RULE zzw5a_rd1r AS ON INSERT TO zzw5a_rd1 DO ALSO INSERT INTO zzw5a_rd1log VALUES (NEW.a);
INSERT INTO zzw5a_rd1 (b) VALUES (7);

-- begin-expected
-- columns: a, b
-- row: dflt, 7
-- end-expected
SELECT a, b FROM zzw5a_rd1;

-- begin-expected
-- columns: m
-- row: dflt
-- end-expected
SELECT m FROM zzw5a_rd1log;

-- DEFAULT written in the VALUES list asks for the same value, and the rule reads that.
INSERT INTO zzw5a_rd1 (a,b) VALUES (DEFAULT, 8);

-- begin-expected
-- columns: m
-- row: dflt
-- row: dflt
-- end-expected
SELECT m FROM zzw5a_rd1log;

DROP TABLE zzw5a_rd1 CASCADE;
DROP TABLE zzw5a_rd1log CASCADE;

-- The qualification reads NEW the same way the action does, so a rule keyed on a defaulted
-- column fires for a statement that left the column out.
DROP TABLE IF EXISTS zzw5a_rd2 CASCADE;
DROP TABLE IF EXISTS zzw5a_rd2log CASCADE;
CREATE TABLE zzw5a_rd2 (id int, a text DEFAULT 'dv');
CREATE TABLE zzw5a_rd2log (m text);
CREATE RULE zzw5a_rd2r AS ON INSERT TO zzw5a_rd2 WHERE NEW.a = 'dv'
    DO ALSO INSERT INTO zzw5a_rd2log VALUES ('sawdefault');
INSERT INTO zzw5a_rd2 (id) VALUES (1);
INSERT INTO zzw5a_rd2 (id, a) VALUES (2, 'other');

-- begin-expected
-- columns: id, a
-- row: 1, dv
-- row: 2, other
-- end-expected
SELECT id, a FROM zzw5a_rd2 ORDER BY id;

-- begin-expected
-- columns: m
-- row: sawdefault
-- end-expected
SELECT m FROM zzw5a_rd2log;

DROP TABLE zzw5a_rd2 CASCADE;
DROP TABLE zzw5a_rd2log CASCADE;

-- A column with no default still reads null in NEW, and a column the system computes reads the
-- value the row is about to hold: PostgreSQL works a stored generated column out where the rule
-- reads it.
DROP TABLE IF EXISTS zzw5a_rd3 CASCADE;
DROP TABLE IF EXISTS zzw5a_rd3log CASCADE;
CREATE TABLE zzw5a_rd3 (a int, s int GENERATED ALWAYS AS (a*2) STORED, t text);
CREATE TABLE zzw5a_rd3log (m text);
CREATE RULE zzw5a_rd3r AS ON INSERT TO zzw5a_rd3
    DO ALSO INSERT INTO zzw5a_rd3log VALUES (coalesce(NEW.s::text, 'null') || '/' || coalesce(NEW.t, 'null'));
INSERT INTO zzw5a_rd3 (a) VALUES (4);

-- begin-expected
-- columns: a, s, t
-- row: 4, 8, null
-- end-expected
SELECT a, s, t FROM zzw5a_rd3;

-- begin-expected
-- columns: m
-- row: 8/null
-- end-expected
SELECT m FROM zzw5a_rd3log;

DROP TABLE zzw5a_rd3 CASCADE;
DROP TABLE zzw5a_rd3log CASCADE;

-- A default drawn from a sequence is drawn again for NEW: PostgreSQL evaluates a default
-- expression once for every place the statement it rewrote holds one.
DROP TABLE IF EXISTS zzw5a_rd4 CASCADE;
DROP TABLE IF EXISTS zzw5a_rd4log CASCADE;
CREATE TABLE zzw5a_rd4 (id serial, b int);
CREATE TABLE zzw5a_rd4log (m int);
CREATE RULE zzw5a_rd4r AS ON INSERT TO zzw5a_rd4 DO ALSO INSERT INTO zzw5a_rd4log VALUES (NEW.id);
INSERT INTO zzw5a_rd4 (b) VALUES (1);

-- begin-expected
-- columns: id, b
-- row: 1, 1
-- end-expected
SELECT id, b FROM zzw5a_rd4;

-- begin-expected
-- columns: m
-- row: 2
-- end-expected
SELECT m FROM zzw5a_rd4log;

DROP TABLE zzw5a_rd4 CASCADE;
DROP TABLE zzw5a_rd4log CASCADE;
