-- A rule rewrites a statement before the table is touched, and a text search configuration that
-- the catalog lists has to be one the parser can actually use.

DROP TABLE IF EXISTS cec_r CASCADE;
CREATE TABLE cec_r (id int);
CREATE RULE cec_rr AS ON DELETE TO cec_r DO INSTEAD NOTHING;
INSERT INTO cec_r VALUES (1);

-- INSTEAD NOTHING swallows the delete: the row stays.
DELETE FROM cec_r;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM cec_r;

CREATE RULE cec_ru AS ON UPDATE TO cec_r DO INSTEAD NOTHING;
UPDATE cec_r SET id = 2;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM cec_r;

DROP RULE cec_ru ON cec_r;
DROP RULE cec_rr ON cec_r;

-- Without the rule the delete goes through.
DELETE FROM cec_r;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM cec_r;

DROP TABLE cec_r;

-- A rule that rewrites the statement speaks of OLD and NEW, which only mean something against a
-- row the statement would have touched.
DROP VIEW IF EXISTS cec_rv CASCADE;
DROP TABLE IF EXISTS cec_rt CASCADE;
CREATE TABLE cec_rt (id int PRIMARY KEY, a int, b text);
INSERT INTO cec_rt VALUES (1, 11, 'ABC'), (2, 22, 'DEF');
CREATE VIEW cec_rv AS SELECT id, a, b FROM cec_rt;
CREATE RULE cec_rv_upd AS ON UPDATE TO cec_rv DO INSTEAD
  UPDATE cec_rt SET a = NEW.a, b = NEW.b WHERE id = OLD.id;

UPDATE cec_rv SET a = 99, b = 'rule_upd' WHERE id = 1;

-- begin-expected
-- columns: id | a | b
-- row: 1, 99, rule_upd
-- row: 2, 22, DEF
-- end-expected
SELECT id, a, b FROM cec_rt ORDER BY id;

-- A statement that names no row runs the rule for none.
UPDATE cec_rv SET a = 77 WHERE id = 42;

-- begin-expected
-- columns: id | a
-- row: 1, 99
-- row: 2, 22
-- end-expected
SELECT id, a FROM cec_rt ORDER BY id;

DROP RULE cec_rv_upd ON cec_rv;
DROP VIEW cec_rv;
DROP TABLE cec_rt;

-- An INSTEAD rule on DELETE resolves OLD the same way.
DROP VIEW IF EXISTS cec_rv3 CASCADE;
DROP TABLE IF EXISTS cec_rt3 CASCADE;
CREATE TABLE cec_rt3 (id int PRIMARY KEY, archived boolean DEFAULT false);
INSERT INTO cec_rt3 VALUES (1, false), (2, false);
CREATE VIEW cec_rv3 AS SELECT id, archived FROM cec_rt3;
CREATE RULE cec_rv3_del AS ON DELETE TO cec_rv3 DO INSTEAD
  UPDATE cec_rt3 SET archived = true WHERE id = OLD.id;

DELETE FROM cec_rv3 WHERE id = 1;

-- begin-expected
-- columns: id | archived
-- row: 1, true
-- row: 2, false
-- end-expected
SELECT id, archived::text AS archived FROM cec_rt3 ORDER BY id;

DROP RULE cec_rv3_del ON cec_rv3;
DROP VIEW cec_rv3;
DROP TABLE cec_rt3;

-- A table that really does publish its deletes, with no way to identify a row, is refused.
DROP TABLE IF EXISTS cec_pub CASCADE;
CREATE TABLE cec_pub (id int);
CREATE PUBLICATION cec_p FOR TABLE cec_pub;
INSERT INTO cec_pub VALUES (1);

-- begin-expected-error
-- sqlstate: 55000
-- message-like: replica identity
-- end-expected-error
DELETE FROM cec_pub;

DROP PUBLICATION cec_p;
DROP TABLE cec_pub;

-- A configuration created by COPY behaves as the configuration it copied.
DROP TEXT SEARCH CONFIGURATION IF EXISTS cec_cfg;
CREATE TEXT SEARCH CONFIGURATION cec_cfg (COPY = simple);

-- begin-expected
-- columns: cfgname
-- row: cec_cfg
-- end-expected
SELECT cfgname FROM pg_ts_config WHERE cfgname = 'cec_cfg';

-- begin-expected
-- columns: a
-- row: 'hello':1 'world':2
-- end-expected
SELECT to_tsvector('cec_cfg', 'hello world')::text AS a;

DROP TEXT SEARCH CONFIGURATION IF EXISTS cec_cfg2;
CREATE TEXT SEARCH CONFIGURATION cec_cfg2 (COPY = english);

-- begin-expected
-- columns: a
-- row: 'run'
-- end-expected
SELECT to_tsquery('cec_cfg2', 'running')::text AS a;

DROP TEXT SEARCH CONFIGURATION cec_cfg;
DROP TEXT SEARCH CONFIGURATION cec_cfg2;

-- Once dropped it is gone from the parser as well as the catalog.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_ts_config WHERE cfgname = 'cec_cfg';

-- begin-expected-error
-- sqlstate: 42704
-- message-like: does not exist
-- end-expected-error
SELECT to_tsvector('cec_cfg', 'hello');

-- begin-expected-error
-- sqlstate: 42704
-- message-like: does not exist
-- end-expected-error
SELECT to_tsvector('cec_nosuch', 'hello');
