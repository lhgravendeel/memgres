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
