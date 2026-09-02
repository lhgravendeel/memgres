-- source: investigation-2026-08.md
-- finding: 122
-- title: WITH is executed without the analysis PostgreSQL performs first: sub-statements do not share one snapshot, a data-modifying CTE without RETURNING is not refused
-- begin-expected-error
-- sqlstate: 57014
-- message-like: canceling statement due to statement timeout
-- end-expected-error
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 1 FROM r) SELECT count(*) FROM r;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_d2 (id int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_d2 VALUES (1),(2);
-- begin-expected
-- columns: id:int4
-- row: 1
-- rowcount: 1
-- end-expected
WITH a AS (DELETE FROM zz_d2 WHERE id = 1 RETURNING id) DELETE FROM zz_d2 WHERE id = 1 RETURNING id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_log (id int, v text);
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: WITH query "w" does not have a RETURNING clause
-- end-expected-error
WITH w AS (INSERT INTO zz_log VALUES (21,'o')) SELECT * FROM w;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_log;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_dag (id int, link int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_dag VALUES (1,2),(2,3),(3,NULL);
-- begin-expected-error
-- sqlstate: 42701
-- message-like: cycle column "id" specified more than once
-- end-expected-error
WITH RECURSIVE g(id, link) AS (SELECT id, link FROM zz_dag WHERE id = 1 UNION ALL SELECT gg.id, gg.link FROM zz_dag gg, g WHERE gg.id = g.link) CYCLE id, id SET c USING p SELECT count(*) FROM g;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_dag" already exists
-- end-expected-error
CREATE TABLE zz_dag (id int, link int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_dag VALUES (1,2),(2,3),(3,NULL);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: cycle mark column name "id" already used in WITH query column list
-- end-expected-error
WITH RECURSIVE g(id, link) AS (SELECT id, link FROM zz_dag WHERE id = 1 UNION ALL SELECT gg.id, gg.link FROM zz_dag gg, g WHERE gg.id = g.link) CYCLE id SET id USING p SELECT count(*) FROM g;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_log" already exists
-- end-expected-error
CREATE TABLE zz_log (id int, v text);
-- begin-expected-error
-- sqlstate: 42P19
-- message-like: recursive query "w" must not contain data-modifying statements
-- end-expected-error
WITH RECURSIVE w AS (INSERT INTO zz_log SELECT id, v FROM w RETURNING id) SELECT id FROM w;
