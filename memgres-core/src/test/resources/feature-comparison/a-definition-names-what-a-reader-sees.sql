-- What a stored definition calls the relations and the columns a reader will see.
--   * a recursive query reads itself, and that reference is written under a name of its own;
--     so is a sub-select's entry whose name an enclosing FROM item already took.
--   * a column the query never named is published under the placeholder PostgreSQL calls it by,
--     and a set operation writes every later arm against the first arm's names.
--   * a star is expanded into the columns it stood for when the view was created, and a WITH
--     item publishes names of its own for it to stand for -- which is a freeze, not a spelling:
--     a column added to the base relation afterwards is not in the view.
-- Every value below was read off PostgreSQL 18. Newlines are written as the two characters
-- backslash-n by the replace() around each call, so one definition fits on one annotated row.

-- setup
CREATE TABLE dnr_r1 (id int, nme text);
CREATE TABLE dnr_l1 (id int, nme text);
CREATE TABLE dnr_s (a int, b text);
INSERT INTO dnr_s VALUES (1,'x'),(2,'y');

-- stmt 1: the recursive reference cannot use the name the query around it uses
CREATE VIEW dnr_cte AS WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5) SELECT n FROM t;
-- begin-expected
-- columns: d
-- row:  WITH RECURSIVE t(n) AS (\n         SELECT 1 AS "?column?"\n        UNION ALL\n         SELECT t_1.n + 1\n           FROM t t_1\n          WHERE t_1.n < 5\n        )\n SELECT n\n   FROM t;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_cte'::regclass, true), chr(10), '\n') AS d;
-- begin-expected
-- columns: d
-- row:  WITH RECURSIVE t(n) AS (\n         SELECT 1 AS "?column?"\n        UNION ALL\n         SELECT (t_1.n + 1)\n           FROM t t_1\n          WHERE (t_1.n < 5)\n        )\n SELECT n\n   FROM t;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_cte'::regclass, false), chr(10), '\n') AS d;

-- stmt 2: two arms, two unnamed columns, both published under the placeholder
CREATE VIEW dnr_cte2 AS WITH RECURSIVE t(n, s) AS (SELECT 1, 'a' UNION ALL SELECT t.n + 1, t.s || 'b' FROM t WHERE t.n < 5) SELECT n, s FROM t;
-- begin-expected
-- columns: d
-- row:  WITH RECURSIVE t(n, s) AS (\n         SELECT 1 AS "?column?",\n            'a'::text AS "?column?"\n        UNION ALL\n         SELECT t_1.n + 1,\n            t_1.s || 'b'::text\n           FROM t t_1\n          WHERE t_1.n < 5\n        )\n SELECT n,\n    s\n   FROM t;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_cte2'::regclass, true), chr(10), '\n') AS d;

-- stmt 3: a reference the query already named for itself keeps that name
CREATE VIEW dnr_cte3 AS WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT tt.n + 1 FROM t tt WHERE tt.n < 5) SELECT n FROM t;
-- begin-expected
-- columns: d
-- row:  WITH RECURSIVE t(n) AS (\n         SELECT 1 AS "?column?"\n        UNION ALL\n         SELECT tt.n + 1\n           FROM t tt\n          WHERE tt.n < 5\n        )\n SELECT n\n   FROM t;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_cte3'::regclass, true), chr(10), '\n') AS d;

-- stmt 4: a second WITH item reading the first is not the recursive reference
CREATE VIEW dnr_cte4 AS WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5), u AS (SELECT n FROM t) SELECT n FROM u;
-- begin-expected
-- columns: d
-- row:  WITH RECURSIVE t(n) AS (\n         SELECT 1 AS "?column?"\n        UNION ALL\n         SELECT t.n + 1\n           FROM t\n          WHERE t.n < 5\n        ), u AS (\n         SELECT t.n\n           FROM t\n        )\n SELECT n\n   FROM u;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_cte4'::regclass, true), chr(10), '\n') AS d;

-- stmt 5: a sub-select reading its caller's relation is named apart, once per level
CREATE VIEW dnr_sub AS SELECT id FROM dnr_r1 WHERE id IN (SELECT id FROM dnr_r1);
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM dnr_r1\n  WHERE (id IN ( SELECT dnr_r1_1.id\n           FROM dnr_r1 dnr_r1_1));
-- end-expected
SELECT replace(pg_get_viewdef('dnr_sub'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW dnr_sub2 AS SELECT id FROM dnr_r1 WHERE id IN (SELECT id FROM dnr_r1 WHERE id IN (SELECT id FROM dnr_r1));
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM dnr_r1\n  WHERE (id IN ( SELECT dnr_r1_1.id\n           FROM dnr_r1 dnr_r1_1\n          WHERE (dnr_r1_1.id IN ( SELECT dnr_r1_2.id\n                   FROM dnr_r1 dnr_r1_2))));
-- end-expected
SELECT replace(pg_get_viewdef('dnr_sub2'::regclass, true), chr(10), '\n') AS d;

-- stmt 6: a name an enclosing FROM item took is taken, whichever relation it names
CREATE VIEW dnr_sub3 AS SELECT a.id FROM dnr_r1 a, (SELECT id FROM dnr_r1) dnr_r1;
-- begin-expected
-- columns: d
-- row:  SELECT a.id\n   FROM dnr_r1 a,\n    ( SELECT dnr_r1_1.id\n           FROM dnr_r1 dnr_r1_1) dnr_r1;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_sub3'::regclass, true), chr(10), '\n') AS d;

-- stmt 7: a column with no name of its own is published as the placeholder
CREATE VIEW dnr_lbl AS SELECT 1 AS one, id + 1 FROM dnr_l1;
-- begin-expected
-- columns: d
-- row:  SELECT 1 AS one,\n    id + 1 AS "?column?"\n   FROM dnr_l1;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_lbl'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW dnr_un AS SELECT 1 AS a UNION ALL SELECT 2;
-- begin-expected
-- columns: d
-- row:  SELECT 1 AS a\nUNION ALL\n SELECT 2 AS a;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_un'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW dnr_quo AS SELECT id AS "Mixed", nme AS "?column?" FROM dnr_l1;
-- begin-expected
-- columns: d
-- row:  SELECT id AS "Mixed",\n    nme AS "?column?"\n   FROM dnr_l1;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_quo'::regclass, true), chr(10), '\n') AS d;

-- stmt 8: a star over a WITH item stands for the names that item publishes
CREATE VIEW dnr_q1 AS WITH q AS (SELECT 1) SELECT * FROM q;
-- begin-expected
-- columns: d
-- row:  WITH q AS (\n         SELECT 1 AS "?column?"\n        )\n SELECT "?column?"\n   FROM q;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_q1'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW dnr_q3 AS WITH q AS (SELECT * FROM dnr_s) SELECT * FROM q;
-- begin-expected
-- columns: d
-- row:  WITH q AS (\n         SELECT dnr_s.a,\n            dnr_s.b\n           FROM dnr_s\n        )\n SELECT a,\n    b\n   FROM q;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_q3'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW dnr_q4 AS WITH q(x, y) AS (SELECT a, b FROM dnr_s) SELECT * FROM q;
-- begin-expected
-- columns: d
-- row:  WITH q(x, y) AS (\n         SELECT dnr_s.a,\n            dnr_s.b\n           FROM dnr_s\n        )\n SELECT x,\n    y\n   FROM q;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_q4'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW dnr_q5 AS WITH q AS (SELECT a, b FROM dnr_s) SELECT q2.* FROM q q2 WHERE q2.a > 1;
-- begin-expected
-- columns: d
-- row:  WITH q AS (\n         SELECT dnr_s.a,\n            dnr_s.b\n           FROM dnr_s\n        )\n SELECT a,\n    b\n   FROM q q2\n  WHERE a > 1;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_q5'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW dnr_q6 AS WITH RECURSIVE q(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM q WHERE n < 5) SELECT * FROM q;
-- begin-expected
-- columns: d
-- row:  WITH RECURSIVE q(n) AS (\n         SELECT 1 AS "?column?"\n        UNION ALL\n         SELECT q_1.n + 1\n           FROM q q_1\n          WHERE q_1.n < 5\n        )\n SELECT n\n   FROM q;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_q6'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW dnr_q7 AS WITH q AS (SELECT count(*) FROM dnr_s) SELECT * FROM q;
-- begin-expected
-- columns: d
-- row:  WITH q AS (\n         SELECT count(*) AS count\n           FROM dnr_s\n        )\n SELECT count\n   FROM q;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_q7'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW dnr_q8 AS WITH q AS (SELECT a, b FROM dnr_s UNION ALL SELECT a, b FROM dnr_s) SELECT * FROM q;
-- begin-expected
-- columns: d
-- row:  WITH q AS (\n         SELECT dnr_s.a,\n            dnr_s.b\n           FROM dnr_s\n        UNION ALL\n         SELECT dnr_s.a,\n            dnr_s.b\n           FROM dnr_s\n        )\n SELECT a,\n    b\n   FROM q;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_q8'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW dnr_q9 AS WITH q AS (SELECT * FROM (SELECT a, b FROM dnr_s) z) SELECT *, a + 1 AS c FROM q;
-- begin-expected
-- columns: d
-- row:  WITH q AS (\n         SELECT z.a,\n            z.b\n           FROM ( SELECT dnr_s.a,\n                    dnr_s.b\n                   FROM dnr_s) z\n        )\n SELECT a,\n    b,\n    a + 1 AS c\n   FROM q;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_q9'::regclass, true), chr(10), '\n') AS d;

-- stmt 9: the expansion is frozen, so a column added afterwards is not in the view
ALTER TABLE dnr_s ADD COLUMN c int;
-- begin-expected
-- columns: d
-- row:  WITH q AS (\n         SELECT dnr_s.a,\n            dnr_s.b\n           FROM dnr_s\n        )\n SELECT a,\n    b\n   FROM q;
-- end-expected
SELECT replace(pg_get_viewdef('dnr_q3'::regclass, true), chr(10), '\n') AS d;
-- begin-expected
-- columns: d
-- row: 2
-- end-expected
SELECT count(*)::text AS d FROM pg_attribute WHERE attrelid = 'dnr_q3'::regclass AND attnum > 0;
-- begin-expected
-- columns: d
-- row: 1/x
-- row: 2/y
-- end-expected
SELECT a::text || '/' || b AS d FROM dnr_q3 ORDER BY a;

-- cleanup
DROP VIEW dnr_cte, dnr_cte2, dnr_cte3, dnr_cte4, dnr_sub, dnr_sub2, dnr_sub3;
DROP VIEW dnr_lbl, dnr_un, dnr_quo, dnr_q1, dnr_q3, dnr_q4, dnr_q5, dnr_q6, dnr_q7, dnr_q8, dnr_q9;
DROP TABLE dnr_r1;
DROP TABLE dnr_l1;
DROP TABLE dnr_s;
