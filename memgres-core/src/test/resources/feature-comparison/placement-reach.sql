-- The clauses the aggregate/window placement walk did not reach, and the names it puts in its
-- messages.
--
-- The rule itself was already right: a clause read before there is a group to aggregate or a
-- result row to be numbered against cannot hold an aggregate or a window call, and PostgreSQL
-- names the clause. What was missing was reach. Nine kinds of clause never asked: ON CONFLICT's
-- DO UPDATE list and its WHERE, its index expression and index predicate, MERGE's ON condition
-- and every one of its actions, a function's arguments in FROM (a TABLESAMPLE percentage is one),
-- a WINDOW clause entry, a partition bound, a CREATE VIEW whose body is a VALUES list, and a SQL
-- function's body. Each of those either ran the statement or reported something else.
--
-- And four names were wrong. GROUPING is not an aggregate: PostgreSQL keeps a parallel set of
-- messages for it ("grouping operations are not allowed in FILTER"), and FILTER, GROUP BY,
-- RETURNING and a window frame offset all said "aggregate functions" instead. A window function
-- written without OVER is 42809 naming the function, in DML and in a definition as much as in a
-- SELECT, where it was reported as a function that does not exist.
--
-- All measured against PostgreSQL 18. The last section is the shape of ordinary SQL, which has to
-- keep working: the cost of a placement rule that reaches too far is a refused valid statement.

-- setup
DROP VIEW IF EXISTS plr_vvalues CASCADE;
DROP TABLE IF EXISTS plr_part CASCADE;
DROP TABLE IF EXISTS plr_src CASCADE;
DROP TABLE IF EXISTS plr_tgt CASCADE;

CREATE TABLE plr_tgt (id int PRIMARY KEY, a int, b text);
INSERT INTO plr_tgt VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 10, 'z');

CREATE TABLE plr_src (id int PRIMARY KEY, a int, c text);
INSERT INTO plr_src VALUES (1, 10, 'p'), (2, 30, 'q');

CREATE TABLE plr_part (a int) PARTITION BY RANGE (a);

-- 1: ON CONFLICT DO UPDATE is an UPDATE of one row, and its WHERE is a WHERE

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in UPDATE
-- end-expected-error
INSERT INTO plr_tgt VALUES (1, 1, 'w') ON CONFLICT (id) DO UPDATE SET a = count(*);

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in UPDATE
-- end-expected-error
INSERT INTO plr_tgt VALUES (1, 1, 'w') ON CONFLICT (id) DO UPDATE SET a = row_number() OVER ();

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
INSERT INTO plr_tgt VALUES (1, 1, 'w') ON CONFLICT (id) DO UPDATE SET a = 5 WHERE count(*) > 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in WHERE
-- end-expected-error
INSERT INTO plr_tgt VALUES (1, 1, 'w') ON CONFLICT (id) DO UPDATE SET a = 5
    WHERE row_number() OVER () = 1;

-- and nothing was written: the row still reads as it did
-- begin-expected
-- columns: id | a | b
-- row: 1, 10, x
-- end-expected
SELECT id, a, b FROM plr_tgt WHERE id = 1;

-- 2: an ON CONFLICT target names an index, so it is an index expression and an index predicate

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in index expressions
-- end-expected-error
INSERT INTO plr_tgt VALUES (1, 1, 'm') ON CONFLICT ((count(id))) DO NOTHING;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in index expressions
-- end-expected-error
INSERT INTO plr_tgt VALUES (1, 1, 'm') ON CONFLICT ((a + count(id))) DO NOTHING;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in index predicates
-- end-expected-error
INSERT INTO plr_tgt VALUES (1, 1, 'm') ON CONFLICT (id) WHERE count(*) > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in index predicates
-- end-expected-error
INSERT INTO plr_tgt VALUES (1, 1, 'm') ON CONFLICT (id) WHERE row_number() OVER () = 1 DO NOTHING;

-- 3: MERGE -- the ON clause is a join condition, the WHEN condition is its own clause, and each
-- action carries the name of the command it is

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in JOIN conditions
-- end-expected-error
MERGE INTO plr_tgt t USING plr_src u ON count(t.id) = 1 WHEN MATCHED THEN UPDATE SET a = 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in JOIN conditions
-- end-expected-error
MERGE INTO plr_tgt t USING plr_src u ON row_number() OVER () = 1 WHEN MATCHED THEN UPDATE SET a = 1;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in MERGE WHEN conditions
-- end-expected-error
MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id
    WHEN MATCHED AND count(*) > 1 THEN UPDATE SET a = 1;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in MERGE WHEN conditions
-- end-expected-error
MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id
    WHEN MATCHED AND row_number() OVER () = 1 THEN DELETE;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in UPDATE
-- end-expected-error
MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id WHEN MATCHED THEN UPDATE SET a = count(*);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in VALUES
-- end-expected-error
MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id
    WHEN NOT MATCHED THEN INSERT VALUES (count(*), 1, 'x');

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in VALUES
-- end-expected-error
MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id
    WHEN NOT MATCHED THEN INSERT VALUES (row_number() OVER (), 1, 'x');

-- and no row of either table was touched
-- begin-expected
-- columns: id | a | b
-- row: 1, 10, x
-- row: 2, 20, y
-- row: 3, 10, z
-- end-expected
SELECT id, a, b FROM plr_tgt ORDER BY id;

-- 4: a function in FROM is what the query reads, settled before there is a row to read

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in functions in FROM
-- end-expected-error
SELECT * FROM plr_tgt, generate_series(1, count(plr_tgt.a));

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in functions in FROM
-- end-expected-error
SELECT count(*) FROM generate_series(1, count(*)) g;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in functions in FROM
-- end-expected-error
SELECT * FROM generate_series(1, row_number() OVER ());

-- a TABLESAMPLE percentage is carried as a function item and is the same clause
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in functions in FROM
-- end-expected-error
SELECT * FROM plr_tgt TABLESAMPLE BERNOULLI (count(*));

-- 5: a WINDOW clause entry may hold an aggregate but not a window call

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in window definitions
-- end-expected-error
SELECT count(*) OVER w FROM plr_tgt WINDOW w AS (PARTITION BY row_number() OVER ());

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in window definitions
-- end-expected-error
SELECT count(*) OVER w FROM plr_tgt WINDOW w AS (ORDER BY row_number() OVER ());

-- an entry nothing references is judged all the same
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in window definitions
-- end-expected-error
SELECT 1 FROM plr_tgt WINDOW w AS (PARTITION BY row_number() OVER ());

-- 6: a partition bound is settled when the partition is created

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in partition bound
-- end-expected-error
CREATE TABLE plr_part_a PARTITION OF plr_part FOR VALUES FROM (count(1)) TO (10);

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in partition bound
-- end-expected-error
CREATE TABLE plr_part_b PARTITION OF plr_part FOR VALUES FROM (row_number() OVER ()) TO (10);

-- an ordinary constant expression is a bound like any other
CREATE TABLE plr_part_c PARTITION OF plr_part FOR VALUES FROM (abs(-1)) TO (1 + 9);
INSERT INTO plr_part VALUES (5);

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT a FROM plr_part_c;

-- 7: a view whose body is a VALUES list

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in VALUES
-- end-expected-error
CREATE VIEW plr_vbad AS VALUES (count(*));

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in VALUES
-- end-expected-error
CREATE VIEW plr_vbad AS VALUES (row_number() OVER ());

CREATE VIEW plr_vvalues AS VALUES (1), (2);

-- begin-expected
-- columns: column1
-- row: 1
-- row: 2
-- end-expected
SELECT * FROM plr_vvalues ORDER BY 1;

-- 8: a SQL function body is analysed when the function is written

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in WHERE
-- end-expected-error
CREATE FUNCTION plr_fbad() RETURNS int AS $$ SELECT a FROM plr_tgt WHERE count(a) > 1 $$
    LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
CREATE FUNCTION plr_fbad() RETURNS bigint AS $$ SELECT row_number() $$ LANGUAGE sql;

-- 9: a sub-query in a definition that is stored and replayed one row at a time

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in check constraint
-- end-expected-error
CREATE TABLE plr_chk (id int PRIMARY KEY, v int CHECK (v < (SELECT count(*) FROM plr_src)));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in check constraint
-- end-expected-error
ALTER TABLE plr_tgt ADD CONSTRAINT plr_c1 CHECK (a < (SELECT count(*) FROM plr_src));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in index expression
-- end-expected-error
CREATE INDEX plr_ix1 ON plr_tgt ((a + (SELECT count(*) FROM plr_src)));

-- 10: GROUPING is not an aggregate, and PostgreSQL says so in every clause that forbids it

-- begin-expected-error
-- sqlstate: 42803
-- message-like: grouping operations are not allowed in GROUP BY
-- end-expected-error
SELECT a FROM plr_tgt GROUP BY grouping(a);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: grouping operations are not allowed in FILTER
-- end-expected-error
SELECT count(*) FILTER (WHERE grouping(a) = 0) FROM plr_tgt GROUP BY a;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: grouping operations are not allowed in RETURNING
-- end-expected-error
UPDATE plr_tgt SET a = 1 RETURNING grouping(a);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: grouping operations are not allowed in window ROWS
-- end-expected-error
SELECT sum(a) OVER (ROWS grouping(a) PRECEDING) FROM plr_tgt GROUP BY a;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: grouping operations are not allowed in JOIN conditions
-- end-expected-error
SELECT * FROM plr_tgt JOIN plr_src ON grouping(plr_tgt.a) = 0;

-- begin-expected-error
-- sqlstate: 42803
-- message-like: grouping operations are not allowed in UPDATE
-- end-expected-error
MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id WHEN MATCHED THEN UPDATE SET a = grouping(t.a);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: grouping operations are not allowed in functions in FROM
-- end-expected-error
SELECT * FROM generate_series(1, grouping(1));

-- 11: a window function written without OVER, wherever it stands

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
UPDATE plr_tgt SET a = row_number() WHERE id = 1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
DELETE FROM plr_tgt WHERE row_number() = 1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
INSERT INTO plr_tgt VALUES (99, row_number(), 'w');

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
INSERT INTO plr_tgt VALUES (1, 1, 'w') ON CONFLICT (id) DO UPDATE SET a = row_number();

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function lag requires an OVER clause
-- end-expected-error
MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id WHEN MATCHED THEN UPDATE SET a = lag(u.a);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function lag requires an OVER clause
-- end-expected-error
CREATE INDEX plr_wi ON plr_tgt ((lag(a)));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
CREATE INDEX plr_wp ON plr_tgt (a) WHERE row_number() = 1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
CREATE TABLE plr_wt (x int CHECK (row_number() = 1));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function rank requires an OVER clause
-- end-expected-error
CREATE TABLE plr_wd (x int DEFAULT rank());

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function ntile requires an OVER clause
-- end-expected-error
UPDATE plr_tgt SET a = 1 RETURNING ntile(2);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
SELECT * FROM plr_tgt TABLESAMPLE BERNOULLI (row_number());

-- 12: what none of this may touch -- the shape of ordinary SQL

-- begin-expected
-- columns: a | count
-- row: 10, 2
-- row: 20, 1
-- end-expected
SELECT a, count(*) FROM plr_tgt GROUP BY a ORDER BY a;

-- begin-expected
-- columns: a | c
-- row: 20, 1
-- row: 10, 2
-- end-expected
SELECT a, count(*) c FROM plr_tgt GROUP BY a HAVING count(*) >= 1 ORDER BY c, a;

-- begin-expected
-- columns: a | b
-- row: 10, x
-- row: 20, y
-- end-expected
SELECT DISTINCT ON (a) a, b FROM plr_tgt ORDER BY a, b;

-- begin-expected
-- columns: a | rank
-- row: 10, 1
-- row: 10, 1
-- row: 20, 3
-- end-expected
SELECT a, rank() OVER (ORDER BY a) FROM plr_tgt ORDER BY 1, 2;

-- begin-expected
-- columns: a | rank
-- row: 10, 1
-- row: 20, 2
-- end-expected
SELECT a, rank() OVER (ORDER BY count(*) DESC) FROM plr_tgt GROUP BY a ORDER BY 1;

-- an aggregate in a window specification is ordinary: it is read once per result row
-- begin-expected
-- columns: a | count
-- row: 10, 1
-- row: 20, 1
-- end-expected
SELECT a, count(*) OVER w FROM plr_tgt GROUP BY a WINDOW w AS (PARTITION BY count(*)) ORDER BY 1;

-- begin-expected
-- columns: rn
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT sub.rn FROM (SELECT row_number() OVER (ORDER BY id) rn FROM plr_tgt) sub
    WHERE sub.rn >= 1 ORDER BY 1;

-- begin-expected
-- columns: a | n
-- row: 10, 2
-- row: 20, 1
-- end-expected
WITH c AS (SELECT a, count(*) n FROM plr_tgt GROUP BY a) SELECT * FROM c ORDER BY 1;

-- begin-expected
-- columns: a | count
-- row: 10, 1
-- row: 20, 1
-- end-expected
SELECT t.a, count(u.id) FROM plr_tgt t JOIN plr_src u ON t.id = u.id GROUP BY t.a ORDER BY 1;

-- begin-expected
-- columns: id | m
-- row: 1, 10
-- row: 2, 30
-- end-expected
SELECT t.id, l.m FROM plr_tgt t, LATERAL (SELECT max(u.a) m FROM plr_src u WHERE u.id = t.id) l
    WHERE l.m IS NOT NULL ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 10
-- row: 20
-- row: 30
-- end-expected
SELECT a FROM plr_tgt UNION SELECT a FROM plr_src ORDER BY 1;

-- begin-expected
-- columns: g
-- row: 1
-- row: 2
-- end-expected
SELECT * FROM generate_series(1, (SELECT count(*)::int FROM plr_src)) g ORDER BY 1;

-- an ON CONFLICT that means something still runs
INSERT INTO plr_tgt VALUES (1, 99, 'w') ON CONFLICT (id) DO UPDATE SET a = excluded.a
    WHERE plr_tgt.a < excluded.a;

-- begin-expected
-- columns: id | a
-- row: 1, 99
-- end-expected
SELECT id, a FROM plr_tgt WHERE id = 1;

-- and so does a MERGE
MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id
    WHEN MATCHED THEN UPDATE SET b = u.c
    WHEN NOT MATCHED THEN INSERT VALUES (u.id, u.a, u.c);

-- begin-expected
-- columns: id | b
-- row: 1, p
-- row: 2, q
-- row: 3, z
-- end-expected
SELECT id, b FROM plr_tgt ORDER BY id;

-- begin-expected
-- columns: a | grouping
-- row: 10, 0
-- row: 20, 0
-- row: 99, 0
-- row: NULL, 1
-- end-expected
SELECT a, grouping(a) FROM plr_tgt GROUP BY ROLLUP(a) ORDER BY 1, 2;

CREATE INDEX plr_okix ON plr_tgt ((a + 1)) WHERE a > 0;
CREATE FUNCTION plr_fok() RETURNS bigint AS $$ SELECT count(*) FROM plr_tgt $$ LANGUAGE sql;

-- begin-expected
-- columns: plr_fok
-- row: 3
-- end-expected
SELECT plr_fok();

-- teardown
DROP FUNCTION IF EXISTS plr_fok();
DROP VIEW IF EXISTS plr_vvalues CASCADE;
DROP TABLE IF EXISTS plr_part CASCADE;
DROP TABLE IF EXISTS plr_src CASCADE;
DROP TABLE IF EXISTS plr_tgt CASCADE;
