-- ============================================================================
-- A WITH item is read before the statement runs, and belongs to it alone
--
-- The rows a recursive item's own name stands for while it is being built are
-- scratch relations of the session running it. Installed as tables in the shared
-- schema they were visible to every other session, and for the length of the
-- recursion they displaced whatever stored relation carried the same name -- so
-- the query that followed read the wrong relation, and the item's own name no
-- longer meant the item.
--
-- A name declared twice is two items, not one: an inner WITH declaring a name the
-- query above already declared is a different query, and what it holds may not
-- answer for the outer one.
--
-- The scope a WITH clause opens closes with the statement, whether the statement
-- answered or raised. Left standing, a later statement of the same session could
-- read an item its own text never declared.
--
-- A statement that writes has no result of its own; RETURNING is what gives it
-- one. A query reading a WITH item that writes without RETURNING is asking for
-- rows that were never produced, and is refused before the write happens -- while
-- an item nothing reads is allowed, and still writes.
--
-- SEARCH and CYCLE are settled while the statement is analysed, so an item
-- carrying either is held to it whether or not anything goes on to read the item.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE wsr_edge (id int, parent int);
INSERT INTO wsr_edge VALUES (1, NULL), (2, 1), (3, 2), (4, 2);
CREATE TABLE wsr_named (n int);
INSERT INTO wsr_named VALUES (7);
CREATE TABLE wsr_log (n int);
INSERT INTO wsr_log VALUES (1), (2), (3);

-- ============================================================================
-- A recursive item's name means the item, and the table of that name is untouched
-- ============================================================================

-- The rows built under the name wsr_named are the recursion's own.
-- begin-expected
-- columns: built
-- row: 5
-- end-expected
WITH RECURSIVE wsr_named(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM wsr_named WHERE n < 5) SELECT count(*) AS built FROM wsr_named;

-- and the stored table of that name still holds what it held
-- begin-expected
-- columns: n
-- row: 7
-- end-expected
SELECT n FROM wsr_named;

-- A name with a schema written out is a reference to a stored relation, and
-- reaches the table even while a recursion of the same name is running.
-- begin-expected
-- columns: n
-- row: 7
-- end-expected
WITH RECURSIVE wsr_named(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM wsr_named WHERE n < 5) SELECT n FROM public.wsr_named;

-- Ordinary recursion over a table answers as it did.
-- begin-expected
-- columns: id | depth
-- row: 1 | 0
-- row: 2 | 1
-- row: 3 | 2
-- row: 4 | 2
-- end-expected
WITH RECURSIVE t(id, depth) AS (SELECT id, 0 FROM wsr_edge WHERE parent IS NULL UNION ALL SELECT e.id, t.depth + 1 FROM wsr_edge e JOIN t ON e.parent = t.id) SELECT id, depth FROM t ORDER BY id;

-- A recursion nested inside another one keeps its own working set.
-- begin-expected
-- columns: n | inner_count
-- row: 1 | 3
-- row: 2 | 3
-- row: 3 | 3
-- end-expected
WITH RECURSIVE outer_r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM outer_r WHERE n < 3) SELECT n, (WITH RECURSIVE inner_r(m) AS (SELECT 10 UNION ALL SELECT m + 1 FROM inner_r WHERE m < 12) SELECT count(*) FROM inner_r) AS inner_count FROM outer_r ORDER BY n;

-- ============================================================================
-- A name declared twice is two items
-- ============================================================================

-- The inner declaration answers inside the sub-select and nowhere else.
-- begin-expected
-- columns: inner_a | outer_a
-- row: 2 | 1
-- end-expected
WITH x AS (SELECT 1 AS a) SELECT (WITH x AS (SELECT 2 AS a) SELECT a FROM x) AS inner_a, (SELECT a FROM x) AS outer_a;

-- and the order the two are written in does not change either answer
-- begin-expected
-- columns: outer_a | inner_a
-- row: 1 | 2
-- end-expected
WITH x AS (SELECT 1 AS a) SELECT (SELECT a FROM x) AS outer_a, (WITH x AS (SELECT 2 AS a) SELECT a FROM x) AS inner_a;

-- The same holds for a sub-select in FROM.
-- begin-expected
-- columns: inner_a | outer_a
-- row: 2 | 1
-- end-expected
WITH x AS (SELECT 1 AS a) SELECT s.a AS inner_a, (SELECT a FROM x) AS outer_a FROM (WITH x AS (SELECT 2 AS a) SELECT a FROM x) s;

-- ============================================================================
-- A WITH scope closes with the statement, answered or raised
-- ============================================================================

-- A statement raising in one arm still takes its scope down.
-- begin-expected-error
-- sqlstate: 22012
-- end-expected-error
WITH s AS (SELECT 1 AS a) SELECT a FROM s UNION ALL SELECT 1 / 0;

-- and the item is gone from the session that raised
-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT a FROM s;

-- the same when the arm that raised is the right one
-- begin-expected-error
-- sqlstate: 22012
-- end-expected-error
WITH s AS (SELECT 1 AS a) SELECT 1 / 0 UNION ALL SELECT a FROM s;

-- begin-expected-error
-- sqlstate: 42P01
-- end-expected-error
SELECT a FROM s;

-- ============================================================================
-- A WITH item that writes answers only where RETURNING gave it rows
-- ============================================================================

-- Reading one that returns nothing is refused ...
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
WITH w AS (DELETE FROM wsr_log) SELECT * FROM w;

-- ... and the delete it would have applied did not happen
-- begin-expected
-- columns: still_here
-- row: 3
-- end-expected
SELECT count(*) AS still_here FROM wsr_log;

-- The same for an update ...
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
WITH w AS (UPDATE wsr_log SET n = n + 100) SELECT count(*) FROM w;

-- ... and for an insert ...
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
WITH w AS (INSERT INTO wsr_log VALUES (99)) SELECT * FROM w;

-- ... whose rows are all still what they were.
-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT n FROM wsr_log ORDER BY n;

-- An item nothing reads is not refused, and writes.
-- begin-expected
-- columns: answered
-- row: 1
-- end-expected
WITH w AS (INSERT INTO wsr_log VALUES (9)) SELECT 1 AS answered;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- row: 9
-- end-expected
SELECT n FROM wsr_log ORDER BY n;

-- An item read through RETURNING is what the clause exists for.
-- begin-expected
-- columns: n
-- row: 9
-- end-expected
WITH w AS (DELETE FROM wsr_log WHERE n = 9 RETURNING n) SELECT n FROM w;

-- begin-expected
-- columns: n
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT n FROM wsr_log ORDER BY n;

-- ============================================================================
-- SEARCH and CYCLE are held to the item whether or not it is read
-- ============================================================================

-- A column named twice in a BY list is asked for twice.
-- begin-expected-error
-- sqlstate: 42701
-- end-expected-error
WITH RECURSIVE w(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM w WHERE id < 3) SEARCH DEPTH FIRST BY id, id SET ord SELECT 1;

-- begin-expected-error
-- sqlstate: 42701
-- end-expected-error
WITH RECURSIVE w(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM w WHERE id < 3) CYCLE id, id SET c USING p SELECT 1;

-- A BY list may only name columns the item has.
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
WITH RECURSIVE w(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM w WHERE id < 3) SEARCH DEPTH FIRST BY nope SET ord SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
WITH RECURSIVE w(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM w WHERE id < 3) CYCLE nope SET c USING p SELECT 1;

-- The added column is in place while the recursive term is read, so a term
-- that names the colliding column without saying where it comes from is what
-- the statement is refused for.
-- begin-expected-error
-- sqlstate: 42702
-- end-expected-error
WITH RECURSIVE w(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM w WHERE id < 3) SEARCH DEPTH FIRST BY id SET id SELECT 1;

-- begin-expected-error
-- sqlstate: 42702
-- end-expected-error
WITH RECURSIVE w(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM w WHERE id < 3) CYCLE id SET id USING p SELECT 1;

-- begin-expected-error
-- sqlstate: 42702
-- end-expected-error
WITH RECURSIVE w(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM w WHERE id < 3) CYCLE id SET c USING id SELECT 1;

-- Where the recursive term does not name it, the collision itself is.
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
WITH RECURSIVE w(id, n) AS (SELECT 1, 1 UNION ALL SELECT 9, n + 1 FROM w WHERE n < 3) SEARCH DEPTH FIRST BY n SET id SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
WITH RECURSIVE w(id, n) AS (SELECT 1, 1 UNION ALL SELECT 9, n + 1 FROM w WHERE n < 3) CYCLE n SET id USING p SELECT 1;

-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
WITH RECURSIVE w(id, n) AS (SELECT 1, 1 UNION ALL SELECT 9, n + 1 FROM w WHERE n < 3) CYCLE n SET c USING id SELECT 1;

-- and the two clauses may not add the same name as each other
-- begin-expected-error
-- sqlstate: 42601
-- end-expected-error
WITH RECURSIVE w(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM w WHERE id < 3) SEARCH DEPTH FIRST BY id SET o CYCLE id SET o USING p SELECT 1;

-- The clauses an item can carry still answer.
-- begin-expected
-- columns: id | depth
-- row: 1 | 0
-- row: 2 | 1
-- row: 3 | 2
-- row: 4 | 2
-- end-expected
WITH RECURSIVE t(id, depth) AS (SELECT id, 0 FROM wsr_edge WHERE parent IS NULL UNION ALL SELECT e.id, t.depth + 1 FROM wsr_edge e JOIN t ON e.parent = t.id) SEARCH DEPTH FIRST BY id SET ord SELECT id, depth FROM t ORDER BY ord;

-- ============================================================================
-- RECURSIVE on an item that never names itself is not recursion
-- ============================================================================

-- The word is written once for the whole clause, so an item that writes may
-- stand under it; nothing about it is iterated.

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
WITH RECURSIVE w AS (INSERT INTO wsr_log VALUES (1) RETURNING n) SELECT n FROM w;

-- begin-expected
-- columns: n
-- row: 1
-- row: 1
-- end-expected
WITH RECURSIVE w AS (DELETE FROM wsr_log WHERE n = 1 RETURNING n) SELECT n FROM w;

-- begin-expected
-- columns: n
-- row: 2
-- row: 3
-- end-expected
SELECT n FROM wsr_log ORDER BY n;

-- teardown
DROP TABLE wsr_edge;
DROP TABLE wsr_named;
DROP TABLE wsr_log;
