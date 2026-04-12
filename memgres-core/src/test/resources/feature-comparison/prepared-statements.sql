-- ============================================================================
-- Feature Comparison: Prepared Statements (PREPARE / EXECUTE / DEALLOCATE)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   → expected result set
--   -- begin-expected-error / message-like: / end-expected-error → expected error
--   -- command: TAG                                       → expected command tag
--   -- note: ...                                          → informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS ps_test CASCADE;
CREATE TABLE ps_test (
    id   integer PRIMARY KEY,
    name text NOT NULL,
    val  numeric(10,2)
);
INSERT INTO ps_test VALUES (1, 'alpha', 10.50), (2, 'beta', 20.75), (3, 'gamma', 30.00);

DEALLOCATE ALL;

-- ============================================================================
-- 1. Basic PREPARE and EXECUTE
-- ============================================================================

-- 1a. PREPARE a simple SELECT
PREPARE ps_select AS SELECT id, name FROM ps_test ORDER BY id;

-- begin-expected
-- columns: id|name
-- row: 1|alpha
-- row: 2|beta
-- row: 3|gamma
-- end-expected
EXECUTE ps_select;

-- 1b. PREPARE with typed parameters
PREPARE ps_param (integer) AS SELECT name FROM ps_test WHERE id = $1;

-- begin-expected
-- columns: name
-- row: beta
-- end-expected
EXECUTE ps_param(2);

-- 1c. PREPARE with multiple parameters
PREPARE ps_multi (integer, text) AS SELECT id FROM ps_test WHERE id > $1 AND name = $2;

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
EXECUTE ps_multi(1, 'beta');

-- 1d. Execute same prepared statement multiple times
-- begin-expected
-- columns: name
-- row: alpha
-- end-expected
EXECUTE ps_param(1);

-- begin-expected
-- columns: name
-- row: gamma
-- end-expected
EXECUTE ps_param(3);

-- ============================================================================
-- 2. PREPARE with DML statements
-- ============================================================================

-- 2a. PREPARE INSERT
PREPARE ps_ins (integer, text, numeric) AS INSERT INTO ps_test VALUES ($1, $2, $3);

-- command: INSERT 0 1
EXECUTE ps_ins(4, 'delta', 40.25);

-- begin-expected
-- columns: name
-- row: delta
-- end-expected
SELECT name FROM ps_test WHERE id = 4;

-- 2b. PREPARE UPDATE
PREPARE ps_upd (text, integer) AS UPDATE ps_test SET name = $1 WHERE id = $2;

-- command: UPDATE 1
EXECUTE ps_upd('ALPHA', 1);

-- begin-expected
-- columns: name
-- row: ALPHA
-- end-expected
SELECT name FROM ps_test WHERE id = 1;

-- 2c. PREPARE DELETE
PREPARE ps_del (integer) AS DELETE FROM ps_test WHERE id = $1;

-- command: DELETE 1
EXECUTE ps_del(4);

-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) AS count FROM ps_test;

-- 2d. PREPARE INSERT ... RETURNING
PREPARE ps_ins_ret (integer, text, numeric) AS INSERT INTO ps_test VALUES ($1, $2, $3) RETURNING id, name;

-- begin-expected
-- columns: id|name
-- row: 5|epsilon
-- end-expected
EXECUTE ps_ins_ret(5, 'epsilon', 50.00);

DELETE FROM ps_test WHERE id = 5;

-- ============================================================================
-- 3. PREPARE with complex queries
-- ============================================================================

-- 3a. PREPARE with CTE
PREPARE ps_cte AS
    WITH ranked AS (
        SELECT id, name, row_number() OVER (ORDER BY val DESC) AS rn
        FROM ps_test
    )
    SELECT name FROM ranked WHERE rn = 1;

-- begin-expected
-- columns: name
-- row: gamma
-- end-expected
EXECUTE ps_cte;

-- 3b. PREPARE with subquery
PREPARE ps_sub AS SELECT name FROM ps_test WHERE val > (SELECT avg(val) FROM ps_test);

-- begin-expected
-- columns: name
-- row: gamma
-- end-expected
EXECUTE ps_sub;

-- 3c. PREPARE with aggregate
PREPARE ps_agg AS SELECT count(*) AS cnt, sum(val) AS total FROM ps_test;

-- begin-expected
-- columns: cnt|total
-- row: 3|61.25
-- end-expected
EXECUTE ps_agg;

-- ============================================================================
-- 4. DEALLOCATE
-- ============================================================================

-- 4a. DEALLOCATE specific
DEALLOCATE ps_select;

-- 4b. Verify it's gone
-- begin-expected-error
-- message-like: prepared statement "ps_select" does not exist
-- end-expected-error
EXECUTE ps_select;

-- 4c. DEALLOCATE ALL
DEALLOCATE ALL;

-- 4d. All gone
-- begin-expected-error
-- message-like: prepared statement "ps_param" does not exist
-- end-expected-error
EXECUTE ps_param(1);

-- ============================================================================
-- 5. Error cases
-- ============================================================================

-- 5a. Duplicate PREPARE name
PREPARE ps_dup AS SELECT 1;

-- begin-expected-error
-- message-like: prepared statement "ps_dup" already exists
-- end-expected-error
PREPARE ps_dup AS SELECT 2;

DEALLOCATE ps_dup;

-- 5b. DEALLOCATE nonexistent
-- begin-expected-error
-- message-like: prepared statement "nonexistent" does not exist
-- end-expected-error
DEALLOCATE nonexistent;

-- 5c. Wrong number of parameters
PREPARE ps_wrongp (integer) AS SELECT $1;

-- begin-expected-error
-- message-like: wrong number of parameters
-- end-expected-error
EXECUTE ps_wrongp(1, 2);

-- begin-expected-error
-- message-like: wrong number of parameters
-- end-expected-error
EXECUTE ps_wrongp;

DEALLOCATE ps_wrongp;

-- ============================================================================
-- 6. Name case sensitivity
-- ============================================================================

-- note: PG lowercases unquoted identifiers at parse time
PREPARE MyPlan AS SELECT 1 AS val;

-- begin-expected
-- columns: val
-- row: 1
-- end-expected
EXECUTE myplan;

DEALLOCATE myplan;

-- ============================================================================
-- 7. PREPARE with no parameters but parameterized body ($N refs)
-- ============================================================================

-- note: PG infers parameter count from $N references when no explicit type list given
PREPARE ps_infer AS SELECT $1::integer + $2::integer AS result;

-- begin-expected
-- columns: result
-- row: 30
-- end-expected
EXECUTE ps_infer(10, 20);

DEALLOCATE ps_infer;

-- ============================================================================
-- 8. DISCARD interactions
-- ============================================================================

PREPARE ps_disc1 AS SELECT 1;
PREPARE ps_disc2 AS SELECT 2;

-- 8a. DISCARD PLANS should NOT remove prepared statements
DISCARD PLANS;

-- begin-expected
-- columns: ?column?
-- row: 1
-- end-expected
EXECUTE ps_disc1;

-- begin-expected
-- columns: ?column?
-- row: 2
-- end-expected
EXECUTE ps_disc2;

-- 8b. DISCARD ALL removes prepared statements
DISCARD ALL;

-- begin-expected-error
-- message-like: prepared statement "ps_disc1" does not exist
-- end-expected-error
EXECUTE ps_disc1;

-- ============================================================================
-- 9. PREPARE with special SQL constructs
-- ============================================================================

-- 9a. String containing AS keyword
PREPARE ps_as_str AS SELECT 'looks AS if' AS label;

-- begin-expected
-- columns: label
-- row: looks AS if
-- end-expected
EXECUTE ps_as_str;

DEALLOCATE ps_as_str;

-- 9b. Dollar-quoted body
PREPARE ps_dollar AS SELECT $$hello AS world$$ AS txt;

-- begin-expected
-- columns: txt
-- row: hello AS world
-- end-expected
EXECUTE ps_dollar;

DEALLOCATE ps_dollar;

-- 9c. Block comment before AS
PREPARE ps_comment /* this is AS a comment */ AS SELECT 1 AS val;

-- begin-expected
-- columns: val
-- row: 1
-- end-expected
EXECUTE ps_comment;

DEALLOCATE ps_comment;

-- 9d. Line comment containing AS
PREPARE ps_lc -- AS tricky
AS SELECT 42 AS answer;

-- begin-expected
-- columns: answer
-- row: 42
-- end-expected
EXECUTE ps_lc;

DEALLOCATE ps_lc;

-- ============================================================================
-- 10. PREPARE / DEALLOCATE are not transactional
-- ============================================================================

-- 10a. PREPARE inside transaction survives COMMIT
BEGIN;
PREPARE ps_txn AS SELECT 'from txn' AS src;
COMMIT;

-- begin-expected
-- columns: src
-- row: from txn
-- end-expected
EXECUTE ps_txn;

-- 10b. DEALLOCATE inside transaction is permanent even after ROLLBACK
BEGIN;
DEALLOCATE ps_txn;
ROLLBACK;

-- note: In PG, DEALLOCATE is NOT transactional — it takes effect immediately
--       and cannot be rolled back. The prepared statement is gone.
-- begin-expected-error
-- message-like: prepared statement "ps_txn" does not exist
-- end-expected-error
EXECUTE ps_txn;

-- 10c. PREPARE inside a rolled-back transaction persists
-- note: In PG, PREPARE is also NOT transactional — it persists after ROLLBACK
BEGIN;
PREPARE ps_rb AS SELECT 'survived' AS val;
ROLLBACK;

-- begin-expected
-- columns: val
-- row: survived
-- end-expected
EXECUTE ps_rb;

DEALLOCATE ps_rb;

-- ============================================================================
-- 11. Re-PREPARE after DEALLOCATE (name reuse)
-- ============================================================================

PREPARE ps_reuse AS SELECT 'first' AS version;

-- begin-expected
-- columns: version
-- row: first
-- end-expected
EXECUTE ps_reuse;

DEALLOCATE ps_reuse;

PREPARE ps_reuse AS SELECT 'second' AS version;

-- begin-expected
-- columns: version
-- row: second
-- end-expected
EXECUTE ps_reuse;

DEALLOCATE ps_reuse;

-- ============================================================================
-- 12. PREPARE with various query constructs
-- ============================================================================

-- 12a. PREPARE with UNION
PREPARE ps_union AS
    SELECT 1 AS val UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY val;

-- begin-expected
-- columns: val
-- row: 1
-- row: 2
-- row: 3
-- end-expected
EXECUTE ps_union;

DEALLOCATE ps_union;

-- 12b. PREPARE with LIMIT/OFFSET
PREPARE ps_limit AS SELECT id FROM ps_test ORDER BY id LIMIT 2 OFFSET 1;

-- begin-expected
-- columns: id
-- row: 2
-- row: 3
-- end-expected
EXECUTE ps_limit;

DEALLOCATE ps_limit;

-- 12c. PREPARE with GROUP BY / HAVING
PREPARE ps_group AS
    SELECT val > 15 AS above_15, count(*) AS cnt
    FROM ps_test
    GROUP BY val > 15
    HAVING count(*) > 0
    ORDER BY above_15;

-- begin-expected
-- columns: above_15|cnt
-- row: false|1
-- row: true|2
-- end-expected
EXECUTE ps_group;

DEALLOCATE ps_group;

-- 12d. PREPARE with window function
PREPARE ps_window AS
    SELECT id, name, row_number() OVER (ORDER BY id) AS rn FROM ps_test ORDER BY id;

-- begin-expected
-- columns: id|name|rn
-- row: 1|alpha|1
-- row: 2|beta|2
-- row: 3|gamma|3
-- end-expected
EXECUTE ps_window;

DEALLOCATE ps_window;

-- 12e. PREPARE with DISTINCT
PREPARE ps_distinct AS
    SELECT DISTINCT val > 20 AS above_20 FROM ps_test ORDER BY above_20;

-- begin-expected
-- columns: above_20
-- row: false
-- row: true
-- end-expected
EXECUTE ps_distinct;

DEALLOCATE ps_distinct;

-- ============================================================================
-- 13. EXECUTE with expression parameters
-- ============================================================================

PREPARE ps_expr (integer, text) AS SELECT $1 AS num, $2 AS txt;

-- begin-expected
-- columns: num|txt
-- row: 3|hello world
-- end-expected
EXECUTE ps_expr(1 + 2, 'hello' || ' world');

DEALLOCATE ps_expr;

-- ============================================================================
-- 14. PREPARE UPDATE / DELETE ... RETURNING
-- ============================================================================

-- 14a. UPDATE RETURNING
PREPARE ps_upd_ret (text, integer) AS
    UPDATE ps_test SET name = $1 WHERE id = $2 RETURNING id, name;

-- begin-expected
-- columns: id|name
-- row: 1|UPDATED
-- end-expected
EXECUTE ps_upd_ret('UPDATED', 1);

UPDATE ps_test SET name = 'alpha' WHERE id = 1;
DEALLOCATE ps_upd_ret;

-- 14b. DELETE RETURNING
PREPARE ps_del_ret (integer) AS DELETE FROM ps_test WHERE id = $1 RETURNING id, name;

INSERT INTO ps_test VALUES (99, 'temp', 0);

-- begin-expected
-- columns: id|name
-- row: 99|temp
-- end-expected
EXECUTE ps_del_ret(99);

DEALLOCATE ps_del_ret;

-- ============================================================================
-- 15. $1 parameter reference not confused with dollar-quote
-- ============================================================================

-- note: $1 is a parameter reference, NOT a dollar-quote opening tag
--       Dollar-quote tags must start with letter or underscore after $
PREPARE ps_dollar_param (integer) AS SELECT $1 AS val;

-- begin-expected
-- columns: val
-- row: 42
-- end-expected
EXECUTE ps_dollar_param(42);

DEALLOCATE ps_dollar_param;

-- ============================================================================
-- 16. Double-quoted identifier containing AS
-- ============================================================================

PREPARE ps_dq_as AS SELECT 1 AS "column AS alias";

-- begin-expected
-- columns: column AS alias
-- row: 1
-- end-expected
EXECUTE ps_dq_as;

DEALLOCATE ps_dq_as;

-- ============================================================================
-- 17. EXPLAIN EXECUTE
-- ============================================================================

-- note: PG supports EXPLAIN EXECUTE to show the plan for a prepared statement.
--       This is a known gap in Memgres (no query planner), but the syntax should
--       at minimum not crash.

PREPARE ps_explain AS SELECT id FROM ps_test WHERE id = 1;

-- note: Expected to produce a plan output. Memgres may differ from PG here.
-- The exact output format is implementation-dependent.
EXPLAIN EXECUTE ps_explain;

DEALLOCATE ps_explain;

-- ============================================================================
-- 18. Nested dollar-quotes and tagged dollar-quotes
-- ============================================================================

-- 18a. Tagged dollar-quote
PREPARE ps_tagged AS SELECT $body$text with 'quotes' and "identifiers"$body$ AS val;

-- begin-expected
-- columns: val
-- row: text with 'quotes' and "identifiers"
-- end-expected
EXECUTE ps_tagged;

DEALLOCATE ps_tagged;

-- 18b. Dollar-quote containing $1 (should not confuse param parsing)
PREPARE ps_dq_param (integer) AS SELECT $1 AS num, $$literal $1 ref$$ AS txt;

-- begin-expected
-- columns: num|txt
-- row: 7|literal $1 ref
-- end-expected
EXECUTE ps_dq_param(7);

DEALLOCATE ps_dq_param;

-- ============================================================================
-- 19. PREPARE with boolean and null parameters
-- ============================================================================

PREPARE ps_bool (boolean) AS SELECT $1 AS flag;

-- begin-expected
-- columns: flag
-- row: true
-- end-expected
EXECUTE ps_bool(true);

-- begin-expected
-- columns: flag
-- row: false
-- end-expected
EXECUTE ps_bool(false);

DEALLOCATE ps_bool;

PREPARE ps_null_param (text) AS SELECT COALESCE($1, 'was_null') AS val;

-- begin-expected
-- columns: val
-- row: was_null
-- end-expected
EXECUTE ps_null_param(NULL);

DEALLOCATE ps_null_param;

-- ============================================================================
-- Cleanup
-- ============================================================================

DEALLOCATE ALL;
DROP TABLE IF EXISTS ps_test CASCADE;
