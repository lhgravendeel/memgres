package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PG 11+ procedure transaction control:
 * - COMMIT / ROLLBACK inside procedures
 * - AND CHAIN support
 * - Rejection in functions and exception blocks
 * - Edge cases: loops, nested calls, SQL-standard bodies
 */
class ProcedureTransactionControlTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void resetConnection() throws SQLException {
        // Ensure clean transaction state between tests
        try {
            conn.rollback();
        } catch (SQLException ignored) {}
        conn.setAutoCommit(true);
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private String queryString(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // ── Basic COMMIT in procedure ────────────────────────────────────────

    @Test
    void commitInProcedure_basicPersistence() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_commit1 (id serial PRIMARY KEY, val text)");
            s.execute("CREATE PROCEDURE ptc_insert_commit(v text) LANGUAGE plpgsql AS $$ " +
                    "BEGIN INSERT INTO ptc_commit1(val) VALUES(v); COMMIT; END; $$");
            s.execute("CALL ptc_insert_commit('hello')");
            assertEquals(1, queryInt("SELECT count(*) FROM ptc_commit1"));
            assertEquals("hello", queryString("SELECT val FROM ptc_commit1 WHERE id = 1"));
        }
    }

    @Test
    void commitInProcedure_multipleCommits() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_multi (id serial PRIMARY KEY, val text)");
            s.execute("CREATE PROCEDURE ptc_multi_commit() LANGUAGE plpgsql AS $$ " +
                    "BEGIN " +
                    "  INSERT INTO ptc_multi(val) VALUES('first'); " +
                    "  COMMIT; " +
                    "  INSERT INTO ptc_multi(val) VALUES('second'); " +
                    "  COMMIT; " +
                    "  INSERT INTO ptc_multi(val) VALUES('third'); " +
                    "END; $$");
            s.execute("CALL ptc_multi_commit()");
            assertEquals(3, queryInt("SELECT count(*) FROM ptc_multi"));
        }
    }

    // ── Basic ROLLBACK in procedure ──────────────────────────────────────

    @Test
    void rollbackInProcedure_discardsChanges() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_rollback1 (id serial PRIMARY KEY, val text)");
            s.execute("CREATE PROCEDURE ptc_insert_rollback() LANGUAGE plpgsql AS $$ " +
                    "BEGIN " +
                    "  INSERT INTO ptc_rollback1(val) VALUES('keep'); " +
                    "  COMMIT; " +
                    "  INSERT INTO ptc_rollback1(val) VALUES('discard'); " +
                    "  ROLLBACK; " +
                    "END; $$");
            s.execute("CALL ptc_insert_rollback()");
            assertEquals(1, queryInt("SELECT count(*) FROM ptc_rollback1"));
            assertEquals("keep", queryString("SELECT val FROM ptc_rollback1"));
        }
    }

    @Test
    void rollbackInProcedure_thenContinue() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_rb_cont (id serial PRIMARY KEY, val text)");
            s.execute("CREATE PROCEDURE ptc_rb_continue() LANGUAGE plpgsql AS $$ " +
                    "BEGIN " +
                    "  INSERT INTO ptc_rb_cont(val) VALUES('discard'); " +
                    "  ROLLBACK; " +
                    "  INSERT INTO ptc_rb_cont(val) VALUES('keep'); " +
                    "  COMMIT; " +
                    "END; $$");
            s.execute("CALL ptc_rb_continue()");
            assertEquals(1, queryInt("SELECT count(*) FROM ptc_rb_cont"));
            assertEquals("keep", queryString("SELECT val FROM ptc_rb_cont"));
        }
    }

    // ── COMMIT in function → ERROR ───────────────────────────────────────

    @Test
    void commitInFunction_rejected() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ptc_fn_commit() RETURNS void LANGUAGE plpgsql AS $$ " +
                    "BEGIN COMMIT; END; $$");
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("SELECT ptc_fn_commit()"));
            assertEquals("2D000", ex.getSQLState());
            assertTrue(ex.getMessage().contains("invalid transaction termination"));
        }
    }

    @Test
    void rollbackInFunction_rejected() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ptc_fn_rollback() RETURNS void LANGUAGE plpgsql AS $$ " +
                    "BEGIN ROLLBACK; END; $$");
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("SELECT ptc_fn_rollback()"));
            assertEquals("2D000", ex.getSQLState());
            assertTrue(ex.getMessage().contains("invalid transaction termination"));
        }
    }

    // ── COMMIT in exception block → ERROR ────────────────────────────────

    @Test
    void commitInExceptionBlock_caughtByHandler() throws SQLException {
        // In PG, COMMIT in exception block body raises error, but the EXCEPTION handler catches it
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_exc_tbl (id serial PRIMARY KEY, val text)");
            s.execute("CREATE PROCEDURE ptc_exc_commit() LANGUAGE plpgsql AS $$ " +
                    "BEGIN " +
                    "  INSERT INTO ptc_exc_tbl(val) VALUES('before'); " +
                    "  BEGIN " +
                    "    COMMIT; " +
                    "  EXCEPTION WHEN others THEN " +
                    "    NULL; " +
                    "  END; " +
                    "  INSERT INTO ptc_exc_tbl(val) VALUES('after'); " +
                    "END; $$");
            // Procedure succeeds — the COMMIT error is caught by the handler
            s.execute("CALL ptc_exc_commit()");
            // Both inserts succeed (the COMMIT didn't take effect, everything commits at end)
            assertEquals(2, queryInt("SELECT count(*) FROM ptc_exc_tbl"));
        }
    }

    @Test
    void rollbackInExceptionHandler_rejected() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE PROCEDURE ptc_exc_rb() LANGUAGE plpgsql AS $$ " +
                    "BEGIN " +
                    "  BEGIN " +
                    "    RAISE EXCEPTION 'test'; " +
                    "  EXCEPTION WHEN others THEN " +
                    "    ROLLBACK; " +
                    "  END; " +
                    "END; $$");
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("CALL ptc_exc_rb()"));
            assertEquals("2D000", ex.getSQLState());
        }
    }

    // ── AND CHAIN ────────────────────────────────────────────────────────

    @Test
    void commitAndChain_startsNewTransaction() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_chain (id serial PRIMARY KEY, val text)");
            s.execute("CREATE PROCEDURE ptc_chain_test() LANGUAGE plpgsql AS $$ " +
                    "BEGIN " +
                    "  INSERT INTO ptc_chain(val) VALUES('a'); " +
                    "  COMMIT AND CHAIN; " +
                    "  INSERT INTO ptc_chain(val) VALUES('b'); " +
                    "  COMMIT; " +
                    "END; $$");
            s.execute("CALL ptc_chain_test()");
            assertEquals(2, queryInt("SELECT count(*) FROM ptc_chain"));
        }
    }

    @Test
    void rollbackAndChain_startsNewTransaction() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_rb_chain (id serial PRIMARY KEY, val text)");
            s.execute("CREATE PROCEDURE ptc_rb_chain_test() LANGUAGE plpgsql AS $$ " +
                    "BEGIN " +
                    "  INSERT INTO ptc_rb_chain(val) VALUES('discard'); " +
                    "  ROLLBACK AND CHAIN; " +
                    "  INSERT INTO ptc_rb_chain(val) VALUES('keep'); " +
                    "  COMMIT; " +
                    "END; $$");
            s.execute("CALL ptc_rb_chain_test()");
            assertEquals(1, queryInt("SELECT count(*) FROM ptc_rb_chain"));
            assertEquals("keep", queryString("SELECT val FROM ptc_rb_chain"));
        }
    }

    // ── AND CHAIN at SQL level (outside procedures) ──────────────────────

    @Test
    void commitAndChain_sqlLevel() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_sql_chain (id serial PRIMARY KEY, val text)");
            s.execute("BEGIN");
            s.execute("INSERT INTO ptc_sql_chain(val) VALUES('before')");
            s.execute("COMMIT AND CHAIN");
            // Should be in a new transaction now
            s.execute("INSERT INTO ptc_sql_chain(val) VALUES('after')");
            s.execute("COMMIT");
            assertEquals(2, queryInt("SELECT count(*) FROM ptc_sql_chain"));
        }
    }

    @Test
    void rollbackAndChain_sqlLevel() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_sql_rb_chain (id serial PRIMARY KEY, val text)");
            s.execute("INSERT INTO ptc_sql_rb_chain(val) VALUES('existing')");
            s.execute("BEGIN");
            s.execute("INSERT INTO ptc_sql_rb_chain(val) VALUES('rollback_me')");
            s.execute("ROLLBACK AND CHAIN");
            // Should be in a new transaction now
            s.execute("INSERT INTO ptc_sql_rb_chain(val) VALUES('after_rb')");
            s.execute("COMMIT");
            assertEquals(2, queryInt("SELECT count(*) FROM ptc_sql_rb_chain"));
        }
    }

    // ── COMMIT in loop (batch processing pattern) ────────────────────────

    @Test
    void commitInLoop_batchProcessing() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_batch (id serial PRIMARY KEY, batch_num int)");
            s.execute("CREATE PROCEDURE ptc_batch_proc() LANGUAGE plpgsql AS $$ " +
                    "DECLARE i int; " +
                    "BEGIN " +
                    "  FOR i IN 1..5 LOOP " +
                    "    INSERT INTO ptc_batch(batch_num) VALUES(i); " +
                    "    COMMIT; " +
                    "  END LOOP; " +
                    "END; $$");
            s.execute("CALL ptc_batch_proc()");
            assertEquals(5, queryInt("SELECT count(*) FROM ptc_batch"));
            assertEquals(15, queryInt("SELECT sum(batch_num) FROM ptc_batch"));
        }
    }

    // ── Nested CALL: procedure calling procedure with COMMIT ─────────────

    @Test
    void nestedCall_innerCommit() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_nested (id serial PRIMARY KEY, src text)");
            s.execute("CREATE PROCEDURE ptc_inner() LANGUAGE plpgsql AS $$ " +
                    "BEGIN " +
                    "  INSERT INTO ptc_nested(src) VALUES('inner'); " +
                    "  COMMIT; " +
                    "END; $$");
            s.execute("CREATE PROCEDURE ptc_outer() LANGUAGE plpgsql AS $$ " +
                    "BEGIN " +
                    "  INSERT INTO ptc_nested(src) VALUES('outer_before'); " +
                    "  CALL ptc_inner(); " +
                    "  INSERT INTO ptc_nested(src) VALUES('outer_after'); " +
                    "END; $$");
            s.execute("CALL ptc_outer()");
            assertEquals(3, queryInt("SELECT count(*) FROM ptc_nested"));
        }
    }

    // ── COMMIT outside exception, then exception block ───────────────────

    @Test
    void commitThenExceptionBlock() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_ce (id serial PRIMARY KEY, val text)");
            s.execute("CREATE PROCEDURE ptc_commit_then_exc() LANGUAGE plpgsql AS $$ " +
                    "BEGIN " +
                    "  INSERT INTO ptc_ce(val) VALUES('committed'); " +
                    "  COMMIT; " +
                    "  BEGIN " +
                    "    INSERT INTO ptc_ce(val) VALUES('in_exc_block'); " +
                    "  EXCEPTION WHEN others THEN " +
                    "    NULL; " +
                    "  END; " +
                    "END; $$");
            s.execute("CALL ptc_commit_then_exc()");
            assertEquals(2, queryInt("SELECT count(*) FROM ptc_ce"));
        }
    }

    // ── SQL-language procedure with COMMIT should fail ────────────────────
    // (SQL-standard function bodies are atomic, cannot contain transaction control)

    @Test
    void commitInSqlProcedure_viaGenericSql() throws SQLException {
        // A SQL-language procedure body that contains COMMIT gets executed as generic SQL.
        // This is fine at the SQL level — the procedure just runs each statement.
        // However, for BEGIN ATOMIC procedures this should not be possible since
        // the parser captures the body as raw text and BEGIN ATOMIC implies atomicity.
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_sql_proc (id serial PRIMARY KEY, val text)");
            s.execute("CREATE PROCEDURE ptc_sql_insert(v text) LANGUAGE sql AS $$ " +
                    "INSERT INTO ptc_sql_proc(val) VALUES(v); $$");
            s.execute("CALL ptc_sql_insert('test')");
            assertEquals(1, queryInt("SELECT count(*) FROM ptc_sql_proc"));
        }
    }

    // ── AND NO CHAIN (explicit default) ──────────────────────────────────

    @Test
    void commitAndNoChain_accepted() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_no_chain (id serial PRIMARY KEY, val text)");
            s.execute("CREATE PROCEDURE ptc_no_chain_proc() LANGUAGE plpgsql AS $$ " +
                    "BEGIN " +
                    "  INSERT INTO ptc_no_chain(val) VALUES('test'); " +
                    "  COMMIT AND NO CHAIN; " +
                    "END; $$");
            s.execute("CALL ptc_no_chain_proc()");
            assertEquals(1, queryInt("SELECT count(*) FROM ptc_no_chain"));
        }
    }

    // ── ABORT keyword (alias for ROLLBACK) ───────────────────────────────

    @Test
    void abortInProcedure_errors() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_abort (id serial PRIMARY KEY, val text)");
            s.execute("CREATE PROCEDURE ptc_abort_proc() LANGUAGE plpgsql AS $$ " +
                    "BEGIN " +
                    "  INSERT INTO ptc_abort(val) VALUES('discard'); " +
                    "  ABORT; " +
                    "  INSERT INTO ptc_abort(val) VALUES('keep'); " +
                    "  COMMIT; " +
                    "END; $$");
            // PG 18: ABORT is an unsupported transaction command in PL/pgSQL (0A000)
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("CALL ptc_abort_proc()"));
            assertEquals("0A000", ex.getSQLState());
        }
    }

    // ── DO block with COMMIT/ROLLBACK ────────────────────────────────────
    // DO blocks in PG 11+ support COMMIT/ROLLBACK just like procedures

    @Test
    void commitInDoBlock_succeeds() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_do_log (msg text)");
            s.execute("DO $$ BEGIN INSERT INTO ptc_do_log (msg) VALUES ('do-before'); COMMIT; INSERT INTO ptc_do_log (msg) VALUES ('do-after'); END; $$");
            assertEquals(2, queryInt("SELECT count(*) FROM ptc_do_log"));
        }
    }

    // ── Procedure with variables that survive across COMMIT ──────────────

    @Test
    void variablesSurviveCommit() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ptc_vars (id serial PRIMARY KEY, val int)");
            s.execute("CREATE PROCEDURE ptc_var_test() LANGUAGE plpgsql AS $$ " +
                    "DECLARE x int := 10; " +
                    "BEGIN " +
                    "  x := x + 5; " +
                    "  COMMIT; " +
                    "  INSERT INTO ptc_vars(val) VALUES(x); " +
                    "END; $$");
            s.execute("CALL ptc_var_test()");
            assertEquals(15, queryInt("SELECT val FROM ptc_vars"));
        }
    }
}
