package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 27 failures from procedure-transaction-control.sql where Memgres diverges from PG 18.
 *
 * PG 11+ allows COMMIT and ROLLBACK inside procedures (but NOT functions).
 * Memgres does not correctly handle in-procedure transaction control, causing
 * inserts that should have been rolled back to persist, incorrect row counts,
 * wrong SQLSTATE codes, and missing error enforcement.
 */
class ProcedureTransactionControlCompat15Test {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS ptc_test CASCADE");
            s.execute("CREATE SCHEMA ptc_test");
            s.execute("SET search_path = ptc_test, public");

            s.execute("CREATE TABLE ptc_log (id serial PRIMARY KEY, msg text)");

            // Procedure 1: Basic COMMIT
            s.execute("""
                CREATE PROCEDURE ptc_commit_basic() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('before commit');
                  COMMIT;
                  INSERT INTO ptc_log (msg) VALUES ('after commit');
                END;
                $$""");

            // Procedure 2: Basic ROLLBACK
            s.execute("""
                CREATE PROCEDURE ptc_rollback_basic() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('will be rolled back');
                  ROLLBACK;
                  INSERT INTO ptc_log (msg) VALUES ('after rollback');
                END;
                $$""");

            // Procedure 3: Multiple COMMITs
            s.execute("""
                CREATE PROCEDURE ptc_multi_commit() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('batch1');
                  COMMIT;
                  INSERT INTO ptc_log (msg) VALUES ('batch2');
                  COMMIT;
                  INSERT INTO ptc_log (msg) VALUES ('batch3');
                  COMMIT;
                END;
                $$""");

            // Procedure 4: COMMIT then ROLLBACK
            s.execute("""
                CREATE PROCEDURE ptc_commit_then_rollback() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('committed');
                  COMMIT;
                  INSERT INTO ptc_log (msg) VALUES ('rolled back');
                  ROLLBACK;
                  INSERT INTO ptc_log (msg) VALUES ('final');
                END;
                $$""");

            // Procedure 5: COMMIT AND CHAIN
            s.execute("""
                CREATE PROCEDURE ptc_commit_chain() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('before chain');
                  COMMIT AND CHAIN;
                  INSERT INTO ptc_log (msg) VALUES ('after chain');
                END;
                $$""");

            // Procedure 6: ROLLBACK AND CHAIN
            s.execute("""
                CREATE PROCEDURE ptc_rollback_chain() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('will be rolled back');
                  ROLLBACK AND CHAIN;
                  INSERT INTO ptc_log (msg) VALUES ('in new txn');
                END;
                $$""");

            // Procedure 7: Exception after COMMIT
            s.execute("""
                CREATE PROCEDURE ptc_error_after_commit() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('committed data');
                  COMMIT;
                  RAISE EXCEPTION 'intentional error';
                END;
                $$""");

            // Procedure 8: COMMIT in loop (batch processing)
            s.execute("""
                CREATE PROCEDURE ptc_batch_loop() LANGUAGE plpgsql AS $$
                DECLARE
                  i integer;
                BEGIN
                  FOR i IN 1..5 LOOP
                    INSERT INTO ptc_log (msg) VALUES ('item ' || i::text);
                    IF i % 2 = 0 THEN
                      COMMIT;
                    END IF;
                  END LOOP;
                END;
                $$""");

            // Functions 9 & 10: COMMIT/ROLLBACK rejected in function (created for later tests)
            s.execute("""
                CREATE FUNCTION ptc_bad_fn() RETURNS void LANGUAGE plpgsql AS $$
                BEGIN
                  COMMIT;
                END;
                $$""");

            s.execute("""
                CREATE FUNCTION ptc_bad_fn2() RETURNS void LANGUAGE plpgsql AS $$
                BEGIN
                  ROLLBACK;
                END;
                $$""");

            // Procedure 11: Nested procedure calls with COMMIT
            s.execute("""
                CREATE PROCEDURE ptc_inner() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('inner');
                  COMMIT;
                END;
                $$""");

            s.execute("""
                CREATE PROCEDURE ptc_outer() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('outer before');
                  CALL ptc_inner();
                  INSERT INTO ptc_log (msg) VALUES ('outer after');
                END;
                $$""");

            // Procedure 12: OUT parameters + COMMIT
            s.execute("""
                CREATE PROCEDURE ptc_out_param(INOUT result text) LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('with out param');
                  COMMIT;
                  result := 'done';
                END;
                $$""");

            // Procedure 13: ROLLBACK in exception handler
            s.execute("""
                CREATE PROCEDURE ptc_rollback_in_exception() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('before error');
                  COMMIT;
                  BEGIN
                    INSERT INTO ptc_log (msg) VALUES ('will fail');
                    PERFORM 1/0;
                  EXCEPTION
                    WHEN division_by_zero THEN
                      ROLLBACK;
                      INSERT INTO ptc_log (msg) VALUES ('recovered');
                  END;
                END;
                $$""");

            // Procedure 14: Empty procedure with just COMMIT
            s.execute("""
                CREATE PROCEDURE ptc_just_commit() LANGUAGE plpgsql AS $$
                BEGIN
                  COMMIT;
                END;
                $$""");

            // Procedure 15: COMMIT preserves sequence values
            s.execute("""
                CREATE PROCEDURE ptc_seq_test() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('seq1');
                  COMMIT;
                  INSERT INTO ptc_log (msg) VALUES ('seq2');
                END;
                $$""");

            // Procedure 16: Multiple ROLLBACKs
            s.execute("""
                CREATE PROCEDURE ptc_multi_rollback() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('will vanish');
                  ROLLBACK;
                  ROLLBACK;
                  INSERT INTO ptc_log (msg) VALUES ('survives');
                END;
                $$""");

            // Procedure 17: Recursive with COMMIT
            s.execute("""
                CREATE PROCEDURE ptc_recursive(n integer) LANGUAGE plpgsql AS $$
                BEGIN
                  IF n <= 0 THEN RETURN; END IF;
                  INSERT INTO ptc_log (msg) VALUES ('r' || n::text);
                  COMMIT;
                  CALL ptc_recursive(n - 1);
                END;
                $$""");

            // Procedure 18: SAVEPOINT inside procedure (should fail)
            s.execute("""
                CREATE PROCEDURE ptc_savepoint() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('before-sp');
                  SAVEPOINT sp1;
                  INSERT INTO ptc_log (msg) VALUES ('after-sp');
                  ROLLBACK TO SAVEPOINT sp1;
                  INSERT INTO ptc_log (msg) VALUES ('after-rollback-sp');
                  COMMIT;
                END;
                $$""");

            // Procedure 19: COMMIT in procedure called from function (should error)
            s.execute("""
                CREATE PROCEDURE ptc_inner_commit() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('inner');
                  COMMIT;
                END;
                $$""");

            s.execute("""
                CREATE FUNCTION ptc_func_calls_proc() RETURNS void LANGUAGE plpgsql AS $$
                BEGIN
                  CALL ptc_inner_commit();
                END;
                $$""");

            // Procedure 20: COMMIT in DO block is tested inline

            // Procedure 22: SET LOCAL reset across COMMIT
            s.execute("""
                CREATE PROCEDURE ptc_set_local() LANGUAGE plpgsql AS $$
                BEGIN
                  SET LOCAL work_mem = '256MB';
                  INSERT INTO ptc_log (msg) VALUES (current_setting('work_mem'));
                  COMMIT;
                  INSERT INTO ptc_log (msg) VALUES (current_setting('work_mem'));
                END;
                $$""");

            // Procedure 24: Nested exception handlers with COMMIT
            s.execute("""
                CREATE PROCEDURE ptc_nested_exception() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('outer-start');
                  COMMIT;
                  BEGIN
                    INSERT INTO ptc_log (msg) VALUES ('inner-start');
                    RAISE EXCEPTION 'inner error';
                  EXCEPTION WHEN OTHERS THEN
                    INSERT INTO ptc_log (msg) VALUES ('inner-caught');
                  END;
                END;
                $$""");

            // Procedures 25: Multiple procedures chained
            s.execute("""
                CREATE PROCEDURE ptc_chain_a() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('chain-a');
                  COMMIT;
                END;
                $$""");

            s.execute("""
                CREATE PROCEDURE ptc_chain_b() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('chain-b');
                  COMMIT;
                END;
                $$""");

            s.execute("""
                CREATE PROCEDURE ptc_chain_outer() LANGUAGE plpgsql AS $$
                BEGIN
                  CALL ptc_chain_a();
                  CALL ptc_chain_b();
                END;
                $$""");

            // Procedure 27: ABORT (synonym for ROLLBACK) in procedure
            s.execute("""
                CREATE PROCEDURE ptc_abort_test() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('discard-me');
                  ABORT;
                  INSERT INTO ptc_log (msg) VALUES ('after-abort');
                  COMMIT;
                END;
                $$""");

            // Procedure 28: Sequence values survive ROLLBACK
            s.execute("CREATE SEQUENCE ptc_seq");

            s.execute("""
                CREATE PROCEDURE ptc_seq_rollback() LANGUAGE plpgsql AS $$
                DECLARE
                  v1 bigint;
                  v2 bigint;
                BEGIN
                  v1 := nextval('ptc_seq');
                  INSERT INTO ptc_log (msg) VALUES ('seq=' || v1::text);
                  ROLLBACK;
                  v2 := nextval('ptc_seq');
                  INSERT INTO ptc_log (msg) VALUES ('seq=' || v2::text);
                  COMMIT;
                END;
                $$""");

            // Procedure 30: LOCK TABLE released after COMMIT
            s.execute("CREATE TABLE ptc_lockable (id integer PRIMARY KEY)");
            s.execute("INSERT INTO ptc_lockable VALUES (1)");

            s.execute("""
                CREATE PROCEDURE ptc_lock_commit() LANGUAGE plpgsql AS $$
                BEGIN
                  LOCK TABLE ptc_lockable IN ACCESS EXCLUSIVE MODE;
                  INSERT INTO ptc_log (msg) VALUES ('locked');
                  COMMIT;
                  INSERT INTO ptc_log (msg) VALUES ('unlocked');
                  COMMIT;
                END;
                $$""");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS ptc_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private void truncateLog() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("TRUNCATE ptc_log");
        }
    }

    private int countLog() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::integer AS cnt FROM ptc_log")) {
            assertTrue(rs.next());
            return rs.getInt("cnt");
        }
    }

    private List<String> getLogMessages() throws SQLException {
        List<String> msgs = new ArrayList<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT msg FROM ptc_log ORDER BY id")) {
            while (rs.next()) {
                msgs.add(rs.getString("msg"));
            }
        }
        return msgs;
    }

    // ==================== Test 2: Basic ROLLBACK (Stmt 11) ====================

    /**
     * Stmt 11: After CALL ptc_rollback_basic(), only 'after rollback' should remain.
     * The INSERT before ROLLBACK is undone.
     *
     * PG:      1 row  [after rollback]
     * Memgres: 3 rows [before commit] ; [after commit] ; [after rollback]
     */
    @Test
    void testStmt11_rollbackBasicMsgs() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_rollback_basic()");
        }
        List<String> msgs = getLogMessages();
        assertEquals(1, msgs.size(), "Should have exactly 1 row after rollback_basic");
        assertEquals("after rollback", msgs.get(0));
        truncateLog();
    }

    // ==================== Test 3: Multiple COMMITs (Stmt 15) ====================

    /**
     * Stmt 15: After CALL ptc_multi_commit(), count should be 3.
     *
     * PG:      3
     * Memgres: 6
     */
    @Test
    void testStmt15_multiCommitCount() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_multi_commit()");
        }
        assertEquals(3, countLog(), "multi_commit should produce exactly 3 rows");
        truncateLog();
    }

    // ==================== Test 4: COMMIT then ROLLBACK (Stmt 19, 20) ====================

    /**
     * Stmt 19: After CALL ptc_commit_then_rollback(), count should be 2.
     *
     * PG:      2
     * Memgres: 8
     */
    @Test
    void testStmt19_commitThenRollbackCount() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_commit_then_rollback()");
        }
        assertEquals(2, countLog(), "commit_then_rollback should produce exactly 2 rows");
    }

    /**
     * Stmt 20: After CALL ptc_commit_then_rollback(), messages should be 'committed' and 'final'.
     * 'rolled back' is undone by ROLLBACK.
     *
     * PG:      [committed] ; [final]
     * Memgres: 8 rows
     */
    @Test
    void testStmt20_commitThenRollbackMsgs() throws SQLException {
        // Relies on state from testStmt19 or re-runs
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_commit_then_rollback()");
        }
        List<String> msgs = getLogMessages();
        assertEquals(2, msgs.size(), "Should have exactly 2 rows");
        assertEquals("committed", msgs.get(0));
        assertEquals("final", msgs.get(1));
        truncateLog();
    }

    // ==================== Test 5: COMMIT AND CHAIN (Stmt 24) ====================

    /**
     * Stmt 24: After CALL ptc_commit_chain(), count should be 2.
     *
     * PG:      2
     * Memgres: 10
     */
    @Test
    void testStmt24_commitChainCount() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_commit_chain()");
        }
        assertEquals(2, countLog(), "commit_chain should produce exactly 2 rows");
        truncateLog();
    }

    // ==================== Test 6: ROLLBACK AND CHAIN (Stmt 28) ====================

    /**
     * Stmt 28: After CALL ptc_rollback_chain(), only 'in new txn' should remain.
     *
     * PG:      1 row [in new txn]
     * Memgres: 11 rows
     */
    @Test
    void testStmt28_rollbackChainMsgs() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_rollback_chain()");
        }
        List<String> msgs = getLogMessages();
        assertEquals(1, msgs.size(), "Should have exactly 1 row after rollback_chain");
        assertEquals("in new txn", msgs.get(0));
        truncateLog();
    }

    // ==================== Test 7: Exception after COMMIT (Stmt 32) ====================

    /**
     * Stmt 32: After CALL ptc_error_after_commit() (which raises an error),
     * the committed data should survive. Only 'committed data' should be in ptc_log.
     *
     * PG:      1 row [committed data]
     * Memgres: 12 rows
     */
    @Test
    void testStmt32_errorAfterCommitMsgs() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            try {
                s.execute("CALL ptc_error_after_commit()");
                fail("Expected an error from ptc_error_after_commit()");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("intentional error"),
                        "Error should mention 'intentional error', got: " + e.getMessage());
            }
        }
        List<String> msgs = getLogMessages();
        assertEquals(1, msgs.size(), "Only the committed row should survive the error");
        assertEquals("committed data", msgs.get(0));
        truncateLog();
    }

    // ==================== Test 8: COMMIT in loop (Stmt 36) ====================

    /**
     * Stmt 36: After CALL ptc_batch_loop(), count should be 5.
     *
     * PG:      5
     * Memgres: 17
     */
    @Test
    void testStmt36_batchLoopCount() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_batch_loop()");
        }
        assertEquals(5, countLog(), "batch_loop should produce exactly 5 rows");
        truncateLog();
    }

    // ==================== Test 11: Nested procedure calls (Stmt 43) ====================

    /**
     * Stmt 43: After CALL ptc_outer(), count should be 3.
     *
     * PG:      3
     * Memgres: 20
     */
    @Test
    void testStmt43_nestedProcedureCount() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_outer()");
        }
        assertEquals(3, countLog(), "nested procedure call should produce exactly 3 rows");
        truncateLog();
    }

    // ==================== Test 12: OUT parameters (Stmt 47) ====================

    /**
     * Stmt 47: After CALL ptc_out_param(NULL), ptc_log should have 'with out param'.
     *
     * PG:      1 row [with out param]
     * Memgres: 21 rows
     */
    @Test
    void testStmt47_outParamMsgs() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_out_param(NULL)");
        }
        List<String> msgs = getLogMessages();
        assertEquals(1, msgs.size(), "out_param should produce exactly 1 log row");
        assertEquals("with out param", msgs.get(0));
        truncateLog();
    }

    // ==================== Test 13: ROLLBACK in exception handler (Stmt 51) ====================

    /**
     * Stmt 51: After CALL ptc_rollback_in_exception(), count should be 1.
     * In PG 18, ROLLBACK inside exception handler is rejected at runtime because
     * exception handlers use subtransactions. The 'before error' row was committed,
     * but 'will fail' is rolled back by the subtransaction savepoint when the
     * division_by_zero exception fires. Then ROLLBACK in the handler throws
     * "cannot rollback while a subtransaction is active" which aborts the procedure.
     *
     * PG 18:   1
     * Memgres:  1
     */
    @Test
    void testStmt51_rollbackInExceptionCount() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            try {
                s.execute("CALL ptc_rollback_in_exception()");
            } catch (SQLException ignored) {
                // PG errors here with "cannot rollback while a subtransaction is active"
            }
        }
        assertEquals(1, countLog(),
                "rollback_in_exception: 'before error' was committed, 'will fail' rolled back by subtxn = 1 row");
        truncateLog();
    }

    // ==================== Test 15: Sequence values across COMMIT (Stmt 58) ====================

    /**
     * Stmt 58: After CALL ptc_seq_test(), count should be 2.
     *
     * PG:      2
     * Memgres: 25
     */
    @Test
    void testStmt58_seqTestCount() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_seq_test()");
        }
        assertEquals(2, countLog(), "seq_test should produce exactly 2 rows");
        truncateLog();
    }

    // ==================== Test 16: Multiple ROLLBACKs (Stmt 63) ====================

    /**
     * Stmt 63: After CALL ptc_multi_rollback(), only 'survives' should remain.
     *
     * PG:      1 row [survives]
     * Memgres: 26 rows
     */
    @Test
    void testStmt63_multiRollbackMsgs() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_multi_rollback()");
        }
        List<String> msgs = getLogMessages();
        assertEquals(1, msgs.size(), "multi_rollback should leave exactly 1 row");
        assertEquals("survives", msgs.get(0));
        truncateLog();
    }

    // ==================== Test 17: Recursive with COMMIT (Stmt 67) ====================

    /**
     * Stmt 67: After CALL ptc_recursive(3), count should be 3.
     *
     * PG:      3
     * Memgres: 29
     */
    @Test
    void testStmt67_recursiveCount() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_recursive(3)");
        }
        assertEquals(3, countLog(), "recursive(3) should produce exactly 3 rows");
        truncateLog();
    }

    // ==================== Test 18: SAVEPOINT in procedure (Stmt 70, 71) ====================

    /**
     * Stmt 70: CALL ptc_savepoint() should error with SQLSTATE 42883
     * (procedure does not exist, because PG rejects SAVEPOINT in procedures).
     *
     * PG:      ERROR [42883]: procedure ptc_savepoint() does not exist
     * Memgres: ERROR [42601]: Unsupported statement at position 0 near 'TO'
     */
    @Test
    void testStmt70_savepointSqlstate() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_savepoint()");
            fail("Expected an error from CALL ptc_savepoint()");
        } catch (SQLException e) {
            assertEquals("42883", e.getSQLState(),
                    "SQLSTATE should be 42883 (procedure does not exist), got: "
                            + e.getSQLState() + " - " + e.getMessage());
            assertTrue(e.getMessage().contains("does not exist"),
                    "Error should mention 'does not exist', got: " + e.getMessage());
        }
    }

    /**
     * Stmt 71: After the failed CALL ptc_savepoint(), ptc_log should be empty.
     *
     * PG:      0 rows
     * Memgres: 29 rows
     */
    @Test
    void testStmt71_savepointLogEmpty() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            try {
                s.execute("CALL ptc_savepoint()");
            } catch (SQLException ignored) {
                // Expected error
            }
        }
        List<String> msgs = getLogMessages();
        assertEquals(0, msgs.size(), "ptc_log should be empty after failed savepoint call");
        truncateLog();
    }

    // ==================== Test 19: COMMIT in proc called from function (Stmt 75) ====================

    /**
     * Stmt 75: SELECT ptc_func_calls_proc() should error with SQLSTATE 2D000
     * (invalid transaction termination) because functions cannot do transaction control.
     *
     * PG:      ERROR [2D000]: invalid transaction termination
     * Memgres: OK (succeeds with NULL)
     */
    @Test
    void testStmt75_funcCallsProcShouldError() throws SQLException {
        try (Statement s = conn.createStatement()) {
            try {
                s.executeQuery("SELECT ptc_func_calls_proc()");
                fail("Expected error 2D000 (invalid transaction termination) when function calls proc with COMMIT");
            } catch (SQLException e) {
                assertEquals("2D000", e.getSQLState(),
                        "SQLSTATE should be 2D000 (invalid_transaction_termination), got: "
                                + e.getSQLState() + " - " + e.getMessage());
            }
        }
    }

    // ==================== Test 20: COMMIT in DO block (Stmt 77) ====================

    /**
     * Stmt 77: After DO block with COMMIT, count should be 2.
     *
     * PG:      2
     * Memgres: 31
     */
    @Test
    void testStmt77_doBlockCommitCount() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("""
                DO $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('do-before');
                  COMMIT;
                  INSERT INTO ptc_log (msg) VALUES ('do-after');
                END;
                $$""");
        }
        assertEquals(2, countLog(), "DO block with COMMIT should produce exactly 2 rows");
        truncateLog();
    }

    // ==================== Test 22: SET LOCAL reset across COMMIT (Stmt 85) ====================

    /**
     * Stmt 85: After CALL ptc_set_local(), count should be 2.
     *
     * PG:      2
     * Memgres: 33
     */
    @Test
    void testStmt85_setLocalCount() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_set_local()");
        }
        assertEquals(2, countLog(), "set_local should produce exactly 2 rows");
        truncateLog();
    }

    // ==================== Test 23: Cursor WITH HOLD across COMMIT (Stmt 89) ====================

    /**
     * Stmt 89: After CALL ptc_cursor_hold() (which may error), ptc_log should be empty
     * according to PG behavior (0 rows).
     *
     * PG:      0 rows
     * Memgres: 33 rows
     */
    @Test
    void testStmt89_cursorHoldMsgs() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            // Create the procedure inline since it uses CURSOR WITH HOLD
            s.execute("""
                CREATE OR REPLACE PROCEDURE ptc_cursor_hold() LANGUAGE plpgsql AS $$
                DECLARE
                  cur CURSOR WITH HOLD FOR SELECT generate_series(1, 3) AS n;
                  rec record;
                BEGIN
                  OPEN cur;
                  FETCH cur INTO rec;
                  INSERT INTO ptc_log (msg) VALUES ('fetched-' || rec.n::text);
                  COMMIT;
                  FETCH cur INTO rec;
                  INSERT INTO ptc_log (msg) VALUES ('fetched-' || rec.n::text);
                  CLOSE cur;
                END;
                $$""");
            try {
                s.execute("CALL ptc_cursor_hold()");
            } catch (SQLException ignored) {
                // May error
            }
        }
        List<String> msgs = getLogMessages();
        assertEquals(0, msgs.size(), "cursor_hold: PG expects 0 rows in ptc_log");
        truncateLog();
    }

    // ==================== Test 24: Nested exception handlers (Stmt 93) ====================

    /**
     * Stmt 93: After CALL ptc_nested_exception(), count should be 2.
     *
     * PG:      2
     * Memgres: 36
     */
    @Test
    void testStmt93_nestedExceptionCount() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_nested_exception()");
        }
        assertEquals(2, countLog(), "nested_exception should produce exactly 2 rows");
        truncateLog();
    }

    // ==================== Test 25: Multiple procedures chained (Stmt 99) ====================

    /**
     * Stmt 99: After CALL ptc_chain_outer(), messages should be 'chain-a' and 'chain-b'.
     *
     * PG:      2 rows [chain-a] ; [chain-b]
     * Memgres: 38 rows
     */
    @Test
    void testStmt99_chainOuterMsgs() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_chain_outer()");
        }
        List<String> msgs = getLogMessages();
        assertEquals(2, msgs.size(), "chain_outer should produce exactly 2 rows");
        assertEquals("chain-a", msgs.get(0));
        assertEquals("chain-b", msgs.get(1));
        truncateLog();
    }

    // ==================== Test 27: ABORT in procedure (Stmt 109, 110) ====================

    /**
     * Stmt 109: CALL ptc_abort_test() should error with SQLSTATE 0A000
     * (unsupported transaction command in PL/pgSQL).
     *
     * PG:      ERROR [0A000]: unsupported transaction command in PL/pgSQL
     * Memgres: OK (succeeds)
     */
    @Test
    void testStmt109_abortTestShouldError() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_abort_test()");
            fail("Expected error 0A000 (unsupported transaction command) from CALL ptc_abort_test()");
        } catch (SQLException e) {
            assertEquals("0A000", e.getSQLState(),
                    "SQLSTATE should be 0A000 (feature_not_supported), got: "
                            + e.getSQLState() + " - " + e.getMessage());
            assertTrue(e.getMessage().contains("unsupported transaction command"),
                    "Error should mention 'unsupported transaction command', got: " + e.getMessage());
        }
    }

    /**
     * Stmt 110: After the failed CALL ptc_abort_test(), ptc_log should be empty.
     *
     * PG:      0 rows
     * Memgres: 39 rows
     */
    @Test
    void testStmt110_abortTestLogEmpty() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            try {
                s.execute("CALL ptc_abort_test()");
            } catch (SQLException ignored) {
                // Expected error
            }
        }
        List<String> msgs = getLogMessages();
        assertEquals(0, msgs.size(), "ptc_log should be empty after failed abort_test call");
        truncateLog();
    }

    // ==================== Test 28: Sequence values survive ROLLBACK (Stmt 115) ====================

    /**
     * Stmt 115: After CALL ptc_seq_rollback(), only 'seq=2' should remain.
     * The first nextval (1) was rolled back but sequence doesn't revert;
     * second nextval should be 2.
     *
     * PG:      1 row [seq=2]
     * Memgres: 40 rows
     */
    @Test
    void testStmt115_seqRollbackMsgs() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            // Reset the sequence for a clean test
            s.execute("ALTER SEQUENCE ptc_seq RESTART WITH 1");
            s.execute("CALL ptc_seq_rollback()");
        }
        List<String> msgs = getLogMessages();
        assertEquals(1, msgs.size(), "seq_rollback should leave exactly 1 row");
        assertEquals("seq=2", msgs.get(0));
        truncateLog();
    }

    // ==================== Test 29: AND CHAIN preserves isolation (Stmt 121) ====================

    /**
     * Stmt 121: After BEGIN ISOLATION LEVEL SERIALIZABLE; CALL ptc_chain_isolation();
     * ptc_log should be empty (0 rows) according to PG expected output.
     *
     * PG:      0 rows
     * Memgres: 42 rows
     */
    @Test
    void testStmt121_chainIsolationMsgs() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("""
                CREATE OR REPLACE PROCEDURE ptc_chain_isolation() LANGUAGE plpgsql AS $$
                BEGIN
                  INSERT INTO ptc_log (msg) VALUES ('batch1');
                  COMMIT AND CHAIN;
                  INSERT INTO ptc_log (msg) VALUES ('batch2');
                  COMMIT;
                END;
                $$""");
            try {
                s.execute("BEGIN ISOLATION LEVEL SERIALIZABLE");
                s.execute("CALL ptc_chain_isolation()");
            } catch (SQLException ignored) {
                // May error
            }
        }
        List<String> msgs = getLogMessages();
        assertEquals(0, msgs.size(), "chain_isolation: PG expects 0 rows in ptc_log");
        truncateLog();
    }

    // ==================== Test 30: LOCK TABLE released after COMMIT (Stmt 127) ====================

    /**
     * Stmt 127: After CALL ptc_lock_commit(), messages should be 'locked' and 'unlocked'.
     *
     * PG:      2 rows [locked] ; [unlocked]
     * Memgres: 44 rows
     */
    @Test
    void testStmt127_lockCommitMsgs() throws SQLException {
        truncateLog();
        try (Statement s = conn.createStatement()) {
            s.execute("CALL ptc_lock_commit()");
        }
        List<String> msgs = getLogMessages();
        assertEquals(2, msgs.size(), "lock_commit should produce exactly 2 rows");
        assertEquals("locked", msgs.get(0));
        assertEquals("unlocked", msgs.get(1));
        truncateLog();
    }
}
