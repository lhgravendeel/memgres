package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 7 discard-commands.sql failures where Memgres diverges from PG 18.
 *
 * Stmt 13: pg_cursors count should be 2 after declaring 2 WITH HOLD cursors, Memgres returns 1
 * Stmt 15: pg_cursors count should be 1 after CLOSE one, Memgres returns 0
 * Stmt 17: SHOW statement_timeout should return '5s', Memgres returns '5000'
 * Stmt 32: pg_cursors cur_count should be 2 after declaring 2 cursors, Memgres returns 1
 * Stmt 35: pg_cursors cur_count should be 1 after DISCARD PLANS closes one, Memgres returns 0
 * Stmt 48: pg_cursors count should be 2, Memgres returns 1
 * Stmt 71: DISCARD ALL inside transaction should error 25001, Memgres succeeds
 */
class DiscardCommandsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS disc_test CASCADE");
            st.execute("CREATE TABLE disc_test (id integer PRIMARY KEY, name text)");
            st.execute("INSERT INTO disc_test VALUES (1, 'alpha'), (2, 'beta')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement st = conn.createStatement()) {
                st.execute("DEALLOCATE ALL");
                st.execute("DROP TABLE IF EXISTS disc_test CASCADE");
            } catch (Exception ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    /**
     * Stmt 13 - After DISCARD ALL removes prepared stmts, then declaring a
     * WITH HOLD cursor, pg_cursors count should be 2 (the new cursor plus
     * the portal cursor). Memgres returns 1.
     *
     * Context: Section 2 of discard-commands.sql. After the previous DISCARD ALL
     * (which cleared prepared stmts), we BEGIN, DECLARE disc_cur WITH HOLD, COMMIT,
     * then SELECT count(*) FROM pg_cursors. PG returns 2.
     */
    @Test
    void stmt13_pgCursorsCountShouldBe2AfterDeclaringWithHoldCursor() throws Exception {
        try (Statement st = conn.createStatement()) {
            // Clean slate
            st.execute("DISCARD ALL");
            st.execute("DEALLOCATE ALL");

            // Declare a WITH HOLD cursor (survives transaction end)
            st.execute("BEGIN");
            st.execute("DECLARE disc_cur CURSOR WITH HOLD FOR SELECT 1");
            st.execute("COMMIT");

            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*)::integer AS count FROM pg_cursors")) {
                assertTrue(rs.next(), "Expected a result row");
                assertEquals(2, rs.getInt("count"),
                        "pg_cursors should report 2 cursors after declaring a WITH HOLD cursor");
            }

            // Cleanup
            st.execute("CLOSE disc_cur");
        }
    }

    /**
     * Stmt 15 - After DISCARD ALL closes the WITH HOLD cursor, pg_cursors count
     * should be 1. Memgres returns 0.
     *
     * Context: Section 2 continued. After declaring disc_cur WITH HOLD and verifying
     * count=2, DISCARD ALL is run, then count should be 1.
     */
    @Test
    void stmt15_pgCursorsCountShouldBe1AfterDiscardAll() throws Exception {
        try (Statement st = conn.createStatement()) {
            // Clean slate
            st.execute("DISCARD ALL");
            st.execute("DEALLOCATE ALL");

            // Declare a WITH HOLD cursor
            st.execute("BEGIN");
            st.execute("DECLARE disc_cur CURSOR WITH HOLD FOR SELECT 1");
            st.execute("COMMIT");

            // DISCARD ALL should close the WITH HOLD cursor
            st.execute("DISCARD ALL");

            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*)::integer AS count FROM pg_cursors")) {
                assertTrue(rs.next(), "Expected a result row");
                assertEquals(1, rs.getInt("count"),
                        "pg_cursors should report 1 after DISCARD ALL closes the WITH HOLD cursor");
            }
        }
    }

    /**
     * Stmt 17 - SHOW statement_timeout after SET '5000' should return '5s'.
     * Memgres returns '5000'.
     *
     * Context: Section 3. SET statement_timeout = '5000' then SHOW statement_timeout.
     * PG normalizes the display to '5s'.
     */
    @Test
    void stmt17_showStatementTimeoutShouldReturn5s() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("SET statement_timeout = '5000'");

            try (ResultSet rs = st.executeQuery("SHOW statement_timeout")) {
                assertTrue(rs.next(), "Expected a result row");
                assertEquals("5s", rs.getString("statement_timeout"),
                        "SHOW statement_timeout should display '5s' after SET '5000'");
            }

            // Reset to default
            st.execute("RESET statement_timeout");
        }
    }

    /**
     * Stmt 32 - After setting up multiple session state items including a
     * WITH HOLD cursor, pg_cursors cur_count should be 2. Memgres returns 1.
     *
     * Context: Section 5 (combined effect). PREPARE disc_combo, SET work_mem,
     * CREATE TEMP TABLE, BEGIN, DECLARE disc_combo_cur WITH HOLD, COMMIT,
     * then SELECT count(*) AS cur_count FROM pg_cursors. PG returns 2.
     */
    @Test
    void stmt32_pgCursorsCurCountShouldBe2AfterCombinedSetup() throws Exception {
        try (Statement st = conn.createStatement()) {
            // Clean slate
            st.execute("DISCARD ALL");
            st.execute("DEALLOCATE ALL");

            // Set up multiple session state items
            st.execute("PREPARE disc_combo AS SELECT 'prepared' AS kind");
            st.execute("SET work_mem = '64MB'");
            try {
                st.execute("CREATE TEMP TABLE disc_combo_temp (x integer)");
            } catch (SQLException ignored) {
                // table may already exist
            }

            st.execute("BEGIN");
            st.execute("DECLARE disc_combo_cur CURSOR WITH HOLD FOR SELECT 'cursor' AS kind");
            st.execute("COMMIT");

            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*)::integer AS cur_count FROM pg_cursors")) {
                assertTrue(rs.next(), "Expected a result row");
                assertEquals(2, rs.getInt("cur_count"),
                        "pg_cursors should report 2 cursors after declaring a WITH HOLD cursor");
            }

            // Cleanup
            st.execute("DISCARD ALL");
        }
    }

    /**
     * Stmt 35 - After DISCARD ALL clears everything, pg_cursors cur_count should
     * be 1. Memgres returns 0.
     *
     * Context: Section 5 continued. After the combined setup and verifying cur_count=2,
     * DISCARD ALL is run, then cur_count should be 1.
     */
    @Test
    void stmt35_pgCursorsCurCountShouldBe1AfterDiscardAll() throws Exception {
        try (Statement st = conn.createStatement()) {
            // Clean slate
            st.execute("DISCARD ALL");
            st.execute("DEALLOCATE ALL");

            // Set up session state with a WITH HOLD cursor
            st.execute("PREPARE disc_combo AS SELECT 'prepared' AS kind");
            st.execute("SET work_mem = '64MB'");

            st.execute("BEGIN");
            st.execute("DECLARE disc_combo_cur CURSOR WITH HOLD FOR SELECT 'cursor' AS kind");
            st.execute("COMMIT");

            // DISCARD ALL should close the WITH HOLD cursor
            st.execute("DISCARD ALL");

            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*)::integer AS cur_count FROM pg_cursors")) {
                assertTrue(rs.next(), "Expected a result row");
                assertEquals(1, rs.getInt("cur_count"),
                        "pg_cursors should report 1 after DISCARD ALL closes the WITH HOLD cursor");
            }
        }
    }

    /**
     * Stmt 48 - After DISCARD PLANS, pg_cursors count should still be 2
     * (DISCARD PLANS does not affect cursors). Memgres returns 1.
     *
     * Context: Section 7. BEGIN, DECLARE disc_plan_cur WITH HOLD, COMMIT,
     * DISCARD PLANS, then SELECT count(*) FROM pg_cursors. PG returns 2.
     */
    @Test
    void stmt48_pgCursorsCountShouldBe2AfterDiscardPlans() throws Exception {
        try (Statement st = conn.createStatement()) {
            // Clean slate
            st.execute("DISCARD ALL");
            st.execute("DEALLOCATE ALL");

            // Declare a WITH HOLD cursor
            st.execute("BEGIN");
            st.execute("DECLARE disc_plan_cur CURSOR WITH HOLD FOR SELECT 1 AS val");
            st.execute("COMMIT");

            // DISCARD PLANS should NOT affect cursors
            st.execute("DISCARD PLANS");

            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*)::integer AS count FROM pg_cursors")) {
                assertTrue(rs.next(), "Expected a result row");
                assertEquals(2, rs.getInt("count"),
                        "pg_cursors should report 2 after DISCARD PLANS (cursors unaffected)");
            }

            // Cleanup
            st.execute("CLOSE disc_plan_cur");
        }
    }

    /**
     * Stmt 71 - DISCARD ALL inside a transaction block should fail with
     * SQLSTATE 25001. Memgres succeeds instead of erroring.
     *
     * Context: Section 12. BEGIN, then DISCARD ALL should raise:
     * "DISCARD ALL cannot run inside a transaction block" (25001).
     */
    @Test
    void stmt71_discardAllInsideTransactionShouldError25001() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("BEGIN");
            try {
                st.execute("DISCARD ALL");
                fail("DISCARD ALL inside a transaction block should throw SQLException with SQLSTATE 25001");
            } catch (SQLException e) {
                assertEquals("25001", e.getSQLState(),
                        "DISCARD ALL in transaction should produce SQLSTATE 25001");
                assertTrue(e.getMessage().contains("DISCARD ALL cannot run inside a transaction block"),
                        "Error message should mention that DISCARD ALL cannot run inside a transaction block");
            } finally {
                try {
                    st.execute("ROLLBACK");
                } catch (SQLException ignored) {
                    // transaction may already be aborted
                }
            }
        }
    }
}
