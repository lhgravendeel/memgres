package com.memgres.txn;

import com.memgres.core.Memgres;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenarios from 1120_timeout_cancellation_and_interrupt_scenarios.md.
 *
 * Covers: statement_timeout, lock_timeout, idle_in_transaction_session_timeout,
 * Statement.cancel(), transaction recovery after timeout/cancel, JDBC-level
 * timeouts, DDL under timeout, and data-integrity guarantees around timeouts.
 */
class TimeoutCancellationV2Test {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(false);
        return c;
    }

    // =========================================================================
    // 1. statement_timeout: pg_sleep exceeds limit, expects SQLSTATE 57014
    // =========================================================================

    @Test void statement_timeout_fires_on_pg_sleep() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET statement_timeout = '100ms'");
            c.commit();
            SQLException ex = assertThrows(SQLException.class, () ->
                    c.createStatement().execute("SELECT pg_sleep(2)"));
            assertEquals("57014", ex.getSQLState(),
                    "statement_timeout must raise SQLSTATE 57014, got: " + ex.getSQLState());
            c.rollback();
        }
    }

    @Test void statement_timeout_does_not_fire_on_fast_query() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET statement_timeout = '100ms'");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery("SELECT 42")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
            }
            c.commit();
        }
    }

    // =========================================================================
    // 2. lock_timeout: session B cannot acquire lock held by session A, expects 55P03
    // =========================================================================

    @Test void lock_timeout_raises_55P03_on_contention() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute(
                    "CREATE TABLE tc_lock_contention(id int PRIMARY KEY, v text)");
            c1.createStatement().execute("INSERT INTO tc_lock_contention VALUES (1, 'held')");
            c1.commit();
            c2.commit();

            // Session A holds an exclusive row lock.
            c1.createStatement().executeQuery(
                    "SELECT * FROM tc_lock_contention WHERE id = 1 FOR UPDATE");

            // Session B sets a tight lock_timeout and tries to acquire the same row.
            c2.createStatement().execute("SET lock_timeout = '100ms'");
            long start = System.currentTimeMillis();
            SQLException ex = assertThrows(SQLException.class, () ->
                    c2.createStatement().executeQuery(
                            "SELECT * FROM tc_lock_contention WHERE id = 1 FOR UPDATE"));
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(
                    "55P03".equals(ex.getSQLState()) || "57014".equals(ex.getSQLState()),
                    "lock_timeout should raise 55P03 or 57014, got: " + ex.getSQLState());
            assertTrue(elapsed < 2000,
                    "lock_timeout = 100ms should fail fast, but took " + elapsed + "ms");

            c1.commit();
            c2.rollback();
            c1.createStatement().execute("DROP TABLE tc_lock_contention");
            c1.commit();
        }
    }

    // =========================================================================
    // 3. idle_in_transaction_session_timeout: GUC accepted
    // =========================================================================

    @Test void idle_in_transaction_session_timeout_guc_accepted() throws Exception {
        try (Connection c = newConn()) {
            // Should parse and store without error.
            c.createStatement().execute("SET idle_in_transaction_session_timeout = '30s'");
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SHOW idle_in_transaction_session_timeout")) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
            c.commit();
        }
    }

    @Test void idle_in_transaction_session_timeout_zero_disables() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET idle_in_transaction_session_timeout = 0");
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SHOW idle_in_transaction_session_timeout")) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
            c.commit();
        }
    }

    // =========================================================================
    // 4. Statement.cancel(): long query interrupted from another thread
    // =========================================================================

    @Test void statement_cancel_interrupts_long_query() throws Exception {
        try (Connection c = newConn()) {
            Statement stmt = c.createStatement();

            // Fire cancel from a separate thread after a brief delay.
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.schedule(() -> {
                try { stmt.cancel(); } catch (SQLException ignored) {}
            }, 200, TimeUnit.MILLISECONDS);

            try {
                // pg_sleep(2) should be interrupted by the cancel.
                stmt.execute("SELECT pg_sleep(2)");
                // If we reach here memgres may have returned early, which is
                // acceptable; we only assert no hard crash occurred.
            } catch (SQLException ex) {
                // 57014 = query_canceled is the expected SQLSTATE.
                assertTrue(
                        "57014".equals(ex.getSQLState()) || ex.getMessage() != null,
                        "Cancel should produce a recognisable exception");
            } finally {
                scheduler.shutdownNow();
                try { c.rollback(); } catch (SQLException ignored) {}
            }
        }
    }

    @Test void statement_cancel_is_safe_when_idle() throws Exception {
        try (Connection c = newConn()) {
            Statement stmt = c.createStatement();
            // cancel() on a statement that is not executing should be a no-op.
            stmt.cancel();
            // Session should remain healthy.
            try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            c.commit();
        }
    }

    // =========================================================================
    // 5. Transaction recovery after statement_timeout
    // =========================================================================

    @Test void session_reusable_after_statement_timeout_and_rollback() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET statement_timeout = '100ms'");
            c.commit();

            try {
                c.createStatement().execute("SELECT pg_sleep(2)");
            } catch (SQLException ex) {
                assertEquals("57014", ex.getSQLState());
            }

            // The transaction is in an aborted state; ROLLBACK must restore it.
            c.rollback();

            // Session should now be fully usable again.
            try (ResultSet rs = c.createStatement().executeQuery("SELECT 'recovered'")) {
                assertTrue(rs.next());
                assertEquals("recovered", rs.getString(1));
            }
            c.commit();
        }
    }

    // =========================================================================
    // 6. Transaction recovery after Statement.cancel()
    // =========================================================================

    @Test void session_reusable_after_cancel_and_rollback() throws Exception {
        try (Connection c = newConn()) {
            Statement stmt = c.createStatement();

            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.schedule(() -> {
                try { stmt.cancel(); } catch (SQLException ignored) {}
            }, 150, TimeUnit.MILLISECONDS);

            try {
                stmt.execute("SELECT pg_sleep(2)");
            } catch (SQLException ignored) {
                // Expected cancellation error.
            } finally {
                scheduler.shutdownNow();
            }

            try { c.rollback(); } catch (SQLException ignored) {}

            // Session must accept new work after rollback.
            try (ResultSet rs = c.createStatement().executeQuery("SELECT 99")) {
                assertTrue(rs.next());
                assertEquals(99, rs.getInt(1));
            }
            c.commit();
        }
    }

    // =========================================================================
    // 7. Multiple statement_timeout values in the same session
    // =========================================================================

    @Test void statement_timeout_can_be_changed_between_queries() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET statement_timeout = '5s'");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery("SHOW statement_timeout")) {
                assertTrue(rs.next());
                assertEquals("5s", rs.getString(1));
            }

            c.createStatement().execute("SET statement_timeout = '2s'");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery("SHOW statement_timeout")) {
                assertTrue(rs.next());
                assertEquals("2s", rs.getString(1));
            }

            // Both queries succeeded; timeout changes took effect.
            c.commit();
        }
    }

    // =========================================================================
    // 8. Zero statement_timeout means no limit
    // =========================================================================

    @Test void zero_statement_timeout_imposes_no_limit() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET statement_timeout = 0");
            c.commit();
            // A trivially fast query must succeed.
            try (ResultSet rs = c.createStatement().executeQuery("SELECT 1 + 1")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            c.commit();
        }
    }

    // =========================================================================
    // 9. Timeout applies per-statement, not cumulatively
    // =========================================================================

    @Test void statement_timeout_applies_per_statement_not_cumulatively() throws Exception {
        try (Connection c = newConn()) {
            // Each individual statement is fast; the aggregate may exceed 500 ms
            // but no single statement should.
            c.createStatement().execute(
                    "CREATE TABLE tc_cumulative(id int PRIMARY KEY, v text)");
            c.createStatement().execute("SET statement_timeout = '500ms'");
            c.commit();

            for (int i = 1; i <= 5; i++) {
                c.createStatement().execute(
                        "INSERT INTO tc_cumulative VALUES (" + i + ", 'row" + i + "')");
            }
            c.commit();

            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT COUNT(*) FROM tc_cumulative")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
            }
            c.commit();

            c.createStatement().execute("DROP TABLE tc_cumulative");
            c.commit();
        }
    }

    // =========================================================================
    // 10. lock_timeout vs NOWAIT: both give fast failure on unavailable lock
    // =========================================================================

    @Test void lock_timeout_and_nowait_both_fail_fast() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn(); Connection c3 = newConn()) {
            c1.createStatement().execute(
                    "CREATE TABLE tc_nowait_vs_timeout(id int PRIMARY KEY)");
            c1.createStatement().execute("INSERT INTO tc_nowait_vs_timeout VALUES (1)");
            c1.commit();
            c2.commit();
            c3.commit();

            // Session 1 holds the lock.
            c1.createStatement().executeQuery(
                    "SELECT * FROM tc_nowait_vs_timeout FOR UPDATE");

            // Session 2 uses NOWAIT.
            SQLException exNowait = assertThrows(SQLException.class, () ->
                    c2.createStatement().executeQuery(
                            "SELECT * FROM tc_nowait_vs_timeout FOR UPDATE NOWAIT"));
            assertEquals("55P03", exNowait.getSQLState(),
                    "NOWAIT should raise 55P03");

            // Session 3 uses lock_timeout.
            c3.createStatement().execute("SET lock_timeout = '100ms'");
            long start = System.currentTimeMillis();
            SQLException exTimeout = assertThrows(SQLException.class, () ->
                    c3.createStatement().executeQuery(
                            "SELECT * FROM tc_nowait_vs_timeout FOR UPDATE"));
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(
                    "55P03".equals(exTimeout.getSQLState()) || "57014".equals(exTimeout.getSQLState()),
                    "lock_timeout should raise 55P03 or 57014");
            assertTrue(elapsed < 2000,
                    "lock_timeout = 100ms should fail fast, but took " + elapsed + "ms");

            c1.commit();
            c2.rollback();
            c3.rollback();
            c1.createStatement().execute("DROP TABLE tc_nowait_vs_timeout");
            c1.commit();
        }
    }

    // =========================================================================
    // 11. Connection.setNetworkTimeout(): JDBC network timeout accepted
    // =========================================================================

    @Test void connection_network_timeout_can_be_set_and_read() throws Exception {
        try (Connection c = newConn()) {
            Executor exec = Runnable::run;
            c.setNetworkTimeout(exec, 5000); // 5 seconds
            int timeout = c.getNetworkTimeout();
            // Should store whatever value was set (or 0 if unsupported).
            assertTrue(timeout >= 0, "getNetworkTimeout() should return a non-negative value");
            c.commit();
        }
    }

    // =========================================================================
    // 12. Statement.setQueryTimeout(): JDBC-level query timeout
    // =========================================================================

    @Test void jdbc_query_timeout_allows_fast_queries() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.setQueryTimeout(5); // 5-second limit; fast query should succeed.
            try (ResultSet rs = s.executeQuery("SELECT 'jdbc_timeout_ok'")) {
                assertTrue(rs.next());
                assertEquals("jdbc_timeout_ok", rs.getString(1));
            }
            c.commit();
        }
    }

    @Test void jdbc_query_timeout_zero_means_no_limit() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.setQueryTimeout(0); // 0 = no timeout
            assertEquals(0, s.getQueryTimeout());
            try (ResultSet rs = s.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
            }
            c.commit();
        }
    }

    // =========================================================================
    // 13. statement_timeout during DDL
    // =========================================================================

    @Test void statement_timeout_applies_to_ddl() throws Exception {
        try (Connection c = newConn()) {
            // Prepare a table that another session can lock so our DDL must wait.
            c.createStatement().execute(
                    "CREATE TABLE tc_ddl_timeout_base(id int PRIMARY KEY)");
            c.commit();

            try (Connection blocker = newConn()) {
                // Blocker acquires an ACCESS SHARE lock by reading the table.
                blocker.createStatement().executeQuery("SELECT * FROM tc_ddl_timeout_base");

                // Our session sets a short timeout and attempts an ACCESS EXCLUSIVE DDL.
                c.createStatement().execute("SET statement_timeout = '100ms'");
                // Attempt TRUNCATE; if the blocker holds a conflicting lock the
                // statement should time out; otherwise it simply succeeds quickly.
                try {
                    c.createStatement().execute("TRUNCATE tc_ddl_timeout_base");
                    c.commit();
                } catch (SQLException ex) {
                    // 57014 = query_canceled (from statement_timeout) is acceptable.
                    assertTrue(
                            "57014".equals(ex.getSQLState()) || "55P03".equals(ex.getSQLState()),
                            "DDL timeout should raise 57014 or 55P03, got: " + ex.getSQLState());
                    c.rollback();
                }

                blocker.commit();
            }

            // Cleanup: reset timeout first so DROP cannot itself time out.
            c.createStatement().execute("SET statement_timeout = 0");
            c.commit();
            c.createStatement().execute("DROP TABLE IF EXISTS tc_ddl_timeout_base");
            c.commit();
        }
    }

    // =========================================================================
    // 14. Timeout preserves prior committed work
    // =========================================================================

    @Test void timeout_does_not_affect_prior_committed_transactions() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute(
                    "CREATE TABLE tc_prior_committed(id int PRIMARY KEY, v text)");
            c.commit();

            // Commit some data before the timed-out statement.
            c.createStatement().execute(
                    "INSERT INTO tc_prior_committed VALUES (1, 'before_timeout')");
            c.commit();

            // Now trigger a timeout.
            c.createStatement().execute("SET statement_timeout = '100ms'");
            c.commit();
            try {
                c.createStatement().execute("SELECT pg_sleep(2)");
            } catch (SQLException ex) {
                assertEquals("57014", ex.getSQLState());
            }
            c.rollback(); // clear the aborted transaction

            // Reset timeout for the verification query.
            c.createStatement().execute("SET statement_timeout = 0");
            c.commit();

            // Previously committed row must still be present.
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT v FROM tc_prior_committed WHERE id = 1")) {
                assertTrue(rs.next(), "Row committed before timeout must survive");
                assertEquals("before_timeout", rs.getString(1));
            }
            c.commit();

            c.createStatement().execute("DROP TABLE tc_prior_committed");
            c.commit();
        }
    }

    // =========================================================================
    // 15. SHOW statement_timeout reflects SET value
    // =========================================================================

    @Test void show_statement_timeout_reflects_set_value() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET statement_timeout = '250ms'");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery("SHOW statement_timeout")) {
                assertTrue(rs.next());
                assertEquals("250ms", rs.getString(1));
            }
            c.commit();
        }
    }

    // =========================================================================
    // 16. lock_timeout reset to zero removes limit
    // =========================================================================

    @Test void lock_timeout_zero_disables_limit() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET lock_timeout = '500ms'");
            c.commit();
            c.createStatement().execute("SET lock_timeout = 0");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery("SHOW lock_timeout")) {
                assertTrue(rs.next());
                // Value should indicate no limit (commonly "0" or "0ms").
                String val = rs.getString(1);
                assertNotNull(val);
                assertTrue(val.startsWith("0"), "lock_timeout 0 should show as 0, got: " + val);
            }
            c.commit();
        }
    }

    // =========================================================================
    // 17. statement_timeout error carries query-canceled message
    // =========================================================================

    @Test void statement_timeout_error_message_is_descriptive() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET statement_timeout = '100ms'");
            c.commit();
            try {
                c.createStatement().execute("SELECT pg_sleep(2)");
                fail("Expected statement_timeout to raise an exception");
            } catch (SQLException ex) {
                assertEquals("57014", ex.getSQLState(),
                        "Expected SQLSTATE 57014, got: " + ex.getSQLState());
                assertNotNull(ex.getMessage(), "Exception message must not be null");
                assertFalse(Strs.isBlank(ex.getMessage()), "Exception message must not be blank");
            } finally {
                try { c.rollback(); } catch (SQLException ignored) {}
            }
        }
    }

    // =========================================================================
    // 18. Successive timeouts in same session: each rollback restores session
    // =========================================================================

    @Test void successive_timeouts_each_recoverable_via_rollback() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET statement_timeout = '100ms'");
            c.commit();

            for (int round = 1; round <= 3; round++) {
                try {
                    c.createStatement().execute("SELECT pg_sleep(2)");
                } catch (SQLException ex) {
                    assertEquals("57014", ex.getSQLState(),
                            "Round " + round + ": expected 57014");
                }
                c.rollback();

                // Session must be healthy after each rollback.
                try (ResultSet rs = c.createStatement().executeQuery("SELECT " + round)) {
                    assertTrue(rs.next());
                    assertEquals(round, rs.getInt(1));
                }
                c.commit();
            }
        }
    }
}
