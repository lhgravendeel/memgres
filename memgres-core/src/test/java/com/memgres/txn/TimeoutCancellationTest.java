package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document sections 9, 20: Cancellation and timeout behavior.
 * Tests statement timeout, lock timeout, idle-in-transaction timeout.
 */
class TimeoutCancellationTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    // --- Statement timeout ---

    @Test void statement_timeout_setting() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET statement_timeout = '1s'");
            try (ResultSet rs = c.createStatement().executeQuery("SHOW statement_timeout")) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
        }
    }

    @Test void statement_timeout_zero_means_no_limit() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET statement_timeout = 0");
            // Should execute without timeout
            try (ResultSet rs = c.createStatement().executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test void statement_timeout_on_long_query() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET statement_timeout = '100ms'");
            // A fast query should succeed even with short timeout
            try (ResultSet rs = c.createStatement().executeQuery("SELECT 1")) {
                assertTrue(rs.next());
            }
        }
    }

    // --- Lock timeout ---

    @Test void lock_timeout_setting() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET lock_timeout = '500ms'");
            try (ResultSet rs = c.createStatement().executeQuery("SHOW lock_timeout")) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
        }
    }

    @Test void lock_timeout_with_contention() throws Exception {
        try (Connection c1 = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
             Connection c2 = DriverManager.getConnection(
                     memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword())) {
            c1.setAutoCommit(false);
            c2.setAutoCommit(false);
            c1.createStatement().execute("CREATE TABLE to_lock(id int PRIMARY KEY)");
            c1.createStatement().execute("INSERT INTO to_lock VALUES (1)");
            c1.commit();
            c2.commit();
            // Session 1 locks the row
            c1.createStatement().executeQuery("SELECT * FROM to_lock WHERE id = 1 FOR UPDATE");
            // Session 2 has short lock timeout
            c2.createStatement().execute("SET lock_timeout = '100ms'");
            long start = System.currentTimeMillis();
            try {
                c2.createStatement().executeQuery("SELECT * FROM to_lock WHERE id = 1 FOR UPDATE");
                fail("Should have timed out");
            } catch (SQLException e) {
                long elapsed = System.currentTimeMillis() - start;
                // Should be 55P03 (lock_not_available)
                assertTrue(e.getSQLState().equals("55P03") || e.getSQLState().equals("57014"),
                        "Expected lock timeout error, got: " + e.getSQLState());
                assertTrue(elapsed < 2000,
                        "lock_timeout = 100ms should fail fast, but took " + elapsed + "ms");
            }
            c1.commit();
            c2.rollback();
            c1.createStatement().execute("DROP TABLE to_lock");
            c1.commit();
        }
    }

    // --- Idle-in-transaction timeout ---

    @Test void idle_in_transaction_timeout_setting() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SET idle_in_transaction_session_timeout = '10s'");
            try (ResultSet rs = c.createStatement().executeQuery("SHOW idle_in_transaction_session_timeout")) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
        }
    }

    // --- JDBC statement timeout ---

    @Test void jdbc_query_timeout() throws Exception {
        try (Connection c = newConn()) {
            try (Statement s = c.createStatement()) {
                s.setQueryTimeout(5); // 5 seconds
                try (ResultSet rs = s.executeQuery("SELECT 1")) {
                    assertTrue(rs.next());
                }
            }
        }
    }

    // --- Connection validity check ---

    @Test void connection_isValid_after_operations() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("SELECT 1");
            assertTrue(c.isValid(5), "Connection should be valid");
        }
    }

    // --- Cancel from JDBC ---

    @Test void jdbc_cancel_statement() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            // Cancel on a statement that's not running should be a no-op
            s.cancel();
            // Statement should still work after cancel
            try (ResultSet rs = s.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
            }
            s.close();
        }
    }
}
