package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Connection-pool retry and failover client scenarios from:
 * 1160_pooling_retries_and_failover_client_scenarios.md
 *
 * Covers: connection isolation (transactions, search_path, session variables,
 * temp tables), connection reuse after commit/rollback/error, concurrent
 * connections, autocommit toggling, and serialization-failure retry patterns.
 *
 * Table prefix: pool_
 */
class PoolingRetriesTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (memgres != null) memgres.close();
    }

    Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(false);
        return c;
    }

    // =========================================================================
    // 1. Connection pool isolation: two connections must not share tx state
    // =========================================================================

    @Test
    void testConnectionPoolTransactionIsolation() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            try (Statement s = c1.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS pool_tx_isolation (id int PRIMARY KEY, val text)");
                c1.commit();
            }

            // Insert on c1 but do NOT commit
            try (Statement s = c1.createStatement()) {
                s.execute("INSERT INTO pool_tx_isolation VALUES (1, 'from-c1')");
            }

            // c2 should not see the uncommitted row
            try (Statement s = c2.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*) FROM pool_tx_isolation WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getLong(1), "c2 must not see uncommitted row from c1");
            }

            c1.rollback();
            c2.rollback();
        }
    }

    // =========================================================================
    // 2. Prepared statement across connections: same SQL works independently
    // =========================================================================

    @Test
    void testPreparedStatementAcrossConnections() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            try (Statement s = c1.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS pool_ps_shared (id int PRIMARY KEY, val text)");
                s.execute("INSERT INTO pool_ps_shared VALUES (10, 'hello')");
                c1.commit();
            }

            final String sql = "SELECT val FROM pool_ps_shared WHERE id = ?";

            try (PreparedStatement ps1 = c1.prepareStatement(sql);
                 PreparedStatement ps2 = c2.prepareStatement(sql)) {
                ps1.setInt(1, 10);
                ps2.setInt(1, 10);

                try (ResultSet rs = ps1.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("hello", rs.getString(1));
                }
                try (ResultSet rs = ps2.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("hello", rs.getString(1));
                }
            }

            c1.rollback();
            c2.rollback();
        }
    }

    // =========================================================================
    // 3. Search path isolation: SET search_path on c1 doesn't affect c2
    // =========================================================================

    @Test
    void testSearchPathIsolation() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            // Create a separate schema on c1 and set search_path
            try (Statement s = c1.createStatement()) {
                s.execute("CREATE SCHEMA IF NOT EXISTS pool_schema_a");
                c1.commit();
                s.execute("SET search_path = pool_schema_a, public");
            }

            // c2 retains the default search_path
            try (Statement s = c2.createStatement();
                 ResultSet rs = s.executeQuery("SHOW search_path")) {
                assertTrue(rs.next());
                String sp = rs.getString(1);
                assertFalse(sp.contains("pool_schema_a"),
                        "c2 search_path must not contain pool_schema_a, got: " + sp);
            }

            c1.rollback();
            c2.rollback();
        }
    }

    // =========================================================================
    // 4. Session variable isolation: SET on c1 doesn't leak to c2
    // =========================================================================

    @Test
    void testSessionVariableIsolation() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            try (Statement s = c1.createStatement()) {
                s.execute("SET application_name = 'pool_test_c1'");
            }

            try (Statement s = c2.createStatement();
                 ResultSet rs = s.executeQuery("SHOW application_name")) {
                assertTrue(rs.next());
                assertNotEquals("pool_test_c1", rs.getString(1),
                        "Session variable set on c1 must not be visible on c2");
            }

            c1.rollback();
            c2.rollback();
        }
    }

    // =========================================================================
    // 5. Temporary table isolation: TEMP TABLE on c1 invisible to c2
    // =========================================================================

    @Test
    void testTemporaryTableIsolation() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            try (Statement s = c1.createStatement()) {
                s.execute("CREATE TEMP TABLE pool_temp_private (id int)");
                s.execute("INSERT INTO pool_temp_private VALUES (99)");
                c1.commit();
            }

            // c2 must not be able to see pool_temp_private
            try (Statement s = c2.createStatement()) {
                assertThrows(SQLException.class,
                        () -> s.executeQuery("SELECT * FROM pool_temp_private"),
                        "Temp table created on c1 must not be visible from c2");
            }

            c1.rollback();
            c2.rollback();
        }
    }

    // =========================================================================
    // 6. Connection reuse after COMMIT: connection is fully reusable
    // =========================================================================

    @Test
    void testConnectionReuseAfterCommit() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS pool_reuse_commit (id int PRIMARY KEY, val text)");
            c.commit();

            s.execute("INSERT INTO pool_reuse_commit VALUES (1, 'first')");
            c.commit();

            // Reuse: another insert after commit
            s.execute("INSERT INTO pool_reuse_commit VALUES (2, 'second')");
            c.commit();

            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM pool_reuse_commit")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getLong(1));
            }
            c.commit();
        }
    }

    // =========================================================================
    // 7. Connection reuse after ROLLBACK: connection is fully reusable
    // =========================================================================

    @Test
    void testConnectionReuseAfterRollback() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS pool_reuse_rollback (id int PRIMARY KEY, val text)");
            c.commit();

            s.execute("INSERT INTO pool_reuse_rollback VALUES (1, 'gone')");
            c.rollback();

            // Reuse after rollback
            s.execute("INSERT INTO pool_reuse_rollback VALUES (2, 'kept')");
            c.commit();

            try (ResultSet rs = s.executeQuery("SELECT val FROM pool_reuse_rollback WHERE id = 2")) {
                assertTrue(rs.next());
                assertEquals("kept", rs.getString(1));
            }
            c.commit();
        }
    }

    // =========================================================================
    // 8. Connection reuse after error + rollback
    // =========================================================================

    @Test
    void testConnectionReuseAfterError() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS pool_reuse_error (id int PRIMARY KEY, val text)");
            s.execute("INSERT INTO pool_reuse_error VALUES (1, 'existing')");
            c.commit();

            // Force a duplicate-key error
            assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO pool_reuse_error VALUES (1, 'duplicate')"));
            c.rollback();

            // Connection must still be usable
            s.execute("INSERT INTO pool_reuse_error VALUES (2, 'new')");
            c.commit();

            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM pool_reuse_error")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getLong(1));
            }
            c.commit();
        }
    }

    // =========================================================================
    // 9. Concurrent connections: 10 threads all execute queries successfully
    // =========================================================================

    @Test
    void testConcurrentConnections() throws Exception {
        // Create shared table on a setup connection
        try (Connection setup = newConn(); Statement s = setup.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS pool_concurrent (id int PRIMARY KEY, val text)");
            for (int i = 1; i <= 10; i++) {
                s.execute("INSERT INTO pool_concurrent VALUES (" + i + ", 'row" + i + "') ON CONFLICT DO NOTHING");
            }
            setup.commit();
        }

        int threads = 10;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        List<Future<Integer>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int tid = t + 1;
            futures.add(exec.submit(() -> {
                try (Connection c = newConn();
                     PreparedStatement ps = c.prepareStatement(
                             "SELECT val FROM pool_concurrent WHERE id = ?")) {
                    ps.setInt(1, tid);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next(), "Thread " + tid + " should find its row");
                        assertEquals("row" + tid, rs.getString(1));
                    }
                    c.commit();
                    return tid;
                }
            }));
        }

        exec.shutdown();
        exec.awaitTermination(30, TimeUnit.SECONDS);

        for (Future<Integer> f : futures) {
            assertDoesNotThrow(() -> assertEquals(futures.indexOf(f) + 1, f.get()),
                    "Concurrent query should succeed without exception");
        }
    }

    // =========================================================================
    // 10. Close and reopen: verify clean state on fresh connection
    // =========================================================================

    @Test
    void testConnectionCloseAndReopen() throws Exception {
        // Establish a connection, set a session variable, then close it
        String jdbcUrl = memgres.getJdbcUrl() + "?preferQueryMode=simple";
        String user = memgres.getUser();
        String pass = memgres.getPassword();

        try (Connection c = DriverManager.getConnection(jdbcUrl, user, pass)) {
            c.setAutoCommit(false);
            c.createStatement().execute("SET application_name = 'pool_dirty_session'");
            c.commit();
        }

        // A new connection should NOT inherit the previous session state
        try (Connection fresh = DriverManager.getConnection(jdbcUrl, user, pass)) {
            fresh.setAutoCommit(false);
            try (ResultSet rs = fresh.createStatement().executeQuery("SHOW application_name")) {
                assertTrue(rs.next());
                assertNotEquals("pool_dirty_session", rs.getString(1),
                        "New connection must start with clean session state");
            }
            fresh.rollback();
        }
    }

    // =========================================================================
    // 11. Transaction state after close: uncommitted work is rolled back
    // =========================================================================

    @Test
    void testUncommittedWorkRolledBackOnClose() throws Exception {
        try (Connection c1 = newConn(); Statement s = c1.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS pool_close_rollback (id int PRIMARY KEY, val text)");
            c1.commit();
        }

        // Open a connection, insert without committing, then close
        try (Connection c = newConn()) {
            c.createStatement().execute("INSERT INTO pool_close_rollback VALUES (1, 'uncommitted')");
            // Close without commit (implicit rollback)
        }

        // Verify the row was NOT persisted
        try (Connection verify = newConn();
             Statement s = verify.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM pool_close_rollback WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1), "Uncommitted insert should have been rolled back on close");
            verify.rollback();
        }
    }

    // =========================================================================
    // 12. Autocommit toggle: switching autocommit on/off within same connection
    // =========================================================================

    @Test
    void testAutocommitToggle() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS pool_autocommit (id int PRIMARY KEY, val text)");
            c.commit();

            // Insert with autocommit OFF, then rollback
            s.execute("INSERT INTO pool_autocommit VALUES (1, 'will-rollback')");
            c.rollback();

            // Switch to autocommit ON; insert is committed immediately
            c.setAutoCommit(true);
            s.execute("INSERT INTO pool_autocommit VALUES (2, 'auto-committed')");

            // Switch back to autocommit OFF and verify
            c.setAutoCommit(false);
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM pool_autocommit")) {
                assertTrue(rs.next());
                // Only row 2 should exist (row 1 was rolled back)
                assertEquals(1, rs.getLong(1),
                        "Only the autocommit-ON insert should persist");
            }
            c.commit();
        }
    }

    // =========================================================================
    // 13. Serialization failure retry: catch SQLSTATE 40001 and retry
    // =========================================================================

    @Test
    void testSerializationFailureRetryPattern() throws Exception {
        try (Connection setup = newConn(); Statement s = setup.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS pool_serial_retry (id int PRIMARY KEY, counter int DEFAULT 0)");
            s.execute("INSERT INTO pool_serial_retry VALUES (1, 0) ON CONFLICT DO NOTHING");
            setup.commit();
        }

        // Simulate a transaction that may fail with 40001 and must be retried.
        // Here we simply verify that SERIALIZABLE transactions can complete
        // successfully and that the retry logic pattern is correct.
        final int MAX_RETRIES = 5;
        boolean committed = false;

        for (int attempt = 0; attempt < MAX_RETRIES && !committed; attempt++) {
            try (Connection c = newConn()) {
                c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                try (Statement s = c.createStatement()) {
                    // Read-modify-write: classic serialization conflict pattern
                    ResultSet rs = s.executeQuery("SELECT counter FROM pool_serial_retry WHERE id = 1");
                    assertTrue(rs.next());
                    int current = rs.getInt(1);
                    s.execute("UPDATE pool_serial_retry SET counter = " + (current + 1) + " WHERE id = 1");
                    c.commit();
                    committed = true;
                } catch (SQLException ex) {
                    c.rollback();
                    if (!"40001".equals(ex.getSQLState())) {
                        throw ex; // Re-throw non-serialization errors
                    }
                    // 40001 → retry the whole transaction
                }
            }
        }

        assertTrue(committed, "Transaction should have committed within " + MAX_RETRIES + " retries");

        // Verify the counter was incremented
        try (Connection verify = newConn();
             Statement s = verify.createStatement();
             ResultSet rs = s.executeQuery("SELECT counter FROM pool_serial_retry WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1), "Counter should have been incremented to 1");
            verify.rollback();
        }
    }
}
