package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for connection state recovery after errors, especially in
 * transaction and connection-pool scenarios.
 *
 * The PG protocol has two independent message flows:
 *   - Simple query: QUERY message → always independent, always ends with ReadyForQuery
 *   - Extended protocol: Parse/Bind/Describe/Execute/Sync → error skips until Sync
 *
 * These must NOT interfere with each other. A simple QUERY (like ROLLBACK or
 * a pool health check) must always work, even if the extended protocol is in
 * error state.
 *
 * Connection pools often:
 *   1. Set autocommit=false at checkout
 *   2. Send BEGIN implicitly
 *   3. On error, send ROLLBACK to clean up
 *   4. Return connection to pool
 *   5. Next checkout gets the same connection
 *
 * If the ROLLBACK is blocked or the session stays in FAILED state,
 * all subsequent queries on that connection fail with:
 *   "current transaction is aborted, commands ignored until end of transaction block"
 */
class ConnectionStateAfterErrorTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (memgres != null) memgres.close();
    }

    private Connection freshConnection() throws SQLException {
        Connection c = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        return c;
    }

    // =========================================================================
    // 1. Explicit transaction: BEGIN → error → ROLLBACK → new query
    // =========================================================================

    @Test
    void testExplicitTransactionErrorThenRollback() throws SQLException {
        try (Connection conn = freshConnection()) {
            conn.setAutoCommit(false);
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS cse_t1 (id serial PRIMARY KEY, val TEXT NOT NULL UNIQUE)");
                s.execute("INSERT INTO cse_t1 (val) VALUES ('existing')");
            }
            conn.commit();

            // Start a new transaction, cause an error
            try (Statement s = conn.createStatement()) {
                assertThrows(SQLException.class, () ->
                        s.execute("INSERT INTO cse_t1 (val) VALUES ('existing')")); // duplicate
            }

            // Connection is now in FAILED transaction state; ROLLBACK must work
            conn.rollback();

            // After ROLLBACK, connection must be fully usable
            try (Statement s = conn.createStatement()) {
                s.execute("INSERT INTO cse_t1 (val) VALUES ('after_rollback')");
            }
            conn.commit();

            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT val FROM cse_t1 WHERE val = 'after_rollback'")) {
                assertTrue(rs.next(), "Row inserted after ROLLBACK must be visible");
            }
            conn.commit();
        }
    }

    // =========================================================================
    // 2. Autocommit error → next query must work immediately
    // =========================================================================

    @Test
    void testAutocommitErrorThenNextQuery() throws SQLException {
        try (Connection conn = freshConnection()) {
            conn.setAutoCommit(true);
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS cse_auto (id serial PRIMARY KEY, val TEXT NOT NULL UNIQUE)");
                s.execute("INSERT INTO cse_auto (val) VALUES ('first')");
            }

            // Error in autocommit mode
            assertThrows(SQLException.class, () -> {
                try (Statement s = conn.createStatement()) {
                    s.execute("INSERT INTO cse_auto (val) VALUES ('first')"); // duplicate
                }
            });

            // Next query must work immediately; no ROLLBACK needed in autocommit
            try (Statement s = conn.createStatement()) {
                s.execute("INSERT INTO cse_auto (val) VALUES ('second')");
                try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM cse_auto")) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                }
            }
        }
    }

    // =========================================================================
    // 3. PreparedStatement FK error in autocommit → next PS works
    // =========================================================================

    @Test
    void testPreparedFkErrorAutocommitRecovery() throws SQLException {
        try (Connection conn = freshConnection()) {
            conn.setAutoCommit(true);
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS cse_parent (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT)");
                s.execute("CREATE TABLE IF NOT EXISTS cse_child (id serial PRIMARY KEY, parent_id UUID NOT NULL REFERENCES cse_parent(id), info TEXT)");
                s.execute("INSERT INTO cse_parent (name) VALUES ('p1')");
            }

            // FK violation via PreparedStatement
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO cse_child (parent_id, info) VALUES (?, ?)")) {
                ps.setObject(1, UUID.randomUUID()); // nonexistent parent
                ps.setString(2, "orphan");
                assertThrows(SQLException.class, ps::executeUpdate);
            }

            // Next PreparedStatement must work
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT name FROM cse_parent WHERE name = ?")) {
                ps.setString(1, "p1");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "SELECT must work after FK violation in autocommit");
                    assertEquals("p1", rs.getString(1));
                }
            }
        }
    }

    // =========================================================================
    // 4. setAutoCommit(false) → error → rollback → setAutoCommit(true) → query
    //    This is the exact connection pool lifecycle
    // =========================================================================

    @Test
    void testPoolLifecycleErrorRecovery() throws SQLException {
        try (Connection conn = freshConnection()) {
            conn.setAutoCommit(true);
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS cse_pool (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), email TEXT NOT NULL UNIQUE, hash TEXT NOT NULL)");
                s.execute("INSERT INTO cse_pool (email, hash) VALUES ('taken@test.com', 'hash1')");
            }

            // Simulate pool checkout: set autocommit false
            conn.setAutoCommit(false);

            // Application does some work that fails
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO cse_pool (email, hash) VALUES (?, ?)")) {
                ps.setString(1, "taken@test.com"); // duplicate email
                ps.setString(2, "hash2");
                assertThrows(SQLException.class, ps::executeUpdate);
            }

            // Pool cleanup: rollback and reset autocommit
            conn.rollback();
            conn.setAutoCommit(true);

            // Simulate next pool checkout: connection must be fully clean
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO cse_pool (email, hash) VALUES (?, ?) RETURNING id")) {
                ps.setString(1, "new@test.com");
                ps.setString(2, "hash_new");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "INSERT must succeed on recycled connection");
                    assertNotNull(rs.getObject(1));
                }
            }
            conn.commit();

            // Verify
            conn.setAutoCommit(true);
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT hash FROM cse_pool WHERE email = 'new@test.com'")) {
                assertTrue(rs.next());
                assertEquals("hash_new", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // 5. Multiple errors in explicit transaction → single ROLLBACK recovers
    // =========================================================================

    @Test
    void testMultipleErrorsInTransactionThenRollback() throws SQLException {
        try (Connection conn = freshConnection()) {
            conn.setAutoCommit(true);
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS cse_multi (id serial PRIMARY KEY, val TEXT NOT NULL UNIQUE)");
            }

            conn.setAutoCommit(false);
            try (Statement s = conn.createStatement()) {
                s.execute("INSERT INTO cse_multi (val) VALUES ('a')");
            }
            // First error
            assertThrows(SQLException.class, () -> {
                try (Statement s = conn.createStatement()) {
                    s.execute("INSERT INTO cse_multi (val) VALUES ('a')"); // dup
                }
            });

            // In FAILED state, second query also fails
            assertThrows(SQLException.class, () -> {
                try (Statement s = conn.createStatement()) {
                    s.execute("INSERT INTO cse_multi (val) VALUES ('b')");
                }
            });

            // ROLLBACK fixes everything
            conn.rollback();

            // Now it works again
            try (Statement s = conn.createStatement()) {
                s.execute("INSERT INTO cse_multi (val) VALUES ('c')");
            }
            conn.commit();

            conn.setAutoCommit(true);
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT val FROM cse_multi WHERE val = 'c'")) {
                assertTrue(rs.next());
            }
        }
    }

    // =========================================================================
    // 6. Rapid error/success cycles mimicking on-demand connection usage
    // =========================================================================

    @Test
    void testRapidErrorSuccessCyclesWithTransactions() throws SQLException {
        try (Connection conn = freshConnection()) {
            conn.setAutoCommit(true);
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS cse_rapid_p (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT)");
                s.execute("CREATE TABLE IF NOT EXISTS cse_rapid_c (id serial PRIMARY KEY, pid UUID NOT NULL REFERENCES cse_rapid_p(id), data TEXT NOT NULL)");
            }

            for (int i = 0; i < 20; i++) {
                // Simulate pool checkout
                conn.setAutoCommit(false);

                try {
                    // Insert parent
                    UUID parentId;
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO cse_rapid_p (name) VALUES (?) RETURNING id")) {
                        ps.setString(1, "parent_" + i);
                        try (ResultSet rs = ps.executeQuery()) {
                            assertTrue(rs.next());
                            parentId = (UUID) rs.getObject(1);
                        }
                    }

                    if (i % 3 == 0) {
                        // Every 3rd iteration: cause FK violation
                        try (PreparedStatement ps = conn.prepareStatement(
                                "INSERT INTO cse_rapid_c (pid, data) VALUES (?, ?)")) {
                            ps.setObject(1, UUID.randomUUID()); // bad FK
                            ps.setString(2, "bad_" + i);
                            ps.executeUpdate();
                            fail("FK violation expected");
                        } catch (SQLException expected) {
                            conn.rollback();
                            conn.setAutoCommit(true);
                            continue; // skip to next iteration
                        }
                    }

                    // Valid child insert
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO cse_rapid_c (pid, data) VALUES (?, ?)")) {
                        ps.setObject(1, parentId);
                        ps.setString(2, "child_" + i);
                        ps.executeUpdate();
                    }
                    conn.commit();

                } catch (SQLException e) {
                    conn.rollback();
                } finally {
                    conn.setAutoCommit(true);
                }

                // Verify connection is clean for next cycle
                try (Statement s = conn.createStatement();
                     ResultSet rs = s.executeQuery("SELECT 1")) {
                    assertTrue(rs.next(), "Connection must be usable after cycle " + i);
                }
            }
        }
    }

    // =========================================================================
    // 7. Simple query ROLLBACK after extended protocol error
    // =========================================================================

    @Test
    void testSimpleQueryRollbackAfterExtendedProtocolError() throws SQLException {
        try (Connection conn = freshConnection()) {
            conn.setAutoCommit(true);
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS cse_sq (id serial PRIMARY KEY, val TEXT NOT NULL)");
            }

            conn.setAutoCommit(false);

            // Extended protocol: PreparedStatement error
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO cse_sq (val) VALUES (?)")) {
                ps.setNull(1, Types.VARCHAR); // NOT NULL violation
                assertThrows(SQLException.class, ps::executeUpdate);
            }

            // Simple query ROLLBACK (Connection.rollback() uses simple query protocol)
            conn.rollback();

            // Must work now
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO cse_sq (val) VALUES (?)")) {
                ps.setString(1, "recovered");
                ps.executeUpdate();
            }
            conn.commit();

            conn.setAutoCommit(true);
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT val FROM cse_sq WHERE val = 'recovered'")) {
                assertTrue(rs.next());
                assertEquals("recovered", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // 8. Connection reuse across multiple "sessions" (pool simulation)
    // =========================================================================

    @Test
    void testConnectionReuseAcrossPoolSessions() throws SQLException {
        try (Connection conn = freshConnection()) {
            conn.setAutoCommit(true);
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS cse_reuse (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), payload TEXT NOT NULL)");
            }

            for (int session = 0; session < 10; session++) {
                // Simulate pool checkout
                conn.setAutoCommit(false);

                boolean errorOccurred = false;
                try {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO cse_reuse (payload) VALUES (?) RETURNING id, payload")) {
                        if (session % 4 == 2) {
                            // Cause error: insert null into NOT NULL column
                            ps.setNull(1, Types.VARCHAR);
                            try {
                                ps.executeQuery();
                                fail("Should have thrown");
                            } catch (SQLException expected) {
                                errorOccurred = true;
                            }
                        } else {
                            ps.setString(1, "session_" + session);
                            try (ResultSet rs = ps.executeQuery()) {
                                assertTrue(rs.next(), "INSERT must succeed on session " + session);
                                assertNotNull(rs.getObject("id"));
                                assertEquals("session_" + session, rs.getString("payload"),
                                        "Payload must match on session " + session);
                            }
                        }
                    }

                    if (!errorOccurred) {
                        conn.commit();
                    }
                } finally {
                    if (errorOccurred) {
                        conn.rollback();
                    }
                    conn.setAutoCommit(true);
                }

                // Verify connection is clean
                try (Statement s = conn.createStatement();
                     ResultSet rs = s.executeQuery("SELECT 1")) {
                    assertTrue(rs.next(), "Connection must work after session " + session);
                    assertEquals(1, rs.getInt(1));
                }
            }
        }
    }
}
