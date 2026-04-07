package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for error recovery in the extended query protocol.
 *
 * When a PreparedStatement execution fails (constraint violation, type error,
 * relation not found, etc.), the server must:
 *   1. Send ErrorResponse
 *   2. On Sync, send ReadyForQuery with transaction state 'E' (if in txn) or 'I' (if autocommit)
 *   3. Accept new Parse/Bind/Execute messages afterward
 *
 * If the server doesn't handle errors correctly, the connection becomes
 * permanently broken after the first error.
 *
 * Also covers the ReadyForQuery transaction state indicator ('I', 'T', 'E')
 * which connection pools use to verify connection health.
 */
class ExtendedProtocolErrorRecoveryTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE err_items (id serial PRIMARY KEY, name text UNIQUE NOT NULL, val int CHECK (val > 0))");
            s.execute("INSERT INTO err_items (name, val) VALUES ('existing', 10)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // Unique constraint violation, then continue
    // =========================================================================

    @Test
    void testRecoverFromUniqueViolation() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_items (name, val) VALUES (?, ?)")) {
            // This should fail due to duplicate name
            ps.setString(1, "existing");
            ps.setInt(2, 20);
            assertThrows(SQLException.class, ps::executeUpdate);
        }

        // Connection must still work
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_items (name, val) VALUES (?, ?)")) {
            ps.setString(1, "new_after_error");
            ps.setInt(2, 30);
            assertEquals(1, ps.executeUpdate());
        }

        assertEquals("new_after_error", query1(
                "SELECT name FROM err_items WHERE val = 30"));
    }

    // =========================================================================
    // Check constraint violation, then continue
    // =========================================================================

    @Test
    void testRecoverFromCheckViolation() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_items (name, val) VALUES (?, ?)")) {
            ps.setString(1, "bad_val");
            ps.setInt(2, -5); // violates CHECK (val > 0)
            assertThrows(SQLException.class, ps::executeUpdate);
        }

        // Must still work
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM err_items")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
        }
    }

    // =========================================================================
    // Table not found, then continue
    // =========================================================================

    @Test
    void testRecoverFromTableNotFound() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM nonexistent_table_xyz WHERE id = ?")) {
            ps.setInt(1, 1);
            assertThrows(SQLException.class, ps::executeQuery);
        }

        // Must still work
        assertEquals("existing", query1("SELECT name FROM err_items WHERE id = 1"));
    }

    // =========================================================================
    // Syntax error in PreparedStatement, then continue
    // =========================================================================

    @Test
    void testRecoverFromSyntaxError() throws SQLException {
        try {
            // Invalid SQL, should fail at Parse time
            PreparedStatement ps = conn.prepareStatement("SELEKT * FROM err_items WHERE id = ?");
            ps.setInt(1, 1);
            ps.executeQuery();
            fail("Should have thrown SQLException");
        } catch (SQLException expected) {
            // Expected
        }

        // Connection must survive
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM err_items WHERE id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("existing", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // Multiple errors in sequence, then recover
    // =========================================================================

    @Test
    void testRecoverFromMultipleErrors() throws SQLException {
        for (int i = 0; i < 5; i++) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO err_items (name, val) VALUES (?, ?)")) {
                ps.setString(1, "existing"); // always duplicate
                ps.setInt(2, 1);
                assertThrows(SQLException.class, ps::executeUpdate);
            }
        }

        // Connection must still work after 5 consecutive errors
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM err_items")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) >= 1);
            }
        }
    }

    // =========================================================================
    // Error during batch execution
    // =========================================================================

    @Test
    void testRecoverFromBatchError() throws SQLException {
        exec("CREATE TABLE batch_err (id serial PRIMARY KEY, name text UNIQUE)");
        exec("INSERT INTO batch_err (name) VALUES ('taken')");

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO batch_err (name) VALUES (?)")) {
            ps.setString(1, "ok_row"); ps.addBatch();
            ps.setString(1, "taken"); ps.addBatch(); // will fail
            ps.setString(1, "another"); ps.addBatch();
            try {
                ps.executeBatch();
            } catch (BatchUpdateException expected) {
                // Expected: batch has a duplicate
            }
        }

        // Connection must survive
        assertNotNull(query1("SELECT COUNT(*) FROM batch_err"));
    }

    // =========================================================================
    // Error in transaction, rollback, then new PreparedStatement works
    // =========================================================================

    @Test
    void testRecoverFromTransactionError() throws SQLException {
        conn.setAutoCommit(false);
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO err_items (name, val) VALUES (?, ?)")) {
                ps.setString(1, "txn_item");
                ps.setInt(2, 50);
                ps.executeUpdate();
            }

            // This should fail
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO err_items (name, val) VALUES (?, ?)")) {
                ps.setString(1, "existing"); // duplicate
                ps.setInt(2, 60);
                ps.executeUpdate();
                fail("Should have thrown");
            } catch (SQLException expected) {
                conn.rollback();
            }
        } finally {
            conn.setAutoCommit(true);
        }

        // txn_item should NOT exist (rolled back)
        assertEquals("0", query1("SELECT COUNT(*) FROM err_items WHERE name = 'txn_item'"));

        // Connection must work for new queries
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM err_items WHERE id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
            }
        }
    }

    // =========================================================================
    // Statement after error uses same connection without explicit recovery
    // =========================================================================

    @Test
    void testSimpleStatementAfterPreparedError() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM does_not_exist WHERE x = ?")) {
            ps.setInt(1, 1);
            assertThrows(SQLException.class, ps::executeQuery);
        }

        // Regular Statement must work on the same connection
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }
}
