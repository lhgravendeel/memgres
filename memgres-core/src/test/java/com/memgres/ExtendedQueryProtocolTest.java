package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 11: Extended Query Protocol tests.
 * Tests that the extended query protocol (Parse/Bind/Describe/Execute/Sync) works
 * WITHOUT . Covers PreparedStatement, parameterized queries,
 * portals, and named/unnamed statements.
 */
class ExtendedQueryProtocolTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        // No preferQueryMode=simple, so it uses extended query protocol by default
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ext_test (id SERIAL PRIMARY KEY, name TEXT, value INTEGER)");
            stmt.execute("INSERT INTO ext_test (name, value) VALUES ('alpha', 10)");
            stmt.execute("INSERT INTO ext_test (name, value) VALUES ('beta', 20)");
            stmt.execute("INSERT INTO ext_test (name, value) VALUES ('gamma', 30)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    void testSimpleSelectWithStatement() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 AS num")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("num"));
        }
    }

    @Test
    void testPreparedSelectWithIntParam() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM ext_test WHERE value > ? ORDER BY value")) {
            ps.setInt(1, 15);
            try (ResultSet rs = ps.executeQuery()) {
                int count = 0;
                while (rs.next()) {
                    count++;
                    String name = rs.getString("name");
                    assertTrue(name.equals("beta") || name.equals("gamma") || name.equals("delta"),
                            "Unexpected name: " + name);
                }
                assertTrue(count >= 2, "Expected at least 2 rows but got " + count);
            }
        }
    }

    @Test
    void testPreparedSelectWithStringParam() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM ext_test WHERE name = ?")) {
            ps.setString(1, "alpha");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt("value"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void testPreparedInsert() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ext_test (name, value) VALUES (?, ?)")) {
            ps.setString(1, "delta");
            ps.setInt(2, 40);
            int affected = ps.executeUpdate();
            assertEquals(1, affected);
        }
        // Verify insert
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT value FROM ext_test WHERE name = 'delta'")) {
            assertTrue(rs.next());
            assertEquals(40, rs.getInt("value"));
        }
    }

    @Test
    void testPreparedUpdate() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("UPDATE ext_test SET value = ? WHERE name = ?")) {
            ps.setInt(1, 99);
            ps.setString(2, "alpha");
            int affected = ps.executeUpdate();
            assertEquals(1, affected);
        }
        // Verify update
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT value FROM ext_test WHERE name = 'alpha'")) {
            assertTrue(rs.next());
            assertEquals(99, rs.getInt("value"));
        }
        // Restore
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE ext_test SET value = 10 WHERE name = 'alpha'");
        }
    }

    @Test
    void testPreparedDelete() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO ext_test (name, value) VALUES ('toremove', 999)");
        }
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM ext_test WHERE name = ?")) {
            ps.setString(1, "toremove");
            int affected = ps.executeUpdate();
            assertEquals(1, affected);
        }
    }

    @Test
    void testPreparedStatementReuse() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM ext_test WHERE value = ?")) {
            // First execution
            ps.setInt(1, 10);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("alpha", rs.getString("name"));
            }
            // Reuse with different parameter
            ps.setInt(1, 20);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("beta", rs.getString("name"));
            }
        }
    }

    @Test
    void testMultipleParams() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM ext_test WHERE value >= ? AND value <= ? ORDER BY value")) {
            ps.setInt(1, 10);
            ps.setInt(2, 20);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("alpha", rs.getString("name"));
                assertTrue(rs.next());
                assertEquals("beta", rs.getString("name"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void testNullParam() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT ? IS NULL AS result")) {
            ps.setNull(1, Types.VARCHAR);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("t", rs.getString("result"));
            }
        }
    }

    @Test
    void testPreparedSelectLiteral() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT 42 AS answer")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt("answer"));
            }
        }
    }

    @Test
    void testPreparedCreateAndDrop() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("CREATE TABLE ext_temp (id INTEGER)")) {
            ps.execute();
        }
        try (PreparedStatement ps = conn.prepareStatement("DROP TABLE ext_temp")) {
            ps.execute();
        }
    }

    @Test
    void testConnectionWithoutSimpleMode() throws SQLException {
        // Create a fresh connection without preferQueryMode=simple
        try (Connection freshConn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test")) {
            try (Statement stmt = freshConn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 'hello' AS greeting")) {
                assertTrue(rs.next());
                assertEquals("hello", rs.getString("greeting"));
            }
        }
    }

    @Test
    void testPreparedWithExpression() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT ? + 1 AS result")) {
            ps.setInt(1, 41);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("42", rs.getString("result"));
            }
        }
    }
}
