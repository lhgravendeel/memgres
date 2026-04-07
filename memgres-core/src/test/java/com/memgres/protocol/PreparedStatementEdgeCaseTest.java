package com.memgres.protocol;

import com.memgres.core.Memgres;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for less common but real-world PreparedStatement patterns that
 * exercise edge cases in the extended query protocol.
 *
 * Covers:
 *   - setObject() with explicit SQL type (ORM pattern)
 *   - setNull() for various types
 *   - DDL via PreparedStatement
 *   - getMetaData() before execution (framework introspection)
 *   - Close message / statement deallocation
 *   - Column aliases in RowDescription
 *   - Zero-row executeUpdate result
 *   - LIKE patterns with % in parameters
 *   - Very large parameter values
 *   - Empty/zero-length string parameters
 */
class PreparedStatementEdgeCaseTest {

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
            s.execute("CREATE TABLE edge_items (id serial PRIMARY KEY, name text NOT NULL, category text, price numeric(10,2), active boolean DEFAULT true, metadata jsonb DEFAULT '{}')");
            s.execute("INSERT INTO edge_items (name, category, price) VALUES ('Alpha Widget', 'hardware', 9.99)");
            s.execute("INSERT INTO edge_items (name, category, price) VALUES ('Beta Service', 'services', 49.99)");
            s.execute("INSERT INTO edge_items (name, category, price) VALUES ('Gamma Tool', 'hardware', 24.50)");
            s.execute("INSERT INTO edge_items (name, category, price, active) VALUES ('Deleted Item', 'obsolete', 0, false)");
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
    // 1. setObject() with explicit SQL type
    // =========================================================================

    @Test
    void testSetObjectWithVarcharType() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM edge_items WHERE category = ?")) {
            ps.setObject(1, "hardware", Types.VARCHAR);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
        }
    }

    @Test
    void testSetObjectWithIntegerType() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM edge_items WHERE id = ?")) {
            ps.setObject(1, 1, Types.INTEGER);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("Alpha Widget", rs.getString(1));
            }
        }
    }

    @Test
    void testSetObjectWithBooleanType() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM edge_items WHERE active = ?")) {
            ps.setObject(1, true, Types.BOOLEAN);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    @Test
    void testSetObjectWithNumericType() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM edge_items WHERE price > ?")) {
            ps.setObject(1, new BigDecimal("20.00"), Types.NUMERIC);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void testSetObjectWithoutExplicitType() throws SQLException {
        // Untyped setObject; driver infers from Java type
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM edge_items WHERE category = ?")) {
            ps.setObject(1, "hardware");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void testSetObjectNullWithType() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM edge_items WHERE category = ?")) {
            ps.setObject(1, null, Types.VARCHAR);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1)); // NULL = NULL is false in SQL
            }
        }
    }

    // =========================================================================
    // 2. setNull() for various types
    // =========================================================================

    @Test
    void testSetNullVarchar() throws SQLException {
        exec("CREATE TABLE null_types (id serial PRIMARY KEY, txt text, num int, flag boolean, ts timestamp, amt numeric)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO null_types (txt, num, flag, ts, amt) VALUES (?, ?, ?, ?, ?)")) {
            ps.setNull(1, Types.VARCHAR);
            ps.setNull(2, Types.INTEGER);
            ps.setNull(3, Types.BOOLEAN);
            ps.setNull(4, Types.TIMESTAMP);
            ps.setNull(5, Types.NUMERIC);
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals("1", query1("SELECT COUNT(*) FROM null_types WHERE txt IS NULL AND num IS NULL AND flag IS NULL AND ts IS NULL AND amt IS NULL"));
    }

    @Test
    void testSetNullTypesOther() throws SQLException {
        exec("CREATE TABLE null_other (id serial PRIMARY KEY, data jsonb, tags text[])");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO null_other (data, tags) VALUES (?, ?)")) {
            ps.setNull(1, Types.OTHER);
            ps.setNull(2, Types.OTHER);
            ps.executeUpdate();
        }
        assertEquals("1", query1("SELECT COUNT(*) FROM null_other WHERE data IS NULL"));
    }

    // =========================================================================
    // 3. DDL via PreparedStatement
    // =========================================================================

    @Test
    void testCreateTableViaPrepared() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "CREATE TABLE ps_ddl_create (id serial PRIMARY KEY, val text)")) {
            assertFalse(ps.execute()); // DDL returns false (no result set)
        }
        exec("INSERT INTO ps_ddl_create (val) VALUES ('works')");
        assertEquals("works", query1("SELECT val FROM ps_ddl_create"));
    }

    @Test
    void testAlterTableViaPrepared() throws SQLException {
        exec("CREATE TABLE ps_ddl_alter (id serial PRIMARY KEY)");
        try (PreparedStatement ps = conn.prepareStatement(
                "ALTER TABLE ps_ddl_alter ADD COLUMN info text DEFAULT 'none'")) {
            ps.execute();
        }
        exec("INSERT INTO ps_ddl_alter DEFAULT VALUES");
        assertEquals("none", query1("SELECT info FROM ps_ddl_alter"));
    }

    @Test
    void testCreateIndexViaPrepared() throws SQLException {
        exec("CREATE TABLE ps_ddl_idx (id serial PRIMARY KEY, code text)");
        try (PreparedStatement ps = conn.prepareStatement(
                "CREATE INDEX idx_ps_code ON ps_ddl_idx (code)")) {
            ps.execute();
        }
    }

    @Test
    void testDropTableViaPrepared() throws SQLException {
        exec("CREATE TABLE ps_ddl_drop (id serial PRIMARY KEY)");
        try (PreparedStatement ps = conn.prepareStatement("DROP TABLE ps_ddl_drop")) {
            ps.execute();
        }
    }

    // =========================================================================
    // 7. getMetaData() before execution
    // =========================================================================

    @Test
    void testGetMetaDataBeforeExecute() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, name, price FROM edge_items WHERE category = ?")) {
            // Get metadata WITHOUT executing first, which triggers Describe(Statement)
            ResultSetMetaData md = ps.getMetaData();
            assertNotNull(md);
            assertEquals(3, md.getColumnCount());
            assertEquals("id", md.getColumnName(1));
            assertEquals("name", md.getColumnName(2));
            assertEquals("price", md.getColumnName(3));
        }
    }

    @Test
    void testGetMetaDataBeforeExecuteAllColumns() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM edge_items WHERE id = ?")) {
            ResultSetMetaData md = ps.getMetaData();
            assertNotNull(md);
            assertTrue(md.getColumnCount() >= 5);
        }
    }

    // =========================================================================
    // 8. Close message / statement deallocation
    // =========================================================================

    @Test
    void testCloseAndReopenPreparedStatement() throws SQLException {
        // First PreparedStatement
        PreparedStatement ps1 = conn.prepareStatement("SELECT name FROM edge_items WHERE id = ?");
        ps1.setInt(1, 1);
        ResultSet rs1 = ps1.executeQuery();
        assertTrue(rs1.next());
        rs1.close();
        ps1.close(); // sends Close message to server

        // Second PreparedStatement with same SQL; should get a new server-side statement
        PreparedStatement ps2 = conn.prepareStatement("SELECT name FROM edge_items WHERE id = ?");
        ps2.setInt(1, 2);
        ResultSet rs2 = ps2.executeQuery();
        assertTrue(rs2.next());
        assertEquals("Beta Service", rs2.getString(1));
        rs2.close();
        ps2.close();
    }

    @Test
    void testCloseManyPreparedStatements() throws SQLException {
        // Open and close many PreparedStatements to test handle cleanup
        for (int i = 0; i < 50; i++) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT ? + 1")) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(i + 1, rs.getInt(1));
                }
            } // close sends Close message
        }
        // Connection should still work
        assertEquals("1", query1("SELECT 1"));
    }

    // =========================================================================
    // 14. Column aliases in RowDescription
    // =========================================================================

    @Test
    void testColumnAliases() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id AS \"itemId\", name AS \"itemName\", price AS \"unitPrice\" FROM edge_items WHERE category = ?")) {
            ps.setString(1, "hardware");
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals("itemId", md.getColumnName(1));
                assertEquals("itemName", md.getColumnName(2));
                assertEquals("unitPrice", md.getColumnName(3));
                assertTrue(rs.next());
                assertNotNull(rs.getString("itemName"));
            }
        }
    }

    @Test
    void testExpressionAliases() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) AS total, SUM(price) AS revenue FROM edge_items WHERE active = ?")) {
            ps.setBoolean(1, true);
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals("total", md.getColumnName(1));
                assertEquals("revenue", md.getColumnName(2));
                assertTrue(rs.next());
            }
        }
    }

    // =========================================================================
    // 15. Zero-row executeUpdate result
    // =========================================================================

    @Test
    void testUpdateZeroRows() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE edge_items SET price = price + 1 WHERE id = ?")) {
            ps.setInt(1, 99999); // nonexistent
            assertEquals(0, ps.executeUpdate());
        }
    }

    @Test
    void testDeleteZeroRows() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM edge_items WHERE name = ?")) {
            ps.setString(1, "nonexistent_item_xyz");
            assertEquals(0, ps.executeUpdate());
        }
    }

    @Test
    void testInsertOnConflictDoNothingZeroRows() throws SQLException {
        exec("CREATE TABLE upsert_zero (id int PRIMARY KEY, val text)");
        exec("INSERT INTO upsert_zero VALUES (1, 'existing')");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO upsert_zero (id, val) VALUES (?, ?) ON CONFLICT DO NOTHING")) {
            ps.setInt(1, 1); // conflict
            ps.setString(2, "ignored");
            // ON CONFLICT DO NOTHING returns 0 rows affected
            int affected = ps.executeUpdate();
            assertEquals(0, affected);
        }
        assertEquals("existing", query1("SELECT val FROM upsert_zero WHERE id = 1"));
    }

    // =========================================================================
    // 12. LIKE patterns with % in parameters
    // =========================================================================

    @Test
    void testLikeWithPercentParam() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM edge_items WHERE name LIKE ?")) {
            ps.setString(1, "%Widget%");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    void testLikeWithPrefixPattern() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM edge_items WHERE name LIKE ?")) {
            ps.setString(1, "Beta%");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("Beta Service", rs.getString(1));
            }
        }
    }

    @Test
    void testIlikeWithParam() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM edge_items WHERE name ILIKE ?")) {
            ps.setString(1, "%WIDGET%");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    // =========================================================================
    // 16. Large parameter values
    // =========================================================================

    @Test
    void testLargeTextParameter() throws SQLException {
        exec("CREATE TABLE large_param (id serial PRIMARY KEY, content text)");
        String largeText = Strs.repeat("x", 100_000); // 100KB
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO large_param (content) VALUES (?)")) {
            ps.setString(1, largeText);
            ps.executeUpdate();
        }
        assertEquals("100000", query1("SELECT LENGTH(content) FROM large_param WHERE id = 1"));
    }

    @Test
    void testLargeByteaParameter() throws SQLException {
        exec("CREATE TABLE large_bytea (id serial PRIMARY KEY, data bytea)");
        byte[] largeBytes = new byte[50_000]; // 50KB
        for (int i = 0; i < largeBytes.length; i++) largeBytes[i] = (byte) (i % 256);
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO large_bytea (data) VALUES (?)")) {
            ps.setBytes(1, largeBytes);
            ps.executeUpdate();
        }
        assertEquals("50000", query1("SELECT octet_length(data) FROM large_bytea WHERE id = 1"));
    }

    // =========================================================================
    // Empty/zero-length string parameters
    // =========================================================================

    @Test
    void testEmptyStringParameter() throws SQLException {
        exec("CREATE TABLE empty_str (id serial PRIMARY KEY, val text)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO empty_str (val) VALUES (?)")) {
            ps.setString(1, "");
            ps.executeUpdate();
        }
        // Empty string is NOT NULL in PG
        assertEquals("1", query1("SELECT COUNT(*) FROM empty_str WHERE val = ''"));
        assertEquals("0", query1("SELECT COUNT(*) FROM empty_str WHERE val IS NULL"));
    }

    @Test
    void testSelectWithEmptyStringParameter() throws SQLException {
        exec("CREATE TABLE empty_str2 (id serial PRIMARY KEY, code text DEFAULT '')");
        exec("INSERT INTO empty_str2 DEFAULT VALUES");
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM empty_str2 WHERE code = ?")) {
            ps.setString(1, "");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }
}
