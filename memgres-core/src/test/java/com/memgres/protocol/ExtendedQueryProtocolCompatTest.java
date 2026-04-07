package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the PostgreSQL extended query protocol (Parse/Bind/Describe/Execute).
 *
 * When the PG JDBC driver uses PreparedStatement with ? parameter placeholders,
 * it sends queries via the extended query protocol instead of the simple query
 * protocol. This involves:
 *   1. Parse:    send SQL with $1, $2 placeholders, get statement handle
 *   2. Bind:     send parameter values, get portal handle
 *   3. Describe: ask server for result column metadata
 *   4. Execute:  run the portal and get results
 *   5. Sync:     end the request cycle
 *
 * The server must correctly:
 *   - Parse the SQL with $N parameter placeholders
 *   - Accept parameter values of various types (text, int, bytea, etc.)
 *   - Return correct RowDescription for Describe
 *   - Return DataRow messages for Execute
 *   - Handle multiple executions of the same prepared statement
 *   - Handle prepared statements with no parameters
 *   - Handle prepared statements that return no rows
 *   - Handle DML (INSERT/UPDATE/DELETE) via PreparedStatement
 */
class ExtendedQueryProtocolCompatTest {

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
            s.execute("""
                CREATE TABLE ext_items (
                    id serial PRIMARY KEY,
                    name text NOT NULL,
                    category text,
                    price numeric(10,2),
                    active boolean DEFAULT true,
                    created_at timestamp DEFAULT now()
                )
            """);
            s.execute("INSERT INTO ext_items (name, category, price) VALUES ('Alpha', 'tools', 9.99)");
            s.execute("INSERT INTO ext_items (name, category, price) VALUES ('Beta', 'parts', 19.99)");
            s.execute("INSERT INTO ext_items (name, category, price) VALUES ('Gamma', 'tools', 29.99)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // Basic SELECT with single parameter
    // =========================================================================

    @Test
    void testSelectWithStringParameter() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM ext_items WHERE category = ?")) {
            ps.setString(1, "tools");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
        }
    }

    @Test
    void testSelectWithIntParameter() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM ext_items WHERE id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("Alpha", rs.getString(1));
            }
        }
    }

    @Test
    void testSelectWithBooleanParameter() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM ext_items WHERE active = ?")) {
            ps.setBoolean(1, true);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    @Test
    void testSelectWithNumericParameter() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM ext_items WHERE price > ?")) {
            ps.setBigDecimal(1, new BigDecimal("15.00"));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next()); // Beta or Gamma
            }
        }
    }

    // =========================================================================
    // Multiple parameters
    // =========================================================================

    @Test
    void testSelectWithMultipleParameters() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM ext_items WHERE category = ? AND price > ?")) {
            ps.setString(1, "tools");
            ps.setBigDecimal(2, new BigDecimal("20.00"));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("Gamma", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void testSelectWithThreeParameters() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id FROM ext_items WHERE name = ? AND category = ? AND active = ?")) {
            ps.setString(1, "Alpha");
            ps.setString(2, "tools");
            ps.setBoolean(3, true);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    // =========================================================================
    // Parameter reuse (same parameter position used multiple times conceptually)
    // =========================================================================

    @Test
    void testSelectWithSameValueDifferentPositions() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM ext_items WHERE category = ? OR name = ?")) {
            ps.setString(1, "parts");
            ps.setString(2, "Alpha");
            try (ResultSet rs = ps.executeQuery()) {
                int count = 0;
                while (rs.next()) count++;
                assertEquals(2, count); // Beta (parts) + Alpha
            }
        }
    }

    // =========================================================================
    // Prepared INSERT
    // =========================================================================

    @Test
    void testPreparedInsert() throws SQLException {
        exec("CREATE TABLE ext_insert (id serial PRIMARY KEY, name text, value int)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO ext_insert (name, value) VALUES (?, ?)")) {
            ps.setString(1, "test");
            ps.setInt(2, 42);
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals("42", query1("SELECT value FROM ext_insert WHERE name = 'test'"));
    }

    @Test
    void testPreparedInsertWithReturning() throws SQLException {
        exec("CREATE TABLE ext_ins_ret (id serial PRIMARY KEY, name text)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO ext_ins_ret (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "returned");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) > 0);
            }
        }
    }

    @Test
    void testPreparedInsertMultipleExecutions() throws SQLException {
        exec("CREATE TABLE ext_batch_ins (id serial PRIMARY KEY, label text)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO ext_batch_ins (label) VALUES (?)")) {
            for (int i = 1; i <= 5; i++) {
                ps.setString(1, "item_" + i);
                ps.executeUpdate();
            }
        }
        assertEquals("5", query1("SELECT COUNT(*) FROM ext_batch_ins"));
    }

    // =========================================================================
    // Prepared UPDATE
    // =========================================================================

    @Test
    void testPreparedUpdate() throws SQLException {
        exec("CREATE TABLE ext_upd (id serial PRIMARY KEY, val text, version int DEFAULT 0)");
        exec("INSERT INTO ext_upd (val) VALUES ('original')");
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE ext_upd SET val = ?, version = version + 1 WHERE id = ?")) {
            ps.setString(1, "updated");
            ps.setInt(2, 1);
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals("updated", query1("SELECT val FROM ext_upd WHERE id = 1"));
    }

    // =========================================================================
    // Prepared DELETE
    // =========================================================================

    @Test
    void testPreparedDelete() throws SQLException {
        exec("CREATE TABLE ext_del (id serial PRIMARY KEY, name text)");
        exec("INSERT INTO ext_del (name) VALUES ('keep'), ('remove')");
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM ext_del WHERE name = ?")) {
            ps.setString(1, "remove");
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals("1", query1("SELECT COUNT(*) FROM ext_del"));
    }

    // =========================================================================
    // No rows returned
    // =========================================================================

    @Test
    void testPreparedSelectNoRows() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM ext_items WHERE category = ?")) {
            ps.setString(1, "nonexistent_category");
            try (ResultSet rs = ps.executeQuery()) {
                assertFalse(rs.next());
            }
        }
    }

    // =========================================================================
    // No parameters (still uses extended protocol via PreparedStatement)
    // =========================================================================

    @Test
    void testPreparedSelectNoParameters() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM ext_items")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    // =========================================================================
    // NULL parameter values
    // =========================================================================

    @Test
    void testPreparedWithNullParameter() throws SQLException {
        exec("CREATE TABLE ext_nullp (id serial PRIMARY KEY, name text, note text)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO ext_nullp (name, note) VALUES (?, ?)")) {
            ps.setString(1, "with_null");
            ps.setNull(2, Types.VARCHAR);
            ps.executeUpdate();
        }
        assertEquals("1", query1("SELECT COUNT(*) FROM ext_nullp WHERE note IS NULL"));
    }

    @Test
    void testPreparedSelectWithNullComparison() throws SQLException {
        exec("CREATE TABLE ext_nullq (id serial PRIMARY KEY, category text)");
        exec("INSERT INTO ext_nullq (category) VALUES ('a'), (NULL), ('b')");
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM ext_nullq WHERE category = ?")) {
            ps.setString(1, "a");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    // =========================================================================
    // Various data types as parameters
    // =========================================================================

    @Test
    void testPreparedWithDateParameter() throws SQLException {
        exec("CREATE TABLE ext_dates (id serial PRIMARY KEY, event_date date, label text)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO ext_dates (event_date, label) VALUES (?, ?)")) {
            ps.setDate(1, Date.valueOf(LocalDate.of(2024, 6, 15)));
            ps.setString(2, "mid_year");
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT label FROM ext_dates WHERE event_date = ?")) {
            ps.setDate(1, Date.valueOf(LocalDate.of(2024, 6, 15)));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("mid_year", rs.getString(1));
            }
        }
    }

    @Test
    void testPreparedWithTimestampParameter() throws SQLException {
        exec("CREATE TABLE ext_ts (id serial PRIMARY KEY, ts timestamp, label text)");
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2024, 1, 15, 10, 30, 0));
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO ext_ts (ts, label) VALUES (?, ?)")) {
            ps.setTimestamp(1, ts);
            ps.setString(2, "morning");
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT label FROM ext_ts WHERE ts = ?")) {
            ps.setTimestamp(1, ts);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("morning", rs.getString(1));
            }
        }
    }

    @Test
    void testPreparedWithLongParameter() throws SQLException {
        exec("CREATE TABLE ext_longs (id bigint PRIMARY KEY, data text)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO ext_longs (id, data) VALUES (?, ?)")) {
            ps.setLong(1, 9999999999L);
            ps.setString(2, "big_id");
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT data FROM ext_longs WHERE id = ?")) {
            ps.setLong(1, 9999999999L);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("big_id", rs.getString(1));
            }
        }
    }

    @Test
    void testPreparedWithDoubleParameter() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM ext_items WHERE price < ?")) {
            ps.setDouble(1, 15.0);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("Alpha", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // Subqueries with parameters
    // =========================================================================

    @Test
    void testPreparedWithSubquery() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM ext_items WHERE price > (SELECT AVG(price) FROM ext_items WHERE category = ?)")) {
            ps.setString(1, "tools");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                // Gamma (29.99) is above the avg of tools (9.99+29.99)/2 = 19.99
            }
        }
    }

    @Test
    void testPreparedExistsWithParameter() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM ext_items WHERE category = ?)")) {
            ps.setString(1, "tools");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    // =========================================================================
    // JOIN with parameters
    // =========================================================================

    @Test
    void testPreparedJoinWithParameters() throws SQLException {
        exec("CREATE TABLE ext_categories (code text PRIMARY KEY, label text)");
        exec("INSERT INTO ext_categories VALUES ('tools', 'Hardware Tools'), ('parts', 'Spare Parts')");
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT i.name, c.label
            FROM ext_items i
            JOIN ext_categories c ON c.code = i.category
            WHERE i.price > ? AND c.code = ?
        """)) {
            ps.setBigDecimal(1, new BigDecimal("5.00"));
            ps.setString(2, "tools");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
            }
        }
    }

    // =========================================================================
    // ResultSet metadata from extended protocol
    // =========================================================================

    @Test
    void testResultSetMetadata() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, name, price, active FROM ext_items WHERE category = ?")) {
            ps.setString(1, "tools");
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(4, md.getColumnCount());
                assertEquals("id", md.getColumnName(1));
                assertEquals("name", md.getColumnName(2));
                assertEquals("price", md.getColumnName(3));
                assertEquals("active", md.getColumnName(4));
            }
        }
    }

    @Test
    void testPreparedStatementMetaData() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, name FROM ext_items WHERE id = ?")) {
            // ParameterMetaData describes the ? parameters
            ParameterMetaData pmd = ps.getParameterMetaData();
            assertEquals(1, pmd.getParameterCount());
        }
    }

    // =========================================================================
    // Prepared DDL (less common but valid)
    // =========================================================================

    @Test
    void testPreparedDdlNoParameters() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "CREATE TABLE ext_ddl_test (id serial PRIMARY KEY, val text)")) {
            ps.execute();
        }
        exec("INSERT INTO ext_ddl_test (val) VALUES ('ddl_ok')");
        assertEquals("ddl_ok", query1("SELECT val FROM ext_ddl_test"));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }
}
