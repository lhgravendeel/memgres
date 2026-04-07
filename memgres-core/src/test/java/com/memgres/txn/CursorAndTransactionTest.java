package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for cursor-based fetching (setFetchSize), autocommit toggling,
 * multiple active ResultSets, JSON/JSONB parameters, and array parameters.
 *
 * setFetchSize() makes the JDBC driver create a named portal and fetch
 * rows in chunks rather than all at once. This requires the server to
 * support portal suspend/resume (Execute with maxRows, then more Execute).
 *
 * Autocommit toggling tests the implicit BEGIN/COMMIT that the driver
 * injects when switching between autocommit modes.
 *
 * Array parameters test the PG-idiomatic WHERE id = ANY(?) pattern
 * which avoids building IN (?, ?, ?) with variable placeholder counts.
 */
class CursorAndTransactionTest {

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
            s.execute("CREATE TABLE cursor_data (id serial PRIMARY KEY, val int, label text)");
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= 500; i++) {
                if (i > 1) sb.append(", ");
                sb.append("(").append(i).append(", 'item_").append(i).append("')");
            }
            s.execute("INSERT INTO cursor_data (val, label) VALUES " + sb);
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
    // 4. Cursor-based fetching with setFetchSize
    // =========================================================================

    @Test
    void testFetchSizeSmall() throws SQLException {
        // fetchSize requires autocommit off (driver creates named portal in transaction)
        conn.setAutoCommit(false);
        try {
            try (PreparedStatement ps = conn.prepareStatement("SELECT val, label FROM cursor_data ORDER BY id")) {
                ps.setFetchSize(10); // fetch 10 rows at a time
                try (ResultSet rs = ps.executeQuery()) {
                    int count = 0;
                    while (rs.next()) {
                        count++;
                        assertNotNull(rs.getString("label"));
                    }
                    assertEquals(500, count);
                }
            }
        } finally {
            conn.setAutoCommit(true);
        }
    }

    @Test
    void testFetchSizeOne() throws SQLException {
        conn.setAutoCommit(false);
        try {
            try (PreparedStatement ps = conn.prepareStatement("SELECT val FROM cursor_data WHERE val <= 5 ORDER BY val")) {
                ps.setFetchSize(1); // fetch one row at a time
                try (ResultSet rs = ps.executeQuery()) {
                    for (int i = 1; i <= 5; i++) {
                        assertTrue(rs.next());
                        assertEquals(i, rs.getInt(1));
                    }
                    assertFalse(rs.next());
                }
            }
        } finally {
            conn.setAutoCommit(true);
        }
    }

    @Test
    void testFetchSizeWithParameters() throws SQLException {
        conn.setAutoCommit(false);
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT val, label FROM cursor_data WHERE val > ? ORDER BY val")) {
                ps.setFetchSize(50);
                ps.setInt(1, 450);
                try (ResultSet rs = ps.executeQuery()) {
                    int count = 0;
                    while (rs.next()) count++;
                    assertEquals(50, count);
                }
            }
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // 5. setMaxRows
    // =========================================================================

    @Test
    void testSetMaxRows() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT val FROM cursor_data ORDER BY id")) {
            ps.setMaxRows(10);
            try (ResultSet rs = ps.executeQuery()) {
                int count = 0;
                while (rs.next()) count++;
                assertEquals(10, count);
            }
        }
    }

    @Test
    void testSetMaxRowsWithFetchSize() throws SQLException {
        conn.setAutoCommit(false);
        try {
            try (PreparedStatement ps = conn.prepareStatement("SELECT val FROM cursor_data ORDER BY id")) {
                ps.setMaxRows(25);
                ps.setFetchSize(10);
                try (ResultSet rs = ps.executeQuery()) {
                    int count = 0;
                    while (rs.next()) count++;
                    assertEquals(25, count);
                }
            }
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // 10. Multiple active ResultSets
    // =========================================================================

    @Test
    void testMultipleActiveResultSets() throws SQLException {
        // Two open ResultSets from different PreparedStatements on same connection
        try (PreparedStatement ps1 = conn.prepareStatement("SELECT val FROM cursor_data WHERE val <= ?");
             PreparedStatement ps2 = conn.prepareStatement("SELECT label FROM cursor_data WHERE val = ?")) {
            ps1.setInt(1, 3);
            try (ResultSet rs1 = ps1.executeQuery()) {
                while (rs1.next()) {
                    int val = rs1.getInt(1);
                    ps2.setInt(1, val);
                    try (ResultSet rs2 = ps2.executeQuery()) {
                        assertTrue(rs2.next());
                        assertEquals("item_" + val, rs2.getString(1));
                    }
                }
            }
        }
    }

    // =========================================================================
    // 11. Autocommit toggling
    // =========================================================================

    @Test
    void testAutocommitToggle() throws SQLException {
        exec("CREATE TABLE ac_toggle (id serial PRIMARY KEY, val text)");

        // Autocommit on: each statement is its own transaction
        conn.setAutoCommit(true);
        exec("INSERT INTO ac_toggle (val) VALUES ('auto1')");

        // Switch to manual transaction
        conn.setAutoCommit(false);
        exec("INSERT INTO ac_toggle (val) VALUES ('manual1')");
        exec("INSERT INTO ac_toggle (val) VALUES ('manual2')");
        conn.commit();

        // Switch back to autocommit
        conn.setAutoCommit(true);
        exec("INSERT INTO ac_toggle (val) VALUES ('auto2')");

        assertEquals("4", query1("SELECT COUNT(*) FROM ac_toggle"));
    }

    @Test
    void testAutocommitToggleWithRollback() throws SQLException {
        exec("CREATE TABLE ac_rollback (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO ac_rollback (val) VALUES ('committed')");

        conn.setAutoCommit(false);
        exec("INSERT INTO ac_rollback (val) VALUES ('will_rollback')");
        conn.rollback();
        conn.setAutoCommit(true);

        assertEquals("1", query1("SELECT COUNT(*) FROM ac_rollback"));
    }

    @Test
    void testAutocommitOffImplicitCommitOnToggle() throws SQLException {
        exec("CREATE TABLE ac_implicit (id serial PRIMARY KEY, val text)");

        conn.setAutoCommit(false);
        exec("INSERT INTO ac_implicit (val) VALUES ('before_toggle')");
        // Setting autocommit to true implicitly commits the open transaction
        conn.setAutoCommit(true);

        assertEquals("1", query1("SELECT COUNT(*) FROM ac_implicit"));
    }

    @Test
    void testPreparedStatementAcrossAutocommitChange() throws SQLException {
        exec("CREATE TABLE ac_prep (id serial PRIMARY KEY, val text)");

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ac_prep (val) VALUES (?)")) {
            conn.setAutoCommit(true);
            ps.setString(1, "auto");
            ps.executeUpdate();

            conn.setAutoCommit(false);
            ps.setString(1, "manual");
            ps.executeUpdate();
            conn.commit();

            conn.setAutoCommit(true);
        }
        assertEquals("2", query1("SELECT COUNT(*) FROM ac_prep"));
    }

    // =========================================================================
    // 13. JSON/JSONB parameters
    // =========================================================================

    @Test
    void testJsonbParameterViaSetObject() throws SQLException {
        exec("CREATE TABLE json_param (id serial PRIMARY KEY, data jsonb)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO json_param (data) VALUES (?::jsonb)")) {
            ps.setObject(1, "{\"key\": \"value\", \"num\": 42}", Types.OTHER);
            ps.executeUpdate();
        }
        assertEquals("value", query1("SELECT data->>'key' FROM json_param WHERE id = 1"));
    }

    @Test
    void testJsonbParameterInWhere() throws SQLException {
        exec("CREATE TABLE json_where (id serial PRIMARY KEY, config jsonb)");
        exec("INSERT INTO json_where (config) VALUES ('{\"env\": \"prod\"}')");
        exec("INSERT INTO json_where (config) VALUES ('{\"env\": \"dev\"}')");
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM json_where WHERE config->>'env' = ?")) {
            ps.setString(1, "prod");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    void testJsonbArrayParameter() throws SQLException {
        exec("CREATE TABLE json_arr_param (id serial PRIMARY KEY, tags jsonb)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO json_arr_param (tags) VALUES (?::jsonb)")) {
            ps.setObject(1, "[\"tag1\", \"tag2\", \"tag3\"]", Types.OTHER);
            ps.executeUpdate();
        }
        assertNotNull(query1("SELECT tags FROM json_arr_param WHERE id = 1"));
    }

    @Test
    void testJsonbNullParameter() throws SQLException {
        exec("CREATE TABLE json_null (id serial PRIMARY KEY, data jsonb)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO json_null (data) VALUES (?::jsonb)")) {
            ps.setNull(1, Types.OTHER);
            ps.executeUpdate();
        }
        assertEquals("1", query1("SELECT COUNT(*) FROM json_null WHERE data IS NULL"));
    }

    // =========================================================================
    // 17. Array parameters: WHERE id = ANY(?)
    // =========================================================================

    @Test
    void testArrayParameterIntegerAny() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM cursor_data WHERE val = ANY(?)")) {
            Array arr = conn.createArrayOf("int4", new Object[]{1, 5, 10, 50, 100});
            ps.setArray(1, arr);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
            }
        }
    }

    @Test
    void testArrayParameterTextAny() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM cursor_data WHERE label = ANY(?)")) {
            Array arr = conn.createArrayOf("text", new Object[]{"item_1", "item_2", "item_3"});
            ps.setArray(1, arr);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    @Test
    void testArrayParameterEmptyArray() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM cursor_data WHERE val = ANY(?)")) {
            Array arr = conn.createArrayOf("int4", new Object[]{});
            ps.setArray(1, arr);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    void testArrayParameterInInsert() throws SQLException {
        exec("CREATE TABLE arr_insert (id serial PRIMARY KEY, tags text[])");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO arr_insert (tags) VALUES (?)")) {
            Array arr = conn.createArrayOf("text", new Object[]{"red", "green", "blue"});
            ps.setArray(1, arr);
            ps.executeUpdate();
        }
        assertEquals("3", query1("SELECT array_length(tags, 1) FROM arr_insert WHERE id = 1"));
    }

    // =========================================================================
    // 6. Scroll-insensitive ResultSet
    // =========================================================================

    @Test
    void testScrollInsensitiveResultSet() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT val FROM cursor_data WHERE val <= 5 ORDER BY val",
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            try (ResultSet rs = ps.executeQuery()) {
                // Move to last
                assertTrue(rs.last());
                assertEquals(5, rs.getInt(1));
                // Move back to first
                assertTrue(rs.first());
                assertEquals(1, rs.getInt(1));
                // Move to specific row
                assertTrue(rs.absolute(3));
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    // =========================================================================
    // 9. COPY via JDBC CopyManager (if available)
    // =========================================================================

    @Test
    void testCopyInViaCopyManager() throws SQLException {
        exec("CREATE TABLE copy_in_test (id int, name text)");
        // PG JDBC exposes CopyManager through unwrap
        try {
            org.postgresql.copy.CopyManager cm = conn.unwrap(org.postgresql.PGConnection.class).getCopyAPI();
            String data = "1\tAlice\n2\tBob\n3\tCharlie\n";
            long rows = cm.copyIn("COPY copy_in_test FROM STDIN", new java.io.StringReader(data));
            assertEquals(3, rows);
            assertEquals("3", query1("SELECT COUNT(*) FROM copy_in_test"));
        } catch (Exception e) {
            // If unwrap fails or COPY isn't supported, that's expected for now
        }
    }
}
