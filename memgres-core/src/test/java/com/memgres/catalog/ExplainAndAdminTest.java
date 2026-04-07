package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for EXPLAIN options, COMMENT ON, role management, and admin commands.
 *
 * Key behaviors:
 * - EXPLAIN (FORMAT json/text/yaml): different output formats
 * - EXPLAIN (ANALYZE false, COSTS false, VERBOSE true): option flags
 * - EXPLAIN (COSTS OFF): simplified plan output
 * - COMMENT ON TABLE/INDEX/COLUMN/FUNCTION
 * - SET ROLE / RESET ROLE / DROP ROLE interactions
 * - SELECT FROM pg_class: no-column SELECT (returns rows with no columns)
 */
class ExplainAndAdminTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE admin_t(id int PRIMARY KEY, a int, b text)");
        exec("INSERT INTO admin_t VALUES (1, 10, 'x'), (2, 20, 'y')");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = 0; while (rs.next()) n++; return n;
        }
    }

    static List<String> column(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            List<String> vals = new ArrayList<>();
            while (rs.next()) vals.add(rs.getString(1));
            return vals;
        }
    }

    // ========================================================================
    // EXPLAIN options
    // ========================================================================

    @Test
    void explain_costs_off() throws SQLException {
        List<String> lines = column("EXPLAIN (COSTS OFF) SELECT * FROM admin_t ORDER BY id");
        assertFalse(lines.isEmpty(), "EXPLAIN should return at least one line");
        // With COSTS OFF, lines should NOT contain cost estimates like "(cost=..."
        for (String line : lines) {
            assertFalse(line.contains("cost="), "COSTS OFF should suppress cost estimates: " + line);
        }
    }

    @Test
    void explain_format_json() throws SQLException {
        String result = scalar("EXPLAIN (FORMAT json) SELECT * FROM admin_t");
        assertNotNull(result, "EXPLAIN FORMAT json should return a result");
        // JSON format should start with [ and contain a query plan
        assertTrue(result.trim().startsWith("[") || result.trim().startsWith("{"),
                "JSON format should return JSON: " + result.substring(0, Math.min(50, result.length())));
    }

    @Test
    void explain_format_text() throws SQLException {
        String result = scalar("EXPLAIN (FORMAT text) SELECT * FROM admin_t");
        assertNotNull(result, "EXPLAIN FORMAT text should return a result");
    }

    @Test
    void explain_analyze_false_costs_false() throws SQLException {
        // These are boolean option flags
        List<String> lines = column("EXPLAIN (ANALYZE false, COSTS false) SELECT * FROM admin_t");
        assertFalse(lines.isEmpty());
        for (String line : lines) {
            assertFalse(line.contains("cost="), "COSTS false should suppress costs");
            // ANALYZE false should NOT show actual time
            assertFalse(line.contains("actual time="), "ANALYZE false should suppress timing");
        }
    }

    @Test
    void explain_bad_format_fails() {
        // EXPLAIN (FORMAT yamlish) should fail because it is not a valid format
        assertThrows(SQLException.class,
                () -> exec("EXPLAIN (FORMAT yamlish) SELECT * FROM admin_t"),
                "Invalid EXPLAIN format should fail");
    }

    @Test
    void explain_bad_analyze_value_fails() {
        // EXPLAIN (ANALYZE maybe) should fail because it is not a valid boolean
        assertThrows(SQLException.class,
                () -> exec("EXPLAIN (ANALYZE maybe) SELECT * FROM admin_t"),
                "Invalid ANALYZE value should fail");
    }

    // ========================================================================
    // COMMENT ON
    // ========================================================================

    @Test
    void comment_on_table() throws SQLException {
        exec("COMMENT ON TABLE admin_t IS 'the admin table'");
        String comment = scalar(
                "SELECT obj_description('admin_t'::regclass)");
        assertEquals("the admin table", comment);
    }

    @Test
    void comment_on_column() throws SQLException {
        exec("COMMENT ON COLUMN admin_t.a IS 'the a column'");
        String comment = scalar(
                "SELECT col_description('admin_t'::regclass, 2)");
        assertEquals("the a column", comment);
    }

    @Test
    void comment_on_index() throws SQLException {
        // COMMENT ON INDEX should be supported
        exec("CREATE INDEX admin_t_a_idx ON admin_t(a)");
        try {
            exec("COMMENT ON INDEX admin_t_a_idx IS 'index on a'");
            // Verify comment was stored (via obj_description or pg_description)
            String comment = scalar(
                    "SELECT obj_description('admin_t_a_idx'::regclass)");
            assertEquals("index on a", comment);
        } finally {
            exec("DROP INDEX IF EXISTS admin_t_a_idx");
        }
    }

    // ========================================================================
    // Role management
    // ========================================================================

    @Test
    void set_role_and_drop_role_lifecycle() throws SQLException {
        exec("CREATE ROLE compat_role");
        try {
            // PG16+ requires explicit membership before SET ROLE
            exec("GRANT compat_role TO memgres");
            // SET ROLE should succeed
            exec("SET ROLE compat_role");
            exec("RESET ROLE");

            // DROP ROLE should succeed
            exec("DROP ROLE compat_role");

            // After DROP, SET ROLE should fail
            assertThrows(SQLException.class,
                    () -> exec("SET ROLE compat_role"),
                    "SET ROLE to dropped role should fail");
        } catch (SQLException e) {
            // cleanup in case test fails midway
            try { exec("RESET ROLE"); } catch (SQLException ignored) {}
            try { exec("DROP ROLE IF EXISTS compat_role"); } catch (SQLException ignored) {}
            throw e;
        }
    }

    @Test
    void grant_on_nonexistent_table_fails() {
        try {
            exec("CREATE ROLE compat_role2");
            try {
                // GRANT on nonexistent table should fail with 42P01 (undefined_table)
                assertThrows(SQLException.class,
                        () -> exec("GRANT SELECT ON no_such TO compat_role2"));
            } finally {
                exec("DROP ROLE IF EXISTS compat_role2");
            }
        } catch (SQLException e) {
            // ignore setup failures
        }
    }

    // ========================================================================
    // SELECT FROM table (no column list)
    // ========================================================================

    @Test
    void select_from_pg_class_without_columns() throws SQLException {
        // SELECT FROM pg_class is valid PG syntax, returning rows with zero output columns
        // but the row count should match the number of pg_class entries
        int count = countRows("SELECT FROM pg_class");
        assertTrue(count > 0, "SELECT FROM pg_class should return rows (system catalogs exist)");
        // PG 18 typically has hundreds of entries
        assertTrue(count > 10, "pg_class should have many entries, got " + count);
    }

    // ========================================================================
    // version() reports PG 18
    // ========================================================================

    @Test
    void version_reports_pg18() throws SQLException {
        String version = scalar("SELECT version()");
        assertNotNull(version);
        assertTrue(version.contains("18.0") || version.contains("18"),
                "version() should report PG 18, got: " + version);
    }

    @Test
    void server_version_setting_is_18() throws SQLException {
        String sv = scalar("SELECT current_setting('server_version')");
        assertTrue(sv != null && sv.startsWith("18"),
                "server_version should be 18.x, got: " + sv);
    }
}
