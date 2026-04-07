package com.memgres.catalog;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for pg_show_all_settings() SRF and set_config() combined patterns.
 *
 * Real-world tools (e.g. JDBC drivers, ORMs) issue queries like:
 *   SELECT set_config('bytea_output','hex',false)
 *   FROM pg_show_all_settings() WHERE name = 'bytea_output'
 *
 * This requires:
 * - pg_show_all_settings() as a set-returning function in FROM
 * - Columns: name, setting, (and others)
 * - set_config(name, value, is_local) function
 * - WHERE filtering on the SRF output
 */
class PgShowAllSettingsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
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

    static List<String> columnNames(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> cols = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) cols.add(md.getColumnName(i));
            return cols;
        }
    }

    // ========================================================================
    // The exact real-world query from JDBC drivers / tools
    // ========================================================================

    @Test
    void set_config_from_pg_show_all_settings_with_where() throws SQLException {
        String val = scalar(
                "SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name = 'bytea_output'");
        assertEquals("hex", val, "Should return 'hex' as 1 row with column 'set_config'");
    }

    // ========================================================================
    // pg_show_all_settings() as SRF
    // ========================================================================

    @Test
    void pg_show_all_settings_returns_rows() throws SQLException {
        int count = countRows("SELECT * FROM pg_show_all_settings()");
        assertTrue(count > 0, "pg_show_all_settings() should return rows");
        assertTrue(count >= 10, "Should have at least 10 settings, got " + count);
    }

    @Test
    void pg_show_all_settings_has_name_and_setting_columns() throws SQLException {
        List<String> cols = columnNames("SELECT * FROM pg_show_all_settings() LIMIT 1");
        assertTrue(cols.contains("name"), "Should have 'name' column, got: " + cols);
        assertTrue(cols.contains("setting"), "Should have 'setting' column, got: " + cols);
    }

    @Test
    void pg_show_all_settings_where_filter() throws SQLException {
        String setting = scalar(
                "SELECT setting FROM pg_show_all_settings() WHERE name = 'server_version'");
        assertNotNull(setting, "server_version should be in pg_show_all_settings()");
        assertTrue(setting.startsWith("18"), "server_version should be 18.x, got: " + setting);
    }

    @Test
    void pg_show_all_settings_contains_common_settings() throws SQLException {
        // These should all be present
        for (String name : Cols.listOf("server_version", "DateStyle", "TimeZone", "search_path")) {
            int count = countRows(
                    "SELECT 1 FROM pg_show_all_settings() WHERE name = '" + name + "'");
            assertTrue(count >= 1, "pg_show_all_settings should contain '" + name + "'");
        }
    }

    // ========================================================================
    // set_config() function
    // ========================================================================

    @Test
    void set_config_returns_new_value() throws SQLException {
        String val = scalar("SELECT set_config('application_name', 'test_app', false)");
        assertEquals("test_app", val, "set_config should return the new value");
    }

    @Test
    void set_config_persists_value() throws SQLException {
        exec("SELECT set_config('application_name', 'persist_test', false)");
        String val = scalar("SELECT current_setting('application_name')");
        assertEquals("persist_test", val, "Value set by set_config should persist");
    }
}
