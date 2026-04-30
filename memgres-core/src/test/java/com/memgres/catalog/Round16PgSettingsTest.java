package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category O: pg_settings completeness.
 *
 * Covers:
 *  - pg_settings.unit populated for memory/time parameters
 *  - pg_settings.min_val / max_val populated for bounded parameters
 *  - pg_settings.enumvals populated for enum parameters
 *  - boot_val ≠ reset_val observable after user-level SET
 */
class Round16PgSettingsTest {

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

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // O1. pg_settings.unit populated for memory param
    // =========================================================================

    @Test
    void work_mem_has_non_null_unit() throws SQLException {
        String u = str("SELECT unit FROM pg_settings WHERE name='work_mem'");
        assertNotNull(u, "pg_settings.unit must be populated for work_mem");
        assertFalse(u.isEmpty(),
                "pg_settings.unit must not be empty for work_mem (expected 'kB' or 'B')");
    }

    @Test
    void statement_timeout_has_ms_unit() throws SQLException {
        String u = str("SELECT unit FROM pg_settings WHERE name='statement_timeout'");
        assertEquals("ms", u,
                "pg_settings.unit for statement_timeout must be 'ms'; got '" + u + "'");
    }

    // =========================================================================
    // O2. pg_settings.min_val / max_val bounded params
    // =========================================================================

    @Test
    void work_mem_has_min_val_and_max_val() throws SQLException {
        String min = str("SELECT min_val FROM pg_settings WHERE name='work_mem'");
        String max = str("SELECT max_val FROM pg_settings WHERE name='work_mem'");
        assertNotNull(min, "pg_settings.min_val for work_mem must not be null");
        assertNotNull(max, "pg_settings.max_val for work_mem must not be null");
    }

    // =========================================================================
    // O3. pg_settings.enumvals for enum param
    // =========================================================================

    @Test
    void client_min_messages_has_enumvals_array() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT enumvals FROM pg_settings WHERE name='client_min_messages'")) {
            assertTrue(rs.next());
            Object v = rs.getArray(1);
            assertNotNull(v,
                    "pg_settings.enumvals for client_min_messages must be populated with enum labels");
        }
    }

    // =========================================================================
    // O4. boot_val ≠ reset_val observable after SET
    // =========================================================================

    @Test
    void boot_val_differs_after_user_level_set() throws SQLException {
        // After user-level SET, reset_val should reflect the server default while
        // setting reflects the per-session value. boot_val must stay at compile-time default.
        try (Statement s = conn.createStatement()) {
            s.execute("SET work_mem = '8MB'");
            try (ResultSet rs = s.executeQuery(
                    "SELECT setting, boot_val FROM pg_settings WHERE name='work_mem'")) {
                assertTrue(rs.next());
                String setting = rs.getString(1);
                String bootVal = rs.getString(2);
                assertNotEquals(setting, bootVal,
                        "After SET work_mem='8MB', pg_settings.setting must differ from boot_val; " +
                                "got setting=" + setting + ", boot_val=" + bootVal);
            }
            s.execute("RESET work_mem");
        }
    }
}
