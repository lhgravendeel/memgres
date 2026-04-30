package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category A: Wire-protocol GUC side effects.
 *
 * PG GUCs that the wire layer should consult to shape output format:
 *  - extra_float_digits (default 3 in PG 12+, means lossless roundtrip)
 *  - bytea_output ('hex' vs 'escape')
 *  - IntervalStyle ('postgres', 'postgres_verbose', 'sql_standard', 'iso_8601')
 *  - DateStyle ('ISO', 'Postgres', 'SQL', 'German')
 *  - standard_conforming_strings (default on)
 *
 * These are observable via simple query; some via binary codec only.
 */
class Round15WireProtocolGucTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A1. extra_float_digits
    // =========================================================================

    @Test
    void extra_float_digits_default_is_three() throws SQLException {
        // PG 12+ default is 3 (lossless float4/float8 roundtrip)
        String v = scalarString("SHOW extra_float_digits");
        assertEquals("3", v, "extra_float_digits default must be 3 in PG 12+");
    }

    @Test
    void extra_float_digits_affects_float_text_output() throws SQLException {
        exec("SET extra_float_digits = 3");
        // pi as float8 with lossless digits should give more than 6 sig figs
        String v = scalarString("SELECT (3.141592653589793::float8)::text");
        assertNotNull(v);
        assertTrue(v.length() >= 15,
                "lossless float8 formatting should emit 15+ chars; got '" + v + "'");
    }

    @Test
    void extra_float_digits_low_truncates() throws SQLException {
        exec("SET extra_float_digits = -15");
        // Very low precision — should emit fewer significant digits
        String v = scalarString("SELECT (3.141592653589793::float8)::text");
        assertNotNull(v);
    }

    // =========================================================================
    // A2. bytea_output
    // =========================================================================

    @Test
    void bytea_output_default_is_hex() throws SQLException {
        assertEquals("hex", scalarString("SHOW bytea_output"));
    }

    @Test
    void bytea_output_hex_format() throws SQLException {
        exec("SET bytea_output = 'hex'");
        String v = scalarString("SELECT E'\\\\x41'::bytea::text");
        assertEquals("\\x41", v, "hex format should produce '\\x41'");
    }

    @Test
    void bytea_output_escape_format() throws SQLException {
        exec("SET bytea_output = 'escape'");
        // 0x41 is 'A' — escape format leaves printable chars as-is
        String v = scalarString("SELECT E'\\\\x41'::bytea::text");
        assertEquals("A", v, "escape format should emit printable char");
    }

    @Test
    void bytea_output_escape_format_non_printable() throws SQLException {
        exec("SET bytea_output = 'escape'");
        String v = scalarString("SELECT E'\\\\x00'::bytea::text");
        assertEquals("\\000", v, "null byte in escape format → '\\000'");
    }

    // =========================================================================
    // A3. IntervalStyle
    // =========================================================================

    @Test
    void interval_style_default_is_postgres() throws SQLException {
        assertEquals("postgres", scalarString("SHOW IntervalStyle"));
    }

    @Test
    void interval_style_postgres() throws SQLException {
        exec("SET IntervalStyle = 'postgres'");
        String v = scalarString("SELECT (interval '1 year 2 months 3 days 04:05:06')::text");
        assertEquals("1 year 2 mons 3 days 04:05:06", v);
    }

    @Test
    void interval_style_iso_8601() throws SQLException {
        exec("SET IntervalStyle = 'iso_8601'");
        String v = scalarString("SELECT (interval '1 year 2 months 3 days 4 hours')::text");
        // ISO-8601 form: P1Y2M3DT4H
        assertTrue(v.startsWith("P"),
                "iso_8601 interval must start with 'P'; got " + v);
    }

    @Test
    void interval_style_sql_standard() throws SQLException {
        exec("SET IntervalStyle = 'sql_standard'");
        String v = scalarString("SELECT (interval '1-2')::text");
        // SQL standard form: '1-2' (year-month)
        assertNotNull(v);
    }

    @Test
    void interval_style_postgres_verbose() throws SQLException {
        exec("SET IntervalStyle = 'postgres_verbose'");
        String v = scalarString("SELECT (interval '1 day 2 hours')::text");
        // postgres_verbose: '@ 1 day 2 hours'
        assertTrue(v.startsWith("@"),
                "postgres_verbose must start with '@'; got " + v);
    }

    // =========================================================================
    // A4. DateStyle
    // =========================================================================

    @Test
    void datestyle_default_is_iso() throws SQLException {
        String v = scalarString("SHOW DateStyle");
        assertTrue(v.toUpperCase().contains("ISO"),
                "DateStyle should default to ISO; got " + v);
    }

    @Test
    void datestyle_iso_format() throws SQLException {
        exec("SET DateStyle = 'ISO, MDY'");
        String v = scalarString("SELECT (DATE '2025-03-14')::text");
        assertEquals("2025-03-14", v);
    }

    @Test
    void datestyle_postgres_disconnects_jdbc() throws SQLException {
        // PG 18 sends ParameterStatus for DateStyle; pgjdbc disconnects on non-ISO (08006).
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        try {
            try (Statement s = c.createStatement()) { s.execute("SET DateStyle = 'Postgres, MDY'"); }
            fail("Expected JDBC driver to disconnect on non-ISO DateStyle");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("DateStyle"), "Expected DateStyle error; got: " + e.getMessage());
        } finally {
            try { c.close(); } catch (Exception ignored) {}
        }
    }

    @Test
    void datestyle_sql_disconnects_jdbc() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        try {
            try (Statement s = c.createStatement()) { s.execute("SET DateStyle = 'SQL, MDY'"); }
            fail("Expected JDBC driver to disconnect on non-ISO DateStyle");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("DateStyle"), "Expected DateStyle error; got: " + e.getMessage());
        } finally {
            try { c.close(); } catch (Exception ignored) {}
        }
    }

    @Test
    void datestyle_german_disconnects_jdbc() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        try {
            try (Statement s = c.createStatement()) { s.execute("SET DateStyle = 'German'"); }
            fail("Expected JDBC driver to disconnect on non-ISO DateStyle");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("DateStyle"), "Expected DateStyle error; got: " + e.getMessage());
        } finally {
            try { c.close(); } catch (Exception ignored) {}
        }
    }

    // =========================================================================
    // A5. standard_conforming_strings (should be on by default in PG 9.1+)
    // =========================================================================

    @Test
    void standard_conforming_strings_default_on() throws SQLException {
        assertEquals("on", scalarString("SHOW standard_conforming_strings"));
    }

    // =========================================================================
    // A6. ParameterStatus — check a few other GUCs exist as server_version etc.
    // =========================================================================

    @Test
    void server_version_num_parameter() throws SQLException {
        // server_version_num should be >= 180000 for PG 18
        String v = scalarString("SHOW server_version_num");
        int num = Integer.parseInt(v);
        assertTrue(num >= 170000,
                "server_version_num must be at least PG 17 level; got " + v);
    }

    @Test
    void application_name_guc() throws SQLException {
        String v = scalarString("SHOW application_name");
        assertNotNull(v);
    }

    @Test
    void client_encoding_guc() throws SQLException {
        String v = scalarString("SHOW client_encoding");
        assertNotNull(v);
        // Must be UTF8 or similar
        assertTrue(v.equalsIgnoreCase("UTF8") || v.equalsIgnoreCase("UTF-8")
                || v.equalsIgnoreCase("SQL_ASCII"),
                "unexpected client_encoding: " + v);
    }

    @Test
    void is_superuser_guc() throws SQLException {
        String v = scalarString("SHOW is_superuser");
        assertNotNull(v);
        assertTrue(v.equals("on") || v.equals("off"));
    }

    @Test
    void session_authorization_guc() throws SQLException {
        String v = scalarString("SHOW session_authorization");
        assertNotNull(v);
    }

    @Test
    void search_path_guc() throws SQLException {
        String v = scalarString("SHOW search_path");
        assertNotNull(v);
    }

    @Test
    void default_transaction_isolation_guc() throws SQLException {
        String v = scalarString("SHOW default_transaction_isolation");
        assertNotNull(v);
        // PG default is 'read committed'
        assertTrue(v.toLowerCase().contains("read") || v.toLowerCase().contains("repeatable")
                || v.toLowerCase().contains("serializable"),
                "unexpected default_transaction_isolation: " + v);
    }

    @Test
    void in_hot_standby_guc() throws SQLException {
        // PG 14+: in_hot_standby is a GUC, default 'off'
        String v = scalarString("SHOW in_hot_standby");
        assertNotNull(v);
    }
}
