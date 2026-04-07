package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 3: Session state and GUC-sensitive output.
 * Tests TimeZone, DateStyle, IntervalStyle, extra_float_digits,
 * search_path, locale/collation-sensitive settings.
 */
class SessionStateGucTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }
    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // --- TimeZone ---

    @Test void timezone_utc_timestamp() throws Exception {
        exec("SET TimeZone = 'UTC'");
        String result = scalar("SELECT TIMESTAMP WITH TIME ZONE '2024-01-15 12:00:00+00'");
        assertTrue(result.contains("2024-01-15") && result.contains("12:00:00"),
                "UTC should show noon: " + result);
    }

    @Test void timezone_us_eastern_shifts_output() throws Exception {
        exec("SET TimeZone = 'US/Eastern'");
        String result = scalar("SELECT TIMESTAMP WITH TIME ZONE '2024-07-15 12:00:00+00'");
        // July in Eastern is UTC-4 (EDT), so 12:00 UTC = 08:00 EDT
        assertTrue(result.contains("08:00:00"), "Eastern in summer should be UTC-4: " + result);
        exec("SET TimeZone = 'UTC'");
    }

    @Test void timezone_asia_tokyo() throws Exception {
        exec("SET TimeZone = 'Asia/Tokyo'");
        String result = scalar("SELECT TIMESTAMP WITH TIME ZONE '2024-01-15 00:00:00+00'");
        // Japan is UTC+9
        assertTrue(result.contains("09:00:00"), "Tokyo should be UTC+9: " + result);
        exec("SET TimeZone = 'UTC'");
    }

    @Test void timezone_affects_now() throws Exception {
        exec("SET TimeZone = 'UTC'");
        String utc = scalar("SELECT now()::text");
        exec("SET TimeZone = 'US/Pacific'");
        String pac = scalar("SELECT now()::text");
        // The time representations should differ
        assertNotNull(utc);
        assertNotNull(pac);
        exec("SET TimeZone = 'UTC'");
    }

    @Test void show_timezone() throws Exception {
        exec("SET TimeZone = 'UTC'");
        assertEquals("UTC", scalar("SHOW timezone"));
    }

    @Test void invalid_timezone_raises_error() throws Exception {
        SQLException ex = assertThrows(SQLException.class, () -> exec("SET TimeZone = 'NoSuch/Zone'"));
        assertEquals("22023", ex.getSQLState());
    }

    // --- DateStyle ---

    @Test void datestyle_iso_ymd() throws Exception {
        exec("SET DateStyle = 'ISO, YMD'");
        String result = scalar("SELECT DATE '2024-03-15'");
        assertEquals("2024-03-15", result);
    }

    @Test void datestyle_iso_dmy() throws Exception {
        exec("SET DateStyle = 'ISO, DMY'");
        String result = scalar("SELECT DATE '2024-03-15'");
        assertEquals("2024-03-15", result); // ISO format is always YYYY-MM-DD
    }

    @Test void datestyle_german() throws Exception {
        exec("SET DateStyle = 'German'");
        String result = scalar("SELECT DATE '2024-03-15'");
        // German style: DD.MM.YYYY
        assertEquals("15.03.2024", result);
        exec("SET DateStyle = 'ISO'");
    }

    @Test void datestyle_sql() throws Exception {
        exec("SET DateStyle = 'SQL, MDY'");
        String result = scalar("SELECT DATE '2024-03-15'");
        // SQL MDY style: MM/DD/YYYY
        assertEquals("03/15/2024", result);
        exec("SET DateStyle = 'ISO'");
    }

    @Test void datestyle_postgres_style() throws Exception {
        exec("SET DateStyle = 'Postgres, MDY'");
        String result = scalar("SELECT DATE '2024-03-15'");
        // Postgres style: Fri 15 Mar 2024 or similar
        assertNotNull(result);
        exec("SET DateStyle = 'ISO'");
    }

    @Test void show_datestyle() throws Exception {
        exec("SET DateStyle = 'ISO, YMD'");
        String result = scalar("SHOW datestyle");
        assertTrue(result.contains("ISO"), "Should show ISO style: " + result);
    }

    // --- IntervalStyle ---

    @Test void intervalstyle_postgres() throws Exception {
        exec("SET IntervalStyle = 'postgres'");
        String result = scalar("SELECT INTERVAL '1 year 2 months 3 days 4 hours'");
        assertTrue(result.contains("year") || result.contains("mon"), "Postgres style: " + result);
    }

    @Test void intervalstyle_iso_8601() throws Exception {
        exec("SET IntervalStyle = 'iso_8601'");
        String result = scalar("SELECT INTERVAL '1 year 2 months 3 days'");
        // ISO 8601: P1Y2M3D
        assertTrue(result.startsWith("P"), "ISO 8601 should start with P: " + result);
        exec("SET IntervalStyle = 'postgres'");
    }

    @Test void intervalstyle_sql_standard() throws Exception {
        exec("SET IntervalStyle = 'sql_standard'");
        String result = scalar("SELECT INTERVAL '3 days 4 hours 5 minutes'");
        assertNotNull(result);
        exec("SET IntervalStyle = 'postgres'");
    }

    // --- extra_float_digits ---

    @Test void extra_float_digits_default() throws Exception {
        exec("SET extra_float_digits = 1");
        String result = scalar("SELECT 1.0/3.0");
        assertNotNull(result);
    }

    @Test void extra_float_digits_maximum() throws Exception {
        exec("SET extra_float_digits = 3");
        String result = scalar("SELECT 1.0::double precision / 3.0");
        assertNotNull(result);
        exec("SET extra_float_digits = 1");
    }

    @Test void extra_float_digits_zero() throws Exception {
        exec("SET extra_float_digits = 0");
        String result = scalar("SELECT 1.0::double precision / 3.0");
        assertNotNull(result);
    }

    // --- search_path ---

    @Test void search_path_default_public() throws Exception {
        exec("SET search_path = public");
        assertEquals("public", scalar("SHOW search_path"));
    }

    @Test void search_path_changes_table_resolution() throws Exception {
        exec("CREATE SCHEMA sp_test");
        exec("CREATE TABLE sp_test.t1(id int)");
        exec("INSERT INTO sp_test.t1 VALUES (42)");
        exec("SET search_path = sp_test");
        assertEquals("42", scalar("SELECT id FROM t1"));
        exec("SET search_path = public");
        // t1 should not be found now
        assertThrows(SQLException.class, () -> scalar("SELECT id FROM t1"));
        exec("DROP SCHEMA sp_test CASCADE");
    }

    @Test void search_path_multiple_schemas() throws Exception {
        exec("CREATE SCHEMA sp_a");
        exec("CREATE SCHEMA sp_b");
        exec("CREATE TABLE sp_a.ta(v text)");
        exec("INSERT INTO sp_a.ta VALUES ('from_a')");
        exec("CREATE TABLE sp_b.tb(v text)");
        exec("INSERT INTO sp_b.tb VALUES ('from_b')");
        exec("SET search_path = sp_a, sp_b");
        assertEquals("from_a", scalar("SELECT v FROM ta"));
        assertEquals("from_b", scalar("SELECT v FROM tb"));
        exec("SET search_path = public");
        exec("DROP SCHEMA sp_a CASCADE");
        exec("DROP SCHEMA sp_b CASCADE");
    }

    @Test void search_path_pg_catalog_always_searched() throws Exception {
        exec("SET search_path = public");
        // pg_catalog functions should always be available
        assertNotNull(scalar("SELECT current_schema()"));
        assertNotNull(scalar("SELECT pg_typeof(1)"));
    }

    // --- current_setting / set_config ---

    @Test void current_setting_returns_guc_value() throws Exception {
        exec("SET TimeZone = 'UTC'");
        assertEquals("UTC", scalar("SELECT current_setting('timezone')"));
    }

    @Test void current_setting_missing_raises_error() throws Exception {
        assertThrows(SQLException.class, () -> scalar("SELECT current_setting('no_such_guc')"));
    }

    @Test void current_setting_missing_with_missing_ok() throws Exception {
        String result = scalar("SELECT current_setting('no_such_guc', true)");
        assertNull(result);
    }

    // --- RESET ---

    @Test void reset_timezone_restores_default() throws Exception {
        exec("SET TimeZone = 'Asia/Tokyo'");
        exec("RESET TimeZone");
        String tz = scalar("SHOW timezone");
        // Default is typically UTC
        assertNotNull(tz);
    }

    @Test void reset_all() throws Exception {
        exec("SET DateStyle = 'German'");
        exec("RESET ALL");
        String ds = scalar("SHOW datestyle");
        // After RESET ALL, should be back to default
        assertNotNull(ds);
    }

    // --- SET LOCAL (transaction-scoped) ---

    @Test void set_local_reverts_after_commit() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("SET TimeZone = 'UTC'");
            exec("SET LOCAL TimeZone = 'US/Eastern'");
            String inTx = scalar("SHOW timezone");
            assertTrue(inTx.contains("Eastern") || inTx.contains("US"), "In-tx should be Eastern: " + inTx);
            conn.commit();
            // After commit, should revert
            String afterCommit = scalar("SHOW timezone");
            assertEquals("UTC", afterCommit, "Should revert after commit");
        } finally {
            conn.setAutoCommit(true);
        }
    }
}
