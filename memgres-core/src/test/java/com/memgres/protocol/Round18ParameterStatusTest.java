package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.PGConnection;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap category AD: ParameterStatus auto-push on GUC change.
 *
 * Covers:
 *  - SET application_name pushes ParameterStatus
 *  - SET DateStyle / IntervalStyle / TimeZone / client_encoding push
 *  - Startup burst includes server_encoding, standard_conforming_strings,
 *    integer_datetimes, is_superuser, TimeZone
 */
class Round18ParameterStatusTest {

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

    private static String ps(String name) throws SQLException {
        return conn.unwrap(PGConnection.class).getParameterStatus(name);
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // =========================================================================
    // AD1. application_name auto-push
    // =========================================================================

    @Test
    void application_name_change_pushed_via_ParameterStatus() throws SQLException {
        exec("SET application_name = 'r18_app'");
        String v = ps("application_name");
        assertEquals("r18_app", v,
                "ParameterStatus must reflect SET application_name; got '" + v + "'");
    }

    // =========================================================================
    // AD2. DateStyle auto-push
    // =========================================================================

    @Test
    void datestyle_change_pushed_via_ParameterStatus() throws SQLException {
        exec("SET DateStyle = 'ISO, MDY'");
        String v = ps("DateStyle");
        assertNotNull(v, "ParameterStatus DateStyle must be present");
        assertTrue(v.toUpperCase().contains("MDY"),
                "ParameterStatus must reflect SET DateStyle MDY; got '" + v + "'");
    }

    // =========================================================================
    // AD3. IntervalStyle auto-push
    // =========================================================================

    @Test
    void intervalstyle_change_pushed_via_ParameterStatus() throws SQLException {
        exec("SET IntervalStyle = 'iso_8601'");
        String v = ps("IntervalStyle");
        assertEquals("iso_8601", v,
                "ParameterStatus must reflect SET IntervalStyle; got '" + v + "'");
    }

    // =========================================================================
    // AD4. TimeZone auto-push
    // =========================================================================

    @Test
    void timezone_change_pushed_via_ParameterStatus() throws SQLException {
        exec("SET TimeZone = 'UTC'");
        String v = ps("TimeZone");
        assertEquals("UTC", v,
                "ParameterStatus must reflect SET TimeZone; got '" + v + "'");
    }

    // =========================================================================
    // AD5. client_encoding auto-push
    // =========================================================================

    @Test
    void client_encoding_change_pushed_via_ParameterStatus() throws SQLException {
        exec("SET client_encoding = 'UTF8'");
        String v = ps("client_encoding");
        assertEquals("UTF8", v,
                "ParameterStatus must reflect SET client_encoding; got '" + v + "'");
    }

    // =========================================================================
    // AD6. Startup burst completeness
    // =========================================================================

    @Test
    void startup_burst_includes_server_encoding() throws SQLException {
        String v = ps("server_encoding");
        assertNotNull(v,
                "Startup burst must include server_encoding ParameterStatus");
    }

    @Test
    void startup_burst_includes_standard_conforming_strings() throws SQLException {
        String v = ps("standard_conforming_strings");
        assertNotNull(v,
                "Startup burst must include standard_conforming_strings ParameterStatus");
    }

    @Test
    void startup_burst_includes_integer_datetimes() throws SQLException {
        String v = ps("integer_datetimes");
        assertNotNull(v,
                "Startup burst must include integer_datetimes ParameterStatus");
    }

    @Test
    void startup_burst_includes_is_superuser() throws SQLException {
        String v = ps("is_superuser");
        assertNotNull(v,
                "Startup burst must include is_superuser ParameterStatus");
    }

    @Test
    void startup_burst_includes_timezone() throws SQLException {
        String v = ps("TimeZone");
        assertNotNull(v,
                "Startup burst must include TimeZone ParameterStatus");
    }
}
