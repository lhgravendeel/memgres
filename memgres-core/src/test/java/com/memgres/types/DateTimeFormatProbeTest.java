package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests all PG-documented date/time input formats.
 */
class DateTimeFormatProbeTest {

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

    // =========================================================================
    // A. DATE INPUT FORMATS
    // =========================================================================

    @Test void date_iso() throws Exception {
        assertEquals("1999-01-08", scalar("SELECT DATE '1999-01-08'"));
    }

    @Test void date_iso_with_slashes() throws Exception {
        assertEquals("1999-01-08", scalar("SELECT DATE '1999/01/08'"));
    }

    @Test void date_us_mdy() throws Exception {
        assertEquals("1999-01-08", scalar("SELECT DATE '01/08/1999'"));
    }

    @Test void date_compact_yyyymmdd() throws Exception {
        assertEquals("1999-01-08", scalar("SELECT DATE '19990108'"));
    }

    @Test void date_named_month_long() throws Exception {
        assertEquals("1999-01-08", scalar("SELECT DATE 'January 8, 1999'"));
    }

    @Test void date_named_month_short() throws Exception {
        assertEquals("1999-01-08", scalar("SELECT DATE 'Jan 8, 1999'"));
    }

    @Test void date_named_month_iso() throws Exception {
        assertEquals("1999-01-08", scalar("SELECT DATE '1999-Jan-08'"));
    }

    @Test void date_special_epoch() throws Exception {
        assertEquals("1970-01-01", scalar("SELECT DATE 'epoch'"));
    }

    @Test void date_special_infinity() throws Exception {
        assertEquals("infinity", scalar("SELECT DATE 'infinity'"));
    }

    @Test void date_special_neg_infinity() throws Exception {
        assertEquals("-infinity", scalar("SELECT DATE '-infinity'"));
    }

    @Test void date_special_today() throws Exception {
        assertNotNull(scalar("SELECT DATE 'today'"));
    }

    @Test void date_special_yesterday() throws Exception {
        assertNotNull(scalar("SELECT DATE 'yesterday'"));
    }

    @Test void date_special_tomorrow() throws Exception {
        assertNotNull(scalar("SELECT DATE 'tomorrow'"));
    }

    @Test void date_julian() throws Exception {
        assertEquals("2000-01-01", scalar("SELECT DATE 'J2451545'"));
    }

    // =========================================================================
    // B. TIME INPUT FORMATS
    // =========================================================================

    @Test void time_hms() throws Exception {
        assertEquals("04:05:06", scalar("SELECT TIME '04:05:06'"));
    }

    @Test void time_hms_microseconds() throws Exception {
        assertEquals("04:05:06.789", scalar("SELECT TIME '04:05:06.789'"));
    }

    @Test void time_hm_no_seconds() throws Exception {
        assertEquals("04:05:00", scalar("SELECT TIME '04:05'"));
    }

    @Test void time_special_allballs() throws Exception {
        assertEquals("00:00:00", scalar("SELECT TIME 'allballs'"));
    }

    // =========================================================================
    // C. TIMESTAMP INPUT FORMATS
    // =========================================================================

    @Test void timestamp_space_separator() throws Exception {
        assertEquals("2024-01-08 04:05:06", scalar("SELECT TIMESTAMP '2024-01-08 04:05:06'"));
    }

    @Test void timestamp_t_separator() throws Exception {
        assertEquals("2024-01-08 04:05:06", scalar("SELECT TIMESTAMP '2024-01-08T04:05:06'"));
    }

    @Test void timestamp_special_epoch() throws Exception {
        assertEquals("1970-01-01 00:00:00", scalar("SELECT TIMESTAMP 'epoch'"));
    }

    @Test void timestamp_special_now() throws Exception {
        assertNotNull(scalar("SELECT TIMESTAMP 'now'"));
    }

    @Test void timestamp_special_today() throws Exception {
        String r = scalar("SELECT TIMESTAMP 'today'");
        assertTrue(r.endsWith("00:00:00"), "today should be midnight: " + r);
    }

    @Test void timestamp_special_yesterday() throws Exception {
        assertNotNull(scalar("SELECT TIMESTAMP 'yesterday'"));
    }

    @Test void timestamp_special_tomorrow() throws Exception {
        assertNotNull(scalar("SELECT TIMESTAMP 'tomorrow'"));
    }

    // =========================================================================
    // D. TIMESTAMPTZ OFFSET FORMATS
    // =========================================================================

    @Test void timestamptz_offset_hhmm_colon() throws Exception {
        exec("SET TimeZone = 'UTC'");
        assertEquals("2024-01-08 04:05:06+00",
                scalar("SELECT '2024-01-08 04:05:06+00:00'::timestamptz::text"));
    }

    @Test void timestamptz_offset_hh_only() throws Exception {
        exec("SET TimeZone = 'UTC'");
        assertEquals("2024-01-08 04:05:06+00",
                scalar("SELECT '2024-01-08 04:05:06+00'::timestamptz::text"));
    }

    @Test void timestamptz_offset_hhmm_no_colon() throws Exception {
        exec("SET TimeZone = 'UTC'");
        assertEquals("2024-01-08 04:05:06+00",
                scalar("SELECT '2024-01-08 04:05:06+0000'::timestamptz::text"));
    }

    @Test void timestamptz_offset_z() throws Exception {
        exec("SET TimeZone = 'UTC'");
        assertEquals("2024-01-08 04:05:06+00",
                scalar("SELECT '2024-01-08T04:05:06Z'::timestamptz::text"));
    }

    @Test void timestamptz_space_before_offset() throws Exception {
        exec("SET TimeZone = 'UTC'");
        assertEquals("2024-01-08 04:05:06+00",
                scalar("SELECT '2024-01-08 04:05:06 +00:00'::timestamptz::text"));
    }

    @Test void timestamptz_named_tz_abbreviation() throws Exception {
        exec("SET TimeZone = 'UTC'");
        assertEquals("2024-01-08 12:05:06+00",
                scalar("SELECT '2024-01-08 04:05:06 PST'::timestamptz::text"));
    }

    @Test void timestamptz_named_tz_region() throws Exception {
        exec("SET TimeZone = 'UTC'");
        assertNotNull(scalar("SELECT '2024-01-08 04:05:06 Europe/Amsterdam'::timestamptz::text"));
    }

    @Test void timestamptz_special_epoch() throws Exception {
        exec("SET TimeZone = 'UTC'");
        assertEquals("1970-01-01 00:00:00+00",
                scalar("SELECT TIMESTAMPTZ 'epoch'"));
    }

    @Test void timestamptz_special_now() throws Exception {
        exec("SET TimeZone = 'UTC'");
        assertNotNull(scalar("SELECT TIMESTAMPTZ 'now'"));
    }

    // =========================================================================
    // E. TIMETZ FORMATS
    // =========================================================================

    @Test void timetz_with_offset() throws Exception {
        assertNotNull(scalar("SELECT TIMETZ '04:05:06+02'"));
    }

    @Test void timetz_with_offset_hhmm() throws Exception {
        assertNotNull(scalar("SELECT TIMETZ '04:05:06+02:00'"));
    }

    @Test void timetz_z_suffix() throws Exception {
        assertNotNull(scalar("SELECT TIMETZ '04:05:06Z'"));
    }

    // =========================================================================
    // F. INTERVAL FORMATS
    // =========================================================================

    @Test void interval_postgres_verbose() throws Exception {
        assertEquals("1 year 2 mons 3 days 04:05:06",
                scalar("SELECT INTERVAL '1 year 2 months 3 days 4 hours 5 minutes 6 seconds'"));
    }

    @Test void interval_negative() throws Exception {
        assertEquals("-1 days", scalar("SELECT INTERVAL '-1 day'"));
    }

    @Test void interval_iso8601_duration() throws Exception {
        assertEquals("1 year 2 mons 3 days 04:05:06",
                scalar("SELECT INTERVAL 'P1Y2M3DT4H5M6S'"));
    }
}
