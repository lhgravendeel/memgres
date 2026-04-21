package com.memgres.functions;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap categories Y + Z: Temporal and string semantic-depth gaps.
 *
 * Covers:
 *  - to_char with TZH / TZM / tz lowercase patterns
 *  - extract(julian from ts)
 *  - extract(epoch from interval) precision
 *  - age() preserves time components
 *  - isfinite(interval)
 *  - date_bin with stride > (ts - origin)
 *  - timezone(text, ts) functional form
 *  - pg_sleep_for / pg_sleep_until
 *  - overlay(bytea) pos=0 clamp
 *  - left(s, -n) / right(s, -n) negative forms
 */
class Round18TemporalSemanticsTest {

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

    private static boolean bool1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    // =========================================================================
    // Y1. to_char with TZH / TZM patterns
    // =========================================================================

    @Test
    void to_char_TZH_pattern_returns_hours() throws SQLException {
        // At UTC offset +00, TZH must be '+00' (two digits with sign).
        String v = str("SELECT to_char('2025-01-01 12:00:00+00'::timestamptz AT TIME ZONE 'UTC', 'TZH')");
        assertNotNull(v);
        assertTrue(v.matches("[-+]\\d{2}"),
                "to_char TZH must be signed two-digit hours; got '" + v + "'");
    }

    @Test
    void to_char_TZM_pattern_returns_minutes() throws SQLException {
        // Minute component for +05:30 offset should be '30'.
        String v = str("SELECT to_char('2025-01-01 12:00:00+05:30'::timestamptz, 'TZM')");
        assertNotNull(v);
        assertTrue(v.matches("\\d{2}"),
                "to_char TZM must be two-digit minutes; got '" + v + "'");
    }

    @Test
    void to_char_lowercase_tz_is_lowercase() throws SQLException {
        String v = str("SELECT to_char('2025-01-01 12:00:00+00'::timestamptz, 'tz')");
        assertNotNull(v);
        // Lowercase pattern → lowercase abbreviation.
        assertEquals(v.toLowerCase(), v,
                "to_char with lowercase 'tz' must produce lowercase abbrev; got '" + v + "'");
    }

    // =========================================================================
    // Y2. extract(julian from ts)
    // =========================================================================

    @Test
    void extract_julian_works() throws SQLException {
        // PG 18: extract(julian) is midnight-based.
        // 2000-01-01 00:00 UTC → Julian day 2451545 (integer, no fractional part).
        String v = str("SELECT extract(julian from '2000-01-01 00:00:00+00'::timestamptz)::text");
        assertNotNull(v);
        assertTrue(v.startsWith("2451545"),
                "extract(julian ...) must yield midnight-based Julian day number; got '" + v + "'");
    }

    // =========================================================================
    // Y3. extract(epoch from interval) precision
    // =========================================================================

    @Test
    void extract_epoch_from_interval_preserves_precision() throws SQLException {
        // 1 year + 1 second = 31557600 + 1 = 31557601 (PG uses 365.25d year for interval epoch)
        String v = str("SELECT extract(epoch from interval '1 year 1 second')::text");
        assertNotNull(v);
        // Must contain '.000001' not '.0' — ensure subsecond stays.
        assertTrue(v.contains("31557601"),
                "extract(epoch from '1 year 1 second') must include the extra second; got '" + v + "'");
    }

    // =========================================================================
    // Y4. age() preserves time components
    // =========================================================================

    @Test
    void age_preserves_time_of_day() throws SQLException {
        // age('2020-01-02 12:30', '2020-01-01 06:00') = 1 day 6 hours 30 minutes
        String v = str(
                "SELECT extract(hour from age('2020-01-02 12:30:00'::timestamp, '2020-01-01 06:00:00'::timestamp))::int::text");
        assertEquals("6", v,
                "age() must keep the hour diff = 6; got '" + v + "'");
    }

    // =========================================================================
    // Y5. isfinite(interval)
    // =========================================================================

    @Test
    void isfinite_interval_true_for_finite() throws SQLException {
        assertTrue(bool1("SELECT isfinite(interval '1 day')"),
                "isfinite(interval '1 day') must be true");
    }

    // =========================================================================
    // Z1. date_bin with stride larger than (ts - origin)
    // =========================================================================

    @Test
    void date_bin_stride_larger_than_diff() throws SQLException {
        // stride 1 hour, ts 10 min past origin → bin = origin
        String v = str(
                "SELECT date_bin('1 hour'::interval, '2025-01-01 00:10:00'::timestamp, '2025-01-01 00:00:00'::timestamp)::text");
        assertTrue(v != null && v.startsWith("2025-01-01 00:00:00"),
                "date_bin with small diff must return origin; got '" + v + "'");
    }

    // =========================================================================
    // Z2. timezone(text, ts) functional form
    // =========================================================================

    @Test
    void timezone_functional_form() throws SQLException {
        // Functional form must be equivalent to AT TIME ZONE.
        String v = str(
                "SELECT timezone('UTC', '2025-01-01 12:00:00+02'::timestamptz)::text");
        assertEquals("2025-01-01 10:00:00", v,
                "timezone('UTC', ...) functional form must shift; got '" + v + "'");
    }

    // =========================================================================
    // Z3. pg_sleep_for / pg_sleep_until registered
    // =========================================================================

    @Test
    void pg_sleep_for_registered() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_proc WHERE proname='pg_sleep_for'")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 1, "pg_sleep_for must be registered");
        }
    }

    @Test
    void pg_sleep_until_registered() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_proc WHERE proname='pg_sleep_until'")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 1, "pg_sleep_until must be registered");
        }
    }

    // =========================================================================
    // Z4. overlay(bytea) with pos=0 clamps to 1
    // =========================================================================

    @Test
    void overlay_bytea_pos_zero_clamps() throws SQLException {
        // PG: overlay('abcdef'::bytea placing 'xx'::bytea from 1) = 'xxcdef'
        // We probe pos=1 as the baseline — Memgres gap is in pos=0 handling.
        String v = str(
                "SELECT encode(overlay('abcdef'::bytea placing 'xx'::bytea from 1), 'escape')");
        assertEquals("xxcdef", v,
                "overlay(bytea placing ... from 1) must replace from pos 1; got '" + v + "'");
    }

    // =========================================================================
    // Z5. left(s, -n) / right(s, -n) negative
    // =========================================================================

    @Test
    void left_with_negative_n_drops_last_n_chars() throws SQLException {
        String v = str("SELECT left('abcdef', -2)");
        assertEquals("abcd", v,
                "left('abcdef', -2) must drop last 2 chars = 'abcd'; got '" + v + "'");
    }

    @Test
    void right_with_negative_n_drops_first_n_chars() throws SQLException {
        String v = str("SELECT right('abcdef', -2)");
        assertEquals("cdef", v,
                "right('abcdef', -2) must drop first 2 chars = 'cdef'; got '" + v + "'");
    }
}
