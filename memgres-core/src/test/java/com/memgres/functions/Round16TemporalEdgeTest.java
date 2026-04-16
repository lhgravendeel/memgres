package com.memgres.functions;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category E: Temporal edge cases.
 *
 * Covers:
 *  - date_trunc 3-arg form (field, source, timezone) (PG 12+)
 *  - INTERVAL YEAR TO MONTH / DAY TO SECOND qualifiers
 *  - to_char TZ / OF / TZH / TZM patterns
 *  - timestamp 'infinity' arithmetic preserves infinity
 *  - extract(timezone / timezone_hour / timezone_minute) uses actual offset
 *  - TIMETZ arithmetic respects offset
 *  - Year 0000 rejection
 */
class Round16TemporalEdgeTest {

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

    private static BigDecimal dec(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBigDecimal(1);
        }
    }

    // =========================================================================
    // E1. date_trunc 3-arg timezone form (PG 12+)
    // =========================================================================

    @Test
    void date_trunc_with_timezone() throws SQLException {
        // date_trunc('day', ts, 'UTC') truncates in UTC
        String v = str("SELECT date_trunc('day', '2025-06-15 23:30:00+00'::timestamptz, 'UTC')::text");
        assertNotNull(v);
        assertTrue(v.contains("2025-06-15"),
                "date_trunc('day', ts, 'UTC') must truncate to 2025-06-15; got: " + v);
    }

    // =========================================================================
    // E2. INTERVAL YEAR TO MONTH / DAY TO SECOND qualifiers
    // =========================================================================

    @Test
    void interval_year_to_month_qualifier_accepted() throws SQLException {
        // '1-3' with YEAR TO MONTH qualifier → 1 year 3 months
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT INTERVAL '1-3' YEAR TO MONTH")) {
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            assertTrue(v.contains("1 year") && v.contains("3 mon"),
                    "INTERVAL '1-3' YEAR TO MONTH must parse as 1 year 3 months; got: " + v);
        }
    }

    @Test
    void interval_day_to_second_qualifier_accepted() throws SQLException {
        // '2 04:05:06' with DAY TO SECOND
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT INTERVAL '2 04:05:06' DAY TO SECOND")) {
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            assertTrue(v.contains("2 day") && v.contains("04:05:06"),
                    "INTERVAL '2 04:05:06' DAY TO SECOND must parse; got: " + v);
        }
    }

    // =========================================================================
    // E3. to_char timezone pattern letters
    // =========================================================================

    @Test
    void to_char_tz_pattern_emits_abbreviation() throws SQLException {
        String v = str("SELECT to_char('2025-06-15 12:00:00+00'::timestamptz, 'TZ')");
        assertNotNull(v);
        // Should not emit the literal "TZ"
        assertNotEquals("TZ", v,
                "to_char(ts, 'TZ') must format the timezone, not emit literal 'TZ'");
    }

    @Test
    void to_char_of_pattern_emits_offset() throws SQLException {
        String v = str("SELECT to_char('2025-06-15 12:00:00+00'::timestamptz, 'OF')");
        assertNotNull(v);
        assertNotEquals("OF", v,
                "to_char(ts, 'OF') must format the UTC offset (e.g., '+00'), not emit literal 'OF'");
    }

    // =========================================================================
    // E4. timestamp 'infinity' arithmetic preserves infinity
    // =========================================================================

    @Test
    void timestamp_infinity_plus_interval_stays_infinity() throws SQLException {
        String v = str("SELECT ('infinity'::timestamp + interval '1 day')::text");
        assertEquals("infinity", v,
                "'infinity'::timestamp + interval must remain 'infinity' (not a sentinel year 9999)");
    }

    // =========================================================================
    // E5. extract(timezone*) returns the actual offset
    // =========================================================================

    @Test
    void extract_timezone_returns_offset_seconds() throws SQLException {
        BigDecimal v = dec("SELECT extract(timezone FROM '2025-06-15 12:00:00+05'::timestamptz)");
        assertNotNull(v);
        assertEquals(0, new BigDecimal("18000").compareTo(v),
                "extract(timezone FROM ts) must return offset in seconds (+05 → 18000)");
    }

    @Test
    void extract_timezone_hour_returns_hours() throws SQLException {
        BigDecimal v = dec("SELECT extract(timezone_hour FROM '2025-06-15 12:00:00+05:30'::timestamptz)");
        assertNotNull(v);
        assertEquals(0, new BigDecimal("5").compareTo(v),
                "extract(timezone_hour) must return 5 for +05:30");
    }

    @Test
    void extract_timezone_minute_returns_minutes() throws SQLException {
        BigDecimal v = dec("SELECT extract(timezone_minute FROM '2025-06-15 12:00:00+05:30'::timestamptz)");
        assertNotNull(v);
        assertEquals(0, new BigDecimal("30").compareTo(v),
                "extract(timezone_minute) must return 30 for +05:30");
    }

    // =========================================================================
    // E6. TIMETZ arithmetic / comparison uses offset-aware ordering
    // =========================================================================

    @Test
    void timetz_comparison_respects_offset() throws SQLException {
        // 12:00:00+05 == 07:00:00+00 (UTC wall-clock)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT '12:00:00+05'::timetz = '07:00:00+00'::timetz")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1),
                    "TIMETZ equality must compare UTC-wall-clock: '12:00:00+05' = '07:00:00+00'");
        }
    }

    // =========================================================================
    // E7. Year 0000 is not a valid date
    // =========================================================================

    @Test
    void year_0000_rejected() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT '0000-01-01'::date")) {
            if (rs.next()) {
                String got = rs.getString(1);
                fail("'0000-01-01'::date must be rejected (no year 0 in PG); got " + got);
            }
        } catch (SQLException e) {
            // Expected: "date/time field value out of range" or similar
            assertTrue(e.getMessage() != null && e.getMessage().toLowerCase().contains("year")
                            || "22008".equals(e.getSQLState()) || "22007".equals(e.getSQLState()),
                    "Rejection must reference year/date out of range; got " + e.getSQLState() + " " + e.getMessage());
        }
    }
}
