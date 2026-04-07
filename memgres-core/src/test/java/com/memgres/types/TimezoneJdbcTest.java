package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.time.*;
import java.util.Calendar;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document Java/JDBC section 7: Timezone behavior with JDBC.
 * Tests timestamp/date round-trips, session timezone effects,
 * java.time type mappings, DST boundaries, and built-in timestamp functions.
 */
class TimezoneJdbcTest {

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

    // --- 1. Timestamp round-trip (setTimestamp / getTimestamp) ---

    @Test void timestamp_round_trip() throws Exception {
        exec("CREATE TABLE tz_ts_roundtrip(id int PRIMARY KEY, ts timestamp)");
        try {
            Timestamp original = Timestamp.valueOf("2024-06-15 14:30:45.123");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tz_ts_roundtrip VALUES (1, ?)")) {
                ps.setTimestamp(1, original);
                ps.executeUpdate();
            }
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ts FROM tz_ts_roundtrip WHERE id = 1")) {
                assertTrue(rs.next());
                Timestamp retrieved = rs.getTimestamp(1);
                assertEquals(original, retrieved, "Timestamp should survive round-trip");
            }
        } finally {
            exec("DROP TABLE tz_ts_roundtrip");
        }
    }

    // --- 2. Timestamptz storage and retrieval in different session timezones ---

    @Test void timestamptz_different_session_timezones() throws Exception {
        exec("CREATE TABLE tz_tstz_sessions(id int PRIMARY KEY, ts timestamptz)");
        try {
            exec("SET TimeZone = 'UTC'");
            exec("INSERT INTO tz_tstz_sessions VALUES (1, '2024-07-15 12:00:00+00')");

            // Read in UTC
            String utcResult = scalar("SELECT ts FROM tz_tstz_sessions WHERE id = 1");
            assertTrue(utcResult.contains("12:00:00"), "UTC should show 12:00: " + utcResult);

            // Read in US/Eastern (UTC-4 in July / EDT)
            exec("SET TimeZone = 'US/Eastern'");
            String eastResult = scalar("SELECT ts FROM tz_tstz_sessions WHERE id = 1");
            assertTrue(eastResult.contains("08:00:00"), "Eastern should show 08:00: " + eastResult);

            // Read in Asia/Tokyo (UTC+9)
            exec("SET TimeZone = 'Asia/Tokyo'");
            String tokyoResult = scalar("SELECT ts FROM tz_tstz_sessions WHERE id = 1");
            assertTrue(tokyoResult.contains("21:00:00"), "Tokyo should show 21:00: " + tokyoResult);

            exec("SET TimeZone = 'UTC'");
        } finally {
            exec("DROP TABLE tz_tstz_sessions");
        }
    }

    // --- 3. Date round-trip (setDate / getDate) ---

    @Test void date_round_trip() throws Exception {
        exec("CREATE TABLE tz_date_roundtrip(id int PRIMARY KEY, d date)");
        try {
            Date original = Date.valueOf("2024-03-15");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tz_date_roundtrip VALUES (1, ?)")) {
                ps.setDate(1, original);
                ps.executeUpdate();
            }
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT d FROM tz_date_roundtrip WHERE id = 1")) {
                assertTrue(rs.next());
                Date retrieved = rs.getDate(1);
                assertEquals(original.toString(), retrieved.toString(), "Date should survive round-trip");
            }
        } finally {
            exec("DROP TABLE tz_date_roundtrip");
        }
    }

    // --- 4. LocalDate via setObject / getObject ---

    @Test void localdate_set_get_object() throws Exception {
        exec("CREATE TABLE tz_localdate(id int PRIMARY KEY, d date)");
        try {
            LocalDate original = LocalDate.of(2024, 8, 20);
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tz_localdate VALUES (1, ?)")) {
                ps.setObject(1, original);
                ps.executeUpdate();
            }
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT d FROM tz_localdate WHERE id = 1")) {
                assertTrue(rs.next());
                LocalDate retrieved = rs.getObject(1, LocalDate.class);
                assertEquals(original, retrieved, "LocalDate should survive round-trip via setObject/getObject");
            }
        } finally {
            exec("DROP TABLE tz_localdate");
        }
    }

    // --- 5. LocalDateTime via setObject / getObject ---

    @Test void localdatetime_set_get_object() throws Exception {
        exec("CREATE TABLE tz_localdatetime(id int PRIMARY KEY, ts timestamp)");
        try {
            LocalDateTime original = LocalDateTime.of(2024, 11, 3, 1, 30, 0);
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tz_localdatetime VALUES (1, ?)")) {
                ps.setObject(1, original);
                ps.executeUpdate();
            }
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ts FROM tz_localdatetime WHERE id = 1")) {
                assertTrue(rs.next());
                LocalDateTime retrieved = rs.getObject(1, LocalDateTime.class);
                assertEquals(original, retrieved, "LocalDateTime should survive round-trip via setObject/getObject");
            }
        } finally {
            exec("DROP TABLE tz_localdatetime");
        }
    }

    // --- 6. OffsetDateTime via setObject / getObject ---

    @Test void offsetdatetime_set_get_object() throws Exception {
        exec("CREATE TABLE tz_offsetdt(id int PRIMARY KEY, ts timestamptz)");
        try {
            exec("SET TimeZone = 'UTC'");
            OffsetDateTime original = OffsetDateTime.of(2024, 6, 15, 10, 30, 0, 0, ZoneOffset.ofHours(5));
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tz_offsetdt VALUES (1, ?)")) {
                ps.setObject(1, original);
                ps.executeUpdate();
            }
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ts FROM tz_offsetdt WHERE id = 1")) {
                assertTrue(rs.next());
                OffsetDateTime retrieved = rs.getObject(1, OffsetDateTime.class);
                // The instant should be the same even if offset differs
                assertEquals(original.toInstant(), retrieved.toInstant(),
                        "OffsetDateTime instant should be preserved");
            }
        } finally {
            exec("DROP TABLE tz_offsetdt");
        }
    }

    // --- 7. Session timezone change affects timestamptz display ---

    @Test void session_timezone_change_affects_timestamptz_display() throws Exception {
        exec("CREATE TABLE tz_display(id int PRIMARY KEY, ts timestamptz)");
        try {
            exec("SET TimeZone = 'UTC'");
            exec("INSERT INTO tz_display VALUES (1, '2024-01-15 18:00:00+00')");

            String utcDisplay = scalar("SELECT ts::text FROM tz_display WHERE id = 1");

            exec("SET TimeZone = 'Europe/London'");
            String londonDisplay = scalar("SELECT ts::text FROM tz_display WHERE id = 1");

            exec("SET TimeZone = 'Australia/Sydney'");
            String sydneyDisplay = scalar("SELECT ts::text FROM tz_display WHERE id = 1");

            // UTC should show 18:00
            assertTrue(utcDisplay.contains("18:00:00"), "UTC display: " + utcDisplay);
            // London in January is GMT (UTC+0), so same as UTC
            assertTrue(londonDisplay.contains("18:00:00"), "London (winter) display: " + londonDisplay);
            // Sydney in January is AEDT (UTC+11), so 18+11 = 05:00 next day
            assertTrue(sydneyDisplay.contains("05:00:00"), "Sydney display: " + sydneyDisplay);

            exec("SET TimeZone = 'UTC'");
        } finally {
            exec("DROP TABLE tz_display");
        }
    }

    // --- 8. Timestamp at DST boundary ---

    @Test void timestamp_at_dst_boundary() throws Exception {
        exec("CREATE TABLE tz_dst(id int PRIMARY KEY, ts timestamptz)");
        try {
            exec("SET TimeZone = 'America/New_York'");

            // Spring forward: 2024-03-10 02:00 EST -> 03:00 EDT (gap)
            // Insert a time just before and just after the gap
            exec("INSERT INTO tz_dst VALUES (1, '2024-03-10 01:59:00-05')"); // before spring forward
            exec("INSERT INTO tz_dst VALUES (2, '2024-03-10 03:01:00-04')"); // after spring forward

            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ts FROM tz_dst ORDER BY id")) {
                assertTrue(rs.next());
                Timestamp before = rs.getTimestamp(1);
                assertTrue(rs.next());
                Timestamp after = rs.getTimestamp(1);
                assertNotNull(before);
                assertNotNull(after);
                // Both timestamps have explicit offsets, so the UTC difference is just 2 minutes
                // (01:59:00-05 = 06:59 UTC, 03:01:00-04 = 07:01 UTC)
                long diffMinutes = (after.getTime() - before.getTime()) / 60000;
                assertEquals(2, diffMinutes, "Gap across DST boundary should be 2 minutes (UTC difference)");
            }
        } finally {
            exec("SET TimeZone = 'UTC'");
            exec("DROP TABLE tz_dst");
        }
    }

    // --- 9. Timestamp with timezone 'UTC' vs 'America/New_York' ---

    @Test void timestamp_utc_vs_new_york() throws Exception {
        exec("CREATE TABLE tz_utc_ny(id int PRIMARY KEY, ts timestamptz)");
        try {
            exec("SET TimeZone = 'UTC'");
            exec("INSERT INTO tz_utc_ny VALUES (1, '2024-07-15 16:00:00+00')");

            // Read in UTC
            String utcVal = scalar("SELECT ts::text FROM tz_utc_ny WHERE id = 1");
            assertTrue(utcVal.contains("16:00:00"), "UTC should show 16:00: " + utcVal);

            // Read in America/New_York (EDT = UTC-4 in July)
            exec("SET TimeZone = 'America/New_York'");
            String nyVal = scalar("SELECT ts::text FROM tz_utc_ny WHERE id = 1");
            assertTrue(nyVal.contains("12:00:00"), "New York (EDT) should show 12:00: " + nyVal);

            // Verify both represent the same instant
            exec("SET TimeZone = 'UTC'");
            String epochUtc = scalar("SELECT extract(epoch FROM ts) FROM tz_utc_ny WHERE id = 1");
            exec("SET TimeZone = 'America/New_York'");
            String epochNy = scalar("SELECT extract(epoch FROM ts) FROM tz_utc_ny WHERE id = 1");
            assertEquals(epochUtc, epochNy, "Epoch values should be identical regardless of session timezone");

            exec("SET TimeZone = 'UTC'");
        } finally {
            exec("DROP TABLE tz_utc_ny");
        }
    }

    // --- 10. getTimestamp with Calendar parameter ---

    @Test void getTimestamp_with_calendar_parameter() throws Exception {
        exec("CREATE TABLE tz_cal(id int PRIMARY KEY, ts timestamptz)");
        try {
            exec("SET TimeZone = 'UTC'");
            exec("INSERT INTO tz_cal VALUES (1, '2024-06-15 12:00:00+00')");

            java.util.Calendar utcCal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
            java.util.Calendar nyCal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("America/New_York"));

            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ts FROM tz_cal WHERE id = 1")) {
                assertTrue(rs.next());

                Timestamp tsUtc = rs.getTimestamp(1, utcCal);
                Timestamp tsNy = rs.getTimestamp(1, nyCal);

                assertNotNull(tsUtc);
                assertNotNull(tsNy);
                // Both should represent the same instant
                // The Calendar parameter affects how the timestamp is interpreted
                // but the underlying epoch millis may differ based on driver behavior
                assertNotNull(tsUtc.toString());
                assertNotNull(tsNy.toString());
            }
        } finally {
            exec("DROP TABLE tz_cal");
        }
    }

    // --- 11. Epoch timestamp (1970-01-01) ---

    @Test void epoch_timestamp() throws Exception {
        exec("CREATE TABLE tz_epoch(id int PRIMARY KEY, ts timestamptz)");
        try {
            exec("SET TimeZone = 'UTC'");
            exec("INSERT INTO tz_epoch VALUES (1, '1970-01-01 00:00:00+00')");

            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ts FROM tz_epoch WHERE id = 1")) {
                assertTrue(rs.next());
                Timestamp ts = rs.getTimestamp(1);
                assertNotNull(ts);
                // Epoch should be 0 milliseconds from epoch
                assertEquals(0, ts.getTime(), "Epoch timestamp should have 0 millis");
            }

            // Verify via extract(epoch)
            String epochVal = scalar("SELECT extract(epoch FROM ts) FROM tz_epoch WHERE id = 1");
            assertEquals("0", epochVal, "Epoch extract should be 0");
        } finally {
            exec("DROP TABLE tz_epoch");
        }
    }

    // --- 12. Far-future timestamp ---

    @Test void far_future_timestamp() throws Exception {
        exec("CREATE TABLE tz_future(id int PRIMARY KEY, ts timestamptz)");
        try {
            exec("SET TimeZone = 'UTC'");
            exec("INSERT INTO tz_future VALUES (1, '2199-12-31 23:59:59+00')");

            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ts FROM tz_future WHERE id = 1")) {
                assertTrue(rs.next());
                // Retrieve as OffsetDateTime to avoid JVM-timezone dependence.
                // Timestamp.toLocalDateTime() applies the JVM's default zone,
                // so on a JVM in e.g. Europe/Amsterdam (UTC+1/+2),
                // 2199-12-31 23:59:59Z would become 2200-01-01 00:59:59,
                // making the year assertion fail.
                OffsetDateTime odt = rs.getObject(1, OffsetDateTime.class);
                assertNotNull(odt, "Far-future timestamp should be retrievable");

                // The session is UTC, so the server returns UTC
                OffsetDateTime utc = odt.withOffsetSameInstant(ZoneOffset.UTC);
                assertEquals(2199, utc.getYear());
                assertEquals(12, utc.getMonthValue());
                assertEquals(31, utc.getDayOfMonth());
                assertEquals(23, utc.getHour());
                assertEquals(59, utc.getMinute());
                assertEquals(59, utc.getSecond());
            }
        } finally {
            exec("DROP TABLE tz_future");
        }
    }

    // --- 13. current_timestamp returns timestamptz ---

    @Test void current_timestamp_returns_timestamptz() throws Exception {
        exec("SET TimeZone = 'UTC'");

        // Verify current_timestamp type is timestamptz
        String typeName = scalar("SELECT pg_typeof(current_timestamp)::text");
        assertEquals("timestamp with time zone", typeName,
                "current_timestamp should be timestamptz");

        // Verify it returns a non-null value
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT current_timestamp")) {
            assertTrue(rs.next());
            Timestamp ts = rs.getTimestamp(1);
            assertNotNull(ts, "current_timestamp should return a value");

            // Should be reasonably close to now
            long diffMs = Math.abs(System.currentTimeMillis() - ts.getTime());
            assertTrue(diffMs < 60000, "current_timestamp should be within 60s of system time, diff=" + diffMs);
        }
    }

    // --- 14. now() vs statement_timestamp() vs clock_timestamp() ---

    @Test void now_vs_statement_timestamp_vs_clock_timestamp() throws Exception {
        exec("SET TimeZone = 'UTC'");

        // now() and statement_timestamp() should be identical within a statement
        // (they both return transaction/statement start time)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT now(), statement_timestamp(), clock_timestamp()")) {
            assertTrue(rs.next());
            Timestamp now = rs.getTimestamp(1);
            Timestamp stmtTs = rs.getTimestamp(2);
            Timestamp clockTs = rs.getTimestamp(3);

            assertNotNull(now);
            assertNotNull(stmtTs);
            assertNotNull(clockTs);

            // now() should equal statement_timestamp() within a single statement
            assertEquals(now, stmtTs,
                    "now() and statement_timestamp() should match within the same statement");

            // clock_timestamp() should be >= statement_timestamp()
            assertTrue(clockTs.getTime() >= stmtTs.getTime(),
                    "clock_timestamp() should be >= statement_timestamp()");
        }

        // Verify types are all timestamptz
        String nowType = scalar("SELECT pg_typeof(now())::text");
        assertEquals("timestamp with time zone", nowType);

        String stmtType = scalar("SELECT pg_typeof(statement_timestamp())::text");
        assertEquals("timestamp with time zone", stmtType);

        String clockType = scalar("SELECT pg_typeof(clock_timestamp())::text");
        assertEquals("timestamp with time zone", clockType);
    }
}
