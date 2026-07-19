package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that timestamptz EXTRACT and date_trunc use session timezone (UTC),
 * not the stored offset from the literal.
 */
class TimestamptzExtractTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void extract_hour_converts_to_utc() throws Exception {
        // '2024-01-15 10:30:00+05' → in UTC is 05:30, so EXTRACT(HOUR) = 5
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(HOUR FROM TIMESTAMPTZ '2024-01-15 10:30:00+05') AS result");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("result"),
                    "EXTRACT(HOUR) should use session TZ (UTC), not stored offset");
        }
    }

    @Test void extract_day_crosses_midnight() throws Exception {
        // '2024-01-15 02:00:00+05' → UTC is 2024-01-14 21:00, so day = 14
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(DAY FROM TIMESTAMPTZ '2024-01-15 02:00:00+05') AS result");
            assertTrue(rs.next());
            assertEquals(14, rs.getInt("result"),
                    "EXTRACT(DAY) should reflect UTC date, not +05 date");
        }
    }

    @Test void extract_hour_negative_offset() throws Exception {
        // '2024-01-15 20:00:00-05' → UTC is 2024-01-16 01:00, hour = 1
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(HOUR FROM TIMESTAMPTZ '2024-01-15 20:00:00-05') AS result");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("result"),
                    "EXTRACT(HOUR) with negative offset should convert to UTC");
        }
    }

    @Test void date_trunc_hour_uses_utc() throws Exception {
        // '2024-01-15 10:30:00+05' → UTC is 05:30 → trunc to hour → 05:00 UTC
        // Result as timestamptz: '2024-01-15 05:00:00+00'
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT date_trunc('hour', TIMESTAMPTZ '2024-01-15 10:30:00+05') AS result");
            assertTrue(rs.next());
            java.time.OffsetDateTime odt = rs.getObject("result", java.time.OffsetDateTime.class);
            assertEquals(5, odt.atZoneSameInstant(java.time.ZoneOffset.UTC).getHour(),
                    "date_trunc('hour') should truncate in UTC");
        }
    }

    @Test void date_trunc_day_uses_utc() throws Exception {
        // '2024-01-15 02:00:00+05' → UTC is Jan 14 21:00 → trunc to day → Jan 14 00:00 UTC
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT date_trunc('day', TIMESTAMPTZ '2024-01-15 02:00:00+05') AS result");
            assertTrue(rs.next());
            java.time.OffsetDateTime odt = rs.getObject("result", java.time.OffsetDateTime.class);
            java.time.OffsetDateTime utc = odt.withOffsetSameInstant(java.time.ZoneOffset.UTC);
            assertEquals(14, utc.getDayOfMonth(),
                    "date_trunc('day') should truncate in UTC, giving Jan 14");
        }
    }

    @Test void extract_utc_offset_unchanged() throws Exception {
        // UTC input should be unchanged: '2024-01-15 10:30:00+00' → hour = 10
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(HOUR FROM TIMESTAMPTZ '2024-01-15 10:30:00+00') AS result");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("result"));
        }
    }
}
