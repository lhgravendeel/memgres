package com.memgres.functions;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for generate_series with timestamp/timestamptz/date arguments,
 * including now()-based ranges, descending intervals, and FROM clause usage.
 */
class GenerateSeriesTimestampTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- now()-based (OffsetDateTime) ----

    @Test
    void generate_series_now_interval() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                "SELECT count(*) FROM generate_series(now() - interval '1 hour', now(), interval '1 minute') AS gs");
            assertTrue(rs.next());
            assertEquals(61, rs.getInt(1));
        }
    }

    @Test
    void generate_series_now_in_select() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                "SELECT generate_series(now() - interval '3 hours', now(), interval '1 hour')");
            int count = 0;
            while (rs.next()) { count++; assertNotNull(rs.getTimestamp(1)); }
            assertEquals(4, count);
        }
    }

    // ---- Typed TIMESTAMP literals ----

    @Test
    void generate_series_typed_timestamp() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                "SELECT gs FROM generate_series(TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 03:00:00', INTERVAL '1 hour') AS gs");
            int count = 0;
            while (rs.next()) { count++; assertNotNull(rs.getTimestamp(1)); }
            assertEquals(4, count);
        }
    }

    // ---- Typed DATE literals ----

    @Test
    void generate_series_typed_date() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                "SELECT gs FROM generate_series(DATE '2026-01-01', DATE '2026-01-05', INTERVAL '1 day') AS gs");
            int count = 0;
            while (rs.next()) { count++; assertNotNull(rs.getObject(1)); }
            assertEquals(5, count);
        }
    }

    // ---- Typed TIMESTAMPTZ literals ----

    @Test
    void generate_series_typed_timestamptz() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                "SELECT gs FROM generate_series('2026-06-01 00:00:00+00'::timestamptz, '2026-06-01 03:00:00+00'::timestamptz, interval '1 hour') AS gs");
            int count = 0;
            while (rs.next()) { count++; assertNotNull(rs.getTimestamp(1)); }
            assertEquals(4, count);
        }
    }

    // ---- String timestamps with interval step (disambiguation) ----

    @Test
    void generate_series_string_timestamps_with_interval() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                "SELECT count(*) FROM generate_series('2026-01-01', '2026-01-05', interval '1 day') AS gs");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    // ---- Descending series ----

    @Test
    void generate_series_descending_date() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                "SELECT gs FROM generate_series(DATE '2026-01-05', DATE '2026-01-01', INTERVAL '-1 day') AS gs");
            int count = 0;
            while (rs.next()) { count++; assertNotNull(rs.getObject(1)); }
            assertEquals(5, count);
        }
    }

    @Test
    void generate_series_descending_timestamp() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                "SELECT gs FROM generate_series(TIMESTAMP '2026-01-01 03:00:00', TIMESTAMP '2026-01-01 00:00:00', INTERVAL '-1 hour') AS gs");
            int count = 0;
            while (rs.next()) { count++; assertNotNull(rs.getTimestamp(1)); }
            assertEquals(4, count);
        }
    }

    // ---- Integer baseline (should still work) ----

    @Test
    void generate_series_integer_baseline() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT generate_series(1, 5)");
            int count = 0;
            while (rs.next()) { count++; }
            assertEquals(5, count);
        }
    }
}
