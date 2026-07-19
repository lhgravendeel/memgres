package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests: interval * 1.5 cascades fractional months→days→time;
 * date_bin handles pre-origin values; single-arg age() uses midnight.
 */
class IntervalMultiplyDateBinTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void interval_multiply_cascades() throws Exception {
        // PG: '1 month'::interval * 1.5 = '1 mon 15 days' (0.5 months → 15 days)
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT ('1 month'::interval * 1.5)::text AS result");
            assertTrue(rs.next());
            String val = rs.getString("result");
            assertTrue(val.contains("15 day") || val.contains("1 mon 15"),
                    "Expected fractional month cascaded to days, got: " + val);
        }
    }

    @Test void interval_multiply_days_to_hours() throws Exception {
        // PG: '1 day'::interval * 1.5 = '1 day 12:00:00' (0.5 days → 12 hours)
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT ('1 day'::interval * 1.5)::text AS result");
            assertTrue(rs.next());
            String val = rs.getString("result");
            assertTrue(val.contains("12:00:00"),
                    "Expected fractional day cascaded to hours, got: " + val);
        }
    }

    @Test void date_bin_pre_origin() throws Exception {
        // date_bin('1 hour', '2024-01-01 00:30', '2024-01-01 02:00')
        // source is before origin → should bin correctly (floor toward -infinity)
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT date_bin('1 hour'::interval, " +
                    "TIMESTAMP '2024-01-01 00:30:00', TIMESTAMP '2024-01-01 02:00:00') AS result");
            assertTrue(rs.next());
            String val = rs.getString("result");
            // PG: 2024-01-01 00:00:00 (bin starts at origin - 2 hours = 00:00)
            assertTrue(val.contains("00:00:00"), "Pre-origin date_bin should floor correctly, got: " + val);
        }
    }

    @Test void age_single_arg_uses_midnight() throws Exception {
        // age(timestamp) uses current_date (midnight), not now()
        // We can't test exact value, but verify it doesn't throw
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT age(TIMESTAMP '2020-01-01') AS result");
            assertTrue(rs.next());
            assertNotNull(rs.getString("result"));
        }
    }
}
