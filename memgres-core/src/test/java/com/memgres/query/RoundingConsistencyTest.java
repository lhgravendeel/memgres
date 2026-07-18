package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that numeric-to-integer casts and round() use PG's HALF_UP rounding.
 * PG: 2.5::int = 3, round(-2.5) = -3 (always rounds away from zero at midpoint).
 * Banker's rounding (HALF_EVEN) would give 2 and -2 respectively.
 */
class RoundingConsistencyTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void cast_2_5_to_int() throws Exception {
        // PG: 2.5::int = 3 (HALF_UP), not 2 (HALF_EVEN)
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 2.5::int AS result");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("result"));
        }
    }

    @Test void cast_3_5_to_int() throws Exception {
        // PG: 3.5::int = 4; banker's rounding would also give 4
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 3.5::int AS result");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("result"));
        }
    }

    @Test void cast_neg_2_5_to_int() throws Exception {
        // PG: (-2.5)::int = -3 (rounds away from zero)
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT (-2.5)::int AS result");
            assertTrue(rs.next());
            assertEquals(-3, rs.getInt("result"));
        }
    }

    @Test void cast_2_5_to_bigint() throws Exception {
        // PG: 2.5::bigint = 3 (should match ::int behavior)
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 2.5::bigint AS result");
            assertTrue(rs.next());
            assertEquals(3L, rs.getLong("result"));
        }
    }

    @Test void round_neg_2_5() throws Exception {
        // PG: round(-2.5) = -3, not -2
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT round(-2.5) AS result");
            assertTrue(rs.next());
            assertEquals(-3L, rs.getLong("result"));
        }
    }

    @Test void round_2_5() throws Exception {
        // PG: round(2.5) = 3
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT round(2.5) AS result");
            assertTrue(rs.next());
            assertEquals(3L, rs.getLong("result"));
        }
    }

    @Test void round_with_scale_consistent() throws Exception {
        // PG: round(2.55, 1) = 2.6
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT round(2.55, 1) AS result");
            assertTrue(rs.next());
            assertEquals(2.6, rs.getDouble("result"), 0.001);
        }
    }
}
