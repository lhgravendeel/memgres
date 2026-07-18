package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that bigint bitwise and shift operators preserve 64-bit precision.
 * PG: 1::bigint << 40 = 1099511627776, not 0 (32-bit truncation).
 */
class BigintBitwiseTruncationTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void shift_left_beyond_32_bits() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1::bigint << 40 AS result");
            assertTrue(rs.next());
            assertEquals(1099511627776L, rs.getLong("result"));
        }
    }

    @Test void shift_right_from_high_bits() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            // 1099511627776 >> 20 = 1048576
            ResultSet rs = s.executeQuery("SELECT (1::bigint << 40) >> 20 AS result");
            assertTrue(rs.next());
            assertEquals(1048576L, rs.getLong("result"));
        }
    }

    @Test void bitwise_and_64bit() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT (1099511627776::bigint & 1099511627776::bigint) AS result");
            assertTrue(rs.next());
            assertEquals(1099511627776L, rs.getLong("result"));
        }
    }

    @Test void bitwise_or_64bit() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT (1::bigint | (1::bigint << 40)) AS result");
            assertTrue(rs.next());
            assertEquals(1099511627777L, rs.getLong("result"));
        }
    }

    @Test void bitwise_xor_64bit() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT (1099511627777::bigint # 1::bigint) AS result");
            assertTrue(rs.next());
            assertEquals(1099511627776L, rs.getLong("result"));
        }
    }

    @Test void int_operations_stay_32bit() throws Exception {
        // Regular int operations should still return int-range values
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT (255 & 15) AS result");
            assertTrue(rs.next());
            assertEquals(15, rs.getInt("result"));
        }
    }
}
