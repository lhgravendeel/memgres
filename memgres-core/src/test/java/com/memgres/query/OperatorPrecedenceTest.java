package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that bitwise/shift/concat operators bind BELOW +/- in PG.
 * PG precedence (high to low): unary, ^, * / %, + -, other ops (& | # << >> ||), comparison.
 */
class OperatorPrecedenceTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void bitwise_and_below_addition() throws Exception {
        // PG: 2 & 3 + 1 = 2 & (3+1) = 2 & 4 = 0
        // Wrong: (2&3) + 1 = 2 + 1 = 3
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 2 & 3 + 1 AS result");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("result"), "2 & 3+1 should be 2 & 4 = 0");
        }
    }

    @Test void shift_left_below_addition() throws Exception {
        // PG: 2 + 8 << 1 = (2+8) << 1 = 10 << 1 = 20
        // Wrong: 2 + (8<<1) = 2 + 16 = 18
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 2 + 8 << 1 AS result");
            assertTrue(rs.next());
            assertEquals(20L, rs.getLong("result"), "2+8 << 1 should be (2+8)<<1 = 20");
        }
    }

    @Test void bitwise_or_below_subtraction() throws Exception {
        // PG: 7 | 8 - 1 = 7 | (8-1) = 7 | 7 = 7
        // Wrong: (7|8) - 1 = 15 - 1 = 14
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 7 | 8 - 1 AS result");
            assertTrue(rs.next());
            assertEquals(7, rs.getInt("result"));
        }
    }

    @Test void bitwise_xor_below_addition() throws Exception {
        // PG: 5 # 3 + 1 = 5 # (3+1) = 5 # 4 = 1
        // Wrong: (5#3) + 1 = 6 + 1 = 7
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 5 # 3 + 1 AS result");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("result"));
        }
    }

    @Test void shift_right_below_multiplication_is_above() throws Exception {
        // PG: 16 >> 2 * 1 = 16 >> (2*1) = 16 >> 2 = 4
        // * is above >>, so 2*1 is evaluated first — both correct
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 16 >> 2 * 1 AS result");
            assertTrue(rs.next());
            assertEquals(4L, rs.getLong("result"));
        }
    }

    @Test void concat_below_addition() throws Exception {
        // PG: 'a' || 'b' is concat, same precedence as bitwise ops (below +/-)
        // This mainly matters when mixed with casts, but test basic behavior
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'hello' || ' ' || 'world' AS result");
            assertTrue(rs.next());
            assertEquals("hello world", rs.getString("result"));
        }
    }
}
