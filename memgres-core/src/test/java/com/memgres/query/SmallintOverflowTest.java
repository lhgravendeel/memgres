package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that out-of-range casts to smallint raise 22003 instead of wrapping,
 * and that real overflow also raises 22003 instead of returning Infinity.
 */
class SmallintOverflowTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void smallint_positive_overflow() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.executeQuery("SELECT 40000::smallint"));
            assertTrue(ex.getSQLState().equals("22003"), "Expected 22003 but got " + ex.getSQLState());
        }
    }

    @Test void smallint_negative_overflow() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.executeQuery("SELECT (-40000)::smallint"));
            assertTrue(ex.getSQLState().equals("22003"), "Expected 22003 but got " + ex.getSQLState());
        }
    }

    @Test void smallint_max_boundary() throws Exception {
        // 32767 is valid
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 32767::smallint AS result");
            assertTrue(rs.next());
            assertEquals(32767, rs.getInt("result"));
        }
    }

    @Test void smallint_just_over_max() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.executeQuery("SELECT 32768::smallint"));
            assertTrue(ex.getSQLState().equals("22003"), "Expected 22003 but got " + ex.getSQLState());
        }
    }

    @Test void real_overflow_raises_error() throws Exception {
        // 1e40 exceeds float range (~3.4e38), PG raises 22003
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.executeQuery("SELECT 1e40::real"));
            assertTrue(ex.getSQLState().equals("22003"), "Expected 22003 but got " + ex.getSQLState());
        }
    }

    @Test void real_normal_value() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1.5::real AS result");
            assertTrue(rs.next());
            assertEquals(1.5f, rs.getFloat("result"), 0.001f);
        }
    }
}
