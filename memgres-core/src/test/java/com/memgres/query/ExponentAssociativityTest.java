package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that ^ (exponentiation) is left-associative in PG.
 * PG: 2^3^2 = (2^3)^2 = 64, not 2^(3^2) = 512.
 */
class ExponentAssociativityTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void power_left_associative() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 2^3^2 AS result");
            assertTrue(rs.next());
            assertEquals(64.0, rs.getDouble("result"), 0.001, "2^3^2 should be (2^3)^2 = 64");
        }
    }

    @Test void power_explicit_left_grouping() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT (2^3)^2 AS result");
            assertTrue(rs.next());
            assertEquals(64.0, rs.getDouble("result"), 0.001);
        }
    }

    @Test void power_explicit_right_grouping_differs() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 2^(3^2) AS result");
            assertTrue(rs.next());
            assertEquals(512.0, rs.getDouble("result"), 0.001, "Explicit right grouping should give 512");
        }
    }

    @Test void triple_power() throws Exception {
        // 2^2^2^2 = ((2^2)^2)^2 = 4^2^2 = 16^2 = 256
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 2^2^2^2 AS result");
            assertTrue(rs.next());
            assertEquals(256.0, rs.getDouble("result"), 0.001);
        }
    }
}
