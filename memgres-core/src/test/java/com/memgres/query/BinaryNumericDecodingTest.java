package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.math.BigDecimal;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that binary NUMERIC decoding handles weight < -1 correctly.
 * PG: values like 0.00001 have weight=-2; these must not be decoded ~10^4 too large.
 */
class BinaryNumericDecodingTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        // Use extended query mode (default) to exercise binary codec
        return DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @Test void tiny_numeric_0_00001() throws Exception {
        try (Connection c = newConn(); PreparedStatement ps = c.prepareStatement("SELECT ?::numeric AS result")) {
            ps.setBigDecimal(1, new BigDecimal("0.00001"));
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, new BigDecimal("0.00001").compareTo(rs.getBigDecimal("result")),
                    "Expected 0.00001 but got " + rs.getBigDecimal("result"));
        }
    }

    @Test void tiny_numeric_0_0001() throws Exception {
        try (Connection c = newConn(); PreparedStatement ps = c.prepareStatement("SELECT ?::numeric AS result")) {
            ps.setBigDecimal(1, new BigDecimal("0.0001"));
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, new BigDecimal("0.0001").compareTo(rs.getBigDecimal("result")),
                    "Expected 0.0001 but got " + rs.getBigDecimal("result"));
        }
    }

    @Test void tiny_numeric_0_00000001() throws Exception {
        try (Connection c = newConn(); PreparedStatement ps = c.prepareStatement("SELECT ?::numeric AS result")) {
            ps.setBigDecimal(1, new BigDecimal("0.00000001"));
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, new BigDecimal("0.00000001").compareTo(rs.getBigDecimal("result")),
                    "Expected 0.00000001 but got " + rs.getBigDecimal("result"));
        }
    }

    @Test void negative_tiny_numeric() throws Exception {
        try (Connection c = newConn(); PreparedStatement ps = c.prepareStatement("SELECT ?::numeric AS result")) {
            ps.setBigDecimal(1, new BigDecimal("-0.00001"));
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, new BigDecimal("-0.00001").compareTo(rs.getBigDecimal("result")),
                    "Expected -0.00001 but got " + rs.getBigDecimal("result"));
        }
    }

    @Test void normal_numeric_unchanged() throws Exception {
        // Ensure normal values (weight >= 0) still work
        try (Connection c = newConn(); PreparedStatement ps = c.prepareStatement("SELECT ?::numeric AS result")) {
            ps.setBigDecimal(1, new BigDecimal("12345.6789"));
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, new BigDecimal("12345.6789").compareTo(rs.getBigDecimal("result")),
                    "Expected 12345.6789 but got " + rs.getBigDecimal("result"));
        }
    }

    @Test void small_fractional_0_001() throws Exception {
        // weight = -1, boundary case — should already work
        try (Connection c = newConn(); PreparedStatement ps = c.prepareStatement("SELECT ?::numeric AS result")) {
            ps.setBigDecimal(1, new BigDecimal("0.001"));
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, new BigDecimal("0.001").compareTo(rs.getBigDecimal("result")),
                    "Expected 0.001 but got " + rs.getBigDecimal("result"));
        }
    }
}
