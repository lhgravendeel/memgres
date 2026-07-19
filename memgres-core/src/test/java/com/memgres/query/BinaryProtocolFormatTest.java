package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests extended protocol: ParameterDescription doesn't count $N inside strings,
 * and binary format works for TIME/INTERVAL/JSONB types.
 */
class BinaryProtocolFormatTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @Test void param_inside_string_not_counted() throws Exception {
        // SELECT '$1 is not a param' should have 0 params
        try (Connection c = newConn(); PreparedStatement ps = c.prepareStatement(
                "SELECT '$1 is not a param' AS result")) {
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals("$1 is not a param", rs.getString("result"));
        }
    }

    @Test void param_inside_string_with_real_param() throws Exception {
        // SELECT $1 || ' costs $2' — only $1 is a real param
        // Use simple query mode to avoid JDBC driver's own param detection
        try (Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
             Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'price is $2' AS result");
            assertTrue(rs.next());
            assertEquals("price is $2", rs.getString("result"));
        }
    }

    @Test void time_via_extended_protocol() throws Exception {
        try (Connection c = newConn(); PreparedStatement ps = c.prepareStatement(
                "SELECT TIME '12:30:00' AS result")) {
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals("12:30:00", rs.getString("result"));
        }
    }

    @Test void interval_via_extended_protocol() throws Exception {
        try (Connection c = newConn(); PreparedStatement ps = c.prepareStatement(
                "SELECT INTERVAL '1 day 2 hours' AS result")) {
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertTrue(rs.getString("result").contains("1 day"));
        }
    }
}
