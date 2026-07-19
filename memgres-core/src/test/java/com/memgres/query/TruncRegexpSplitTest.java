package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that trunc(numeric, scale) uses BigDecimal (not double arithmetic),
 * and regexp_split_to_array preserves trailing empty strings.
 */
class TruncRegexpSplitTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void trunc_0_29_scale_2() throws Exception {
        // trunc(0.29, 2) should be 0.29, not 0.28 (double artifact: 0.29*100 = 28.999...)
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT trunc(0.29, 2) AS result");
            assertTrue(rs.next());
            assertEquals("0.29", rs.getString("result"));
        }
    }

    @Test void trunc_1_005_scale_2() throws Exception {
        // trunc(1.005, 2) → 1.00
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT trunc(1.005, 2) AS result");
            assertTrue(rs.next());
            assertEquals("1.00", rs.getString("result"));
        }
    }

    @Test void trunc_negative() throws Exception {
        // trunc(-0.29, 2) → -0.29
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT trunc(-0.29, 2) AS result");
            assertTrue(rs.next());
            assertEquals("-0.29", rs.getString("result"));
        }
    }

    @Test void trunc_no_scale() throws Exception {
        // trunc(2.7) → 2
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT trunc(2.7) AS result");
            assertTrue(rs.next());
            assertEquals("2", rs.getString("result"));
        }
    }

    @Test void regexp_split_trailing_empty() throws Exception {
        // PG: regexp_split_to_array('a,b,', ',') → {a,b,""}
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT regexp_split_to_array('a,b,', ',') AS result");
            assertTrue(rs.next());
            assertEquals("{a,b,\"\"}", rs.getString("result"));
        }
    }

    @Test void regexp_split_multiple_trailing() throws Exception {
        // PG: regexp_split_to_array('a,,', ',') → {a,"",""}
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT regexp_split_to_array('a,,', ',') AS result");
            assertTrue(rs.next());
            assertEquals("{a,\"\",\"\"}", rs.getString("result"));
        }
    }

    @Test void regexp_split_no_trailing() throws Exception {
        // Normal case: regexp_split_to_array('a,b,c', ',') → {a,b,c}
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT regexp_split_to_array('a,b,c', ',') AS result");
            assertTrue(rs.next());
            assertEquals("{a,b,c}", rs.getString("result"));
        }
    }
}
