package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that comparison operators can chain after IN/BETWEEN/LIKE.
 * PG: SELECT 1 IN (1,2) = false → (1 IN (1,2)) = false → true = false → false.
 * Bug: parseComparison returns after IN without checking for trailing = operator.
 */
class ComparisonChainingTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void in_then_equals_false() throws Exception {
        // PG: 1 IN (1,2) = false → false
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1 IN (1,2) = false AS result");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean("result"), "1 IN (1,2) = false should be false");
        }
    }

    @Test void in_then_equals_true() throws Exception {
        // PG: 1 IN (1,2) = true → true
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1 IN (1,2) = true AS result");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("result"));
        }
    }

    @Test void not_in_then_equals() throws Exception {
        // PG: 3 NOT IN (1,2) = true → true
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 3 NOT IN (1,2) = true AS result");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("result"));
        }
    }

    @Test void between_then_equals() throws Exception {
        // PG: 5 BETWEEN 1 AND 10 = true → true
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 5 BETWEEN 1 AND 10 = true AS result");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("result"));
        }
    }

    @Test void like_then_equals() throws Exception {
        // PG: 'abc' LIKE 'a%' = true → true
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'abc' LIKE 'a%' = true AS result");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("result"));
        }
    }

    @Test void in_then_not_equals() throws Exception {
        // PG: 1 IN (1,2) <> false → true
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1 IN (1,2) <> false AS result");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("result"));
        }
    }
}
