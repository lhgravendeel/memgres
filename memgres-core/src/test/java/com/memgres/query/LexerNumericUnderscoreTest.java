package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests PG 16+ numeric literal underscores, E-string octal/hex/unicode escapes,
 * and $ in identifiers.
 */
class LexerNumericUnderscoreTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void numeric_underscore_integer() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1_000_000 AS result");
            assertTrue(rs.next());
            assertEquals(1000000, rs.getInt("result"));
        }
    }

    @Test void numeric_underscore_float() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1_000.50 AS result");
            assertTrue(rs.next());
            assertEquals(1000.5, rs.getDouble("result"), 0.001);
        }
    }

    @Test void e_string_octal_escape() throws Exception {
        // E'\101' = 'A' (octal 101 = 65 = 'A')
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT E'\\101' AS result");
            assertTrue(rs.next());
            assertEquals("A", rs.getString("result"));
        }
    }

    @Test void e_string_hex_escape() throws Exception {
        // E'\x41' = 'A'
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT E'\\x41' AS result");
            assertTrue(rs.next());
            assertEquals("A", rs.getString("result"));
        }
    }

    @Test void e_string_unicode_escape() throws Exception {
        // E'\u0041' = 'A'
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT E'\\u0041' AS result");
            assertTrue(rs.next());
            assertEquals("A", rs.getString("result"));
        }
    }

    @Test void dollar_in_identifier() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t$1 (col$a int)");
            s.execute("INSERT INTO t$1 VALUES (42)");
            ResultSet rs = s.executeQuery("SELECT col$a FROM t$1");
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
            s.execute("DROP TABLE t$1");
        }
    }
}
