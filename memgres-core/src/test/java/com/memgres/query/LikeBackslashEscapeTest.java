package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that LIKE uses backslash as the default escape character.
 * PG: '100%' LIKE '100\%' is true (backslash escapes the wildcard).
 * Without escape handling, \% is treated as literal backslash + wildcard.
 */
class LikeBackslashEscapeTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void escaped_percent() throws Exception {
        // PG: '100%' LIKE '100\%' is true
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '100%' LIKE '100\\%' AS result");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("result"));
        }
    }

    @Test void escaped_percent_no_match() throws Exception {
        // PG: '100x' LIKE '100\%' is false (literal % required)
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '100x' LIKE '100\\%' AS result");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean("result"));
        }
    }

    @Test void escaped_underscore() throws Exception {
        // PG: 'foo_bar' LIKE 'foo\_bar' is true
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'foo_bar' LIKE 'foo\\_bar' AS result");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("result"));
        }
    }

    @Test void escaped_underscore_no_match() throws Exception {
        // PG: 'fooxbar' LIKE 'foo\_bar' is false (literal _ required)
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'fooxbar' LIKE 'foo\\_bar' AS result");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean("result"));
        }
    }

    @Test void escaped_backslash() throws Exception {
        // PG: 'a\b' LIKE 'a\\b' is true (escaped backslash matches literal backslash)
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT E'a\\\\b' LIKE E'a\\\\\\\\b' AS result");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("result"));
        }
    }

    @Test void unescaped_percent_still_wildcard() throws Exception {
        // Regular % without escape should still be a wildcard
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'hello world' LIKE 'hello%' AS result");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("result"));
        }
    }

    @Test void not_like_with_escape() throws Exception {
        // NOT LIKE should also respect escape
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '100x' NOT LIKE '100\\%' AS result");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("result"));
        }
    }
}
