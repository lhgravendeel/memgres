package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that statement splitting handles dollar-quoted bodies with apostrophes
 * and E-string backslash escapes correctly.
 */
class StatementSplittingTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void dollar_body_with_apostrophe() throws Exception {
        // The apostrophe inside $$ body must not affect statement splitting
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            // Create a function whose body contains apostrophes
            s.execute("CREATE OR REPLACE FUNCTION greet(name text) RETURNS text AS $$ " +
                    "BEGIN RETURN 'Hello, ' || name || '!'; END; $$ LANGUAGE plpgsql");
            ResultSet rs = s.executeQuery("SELECT greet('world') AS result");
            assertTrue(rs.next());
            assertEquals("Hello, world!", rs.getString("result"));
        }
    }

    @Test void dollar_body_with_multiple_apostrophes() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            // Two statements: function with apostrophes in body, then a call
            s.execute("CREATE OR REPLACE FUNCTION apost_test() RETURNS text AS $$ " +
                    "BEGIN RETURN 'it''s a test'; END; $$ LANGUAGE plpgsql; " +
                    "SELECT 1");
            // If splitting worked, the function was created and SELECT 1 ran separately
            ResultSet rs = s.executeQuery("SELECT apost_test() AS result");
            assertTrue(rs.next());
            assertEquals("it's a test", rs.getString("result"));
        }
    }

    @Test void e_string_with_backslash_escape() throws Exception {
        // E'...\'..' uses backslash escaping — the \' must not end the string
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT E'it\\'s' AS result");
            assertTrue(rs.next());
            assertEquals("it's", rs.getString("result"));
        }
    }

    @Test void e_string_with_backslash_in_multi_statement() throws Exception {
        // Multi-statement: E-string with backslash, then another statement
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            // The backslash-escaped quote inside E'' must not break splitting
            boolean hasResult = s.execute("SELECT E'can\\'t stop' AS r1; SELECT 42 AS r2");
            assertTrue(hasResult);
            ResultSet rs = s.getResultSet();
            assertTrue(rs.next());
            assertEquals("can't stop", rs.getString("r1"));
        }
    }

    @Test void e_string_with_backslash_backslash() throws Exception {
        // E'\\' is a literal backslash, not an escape of the closing quote
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT E'\\\\' AS result");
            assertTrue(rs.next());
            assertEquals("\\", rs.getString("result"));
        }
    }

    @Test void normal_strings_still_work() throws Exception {
        // Regular strings with doubled apostrophes should still work
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'it''s fine' AS result");
            assertTrue(rs.next());
            assertEquals("it's fine", rs.getString("result"));
        }
    }
}
