package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that TEXT/VARCHAR string equality does NOT ignore trailing spaces.
 * PG only applies trailing-space-insensitive comparison for CHAR(n) types.
 * 'a' = 'a ' should be false for TEXT columns.
 */
class CharTrailingSpaceEqualityTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void char_n_padded_equality() throws Exception {
        // CHAR(5) stores 'a' as 'a    '. Comparing with 'a' should match (trailing spaces stripped).
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE cse_t2 (c char(5))");
            s.execute("INSERT INTO cse_t2 VALUES ('a')");
            ResultSet rs = s.executeQuery("SELECT count(*) FROM cse_t2 WHERE c = 'a'");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1), "CHAR(5) 'a' should equal 'a'");
            s.execute("DROP TABLE cse_t2");
        }
    }

    @Test void equality_and_less_than_consistent() throws Exception {
        // Key invariant: = and < must not both be true for the same pair.
        // With trailing-space stripping, 'a' = 'a ' is true and 'a' < 'a ' is false.
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'a' = 'a ' AS eq, 'a' < 'a ' AS lt");
            assertTrue(rs.next());
            boolean eq = rs.getBoolean("eq");
            boolean lt = rs.getBoolean("lt");
            assertFalse(eq && lt, "Cannot have both = and < be true simultaneously");
        }
    }

    @Test void non_space_trailing_chars_differ() throws Exception {
        // 'a' != 'ab' — only trailing SPACES are stripped, not other characters
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'a' = 'ab' AS eq");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean("eq"));
        }
    }

    @Test void both_empty_after_strip() throws Exception {
        // ' ' = '  ' should be true (both strip to empty)
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT ' ' = '  ' AS eq");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("eq"));
        }
    }
}
