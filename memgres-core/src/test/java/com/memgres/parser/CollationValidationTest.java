package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests collation name validation in SELECT ... COLLATE expressions.
 *
 * PG rejects unknown collations (including locale-dependent names like en_US.utf8
 * that are not installed on the host) with SQLSTATE 42704. C and "default" are
 * always available.
 */
class CollationValidationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    @Test
    void collation_unknownName_shouldError42704() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("SELECT 'a' COLLATE \"nonexistent_collation\""));
        assertEquals("42704", ex.getSQLState(),
                "Unknown collation should produce 42704; got: " + ex.getMessage());
    }

    @Test
    void collation_enUS_utf8_shouldError42704() {
        // en_US.utf8 is not guaranteed to exist — PG rejects it when not installed
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("SELECT 'a' COLLATE \"en_US.utf8\""));
        assertEquals("42704", ex.getSQLState(),
                "en_US.utf8 collation should not be accepted blindly; got: " + ex.getMessage());
    }

    @Test
    void collation_C_shouldWork() throws SQLException {
        // C/POSIX are always available
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'hello' COLLATE \"C\"")) {
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
        }
    }

    @Test
    void collation_default_shouldWork() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'hello' COLLATE \"default\"")) {
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
        }
    }
}
