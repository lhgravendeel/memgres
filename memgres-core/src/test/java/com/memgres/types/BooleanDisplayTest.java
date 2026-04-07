package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 8: Boolean wire display. PG sends booleans as 't'/'f' over the wire.
 * Tests that boolean values stored in tables (including after ALTER TYPE conversion)
 * display correctly as 't'/'f', not 'true'/'false'.
 */
class BooleanDisplayTest {

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

    // ========================================================================
    // Direct boolean column display
    // ========================================================================

    @Test
    void boolean_column_displays_t_f() throws SQLException {
        exec("CREATE TABLE bool_disp (id INT, flag BOOLEAN)");
        try {
            exec("INSERT INTO bool_disp VALUES (1, true), (2, false), (3, NULL)");
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT flag FROM bool_disp ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("t", rs.getString(1), "true should display as 't'");
                assertTrue(rs.next());
                assertEquals("f", rs.getString(1), "false should display as 'f'");
                assertTrue(rs.next());
                assertNull(rs.getString(1), "NULL should be null");
            }
        } finally {
            exec("DROP TABLE bool_disp");
        }
    }

    // ========================================================================
    // Boolean after ALTER TABLE ADD COLUMN with conversion
    // ========================================================================

    @Test
    void boolean_after_alter_type_conversion() throws SQLException {
        // This mirrors the convert_t pattern from the verification suite:
        // A table with an integer column that gets ALTER TYPE'd to boolean
        exec("CREATE TABLE bool_conv (id INT, val INT, amt INT)");
        exec("INSERT INTO bool_conv VALUES (1, 1, 10), (2, 0, 20)");
        try {
            exec("ALTER TABLE bool_conv ALTER COLUMN val TYPE boolean USING val::int::boolean");
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT id, val, amt FROM bool_conv ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("1", rs.getString("id"));
                assertEquals("t", rs.getString("val"), "Converted boolean should display as 't'");
                assertEquals("10", rs.getString("amt"));
                assertTrue(rs.next());
                assertEquals("2", rs.getString("id"));
                assertEquals("f", rs.getString("val"), "Converted boolean should display as 'f'");
                assertEquals("20", rs.getString("amt"));
            }
        } finally {
            exec("DROP TABLE bool_conv");
        }
    }

    // ========================================================================
    // Boolean expression results
    // ========================================================================

    @Test
    void boolean_expression_displays_t_f() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT true, false, 1 = 1, 1 = 2")) {
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
            assertEquals("f", rs.getString(2));
            assertEquals("t", rs.getString(3));
            assertEquals("f", rs.getString(4));
        }
    }

    // ========================================================================
    // Boolean in aggregate/complex queries
    // ========================================================================

    @Test
    void boolean_in_case_expression() throws SQLException {
        exec("CREATE TABLE bool_case (id INT, val INT)");
        exec("INSERT INTO bool_case VALUES (1, 10), (2, -5)");
        try {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                     "SELECT id, CASE WHEN val > 0 THEN true ELSE false END AS positive FROM bool_case ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("t", rs.getString("positive"));
                assertTrue(rs.next());
                assertEquals("f", rs.getString("positive"));
            }
        } finally {
            exec("DROP TABLE bool_case");
        }
    }

    @Test
    void boolean_in_subquery() throws SQLException {
        exec("CREATE TABLE bool_sub (id INT, active BOOLEAN)");
        exec("INSERT INTO bool_sub VALUES (1, true), (2, false)");
        try {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                     "SELECT id, (SELECT active FROM bool_sub b WHERE b.id = a.id) AS flag " +
                     "FROM bool_sub a ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("t", rs.getString("flag"));
                assertTrue(rs.next());
                assertEquals("f", rs.getString("flag"));
            }
        } finally {
            exec("DROP TABLE bool_sub");
        }
    }
}
