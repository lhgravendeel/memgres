package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests user-defined operator dispatch in expressions.
 *
 * PG 18: CREATE OPERATOR registers an operator backed by a function.
 * Using the operator in SQL dispatches to that function.
 *
 * Memgres: CREATE OPERATOR succeeds and populates pg_operator, but actually
 * USING the custom operator in an expression may not dispatch to the
 * implementing function. Built-in operators work; custom ones may silently
 * fail or error.
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres.
 */
class CustomOperatorDispatchTest {

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

    @BeforeEach
    void setup() throws SQLException {
        exec("DROP OPERATOR IF EXISTS +~ (integer, integer)");
        exec("DROP FUNCTION IF EXISTS custom_add_10(integer, integer)");

        exec("CREATE FUNCTION custom_add_10(a integer, b integer) RETURNS integer "
                + "LANGUAGE sql IMMUTABLE AS $$ SELECT a + b + 10 $$");
        exec("CREATE OPERATOR +~ ("
                + "LEFTARG = integer, RIGHTARG = integer, "
                + "FUNCTION = custom_add_10)");
    }

    // -------------------------------------------------------------------------
    // Custom operator should dispatch to implementing function
    // -------------------------------------------------------------------------

    @Test
    void customOperator_shouldDispatchToFunction() throws SQLException {
        // 3 +~ 4 should call custom_add_10(3, 4) = 3 + 4 + 10 = 17
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 3 +~ 4 AS result")) {
            assertTrue(rs.next());
            assertEquals(17, rs.getInt("result"),
                    "Custom operator +~ should dispatch to custom_add_10: 3+4+10=17");
        }
    }

    @Test
    void customOperator_inWhereClause() throws SQLException {
        exec("DROP TABLE IF EXISTS cop_test");
        exec("CREATE TABLE cop_test (id int, a int, b int)");
        exec("INSERT INTO cop_test VALUES (1, 3, 4), (2, 10, 20), (3, 1, 1)");

        // WHERE a +~ b > 30 -> custom_add_10(a,b) > 30
        // Row 1: 3+4+10=17 (no), Row 2: 10+20+10=40 (yes), Row 3: 1+1+10=12 (no)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id FROM cop_test WHERE a +~ b > 30")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertFalse(rs.next(), "Only row 2 should match");
        }

        exec("DROP TABLE cop_test");
    }

    @Test
    void customOperator_inOrderBy() throws SQLException {
        exec("DROP TABLE IF EXISTS cop_ord");
        exec("CREATE TABLE cop_ord (id int, a int, b int)");
        exec("INSERT INTO cop_ord VALUES (1, 5, 1), (2, 1, 1), (3, 3, 2)");

        // ORDER BY a +~ b: (5+1+10=16), (1+1+10=12), (3+2+10=15) -> order: 2,3,1
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id FROM cop_ord ORDER BY a +~ b")) {
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
        }

        exec("DROP TABLE cop_ord");
    }

    @Test
    void customOperator_withQualifiedSyntax() throws SQLException {
        // PG supports OPERATOR(schema.op) qualified syntax
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT 3 OPERATOR(public.+~) 4 AS result")) {
            assertTrue(rs.next());
            assertEquals(17, rs.getInt("result"),
                    "Qualified operator syntax should also dispatch correctly");
        }
    }

    // -------------------------------------------------------------------------
    // Unary prefix operator
    // -------------------------------------------------------------------------

    @Test
    void unaryOperator_shouldDispatchToFunction() throws SQLException {
        exec("DROP OPERATOR IF EXISTS !~ (NONE, integer)");
        exec("DROP FUNCTION IF EXISTS custom_negate_plus_100(integer)");

        exec("CREATE FUNCTION custom_negate_plus_100(a integer) RETURNS integer "
                + "LANGUAGE sql IMMUTABLE AS $$ SELECT -a + 100 $$");
        exec("CREATE OPERATOR !~ (RIGHTARG = integer, FUNCTION = custom_negate_plus_100)");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT !~ 42 AS result")) {
            assertTrue(rs.next());
            assertEquals(58, rs.getInt("result"),
                    "Unary operator !~ should call custom_negate_plus_100(42) = -42+100 = 58");
        }
    }

    // -------------------------------------------------------------------------
    // Operator with non-integer types
    // -------------------------------------------------------------------------

    @Test
    void customOperator_textTypes() throws SQLException {
        exec("DROP OPERATOR IF EXISTS %% (text, text)");
        exec("DROP FUNCTION IF EXISTS text_interleave(text, text)");

        exec("CREATE FUNCTION text_interleave(a text, b text) RETURNS text "
                + "LANGUAGE sql IMMUTABLE AS $$ SELECT a || '-' || b $$");
        exec("CREATE OPERATOR %% (LEFTARG = text, RIGHTARG = text, FUNCTION = text_interleave)");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'hello' %% 'world' AS result")) {
            assertTrue(rs.next());
            assertEquals("hello-world", rs.getString("result"),
                    "Custom text operator should concatenate with dash");
        }
    }
}
