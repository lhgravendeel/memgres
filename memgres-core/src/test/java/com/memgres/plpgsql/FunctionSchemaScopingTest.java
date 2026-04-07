package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diffs #8-11: Functions created after DROP SCHEMA CASCADE survive because
 * memgres creates functions in a global namespace. After DROP SCHEMA compat CASCADE,
 * the search_path still says "compat, pg_catalog" but compat doesn't exist.
 * PG refuses to create functions when the target schema doesn't exist.
 *
 * Also tests function overloading behavior.
 */
class FunctionSchemaScopingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }
    static String scalar(String sql) throws SQLException { try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) { return rs.next() ? rs.getString(1) : null; } }

    // Diffs #8-11: CREATE FUNCTION should fail when target schema doesn't exist
    @Test void create_function_fails_when_schema_dropped() throws SQLException {
        exec("CREATE SCHEMA fn_test");
        exec("SET search_path = fn_test, pg_catalog");
        exec("CREATE FUNCTION f_test(a int) RETURNS text LANGUAGE SQL AS $$ SELECT 'int' $$");
        assertEquals("int", scalar("SELECT f_test(1)"));
        exec("DROP SCHEMA fn_test CASCADE");
        // search_path still says fn_test but schema is gone
        // Creating a function should fail because there is no valid schema to create it in
        try {
            exec("CREATE FUNCTION f_over(a int) RETURNS text LANGUAGE SQL AS $$ SELECT 'int' $$");
            fail("CREATE FUNCTION should fail when search_path schema doesn't exist");
        } catch (SQLException e) {
            // Expected: schema doesn't exist
        } finally {
            exec("SET search_path = public");
        }
    }

    // Functions created in a schema should be dropped with the schema
    @Test void function_dropped_with_schema() throws SQLException {
        exec("CREATE SCHEMA fn_test2");
        exec("SET search_path = fn_test2");
        exec("CREATE FUNCTION fn_schema_test() RETURNS int LANGUAGE SQL AS $$ SELECT 42 $$");
        assertEquals("42", scalar("SELECT fn_schema_test()"));
        exec("DROP SCHEMA fn_test2 CASCADE");
        exec("SET search_path = public");
        // Function should be gone
        assertThrows(SQLException.class, () -> scalar("SELECT fn_schema_test()"),
            "Function should be dropped with schema");
    }

    // Function overloading: creating same-name function with different arg type
    // PG allows this only as true overloading (different signatures)
    @Test void function_overloading_different_arg_types() throws SQLException {
        exec("CREATE FUNCTION f_ol(a int) RETURNS text LANGUAGE SQL AS $$ SELECT 'int' $$");
        try {
            exec("CREATE FUNCTION f_ol(a text) RETURNS text LANGUAGE SQL AS $$ SELECT 'text' $$");
            // If overloading works, both should be callable
            assertEquals("int", scalar("SELECT f_ol(1)"));
            assertEquals("text", scalar("SELECT f_ol('x')"));
        } finally {
            try { exec("DROP FUNCTION f_ol(int)"); } catch (SQLException ignored) {}
            try { exec("DROP FUNCTION f_ol(text)"); } catch (SQLException ignored) {}
        }
    }

    // CREATE OR REPLACE on existing function with same signature should replace
    @Test void create_or_replace_function_replaces() throws SQLException {
        exec("CREATE FUNCTION f_rep(a int) RETURNS text LANGUAGE SQL AS $$ SELECT 'v1' $$");
        try {
            assertEquals("v1", scalar("SELECT f_rep(1)"));
            exec("CREATE OR REPLACE FUNCTION f_rep(a int) RETURNS text LANGUAGE SQL AS $$ SELECT 'v2' $$");
            assertEquals("v2", scalar("SELECT f_rep(1)"));
        } finally {
            exec("DROP FUNCTION f_rep(int)");
        }
    }
}
