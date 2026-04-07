package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diffs #8, #9: CREATE FUNCTION after DROP SCHEMA should return SQLSTATE 42501
 * (insufficient_privilege), not 3F000 (invalid_schema_name).
 * PG returns 42501 because the user can't create functions in pg_catalog.
 */
class FunctionSchemaErrorCodeTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // Diff #8: CREATE FUNCTION after schema dropped; PG returns 42501
    @Test void create_function_after_schema_drop_sqlstate() throws SQLException {
        exec("CREATE SCHEMA fn_err_test");
        exec("SET search_path = fn_err_test, pg_catalog");
        exec("DROP SCHEMA fn_err_test CASCADE");
        // search_path now points to non-existent fn_err_test + pg_catalog
        // PG: can't create in pg_catalog → 42501 (insufficient_privilege)
        try {
            exec("CREATE FUNCTION f_over(a int) RETURNS text LANGUAGE SQL AS $$ SELECT 'int' $$");
            fail("Should fail after schema dropped");
        } catch (SQLException e) {
            assertEquals("42501", e.getSQLState(),
                "Should be 42501 (insufficient_privilege), got " + e.getSQLState());
        } finally {
            exec("SET search_path = public");
        }
    }

    // Diff #9: Second CREATE FUNCTION same pattern
    @Test void create_second_function_same_state() throws SQLException {
        exec("CREATE SCHEMA fn_err_test2");
        exec("SET search_path = fn_err_test2, pg_catalog");
        exec("DROP SCHEMA fn_err_test2 CASCADE");
        try {
            exec("CREATE FUNCTION f_over(a text) RETURNS text LANGUAGE SQL AS $$ SELECT 'text' $$");
            fail("Should fail after schema dropped");
        } catch (SQLException e) {
            assertEquals("42501", e.getSQLState(),
                "Should be 42501, got " + e.getSQLState());
        } finally {
            exec("SET search_path = public");
        }
    }
}
