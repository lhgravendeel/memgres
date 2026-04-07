package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for stored procedure and function creation/validation gaps.
 *
 * Covers:
 * - CREATE PROCEDURE referencing non-existent table (should fail with 42P01)
 * - CALL non-existent procedure (should fail with 42883)
 * - Function overload resolution (pg_catalog schema protection)
 * - Bad function body validation (empty SELECT, type mismatches)
 * - CREATE OR REPLACE semantics
 * - DROP FUNCTION/PROCEDURE edge cases
 */
class ProcedureFunctionGapsTest {

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

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    // ========================================================================
    // CREATE PROCEDURE referencing non-existent table
    // ========================================================================

    @Test
    void create_procedure_referencing_nonexistent_table_fails() {
        // PG validates the procedure body at creation time.
        // INSERT INTO proc_t when proc_t doesn't exist → 42P01 (undefined_table)
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("""
                    CREATE OR REPLACE PROCEDURE p_ins_noexist(i int, t text)
                    LANGUAGE SQL AS $$
                        INSERT INTO nonexistent_proc_table VALUES (i, t)
                    $$
                    """),
                "CREATE PROCEDURE referencing non-existent table should fail");

        assertEquals("42P01", ex.getSQLState(),
                "Should be 42P01 (undefined_table), got " + ex.getSQLState());
    }

    @Test
    void call_nonexistent_procedure_fails() {
        // CALL a procedure that was never created → 42883 (undefined_function)
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("CALL nonexistent_proc(1, 'a')"),
                "CALL non-existent procedure should fail");

        assertEquals("42883", ex.getSQLState(),
                "Should be 42883 (undefined_function), got " + ex.getSQLState());
    }

    @Test
    void create_procedure_then_call_succeeds() throws SQLException {
        exec("CREATE TABLE proc_ok_t(id int, val text)");
        try {
            exec("""
                CREATE OR REPLACE PROCEDURE p_insert_ok(i int, t text)
                LANGUAGE SQL AS $$
                    INSERT INTO proc_ok_t VALUES (i, t)
                $$
                """);

            exec("CALL p_insert_ok(1, 'hello')");

            List<List<String>> rows = query("SELECT * FROM proc_ok_t");
            assertEquals(1, rows.size(), "Procedure should have inserted 1 row");
            assertEquals("1", rows.get(0).get(0));
            assertEquals("hello", rows.get(0).get(1));
        } finally {
            try { exec("DROP PROCEDURE IF EXISTS p_insert_ok"); } catch (SQLException ignored) {}
            exec("DROP TABLE proc_ok_t");
        }
    }

    @Test
    void create_procedure_after_table_exists_succeeds() throws SQLException {
        // First attempt without table should fail, then create table, then procedure should succeed
        assertThrows(SQLException.class,
                () -> exec("""
                    CREATE PROCEDURE p_late(i int)
                    LANGUAGE SQL AS $$ INSERT INTO late_t VALUES (i) $$
                    """));

        exec("CREATE TABLE late_t(id int)");
        try {
            exec("""
                CREATE PROCEDURE p_late(i int)
                LANGUAGE SQL AS $$ INSERT INTO late_t VALUES (i) $$
                """);
            exec("CALL p_late(42)");
            assertEquals("1", scalar("SELECT count(*) FROM late_t"));
        } finally {
            try { exec("DROP PROCEDURE IF EXISTS p_late"); } catch (SQLException ignored) {}
            exec("DROP TABLE late_t");
        }
    }

    // ========================================================================
    // Function overload resolution and pg_catalog protection
    // ========================================================================

    @Test
    void function_overload_same_name_different_args() throws SQLException {
        exec("""
            CREATE FUNCTION f_over_test(a int) RETURNS int
            LANGUAGE SQL AS $$ SELECT a * 2 $$
            """);
        try {
            // Creating another function with same name but different arg type
            // In PG, this creates an overload (unless in pg_catalog)
            // PG may restrict overloading depending on schema
            exec("""
                CREATE FUNCTION f_over_test(a text) RETURNS text
                LANGUAGE SQL AS $$ SELECT 'text:' || a $$
                """);

            // Call with int
            assertEquals("20", scalar("SELECT f_over_test(10)"));
            // Call with text
            assertEquals("text:hello", scalar("SELECT f_over_test('hello')"));
        } finally {
            try { exec("DROP FUNCTION IF EXISTS f_over_test(int)"); } catch (SQLException ignored) {}
            try { exec("DROP FUNCTION IF EXISTS f_over_test(text)"); } catch (SQLException ignored) {}
        }
    }

    @Test
    void create_function_in_pg_catalog_fails() {
        // PG: creating functions in pg_catalog requires superuser and is generally blocked
        // Error: 42501 (insufficient_privilege) or 42723 (duplicate_function)
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("""
                    CREATE FUNCTION pg_catalog.my_func(a int) RETURNS int
                    LANGUAGE SQL AS $$ SELECT a $$
                    """),
                "Creating function in pg_catalog should fail");

        // PG returns 42501 (insufficient_privilege)
        String state = ex.getSQLState();
        assertTrue("42501".equals(state) || "42723".equals(state),
                "Should be 42501 or 42723, got " + state);
    }

    @Test
    void function_overload_call_with_null() throws SQLException {
        exec("CREATE FUNCTION f_null_test(a text) RETURNS text LANGUAGE SQL AS $$ SELECT coalesce(a, 'null') $$");
        try {
            // PG: f_null_test(NULL) should resolve to the text overload
            String val = scalar("SELECT f_null_test(NULL)");
            assertEquals("null", val, "NULL should resolve and use coalesce");
        } finally {
            try { exec("DROP FUNCTION f_null_test"); } catch (SQLException ignored) {}
        }
    }

    // ========================================================================
    // Bad function body validation
    // ========================================================================

    @Test
    void function_with_empty_select_body_fails() {
        // CREATE FUNCTION badf(a int) RETURNS int LANGUAGE SQL AS $$ SELECT $$
        // PG: should fail because empty SELECT is not valid as a function body return.
        // Expected error: 42P13 (invalid_function_definition), indicating return type mismatch
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("""
                    CREATE FUNCTION badf_empty(a int) RETURNS int
                    LANGUAGE SQL AS $$ SELECT $$
                    """),
                "Function with empty SELECT body should fail");

        assertEquals("42P13", ex.getSQLState(),
                "Empty SELECT body should be 42P13 (invalid_function_definition), got " + ex.getSQLState());
    }

    @Test
    void function_with_wrong_return_type_fails() {
        // Function declared to return int but body returns text
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("""
                    CREATE FUNCTION badf_type(a int) RETURNS int
                    LANGUAGE SQL AS $$ SELECT 'not an int' $$
                    """),
                "Function with wrong return type should fail");

        // PG: 42P13 (invalid_function_definition)
        assertNotNull(ex.getSQLState());
    }

    @Test
    void function_returning_void_with_select_fails() {
        // RETURNS void but body has SELECT; should fail
        assertThrows(SQLException.class,
                () -> exec("""
                    CREATE FUNCTION badf_void() RETURNS void
                    LANGUAGE SQL AS $$ SELECT 1 $$
                    """),
                "RETURNS void with SELECT body should fail");
    }

    @Test
    void function_with_valid_body_succeeds() throws SQLException {
        exec("""
            CREATE FUNCTION f_valid(a int, b int) RETURNS int
            LANGUAGE SQL AS $$ SELECT a + b $$
            """);
        try {
            assertEquals("5", scalar("SELECT f_valid(2, 3)"));
        } finally {
            exec("DROP FUNCTION f_valid");
        }
    }

    // ========================================================================
    // CREATE OR REPLACE semantics
    // ========================================================================

    @Test
    void create_or_replace_function_updates_body() throws SQLException {
        exec("""
            CREATE FUNCTION f_replace(a int) RETURNS int
            LANGUAGE SQL AS $$ SELECT a * 2 $$
            """);
        try {
            assertEquals("10", scalar("SELECT f_replace(5)"));

            // Replace with new body
            exec("""
                CREATE OR REPLACE FUNCTION f_replace(a int) RETURNS int
                LANGUAGE SQL AS $$ SELECT a * 3 $$
                """);

            assertEquals("15", scalar("SELECT f_replace(5)"),
                    "OR REPLACE should update the function body");
        } finally {
            exec("DROP FUNCTION f_replace");
        }
    }

    @Test
    void create_function_without_replace_duplicate_fails() throws SQLException {
        exec("""
            CREATE FUNCTION f_dup(a int) RETURNS int
            LANGUAGE SQL AS $$ SELECT a $$
            """);
        try {
            // CREATE without OR REPLACE on existing function → error
            assertThrows(SQLException.class,
                    () -> exec("""
                        CREATE FUNCTION f_dup(a int) RETURNS int
                        LANGUAGE SQL AS $$ SELECT a + 1 $$
                        """),
                    "CREATE without OR REPLACE on existing function should fail");
        } finally {
            exec("DROP FUNCTION f_dup");
        }
    }

    // ========================================================================
    // DROP FUNCTION / PROCEDURE edge cases
    // ========================================================================

    @Test
    void drop_function_if_exists_no_error() throws SQLException {
        // DROP IF EXISTS on non-existent function should not error
        exec("DROP FUNCTION IF EXISTS nonexistent_func_xyz(int)");
    }

    @Test
    void drop_procedure_if_exists_no_error() throws SQLException {
        exec("DROP PROCEDURE IF EXISTS nonexistent_proc_xyz(int, text)");
    }

    @Test
    void drop_nonexistent_function_fails() {
        assertThrows(SQLException.class,
                () -> exec("DROP FUNCTION nonexistent_func_abc(int)"),
                "DROP non-existent function without IF EXISTS should fail");
    }

    // ========================================================================
    // Procedure with multiple statements
    // ========================================================================

    @Test
    void procedure_with_multiple_statements() throws SQLException {
        exec("CREATE TABLE multi_proc_t(id int, val text)");
        try {
            exec("""
                CREATE PROCEDURE p_multi(i int, t text)
                LANGUAGE SQL AS $$
                    INSERT INTO multi_proc_t VALUES (i, t);
                    INSERT INTO multi_proc_t VALUES (i + 100, t || '_copy');
                $$
                """);

            exec("CALL p_multi(1, 'test')");

            List<List<String>> rows = query("SELECT * FROM multi_proc_t ORDER BY id");
            assertEquals(2, rows.size(), "Procedure should insert 2 rows");
            assertEquals("1", rows.get(0).get(0));
            assertEquals("101", rows.get(1).get(0));
            assertEquals("test_copy", rows.get(1).get(1));
        } finally {
            try { exec("DROP PROCEDURE IF EXISTS p_multi"); } catch (SQLException ignored) {}
            exec("DROP TABLE multi_proc_t");
        }
    }

    // ========================================================================
    // Function call with wrong argument count/type
    // ========================================================================

    @Test
    void function_call_with_wrong_arg_count() throws SQLException {
        exec("CREATE FUNCTION f_argc(a int) RETURNS int LANGUAGE SQL AS $$ SELECT a $$");
        try {
            assertThrows(SQLException.class,
                    () -> scalar("SELECT f_argc(1, 2)"),
                    "Too many arguments should fail");

            assertThrows(SQLException.class,
                    () -> scalar("SELECT f_argc()"),
                    "Too few arguments should fail");
        } finally {
            exec("DROP FUNCTION f_argc");
        }
    }

    @Test
    void function_call_with_type_coercion() throws SQLException {
        exec("CREATE FUNCTION f_coerce(a int) RETURNS int LANGUAGE SQL AS $$ SELECT a * 2 $$");
        try {
            // PG coerces text '5' to int in function call
            String val = scalar("SELECT f_coerce('5')");
            assertEquals("10", val, "PG should coerce text to int for function args");
        } finally {
            exec("DROP FUNCTION f_coerce");
        }
    }
}
