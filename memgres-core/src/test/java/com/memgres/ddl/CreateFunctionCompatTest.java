package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CREATE FUNCTION syntax variants found in real-world schemas.
 */
class CreateFunctionCompatTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // RETURNS TABLE(...)
    // =========================================================================

    @Test
    void testReturnsTable() throws SQLException {
        exec("""
            CREATE FUNCTION get_user_info()
            RETURNS TABLE(user_id int, user_name text)
            LANGUAGE plpgsql AS $$
            BEGIN
                RETURN QUERY SELECT 1, 'Alice'::text;
            END;
            $$
        """);
        assertEquals("Alice", query1("SELECT user_name FROM get_user_info()"));
    }

    @Test
    void testReturnsTableMultipleColumns() throws SQLException {
        exec("CREATE TABLE fn_items (id serial PRIMARY KEY, name text, price numeric, category text)");
        exec("""
            CREATE FUNCTION items_by_category(cat text)
            RETURNS TABLE(item_id int, item_name text, item_price numeric)
            LANGUAGE plpgsql AS $$
            BEGIN
                RETURN QUERY SELECT id, name, price FROM fn_items WHERE category = cat;
            END;
            $$
        """);
    }

    // =========================================================================
    // RETURNS SETOF
    // =========================================================================

    @Test
    void testReturnsSetof() throws SQLException {
        exec("CREATE TABLE fn_records (id serial PRIMARY KEY, data text)");
        exec("""
            CREATE FUNCTION all_records()
            RETURNS SETOF fn_records
            LANGUAGE sql AS $$
                SELECT * FROM fn_records;
            $$
        """);
    }

    // =========================================================================
    // SECURITY DEFINER
    // =========================================================================

    @Test
    void testSecurityDefiner() throws SQLException {
        exec("""
            CREATE FUNCTION secure_function()
            RETURNS text
            LANGUAGE sql
            SECURITY DEFINER
            AS $$ SELECT 'secure'::text; $$
        """);
        assertEquals("secure", query1("SELECT secure_function()"));
    }

    @Test
    void testSecurityInvoker() throws SQLException {
        exec("""
            CREATE FUNCTION invoker_function()
            RETURNS text
            LANGUAGE sql
            SECURITY INVOKER
            AS $$ SELECT 'invoked'::text; $$
        """);
        assertEquals("invoked", query1("SELECT invoker_function()"));
    }

    // =========================================================================
    // SET search_path and other SET options
    // =========================================================================

    @Test
    void testFunctionSetSearchPath() throws SQLException {
        exec("""
            CREATE FUNCTION safe_func()
            RETURNS text
            LANGUAGE sql
            SECURITY DEFINER
            SET search_path TO 'pg_catalog', 'pg_temp'
            AS $$ SELECT 'safe'::text; $$
        """);
        assertEquals("safe", query1("SELECT safe_func()"));
    }

    @Test
    void testFunctionMultipleSetOptions() throws SQLException {
        exec("""
            CREATE FUNCTION configured_func()
            RETURNS void
            LANGUAGE plpgsql
            SET search_path TO 'public'
            SET statement_timeout TO '5s'
            AS $$
            BEGIN
                -- function body
            END;
            $$
        """);
    }

    // =========================================================================
    // IMMUTABLE / STABLE / VOLATILE
    // =========================================================================

    @Test
    void testImmutableFunction() throws SQLException {
        exec("""
            CREATE FUNCTION add_numbers(a int, b int)
            RETURNS int
            LANGUAGE sql
            IMMUTABLE
            AS $$ SELECT a + b; $$
        """);
        assertEquals("5", query1("SELECT add_numbers(2, 3)"));
    }

    @Test
    void testStableFunction() throws SQLException {
        exec("""
            CREATE FUNCTION current_setting_wrapper(key text)
            RETURNS text
            LANGUAGE sql
            STABLE
            AS $$ SELECT current_setting(key, true); $$
        """);
    }

    // =========================================================================
    // STRICT / RETURNS NULL ON NULL INPUT
    // =========================================================================

    @Test
    void testStrictFunction() throws SQLException {
        exec("""
            CREATE FUNCTION strict_double(val int)
            RETURNS int
            LANGUAGE sql
            STRICT
            AS $$ SELECT val * 2; $$
        """);
        assertEquals("10", query1("SELECT strict_double(5)"));
        // STRICT function with NULL input should return NULL
        assertEquals(null, query1("SELECT strict_double(NULL)"));
    }

    @Test
    void testReturnsNullOnNullInput() throws SQLException {
        exec("""
            CREATE FUNCTION null_safe(val int)
            RETURNS int
            LANGUAGE sql
            RETURNS NULL ON NULL INPUT
            AS $$ SELECT val + 1; $$
        """);
    }

    @Test
    void testCalledOnNullInput() throws SQLException {
        exec("""
            CREATE FUNCTION null_handler(val int)
            RETURNS int
            LANGUAGE sql
            CALLED ON NULL INPUT
            AS $$ SELECT COALESCE(val, 0) + 1; $$
        """);
        assertEquals("1", query1("SELECT null_handler(NULL)"));
    }

    // =========================================================================
    // Polymorphic parameter types (anyelement, anyarray, etc.)
    // =========================================================================

    @Test
    void testAnyelementParameter() throws SQLException {
        exec("""
            CREATE FUNCTION coalesce_default(val anyelement, fallback anyelement)
            RETURNS anyelement
            LANGUAGE sql
            IMMUTABLE
            AS $$ SELECT COALESCE(val, fallback); $$
        """);
    }

    @Test
    void testAnyarrayParameter() throws SQLException {
        exec("""
            CREATE FUNCTION array_first(arr anyarray)
            RETURNS anyelement
            LANGUAGE sql
            IMMUTABLE
            AS $$ SELECT arr[1]; $$
        """);
    }

    // =========================================================================
    // LANGUAGE sql (inline SQL functions)
    // =========================================================================

    @Test
    void testLanguageSqlFunction() throws SQLException {
        exec("""
            CREATE FUNCTION sql_add(a int, b int)
            RETURNS int
            LANGUAGE sql
            AS $$ SELECT a + b; $$
        """);
        assertEquals("7", query1("SELECT sql_add(3, 4)"));
    }

    // =========================================================================
    // DEFAULT parameter values
    // =========================================================================

    @Test
    void testFunctionDefaultParams() throws SQLException {
        exec("""
            CREATE FUNCTION greet(name text, greeting text DEFAULT 'Hello')
            RETURNS text
            LANGUAGE sql
            AS $$ SELECT greeting || ', ' || name || '!'; $$
        """);
        assertEquals("Hello, World!", query1("SELECT greet('World')"));
        assertEquals("Hi, World!", query1("SELECT greet('World', 'Hi')"));
    }

    // =========================================================================
    // OUT parameters
    // =========================================================================

    @Test
    void testOutParameters() throws SQLException {
        exec("""
            CREATE FUNCTION get_stats(OUT min_val int, OUT max_val int, OUT avg_val numeric)
            LANGUAGE sql AS $$
                SELECT 1, 100, 50.5::numeric;
            $$
        """);
    }

    @Test
    void testInOutParameter() throws SQLException {
        exec("""
            CREATE FUNCTION increment(INOUT val int, step int DEFAULT 1)
            LANGUAGE plpgsql AS $$
            BEGIN
                val := val + step;
            END;
            $$
        """);
    }

    // =========================================================================
    // PARALLEL SAFE / UNSAFE / RESTRICTED
    // =========================================================================

    @Test
    void testParallelSafe() throws SQLException {
        exec("""
            CREATE FUNCTION parallel_add(a int, b int)
            RETURNS int
            LANGUAGE sql
            IMMUTABLE
            PARALLEL SAFE
            AS $$ SELECT a + b; $$
        """);
    }

    @Test
    void testParallelRestricted() throws SQLException {
        exec("""
            CREATE FUNCTION restricted_func()
            RETURNS void
            LANGUAGE plpgsql
            PARALLEL RESTRICTED
            AS $$ BEGIN END; $$
        """);
    }

    // =========================================================================
    // COST / ROWS hints
    // =========================================================================

    @Test
    void testFunctionCostAndRows() throws SQLException {
        exec("""
            CREATE FUNCTION expensive_func(n int)
            RETURNS SETOF int
            LANGUAGE sql
            COST 1000
            ROWS 100
            AS $$ SELECT generate_series(1, n); $$
        """);
    }

    // =========================================================================
    // Schema-qualified function
    // =========================================================================

    @Test
    void testCreateFunctionInSchema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS lib");
        exec("""
            CREATE FUNCTION lib.helper()
            RETURNS text
            LANGUAGE sql
            AS $$ SELECT 'from_lib'::text; $$
        """);
        assertEquals("from_lib", query1("SELECT lib.helper()"));
    }

    // =========================================================================
    // CREATE OR REPLACE FUNCTION
    // =========================================================================

    @Test
    void testCreateOrReplaceFunction() throws SQLException {
        exec("""
            CREATE FUNCTION replaceable()
            RETURNS int LANGUAGE sql AS $$ SELECT 1; $$
        """);
        assertEquals("1", query1("SELECT replaceable()"));
        exec("""
            CREATE OR REPLACE FUNCTION replaceable()
            RETURNS int LANGUAGE sql AS $$ SELECT 2; $$
        """);
        assertEquals("2", query1("SELECT replaceable()"));
    }

    // =========================================================================
    // Trigger function (RETURNS trigger)
    // =========================================================================

    @Test
    void testTriggerFunction() throws SQLException {
        exec("""
            CREATE FUNCTION set_updated_at()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                NEW.updated_at := now();
                RETURN NEW;
            END;
            $$
        """);
    }

    // =========================================================================
    // Procedures (CREATE PROCEDURE)
    // =========================================================================

    @Test
    void testCreateProcedure() throws SQLException {
        exec("CREATE TABLE proc_log (msg text, ts timestamp DEFAULT now())");
        exec("""
            CREATE PROCEDURE log_message(message text)
            LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO proc_log (msg) VALUES (message);
            END;
            $$
        """);
        exec("CALL log_message('hello')");
        assertEquals("hello", query1("SELECT msg FROM proc_log"));
    }
}
