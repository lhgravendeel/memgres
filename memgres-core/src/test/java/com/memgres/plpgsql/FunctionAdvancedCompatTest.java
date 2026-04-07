package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CREATE FUNCTION edge cases that still fail.
 *
 * v1 compat covered basic RETURNS TABLE, SECURITY DEFINER, SET options.
 * These tests focus on remaining failures:
 * - OUT parameters with complex types (JSONB, TEXT[], etc.)
 * - LANGUAGE specified as string literal ('plpgsql') vs identifier
 * - Dollar-quoting with $$$ (triple-dollar)
 * - Schema-qualified function with OUT params
 * - Functions returning composite/record types
 * - Nested dollar-quoting ($fn$...$fn$ inside $$...$$)
 * - Functions with variadic parameters
 */
class FunctionAdvancedCompatTest {

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
    // OUT parameter with JSONB type
    // =========================================================================

    @Test
    void testOutParamJsonb() throws SQLException {
        exec("""
            CREATE OR REPLACE FUNCTION extract_config(
                source JSONB,
                path TEXT[],
                outt OUT jsonb
            )
            LANGUAGE plpgsql AS $$
            BEGIN
                outt := source #> path;
            END;
            $$
        """);
    }

    @Test
    void testOutParamTextArray() throws SQLException {
        exec("""
            CREATE FUNCTION split_csv(
                input text,
                result OUT text[]
            )
            LANGUAGE plpgsql AS $$
            BEGIN
                result := string_to_array(input, ',');
            END;
            $$
        """);
    }

    @Test
    void testOutParamWithAnyelement() throws SQLException {
        // Pattern: function with anyelement + OUT parameter
        exec("""
            CREATE FUNCTION remove_from_array(
                source JSONB,
                path TEXT[],
                value anyelement,
                outt OUT jsonb
            )
            LANGUAGE plpgsql AS $$
            BEGIN
                outt := source;
            END;
            $$
        """);
    }

    // =========================================================================
    // LANGUAGE as string literal ('plpgsql')
    // =========================================================================

    @Test
    void testLanguageAsStringLiteral() throws SQLException {
        // Some code generators output: language 'plpgsql' instead of LANGUAGE plpgsql
        exec("""
            CREATE FUNCTION string_lang_fn()
            RETURNS void
            language 'plpgsql' AS $$
            BEGIN
                -- noop
            END;
            $$
        """);
    }

    @Test
    void testLanguageSqlAsStringLiteral() throws SQLException {
        exec("""
            CREATE FUNCTION string_sql_fn()
            RETURNS int
            language 'sql' AS $$
                SELECT 42;
            $$
        """);
        assertEquals("42", query1("SELECT string_sql_fn()"));
    }

    // =========================================================================
    // Dollar-quoting with $$$ (triple-dollar)
    // =========================================================================

    @Test
    void testFunctionWithTripleDollarQuote() throws SQLException {
        // Some projects use $$$ as the dollar-quote delimiter
        // The JDBC driver and splitter need to handle this correctly
        exec("""
            CREATE FUNCTION triple_dollar_fn()
            RETURNS text
            LANGUAGE plpgsql AS $$$
            DECLARE
                result text;
            BEGIN
                result := 'from triple dollar';
                RETURN result;
            END;
            $$$
        """);
        assertEquals("from triple dollar", query1("SELECT triple_dollar_fn()"));
    }

    @Test
    void testFunctionWithTripleDollarAndDeclare() throws SQLException {
        exec("""
            CREATE FUNCTION complex_triple()
            RETURNS int
            LANGUAGE plpgsql AS $$$
            DECLARE
                val1 int := 10;
                val2 int := 20;
            BEGIN
                RETURN val1 + val2;
            END;
            $$$
        """);
        assertEquals("30", query1("SELECT complex_triple()"));
    }

    // =========================================================================
    // Nested dollar-quoting ($fn$ inside $$)
    // =========================================================================

    @Test
    void testNestedDollarQuoting() throws SQLException {
        // Pattern: DO $$ ... EXECUTE $fn$CREATE FUNCTION...$fn$ ... $$
        exec("""
            DO $$
            BEGIN
                EXECUTE $fn$
                    CREATE FUNCTION nested_fn() RETURNS text LANGUAGE sql AS $body$
                        SELECT 'nested'::text;
                    $body$
                $fn$;
            END;
            $$
        """);
        assertEquals("nested", query1("SELECT nested_fn()"));
    }

    // =========================================================================
    // Schema-qualified function creation
    // =========================================================================

    @Test
    void testSchemaQualifiedFunctionWithOutParam() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS lib");
        exec("""
            CREATE OR REPLACE FUNCTION lib.process_data(
                input JSONB,
                path TEXT[],
                result OUT jsonb
            )
            LANGUAGE plpgsql AS $$
            BEGIN
                result := input #> path;
            END;
            $$
        """);
    }

    // =========================================================================
    // Functions with VARIADIC parameters
    // =========================================================================

    @Test
    void testVariadicParameter() throws SQLException {
        exec("""
            CREATE FUNCTION concat_all(VARIADIC parts text[])
            RETURNS text
            LANGUAGE sql
            IMMUTABLE
            AS $$ SELECT array_to_string(parts, ', '); $$
        """);
        assertEquals("a, b, c", query1("SELECT concat_all('a', 'b', 'c')"));
    }

    @Test
    void testVariadicWithNonVariadicParam() throws SQLException {
        exec("""
            CREATE FUNCTION join_with(separator text, VARIADIC parts text[])
            RETURNS text
            LANGUAGE sql
            IMMUTABLE
            AS $$ SELECT array_to_string(parts, separator); $$
        """);
        assertEquals("a-b-c", query1("SELECT join_with('-', 'a', 'b', 'c')"));
    }

    // =========================================================================
    // Functions returning RECORD
    // =========================================================================

    @Test
    void testFunctionReturnsRecord() throws SQLException {
        exec("""
            CREATE FUNCTION get_pair()
            RETURNS RECORD
            LANGUAGE sql AS $$
                SELECT 1, 'hello'::text;
            $$
        """);
    }

    // =========================================================================
    // Functions with multiple RETURN QUERY
    // =========================================================================

    @Test
    void testMultipleReturnQuery() throws SQLException {
        exec("CREATE TABLE rq_data (id serial PRIMARY KEY, grp text, val int)");
        exec("""
            CREATE FUNCTION get_all_groups()
            RETURNS SETOF text
            LANGUAGE plpgsql AS $$
            BEGIN
                RETURN QUERY SELECT DISTINCT grp FROM rq_data WHERE val > 0;
                RETURN QUERY SELECT DISTINCT grp FROM rq_data WHERE val < 0;
            END;
            $$
        """);
    }

    // =========================================================================
    // Functions with EXECUTE ... INTO
    // =========================================================================

    @Test
    void testFunctionExecuteInto() throws SQLException {
        exec("CREATE TABLE exec_into_data (id serial PRIMARY KEY, val int)");
        exec("INSERT INTO exec_into_data (val) VALUES (42)");
        exec("""
            CREATE FUNCTION get_max_val(tbl text)
            RETURNS int
            LANGUAGE plpgsql AS $$
            DECLARE
                result int;
            BEGIN
                EXECUTE FORMAT('SELECT MAX(val) FROM %I', tbl) INTO result;
                RETURN result;
            END;
            $$
        """);
        assertEquals("42", query1("SELECT get_max_val('exec_into_data')"));
    }

    // =========================================================================
    // Functions with complex aggregate in body
    // =========================================================================

    @Test
    void testFunctionWithJsonbAgg() throws SQLException {
        exec("CREATE TABLE agg_data (id serial PRIMARY KEY, key text, value text)");
        exec("""
            CREATE FUNCTION build_config()
            RETURNS jsonb
            LANGUAGE sql AS $$
                SELECT COALESCE(jsonb_object_agg(key, value), '{}'::jsonb) FROM agg_data;
            $$
        """);
    }

    // =========================================================================
    // Functions with SET ... TO in function definition
    // =========================================================================

    @Test
    void testFunctionWithSetTo() throws SQLException {
        exec("""
            CREATE FUNCTION safe_query()
            RETURNS void
            LANGUAGE plpgsql
            SET search_path = 'public'
            SET statement_timeout TO '10s'
            AS $$
            BEGIN
                -- noop
            END;
            $$
        """);
    }

    @Test
    void testFunctionWithSetEquals() throws SQLException {
        // Some use = instead of TO
        exec("""
            CREATE FUNCTION safe_query2()
            RETURNS void
            LANGUAGE plpgsql
            SET search_path = 'pg_catalog, pg_temp'
            AS $$
            BEGIN
                -- noop
            END;
            $$
        """);
    }

    // =========================================================================
    // LEAKPROOF
    // =========================================================================

    @Test
    void testLeakproofFunction() throws SQLException {
        exec("""
            CREATE FUNCTION leakproof_fn(a int, b int)
            RETURNS boolean
            LANGUAGE sql
            IMMUTABLE LEAKPROOF
            AS $$ SELECT a = b; $$
        """);
    }
}
