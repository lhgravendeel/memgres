package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 3 plpgsql-functions.sql failures where Memgres diverges from PG 18.
 *
 * Stmt 31: SELECT fn_bad_sql_body()  -- SQLSTATE 42P01 expected (relation not found), Memgres gives 42883
 * Stmt 68: SELECT fn_set_clause()    -- should return 'pg_catalog', Memgres returns NULL
 * Stmt 114: SELECT fn_to_drop('hello') -- should return 'hello', Memgres errors with 42883
 */
class PlpgsqlFunctionsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );

        try (Statement s = conn.createStatement()) {
            // Schema setup
            s.execute("DROP SCHEMA IF EXISTS fn_test CASCADE");
            s.execute("CREATE SCHEMA fn_test");
            s.execute("SET search_path = fn_test, public");

            // --- Setup for Stmt 31 (fn_bad_sql_body) ---
            // The first CREATE (without check_function_bodies off) should fail; ignore that error.
            // Then set check_function_bodies off so the invalid SQL-language function can be created.
            s.execute("SET check_function_bodies = off");
            s.execute("""
                CREATE FUNCTION fn_bad_sql_body() RETURNS integer LANGUAGE sql AS $$
                  SELECT x FROM fn_no_such_table_sql
                $$
            """);
            s.execute("SET check_function_bodies = on");

            // --- Setup for Stmt 68 (fn_set_clause) ---
            s.execute("""
                CREATE FUNCTION fn_set_clause() RETURNS text LANGUAGE plpgsql
                SET search_path = pg_catalog
                AS $$
                DECLARE
                  sp text;
                BEGIN
                  SHOW search_path INTO sp;
                  RETURN sp;
                END;
                $$
            """);

            // --- Setup for Stmt 114 (fn_to_drop) ---
            // Create two overloaded variants, then drop the integer one.
            // The text variant should still be callable.
            s.execute("CREATE FUNCTION fn_to_drop(integer) RETURNS integer LANGUAGE sql AS $$ SELECT $1 $$");
            s.execute("CREATE FUNCTION fn_to_drop(text) RETURNS text LANGUAGE sql AS $$ SELECT $1 $$");
            s.execute("DROP FUNCTION fn_to_drop(integer)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS fn_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    /**
     * Stmt 31: SELECT fn_bad_sql_body()
     *
     * A SQL-language function was created (with check_function_bodies=off) whose body
     * references a nonexistent table. Calling it at runtime should fail with
     * SQLSTATE 42P01 and a message about the missing relation.
     *
     * PG 18: ERROR [42P01]: relation "fn_no_such_table_sql" does not exist
     * Memgres: ERROR [42883]: function fn_bad_sql_body() does not exist
     */
    @Test
    void testBadSqlBodyReportsRelationNotFound() {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT fn_bad_sql_body()");
            fail("Expected an error when calling fn_bad_sql_body()");
        } catch (SQLException e) {
            assertEquals("42P01", e.getSQLState(),
                    "SQLSTATE should be 42P01 (undefined_table), got: " + e.getSQLState() + " - " + e.getMessage());
            assertTrue(e.getMessage().contains("fn_no_such_table_sql"),
                    "Error message should mention the missing table 'fn_no_such_table_sql', got: " + e.getMessage());
        }
    }

    /**
     * Stmt 68: SELECT fn_set_clause()
     *
     * A PL/pgSQL function with SET search_path = pg_catalog should execute with
     * that search_path in effect, so SHOW search_path returns 'pg_catalog'.
     *
     * PG 18: 'pg_catalog'
     * Memgres: NULL
     */
    @Test
    void testSetClauseReturnsConfiguredSearchPath() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT fn_set_clause()")) {
            assertTrue(rs.next(), "Expected one row from fn_set_clause()");
            String result = rs.getString(1);
            assertNotNull(result, "fn_set_clause() should not return NULL");
            assertEquals("pg_catalog", result,
                    "fn_set_clause() should return 'pg_catalog' (the SET clause value)");
        }
    }

    /**
     * Stmt 114: SELECT fn_to_drop('hello')
     *
     * After creating fn_to_drop(integer) and fn_to_drop(text), then dropping the
     * integer variant, calling fn_to_drop('hello') should still work and return 'hello'.
     *
     * PG 18: 'hello'
     * Memgres: ERROR [42883]: function fn_to_drop(text) does not exist
     */
    @Test
    void testDropOverloadLeavesOtherVariant() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT fn_to_drop('hello')")) {
            assertTrue(rs.next(), "Expected one row from fn_to_drop('hello')");
            assertEquals("hello", rs.getString(1),
                    "fn_to_drop('hello') should return 'hello' after dropping only the integer overload");
        }
    }
}
