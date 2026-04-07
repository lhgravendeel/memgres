package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PL/pgSQL patterns that still fail.
 *
 * Covers:
 * - SELECT FROM table (no column list): PG allows this to check existence
 * - ALTER ROLE ... SET param = value (equals sign, not TO)
 * - GRANT SET ON PARAMETER
 * - CALL procedure() as standalone statement
 * - GET DIAGNOSTICS inside PL/pgSQL loops
 * - DO block with CTE-based DELETE inside BEGIN
 * - Block comments as the entire SQL statement
 */
class PlpgsqlSelectFromTest {

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
    // SELECT FROM table (no column list): existence check
    // =========================================================================

    @Test
    void testSelectFromWithoutColumns() throws SQLException {
        // PG allows: SELECT FROM t WHERE condition
        // This is equivalent to: SELECT * FROM t WHERE condition (but returns no columns)
        exec("CREATE TABLE sel_from (id serial PRIMARY KEY, name text)");
        exec("INSERT INTO sel_from (name) VALUES ('test')");
        exec("""
            DO $$
            BEGIN
                IF EXISTS (SELECT FROM sel_from WHERE name = 'test') THEN
                    RAISE NOTICE 'found';
                END IF;
            END;
            $$
        """);
    }

    @Test
    void testSelectFromCatalogTable() throws SQLException {
        // Real-world pattern: check role existence in pg_catalog
        exec("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'nonexistent_role') THEN
                    RAISE NOTICE 'role does not exist';
                END IF;
            END;
            $$
        """);
    }

    @Test
    void testSelectFromWithSchemaQualifiedTable() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS check_ns");
        exec("CREATE TABLE check_ns.targets (id serial PRIMARY KEY)");
        exec("INSERT INTO check_ns.targets DEFAULT VALUES");
        exec("""
            DO $$
            BEGIN
                IF EXISTS (SELECT FROM check_ns.targets WHERE id = 1) THEN
                    RAISE NOTICE 'exists';
                END IF;
            END;
            $$
        """);
    }

    @Test
    void testBareSelectFrom() throws SQLException {
        // Also valid as a standalone query (returns rows with zero columns)
        exec("CREATE TABLE bare_sel (id serial PRIMARY KEY)");
        exec("INSERT INTO bare_sel DEFAULT VALUES");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT FROM bare_sel WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getMetaData().getColumnCount());
        }
    }

    // =========================================================================
    // ALTER ROLE ... SET param = value
    // =========================================================================

    @Test
    void testAlterRoleSetWithEquals() throws SQLException {
        // Real-world: ALTER ROLE rolename SET param = value
        exec("ALTER ROLE CURRENT_USER SET statement_timeout = '30s'");
    }

    @Test
    void testAlterRoleSetWithTo() throws SQLException {
        // Standard form: SET param TO value
        exec("ALTER ROLE CURRENT_USER SET work_mem TO '64MB'");
    }

    @Test
    void testAlterRoleSetSearchPath() throws SQLException {
        exec("ALTER ROLE CURRENT_USER SET search_path = 'public, pg_catalog'");
    }

    @Test
    void testAlterRoleResetParam() throws SQLException {
        exec("ALTER ROLE CURRENT_USER RESET statement_timeout");
    }

    @Test
    void testAlterRoleResetAll() throws SQLException {
        exec("ALTER ROLE CURRENT_USER RESET ALL");
    }

    @Test
    void testAlterRoleSetInsideDoBlock() throws SQLException {
        exec("""
            DO $$
            BEGIN
                ALTER ROLE CURRENT_USER SET statement_timeout = '30s';
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Failed: %', SQLERRM;
            END;
            $$
        """);
    }

    // =========================================================================
    // GRANT SET ON PARAMETER
    // =========================================================================

    @Test
    void testGrantSetOnParameter() throws SQLException {
        // PG 15+: GRANT SET ON PARAMETER param TO role
        exec("GRANT SET ON PARAMETER statement_timeout TO CURRENT_USER");
    }

    @Test
    void testGrantSetOnParameterMultiple() throws SQLException {
        exec("GRANT SET ON PARAMETER work_mem, statement_timeout TO CURRENT_USER");
    }

    // =========================================================================
    // CALL procedure() as standalone statement
    // =========================================================================

    @Test
    void testCallProcedure() throws SQLException {
        exec("""
            CREATE PROCEDURE do_nothing()
            LANGUAGE plpgsql AS $$
            BEGIN
                -- nothing
            END;
            $$
        """);
        exec("CALL do_nothing()");
    }

    @Test
    void testCallProcedureWithArgs() throws SQLException {
        exec("CREATE TABLE call_log (msg text)");
        exec("""
            CREATE PROCEDURE log_msg(message text)
            LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO call_log (msg) VALUES (message);
            END;
            $$
        """);
        exec("CALL log_msg('hello')");
        assertEquals("hello", query1("SELECT msg FROM call_log"));
    }

    @Test
    void testCallAfterAnotherStatement() throws SQLException {
        // Pattern: procedure definition followed by CALL on next line
        exec("""
            CREATE PROCEDURE setup_view()
            LANGUAGE plpgsql AS $$
            BEGIN
                EXECUTE 'CREATE TABLE IF NOT EXISTS call_test (id serial PRIMARY KEY)';
            END;
            $$
        """);
        exec("CALL setup_view()");
        exec("DROP PROCEDURE setup_view()");
    }

    // =========================================================================
    // GET DIAGNOSTICS inside PL/pgSQL
    // =========================================================================

    @Test
    void testGetDiagnosticsRowCount() throws SQLException {
        exec("CREATE TABLE diag_test (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO diag_test (val) VALUES ('a'), ('b'), ('c')");
        exec("""
            DO $$
            DECLARE
                rows_affected integer;
            BEGIN
                DELETE FROM diag_test WHERE val = 'b';
                GET DIAGNOSTICS rows_affected = ROW_COUNT;
                RAISE NOTICE 'Deleted % rows', rows_affected;
            END;
            $$
        """);
        assertEquals("2", query1("SELECT COUNT(*) FROM diag_test"));
    }

    @Test
    void testGetDiagnosticsInLoop() throws SQLException {
        exec("CREATE TABLE diag_loop (id serial PRIMARY KEY, batch int, processed boolean DEFAULT false)");
        exec("INSERT INTO diag_loop (batch) VALUES (1), (1), (2), (2), (2)");
        exec("""
            DO $$
            DECLARE
                rows_updated integer;
            BEGIN
                LOOP
                    UPDATE diag_loop SET processed = true
                    WHERE batch = 1 AND processed = false;
                    GET DIAGNOSTICS rows_updated = ROW_COUNT;
                    EXIT WHEN rows_updated = 0;
                END LOOP;
            END;
            $$
        """);
        assertEquals("2", query1("SELECT COUNT(*) FROM diag_loop WHERE processed = true"));
    }

    // =========================================================================
    // Block comment as entire SQL statement
    // =========================================================================

    @Test
    void testBlockCommentOnlyStatement() throws SQLException {
        // Some migration files are just a comment explaining a no-op
        exec("/* This migration intentionally left blank */");
    }

    @Test
    void testBlockCommentWithParentheses() throws SQLException {
        // A license block comment with parentheses can confuse the parser
        exec("""
            /*
             * Licensed under the Apache License, Version 2.0 (the "License");
             * you may not use this file except in compliance with the License.
             * See the License for the specific language governing permissions and
             * limitations under the License.
             */
        """);
    }

    @Test
    void testBlockCommentFollowedByStatement() throws SQLException {
        exec("""
            /* This is a preamble comment */
            CREATE TABLE after_block_comment (id serial PRIMARY KEY)
        """);
        exec("INSERT INTO after_block_comment DEFAULT VALUES");
    }

    // =========================================================================
    // DO block with CTE-based DELETE
    // =========================================================================

    @Test
    void testDoBlockWithCteDelete() throws SQLException {
        exec("CREATE TABLE cte_del_src (id serial PRIMARY KEY, token text, active boolean)");
        exec("CREATE TABLE cte_del_sessions (id serial PRIMARY KEY, token text)");
        exec("INSERT INTO cte_del_src (token, active) VALUES ('t1', false), ('t2', true)");
        exec("INSERT INTO cte_del_sessions (token) VALUES ('t1'), ('t2')");
        exec("""
            DO $$
            BEGIN
                WITH removed AS (
                    DELETE FROM cte_del_src d
                    WHERE d.active = false
                    RETURNING d.token
                )
                DELETE FROM cte_del_sessions s
                WHERE s.token IN (SELECT token FROM removed);
            END;
            $$
        """);
        assertEquals("1", query1("SELECT COUNT(*) FROM cte_del_sessions"));
    }

    // =========================================================================
    // DO block with complex IF + subquery in condition
    // =========================================================================

    @Test
    void testDoBlockIfExistsWithSubquery() throws SQLException {
        exec("CREATE TABLE if_exists_test (id serial PRIMARY KEY, status text)");
        exec("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'if_exists_test'
                ) THEN
                    INSERT INTO if_exists_test (status) VALUES ('confirmed');
                END IF;
            END;
            $$
        """);
        assertEquals("confirmed", query1("SELECT status FROM if_exists_test"));
    }
}
