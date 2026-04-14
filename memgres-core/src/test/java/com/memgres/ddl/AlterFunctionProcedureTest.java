package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 9 ALTER FUNCTION / ALTER PROCEDURE failures from alter-function-procedure.sql
 * where Memgres diverges from PostgreSQL 18 behavior.
 *
 * Stmt 12  - SELECT af_to_move(5) should error 42883 after SET SCHEMA, Memgres succeeds
 * Stmt 49  - prorows should be 0 after ALTER FUNCTION ROWS 0 on scalar fn, Memgres returns 500
 * Stmt 59  - af_config() should return 'pg_catalog' after SET search_path, Memgres returns NULL
 * Stmt 62  - af_config() LIKE '%af_test%' should return true after RESET, Memgres returns NULL
 * Stmt 66  - af_overload('hi') should succeed returning 'text', Memgres errors 42883
 * Stmt 78  - count from af_proc_log should be 2 after CALL in moved schema, Memgres returns 1
 * Stmt 108 - ALTER FUNCTION af_sig_test(text) RENAME should error 42883, Memgres succeeds
 * Stmt 111 - proconfig IS NOT NULL should be true after SET work_mem, Memgres returns false
 * Stmt 132 - provolatile should be 'v' after ROLLBACK of IMMUTABLE, Memgres returns 'i'
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AlterFunctionProcedureTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            // Base schema setup
            s.execute("DROP SCHEMA IF EXISTS af_test CASCADE");
            s.execute("CREATE SCHEMA af_test");
            s.execute("SET search_path = af_test, public");

            // -- Section 1: ALTER FUNCTION RENAME TO (not tested here, but needed for schema state)
            s.execute("CREATE FUNCTION af_orig(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x + 1 $$");
            s.execute("ALTER FUNCTION af_orig(integer) RENAME TO af_renamed");

            // -- Section 2: ALTER FUNCTION SET SCHEMA (Stmt 12 setup)
            s.execute("CREATE SCHEMA af_other");
            s.execute("CREATE FUNCTION af_to_move(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x * 2 $$");
            s.execute("ALTER FUNCTION af_to_move(integer) SET SCHEMA af_other");

            // -- Section 8: ALTER FUNCTION COST / ROWS (Stmt 49 setup)
            s.execute("CREATE FUNCTION af_cost(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x $$");
            s.execute("ALTER FUNCTION af_cost(integer) COST 1000");
            // PG 18: ROWS on non-SRF is rejected with 22023; skip it
            try { s.execute("ALTER FUNCTION af_cost(integer) ROWS 500"); } catch (SQLException ignored) {}

            // -- Section 10: ALTER FUNCTION SET configuration (Stmt 59 setup)
            s.execute("CREATE FUNCTION af_config() RETURNS text LANGUAGE plpgsql AS $$\n"
                    + "DECLARE sp text;\n"
                    + "BEGIN\n"
                    + "  SHOW search_path INTO sp;\n"
                    + "  RETURN sp;\n"
                    + "END;\n"
                    + "$$");
            s.execute("ALTER FUNCTION af_config() SET search_path = pg_catalog");

            // -- Section 12: ALTER overloaded function (Stmt 66 setup)
            s.execute("CREATE FUNCTION af_overload(x integer) RETURNS text LANGUAGE sql AS $$ SELECT 'int' $$");
            s.execute("CREATE FUNCTION af_overload(x text) RETURNS text LANGUAGE sql AS $$ SELECT 'text' $$");
            s.execute("ALTER FUNCTION af_overload(integer) RENAME TO af_overload_int");

            // -- Section 14: ALTER PROCEDURE RENAME TO (needed before Stmt 78)
            s.execute("CREATE TABLE af_proc_log (msg text)");
            s.execute("CREATE PROCEDURE af_proc_orig(p_msg text) LANGUAGE plpgsql AS $$\n"
                    + "BEGIN\n"
                    + "  INSERT INTO af_proc_log VALUES (p_msg);\n"
                    + "END;\n"
                    + "$$");
            s.execute("ALTER PROCEDURE af_proc_orig(text) RENAME TO af_proc_renamed");
            s.execute("CALL af_proc_renamed('hello')");

            // -- Section 15: ALTER PROCEDURE SET SCHEMA (Stmt 78 setup)
            s.execute("CREATE SCHEMA af_proc_schema");
            s.execute("CREATE PROCEDURE af_proc_move() LANGUAGE plpgsql AS $$\n"
                    + "BEGIN\n"
                    + "  INSERT INTO af_proc_log VALUES ('moved');\n"
                    + "END;\n"
                    + "$$");
            s.execute("ALTER PROCEDURE af_proc_move() SET SCHEMA af_proc_schema");
            s.execute("CALL af_proc_schema.af_proc_move()");

            // -- Section 25: ALTER FUNCTION with wrong signature (Stmt 108 setup)
            s.execute("CREATE FUNCTION af_sig_test(integer) RETURNS integer LANGUAGE sql AS $$ SELECT $1; $$");

            // -- Section 26: ALTER PROCEDURE SET config parameter (Stmt 111 setup)
            s.execute("CREATE PROCEDURE af_proc_config() LANGUAGE plpgsql AS $$\n"
                    + "BEGIN\n"
                    + "  RAISE NOTICE '%', current_setting('work_mem');\n"
                    + "END;\n"
                    + "$$");
            s.execute("ALTER PROCEDURE af_proc_config() SET work_mem = '256MB'");

            // -- Section 29: ALTER FUNCTION inside transaction (Stmt 132 setup)
            s.execute("CREATE FUNCTION af_txn_test() RETURNS integer LANGUAGE sql AS $$ SELECT 1; $$");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS af_other CASCADE");
                s.execute("DROP SCHEMA IF EXISTS af_proc_schema CASCADE");
                s.execute("DROP SCHEMA IF EXISTS af_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            String val = rs.getString(1);
            return val;
        }
    }

    /**
     * Stmt 12: After ALTER FUNCTION af_to_move SET SCHEMA af_other, calling
     * af_to_move(5) without schema qualification should error with 42883.
     * Memgres incorrectly still resolves the function and returns 10.
     */
    @Test
    @Order(1)
    void testSetSchemaRemovesFunctionFromOriginalSchema() {
        SQLException ex = assertThrows(SQLException.class,
                () -> query1("SELECT af_to_move(5)"),
                "af_to_move(5) should fail after SET SCHEMA moved it to af_other");
        assertEquals("42883", ex.getSQLState(),
                "Expected SQLSTATE 42883 (undefined_function)");
    }

    /**
     * Stmt 49: ROWS clause on a scalar (non-set-returning) function is accepted
     * but PG keeps prorows at 0 because the function does not return a set.
     * Memgres incorrectly stores 500.
     */
    @Test
    @Order(2)
    void testAlterRowsOnScalarFunctionKeepsProrowsZero() throws SQLException {
        String prorows = query1("SELECT prorows FROM pg_proc WHERE proname = 'af_cost'");
        assertEquals("0", prorows,
                "prorows should remain 0 for a scalar function even after ALTER FUNCTION ROWS 500");
    }

    /**
     * Stmt 59: After ALTER FUNCTION af_config() SET search_path = pg_catalog,
     * calling af_config() should return 'pg_catalog'. Memgres returns NULL.
     */
    @Test
    @Order(3)
    void testSetSearchPathAffectsFunctionExecution() throws SQLException {
        String result = query1("SELECT af_config()");
        assertEquals("pg_catalog", result,
                "af_config() should return 'pg_catalog' after SET search_path = pg_catalog");
    }

    /**
     * Stmt 62: After ALTER FUNCTION af_config() RESET search_path, calling
     * af_config() should return a path containing 'af_test'.
     * Memgres returns NULL.
     */
    @Test
    @Order(4)
    void testResetSearchPathRestoresSessionDefault() throws SQLException {
        exec("ALTER FUNCTION af_config() RESET search_path");

        String result = query1("SELECT af_config() LIKE '%af_test%' AS result");
        assertEquals("t", result,
                "After RESET search_path, af_config() should reflect session search_path containing 'af_test'");
    }

    /**
     * Stmt 66: After renaming the integer overload of af_overload to af_overload_int,
     * the text overload should still be callable as af_overload('hi').
     * Memgres errors with 42883.
     */
    @Test
    @Order(5)
    void testRenameOverloadedFunctionLeavesOtherOverload() throws SQLException {
        String result = query1("SELECT af_overload('hi') AS result");
        assertEquals("text", result,
                "Text overload af_overload('hi') should still work after renaming integer overload");
    }

    /**
     * Stmt 78: After moving af_proc_move to af_proc_schema and calling it,
     * af_proc_log should have 2 rows ('hello' from renamed proc + 'moved' from moved proc).
     * Memgres returns 1.
     */
    @Test
    @Order(6)
    void testProcedureSetSchemaAndCallInsertsRow() throws SQLException {
        String cnt = query1("SELECT count(*)::integer AS cnt FROM af_proc_log");
        assertEquals("2", cnt,
                "af_proc_log should have 2 rows after calling both the renamed and schema-moved procedures");
    }

    /**
     * Stmt 108: ALTER FUNCTION af_sig_test(text) RENAME should error because
     * af_sig_test was created with an integer parameter, not text.
     * Memgres incorrectly succeeds.
     */
    @Test
    @Order(7)
    void testAlterFunctionWrongSignatureErrors() {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("ALTER FUNCTION af_sig_test(text) RENAME TO af_sig_test_new"),
                "ALTER FUNCTION with wrong parameter type should fail");
        assertTrue(ex.getSQLState().equals("42883"),
                "Expected SQLSTATE 42883 (undefined_function), got: " + ex.getSQLState());
    }

    /**
     * Stmt 111: After ALTER PROCEDURE af_proc_config() SET work_mem = '256MB',
     * proconfig should not be NULL in pg_proc. Memgres returns false.
     */
    @Test
    @Order(8)
    void testAlterProcedureSetConfigPopulatesProconfig() throws SQLException {
        String hasConfig = query1(
                "SELECT proconfig IS NOT NULL AS has_config FROM pg_proc WHERE proname = 'af_proc_config'");
        assertEquals("t", hasConfig,
                "proconfig should not be NULL after ALTER PROCEDURE SET work_mem");
    }

    /**
     * Stmt 132: After BEGIN; ALTER FUNCTION af_txn_test() IMMUTABLE; ROLLBACK;
     * the function should revert to VOLATILE (provolatile = 'v').
     * Memgres returns 'i', meaning the rollback did not revert the change.
     */
    @Test
    @Order(9)
    void testAlterFunctionRollbackRevertsVolatility() throws SQLException {
        conn.setAutoCommit(false);
        try {
            exec("ALTER FUNCTION af_txn_test() IMMUTABLE");

            // Inside the transaction the change should be visible
            String inside = query1("SELECT provolatile FROM pg_proc WHERE proname = 'af_txn_test'");
            assertEquals("i", inside, "Inside transaction, provolatile should be 'i'");

            conn.rollback();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }

        // After rollback, should revert to VOLATILE
        String after = query1("SELECT provolatile FROM pg_proc WHERE proname = 'af_txn_test'");
        assertEquals("v", after,
                "After ROLLBACK, provolatile should revert to 'v' (VOLATILE)");
    }
}
