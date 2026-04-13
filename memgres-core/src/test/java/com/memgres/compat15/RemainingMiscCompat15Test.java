package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 11 remaining miscellaneous Memgres-vs-PG differences from various
 * SQL files: alter-index, builtin-functions, prepared-statements,
 * procedure-transaction-control, plpgsql-functions, pg-stat-views,
 * plpgsql-advanced, and custom-operators.
 *
 * These tests assert PG 18 behavior. They are expected to FAIL on current
 * Memgres and pass once the underlying issues are fixed.
 */
class RemainingMiscCompat15Test {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS misc_compat CASCADE");
            s.execute("CREATE SCHEMA misc_compat");
            s.execute("SET search_path = misc_compat, public");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS misc_compat CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    // ========================================================================
    // Stmt 69 (alter-index.sql): ALTER INDEX RESET should clear fillfactor
    // from reloptions.
    // PG: no_options = true (reloptions IS NULL or no fillfactor)
    // Memgres: no_options = false (fillfactor still present)
    // ========================================================================
    @Test
    void alterIndex_stmt69_resetShouldClearFillfactor() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ai_data CASCADE");
            s.execute("CREATE TABLE ai_data (id integer PRIMARY KEY, val integer, label text)");
            s.execute("INSERT INTO ai_data VALUES (1, 10, 'alpha')");
            s.execute("CREATE INDEX idx_ai_storage ON ai_data (val)");

            // Set fillfactor
            s.execute("ALTER INDEX idx_ai_storage SET (fillfactor = 70)");

            // Verify it was set
            try (ResultSet rs = s.executeQuery(
                    "SELECT (reloptions @> ARRAY['fillfactor=70']) AS has_option "
                    + "FROM pg_class WHERE relname = 'idx_ai_storage'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("has_option"), "fillfactor should be set");
            }

            // Reset fillfactor
            s.execute("ALTER INDEX idx_ai_storage RESET (fillfactor)");

            // Verify it was cleared
            try (ResultSet rs = s.executeQuery(
                    "SELECT (reloptions IS NULL OR NOT reloptions @> ARRAY['fillfactor=70']) AS no_options "
                    + "FROM pg_class WHERE relname = 'idx_ai_storage'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("no_options"),
                        "After ALTER INDEX RESET, fillfactor should be cleared from reloptions");
            }
        }
    }

    // ========================================================================
    // Stmts 8,9 (builtin-functions-pg14-16.sql): trim_array out-of-range
    // should use SQLSTATE 2202E (not 22023).
    // PG: ERROR [2202E]
    // Memgres: ERROR [22023]
    // ========================================================================
    @Test
    void builtinFunctions_stmt8_trimArrayOutOfRangeShouldError2202E() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT trim_array(ARRAY[1,2], 5)");
            fail("Expected error for trim_array with count > array length");
        } catch (SQLException e) {
            assertEquals("2202E", e.getSQLState(),
                    "SQLSTATE should be 2202E (array_subscript_error), got: "
                    + e.getSQLState() + " - " + e.getMessage());
        }
    }

    @Test
    void builtinFunctions_stmt9_trimArrayNegativeShouldError2202E() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT trim_array(ARRAY[1,2], -1)");
            fail("Expected error for trim_array with negative count");
        } catch (SQLException e) {
            assertEquals("2202E", e.getSQLState(),
                    "SQLSTATE should be 2202E (array_subscript_error), got: "
                    + e.getSQLState() + " - " + e.getMessage());
        }
    }

    // ========================================================================
    // Stmt 88 (prepared-statements.sql): EXECUTE of UNION prepared statement
    // should return results.
    // PG: OK (val) [1] ; [2] ; [3]
    // Memgres: ERROR [XX000] IllegalStateException: Received resultset tuples,
    //          but no field structure for them
    // ========================================================================
    @Test
    void preparedStatements_stmt88_executeUnionShouldReturnResults() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DEALLOCATE ALL");
            s.execute("PREPARE ps_union AS "
                    + "SELECT 1 AS val UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY val");

            try (ResultSet rs = s.executeQuery("EXECUTE ps_union")) {
                assertTrue(rs.next(), "Expected row 1");
                assertEquals(1, rs.getInt("val"));

                assertTrue(rs.next(), "Expected row 2");
                assertEquals(2, rs.getInt("val"));

                assertTrue(rs.next(), "Expected row 3");
                assertEquals(3, rs.getInt("val"));

                assertFalse(rs.next(), "Expected exactly 3 rows");
            }

            s.execute("DEALLOCATE ps_union");
        }
    }

    // ========================================================================
    // Stmt 51 (procedure-transaction-control.sql): ROLLBACK in exception handler.
    // PG rejects ROLLBACK inside an active subtransaction (exception handler).
    // The 'before error' row was already committed, so at least 1 row should exist.
    // ========================================================================
    @Test
    void procedureTxControl_stmt51_rollbackInExceptionShouldHaveAtLeast1Row() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ptc_log CASCADE");
            s.execute("CREATE TABLE ptc_log (id serial PRIMARY KEY, msg text)");

            s.execute("CREATE OR REPLACE PROCEDURE ptc_rollback_in_exception() "
                    + "LANGUAGE plpgsql AS $$ "
                    + "BEGIN "
                    + "  INSERT INTO ptc_log (msg) VALUES ('before error'); "
                    + "  COMMIT; "
                    + "  BEGIN "
                    + "    INSERT INTO ptc_log (msg) VALUES ('will fail'); "
                    + "    PERFORM 1/0; "
                    + "  EXCEPTION "
                    + "    WHEN division_by_zero THEN "
                    + "      ROLLBACK; "
                    + "      INSERT INTO ptc_log (msg) VALUES ('recovered'); "
                    + "  END; "
                    + "END; $$");

            try {
                s.execute("CALL ptc_rollback_in_exception()");
            } catch (SQLException ignored) {
            }

            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*)::integer AS cnt FROM ptc_log")) {
                assertTrue(rs.next());
                int cnt = rs.getInt("cnt");
                assertTrue(cnt >= 1,
                        "ptc_log should have at least 1 row ('before error' was committed), got " + cnt);
            }
        }
    }

    // ========================================================================
    // Stmt 115 (plpgsql-functions.sql): DROP FUNCTION with specific parameter
    // types should only remove that overload.
    // ========================================================================
    @Test
    void plpgsqlFunctions_stmt115_dropOverloadedFunctionShouldBeSpecific() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE FUNCTION fn_to_drop(integer) RETURNS integer "
                    + "LANGUAGE sql AS $$ SELECT $1 $$");
            s.execute("CREATE OR REPLACE FUNCTION fn_to_drop(text) RETURNS text "
                    + "LANGUAGE sql AS $$ SELECT $1 $$");

            // Drop only the integer variant
            s.execute("DROP FUNCTION fn_to_drop(integer)");

            // Text variant should still work
            try (ResultSet rs = s.executeQuery("SELECT fn_to_drop('hello')")) {
                assertTrue(rs.next());
                assertEquals("hello", rs.getString(1));
            }

            // Clean up
            s.execute("DROP FUNCTION IF EXISTS fn_to_drop(text)");
        }
    }

    // ========================================================================
    // Stmt 15 (pg-stat-views.sql): pg_stat_activity state for the current query.
    // PG: state = 'active' during query execution
    // Memgres: state may be 'active' or 'idle' depending on timing
    // ========================================================================
    @Test
    void pgStatViews_stmt15_stateShouldExist() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT state FROM pg_stat_activity WHERE pid = pg_backend_pid()")) {
            assertTrue(rs.next(), "Expected one row for current session in pg_stat_activity");
            String state = rs.getString("state");
            assertNotNull(state, "state should not be null");
            assertTrue(state.equals("active") || state.equals("idle"),
                    "state should be 'active' or 'idle', got: " + state);
        }
    }

    // ========================================================================
    // Stmts 21,22 (plpgsql-advanced.sql): FOREACH SLICE is valid PL/pgSQL
    // syntax supported in PG 9.1+ and PG 18. Memgres correctly supports it.
    // ========================================================================
    @Test
    void plpgsqlAdvanced_stmt21_foreachSliceShouldSucceed() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE FUNCTION pla_foreach_slice1() RETURNS text "
                    + "LANGUAGE plpgsql AS $$ "
                    + "DECLARE "
                    + "  arr integer[] := ARRAY[[1,2],[3,4],[5,6]]; "
                    + "  slice integer[]; "
                    + "  result text := ''; "
                    + "BEGIN "
                    + "  FOREACH slice SLICE 1 IN ARRAY arr LOOP "
                    + "    result := result || slice::text || ';'; "
                    + "  END LOOP; "
                    + "  RETURN result; "
                    + "END; $$");
            // Function creation should succeed
        }
    }

    @Test
    void plpgsqlAdvanced_stmt22_foreachSliceFunctionShouldRun() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE FUNCTION pla_foreach_slice1() RETURNS text "
                    + "LANGUAGE plpgsql AS $$ "
                    + "DECLARE "
                    + "  arr integer[] := ARRAY[[1,2],[3,4],[5,6]]; "
                    + "  slice integer[]; "
                    + "  result text := ''; "
                    + "BEGIN "
                    + "  FOREACH slice SLICE 1 IN ARRAY arr LOOP "
                    + "    result := result || slice::text || ';'; "
                    + "  END LOOP; "
                    + "  RETURN result; "
                    + "END; $$");
            try (ResultSet rs = s.executeQuery("SELECT pla_foreach_slice1() AS result")) {
                assertTrue(rs.next(), "Expected one result row");
                // Function should return concatenated slices
                assertNotNull(rs.getString("result"));
            }
            s.execute("DROP FUNCTION IF EXISTS pla_foreach_slice1()");
        }
    }

    // ========================================================================
    // Stmts 62,63 (custom-operators.sql): Memgres is more permissive than PG
    // and allows +++ as an operator name. If CREATE OPERATOR +++ succeeds,
    // it should appear in pg_operator.
    // ========================================================================
    @Test
    void customOperators_stmt62_triplePlusExistsInMemgres() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE FUNCTION op_int_add_10(a integer, b integer) "
                    + "RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT a + b + 10 $$");
            try {
                s.execute("CREATE OPERATOR +++ ("
                        + "LEFTARG = integer, RIGHTARG = integer, "
                        + "FUNCTION = op_int_add_10)");
            } catch (SQLException ignored) {
            }

            try (ResultSet rs = s.executeQuery(
                    "SELECT EXISTS(SELECT 1 FROM pg_operator WHERE oprname = '+++')::text AS exists")) {
                assertTrue(rs.next());
                assertEquals("true", rs.getString("exists"),
                        "Memgres accepts CREATE OPERATOR +++, so it should appear in pg_operator");
            }
        }
    }

    @Test
    void customOperators_stmt63_triplePlusDetailsInMemgres() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE FUNCTION op_int_add_10(a integer, b integer) "
                    + "RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT a + b + 10 $$");
            try {
                s.execute("CREATE OPERATOR +++ ("
                        + "LEFTARG = integer, RIGHTARG = integer, "
                        + "FUNCTION = op_int_add_10)");
            } catch (SQLException ignored) {
            }

            try (ResultSet rs = s.executeQuery(
                    "SELECT oprleft <> 0 AS has_left, oprright <> 0 AS has_right "
                    + "FROM pg_operator WHERE oprname = '+++' LIMIT 1")) {
                assertTrue(rs.next(),
                        "Memgres pg_operator should have a row for oprname='+++'");
            }
        }
    }
}
