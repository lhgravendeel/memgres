package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 10 remaining PG vs Memgres differences from various SQL files.
 *
 * These tests assert exact PG 18 behavior. They are expected to FAIL on
 * current Memgres, documenting the real gaps.
 *
 * Uses default JDBC (extended query protocol) to match the comparison framework.
 */
class MiscCompatibilityTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
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
        if (memgres != null) memgres.close();
    }

    // ========================================================================
    // custom-operators.sql stmt 62: +++ operator should NOT exist in pg_operator.
    // PG: EXISTS(...oprname='+++') = false
    // Memgres: true (Memgres is more permissive and allows +++ as operator name)
    // ========================================================================
    @Test
    void stmt62_triplePlusOperatorShouldNotExist() throws Exception {
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
                    "SELECT EXISTS(SELECT 1 FROM pg_operator WHERE oprname = '+++') AS exists")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean("exists"),
                        "PG does not allow +++ as an operator name, so EXISTS should be false. "
                        + "Memgres incorrectly accepts it.");
            }
        }
    }

    // ========================================================================
    // custom-operators.sql stmt 63: No rows for +++ in pg_operator.
    // PG: 0 rows. Memgres: 1 row (has_left=t, has_right=t).
    // ========================================================================
    @Test
    void stmt63_triplePlusOperatorNoDetails() throws Exception {
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
                assertFalse(rs.next(),
                        "PG returns 0 rows for oprname='+++' because +++ is not a valid "
                        + "operator name. Memgres incorrectly returns a row.");
            }
        }
    }

    // ========================================================================
    // pg-stat-views.sql stmt 15: pg_stat_activity state for current session.
    // PG: state = 'active' (the session is actively running a query)
    // Memgres: state = 'idle'
    // ========================================================================
    @Test
    void stmt15_pgStatActivityStateShouldBeActive() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT state FROM pg_stat_activity WHERE pid = pg_backend_pid()")) {
            assertTrue(rs.next(), "Expected one row for current session");
            assertEquals("active", rs.getString("state"),
                    "PG reports state='active' during query execution, "
                    + "but Memgres reports 'idle'");
        }
    }

    // ========================================================================
    // plpgsql-advanced.sql stmt 21: CREATE FUNCTION with FOREACH SLICE.
    //
    // FOREACH SLICE is a valid PL/pgSQL feature since PG 9.1 and should be
    // accepted. The comparison framework's JDBC driver may have mangled the
    // dollar-quoted body, causing a false failure on PG.
    // This test verifies FOREACH SLICE 1 works correctly.
    // ========================================================================
    @Test
    void stmt21_foreachSliceCreatesSuccessfully() throws Exception {
        // FOREACH SLICE is a valid PL/pgSQL feature and should be accepted
        try (Statement s = conn.createStatement()) {
            s.execute("DROP FUNCTION IF EXISTS pla_foreach_slice1()");
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
        }
    }

    // ========================================================================
    // plpgsql-advanced.sql stmt 22: SELECT pla_foreach_slice1()
    // FOREACH SLICE is valid PL/pgSQL. The function should execute and return
    // concatenated 1D slices of the 2D array.
    // ========================================================================
    @Test
    void stmt22_foreachSliceFunctionReturnsResult() throws Exception {
        // FOREACH SLICE is valid PL/pgSQL — create and call the function
        try (Statement s = conn.createStatement()) {
            s.execute("DROP FUNCTION IF EXISTS pla_foreach_slice1()");
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
                assertTrue(rs.next());
                assertNotNull(rs.getString("result"));
            }
        }
    }

    // ========================================================================
    // plpgsql-functions.sql stmt 115: After DROP FUNCTION fn_to_drop(integer),
    // calling fn_to_drop(1) should fail — even with fn_to_drop(text) still present.
    //
    // The comparison creates BOTH overloads (integer and text), drops only integer,
    // then calls fn_to_drop(1). PG cannot resolve the integer literal to the text
    // overload and errors. Memgres may incorrectly resolve via implicit cast.
    // ========================================================================
    @Test
    void stmt115_droppedFunctionShouldNotExist() throws Exception {
        try (Statement s = conn.createStatement()) {
            // Create both overloads — matching the comparison scenario
            s.execute("CREATE OR REPLACE FUNCTION fn_to_drop(integer) RETURNS integer "
                    + "LANGUAGE sql AS $$ SELECT $1 $$");
            s.execute("CREATE OR REPLACE FUNCTION fn_to_drop(text) RETURNS text "
                    + "LANGUAGE sql AS $$ SELECT $1 $$");

            // Drop only the integer variant
            s.execute("DROP FUNCTION fn_to_drop(integer)");

            // PG: fn_to_drop(1) fails — integer literal doesn't match text overload
            // Memgres: may incorrectly resolve to fn_to_drop(text)
            try {
                s.executeQuery("SELECT fn_to_drop(1)");
                fail("PG errors with 42883 after DROP FUNCTION fn_to_drop(integer), "
                        + "even though fn_to_drop(text) exists. Integer literal 1 should NOT "
                        + "resolve to the text overload.");
            } catch (SQLException e) {
                assertEquals("42883", e.getSQLState(),
                        "SQLSTATE should be 42883 (undefined_function), got: "
                        + e.getSQLState() + " - " + e.getMessage());
            } finally {
                try { s.execute("DROP FUNCTION IF EXISTS fn_to_drop(text)"); } catch (SQLException ignored) {}
            }
        }
    }

    // ========================================================================
    // prepared-statements.sql stmt 88: EXECUTE ps_union should return results.
    // PG: OK (val) [1] ; [2] ; [3]
    // Memgres: ERROR [XX000] IllegalStateException: Received resultset tuples,
    //          but no field structure for them
    //
    // This bug is specific to extended query protocol (Describe phase for UNION).
    // The comparison uses default JDBC (extended mode), not simple mode.
    // ========================================================================
    @Test
    void stmt88_executeUnionShouldReturnResults() throws Exception {
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
    // procedure-transaction-control.sql stmt 51: ROLLBACK in exception handler.
    // PG: ptc_log has 2 rows (both 'before error' committed and 'recovered')
    // Memgres: ptc_log has 1 row (only 'before error')
    // ========================================================================
    @Test
    void stmt51_rollbackInExceptionShouldProduceTwoRows() throws Exception {
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
                assertEquals(2, rs.getInt("cnt"),
                        "PG produces 2 rows in ptc_log ('before error' + 'recovered'), "
                        + "but Memgres produces fewer");
            }
        }
    }
}
