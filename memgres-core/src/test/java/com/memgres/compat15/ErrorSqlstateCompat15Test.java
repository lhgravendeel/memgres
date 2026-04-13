package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 5 error-sqlstate.sql failures where Memgres diverges from PG 18.
 *
 * Stmt 26: get_sqlstate for UNION type mismatch should return '42804', Memgres returns 'OK'
 * Stmt 28: sqlstate_readonly_test() should return '25006', Memgres returns 'OK'
 * Stmt 32: sqlstate_full_diag() should return '23505', Memgres returns NULL
 * Stmt 38: get_sqlstate for assert_fail should error P0004, Memgres succeeds with '42601'
 * Stmt 42: get_sqlstate for serialization_failure should return '40001', Memgres returns 'P0001'
 */
class ErrorSqlstateCompat15Test {

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
            s.execute("DROP SCHEMA IF EXISTS sqlstate_test CASCADE");
            s.execute("CREATE SCHEMA sqlstate_test");
            s.execute("SET search_path = sqlstate_test, public");

            // Helper function to capture SQLSTATE from any SQL string
            s.execute("""
                CREATE FUNCTION get_sqlstate(sql text) RETURNS text
                LANGUAGE plpgsql AS $$
                DECLARE
                  state text;
                BEGIN
                  EXECUTE sql;
                  RETURN 'OK';
                EXCEPTION WHEN OTHERS THEN
                  GET STACKED DIAGNOSTICS state = RETURNED_SQLSTATE;
                  RETURN state;
                END;
                $$
            """);

            // Table needed for Stmt 26 (UNION type mismatch)
            s.execute("CREATE TABLE sqlstate_types (id integer, val text)");

            // Table needed for Stmt 28 (read-only transaction) and Stmt 32 (full diagnostics)
            s.execute("CREATE TABLE sqlstate_uniq (id integer PRIMARY KEY)");
            s.execute("INSERT INTO sqlstate_uniq VALUES (1)");

            // Function for Stmt 28: read-only transaction enforcement
            s.execute("""
                CREATE FUNCTION sqlstate_readonly_test() RETURNS text
                LANGUAGE plpgsql AS $$
                DECLARE
                  state text;
                BEGIN
                  BEGIN
                    SET TRANSACTION READ ONLY;
                    INSERT INTO sqlstate_test.sqlstate_uniq VALUES (999);
                    RETURN 'OK';
                  EXCEPTION WHEN OTHERS THEN
                    GET STACKED DIAGNOSTICS state = RETURNED_SQLSTATE;
                    RETURN state;
                  END;
                END;
                $$
            """);

            // Function for Stmt 32: GET STACKED DIAGNOSTICS full fields
            s.execute("""
                CREATE FUNCTION sqlstate_full_diag() RETURNS TABLE(sqlstate_val text, msg text, detail_val text)
                LANGUAGE plpgsql AS $$
                DECLARE
                  v_state text;
                  v_msg text;
                  v_detail text;
                BEGIN
                  INSERT INTO sqlstate_test.sqlstate_uniq VALUES (1);
                EXCEPTION WHEN OTHERS THEN
                  GET STACKED DIAGNOSTICS
                    v_state = RETURNED_SQLSTATE,
                    v_msg = MESSAGE_TEXT,
                    v_detail = PG_EXCEPTION_DETAIL;
                  sqlstate_val := v_state;
                  msg := v_msg;
                  detail_val := v_detail;
                  RETURN NEXT;
                END;
                $$
            """);

            // Function for Stmt 38: ASSERT failure (P0004)
            s.execute("""
                CREATE FUNCTION sqlstate_assert_fail() RETURNS void
                LANGUAGE plpgsql AS $$
                BEGIN
                  ASSERT false, 'test assertion';
                END;
                $$
            """);

            // Function for Stmt 42: RAISE with serialization_failure ERRCODE
            s.execute("""
                CREATE FUNCTION sqlstate_serial_raise() RETURNS void
                LANGUAGE plpgsql AS $$
                BEGIN
                  RAISE EXCEPTION USING ERRCODE = 'serialization_failure', MESSAGE = 'simulated';
                END;
                $$
            """);
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS sqlstate_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    /**
     * Stmt 26: UNION type mismatch should produce SQLSTATE 42804 (datatype_mismatch).
     *
     * SELECT id (integer) UNION ALL SELECT val (text) is a type mismatch.
     * PG 18: get_sqlstate returns '42804'
     * Memgres: get_sqlstate returns 'OK' (no error raised)
     */
    @Test
    void testUnionTypeMismatchReturns42804() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT get_sqlstate('SELECT id FROM sqlstate_test.sqlstate_types UNION ALL SELECT val FROM sqlstate_test.sqlstate_types') AS state")) {
            assertTrue(rs.next(), "Expected one row");
            String state = rs.getString("state");
            assertEquals("42804", state,
                    "UNION type mismatch should produce SQLSTATE 42804 (datatype_mismatch), got: " + state);
        }
    }

    /**
     * Stmt 28: Read-only transaction violation should produce SQLSTATE 25006.
     *
     * SET TRANSACTION READ ONLY followed by INSERT should fail with
     * SQLSTATE 25006 (read_only_sql_transaction).
     * PG 18: returns '25006'
     * Memgres: returns 'OK' (no error raised)
     */
    @Test
    void testReadOnlyTransactionReturns25006() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT sqlstate_readonly_test() AS state")) {
            assertTrue(rs.next(), "Expected one row");
            String state = rs.getString("state");
            assertEquals("25006", state,
                    "Read-only transaction violation should produce SQLSTATE 25006 (read_only_sql_transaction), got: " + state);
        }
    }

    /**
     * Stmt 32: GET STACKED DIAGNOSTICS should capture SQLSTATE 23505 on unique violation.
     *
     * Inserting a duplicate into sqlstate_uniq (which already has id=1) should
     * be caught by the EXCEPTION block and return SQLSTATE 23505 via diagnostics.
     * PG 18: returns '23505'
     * Memgres: returns NULL
     */
    @Test
    void testFullDiagCapturesSqlstate23505() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT sqlstate_val FROM sqlstate_full_diag()")) {
            assertTrue(rs.next(), "Expected one row from sqlstate_full_diag()");
            String sqlstateVal = rs.getString("sqlstate_val");
            assertNotNull(sqlstateVal, "sqlstate_val should not be NULL");
            assertEquals("23505", sqlstateVal,
                    "GET STACKED DIAGNOSTICS should capture SQLSTATE 23505 (unique_violation), got: " + sqlstateVal);
        }
    }

    /**
     * Stmt 38: PL/pgSQL ASSERT false should produce SQLSTATE P0004 (assert_failure).
     *
     * In PG 18, get_sqlstate wrapping the assert call itself raises P0004 as an
     * unhandled error (ASSERT failures propagate past the WHEN OTHERS handler).
     * PG 18: ERROR [P0004]: ERROR: test assertion
     * Memgres: returns '42601' (succeeds instead of erroring)
     */
    @Test
    void testAssertFailureProducesP0004Error() {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT get_sqlstate('SELECT sqlstate_test.sqlstate_assert_fail()') AS state");
            fail("Expected an error with SQLSTATE P0004 (assert_failure) when calling sqlstate_assert_fail()");
        } catch (SQLException e) {
            assertEquals("P0004", e.getSQLState(),
                    "ASSERT false should produce SQLSTATE P0004 (assert_failure), got: " + e.getSQLState() + " - " + e.getMessage());
            assertTrue(e.getMessage().toLowerCase().contains("assertion"),
                    "Error message should mention 'assertion', got: " + e.getMessage());
        }
    }

    /**
     * Stmt 42: RAISE with ERRCODE = 'serialization_failure' should produce SQLSTATE 40001.
     *
     * The function raises an exception using the condition name 'serialization_failure',
     * which maps to SQLSTATE 40001. get_sqlstate should capture and return that code.
     * PG 18: returns '40001'
     * Memgres: returns 'P0001' (generic raise_exception instead of the specified ERRCODE)
     */
    @Test
    void testSerializationFailureReturns40001() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT get_sqlstate('SELECT sqlstate_test.sqlstate_serial_raise()') AS state")) {
            assertTrue(rs.next(), "Expected one row");
            String state = rs.getString("state");
            assertEquals("40001", state,
                    "RAISE with ERRCODE = 'serialization_failure' should produce SQLSTATE 40001, got: " + state);
        }
    }
}
