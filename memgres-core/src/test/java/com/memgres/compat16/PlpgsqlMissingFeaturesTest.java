package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests PL/pgSQL features that are missing or incomplete in Memgres.
 *
 * PG 18 PL/pgSQL features not implemented in Memgres:
 *   - ASSERT statement (D3)
 *   - RETURN QUERY EXECUTE (D1)
 *   - CALL ... INTO for capturing OUT params (D4)
 *   - FOREACH ... SLICE for multi-dimensional array iteration (D2)
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres.
 */
class PlpgsqlMissingFeaturesTest {

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

    // =========================================================================
    // ASSERT statement
    // =========================================================================

    @Test
    void assert_passingCondition_shouldSucceed() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION test_assert_pass() RETURNS void "
                + "LANGUAGE plpgsql AS $$ "
                + "BEGIN "
                + "  ASSERT 1 = 1, 'basic equality'; "
                + "END; $$");

        // Should succeed without error
        exec("SELECT test_assert_pass()");
    }

    @Test
    void assert_failingCondition_shouldRaiseError() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION test_assert_fail() RETURNS void "
                + "LANGUAGE plpgsql AS $$ "
                + "BEGIN "
                + "  ASSERT 1 = 2, 'this should fail'; "
                + "END; $$");

        try (Statement s = conn.createStatement()) {
            s.execute("SELECT test_assert_fail()");
            fail("ASSERT with false condition should raise an error");
        } catch (SQLException e) {
            // PG raises P0004 (assert_failure)
            assertEquals("P0004", e.getSQLState(),
                    "ASSERT failure should produce P0004, got: " + e.getSQLState());
            assertTrue(e.getMessage().contains("this should fail"),
                    "Error message should contain the assert message");
        }
    }

    @Test
    void assert_withoutMessage_shouldStillFail() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION test_assert_no_msg() RETURNS void "
                + "LANGUAGE plpgsql AS $$ "
                + "BEGIN "
                + "  ASSERT false; "
                + "END; $$");

        try (Statement s = conn.createStatement()) {
            s.execute("SELECT test_assert_no_msg()");
            fail("ASSERT false should raise P0004");
        } catch (SQLException e) {
            assertEquals("P0004", e.getSQLState(),
                    "ASSERT false without message should produce P0004");
        }
    }

    @Test
    void assert_canBeDisabledViaSetting() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION test_assert_disabled() RETURNS text "
                + "LANGUAGE plpgsql AS $$ "
                + "BEGIN "
                + "  ASSERT false, 'should not fire'; "
                + "  RETURN 'reached'; "
                + "END; $$");

        exec("SET plpgsql.check_asserts = off");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT test_assert_disabled()")) {
            assertTrue(rs.next());
            assertEquals("reached", rs.getString(1),
                    "With plpgsql.check_asserts=off, ASSERT should be skipped");
        } finally {
            exec("SET plpgsql.check_asserts = on");
        }
    }

    // =========================================================================
    // RETURN QUERY EXECUTE (dynamic SQL in set-returning function)
    // =========================================================================

    @Test
    void returnQueryExecute_shouldReturnDynamicResults() throws SQLException {
        exec("DROP TABLE IF EXISTS rqe_data");
        exec("CREATE TABLE rqe_data (id int, category text, val int)");
        exec("INSERT INTO rqe_data VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)");

        exec("CREATE OR REPLACE FUNCTION get_by_category(cat text) "
                + "RETURNS SETOF rqe_data LANGUAGE plpgsql AS $$ "
                + "BEGIN "
                + "  RETURN QUERY EXECUTE "
                + "    'SELECT * FROM rqe_data WHERE category = $1 ORDER BY id' "
                + "    USING cat; "
                + "END; $$");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM get_by_category('a')")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(10, rs.getInt("val"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals(30, rs.getInt("val"));
            assertFalse(rs.next());
        }
    }

    @Test
    void returnQueryExecute_withTableNameParam() throws SQLException {
        exec("DROP TABLE IF EXISTS rqe_dynamic");
        exec("CREATE TABLE rqe_dynamic (id int, name text)");
        exec("INSERT INTO rqe_dynamic VALUES (1, 'x'), (2, 'y')");

        exec("CREATE OR REPLACE FUNCTION count_rows(tbl text) "
                + "RETURNS bigint LANGUAGE plpgsql AS $$ "
                + "DECLARE result bigint; "
                + "BEGIN "
                + "  EXECUTE 'SELECT count(*) FROM ' || quote_ident(tbl) INTO result; "
                + "  RETURN result; "
                + "END; $$");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count_rows('rqe_dynamic')")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
        }
    }

    // =========================================================================
    // CALL ... INTO (capture OUT parameters in PL/pgSQL)
    // =========================================================================

    @Test
    void callInto_shouldCaptureOutParams() throws SQLException {
        exec("CREATE OR REPLACE PROCEDURE get_stats("
                + "  OUT total bigint, OUT avg_val numeric"
                + ") LANGUAGE plpgsql AS $$ "
                + "BEGIN "
                + "  SELECT count(*), avg(val) INTO total, avg_val "
                + "    FROM (VALUES (10), (20), (30)) AS t(val); "
                + "END; $$");

        exec("CREATE OR REPLACE FUNCTION test_call_into() RETURNS text "
                + "LANGUAGE plpgsql AS $$ "
                + "DECLARE "
                + "  t bigint; "
                + "  a numeric; "
                + "BEGIN "
                + "  CALL get_stats(t, a); "
                + "  RETURN 'total=' || t || ' avg=' || a; "
                + "END; $$");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT test_call_into()")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("total=3"),
                    "CALL INTO should capture OUT params. Got: " + result);
            assertTrue(result.contains("avg=20"),
                    "CALL INTO should capture avg. Got: " + result);
        }
    }

    // =========================================================================
    // FOREACH ... SLICE for multi-dimensional arrays
    // =========================================================================

    @Test
    void foreachSlice_shouldIterateSubArrays() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION test_foreach_slice() RETURNS text "
                + "LANGUAGE plpgsql AS $$ "
                + "DECLARE "
                + "  arr integer[] := ARRAY[[1,2],[3,4],[5,6]]; "
                + "  subarr integer[]; "
                + "  result text := ''; "
                + "BEGIN "
                + "  FOREACH subarr SLICE 1 IN ARRAY arr LOOP "
                + "    result := result || array_to_string(subarr, ',') || ';'; "
                + "  END LOOP; "
                + "  RETURN result; "
                + "END; $$");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT test_foreach_slice()")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            // Should iterate over sub-arrays: [1,2], [3,4], [5,6]
            assertEquals("1,2;3,4;5,6;", result,
                    "FOREACH SLICE 1 should iterate over 1D sub-arrays of a 2D array. Got: " + result);
        }
    }

    // =========================================================================
    // SQL-standard function body (RETURN expr without BEGIN/END)
    // =========================================================================

    @Test
    void sqlStandardFunctionBody_simpleReturn() throws SQLException {
        // PG 14+: CREATE FUNCTION ... RETURN expr (no BEGIN/END)
        exec("CREATE OR REPLACE FUNCTION add_ten(x integer) RETURNS integer "
                + "LANGUAGE sql RETURN x + 10");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT add_ten(5)")) {
            assertTrue(rs.next());
            assertEquals(15, rs.getInt(1),
                    "SQL-standard function body should work: add_ten(5) = 15");
        }
    }

    @Test
    void sqlStandardFunctionBody_beginAtomic() throws SQLException {
        // PG 14+: BEGIN ATOMIC ... END
        exec("CREATE OR REPLACE FUNCTION mult_and_add(a integer, b integer) RETURNS integer "
                + "LANGUAGE sql BEGIN ATOMIC SELECT a * b + 1; END");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT mult_and_add(3, 4)")) {
            assertTrue(rs.next());
            assertEquals(13, rs.getInt(1),
                    "BEGIN ATOMIC function body: 3*4+1 = 13");
        }
    }

    // =========================================================================
    // Procedure transaction control (COMMIT/ROLLBACK inside procedure)
    // =========================================================================

    @Test
    void procedure_commitInsideProcedure_shouldWork() throws SQLException {
        exec("DROP TABLE IF EXISTS ptc_data");
        exec("CREATE TABLE ptc_data (id int, val text)");

        exec("CREATE OR REPLACE PROCEDURE batch_insert() "
                + "LANGUAGE plpgsql AS $$ "
                + "BEGIN "
                + "  INSERT INTO ptc_data VALUES (1, 'first'); "
                + "  COMMIT; "
                + "  INSERT INTO ptc_data VALUES (2, 'second'); "
                + "  COMMIT; "
                + "END; $$");

        exec("CALL batch_insert()");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) FROM ptc_data")) {
            rs.next();
            assertEquals(2, rs.getInt(1),
                    "Both inserts should be committed");
        }
    }

    @Test
    void procedure_rollbackInsideProcedure_shouldWork() throws SQLException {
        exec("DROP TABLE IF EXISTS ptc_rb");
        exec("CREATE TABLE ptc_rb (id int, val text)");

        exec("CREATE OR REPLACE PROCEDURE partial_rollback() "
                + "LANGUAGE plpgsql AS $$ "
                + "BEGIN "
                + "  INSERT INTO ptc_rb VALUES (1, 'keep'); "
                + "  COMMIT; "
                + "  INSERT INTO ptc_rb VALUES (2, 'discard'); "
                + "  ROLLBACK; "
                + "END; $$");

        exec("CALL partial_rollback()");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) FROM ptc_rb")) {
            rs.next();
            assertEquals(1, rs.getInt(1),
                    "First insert should be committed, second rolled back");
        }
    }
}
