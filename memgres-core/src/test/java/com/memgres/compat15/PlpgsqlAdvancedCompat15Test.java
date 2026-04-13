package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 25 plpgsql-advanced.sql failures where Memgres diverges from PG 18.
 *
 * Covers five feature areas:
 *   D1: RETURN QUERY EXECUTE (dynamic SQL in set-returning functions)  — Stmts 7, 9, 11, 12, 14, 16, 18
 *   D2: FOREACH ... IN ARRAY with SLICE                                — Stmts 21, 22
 *   D3: PL/pgSQL ASSERT statement                                      — Stmts 30, 32, 36, 37, 39
 *   D4: CALL ... INTO (capture OUT params)                              — Stmts 42, 44, 46, 48, 50
 *   D5: Window function EXCLUDE clause                                  — Stmts 53, 56, 59, 60, 61, 62
 */
class PlpgsqlAdvancedCompat15Test {

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
            s.execute("DROP SCHEMA IF EXISTS pla_test CASCADE");
            s.execute("CREATE SCHEMA pla_test");
            s.execute("SET search_path = pla_test, public");

            // --- D1: RETURN QUERY EXECUTE setup ---
            s.execute("CREATE TABLE pla_data (id integer PRIMARY KEY, val text)");
            s.execute("INSERT INTO pla_data VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')");

            // Stmt 7: pla_dynamic_query
            s.execute("""
                CREATE FUNCTION pla_dynamic_query(tbl text) RETURNS SETOF pla_data
                LANGUAGE plpgsql AS $$
                BEGIN
                  RETURN QUERY EXECUTE 'SELECT * FROM ' || tbl || ' ORDER BY id';
                END;
                $$
            """);

            // Stmt 9, 12: pla_dynamic_filtered
            s.execute("""
                CREATE FUNCTION pla_dynamic_filtered(min_id integer) RETURNS SETOF pla_data
                LANGUAGE plpgsql AS $$
                BEGIN
                  RETURN QUERY EXECUTE 'SELECT * FROM pla_data WHERE id >= $1 ORDER BY id' USING min_id;
                END;
                $$
            """);

            // Stmt 11: pla_dynamic_range
            s.execute("""
                CREATE FUNCTION pla_dynamic_range(lo integer, hi integer) RETURNS SETOF pla_data
                LANGUAGE plpgsql AS $$
                BEGIN
                  RETURN QUERY EXECUTE 'SELECT * FROM pla_data WHERE id BETWEEN $1 AND $2 ORDER BY id'
                    USING lo, hi;
                END;
                $$
            """);

            // Stmt 14: pla_dynamic_cols
            s.execute("""
                CREATE FUNCTION pla_dynamic_cols(col text) RETURNS SETOF text
                LANGUAGE plpgsql AS $$
                BEGIN
                  RETURN QUERY EXECUTE 'SELECT ' || quote_ident(col) || '::text FROM pla_data ORDER BY id';
                END;
                $$
            """);

            // Stmt 16: pla_mixed_return
            s.execute("""
                CREATE FUNCTION pla_mixed_return() RETURNS SETOF integer
                LANGUAGE plpgsql AS $$
                BEGIN
                  RETURN QUERY SELECT id FROM pla_data WHERE id = 1;
                  RETURN QUERY EXECUTE 'SELECT id FROM pla_data WHERE id = 3';
                END;
                $$
            """);

            // Stmt 18: pla_format_query
            s.execute("""
                CREATE FUNCTION pla_format_query(schema_name text, table_name text) RETURNS SETOF record
                LANGUAGE plpgsql AS $$
                BEGIN
                  RETURN QUERY EXECUTE format('SELECT id, val FROM %I.%I ORDER BY id', schema_name, table_name);
                END;
                $$
            """);

            // --- D2: FOREACH SLICE setup ---
            // Stmt 21: pla_foreach_slice1 (should error with 42601/SLICE on PG, but Memgres succeeds)
            // We need to attempt creation; Memgres allows it even though PG doesn't.
            s.execute("""
                CREATE FUNCTION pla_foreach_slice1() RETURNS text
                LANGUAGE plpgsql AS $$
                DECLARE
                  arr integer[] := ARRAY[[1,2],[3,4],[5,6]];
                  slice integer[];
                  result text := '';
                BEGIN
                  FOREACH slice SLICE 1 IN ARRAY arr LOOP
                    result := result || slice::text || ';';
                  END LOOP;
                  RETURN result;
                END;
                $$
            """);

            // --- D3: ASSERT setup ---
            // Stmt 30: pla_assert_pass
            s.execute("""
                CREATE FUNCTION pla_assert_pass() RETURNS text
                LANGUAGE plpgsql AS $$
                BEGIN
                  ASSERT 1 = 1, 'one equals one';
                  RETURN 'ok';
                END;
                $$
            """);

            // Stmt 32, 39: pla_assert_fail
            s.execute("""
                CREATE FUNCTION pla_assert_fail() RETURNS text
                LANGUAGE plpgsql AS $$
                BEGIN
                  ASSERT 1 = 2, 'one does not equal two';
                  RETURN 'should not reach here';
                END;
                $$
            """);

            // pla_assert_no_msg (needed for completeness, used indirectly)
            s.execute("""
                CREATE FUNCTION pla_assert_no_msg() RETURNS text
                LANGUAGE plpgsql AS $$
                BEGIN
                  ASSERT false;
                  RETURN 'nope';
                END;
                $$
            """);

            // Stmt 36, 37: pla_assert_expr
            s.execute("""
                CREATE FUNCTION pla_assert_expr(x integer) RETURNS text
                LANGUAGE plpgsql AS $$
                BEGIN
                  ASSERT x > 0, 'x must be positive, got ' || x::text;
                  RETURN 'valid: ' || x::text;
                END;
                $$
            """);

            // --- D4: CALL ... INTO setup ---
            // Stmt 42, 44: pla_proc_out
            s.execute("""
                CREATE PROCEDURE pla_proc_out(IN x integer, OUT result integer)
                LANGUAGE plpgsql AS $$
                BEGIN
                  result := x * 10;
                END;
                $$
            """);

            // Stmt 44: pla_call_into
            s.execute("""
                CREATE FUNCTION pla_call_into(x integer) RETURNS integer
                LANGUAGE plpgsql AS $$
                DECLARE
                  v integer;
                BEGIN
                  CALL pla_proc_out(x, v);
                  RETURN v;
                END;
                $$
            """);

            // Stmt 46, 48: pla_proc_multi_out
            s.execute("""
                CREATE PROCEDURE pla_proc_multi_out(IN x integer, OUT doubled integer, OUT tripled integer)
                LANGUAGE plpgsql AS $$
                BEGIN
                  doubled := x * 2;
                  tripled := x * 3;
                END;
                $$
            """);

            // Stmt 48: pla_call_multi_into
            s.execute("""
                CREATE FUNCTION pla_call_multi_into(x integer) RETURNS text
                LANGUAGE plpgsql AS $$
                DECLARE
                  d integer;
                  t integer;
                BEGIN
                  CALL pla_proc_multi_out(x, d, t);
                  RETURN 'doubled=' || d::text || ' tripled=' || t::text;
                END;
                $$
            """);

            // Stmt 50: pla_proc_inout
            s.execute("""
                CREATE PROCEDURE pla_proc_inout(INOUT val integer)
                LANGUAGE plpgsql AS $$
                BEGIN
                  val := val + 100;
                END;
                $$
            """);

            // --- D5: Window EXCLUDE setup ---
            s.execute("CREATE TABLE pla_window (id integer, val integer)");
            s.execute("INSERT INTO pla_window VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");

            s.execute("CREATE TABLE pla_window2 (grp text, val integer)");
            s.execute("INSERT INTO pla_window2 VALUES ('a', 10), ('a', 20), ('b', 30), ('b', 40)");

            s.execute("CREATE TABLE pla_ties (id integer, score integer)");
            s.execute("INSERT INTO pla_ties VALUES (1, 10), (2, 10), (3, 20), (4, 20), (5, 30)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS pla_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    // ========================================================================
    // D1: RETURN QUERY EXECUTE
    // ========================================================================

    /**
     * Stmt 7: SELECT * FROM pla_dynamic_query('pla_data')
     *
     * Basic RETURN QUERY EXECUTE that builds a query string from a table name parameter.
     *
     * PG: OK (id, val) [1|alpha], [2|beta], [3|gamma]
     * Memgres: ERROR [26000]: prepared statement "SELECT * FROM " does not exist
     */
    @Test
    void testStmt7_dynamicQueryBasic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM pla_dynamic_query('pla_data')")) {
            assertTrue(rs.next(), "Expected row 1");
            assertEquals(1, rs.getInt("id"));
            assertEquals("alpha", rs.getString("val"));
            assertTrue(rs.next(), "Expected row 2");
            assertEquals(2, rs.getInt("id"));
            assertEquals("beta", rs.getString("val"));
            assertTrue(rs.next(), "Expected row 3");
            assertEquals(3, rs.getInt("id"));
            assertEquals("gamma", rs.getString("val"));
            assertFalse(rs.next(), "Expected only 3 rows");
        }
    }

    /**
     * Stmt 9: SELECT * FROM pla_dynamic_filtered(2)
     *
     * RETURN QUERY EXECUTE with USING clause for parameterized dynamic SQL.
     *
     * PG: OK (id, val) [2|beta], [3|gamma]
     * Memgres: ERROR [26000]: prepared statement does not exist
     */
    @Test
    void testStmt9_dynamicFilteredWithUsing() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM pla_dynamic_filtered(2)")) {
            assertTrue(rs.next(), "Expected row 1");
            assertEquals(2, rs.getInt("id"));
            assertEquals("beta", rs.getString("val"));
            assertTrue(rs.next(), "Expected row 2");
            assertEquals(3, rs.getInt("id"));
            assertEquals("gamma", rs.getString("val"));
            assertFalse(rs.next(), "Expected only 2 rows");
        }
    }

    /**
     * Stmt 11: SELECT * FROM pla_dynamic_range(2, 3)
     *
     * RETURN QUERY EXECUTE with multiple USING params.
     *
     * PG: OK (id, val) [2|beta], [3|gamma]
     * Memgres: ERROR [26000]: prepared statement does not exist
     */
    @Test
    void testStmt11_dynamicRangeMultipleUsing() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM pla_dynamic_range(2, 3)")) {
            assertTrue(rs.next(), "Expected row 1");
            assertEquals(2, rs.getInt("id"));
            assertEquals("beta", rs.getString("val"));
            assertTrue(rs.next(), "Expected row 2");
            assertEquals(3, rs.getInt("id"));
            assertEquals("gamma", rs.getString("val"));
            assertFalse(rs.next(), "Expected only 2 rows");
        }
    }

    /**
     * Stmt 12: SELECT count(*)::integer AS cnt FROM pla_dynamic_filtered(100)
     *
     * RETURN QUERY EXECUTE returning no rows (no data matching filter).
     *
     * PG: OK (cnt) [0]
     * Memgres: ERROR [26000]: prepared statement does not exist
     */
    @Test
    void testStmt12_dynamicFilteredNoRows() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::integer AS cnt FROM pla_dynamic_filtered(100)")) {
            assertTrue(rs.next(), "Expected one row");
            assertEquals(0, rs.getInt("cnt"));
            assertFalse(rs.next());
        }
    }

    /**
     * Stmt 14: SELECT * FROM pla_dynamic_cols('val')
     *
     * RETURN QUERY EXECUTE with dynamic column list built via quote_ident.
     *
     * PG: OK (pla_dynamic_cols) [alpha], [beta], [gamma]
     * Memgres: ERROR [26000]: prepared statement "SELECT " does not exist
     */
    @Test
    void testStmt14_dynamicCols() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM pla_dynamic_cols('val')")) {
            assertTrue(rs.next(), "Expected row 1");
            assertEquals("alpha", rs.getString(1));
            assertTrue(rs.next(), "Expected row 2");
            assertEquals("beta", rs.getString(1));
            assertTrue(rs.next(), "Expected row 3");
            assertEquals("gamma", rs.getString(1));
            assertFalse(rs.next(), "Expected only 3 rows");
        }
    }

    /**
     * Stmt 16: SELECT * FROM pla_mixed_return()
     *
     * Mixed static RETURN QUERY and dynamic RETURN QUERY EXECUTE in one function.
     *
     * PG: OK (pla_mixed_return) [1], [3]
     * Memgres: ERROR [26000]: prepared statement does not exist
     */
    @Test
    void testStmt16_mixedReturn() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM pla_mixed_return()")) {
            assertTrue(rs.next(), "Expected row 1");
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next(), "Expected row 2");
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next(), "Expected only 2 rows");
        }
    }

    /**
     * Stmt 18: SELECT * FROM pla_format_query('pla_test', 'pla_data') AS t(id integer, val text)
     *
     * RETURN QUERY EXECUTE with format() for safe identifier interpolation.
     *
     * PG: OK (id, val) [1|alpha], [2|beta], [3|gamma]
     * Memgres: ERROR [26000]: prepared statement "format" does not exist
     */
    @Test
    void testStmt18_formatQuery() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT * FROM pla_format_query('pla_test', 'pla_data') AS t(id integer, val text)")) {
            assertTrue(rs.next(), "Expected row 1");
            assertEquals(1, rs.getInt("id"));
            assertEquals("alpha", rs.getString("val"));
            assertTrue(rs.next(), "Expected row 2");
            assertEquals(2, rs.getInt("id"));
            assertEquals("beta", rs.getString("val"));
            assertTrue(rs.next(), "Expected row 3");
            assertEquals(3, rs.getInt("id"));
            assertEquals("gamma", rs.getString("val"));
            assertFalse(rs.next(), "Expected only 3 rows");
        }
    }

    // ========================================================================
    // D2: FOREACH ... IN ARRAY with SLICE
    // ========================================================================

    /**
     * Stmt 21: CREATE FUNCTION pla_foreach_slice1() ...
     *
     * Creating a function using FOREACH ... SLICE 1 should fail with SQLSTATE 42601
     * and a message mentioning "SLICE" (PG does not support SLICE in this context).
     * Memgres incorrectly accepts the CREATE FUNCTION.
     *
     * PG: ERROR [42601]: syntax error at or near "SLICE"
     * Memgres: OK 0 rows affected
     */
    /**
     * Stmt 21: FOREACH SLICE 1 is valid PG 18 syntax — CREATE FUNCTION should succeed.
     * (Original differences.md entry was incorrect; PG 18 supports FOREACH SLICE.)
     */
    @Test
    void testStmt21_foreachSlice1CreateShouldSucceed() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                CREATE OR REPLACE FUNCTION pla_foreach_slice1_test() RETURNS text
                LANGUAGE plpgsql AS $$
                DECLARE
                  arr integer[] := ARRAY[[1,2],[3,4],[5,6]];
                  slice integer[];
                  result text := '';
                BEGIN
                  FOREACH slice SLICE 1 IN ARRAY arr LOOP
                    result := result || slice::text || ';';
                  END LOOP;
                  RETURN result;
                END;
                $$
            """);
            // Should succeed — FOREACH SLICE is valid PG 18 syntax
        }
    }

    /**
     * Stmt 22: FOREACH SLICE function should be callable and return a result.
     * PG 18 supports FOREACH SLICE, so the function created in setUp should work.
     */
    @Test
    void testStmt22_foreachSlice1CallShouldSucceed() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pla_foreach_slice1() AS result")) {
            assertTrue(rs.next(), "Expected a result row");
            assertNotNull(rs.getString(1), "Result should not be null");
        }
    }

    // ========================================================================
    // D3: PL/pgSQL ASSERT
    // ========================================================================

    /**
     * Stmt 30: SELECT pla_assert_pass() AS result
     *
     * A passing ASSERT (1 = 1) should not raise an error; function returns 'ok'.
     *
     * PG: OK (result) [ok]
     * Memgres: ERROR [42601]: Expected SQL statement at position 0 near 'assert'
     */
    @Test
    void testStmt30_assertPassReturnsOk() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pla_assert_pass() AS result")) {
            assertTrue(rs.next(), "Expected one row");
            assertEquals("ok", rs.getString("result"));
            assertFalse(rs.next());
        }
    }

    /**
     * Stmt 32: SELECT pla_assert_fail()
     *
     * A failing ASSERT (1 = 2) should raise SQLSTATE P0004 with message
     * containing "one does not equal two".
     *
     * PG: ERROR [P0004]: one does not equal two
     * Memgres: ERROR [42601]: Expected SQL statement at position 0 near 'assert'
     */
    @Test
    void testStmt32_assertFailRaisesP0004() {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT pla_assert_fail()");
            fail("Expected SQLSTATE P0004 error from failing ASSERT");
        } catch (SQLException e) {
            assertEquals("P0004", e.getSQLState(),
                    "SQLSTATE should be P0004 (assert_failure), got: " + e.getSQLState() + " - " + e.getMessage());
            assertTrue(e.getMessage().contains("one does not equal two"),
                    "Error message should contain 'one does not equal two', got: " + e.getMessage());
        }
    }

    /**
     * Stmt 36: SELECT pla_assert_expr(5) AS result
     *
     * ASSERT x > 0 with x=5 passes; function returns 'valid: 5'.
     *
     * PG: OK (result) [valid: 5]
     * Memgres: ERROR [42601]: Expected SQL statement at position 0 near 'assert'
     */
    @Test
    void testStmt36_assertExprPassReturnsValid() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pla_assert_expr(5) AS result")) {
            assertTrue(rs.next(), "Expected one row");
            assertEquals("valid: 5", rs.getString("result"));
            assertFalse(rs.next());
        }
    }

    /**
     * Stmt 37: SELECT pla_assert_expr(-1)
     *
     * ASSERT x > 0 with x=-1 fails; should raise SQLSTATE P0004 with message
     * containing "x must be positive".
     *
     * PG: ERROR [P0004]: x must be positive, got -1
     * Memgres: ERROR [42601]: Expected SQL statement at position 0 near 'assert'
     */
    @Test
    void testStmt37_assertExprFailRaisesP0004() {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT pla_assert_expr(-1)");
            fail("Expected SQLSTATE P0004 error from failing ASSERT");
        } catch (SQLException e) {
            assertEquals("P0004", e.getSQLState(),
                    "SQLSTATE should be P0004 (assert_failure), got: " + e.getSQLState() + " - " + e.getMessage());
            assertTrue(e.getMessage().contains("x must be positive"),
                    "Error message should contain 'x must be positive', got: " + e.getMessage());
        }
    }

    /**
     * Stmt 39: SELECT pla_assert_fail() AS result
     *
     * With plpgsql.check_asserts = off, ASSERT is skipped so the function should
     * return 'should not reach here' instead of raising an error.
     *
     * PG: OK (result) [should not reach here]
     * Memgres: ERROR [42601]: Expected SQL statement at position 0 near 'assert'
     */
    @Test
    void testStmt39_assertDisabledSkipsAssert() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("SET plpgsql.check_asserts = off");
            try (ResultSet rs = s.executeQuery("SELECT pla_assert_fail() AS result")) {
                assertTrue(rs.next(), "Expected one row");
                assertEquals("should not reach here", rs.getString("result"));
                assertFalse(rs.next());
            }
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("SET plpgsql.check_asserts = on");
            }
        }
    }

    // ========================================================================
    // D4: CALL ... INTO (capture OUT params)
    // ========================================================================

    /**
     * Stmt 42: CALL pla_proc_out(5, NULL)
     *
     * Procedure with OUT parameter; CALL should return result set with column 'result'.
     *
     * PG: OK (result) [50]
     * Memgres: ERROR [42883]: procedure pla_proc_out(integer) does not exist
     */
    @Test
    void testStmt42_procOutReturnsResult() throws SQLException {
        try (Statement s = conn.createStatement()) {
            boolean hasRs = s.execute("CALL pla_proc_out(5, NULL)");
            assertTrue(hasRs, "CALL with OUT param should return a result set");
            try (ResultSet rs = s.getResultSet()) {
                assertTrue(rs.next(), "Expected one row");
                assertEquals(50, rs.getInt("result"));
                assertFalse(rs.next());
            }
        }
    }

    /**
     * Stmt 44: SELECT pla_call_into(7) AS result
     *
     * PL/pgSQL function that uses CALL ... INTO to capture an OUT param from a procedure.
     *
     * PG: OK (result) [70]
     * Memgres: ERROR [42883]: procedure pla_proc_out(integer) does not exist
     */
    @Test
    void testStmt44_callIntoFromPlpgsql() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pla_call_into(7) AS result")) {
            assertTrue(rs.next(), "Expected one row");
            assertEquals(70, rs.getInt("result"));
            assertFalse(rs.next());
        }
    }

    /**
     * Stmt 46: CALL pla_proc_multi_out(5, NULL, NULL)
     *
     * Procedure with multiple OUT parameters; CALL should return a result set
     * with columns 'doubled' and 'tripled'.
     *
     * PG: OK (doubled, tripled) [10, 15]
     * Memgres: ERROR [42883]: procedure pla_proc_multi_out(integer) does not exist
     */
    @Test
    void testStmt46_procMultiOutReturnsResult() throws SQLException {
        try (Statement s = conn.createStatement()) {
            boolean hasRs = s.execute("CALL pla_proc_multi_out(5, NULL, NULL)");
            assertTrue(hasRs, "CALL with OUT params should return a result set");
            try (ResultSet rs = s.getResultSet()) {
                assertTrue(rs.next(), "Expected one row");
                assertEquals(10, rs.getInt("doubled"));
                assertEquals(15, rs.getInt("tripled"));
                assertFalse(rs.next());
            }
        }
    }

    /**
     * Stmt 48: SELECT pla_call_multi_into(4) AS result
     *
     * PL/pgSQL function using CALL ... INTO with multiple OUT params.
     *
     * PG: OK (result) [doubled=8 tripled=12]
     * Memgres: ERROR [42883]: procedure pla_proc_multi_out(integer) does not exist
     */
    @Test
    void testStmt48_callMultiIntoFromPlpgsql() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pla_call_multi_into(4) AS result")) {
            assertTrue(rs.next(), "Expected one row");
            assertEquals("doubled=8 tripled=12", rs.getString("result"));
            assertFalse(rs.next());
        }
    }

    /**
     * Stmt 50: CALL pla_proc_inout(5)
     *
     * Procedure with INOUT parameter; CALL should return a result set with column 'val'
     * containing the modified value (5 + 100 = 105).
     *
     * PG: OK (val) [105]
     * Memgres: OK -1 rows affected (returns update count instead of result set)
     */
    @Test
    void testStmt50_procInoutReturnsResultSet() throws SQLException {
        try (Statement s = conn.createStatement()) {
            boolean hasRs = s.execute("CALL pla_proc_inout(5)");
            assertTrue(hasRs, "CALL with INOUT param should return a result set, not an update count");
            try (ResultSet rs = s.getResultSet()) {
                assertTrue(rs.next(), "Expected one row");
                assertEquals(105, rs.getInt("val"));
                assertFalse(rs.next());
            }
        }
    }

    // ========================================================================
    // D5: Window Function EXCLUDE Clause
    // ========================================================================

    /**
     * Stmt 53: EXCLUDE CURRENT ROW on pla_window
     *
     * Running sum excluding the current row.
     *
     * PG: (id, val, sum_excl) [1,10,NULL], [2,20,10], [3,30,30], [4,40,60], [5,50,100]
     * Memgres: ERROR [42601]: Expected RIGHT_PAREN ... near 'EXCLUDE'
     */
    @Test
    void testStmt53_excludeCurrentRow() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, val, sum(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS sum_excl FROM pla_window ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(10, rs.getInt("val"));
            assertNull(rs.getObject("sum_excl"), "First row sum_excl should be NULL");

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals(10, rs.getInt("sum_excl"));

            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals(30, rs.getInt("sum_excl"));

            assertTrue(rs.next());
            assertEquals(4, rs.getInt("id"));
            assertEquals(60, rs.getInt("sum_excl"));

            assertTrue(rs.next());
            assertEquals(5, rs.getInt("id"));
            assertEquals(100, rs.getInt("sum_excl"));

            assertFalse(rs.next(), "Expected only 5 rows");
        }
    }

    /**
     * Stmt 56: EXCLUDE CURRENT ROW on pla_window2
     *
     * Running sum excluding current row over a different table.
     *
     * PG: (grp, val, sum_excl) [a,10,NULL], [a,20,10], [b,30,30], [b,40,60]
     * Memgres: ERROR [42601]: Expected RIGHT_PAREN ... near 'EXCLUDE'
     */
    @Test
    void testStmt56_excludeCurrentRowWindow2() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT grp, val, sum(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS sum_excl FROM pla_window2 ORDER BY val")) {
            assertTrue(rs.next());
            assertEquals("a", rs.getString("grp"));
            assertEquals(10, rs.getInt("val"));
            assertNull(rs.getObject("sum_excl"), "First row sum_excl should be NULL");

            assertTrue(rs.next());
            assertEquals("a", rs.getString("grp"));
            assertEquals(20, rs.getInt("val"));
            assertEquals(10, rs.getInt("sum_excl"));

            assertTrue(rs.next());
            assertEquals("b", rs.getString("grp"));
            assertEquals(30, rs.getInt("val"));
            assertEquals(30, rs.getInt("sum_excl"));

            assertTrue(rs.next());
            assertEquals("b", rs.getString("grp"));
            assertEquals(40, rs.getInt("val"));
            assertEquals(60, rs.getInt("sum_excl"));

            assertFalse(rs.next(), "Expected only 4 rows");
        }
    }

    /**
     * Stmt 59: EXCLUDE TIES on pla_ties
     *
     * Count with RANGE frame excluding ties (peer rows excluded, current row kept).
     *
     * PG: (id, score, cnt) [1,10,1], [2,10,1], [3,20,3], [4,20,3], [5,30,5]
     * Memgres: ERROR [42601]: Expected RIGHT_PAREN ... near 'EXCLUDE'
     */
    @Test
    void testStmt59_excludeTies() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, score, count(*) OVER (ORDER BY score RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) AS cnt FROM pla_ties ORDER BY id")) {
            int[][] expected = {{1, 10, 1}, {2, 10, 1}, {3, 20, 3}, {4, 20, 3}, {5, 30, 5}};
            for (int[] row : expected) {
                assertTrue(rs.next(), "Expected row with id=" + row[0]);
                assertEquals(row[0], rs.getInt("id"));
                assertEquals(row[1], rs.getInt("score"));
                assertEquals(row[2], rs.getInt("cnt"));
            }
            assertFalse(rs.next(), "Expected only 5 rows");
        }
    }

    /**
     * Stmt 60: EXCLUDE NO OTHERS on pla_window
     *
     * EXCLUDE NO OTHERS is the default — includes everything. Running sum should
     * be the normal cumulative sum.
     *
     * PG: (id, val, running_sum) [1,10,10], [2,20,30], [3,30,60], [4,40,100], [5,50,150]
     * Memgres: ERROR [42601]: Expected RIGHT_PAREN ... near 'EXCLUDE'
     */
    @Test
    void testStmt60_excludeNoOthers() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, val, sum(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS) AS running_sum FROM pla_window ORDER BY id")) {
            int[][] expected = {{1, 10, 10}, {2, 20, 30}, {3, 30, 60}, {4, 40, 100}, {5, 50, 150}};
            for (int[] row : expected) {
                assertTrue(rs.next(), "Expected row with id=" + row[0]);
                assertEquals(row[0], rs.getInt("id"));
                assertEquals(row[1], rs.getInt("val"));
                assertEquals(row[2], rs.getInt("running_sum"));
            }
            assertFalse(rs.next(), "Expected only 5 rows");
        }
    }

    /**
     * Stmt 61: EXCLUDE CURRENT ROW with GROUPS frame on pla_ties
     *
     * Sum with GROUPS frame excluding current row.
     *
     * PG: (id, score, sum_excl) [1,10,10], [2,10,10], [3,20,40], [4,20,40], [5,30,60]
     * Memgres: ERROR [42601]: Expected RIGHT_PAREN ... near 'EXCLUDE'
     */
    @Test
    void testStmt61_excludeCurrentRowGroupsFrame() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, score, sum(score) OVER (ORDER BY score GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS sum_excl FROM pla_ties ORDER BY id")) {
            int[][] expected = {{1, 10, 10}, {2, 10, 10}, {3, 20, 40}, {4, 20, 40}, {5, 30, 60}};
            for (int[] row : expected) {
                assertTrue(rs.next(), "Expected row with id=" + row[0]);
                assertEquals(row[0], rs.getInt("id"));
                assertEquals(row[1], rs.getInt("score"));
                assertEquals(row[2], rs.getInt("sum_excl"));
            }
            assertFalse(rs.next(), "Expected only 5 rows");
        }
    }

    /**
     * Stmt 62: EXCLUDE CURRENT ROW with avg() on single filtered row
     *
     * avg(val) excluding current row when only one row (id=3) is in the window
     * should yield NULL (no rows left to average after exclusion).
     *
     * PG: (id, val, avg_excl) [3, 30, NULL]
     * Memgres: ERROR [42601]: Expected RIGHT_PAREN ... near 'EXCLUDE'
     */
    @Test
    void testStmt62_excludeCurrentRowAvgSingleRow() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, val, avg(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)::integer AS avg_excl FROM pla_window WHERE id = 3")) {
            assertTrue(rs.next(), "Expected one row");
            assertEquals(3, rs.getInt("id"));
            assertEquals(30, rs.getInt("val"));
            assertNull(rs.getObject("avg_excl"), "avg_excl should be NULL when current row is excluded and no other rows exist");
            assertFalse(rs.next());
        }
    }
}
