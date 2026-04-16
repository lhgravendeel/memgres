package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 13 gaps: PL/pgSQL features that PG 18 supports but Memgres
 * does not implement (or implements incorrectly).
 *
 * Coverage:
 *   A. CALL INTO                     — capture OUT args of a procedure
 *   B. FOR IN EXECUTE                — loop over dynamic query results
 *   C. SELECT INTO STRICT multi-row  — must raise P0003 too_many_rows
 *   D. SELECT INTO STRICT zero-row   — must raise P0002 no_data_found
 *   E. FOREACH SLICE                 — multi-dim array slicing in loops
 *   F. RETURN QUERY EXECUTE USING    — parameterized dynamic SQL
 *   G. ASSERT variants               — ASSERT expr / ASSERT expr, msg
 *   H. GET STACKED DIAGNOSTICS       — CONTEXT, PG_EXCEPTION_CONTEXT
 *   I. RAISE USING                   — ERRCODE, DETAIL, HINT, COLUMN, etc.
 *   J. GET DIAGNOSTICS               — ROW_COUNT, PG_CONTEXT
 *   K. Composite-type return         — RETURN NEXT with record
 */
class Round13PlpgsqlGapsTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getString(1);
        }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // A. CALL INTO — capture OUT args into variables
    // =========================================================================

    @Test
    void call_into_capturesOutArg() throws SQLException {
        exec("DROP PROCEDURE IF EXISTS r13_proc_out(int, OUT int)");
        exec("CREATE PROCEDURE r13_proc_out(x int, OUT y int) "
                + "LANGUAGE plpgsql AS $$ BEGIN y := x * 10; END $$");
        exec("DROP FUNCTION IF EXISTS r13_call_into_fn()");
        exec("CREATE FUNCTION r13_call_into_fn() RETURNS int LANGUAGE plpgsql AS $$ "
                + "DECLARE result int; "
                + "BEGIN "
                + "  CALL r13_proc_out(7, result); "
                + "  RETURN result; "
                + "END $$");
        assertEquals(70, scalarInt("SELECT r13_call_into_fn()"));
    }

    // =========================================================================
    // B. FOR IN EXECUTE
    // =========================================================================

    @Test
    void for_in_execute_iteratesDynamicQuery() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_for_exec_fn()");
        exec("CREATE FUNCTION r13_for_exec_fn() RETURNS int LANGUAGE plpgsql AS $$ "
                + "DECLARE total int := 0; r record; "
                + "BEGIN "
                + "  FOR r IN EXECUTE 'SELECT generate_series(1,5) AS x' LOOP "
                + "    total := total + r.x; "
                + "  END LOOP; "
                + "  RETURN total; "
                + "END $$");
        assertEquals(15, scalarInt("SELECT r13_for_exec_fn()"));
    }

    @Test
    void for_in_execute_using_parameterized() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_for_exec_using(int)");
        exec("CREATE FUNCTION r13_for_exec_using(lim int) RETURNS int LANGUAGE plpgsql AS $$ "
                + "DECLARE total int := 0; r record; "
                + "BEGIN "
                + "  FOR r IN EXECUTE 'SELECT generate_series(1, $1) AS x' USING lim LOOP "
                + "    total := total + r.x; "
                + "  END LOOP; "
                + "  RETURN total; "
                + "END $$");
        assertEquals(6, scalarInt("SELECT r13_for_exec_using(3)"));
    }

    // =========================================================================
    // C/D. SELECT INTO STRICT error codes
    // =========================================================================

    @Test
    void select_into_strict_multiRow_P0003() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_strict; CREATE TABLE r13_strict (id int)");
        exec("INSERT INTO r13_strict VALUES (1), (2)");
        exec("DROP FUNCTION IF EXISTS r13_strict_multi()");
        exec("CREATE FUNCTION r13_strict_multi() RETURNS int LANGUAGE plpgsql AS $$ "
                + "DECLARE v int; "
                + "BEGIN "
                + "  SELECT id INTO STRICT v FROM r13_strict; "
                + "  RETURN v; "
                + "END $$");
        SQLException ex = assertThrows(SQLException.class,
                () -> scalarInt("SELECT r13_strict_multi()"));
        assertEquals("P0003", ex.getSQLState(),
                "multi-row SELECT INTO STRICT must raise P0003 too_many_rows");
    }

    @Test
    void select_into_strict_zeroRow_P0002() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_strict0; CREATE TABLE r13_strict0 (id int)");
        exec("DROP FUNCTION IF EXISTS r13_strict_zero()");
        exec("CREATE FUNCTION r13_strict_zero() RETURNS int LANGUAGE plpgsql AS $$ "
                + "DECLARE v int; "
                + "BEGIN "
                + "  SELECT id INTO STRICT v FROM r13_strict0; "
                + "  RETURN v; "
                + "END $$");
        SQLException ex = assertThrows(SQLException.class,
                () -> scalarInt("SELECT r13_strict_zero()"));
        assertEquals("P0002", ex.getSQLState(),
                "zero-row SELECT INTO STRICT must raise P0002 no_data_found");
    }

    // =========================================================================
    // E. FOREACH SLICE
    // =========================================================================

    @Test
    void foreach_slice1_iteratesRows() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_foreach_slice()");
        exec("CREATE FUNCTION r13_foreach_slice() RETURNS int LANGUAGE plpgsql AS $$ "
                + "DECLARE row int[]; total int := 0; "
                + "BEGIN "
                + "  FOREACH row SLICE 1 IN ARRAY ARRAY[[1,2,3],[4,5,6]] LOOP "
                + "    total := total + row[1] + row[2] + row[3]; "
                + "  END LOOP; "
                + "  RETURN total; "
                + "END $$");
        assertEquals(21, scalarInt("SELECT r13_foreach_slice()"));
    }

    // =========================================================================
    // F. RETURN QUERY EXECUTE ... USING
    // =========================================================================

    @Test
    void return_query_execute_using() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_rq_exec(int)");
        exec("CREATE FUNCTION r13_rq_exec(lim int) RETURNS SETOF int LANGUAGE plpgsql AS $$ "
                + "BEGIN "
                + "  RETURN QUERY EXECUTE 'SELECT generate_series(1, $1)' USING lim; "
                + "END $$");
        int count = 0;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM r13_rq_exec(3)")) {
            while (rs.next()) count++;
        }
        assertEquals(3, count);
    }

    // =========================================================================
    // G. ASSERT variants
    // =========================================================================

    @Test
    void assert_false_raises_P0004() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_assert_fn()");
        exec("CREATE FUNCTION r13_assert_fn() RETURNS void LANGUAGE plpgsql AS $$ "
                + "BEGIN ASSERT 1 = 2; END $$");
        SQLException ex = assertThrows(SQLException.class,
                () -> scalarString("SELECT r13_assert_fn()::text"));
        assertEquals("P0004", ex.getSQLState(),
                "ASSERT failure must raise P0004 assert_failure");
    }

    @Test
    void assert_withMessage_usesMessage() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_assert_msg()");
        exec("CREATE FUNCTION r13_assert_msg() RETURNS void LANGUAGE plpgsql AS $$ "
                + "BEGIN ASSERT 1 = 2, 'custom msg r13'; END $$");
        SQLException ex = assertThrows(SQLException.class,
                () -> scalarString("SELECT r13_assert_msg()::text"));
        assertTrue(ex.getMessage().contains("custom msg r13"),
                "ASSERT message must be propagated; got " + ex.getMessage());
    }

    // =========================================================================
    // H. GET STACKED DIAGNOSTICS — PG_EXCEPTION_CONTEXT
    // =========================================================================

    @Test
    void get_stacked_diagnostics_exception_context() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_gsd_ctx()");
        exec("CREATE FUNCTION r13_gsd_ctx() RETURNS text LANGUAGE plpgsql AS $$ "
                + "DECLARE ctx text; "
                + "BEGIN "
                + "  PERFORM 1/0; "
                + "EXCEPTION WHEN division_by_zero THEN "
                + "  GET STACKED DIAGNOSTICS ctx = PG_EXCEPTION_CONTEXT; "
                + "  RETURN ctx; "
                + "END $$");
        String v = scalarString("SELECT r13_gsd_ctx()");
        assertNotNull(v);
        assertTrue(v.toLowerCase().contains("function") || v.contains("r13_gsd_ctx"),
                "PG_EXCEPTION_CONTEXT must include call stack; got " + v);
    }

    // =========================================================================
    // I. RAISE USING extended fields
    // =========================================================================

    @Test
    void raise_using_column_constraint_fields() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_raise_full()");
        exec("CREATE FUNCTION r13_raise_full() RETURNS void LANGUAGE plpgsql AS $$ "
                + "BEGIN "
                + "  RAISE EXCEPTION 'bad' USING "
                + "    ERRCODE = '23514', "
                + "    DETAIL = 'detail msg', "
                + "    HINT = 'hint msg', "
                + "    COLUMN = 'col_x', "
                + "    CONSTRAINT = 'cstr_y', "
                + "    TABLE = 'tbl_z', "
                + "    SCHEMA = 'sch_w'; "
                + "END $$");
        SQLException ex = assertThrows(SQLException.class,
                () -> scalarString("SELECT r13_raise_full()::text"));
        // Check all extended fields via server message
        if (ex instanceof org.postgresql.util.PSQLException) {
            org.postgresql.util.ServerErrorMessage m =
                    ((org.postgresql.util.PSQLException) ex).getServerErrorMessage();
            assertEquals("cstr_y", m.getConstraint());
            assertEquals("col_x", m.getColumn());
            assertEquals("tbl_z", m.getTable());
            assertEquals("sch_w", m.getSchema());
            assertEquals("detail msg", m.getDetail());
            assertEquals("hint msg", m.getHint());
        }
        assertEquals("23514", ex.getSQLState());
    }

    // =========================================================================
    // J. GET DIAGNOSTICS
    // =========================================================================

    @Test
    void get_diagnostics_row_count() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_diag_t");
        exec("CREATE TABLE r13_diag_t (id int)");
        exec("INSERT INTO r13_diag_t SELECT generate_series(1, 5)");
        exec("DROP FUNCTION IF EXISTS r13_diag_rc()");
        exec("CREATE FUNCTION r13_diag_rc() RETURNS int LANGUAGE plpgsql AS $$ "
                + "DECLARE c int; "
                + "BEGIN "
                + "  UPDATE r13_diag_t SET id = id + 10; "
                + "  GET DIAGNOSTICS c = ROW_COUNT; "
                + "  RETURN c; "
                + "END $$");
        assertEquals(5, scalarInt("SELECT r13_diag_rc()"));
    }

    @Test
    void get_diagnostics_pg_context() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_diag_ctx()");
        exec("CREATE FUNCTION r13_diag_ctx() RETURNS text LANGUAGE plpgsql AS $$ "
                + "DECLARE ctx text; "
                + "BEGIN "
                + "  GET DIAGNOSTICS ctx = PG_CONTEXT; "
                + "  RETURN ctx; "
                + "END $$");
        String v = scalarString("SELECT r13_diag_ctx()");
        assertNotNull(v);
        // PG_CONTEXT returns a stack trace-like text
        assertTrue(v.toLowerCase().contains("pl/pgsql") || v.contains("r13_diag_ctx"),
                "PG_CONTEXT must contain caller info; got " + v);
    }

    // =========================================================================
    // K. RETURN NEXT with composite record
    // =========================================================================

    @Test
    void return_next_composite_record() throws SQLException {
        exec("DROP TYPE IF EXISTS r13_rec_t CASCADE");
        exec("CREATE TYPE r13_rec_t AS (a int, b text)");
        exec("DROP FUNCTION IF EXISTS r13_rn_rec()");
        exec("CREATE FUNCTION r13_rn_rec() RETURNS SETOF r13_rec_t LANGUAGE plpgsql AS $$ "
                + "DECLARE r r13_rec_t; "
                + "BEGIN "
                + "  r.a := 1; r.b := 'x'; RETURN NEXT r; "
                + "  r.a := 2; r.b := 'y'; RETURN NEXT r; "
                + "END $$");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT a, b FROM r13_rn_rec() ORDER BY a")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt("a"));
            assertEquals("x", rs.getString("b"));
            assertTrue(rs.next()); assertEquals(2, rs.getInt("a"));
            assertEquals("y", rs.getString("b"));
            assertFalse(rs.next());
        }
    }
}
