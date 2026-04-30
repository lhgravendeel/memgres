package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap category AH: PL/pgSQL language depth.
 *
 * Covers:
 *  - PERFORM sets FOUND
 *  - GET DIAGNOSTICS v = PG_CONTEXT returns context string
 *  - WHEN OTHERS captures correct SQLSTATE via SQLSTATE variable
 *  - ASSERT false, 'msg' raises with message
 *  - %ROWTYPE / %TYPE resolve to the declared column/row type
 */
class Round18PlpgsqlDepthTest {

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

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // AH1. PERFORM sets FOUND
    // =========================================================================

    @Test
    void perform_sets_found_variable() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_pf");
        exec("CREATE TABLE r18_pf(a int)");
        exec("INSERT INTO r18_pf VALUES (1)");
        exec("DROP FUNCTION IF EXISTS r18_pf_fn()");
        exec("CREATE FUNCTION r18_pf_fn() RETURNS boolean LANGUAGE plpgsql AS $$ " +
                "BEGIN PERFORM * FROM r18_pf WHERE a = 1; RETURN FOUND; END $$");
        String v = str("SELECT r18_pf_fn()::text");
        assertEquals("true", v,
                "PERFORM must set FOUND=true when rows match; got '" + v + "'");

        exec("DROP FUNCTION IF EXISTS r18_pf_fn_neg()");
        exec("CREATE FUNCTION r18_pf_fn_neg() RETURNS boolean LANGUAGE plpgsql AS $$ " +
                "BEGIN PERFORM * FROM r18_pf WHERE a = 999; RETURN FOUND; END $$");
        String v2 = str("SELECT r18_pf_fn_neg()::text");
        assertEquals("false", v2,
                "PERFORM must set FOUND=false when no rows match; got '" + v2 + "'");
    }

    // =========================================================================
    // AH2. GET DIAGNOSTICS PG_CONTEXT
    // =========================================================================

    @Test
    void get_diagnostics_pg_context_returns_context() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r18_pc_fn()");
        exec("CREATE FUNCTION r18_pc_fn() RETURNS text LANGUAGE plpgsql AS $$ " +
                "DECLARE ctx text; " +
                "BEGIN GET DIAGNOSTICS ctx = PG_CONTEXT; RETURN ctx; END $$");
        String v = str("SELECT r18_pc_fn()");
        assertNotNull(v, "PG_CONTEXT must return a non-null string");
        assertTrue(v.toLowerCase().contains("r18_pc_fn") || v.toLowerCase().contains("function"),
                "PG_CONTEXT must mention the calling function; got '" + v + "'");
    }

    // =========================================================================
    // AH3. WHEN OTHERS captures SQLSTATE
    // =========================================================================

    @Test
    void when_others_captures_sqlstate() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r18_wo_fn()");
        exec("CREATE FUNCTION r18_wo_fn() RETURNS text LANGUAGE plpgsql AS $$ " +
                "DECLARE s text; " +
                "BEGIN " +
                "  BEGIN PERFORM 1/0; " +
                "  EXCEPTION WHEN OTHERS THEN s := SQLSTATE; RETURN s; END; " +
                "END $$");
        String v = str("SELECT r18_wo_fn()");
        assertEquals("22012", v,
                "WHEN OTHERS must capture SQLSTATE 22012 (divide by zero); got '" + v + "'");
    }

    // =========================================================================
    // AH4. ASSERT false raises with message
    // =========================================================================

    @Test
    void assert_false_raises_with_message() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r18_as_fn()");
        exec("CREATE FUNCTION r18_as_fn() RETURNS void LANGUAGE plpgsql AS $$ " +
                "BEGIN ASSERT false, 'r18_assert_msg'; END $$");
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT r18_as_fn()");
            fail("ASSERT false must raise");
        } catch (SQLException e) {
            assertTrue(e.getMessage() != null && e.getMessage().contains("r18_assert_msg"),
                    "ASSERT message 'r18_assert_msg' must surface in the error; got '" + e.getMessage() + "'");
            // PG: SQLSTATE for assert failure is P0004 (assert_failure).
            assertEquals("P0004", e.getSQLState(),
                    "ASSERT failure SQLSTATE must be P0004; got " + e.getSQLState());
        }
    }

    // =========================================================================
    // AH5. %TYPE resolution
    // =========================================================================

    @Test
    void type_attribute_resolves_column_type() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_tt");
        exec("CREATE TABLE r18_tt(id bigint, label text)");
        exec("DROP FUNCTION IF EXISTS r18_tt_fn()");
        // If r18_tt.id is bigint, v := 9223372036854775807 (max bigint) must fit.
        exec("CREATE FUNCTION r18_tt_fn() RETURNS text LANGUAGE plpgsql AS $$ " +
                "DECLARE v r18_tt.id%TYPE; " +
                "BEGIN v := 9223372036854775807; RETURN v::text; END $$");
        String v = str("SELECT r18_tt_fn()");
        assertEquals("9223372036854775807", v,
                "%TYPE must resolve to bigint allowing max bigint value; got '" + v + "'");
    }
}
