package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category A: PL/pgSQL procedural surface fidelity.
 *
 * Covers:
 *  - RAISE DEBUG must be wire-routed as NoticeResponse when client_min_messages=debug1
 *  - RAISE '%%' must collapse to '%'
 *  - RAISE with extra args must raise 42601 too many parameters for RAISE
 *  - Bare `RAISE;` re-raise must preserve column/constraint/datatype/table/schema
 *  - GET DIAGNOSTICS RESULT_OID
 *  - GET STACKED DIAGNOSTICS PG_EXCEPTION_CONTEXT (real stack, not fixed literal)
 *  - client_min_messages GUC must filter NOTICE/DEBUG on the wire
 */
class Round16PlpgsqlProceduralTest {

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

    /** Collect all SQLWarnings chained on the Statement. */
    private static String warningChain(Statement s) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (SQLWarning w = s.getWarnings(); w != null; w = w.getNextWarning()) {
            if (sb.length() > 0) sb.append("||");
            sb.append(w.getMessage());
        }
        return sb.toString();
    }

    // =========================================================================
    // A1. RAISE levels and wire routing
    // =========================================================================

    @Test
    void raise_debug_reaches_client_under_debug1() throws SQLException {
        exec("SET client_min_messages = debug1");
        exec("CREATE OR REPLACE FUNCTION r16_raise_debug() RETURNS void AS $$\n"
                + "BEGIN RAISE DEBUG 'debug-payload-xyzzy'; END;\n"
                + "$$ LANGUAGE plpgsql");
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT r16_raise_debug()");
            String chain = warningChain(s);
            assertTrue(chain.contains("debug-payload-xyzzy"),
                    "RAISE DEBUG must surface as NoticeResponse under client_min_messages=debug1; got: " + chain);
        } finally {
            exec("SET client_min_messages = notice");
        }
    }

    @Test
    void raise_percent_percent_collapses_to_single_percent() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r16_raise_pct() RETURNS void AS $$\n"
                + "BEGIN RAISE NOTICE '100%%'; END;\n"
                + "$$ LANGUAGE plpgsql");
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT r16_raise_pct()");
            String chain = warningChain(s);
            assertTrue(chain.contains("100%") && !chain.contains("100%%"),
                    "RAISE '%%' must collapse to literal '%'; got: " + chain);
        }
    }

    @Test
    void raise_extra_args_must_error_42601() throws SQLException {
        // PG validates RAISE format string vs argument count at CREATE FUNCTION time
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE FUNCTION r16_raise_extra() RETURNS void AS $$\n"
                    + "BEGIN RAISE NOTICE 'no-placeholder', 'leftover-arg'; END;\n"
                    + "$$ LANGUAGE plpgsql");
            fail("RAISE with extra positional args must error 42601 'too many parameters specified for RAISE'");
        } catch (SQLException e) {
            assertEquals("42601", e.getSQLState(),
                    "SQLSTATE must be 42601 (syntax_error) for extra RAISE args; got " + e.getSQLState());
        }
    }

    // =========================================================================
    // A2. Bare RAISE; re-raise — preserve all diagnostic fields
    // =========================================================================

    @Test
    void bare_reraise_preserves_column_and_constraint_fields() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r16_reraise_probe(OUT col text, OUT cns text,\n"
                + "                                        OUT dt text, OUT tbl text, OUT sch text)\n"
                + "AS $$\n"
                + "BEGIN\n"
                + "  BEGIN\n"
                + "    BEGIN\n"
                + "      RAISE EXCEPTION 'boom' USING\n"
                + "        COLUMN = 'c1', CONSTRAINT = 'k1', DATATYPE = 'int4',\n"
                + "        TABLE = 't1', SCHEMA = 's1';\n"
                + "    EXCEPTION WHEN OTHERS THEN\n"
                + "      RAISE;\n"
                + "    END;\n"
                + "  EXCEPTION WHEN OTHERS THEN\n"
                + "    GET STACKED DIAGNOSTICS\n"
                + "      col = COLUMN_NAME,\n"
                + "      cns = CONSTRAINT_NAME,\n"
                + "      dt  = PG_DATATYPE_NAME,\n"
                + "      tbl = TABLE_NAME,\n"
                + "      sch = SCHEMA_NAME;\n"
                + "  END;\n"
                + "END;\n"
                + "$$ LANGUAGE plpgsql");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM r16_reraise_probe()")) {
            assertTrue(rs.next());
            assertEquals("c1",   rs.getString(1), "COLUMN_NAME lost across bare RAISE");
            assertEquals("k1",   rs.getString(2), "CONSTRAINT_NAME lost across bare RAISE");
            assertEquals("int4", rs.getString(3), "PG_DATATYPE_NAME lost across bare RAISE");
            assertEquals("t1",   rs.getString(4), "TABLE_NAME lost across bare RAISE");
            assertEquals("s1",   rs.getString(5), "SCHEMA_NAME lost across bare RAISE");
        }
    }

    // =========================================================================
    // A3. GET DIAGNOSTICS RESULT_OID
    // =========================================================================

    @Test
    void get_diagnostics_result_oid_populated_after_insert() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS r16_roid (id int)");
        // PG 18: RESULT_OID is no longer a valid GET DIAGNOSTICS item; the
        // function body is rejected at creation time with SQLSTATE 42601.
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE FUNCTION r16_fn_roid() RETURNS oid AS $$\n"
                    + "DECLARE v_oid oid;\n"
                    + "BEGIN\n"
                    + "  INSERT INTO r16_roid VALUES (1);\n"
                    + "  GET DIAGNOSTICS v_oid = RESULT_OID;\n"
                    + "  RETURN v_oid;\n"
                    + "END;\n"
                    + "$$ LANGUAGE plpgsql");
            fail("RESULT_OID in GET DIAGNOSTICS must be rejected with 42601 in PG 18");
        } catch (SQLException e) {
            assertEquals("42601", e.getSQLState(),
                    "GET DIAGNOSTICS RESULT_OID must throw 42601 (syntax_error); got " + e.getSQLState());
        }
    }

    // =========================================================================
    // A4. GET STACKED DIAGNOSTICS PG_EXCEPTION_CONTEXT — real stack, not a fixed literal
    // =========================================================================

    @Test
    void pg_exception_context_contains_function_name_and_line() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r16_ctx_inner() RETURNS void AS $$\n"
                + "BEGIN RAISE EXCEPTION 'inner-boom'; END;\n"
                + "$$ LANGUAGE plpgsql");
        exec("CREATE OR REPLACE FUNCTION r16_ctx_outer(OUT ctx text) AS $$\n"
                + "BEGIN\n"
                + "  BEGIN\n"
                + "    PERFORM r16_ctx_inner();\n"
                + "  EXCEPTION WHEN OTHERS THEN\n"
                + "    GET STACKED DIAGNOSTICS ctx = PG_EXCEPTION_CONTEXT;\n"
                + "  END;\n"
                + "END;\n"
                + "$$ LANGUAGE plpgsql");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT r16_ctx_outer()")) {
            assertTrue(rs.next());
            String ctx = rs.getString(1);
            assertNotNull(ctx, "PG_EXCEPTION_CONTEXT must not be null");
            // PG 18 emits something like:
            //   "PL/pgSQL function r16_ctx_inner() line 1 at RAISE\n
            //    SQL statement \"SELECT r16_ctx_inner()\"\n
            //    PL/pgSQL function r16_ctx_outer() line 3 at PERFORM"
            assertTrue(ctx.contains("r16_ctx_inner"),
                    "PG_EXCEPTION_CONTEXT must reference the failing function by name; got: " + ctx);
            assertTrue(ctx.contains("line "),
                    "PG_EXCEPTION_CONTEXT must include a line number; got: " + ctx);
        }
    }

    // =========================================================================
    // A5. client_min_messages filter on NOTICE
    // =========================================================================

    @Test
    void client_min_messages_warning_suppresses_notice_on_wire() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r16_notice_only() RETURNS void AS $$\n"
                + "BEGIN RAISE NOTICE 'quiet-notice-abcdef'; END;\n"
                + "$$ LANGUAGE plpgsql");
        exec("SET client_min_messages = warning");
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT r16_notice_only()");
            String chain = warningChain(s);
            assertFalse(chain.contains("quiet-notice-abcdef"),
                    "client_min_messages=warning must suppress NOTICEs on the wire; saw: " + chain);
        } finally {
            exec("SET client_min_messages = notice");
        }
    }
}
