package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for ALTER FUNCTION, ALTER PROCEDURE, and ALTER INDEX.
 * Validates PG 18 compatibility for all supported subclauses.
 */
class AlterFunctionProcedureIndexTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String queryString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getInt(1) : -1;
        }
    }

    private boolean queryBoolean(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() && rs.getBoolean(1);
        }
    }

    private double queryDouble(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getDouble(1) : 0;
        }
    }

    // =========================================================================
    // ALTER FUNCTION — RENAME TO
    // =========================================================================

    @Test
    void testAlterFunctionRenameTo() throws SQLException {
        exec("CREATE FUNCTION af_rename_f1() RETURNS integer AS $$ BEGIN RETURN 42; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_rename_f1() RENAME TO af_rename_f1_new");
        // The renamed function should work
        assertEquals(42, queryInt("SELECT af_rename_f1_new()"));
        // The old name should not exist
        assertThrows(SQLException.class, () -> queryInt("SELECT af_rename_f1()"));
    }

    @Test
    void testAlterFunctionRenameWithParams() throws SQLException {
        exec("CREATE FUNCTION af_rename_p(a integer, b integer) RETURNS integer AS $$ BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_rename_p(integer, integer) RENAME TO af_rename_p_new");
        assertEquals(7, queryInt("SELECT af_rename_p_new(3, 4)"));
    }

    @Test
    void testAlterFunctionRenameConflict() throws SQLException {
        exec("CREATE FUNCTION af_conflict_f1() RETURNS integer AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("CREATE FUNCTION af_conflict_f2() RETURNS integer AS $$ BEGIN RETURN 2; END; $$ LANGUAGE plpgsql");
        assertThrows(SQLException.class, () -> exec("ALTER FUNCTION af_conflict_f1() RENAME TO af_conflict_f2"));
    }

    @Test
    void testAlterFunctionRenameNonexistent() throws SQLException {
        assertThrows(SQLException.class, () -> exec("ALTER FUNCTION af_no_such_func() RENAME TO af_whatever"));
    }

    // =========================================================================
    // ALTER FUNCTION — SET SCHEMA
    // =========================================================================

    @Test
    void testAlterFunctionSetSchema() throws SQLException {
        exec("CREATE SCHEMA af_schema_test");
        exec("CREATE FUNCTION af_setsch_f1() RETURNS text AS $$ BEGIN RETURN 'hello'; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_setsch_f1() SET SCHEMA af_schema_test");
        // Function should still be callable (we don't enforce schema-qualified calls strictly)
        // but the schema metadata should be updated
    }

    @Test
    void testAlterFunctionSetSchemaNonexistent() throws SQLException {
        exec("CREATE FUNCTION af_setsch_f2() RETURNS text AS $$ BEGIN RETURN 'hello'; END; $$ LANGUAGE plpgsql");
        assertThrows(SQLException.class, () -> exec("ALTER FUNCTION af_setsch_f2() SET SCHEMA no_such_schema_xyz"));
    }

    // =========================================================================
    // ALTER FUNCTION — OWNER TO
    // =========================================================================

    @Test
    void testAlterFunctionOwnerTo() throws SQLException {
        exec("CREATE ROLE af_owner_role1 LOGIN");
        exec("CREATE FUNCTION af_own_f1() RETURNS integer AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_own_f1() OWNER TO af_owner_role1");
        // Should not throw
    }

    @Test
    void testAlterFunctionOwnerToNonexistentRole() throws SQLException {
        exec("CREATE FUNCTION af_own_f2() RETURNS integer AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        assertThrows(SQLException.class, () -> exec("ALTER FUNCTION af_own_f2() OWNER TO no_such_role_xyz"));
    }

    // =========================================================================
    // ALTER FUNCTION — Volatility attributes
    // =========================================================================

    @Test
    void testAlterFunctionVolatility() throws SQLException {
        exec("CREATE FUNCTION af_vol_f1() RETURNS integer AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_vol_f1() IMMUTABLE");
        exec("ALTER FUNCTION af_vol_f1() STABLE");
        exec("ALTER FUNCTION af_vol_f1() VOLATILE");
        // Should all succeed without error
    }

    // =========================================================================
    // ALTER FUNCTION — STRICT / CALLED ON NULL INPUT
    // =========================================================================

    @Test
    void testAlterFunctionStrict() throws SQLException {
        exec("CREATE FUNCTION af_strict_f1(a integer) RETURNS integer AS $$ BEGIN RETURN a; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_strict_f1(integer) STRICT");
        // STRICT function should return NULL when called with NULL input
        String result = queryString("SELECT af_strict_f1(NULL)");
        assertNull(result);
    }

    @Test
    void testAlterFunctionCalledOnNullInput() throws SQLException {
        exec("CREATE FUNCTION af_conull_f1(a integer) RETURNS integer AS $$ BEGIN RETURN COALESCE(a, -1); END; $$ LANGUAGE plpgsql STRICT");
        // Initially STRICT — NULL input returns NULL
        assertNull(queryString("SELECT af_conull_f1(NULL)"));
        // Change to CALLED ON NULL INPUT
        exec("ALTER FUNCTION af_conull_f1(integer) CALLED ON NULL INPUT");
        // Now the function body executes even with NULL input
        assertEquals(-1, queryInt("SELECT af_conull_f1(NULL)"));
    }

    @Test
    void testAlterFunctionReturnsNullOnNullInput() throws SQLException {
        exec("CREATE FUNCTION af_rnnull_f1(a integer) RETURNS integer AS $$ BEGIN RETURN COALESCE(a, -1); END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_rnnull_f1(integer) RETURNS NULL ON NULL INPUT");
        assertNull(queryString("SELECT af_rnnull_f1(NULL)"));
    }

    // =========================================================================
    // ALTER FUNCTION — SECURITY DEFINER / INVOKER
    // =========================================================================

    @Test
    void testAlterFunctionSecurityDefiner() throws SQLException {
        exec("CREATE FUNCTION af_sec_f1() RETURNS text AS $$ BEGIN RETURN current_user; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_sec_f1() SECURITY DEFINER");
        exec("ALTER FUNCTION af_sec_f1() SECURITY INVOKER");
        // Should both succeed without error
    }

    // =========================================================================
    // ALTER FUNCTION — LEAKPROOF
    // =========================================================================

    @Test
    void testAlterFunctionLeakproof() throws SQLException {
        exec("CREATE FUNCTION af_leak_f1() RETURNS boolean AS $$ BEGIN RETURN true; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_leak_f1() LEAKPROOF");
        exec("ALTER FUNCTION af_leak_f1() NOT LEAKPROOF");
    }

    // =========================================================================
    // ALTER FUNCTION — COST / ROWS
    // =========================================================================

    @Test
    void testAlterFunctionCost() throws SQLException {
        exec("CREATE FUNCTION af_cost_f1() RETURNS integer AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_cost_f1() COST 1000");
    }

    @Test
    void testAlterFunctionRows() throws SQLException {
        exec("CREATE FUNCTION af_rows_f1() RETURNS SETOF integer AS $$ BEGIN RETURN NEXT 1; RETURN NEXT 2; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_rows_f1() ROWS 500");
    }

    // =========================================================================
    // ALTER FUNCTION — PARALLEL
    // =========================================================================

    @Test
    void testAlterFunctionParallel() throws SQLException {
        exec("CREATE FUNCTION af_par_f1() RETURNS integer AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_par_f1() PARALLEL SAFE");
        exec("ALTER FUNCTION af_par_f1() PARALLEL RESTRICTED");
        exec("ALTER FUNCTION af_par_f1() PARALLEL UNSAFE");
    }

    // =========================================================================
    // ALTER FUNCTION — SET / RESET config
    // =========================================================================

    @Test
    void testAlterFunctionSetConfig() throws SQLException {
        exec("CREATE FUNCTION af_cfg_f1() RETURNS text AS $$ BEGIN RETURN current_setting('search_path'); END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_cfg_f1() SET search_path TO public");
        exec("ALTER FUNCTION af_cfg_f1() SET work_mem TO '64MB'");
    }

    @Test
    void testAlterFunctionResetConfig() throws SQLException {
        exec("CREATE FUNCTION af_rst_f1() RETURNS integer AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_rst_f1() SET search_path TO public");
        exec("ALTER FUNCTION af_rst_f1() RESET search_path");
    }

    @Test
    void testAlterFunctionResetAll() throws SQLException {
        exec("CREATE FUNCTION af_rsta_f1() RETURNS integer AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_rsta_f1() SET search_path TO public");
        exec("ALTER FUNCTION af_rsta_f1() RESET ALL");
    }

    // =========================================================================
    // ALTER FUNCTION — Multiple attributes in one statement
    // =========================================================================

    @Test
    void testAlterFunctionMultipleAttributes() throws SQLException {
        exec("CREATE FUNCTION af_multi_f1() RETURNS integer AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION af_multi_f1() IMMUTABLE STRICT LEAKPROOF PARALLEL SAFE COST 50");
    }

    // =========================================================================
    // ALTER PROCEDURE — mirrors ALTER FUNCTION
    // =========================================================================

    @Test
    void testAlterProcedureRenameTo() throws SQLException {
        exec("CREATE PROCEDURE ap_rename_p1() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql");
        exec("ALTER PROCEDURE ap_rename_p1() RENAME TO ap_rename_p1_new");
        // Call renamed procedure
        exec("CALL ap_rename_p1_new()");
        // Old name should fail
        assertThrows(SQLException.class, () -> exec("CALL ap_rename_p1()"));
    }

    @Test
    void testAlterProcedureOwnerTo() throws SQLException {
        exec("CREATE ROLE ap_owner_role1 LOGIN");
        exec("CREATE PROCEDURE ap_own_p1() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql");
        exec("ALTER PROCEDURE ap_own_p1() OWNER TO ap_owner_role1");
    }

    @Test
    void testAlterProcedureSetSchema() throws SQLException {
        exec("CREATE SCHEMA ap_schema_test");
        exec("CREATE PROCEDURE ap_setsch_p1() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql");
        exec("ALTER PROCEDURE ap_setsch_p1() SET SCHEMA ap_schema_test");
    }

    @Test
    void testAlterProcedureSecurityDefiner() throws SQLException {
        exec("CREATE PROCEDURE ap_sec_p1() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql");
        exec("ALTER PROCEDURE ap_sec_p1() SECURITY DEFINER");
        exec("ALTER PROCEDURE ap_sec_p1() SECURITY INVOKER");
    }

    @Test
    void testAlterProcedureSetConfig() throws SQLException {
        exec("CREATE PROCEDURE ap_cfg_p1() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql");
        exec("ALTER PROCEDURE ap_cfg_p1() SET search_path TO public");
        exec("ALTER PROCEDURE ap_cfg_p1() RESET search_path");
    }

    // =========================================================================
    // ALTER FUNCTION — IF EXISTS
    // =========================================================================

    @Test
    void testAlterFunctionIfExistsNonexistent() throws SQLException {
        // PG 18: ALTER FUNCTION does not support IF EXISTS — rejected with 42601
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER FUNCTION IF EXISTS af_no_such_ifex() RENAME TO af_whatever"));
        assertEquals("42601", ex.getSQLState());
    }

    @Test
    void testAlterFunctionIfExistsExisting() throws SQLException {
        // PG 18: ALTER FUNCTION does not support IF EXISTS — rejected with 42601
        exec("CREATE FUNCTION af_ifex_f1() RETURNS integer AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER FUNCTION IF EXISTS af_ifex_f1() IMMUTABLE"));
        assertEquals("42601", ex.getSQLState());
    }

    // =========================================================================
    // ALTER ROUTINE — PG alias for ALTER FUNCTION/PROCEDURE
    // =========================================================================

    @Test
    void testAlterRoutineFunction() throws SQLException {
        exec("CREATE FUNCTION ar_f1() RETURNS integer AS $$ BEGIN RETURN 42; END; $$ LANGUAGE plpgsql");
        exec("ALTER ROUTINE ar_f1() IMMUTABLE STRICT");
        assertEquals(42, queryInt("SELECT ar_f1()"));
    }

    @Test
    void testAlterRoutineRename() throws SQLException {
        exec("CREATE FUNCTION ar_rename_f1() RETURNS integer AS $$ BEGIN RETURN 99; END; $$ LANGUAGE plpgsql");
        exec("ALTER ROUTINE ar_rename_f1() RENAME TO ar_rename_f1_new");
        assertEquals(99, queryInt("SELECT ar_rename_f1_new()"));
    }

    @Test
    void testAlterRoutineIfExists() throws SQLException {
        // PG 18: ALTER ROUTINE does not support IF EXISTS — rejected with 42601
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER ROUTINE IF EXISTS ar_no_such_routine() RENAME TO whatever"));
        assertEquals("42601", ex.getSQLState());
    }

    // =========================================================================
    // ALTER PROCEDURE — IF EXISTS
    // =========================================================================

    @Test
    void testAlterProcedureIfExists() throws SQLException {
        // PG 18: ALTER PROCEDURE does not support IF EXISTS — rejected with 42601
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER PROCEDURE IF EXISTS ap_no_such_ifex() RENAME TO whatever"));
        assertEquals("42601", ex.getSQLState());
    }

    // =========================================================================
    // pg_proc catalog reflects function attributes
    // =========================================================================

    @Test
    void testPgProcReflectsVolatility() throws SQLException {
        exec("CREATE FUNCTION pgp_vol_f1() RETURNS integer AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql IMMUTABLE");
        String vol = queryString(
            "SELECT provolatile FROM pg_proc WHERE proname = 'pgp_vol_f1'");
        assertEquals("i", vol, "pg_proc should reflect IMMUTABLE as 'i'");

        exec("ALTER FUNCTION pgp_vol_f1() STABLE");
        vol = queryString("SELECT provolatile FROM pg_proc WHERE proname = 'pgp_vol_f1'");
        assertEquals("s", vol, "pg_proc should reflect STABLE as 's' after ALTER");

        exec("ALTER FUNCTION pgp_vol_f1() VOLATILE");
        vol = queryString("SELECT provolatile FROM pg_proc WHERE proname = 'pgp_vol_f1'");
        assertEquals("v", vol, "pg_proc should reflect VOLATILE as 'v' after ALTER");
    }

    @Test
    void testPgProcReflectsStrict() throws SQLException {
        exec("CREATE FUNCTION pgp_strict_f1(a int) RETURNS int AS $$ BEGIN RETURN a; END; $$ LANGUAGE plpgsql STRICT");
        boolean strict = queryBoolean(
            "SELECT proisstrict FROM pg_proc WHERE proname = 'pgp_strict_f1'");
        assertTrue(strict, "pg_proc should reflect STRICT as true");

        exec("ALTER FUNCTION pgp_strict_f1(integer) CALLED ON NULL INPUT");
        strict = queryBoolean("SELECT proisstrict FROM pg_proc WHERE proname = 'pgp_strict_f1'");
        assertFalse(strict, "pg_proc should reflect CALLED ON NULL INPUT as false");
    }

    @Test
    void testPgProcReflectsLeakproof() throws SQLException {
        exec("CREATE FUNCTION pgp_leak_f1() RETURNS boolean AS $$ BEGIN RETURN true; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION pgp_leak_f1() LEAKPROOF");
        boolean leak = queryBoolean(
            "SELECT proleakproof FROM pg_proc WHERE proname = 'pgp_leak_f1'");
        assertTrue(leak, "pg_proc should reflect LEAKPROOF as true");
    }

    @Test
    void testPgProcReflectsSecurityDefiner() throws SQLException {
        exec("CREATE FUNCTION pgp_sec_f1() RETURNS text AS $$ BEGIN RETURN 'x'; END; $$ LANGUAGE plpgsql SECURITY DEFINER");
        boolean secdef = queryBoolean(
            "SELECT prosecdef FROM pg_proc WHERE proname = 'pgp_sec_f1'");
        assertTrue(secdef, "pg_proc should reflect SECURITY DEFINER as true");
    }

    @Test
    void testPgProcReflectsParallel() throws SQLException {
        exec("CREATE FUNCTION pgp_par_f1() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION pgp_par_f1() PARALLEL SAFE");
        String par = queryString("SELECT proparallel FROM pg_proc WHERE proname = 'pgp_par_f1'");
        assertEquals("s", par, "pg_proc should reflect PARALLEL SAFE as 's'");
    }

    @Test
    void testPgProcReflectsCost() throws SQLException {
        exec("CREATE FUNCTION pgp_cost_f1() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION pgp_cost_f1() COST 500");
        double cost = queryDouble("SELECT procost FROM pg_proc WHERE proname = 'pgp_cost_f1'");
        assertEquals(500.0, cost, 0.01, "pg_proc should reflect COST 500");
    }

    // =========================================================================
    // DROP ROUTINE
    // =========================================================================

    @Test
    void testDropRoutine() throws SQLException {
        exec("CREATE FUNCTION dr_f1() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        assertEquals(1, queryInt("SELECT dr_f1()"));
        exec("DROP ROUTINE dr_f1()");
        assertThrows(SQLException.class, () -> queryInt("SELECT dr_f1()"));
    }

    @Test
    void testDropRoutineIfExists() throws SQLException {
        exec("DROP ROUTINE IF EXISTS dr_no_such_routine()");
    }

    @Test
    void testDropRoutineProcedure() throws SQLException {
        exec("CREATE PROCEDURE dr_p1() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql");
        exec("CALL dr_p1()");
        exec("DROP ROUTINE dr_p1()");
        assertThrows(SQLException.class, () -> exec("CALL dr_p1()"));
    }

    // =========================================================================
    // GRANT/REVOKE ON ROUTINE
    // =========================================================================

    @Test
    void testGrantOnRoutine() throws SQLException {
        exec("CREATE ROLE gr_role1 LOGIN");
        exec("CREATE FUNCTION gr_f1() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("GRANT EXECUTE ON ROUTINE gr_f1() TO gr_role1");
    }

    @Test
    void testRevokeOnRoutine() throws SQLException {
        exec("CREATE ROLE rv_role1 LOGIN");
        exec("CREATE FUNCTION rv_f1() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("GRANT EXECUTE ON ROUTINE rv_f1() TO rv_role1");
        exec("REVOKE EXECUTE ON ROUTINE rv_f1() FROM rv_role1");
    }

    // =========================================================================
    // COMMENT ON ROUTINE / FUNCTION / PROCEDURE
    // =========================================================================

    @Test
    void testCommentOnFunction() throws SQLException {
        exec("CREATE FUNCTION cf_f1() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("COMMENT ON FUNCTION cf_f1 IS 'This is a test function'");
    }

    @Test
    void testCommentOnRoutine() throws SQLException {
        exec("CREATE FUNCTION cr_f1() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
        exec("COMMENT ON ROUTINE cr_f1 IS 'Comment via ROUTINE keyword'");
    }

    @Test
    void testCommentOnProcedure() throws SQLException {
        exec("CREATE PROCEDURE cp_p1() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql");
        exec("COMMENT ON PROCEDURE cp_p1 IS 'Comment on a procedure'");
    }

    // =========================================================================
    // CREATE FUNCTION with SUPPORT / LEAKPROOF clauses
    // =========================================================================

    @Test
    void testCreateFunctionWithLeakproof() throws SQLException {
        exec("CREATE FUNCTION clf_f1(a int) RETURNS boolean AS $$ BEGIN RETURN a > 0; END; $$ LANGUAGE plpgsql LEAKPROOF");
        boolean leak = queryBoolean("SELECT proleakproof FROM pg_proc WHERE proname = 'clf_f1'");
        assertTrue(leak, "Function created with LEAKPROOF should be reflected in pg_proc");
    }

    @Test
    void testCreateFunctionWithNotLeakproof() throws SQLException {
        exec("CREATE FUNCTION clf_f2(a int) RETURNS boolean AS $$ BEGIN RETURN a > 0; END; $$ LANGUAGE plpgsql NOT LEAKPROOF");
        // NOT LEAKPROOF is the default; should parse without error
    }

    @Test
    void testCreateFunctionWithSupport() throws SQLException {
        // SUPPORT clause is parsed; PG validates that the support function exists.
        // Using a nonexistent support function should raise a non-syntax error.
        try {
            exec("CREATE FUNCTION csf_f1(a int) RETURNS int AS $$ BEGIN RETURN a; END; $$ LANGUAGE plpgsql SUPPORT csf_helper");
            // If no error, the support function happened to exist or was not validated
        } catch (SQLException e) {
            assertFalse(e.getMessage().toLowerCase().contains("syntax"),
                    "SUPPORT should parse without syntax error; got: " + e.getMessage());
        }
    }

    @Test
    void testCreateFunctionWithAllAttributes() throws SQLException {
        exec("CREATE FUNCTION cfa_f1() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql "
             + "IMMUTABLE STRICT LEAKPROOF PARALLEL SAFE COST 50 ROWS 100 SECURITY DEFINER");
        String vol = queryString("SELECT provolatile FROM pg_proc WHERE proname = 'cfa_f1'");
        assertEquals("i", vol);
        boolean strict = queryBoolean("SELECT proisstrict FROM pg_proc WHERE proname = 'cfa_f1'");
        assertTrue(strict);
        boolean leak = queryBoolean("SELECT proleakproof FROM pg_proc WHERE proname = 'cfa_f1'");
        assertTrue(leak);
    }

    // =========================================================================
    // Schema-qualified ALTER FUNCTION
    // =========================================================================

    @Test
    void testAlterFunctionSchemaQualified() throws SQLException {
        exec("CREATE FUNCTION sq_f1() RETURNS int AS $$ BEGIN RETURN 42; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION public.sq_f1() IMMUTABLE");
        String vol = queryString("SELECT provolatile FROM pg_proc WHERE proname = 'sq_f1'");
        assertEquals("i", vol, "Schema-qualified ALTER FUNCTION should work");
    }

    @Test
    void testAlterFunctionSchemaQualifiedRename() throws SQLException {
        exec("CREATE FUNCTION sq_rename_f1() RETURNS int AS $$ BEGIN RETURN 99; END; $$ LANGUAGE plpgsql");
        exec("ALTER FUNCTION public.sq_rename_f1() RENAME TO sq_rename_f1_new");
        assertEquals(99, queryInt("SELECT sq_rename_f1_new()"));
    }

    // =========================================================================
    // ALTER INDEX — RENAME TO (functional)
    // =========================================================================

    @Test
    void testAlterIndexRenameTo() throws SQLException {
        exec("CREATE TABLE ai_rename_tbl (id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX ai_old_idx ON ai_rename_tbl (name)");
        exec("ALTER INDEX ai_old_idx RENAME TO ai_new_idx");

        // Verify the rename: old index should not exist, new one should
        int count = queryInt(
            "SELECT count(*) FROM pg_indexes WHERE indexname = 'ai_new_idx'");
        assertEquals(1, count, "Renamed index should appear in pg_indexes");

        int oldCount = queryInt(
            "SELECT count(*) FROM pg_indexes WHERE indexname = 'ai_old_idx'");
        assertEquals(0, oldCount, "Old index name should no longer appear in pg_indexes");
    }

    @Test
    void testAlterIndexRenameIfExists() throws SQLException {
        // IF EXISTS with non-existent index should not throw
        exec("ALTER INDEX IF EXISTS ai_no_such_idx RENAME TO ai_whatever");
    }

    @Test
    void testAlterIndexRenameNonexistent() throws SQLException {
        assertThrows(SQLException.class,
            () -> exec("ALTER INDEX ai_no_such_idx RENAME TO ai_whatever"));
    }

    @Test
    void testAlterIndexRenameConflict() throws SQLException {
        exec("CREATE TABLE ai_conflict_tbl (id serial, a int, b int)");
        exec("CREATE INDEX ai_idx_a ON ai_conflict_tbl (a)");
        exec("CREATE INDEX ai_idx_b ON ai_conflict_tbl (b)");
        assertThrows(SQLException.class,
            () -> exec("ALTER INDEX ai_idx_a RENAME TO ai_idx_b"));
    }

    // =========================================================================
    // ALTER INDEX — SET TABLESPACE (accepted, no-op)
    // =========================================================================

    @Test
    void testAlterIndexSetTablespace() throws SQLException {
        exec("CREATE TABLE ai_ts_tbl (id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX ai_ts_idx ON ai_ts_tbl (name)");
        exec("ALTER INDEX ai_ts_idx SET TABLESPACE pg_default");
    }

    // =========================================================================
    // ALTER INDEX — SET / RESET storage params (accepted, no-op)
    // =========================================================================

    @Test
    void testAlterIndexSetParams() throws SQLException {
        exec("CREATE TABLE ai_sp_tbl (id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX ai_sp_idx ON ai_sp_tbl (name)");
        exec("ALTER INDEX ai_sp_idx SET (fillfactor = 70)");
        exec("ALTER INDEX ai_sp_idx RESET (fillfactor)");
    }

    // =========================================================================
    // ALTER INDEX — IF EXISTS variations
    // =========================================================================

    @Test
    void testAlterIndexIfExistsSetTablespace() throws SQLException {
        exec("ALTER INDEX IF EXISTS ai_no_exist SET TABLESPACE pg_default");
    }

    // =========================================================================
    // ALTER INDEX — Schema-qualified name
    // =========================================================================

    @Test
    void testAlterIndexSchemaQualified() throws SQLException {
        exec("CREATE SCHEMA ai_sch_test");
        exec("CREATE TABLE ai_sch_test.ai_sch_tbl (id serial, name text)");
        exec("CREATE INDEX ai_sch_idx ON ai_sch_test.ai_sch_tbl (name)");
        exec("ALTER INDEX ai_sch_idx RENAME TO ai_sch_idx_new");
    }
}
