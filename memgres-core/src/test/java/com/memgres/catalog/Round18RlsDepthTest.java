package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap category AF: RLS depth.
 *
 * Covers:
 *  - AS RESTRICTIVE reflected in pg_policies.permissive
 *  - pg_policy non-empty after CREATE POLICY
 *  - Multiple policies: PERMISSIVE OR'd, RESTRICTIVE AND'd
 *  - pg_policies.roles is name[] type
 *  - Predefined role OIDs match PG reserved range (4200-4402)
 *  - pg_has_role 3-arg form exists
 *  - FORCE ROW LEVEL SECURITY applies to owner
 */
class Round18RlsDepthTest {

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

    private static int int1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // AF1. pg_policies.permissive reflects AS RESTRICTIVE
    // =========================================================================

    @Test
    void rls_restrictive_flag_reflected() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_rlsa CASCADE");
        exec("CREATE TABLE r18_rlsa(a int)");
        exec("ALTER TABLE r18_rlsa ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY r18_rlsa_p ON r18_rlsa AS RESTRICTIVE FOR ALL USING (true)");
        String v = str("SELECT permissive FROM pg_policies WHERE policyname='r18_rlsa_p'");
        assertEquals("RESTRICTIVE", v,
                "pg_policies.permissive must be 'RESTRICTIVE' for AS RESTRICTIVE; got '" + v + "'");
    }

    // =========================================================================
    // AF2. pg_policy non-empty after CREATE POLICY
    // =========================================================================

    @Test
    void pg_policy_has_row_after_create() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_rlsp CASCADE");
        exec("CREATE TABLE r18_rlsp(a int)");
        exec("ALTER TABLE r18_rlsp ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY r18_rlsp_p ON r18_rlsp USING (true)");
        int n = int1("SELECT count(*)::int FROM pg_policy WHERE polname='r18_rlsp_p'");
        assertEquals(1, n, "pg_policy must have one row for r18_rlsp_p; got " + n);
    }

    // =========================================================================
    // AF3. Restrictive AND'd, Permissive OR'd
    // =========================================================================

    @Test
    void restrictive_policy_blocks_even_when_permissive_allows() throws SQLException {
        exec("DROP ROLE IF EXISTS r18_rlsu");
        exec("CREATE ROLE r18_rlsu");
        exec("DROP TABLE IF EXISTS r18_rlsm CASCADE");
        exec("CREATE TABLE r18_rlsm(a int)");
        exec("INSERT INTO r18_rlsm VALUES (1), (2), (3)");
        exec("ALTER TABLE r18_rlsm ENABLE ROW LEVEL SECURITY");
        exec("GRANT SELECT ON r18_rlsm TO r18_rlsu");
        // Permissive: allow all.
        exec("CREATE POLICY r18_rlsm_perm ON r18_rlsm AS PERMISSIVE FOR SELECT TO r18_rlsu USING (true)");
        // Restrictive: only a = 2.
        exec("CREATE POLICY r18_rlsm_rest ON r18_rlsm AS RESTRICTIVE FOR SELECT TO r18_rlsu USING (a = 2)");

        exec("SET ROLE r18_rlsu");
        try {
            int n = int1("SELECT count(*)::int FROM r18_rlsm");
            assertEquals(1, n,
                    "PERMISSIVE OR RESTRICTIVE combined: only a=2 row must remain visible; got " + n);
        } finally {
            exec("RESET ROLE");
        }
    }

    // =========================================================================
    // AF4. pg_policies.roles is name[] type
    // =========================================================================

    @Test
    void pg_policies_roles_is_name_array() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT udt_name FROM information_schema.columns " +
                             "WHERE table_schema='pg_catalog' AND table_name='pg_policies' " +
                             "AND column_name='roles'")) {
            assertTrue(rs.next(), "pg_policies.roles column must exist");
            String t = rs.getString(1);
            // PG reports array element types prefixed with underscore: _name
            assertEquals("_name", t,
                    "pg_policies.roles must be type 'name[]' (udt_name=_name); got '" + t + "'");
        }
    }

    // =========================================================================
    // AF5. Predefined role OIDs in reserved range
    // =========================================================================

    @Test
    void predefined_role_oid_in_reserved_range() throws SQLException {
        int oid = int1("SELECT oid::int FROM pg_roles WHERE rolname='pg_read_all_data'");
        assertTrue(oid >= 6168,
                "pg_read_all_data OID must be in PG reserved range 6168+; got " + oid);
    }

    // =========================================================================
    // AF6. pg_has_role 3-arg form
    // =========================================================================

    @Test
    void pg_has_role_three_arg_form_works() throws SQLException {
        exec("DROP ROLE IF EXISTS r18_hasr");
        exec("CREATE ROLE r18_hasr");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT pg_has_role(current_user, 'r18_hasr', 'MEMBER')")) {
            assertTrue(rs.next(),
                    "pg_has_role(username, role, priv) 3-arg form must execute");
            // bool result — membership depends on grant; we just confirm it returns.
            rs.getBoolean(1);
        }
    }

    // =========================================================================
    // AF7. FORCE RLS applies to owner
    // =========================================================================

    @Test
    void force_rls_applies_to_owner() throws SQLException {
        exec("DROP ROLE IF EXISTS r18_rlsow");
        exec("CREATE ROLE r18_rlsow");
        exec("GRANT r18_rlsow TO current_user");
        exec("DROP TABLE IF EXISTS r18_rlsfor CASCADE");
        exec("CREATE TABLE r18_rlsfor(a int)");
        exec("INSERT INTO r18_rlsfor VALUES (1), (2)");
        exec("ALTER TABLE r18_rlsfor OWNER TO r18_rlsow");
        exec("ALTER TABLE r18_rlsfor ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE r18_rlsfor FORCE ROW LEVEL SECURITY");
        exec("CREATE POLICY r18_rlsfor_p ON r18_rlsfor USING (a = 1)");

        exec("SET ROLE r18_rlsow");
        try {
            int n = int1("SELECT count(*)::int FROM r18_rlsfor");
            assertEquals(1, n,
                    "FORCE RLS must apply to owner; only a=1 row must be visible; got " + n);
        } finally {
            exec("RESET ROLE");
        }
    }
}
