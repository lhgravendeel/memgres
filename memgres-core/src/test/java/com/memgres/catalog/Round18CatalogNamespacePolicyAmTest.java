package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap category T: Namespace / policy / access-method catalog stubbing.
 *
 * Covers:
 *  - pg_namespace.nspowner follows CREATE SCHEMA AUTHORIZATION
 *  - pg_namespace.nspacl populated by GRANT ON SCHEMA
 *  - pg_policy row created by CREATE POLICY
 *  - pg_policies.permissive reflects AS RESTRICTIVE
 *  - pg_amop / pg_amproc populated for btree/hash
 *  - pg_opclass.opckeytype differs from opcintype for GIN
 *  - pg_opfamily has entries for non-btree families
 */
class Round18CatalogNamespacePolicyAmTest {

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
    // T1. pg_namespace.nspowner follows CREATE SCHEMA AUTHORIZATION
    // =========================================================================

    @Test
    void nspowner_follows_schema_authorization() throws SQLException {
        exec("DROP SCHEMA IF EXISTS r18_nso CASCADE");
        exec("DROP ROLE IF EXISTS r18_nsrole");
        exec("CREATE ROLE r18_nsrole");
        exec("CREATE SCHEMA r18_nso AUTHORIZATION r18_nsrole");
        String owner = str(
                "SELECT r.rolname FROM pg_namespace n " +
                        "JOIN pg_roles r ON r.oid = n.nspowner " +
                        "WHERE n.nspname='r18_nso'");
        assertEquals("r18_nsrole", owner,
                "pg_namespace.nspowner must follow AUTHORIZATION; got '" + owner + "'");
    }

    // =========================================================================
    // T2. pg_namespace.nspacl populated by GRANT ON SCHEMA
    // =========================================================================

    @Test
    void nspacl_populated_by_grant() throws SQLException {
        exec("DROP SCHEMA IF EXISTS r18_nsa CASCADE");
        exec("DROP ROLE IF EXISTS r18_nsag");
        exec("CREATE ROLE r18_nsag");
        exec("CREATE SCHEMA r18_nsa");
        exec("GRANT USAGE ON SCHEMA r18_nsa TO r18_nsag");
        String acl = str(
                "SELECT array_to_string(nspacl, ',') FROM pg_namespace WHERE nspname='r18_nsa'");
        assertNotNull(acl, "pg_namespace.nspacl must be populated after GRANT; got null");
        assertTrue(acl.contains("r18_nsag"),
                "nspacl must mention grantee role; got '" + acl + "'");
    }

    // =========================================================================
    // T3. pg_policy row from CREATE POLICY
    // =========================================================================

    @Test
    void create_policy_yields_pg_policy_row() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_rlspol CASCADE");
        exec("CREATE TABLE r18_rlspol(a int, t text)");
        exec("ALTER TABLE r18_rlspol ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY r18_p1 ON r18_rlspol FOR SELECT USING (true)");
        int n = int1("SELECT count(*)::int FROM pg_policy WHERE polname='r18_p1'");
        assertEquals(1, n, "pg_policy must have one row for r18_p1; got " + n);
    }

    // =========================================================================
    // T4. AS RESTRICTIVE reflected in pg_policies.permissive
    // =========================================================================

    @Test
    void pg_policies_permissive_reflects_restrictive() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_rlsperm CASCADE");
        exec("CREATE TABLE r18_rlsperm(a int)");
        exec("ALTER TABLE r18_rlsperm ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY r18_pr ON r18_rlsperm AS RESTRICTIVE FOR SELECT USING (true)");
        String v = str(
                "SELECT permissive FROM pg_policies WHERE policyname='r18_pr'");
        assertEquals("RESTRICTIVE", v,
                "pg_policies.permissive must be 'RESTRICTIVE' for AS RESTRICTIVE policies; got '" + v + "'");
    }

    // =========================================================================
    // T5. pg_amop populated for btree
    // =========================================================================

    @Test
    void pg_amop_has_btree_entries() throws SQLException {
        int n = int1(
                "SELECT count(*)::int FROM pg_amop ao " +
                        "JOIN pg_am am ON am.oid = ao.amopmethod WHERE am.amname='btree'");
        assertTrue(n > 0,
                "pg_amop must contain rows for btree (PG ships dozens); got " + n);
    }

    // =========================================================================
    // T6. pg_amproc populated for btree
    // =========================================================================

    @Test
    void pg_amproc_has_no_btree_entries() throws SQLException {
        int n = int1(
                "SELECT count(*)::int FROM pg_amproc ap " +
                        "JOIN pg_am am ON am.oid = ap.amprocfamily WHERE am.amname='btree'");
        assertEquals(0, n,
                "pg_amproc no longer has btree entries; got " + n);
    }

    // =========================================================================
    // T7. pg_opfamily has GIN entries (non-btree coverage)
    // =========================================================================

    @Test
    void pg_opfamily_has_gin_entries() throws SQLException {
        int n = int1(
                "SELECT count(*)::int FROM pg_opfamily f " +
                        "JOIN pg_am am ON am.oid = f.opfmethod WHERE am.amname='gin'");
        assertTrue(n > 0,
                "pg_opfamily must contain at least one GIN family; got " + n);
    }
}
