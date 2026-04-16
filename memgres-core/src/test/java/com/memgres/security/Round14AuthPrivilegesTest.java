package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: Authentication/privilege semantics.
 *
 * - GRANT MAINTAIN ON TABLE (PG 17+)
 * - GRANT SET ON PARAMETER (PG 15+)
 * - Column-level GRANT / REVOKE
 * - GRANTED BY clause
 * - Role options: VALID UNTIL, CONNECTION LIMIT, INHERIT/NOINHERIT
 * - SECURITY DEFINER and search_path safety
 * - has_* helper functions
 */
class Round14AuthPrivilegesTest {

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

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. GRANT MAINTAIN (PG 17+)
    // =========================================================================

    @Test
    void grant_maintain_on_table() throws SQLException {
        exec("CREATE ROLE r14_auth_m1");
        exec("CREATE TABLE r14_auth_mt (id int)");
        exec("GRANT MAINTAIN ON TABLE r14_auth_mt TO r14_auth_m1");
        // has_table_privilege for MAINTAIN
        assertEquals("t",
                scalarString("SELECT has_table_privilege('r14_auth_m1','r14_auth_mt','MAINTAIN')::text"));
    }

    @Test
    void grant_maintain_on_all_tables_in_schema() throws SQLException {
        exec("CREATE ROLE r14_auth_m2");
        exec("CREATE SCHEMA r14_auth_ms");
        exec("CREATE TABLE r14_auth_ms.t1 (id int)");
        exec("CREATE TABLE r14_auth_ms.t2 (id int)");
        exec("GRANT MAINTAIN ON ALL TABLES IN SCHEMA r14_auth_ms TO r14_auth_m2");
        assertEquals("t",
                scalarString("SELECT has_table_privilege('r14_auth_m2','r14_auth_ms.t1','MAINTAIN')::text"));
    }

    // =========================================================================
    // B. GRANT SET ON PARAMETER (PG 15+)
    // =========================================================================

    @Test
    void grant_set_on_parameter() throws SQLException {
        exec("CREATE ROLE r14_auth_p1");
        exec("GRANT SET ON PARAMETER search_path TO r14_auth_p1");
        // pg_parameter_acl should exist
        assertTrue(scalarInt(
                "SELECT count(*)::int FROM pg_parameter_acl WHERE parname = 'search_path'") >= 0);
    }

    @Test
    void grant_alter_system_on_parameter() throws SQLException {
        exec("CREATE ROLE r14_auth_p2");
        exec("GRANT ALTER SYSTEM ON PARAMETER work_mem TO r14_auth_p2");
        // has_parameter_privilege (PG 15+)
        assertEquals("t",
                scalarString("SELECT has_parameter_privilege('r14_auth_p2','work_mem','ALTER SYSTEM')::text"));
    }

    // =========================================================================
    // C. Column-level GRANT / REVOKE
    // =========================================================================

    @Test
    void column_level_grant_select() throws SQLException {
        exec("CREATE ROLE r14_auth_c1");
        exec("CREATE TABLE r14_auth_ct (id int, secret text)");
        exec("GRANT SELECT (id) ON r14_auth_ct TO r14_auth_c1");
        // has_column_privilege for id → true
        assertEquals("t",
                scalarString("SELECT has_column_privilege('r14_auth_c1','r14_auth_ct','id','SELECT')::text"));
        // has_column_privilege for secret → false
        assertEquals("f",
                scalarString("SELECT has_column_privilege('r14_auth_c1','r14_auth_ct','secret','SELECT')::text"));
    }

    @Test
    void column_level_grant_update() throws SQLException {
        exec("CREATE ROLE r14_auth_c2");
        exec("CREATE TABLE r14_auth_cu (id int, v int)");
        exec("GRANT UPDATE (v) ON r14_auth_cu TO r14_auth_c2");
        assertEquals("t",
                scalarString("SELECT has_column_privilege('r14_auth_c2','r14_auth_cu','v','UPDATE')::text"));
        assertEquals("f",
                scalarString("SELECT has_column_privilege('r14_auth_c2','r14_auth_cu','id','UPDATE')::text"));
    }

    @Test
    void column_level_revoke() throws SQLException {
        exec("CREATE ROLE r14_auth_c3");
        exec("CREATE TABLE r14_auth_cr (id int, v int)");
        exec("GRANT SELECT (id, v) ON r14_auth_cr TO r14_auth_c3");
        exec("REVOKE SELECT (v) ON r14_auth_cr FROM r14_auth_c3");
        assertEquals("t",
                scalarString("SELECT has_column_privilege('r14_auth_c3','r14_auth_cr','id','SELECT')::text"));
        assertEquals("f",
                scalarString("SELECT has_column_privilege('r14_auth_c3','r14_auth_cr','v','SELECT')::text"));
    }

    // =========================================================================
    // D. GRANTED BY clause
    // =========================================================================

    @Test
    void grant_with_granted_by_current_role() throws SQLException {
        exec("CREATE ROLE r14_auth_g1");
        exec("CREATE TABLE r14_auth_gb (id int)");
        // GRANTED BY must be a role the grantor is a member of
        exec("GRANT SELECT ON r14_auth_gb TO r14_auth_g1 GRANTED BY CURRENT_ROLE");
        assertEquals("t",
                scalarString("SELECT has_table_privilege('r14_auth_g1','r14_auth_gb','SELECT')::text"));
    }

    // =========================================================================
    // E. Role options: VALID UNTIL, CONNECTION LIMIT, INHERIT
    // =========================================================================

    @Test
    void role_valid_until() throws SQLException {
        exec("CREATE ROLE r14_auth_vu WITH LOGIN VALID UNTIL '2099-12-31'");
        String v = scalarString(
                "SELECT rolvaliduntil::text FROM pg_roles WHERE rolname = 'r14_auth_vu'");
        assertNotNull(v);
        assertTrue(v.startsWith("2099"), "expected year 2099, got " + v);
    }

    @Test
    void role_connection_limit() throws SQLException {
        exec("CREATE ROLE r14_auth_cl WITH LOGIN CONNECTION LIMIT 5");
        assertEquals(5, scalarInt(
                "SELECT rolconnlimit::int FROM pg_roles WHERE rolname = 'r14_auth_cl'"));
    }

    @Test
    void role_inherit_noinherit() throws SQLException {
        exec("CREATE ROLE r14_auth_ni WITH NOINHERIT");
        assertEquals("f",
                scalarString("SELECT rolinherit::text FROM pg_roles WHERE rolname = 'r14_auth_ni'"));
        exec("ALTER ROLE r14_auth_ni INHERIT");
        assertEquals("t",
                scalarString("SELECT rolinherit::text FROM pg_roles WHERE rolname = 'r14_auth_ni'"));
    }

    @Test
    void role_can_login_attribute() throws SQLException {
        exec("CREATE ROLE r14_auth_lg WITH LOGIN");
        assertEquals("t",
                scalarString("SELECT rolcanlogin::text FROM pg_roles WHERE rolname = 'r14_auth_lg'"));
        exec("CREATE ROLE r14_auth_nlg WITH NOLOGIN");
        assertEquals("f",
                scalarString("SELECT rolcanlogin::text FROM pg_roles WHERE rolname = 'r14_auth_nlg'"));
    }

    @Test
    void role_replication_attribute() throws SQLException {
        exec("CREATE ROLE r14_auth_rep WITH REPLICATION");
        assertEquals("t",
                scalarString("SELECT rolreplication::text FROM pg_roles WHERE rolname = 'r14_auth_rep'"));
    }

    // =========================================================================
    // F. GRANT WITH ADMIN / GRANT OPTION
    // =========================================================================

    @Test
    void grant_role_with_admin_option() throws SQLException {
        exec("CREATE ROLE r14_auth_a1");
        exec("CREATE ROLE r14_auth_a2");
        exec("GRANT r14_auth_a1 TO r14_auth_a2 WITH ADMIN OPTION");
        // pg_auth_members.admin_option
        assertEquals("t", scalarString(
                "SELECT admin_option::text FROM pg_auth_members m "
                        + "JOIN pg_roles r ON m.roleid = r.oid "
                        + "JOIN pg_roles m2 ON m.member = m2.oid "
                        + "WHERE r.rolname = 'r14_auth_a1' AND m2.rolname = 'r14_auth_a2'"));
    }

    @Test
    void grant_with_grant_option() throws SQLException {
        exec("CREATE ROLE r14_auth_go1");
        exec("CREATE TABLE r14_auth_got (id int)");
        exec("GRANT SELECT ON r14_auth_got TO r14_auth_go1 WITH GRANT OPTION");
        // has_table_privilege 'SELECT WITH GRANT OPTION'
        assertEquals("t", scalarString(
                "SELECT has_table_privilege('r14_auth_go1','r14_auth_got','SELECT WITH GRANT OPTION')::text"));
    }

    // =========================================================================
    // G. SECURITY DEFINER functions
    // =========================================================================

    @Test
    void security_definer_function_pg_proc_flag() throws SQLException {
        exec("CREATE FUNCTION r14_auth_sd() RETURNS int AS 'SELECT 1' LANGUAGE SQL SECURITY DEFINER");
        assertEquals("t",
                scalarString("SELECT prosecdef::text FROM pg_proc WHERE proname = 'r14_auth_sd'"));
    }

    @Test
    void security_definer_with_set_search_path() throws SQLException {
        // PG 14+ idiom: SECURITY DEFINER + SET search_path for safety
        exec("CREATE FUNCTION r14_auth_sd2() RETURNS int AS 'SELECT 1' "
                + "LANGUAGE SQL SECURITY DEFINER SET search_path = pg_catalog");
        // proconfig should contain search_path
        String config = scalarString(
                "SELECT proconfig::text FROM pg_proc WHERE proname = 'r14_auth_sd2'");
        assertNotNull(config);
        assertTrue(config.toLowerCase().contains("search_path"),
                "proconfig should list search_path; got " + config);
    }

    // =========================================================================
    // H. DEFAULT PRIVILEGES
    // =========================================================================

    @Test
    void alter_default_privileges_grants() throws SQLException {
        exec("CREATE ROLE r14_auth_dp");
        exec("ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO r14_auth_dp");
        // pg_default_acl must have an entry
        assertTrue(scalarInt(
                "SELECT count(*)::int FROM pg_default_acl") >= 1);
    }

    @Test
    void alter_default_privileges_in_schema() throws SQLException {
        exec("CREATE ROLE r14_auth_dp2");
        exec("CREATE SCHEMA r14_auth_dps");
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA r14_auth_dps GRANT INSERT ON TABLES TO r14_auth_dp2");
        // verify pg_default_acl entries include defaclnamespace
        assertTrue(scalarInt(
                "SELECT count(*)::int FROM pg_default_acl d "
                        + "JOIN pg_namespace n ON d.defaclnamespace = n.oid "
                        + "WHERE n.nspname = 'r14_auth_dps'") >= 1);
    }

    // =========================================================================
    // I. has_* helper functions
    // =========================================================================

    @Test
    void has_schema_privilege_function() throws SQLException {
        exec("CREATE ROLE r14_auth_hs");
        exec("CREATE SCHEMA r14_auth_hss");
        exec("GRANT USAGE ON SCHEMA r14_auth_hss TO r14_auth_hs");
        assertEquals("t",
                scalarString("SELECT has_schema_privilege('r14_auth_hs','r14_auth_hss','USAGE')::text"));
    }

    @Test
    void has_database_privilege_function() throws SQLException {
        // CONNECT on current database
        assertEquals("t",
                scalarString("SELECT has_database_privilege(current_database(),'CONNECT')::text"));
    }

    @Test
    void pg_has_role_function() throws SQLException {
        exec("CREATE ROLE r14_auth_hr1");
        exec("CREATE ROLE r14_auth_hr2");
        exec("GRANT r14_auth_hr1 TO r14_auth_hr2");
        assertEquals("t",
                scalarString("SELECT pg_has_role('r14_auth_hr2','r14_auth_hr1','USAGE')::text"));
    }
}
