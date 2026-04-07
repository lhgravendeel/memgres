package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Row-Level Security (RLS) policy syntax found in real-world schemas.
 */
class RlsPolicyCompatTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // =========================================================================
    // ENABLE / DISABLE ROW LEVEL SECURITY
    // =========================================================================

    @Test
    void testEnableRls() throws SQLException {
        exec("CREATE TABLE rls_test (id serial PRIMARY KEY, owner_name text, data text)");
        exec("ALTER TABLE rls_test ENABLE ROW LEVEL SECURITY");
    }

    @Test
    void testForceRls() throws SQLException {
        exec("CREATE TABLE rls_force (id serial PRIMARY KEY, data text)");
        exec("ALTER TABLE rls_force ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE rls_force FORCE ROW LEVEL SECURITY");
    }

    @Test
    void testDisableRls() throws SQLException {
        exec("CREATE TABLE rls_disable (id serial PRIMARY KEY, data text)");
        exec("ALTER TABLE rls_disable ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE rls_disable DISABLE ROW LEVEL SECURITY");
    }

    @Test
    void testNoForceRls() throws SQLException {
        exec("CREATE TABLE rls_noforce (id serial PRIMARY KEY, data text)");
        exec("ALTER TABLE rls_noforce ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE rls_noforce FORCE ROW LEVEL SECURITY");
        exec("ALTER TABLE rls_noforce NO FORCE ROW LEVEL SECURITY");
    }

    // =========================================================================
    // CREATE POLICY
    // =========================================================================

    @Test
    void testCreatePolicyForAll() throws SQLException {
        exec("CREATE TABLE policy_all (id serial PRIMARY KEY, owner_name text, data text)");
        exec("ALTER TABLE policy_all ENABLE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY owner_access ON policy_all
            FOR ALL
            USING (owner_name = current_user)
        """);
    }

    @Test
    void testCreatePolicyForSelect() throws SQLException {
        exec("CREATE TABLE policy_select (id serial PRIMARY KEY, visible boolean, data text)");
        exec("ALTER TABLE policy_all ENABLE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY read_visible ON policy_select
            FOR SELECT
            USING (visible = true)
        """);
    }

    @Test
    void testCreatePolicyForInsert() throws SQLException {
        exec("CREATE TABLE policy_insert (id serial PRIMARY KEY, owner_name text)");
        exec("ALTER TABLE policy_insert ENABLE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY insert_own ON policy_insert
            FOR INSERT
            WITH CHECK (owner_name = current_user)
        """);
    }

    @Test
    void testCreatePolicyForUpdate() throws SQLException {
        exec("CREATE TABLE policy_update (id serial PRIMARY KEY, owner_name text, val int)");
        exec("ALTER TABLE policy_update ENABLE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY update_own ON policy_update
            FOR UPDATE
            USING (owner_name = current_user)
            WITH CHECK (owner_name = current_user)
        """);
    }

    @Test
    void testCreatePolicyForDelete() throws SQLException {
        exec("CREATE TABLE policy_delete (id serial PRIMARY KEY, owner_name text)");
        exec("ALTER TABLE policy_delete ENABLE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY delete_own ON policy_delete
            FOR DELETE
            USING (owner_name = current_user)
        """);
    }

    // =========================================================================
    // Policy with TO role
    // =========================================================================

    @Test
    void testCreatePolicyWithToRole() throws SQLException {
        exec("CREATE TABLE policy_role (id serial PRIMARY KEY, data text)");
        exec("ALTER TABLE policy_role ENABLE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY admin_access ON policy_role
            TO PUBLIC
            USING (true)
        """);
    }

    @Test
    void testCreatePolicyToSpecificRoles() throws SQLException {
        exec("CREATE TABLE policy_roles (id serial PRIMARY KEY, data text)");
        exec("ALTER TABLE policy_roles ENABLE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY restricted_access ON policy_roles
            FOR SELECT
            TO current_user
            USING (true)
        """);
    }

    // =========================================================================
    // DROP POLICY
    // =========================================================================

    @Test
    void testDropPolicy() throws SQLException {
        exec("CREATE TABLE policy_drop (id serial PRIMARY KEY, data text)");
        exec("ALTER TABLE policy_drop ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY temp_policy ON policy_drop USING (true)");
        exec("DROP POLICY temp_policy ON policy_drop");
    }

    @Test
    void testDropPolicyIfExists() throws SQLException {
        exec("CREATE TABLE policy_drop_ie (id serial PRIMARY KEY, data text)");
        exec("DROP POLICY IF EXISTS nonexistent_policy ON policy_drop_ie");
    }

    // =========================================================================
    // ALTER POLICY
    // =========================================================================

    @Test
    void testAlterPolicy() throws SQLException {
        exec("CREATE TABLE policy_alter (id serial PRIMARY KEY, flag boolean)");
        exec("ALTER TABLE policy_alter ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY alter_me ON policy_alter USING (flag = true)");
        exec("ALTER POLICY alter_me ON policy_alter USING (flag = false)");
    }

    // =========================================================================
    // Policy with complex USING expressions
    // =========================================================================

    @Test
    void testPolicyWithSubquery() throws SQLException {
        exec("CREATE TABLE teams (id serial PRIMARY KEY, name text)");
        exec("CREATE TABLE team_members (team_id int REFERENCES teams(id), user_name text)");
        exec("CREATE TABLE team_data (id serial PRIMARY KEY, team_id int REFERENCES teams(id), payload text)");
        exec("ALTER TABLE team_data ENABLE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY team_member_access ON team_data
            FOR ALL
            USING (team_id IN (
                SELECT team_id FROM team_members WHERE user_name = current_user
            ))
        """);
    }

    @Test
    void testPolicyWithCurrentSetting() throws SQLException {
        exec("CREATE TABLE tenant_data (id serial PRIMARY KEY, tenant_id text, data text)");
        exec("ALTER TABLE tenant_data ENABLE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY tenant_isolation ON tenant_data
            FOR ALL
            USING (tenant_id = current_setting('app.current_tenant', true))
        """);
    }

    // =========================================================================
    // Permissive vs Restrictive policies
    // =========================================================================

    @Test
    void testPermissivePolicy() throws SQLException {
        exec("CREATE TABLE perm_test (id serial PRIMARY KEY, data text)");
        exec("ALTER TABLE perm_test ENABLE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY perm_pol ON perm_test
            AS PERMISSIVE
            FOR SELECT
            USING (true)
        """);
    }

    @Test
    void testRestrictivePolicy() throws SQLException {
        exec("CREATE TABLE rest_test (id serial PRIMARY KEY, data text, active boolean)");
        exec("ALTER TABLE rest_test ENABLE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY rest_pol ON rest_test
            AS RESTRICTIVE
            FOR SELECT
            USING (active = true)
        """);
    }
}
