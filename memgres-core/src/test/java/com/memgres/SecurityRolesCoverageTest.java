package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

class SecurityRolesCoverageTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
    }

    @AfterAll
    static void stop() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private ResultSet q(String sql) throws Exception {
        return conn.createStatement().executeQuery(sql);
    }

    private void exec(String sql) throws Exception {
        conn.createStatement().execute(sql);
    }

    // =========================================================================
    // Item 96: Roles
    // =========================================================================

    @Test
    @DisplayName("96.1 CREATE ROLE basic")
    void createRoleBasic() {
        assertDoesNotThrow(() -> exec("CREATE ROLE role_basic"));
    }

    @Test
    @DisplayName("96.2 CREATE ROLE WITH SUPERUSER")
    void createRoleWithSuperuser() {
        assertDoesNotThrow(() -> exec("CREATE ROLE role_su WITH SUPERUSER"));
    }

    @Test
    @DisplayName("96.3 CREATE ROLE WITH multiple attributes")
    void createRoleWithMultipleAttributes() {
        assertDoesNotThrow(() -> exec(
                "CREATE ROLE role_multi WITH SUPERUSER CREATEDB CREATEROLE LOGIN"));
    }

    @Test
    @DisplayName("96.4 CREATE ROLE WITH PASSWORD")
    void createRoleWithPassword() {
        assertDoesNotThrow(() -> exec("CREATE ROLE role_pw WITH PASSWORD 'secret123'"));
    }

    @Test
    @DisplayName("96.5 CREATE ROLE WITH ENCRYPTED PASSWORD")
    void createRoleWithEncryptedPassword() {
        assertDoesNotThrow(() -> exec(
                "CREATE ROLE role_enc WITH ENCRYPTED PASSWORD 'md5abc123'"));
    }

    @Test
    @DisplayName("96.6 CREATE ROLE WITH CONNECTION LIMIT")
    void createRoleWithConnectionLimit() {
        assertDoesNotThrow(() -> exec(
                "CREATE ROLE role_connlim WITH CONNECTION LIMIT 10"));
    }

    @Test
    @DisplayName("96.7 CREATE ROLE WITH VALID UNTIL")
    void createRoleWithValidUntil() {
        assertDoesNotThrow(() -> exec(
                "CREATE ROLE role_valid WITH VALID UNTIL '2030-12-31'"));
    }

    @Test
    @DisplayName("96.8 CREATE ROLE WITH INHERIT / NOINHERIT")
    void createRoleWithInheritNoinherit() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE role_inh WITH INHERIT");
            exec("CREATE ROLE role_noinh WITH NOINHERIT");
        });
    }

    @Test
    @DisplayName("96.9 CREATE ROLE WITH REPLICATION / NOREPLICATION")
    void createRoleWithReplicationNoreplication() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE role_repl WITH REPLICATION");
            exec("CREATE ROLE role_norepl WITH NOREPLICATION");
        });
    }

    @Test
    @DisplayName("96.10 CREATE ROLE WITH BYPASSRLS / NOBYPASSRLS")
    void createRoleWithBypassrlsNobypassrls() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE role_bypass WITH BYPASSRLS");
            exec("CREATE ROLE role_nobypass WITH NOBYPASSRLS");
        });
    }

    @Test
    @DisplayName("96.11 CREATE USER (implies LOGIN)")
    void createUser() {
        assertDoesNotThrow(() -> exec("CREATE USER user_login WITH PASSWORD 'pass'"));
    }

    @Test
    @DisplayName("96.12 CREATE ROLE WITH NOSUPERUSER NOCREATEDB NOCREATEROLE NOLOGIN")
    void createRoleWithAllNegativeAttributes() {
        assertDoesNotThrow(() -> exec(
                "CREATE ROLE role_noall WITH NOSUPERUSER NOCREATEDB NOCREATEROLE NOLOGIN"));
    }

    @Test
    @DisplayName("96.13 ALTER ROLE with attributes (SUPERUSER, CREATEDB)")
    void alterRoleWithAttributes() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE role_alter1");
            exec("ALTER ROLE role_alter1 WITH SUPERUSER CREATEDB");
        });
    }

    @Test
    @DisplayName("96.14 ALTER ROLE RENAME TO")
    void alterRoleRenameTo() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE role_old_name");
            exec("ALTER ROLE role_old_name RENAME TO role_new_name");
        });
    }

    @Test
    @DisplayName("96.15 ALTER ROLE SET/RESET parameter")
    void alterRoleSetResetParameter() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE role_setparam");
            exec("ALTER ROLE role_setparam SET search_path TO public");
            exec("ALTER ROLE role_setparam RESET search_path");
        });
    }

    @Test
    @DisplayName("96.16 DROP ROLE")
    void dropRole() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE role_to_drop");
            exec("DROP ROLE role_to_drop");
        });
    }

    @Test
    @DisplayName("96.17 DROP ROLE IF EXISTS (nonexistent)")
    void dropRoleIfExistsNonexistent() {
        assertDoesNotThrow(() -> exec("DROP ROLE IF EXISTS nonexistent_role_xyz"));
    }

    @Test
    @DisplayName("96.18 SET ROLE name")
    void setRoleName() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE role_setme");
            exec("GRANT role_setme TO test");
            exec("SET ROLE role_setme");
        });
    }

    @Test
    @DisplayName("96.19 RESET ROLE")
    void resetRole() {
        assertDoesNotThrow(() -> exec("RESET ROLE"));
    }

    @Test
    @DisplayName("96.20 SET SESSION AUTHORIZATION")
    void setSessionAuthorization() {
        assertDoesNotThrow(() -> exec("SET SESSION AUTHORIZATION test"));
    }

    @Test
    @DisplayName("96.21 CREATE ROLE with IN ROLE")
    void createRoleWithInRole() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE parent_role1");
            exec("CREATE ROLE child_role1 IN ROLE parent_role1");
        });
    }

    @Test
    @DisplayName("96.22 CREATE ROLE with ADMIN clause")
    void createRoleWithAdminClause() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE managed_role1");
            exec("CREATE ROLE admin_role1 ADMIN managed_role1");
        });
    }

    @Test
    @DisplayName("96.23 SHOW search_path (as no-op)")
    void showSearchPath() {
        assertDoesNotThrow(() -> exec("SHOW search_path"));
    }

    @Test
    @DisplayName("96.24 RESET ALL")
    void resetAll() {
        assertDoesNotThrow(() -> exec("RESET ALL"));
    }

    // =========================================================================
    // Item 97: GRANT & REVOKE
    // =========================================================================

    @Test
    @DisplayName("97.25 GRANT SELECT ON TABLE TO role")
    void grantSelectOnTable() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE grant_t1 (id INT)");
            exec("CREATE ROLE grant_role1");
            exec("GRANT SELECT ON TABLE grant_t1 TO grant_role1");
        });
    }

    @Test
    @DisplayName("97.26 GRANT INSERT, UPDATE ON table TO role")
    void grantInsertUpdateOnTable() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE grant_t2 (id INT)");
            exec("CREATE ROLE grant_role2");
            exec("GRANT INSERT, UPDATE ON grant_t2 TO grant_role2");
        });
    }

    @Test
    @DisplayName("97.27 GRANT ALL PRIVILEGES ON TABLE TO role")
    void grantAllPrivilegesOnTable() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE grant_t3 (id INT)");
            exec("CREATE ROLE grant_role3");
            exec("GRANT ALL PRIVILEGES ON TABLE grant_t3 TO grant_role3");
        });
    }

    @Test
    @DisplayName("97.28 GRANT ALL ON TABLE TO PUBLIC")
    void grantAllOnTableToPublic() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE grant_t4 (id INT)");
            exec("GRANT ALL ON TABLE grant_t4 TO PUBLIC");
        });
    }

    @Test
    @DisplayName("97.29 GRANT WITH GRANT OPTION")
    void grantWithGrantOption() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE grant_t5 (id INT)");
            exec("CREATE ROLE grant_role5");
            exec("GRANT SELECT ON TABLE grant_t5 TO grant_role5 WITH GRANT OPTION");
        });
    }

    @Test
    @DisplayName("97.30 REVOKE SELECT ON TABLE FROM role")
    void revokeSelectOnTable() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE revoke_t1 (id INT)");
            exec("CREATE ROLE revoke_role1");
            exec("GRANT SELECT ON TABLE revoke_t1 TO revoke_role1");
            exec("REVOKE SELECT ON TABLE revoke_t1 FROM revoke_role1");
        });
    }

    @Test
    @DisplayName("97.31 REVOKE ALL PRIVILEGES ON TABLE FROM role")
    void revokeAllPrivilegesOnTable() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE revoke_t2 (id INT)");
            exec("CREATE ROLE revoke_role2");
            exec("REVOKE ALL PRIVILEGES ON TABLE revoke_t2 FROM revoke_role2");
        });
    }

    @Test
    @DisplayName("97.32 REVOKE GRANT OPTION FOR SELECT ON TABLE FROM role")
    void revokeGrantOptionFor() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE revoke_t3 (id INT)");
            exec("CREATE ROLE revoke_role3");
            exec("REVOKE GRANT OPTION FOR SELECT ON TABLE revoke_t3 FROM revoke_role3");
        });
    }

    @Test
    @DisplayName("97.33 GRANT ON SEQUENCE")
    void grantOnSequence() {
        assertDoesNotThrow(() -> {
            exec("CREATE SEQUENCE grant_seq1");
            exec("CREATE ROLE grant_role_seq");
            exec("GRANT USAGE ON SEQUENCE grant_seq1 TO grant_role_seq");
        });
    }

    @Test
    @DisplayName("97.34 GRANT ON FUNCTION")
    void grantOnFunction() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE grant_role_func");
            exec("GRANT EXECUTE ON FUNCTION my_func TO grant_role_func");
        });
    }

    @Test
    @DisplayName("97.35 GRANT ON SCHEMA")
    void grantOnSchema() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE grant_role_schema");
            exec("GRANT CREATE ON SCHEMA public TO grant_role_schema");
        });
    }

    @Test
    @DisplayName("97.36 GRANT ON DATABASE")
    void grantOnDatabase() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE grant_role_db");
            exec("GRANT CONNECT ON DATABASE test TO grant_role_db");
        });
    }

    @Test
    @DisplayName("97.37 GRANT USAGE ON SCHEMA")
    void grantUsageOnSchema() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE grant_role_usage");
            exec("GRANT USAGE ON SCHEMA public TO grant_role_usage");
        });
    }

    @Test
    @DisplayName("97.38 GRANT EXECUTE ON FUNCTION")
    void grantExecuteOnFunction() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE grant_role_exec");
            exec("GRANT EXECUTE ON FUNCTION some_function TO grant_role_exec");
        });
    }

    @Test
    @DisplayName("97.39 GRANT role TO role (role membership)")
    void grantRoleToRole() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE member_role1");
            exec("CREATE ROLE group_role1");
            exec("GRANT group_role1 TO member_role1");
        });
    }

    @Test
    @DisplayName("97.40 GRANT role TO role WITH ADMIN OPTION")
    void grantRoleToRoleWithAdminOption() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE member_role2");
            exec("CREATE ROLE group_role2");
            exec("GRANT group_role2 TO member_role2 WITH ADMIN OPTION");
        });
    }

    @Test
    @DisplayName("97.41 REVOKE role FROM role")
    void revokeRoleFromRole() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE member_role3");
            exec("CREATE ROLE group_role3");
            exec("GRANT group_role3 TO member_role3");
            exec("REVOKE group_role3 FROM member_role3");
        });
    }

    @Test
    @DisplayName("97.42 GRANT ON ALL TABLES IN SCHEMA")
    void grantOnAllTablesInSchema() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE grant_role_all_tables");
            exec("GRANT SELECT ON ALL TABLES IN SCHEMA public TO grant_role_all_tables");
        });
    }

    @Test
    @DisplayName("97.43 REVOKE ON ALL SEQUENCES IN SCHEMA")
    void revokeOnAllSequencesInSchema() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE revoke_role_all_seq");
            exec("REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM revoke_role_all_seq");
        });
    }

    @Test
    @DisplayName("97.44 GRANT column-level privileges")
    void grantColumnLevelPrivileges() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE grant_col_t (col1 INT, col2 TEXT, col3 BOOLEAN)");
            exec("CREATE ROLE grant_role_col");
            exec("GRANT SELECT (col1, col2) ON TABLE grant_col_t TO grant_role_col");
        });
    }

    @Test
    @DisplayName("97.45 REVOKE CASCADE")
    void revokeCascade() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE revoke_casc_t (id INT)");
            exec("CREATE ROLE revoke_casc_role");
            exec("REVOKE ALL ON TABLE revoke_casc_t FROM revoke_casc_role CASCADE");
        });
    }

    @Test
    @DisplayName("97.46 DROP OWNED BY role")
    void dropOwnedByRole() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE drop_owned_role");
            exec("DROP OWNED BY drop_owned_role");
        });
    }

    @Test
    @DisplayName("97.47 REASSIGN OWNED BY role TO other_role")
    void reassignOwnedByRole() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE reassign_from_role");
            exec("CREATE ROLE reassign_to_role");
            exec("REASSIGN OWNED BY reassign_from_role TO reassign_to_role");
        });
    }

    // =========================================================================
    // Item 98: Row-Level Security
    // =========================================================================

    @Test
    @DisplayName("98.48 CREATE POLICY basic with USING clause")
    void createPolicyWithUsing() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_t1 (id INT, owner TEXT)");
            exec("CREATE POLICY p1 ON rls_t1 USING (owner = current_user)");
        });
    }

    @Test
    @DisplayName("98.49 CREATE POLICY with WITH CHECK clause")
    void createPolicyWithCheck() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_t2 (id INT, owner TEXT)");
            exec("CREATE POLICY p2 ON rls_t2 WITH CHECK (owner = current_user)");
        });
    }

    @Test
    @DisplayName("98.50 CREATE POLICY with both USING and WITH CHECK")
    void createPolicyWithUsingAndCheck() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_t3 (id INT, owner TEXT)");
            exec("CREATE POLICY p3 ON rls_t3 USING (id > 0) WITH CHECK (owner = current_user)");
        });
    }

    @Test
    @DisplayName("98.51 CREATE POLICY FOR SELECT")
    void createPolicyForSelect() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_t4 (id INT, owner TEXT)");
            exec("CREATE POLICY p4 ON rls_t4 FOR SELECT USING (owner = current_user)");
        });
    }

    @Test
    @DisplayName("98.52 CREATE POLICY FOR INSERT")
    void createPolicyForInsert() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_t5 (id INT, owner TEXT)");
            exec("CREATE POLICY p5 ON rls_t5 FOR INSERT WITH CHECK (owner = current_user)");
        });
    }

    @Test
    @DisplayName("98.53 CREATE POLICY FOR UPDATE")
    void createPolicyForUpdate() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_t6 (id INT, owner TEXT)");
            exec("CREATE POLICY p6 ON rls_t6 FOR UPDATE USING (id > 0) WITH CHECK (owner = current_user)");
        });
    }

    @Test
    @DisplayName("98.54 CREATE POLICY FOR DELETE")
    void createPolicyForDelete() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_t7 (id INT, owner TEXT)");
            exec("CREATE POLICY p7 ON rls_t7 FOR DELETE USING (owner = current_user)");
        });
    }

    @Test
    @DisplayName("98.55 CREATE POLICY FOR ALL (default)")
    void createPolicyForAll() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_t8 (id INT, owner TEXT)");
            exec("CREATE POLICY p8 ON rls_t8 FOR ALL USING (owner = current_user)");
        });
    }

    @Test
    @DisplayName("98.56 CREATE POLICY AS PERMISSIVE")
    void createPolicyAsPermissive() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_t9 (id INT, owner TEXT)");
            exec("CREATE POLICY p9 ON rls_t9 AS PERMISSIVE USING (owner = current_user)");
        });
    }

    @Test
    @DisplayName("98.57 CREATE POLICY AS RESTRICTIVE")
    void createPolicyAsRestrictive() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_t10 (id INT, owner TEXT)");
            exec("CREATE POLICY p10 ON rls_t10 AS RESTRICTIVE USING (id > 0)");
        });
    }

    @Test
    @DisplayName("98.58 CREATE POLICY with TO role")
    void createPolicyWithToRole() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_t11 (id INT, owner TEXT)");
            exec("CREATE ROLE rls_role1");
            exec("CREATE POLICY p11 ON rls_t11 TO rls_role1 USING (owner = current_user)");
        });
    }

    @Test
    @DisplayName("98.59 CREATE POLICY with multiple roles")
    void createPolicyWithMultipleRoles() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_t12 (id INT, owner TEXT)");
            exec("CREATE ROLE rls_role2");
            exec("CREATE ROLE rls_role3");
            exec("CREATE POLICY p12 ON rls_t12 TO rls_role2, rls_role3 USING (id > 0)");
        });
    }

    @Test
    @DisplayName("98.60 ALTER TABLE ENABLE ROW LEVEL SECURITY")
    void alterTableEnableRls() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_enable_t (id INT)");
            exec("ALTER TABLE rls_enable_t ENABLE ROW LEVEL SECURITY");
        });
    }

    @Test
    @DisplayName("98.61 ALTER TABLE DISABLE ROW LEVEL SECURITY")
    void alterTableDisableRls() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_disable_t (id INT)");
            exec("ALTER TABLE rls_disable_t ENABLE ROW LEVEL SECURITY");
            exec("ALTER TABLE rls_disable_t DISABLE ROW LEVEL SECURITY");
        });
    }

    @Test
    @DisplayName("98.62 ALTER TABLE FORCE ROW LEVEL SECURITY")
    void alterTableForceRls() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_force_t (id INT)");
            exec("ALTER TABLE rls_force_t FORCE ROW LEVEL SECURITY");
        });
    }

    @Test
    @DisplayName("98.63 ALTER TABLE NO FORCE ROW LEVEL SECURITY")
    void alterTableNoForceRls() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_noforce_t (id INT)");
            exec("ALTER TABLE rls_noforce_t FORCE ROW LEVEL SECURITY");
            exec("ALTER TABLE rls_noforce_t NO FORCE ROW LEVEL SECURITY");
        });
    }

    @Test
    @DisplayName("98.64 DROP POLICY name ON table")
    void dropPolicyOnTable() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_drop_t (id INT, owner TEXT)");
            exec("CREATE POLICY pdrop ON rls_drop_t USING (id > 0)");
            exec("DROP POLICY pdrop ON rls_drop_t");
        });
    }

    @Test
    @DisplayName("98.65 DROP POLICY IF EXISTS")
    void dropPolicyIfExists() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_dropie_t (id INT)");
            exec("DROP POLICY IF EXISTS nonexistent_policy ON rls_dropie_t");
        });
    }

    @Test
    @DisplayName("98.66 ALTER POLICY name ON table USING (new_expr)")
    void alterPolicyUsing() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_alter_t1 (id INT, owner TEXT)");
            exec("CREATE POLICY palter1 ON rls_alter_t1 USING (id > 0)");
            exec("ALTER POLICY palter1 ON rls_alter_t1 USING (id > 10)");
        });
    }

    @Test
    @DisplayName("98.67 ALTER POLICY name ON table WITH CHECK (new_expr)")
    void alterPolicyWithCheck() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_alter_t2 (id INT, owner TEXT)");
            exec("CREATE POLICY palter2 ON rls_alter_t2 WITH CHECK (owner = current_user)");
            exec("ALTER POLICY palter2 ON rls_alter_t2 WITH CHECK (id > 0)");
        });
    }

    @Test
    @DisplayName("98.68 ALTER POLICY name ON table RENAME TO new_name")
    void alterPolicyRenameTo() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE rls_alter_t3 (id INT, owner TEXT)");
            exec("CREATE POLICY palter3 ON rls_alter_t3 USING (id > 0)");
            exec("ALTER POLICY palter3 ON rls_alter_t3 RENAME TO palter3_new");
        });
    }

    // =========================================================================
    // Item 99: ALTER DEFAULT PRIVILEGES
    // =========================================================================

    @Test
    @DisplayName("99.69 ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO role")
    void alterDefaultPrivGrantSelectOnTables() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE adp_role1");
            exec("ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO adp_role1");
        });
    }

    @Test
    @DisplayName("99.70 ALTER DEFAULT PRIVILEGES FOR ROLE name GRANT ALL ON SEQUENCES TO role")
    void alterDefaultPrivForRoleGrantAllOnSequences() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE adp_owner1");
            exec("CREATE ROLE adp_role2");
            exec("ALTER DEFAULT PRIVILEGES FOR ROLE adp_owner1 GRANT ALL ON SEQUENCES TO adp_role2");
        });
    }

    @Test
    @DisplayName("99.71 ALTER DEFAULT PRIVILEGES IN SCHEMA name GRANT EXECUTE ON FUNCTIONS TO role")
    void alterDefaultPrivInSchemaGrantExecuteOnFunctions() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE adp_role3");
            exec("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO adp_role3");
        });
    }

    @Test
    @DisplayName("99.72 ALTER DEFAULT PRIVILEGES REVOKE ALL ON TABLES FROM role")
    void alterDefaultPrivRevokeAllOnTables() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE adp_role4");
            exec("ALTER DEFAULT PRIVILEGES REVOKE ALL ON TABLES FROM adp_role4");
        });
    }

    @Test
    @DisplayName("99.73 ALTER DEFAULT PRIVILEGES FOR ROLE IN SCHEMA REVOKE ALL ON FUNCTIONS FROM PUBLIC")
    void alterDefaultPrivForRoleInSchemaRevokeAllOnFunctions() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE adp_owner2");
            exec("ALTER DEFAULT PRIVILEGES FOR ROLE adp_owner2 IN SCHEMA public REVOKE ALL ON FUNCTIONS FROM PUBLIC");
        });
    }
}
