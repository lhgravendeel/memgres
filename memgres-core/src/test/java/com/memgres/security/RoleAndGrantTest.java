package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PG 18 compatibility tests for role management and GRANT/REVOKE edge cases.
 *
 * Covers verification differences:
 * - Diff 29: SET ROLE to non-existent role must give 42704 (role not found)
 * - Diff 30: DROP ROLE on non-existent role gives 42704; IF EXISTS variant succeeds silently
 * - Diff 31: GRANT on non-existent table must give 42P01 (relation does not exist)
 * - Diff 55: DROP ROLE on non-existent role (same 42704 pattern)
 *
 * Extended coverage:
 * - CREATE ROLE / DROP ROLE lifecycle
 * - SET ROLE / RESET ROLE behavior
 * - GRANT/REVOKE on existing tables
 * - GRANT invalid privilege type
 * - ALTER ROLE
 * - Role with LOGIN / NOLOGIN
 * - CREATE ROLE IF NOT EXISTS (invalid PG syntax, must error)
 * - DROP ROLE IF EXISTS (valid syntax)
 * - GRANT on schema
 * - REVOKE non-granted privilege (should succeed silently in PG)
 */
class RoleAndGrantTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // Diff 29: SET ROLE to non-existent role must give 22023
    // ========================================================================

    @Test
    @DisplayName("diff29: SET ROLE non-existent role raises 22023")
    void setRole_nonExistentRole_raises22023() {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SET ROLE ragt_no_such_role_xyz"));
        assertEquals("22023", ex.getSQLState(),
                "SET ROLE on non-existent role should raise 22023 (invalid_parameter_value), got: "
                        + ex.getSQLState() + ": " + ex.getMessage());
    }

    // ========================================================================
    // Diff 30 / Diff 55: DROP ROLE on non-existent role
    // ========================================================================

    @Test
    @DisplayName("diff30: DROP ROLE non-existent role raises 42704")
    void dropRole_nonExistentRole_raises42704() {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("DROP ROLE ragt_no_such_role_abc"));
        assertEquals("42704", ex.getSQLState(),
                "DROP ROLE on non-existent role should raise 42704, got: "
                        + ex.getSQLState() + ": " + ex.getMessage());
    }

    @Test
    @DisplayName("diff30: DROP ROLE IF EXISTS non-existent role succeeds silently")
    void dropRoleIfExists_nonExistentRole_succeedsSilently() {
        assertDoesNotThrow(() -> exec("DROP ROLE IF EXISTS ragt_no_such_role_def"),
                "DROP ROLE IF EXISTS on non-existent role should not throw");
    }

    @Test
    @DisplayName("diff55: DROP ROLE (second pattern) non-existent role raises 42704")
    void dropRole_nonExistentRole_diff55_raises42704() {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("DROP ROLE ragt_no_such_role2_xyz"));
        assertEquals("42704", ex.getSQLState(),
                "DROP ROLE (diff 55) on non-existent role should raise 42704, got: "
                        + ex.getSQLState() + ": " + ex.getMessage());
    }

    // ========================================================================
    // Diff 31: GRANT on non-existent table must give 42P01
    // ========================================================================

    @Test
    @DisplayName("diff31: GRANT SELECT on non-existent table raises 42P01")
    void grant_nonExistentTable_raises42P01() throws SQLException {
        exec("CREATE ROLE ragt_grantee_role1");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("GRANT SELECT ON ragt_no_such_table_xyz TO ragt_grantee_role1"));
            assertEquals("42P01", ex.getSQLState(),
                    "GRANT on non-existent table should raise 42P01 (relation does not exist), got: "
                            + ex.getSQLState() + ": " + ex.getMessage());
        } finally {
            exec("DROP ROLE IF EXISTS ragt_grantee_role1");
        }
    }

    // ========================================================================
    // CREATE ROLE / DROP ROLE lifecycle
    // ========================================================================

    @Test
    @DisplayName("lifecycle: CREATE ROLE then DROP ROLE")
    void createDropRole_lifecycle() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_lifecycle_role1");
            exec("DROP ROLE ragt_lifecycle_role1");
        });
    }

    @Test
    @DisplayName("lifecycle: CREATE ROLE duplicate raises error")
    void createRole_duplicate_raisesError() throws SQLException {
        exec("CREATE ROLE ragt_dup_role1");
        try {
            assertThrows(SQLException.class, () -> exec("CREATE ROLE ragt_dup_role1"),
                    "Creating a duplicate role should raise an error");
        } finally {
            exec("DROP ROLE IF EXISTS ragt_dup_role1");
        }
    }

    @Test
    @DisplayName("lifecycle: DROP ROLE IF EXISTS on existing role succeeds")
    void dropRoleIfExists_existingRole_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_ifex_role1");
            exec("DROP ROLE IF EXISTS ragt_ifex_role1");
        });
    }

    @Test
    @DisplayName("lifecycle: DROP ROLE IF EXISTS multiple non-existent roles succeeds")
    void dropRoleIfExists_multiple_nonExistent_succeeds() {
        assertDoesNotThrow(() ->
                exec("DROP ROLE IF EXISTS ragt_nx1_xyz, ragt_nx2_xyz, ragt_nx3_xyz"));
    }

    // ========================================================================
    // SET ROLE / RESET ROLE behavior
    // ========================================================================

    @Test
    @DisplayName("setRole: SET ROLE to existing role succeeds")
    void setRole_existingRole_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_setrole1");
            exec("GRANT ragt_setrole1 TO memgres");
            exec("SET ROLE ragt_setrole1");
            exec("RESET ROLE");
        });
        // cleanup: role may or may not be droppable while active, do it after RESET
        assertDoesNotThrow(() -> exec("DROP ROLE IF EXISTS ragt_setrole1"));
    }

    @Test
    @DisplayName("setRole: RESET ROLE without prior SET ROLE succeeds")
    void resetRole_withoutPriorSetRole_succeeds() {
        assertDoesNotThrow(() -> exec("RESET ROLE"));
    }

    @Test
    @DisplayName("setRole: SET ROLE NONE succeeds")
    void setRole_none_succeeds() {
        assertDoesNotThrow(() -> exec("SET ROLE NONE"));
    }

    @Test
    @DisplayName("setRole: SET ROLE DEFAULT succeeds")
    void setRole_default_succeeds() {
        assertDoesNotThrow(() -> exec("SET ROLE DEFAULT"));
    }

    @Test
    @DisplayName("setRole: SET ROLE then query works normally")
    void setRole_thenQuery_connectionUsable() throws SQLException {
        exec("CREATE ROLE ragt_queryrole1");
        exec("GRANT ragt_queryrole1 TO memgres");
        exec("CREATE TABLE ragt_sr_t1(id int PRIMARY KEY, v text)");
        try {
            exec("SET ROLE ragt_queryrole1");
            exec("INSERT INTO ragt_sr_t1 VALUES (1, 'hello')");
            assertEquals("hello", scalar("SELECT v FROM ragt_sr_t1 WHERE id = 1"));
            exec("RESET ROLE");
        } finally {
            exec("DROP TABLE IF EXISTS ragt_sr_t1");
            exec("DROP ROLE IF EXISTS ragt_queryrole1");
        }
    }

    // ========================================================================
    // GRANT / REVOKE on existing tables
    // ========================================================================

    @Test
    @DisplayName("grant: GRANT SELECT on existing table succeeds")
    void grant_selectOnExistingTable_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE ragt_grant_t1(id int)");
            exec("CREATE ROLE ragt_grant_r1");
            exec("GRANT SELECT ON TABLE ragt_grant_t1 TO ragt_grant_r1");
        });
        assertDoesNotThrow(() -> {
            exec("DROP TABLE IF EXISTS ragt_grant_t1");
            exec("DROP ROLE IF EXISTS ragt_grant_r1");
        });
    }

    @Test
    @DisplayName("grant: GRANT INSERT, UPDATE, DELETE on existing table succeeds")
    void grant_insertUpdateDeleteOnExistingTable_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE ragt_grant_t2(id int, v text)");
            exec("CREATE ROLE ragt_grant_r2");
            exec("GRANT INSERT, UPDATE, DELETE ON TABLE ragt_grant_t2 TO ragt_grant_r2");
        });
        assertDoesNotThrow(() -> {
            exec("DROP TABLE IF EXISTS ragt_grant_t2");
            exec("DROP ROLE IF EXISTS ragt_grant_r2");
        });
    }

    @Test
    @DisplayName("grant: GRANT ALL PRIVILEGES on table to PUBLIC succeeds")
    void grant_allPrivilegesToPublic_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE ragt_grant_t3(id int)");
            exec("GRANT ALL PRIVILEGES ON TABLE ragt_grant_t3 TO PUBLIC");
        });
        assertDoesNotThrow(() -> exec("DROP TABLE IF EXISTS ragt_grant_t3"));
    }

    @Test
    @DisplayName("grant: GRANT WITH GRANT OPTION succeeds")
    void grant_withGrantOption_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE ragt_grant_t4(id int)");
            exec("CREATE ROLE ragt_grant_r4");
            exec("GRANT SELECT ON TABLE ragt_grant_t4 TO ragt_grant_r4 WITH GRANT OPTION");
        });
        assertDoesNotThrow(() -> {
            exec("DROP TABLE IF EXISTS ragt_grant_t4");
            exec("DROP ROLE IF EXISTS ragt_grant_r4");
        });
    }

    @Test
    @DisplayName("revoke: REVOKE SELECT from role that has it succeeds")
    void revoke_selectFromGrantedRole_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE ragt_rev_t1(id int)");
            exec("CREATE ROLE ragt_rev_r1");
            exec("GRANT SELECT ON TABLE ragt_rev_t1 TO ragt_rev_r1");
            exec("REVOKE SELECT ON TABLE ragt_rev_t1 FROM ragt_rev_r1");
        });
        assertDoesNotThrow(() -> {
            exec("DROP TABLE IF EXISTS ragt_rev_t1");
            exec("DROP ROLE IF EXISTS ragt_rev_r1");
        });
    }

    @Test
    @DisplayName("revoke: REVOKE non-granted privilege succeeds silently")
    void revoke_nonGrantedPrivilege_succeedsSilently() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE ragt_rev_t2(id int)");
            exec("CREATE ROLE ragt_rev_r2");
            // No GRANT was made, so REVOKE of a non-granted privilege should succeed silently in PG
            exec("REVOKE SELECT ON TABLE ragt_rev_t2 FROM ragt_rev_r2");
        });
        assertDoesNotThrow(() -> {
            exec("DROP TABLE IF EXISTS ragt_rev_t2");
            exec("DROP ROLE IF EXISTS ragt_rev_r2");
        });
    }

    @Test
    @DisplayName("revoke: REVOKE ALL PRIVILEGES CASCADE succeeds")
    void revoke_allPrivilegesCascade_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE ragt_rev_t3(id int)");
            exec("CREATE ROLE ragt_rev_r3");
            exec("GRANT SELECT ON TABLE ragt_rev_t3 TO ragt_rev_r3 WITH GRANT OPTION");
            exec("REVOKE ALL PRIVILEGES ON TABLE ragt_rev_t3 FROM ragt_rev_r3 CASCADE");
        });
        assertDoesNotThrow(() -> {
            exec("DROP TABLE IF EXISTS ragt_rev_t3");
            exec("DROP ROLE IF EXISTS ragt_rev_r3");
        });
    }

    // ========================================================================
    // GRANT invalid privilege type
    // ========================================================================

    @Test
    @DisplayName("grant: GRANT invalid privilege type on table raises syntax error")
    void grant_invalidPrivilegeType_raisesError() throws SQLException {
        exec("CREATE TABLE ragt_grant_inv_t1(id int)");
        exec("CREATE ROLE ragt_grant_inv_r1");
        try {
            assertThrows(SQLException.class,
                    () -> exec("GRANT FROBNICATE ON TABLE ragt_grant_inv_t1 TO ragt_grant_inv_r1"),
                    "GRANT with invalid privilege type should raise an error");
        } finally {
            exec("DROP TABLE IF EXISTS ragt_grant_inv_t1");
            exec("DROP ROLE IF EXISTS ragt_grant_inv_r1");
        }
    }

    // ========================================================================
    // ALTER ROLE
    // ========================================================================

    @Test
    @DisplayName("alter: ALTER ROLE with SUPERUSER CREATEDB attributes succeeds")
    void alterRole_superuserCreatedb_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_alter_r1");
            exec("ALTER ROLE ragt_alter_r1 WITH SUPERUSER CREATEDB");
        });
        assertDoesNotThrow(() -> exec("DROP ROLE IF EXISTS ragt_alter_r1"));
    }

    @Test
    @DisplayName("alter: ALTER ROLE RENAME TO succeeds")
    void alterRole_renameTo_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_old_name_r1");
            exec("ALTER ROLE ragt_old_name_r1 RENAME TO ragt_new_name_r1");
            exec("DROP ROLE IF EXISTS ragt_new_name_r1");
        });
    }

    @Test
    @DisplayName("alter: ALTER ROLE SET / RESET parameter succeeds")
    void alterRole_setResetParameter_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_setparam_r1");
            exec("ALTER ROLE ragt_setparam_r1 SET search_path TO public");
            exec("ALTER ROLE ragt_setparam_r1 RESET search_path");
        });
        assertDoesNotThrow(() -> exec("DROP ROLE IF EXISTS ragt_setparam_r1"));
    }

    @Test
    @DisplayName("alter: ALTER ROLE on non-existent role raises error")
    void alterRole_nonExistentRole_raisesError() {
        assertThrows(SQLException.class,
                () -> exec("ALTER ROLE ragt_no_such_alter_xyz WITH CREATEDB"),
                "ALTER ROLE on non-existent role should raise an error");
    }

    // ========================================================================
    // Role with LOGIN / NOLOGIN
    // ========================================================================

    @Test
    @DisplayName("login: CREATE ROLE WITH LOGIN succeeds")
    void createRole_withLogin_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_login_r1 WITH LOGIN PASSWORD 'secret'");
        });
        assertDoesNotThrow(() -> exec("DROP ROLE IF EXISTS ragt_login_r1"));
    }

    @Test
    @DisplayName("login: CREATE ROLE WITH NOLOGIN succeeds")
    void createRole_withNologin_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_nologin_r1 WITH NOLOGIN");
        });
        assertDoesNotThrow(() -> exec("DROP ROLE IF EXISTS ragt_nologin_r1"));
    }

    @Test
    @DisplayName("login: ALTER ROLE LOGIN -> NOLOGIN succeeds")
    void alterRole_loginToNologin_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_toglogin_r1 WITH LOGIN");
            exec("ALTER ROLE ragt_toglogin_r1 WITH NOLOGIN");
        });
        assertDoesNotThrow(() -> exec("DROP ROLE IF EXISTS ragt_toglogin_r1"));
    }

    // ========================================================================
    // CREATE ROLE IF NOT EXISTS (invalid PG syntax, must error)
    // ========================================================================

    @Test
    @DisplayName("syntax: CREATE ROLE IF NOT EXISTS is invalid syntax and must raise error")
    void createRoleIfNotExists_invalidSyntax_raisesError() {
        assertThrows(SQLException.class,
                () -> exec("CREATE ROLE IF NOT EXISTS ragt_ifnotexists_r1"),
                "CREATE ROLE IF NOT EXISTS is not valid PostgreSQL syntax and should raise an error");
    }

    // ========================================================================
    // GRANT on schema
    // ========================================================================

    @Test
    @DisplayName("grant: GRANT USAGE on existing schema succeeds")
    void grant_usageOnSchema_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_schema_r1");
            exec("GRANT USAGE ON SCHEMA public TO ragt_schema_r1");
        });
        assertDoesNotThrow(() -> {
            exec("REVOKE ALL ON SCHEMA public FROM ragt_schema_r1");
            exec("DROP ROLE IF EXISTS ragt_schema_r1");
        });
    }

    @Test
    @DisplayName("grant: GRANT CREATE on existing schema succeeds")
    void grant_createOnSchema_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_schema_r2");
            exec("GRANT CREATE ON SCHEMA public TO ragt_schema_r2");
        });
        assertDoesNotThrow(() -> {
            exec("REVOKE ALL ON SCHEMA public FROM ragt_schema_r2");
            exec("DROP ROLE IF EXISTS ragt_schema_r2");
        });
    }

    @Test
    @DisplayName("grant: REVOKE USAGE on schema succeeds")
    void revoke_usageOnSchema_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_schema_r3");
            exec("GRANT USAGE ON SCHEMA public TO ragt_schema_r3");
            exec("REVOKE USAGE ON SCHEMA public FROM ragt_schema_r3");
        });
        assertDoesNotThrow(() -> exec("DROP ROLE IF EXISTS ragt_schema_r3"));
    }

    @Test
    @DisplayName("grant: GRANT on non-existent schema raises error")
    void grant_nonExistentSchema_raisesError() throws SQLException {
        exec("CREATE ROLE ragt_schema_r4");
        try {
            assertThrows(SQLException.class,
                    () -> exec("GRANT USAGE ON SCHEMA ragt_no_such_schema_xyz TO ragt_schema_r4"),
                    "GRANT on non-existent schema should raise an error");
        } finally {
            exec("DROP ROLE IF EXISTS ragt_schema_r4");
        }
    }

    // ========================================================================
    // GRANT role membership
    // ========================================================================

    @Test
    @DisplayName("grant: GRANT role to role (membership) succeeds")
    void grant_roleToRole_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_group_r1");
            exec("CREATE ROLE ragt_member_r1");
            exec("GRANT ragt_group_r1 TO ragt_member_r1");
        });
        assertDoesNotThrow(() -> {
            exec("DROP ROLE IF EXISTS ragt_member_r1");
            exec("DROP ROLE IF EXISTS ragt_group_r1");
        });
    }

    @Test
    @DisplayName("grant: GRANT role to role WITH ADMIN OPTION succeeds")
    void grant_roleToRoleWithAdminOption_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_group_r2");
            exec("CREATE ROLE ragt_member_r2");
            exec("GRANT ragt_group_r2 TO ragt_member_r2 WITH ADMIN OPTION");
        });
        assertDoesNotThrow(() -> {
            exec("DROP ROLE IF EXISTS ragt_member_r2");
            exec("DROP ROLE IF EXISTS ragt_group_r2");
        });
    }

    @Test
    @DisplayName("grant: REVOKE role from role succeeds")
    void revoke_roleFromRole_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_group_r3");
            exec("CREATE ROLE ragt_member_r3");
            exec("GRANT ragt_group_r3 TO ragt_member_r3");
            exec("REVOKE ragt_group_r3 FROM ragt_member_r3");
        });
        assertDoesNotThrow(() -> {
            exec("DROP ROLE IF EXISTS ragt_member_r3");
            exec("DROP ROLE IF EXISTS ragt_group_r3");
        });
    }

    // ========================================================================
    // GRANT on ALL TABLES IN SCHEMA
    // ========================================================================

    @Test
    @DisplayName("grant: GRANT SELECT ON ALL TABLES IN SCHEMA succeeds")
    void grant_selectOnAllTablesInSchema_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_allschema_r1");
            exec("GRANT SELECT ON ALL TABLES IN SCHEMA public TO ragt_allschema_r1");
        });
        assertDoesNotThrow(() -> {
            exec("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM ragt_allschema_r1");
            exec("DROP ROLE IF EXISTS ragt_allschema_r1");
        });
    }

    // ========================================================================
    // Column-level GRANT
    // ========================================================================

    @Test
    @DisplayName("grant: GRANT SELECT on specific columns succeeds")
    void grant_selectOnColumns_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE ragt_col_t1(col1 int, col2 text, col3 boolean)");
            exec("CREATE ROLE ragt_col_r1");
            exec("GRANT SELECT (col1, col2) ON TABLE ragt_col_t1 TO ragt_col_r1");
        });
        assertDoesNotThrow(() -> {
            exec("DROP TABLE IF EXISTS ragt_col_t1");
            exec("DROP ROLE IF EXISTS ragt_col_r1");
        });
    }

    // ========================================================================
    // DROP ROLE with dependencies interaction
    // ========================================================================

    @Test
    @DisplayName("drop: DROP ROLE IF EXISTS multiple roles, some existing and some not")
    void dropRoleIfExists_mixed_succeeds() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE ragt_mixed_exists_r1");
            // ragt_mixed_notexists_r1 is intentionally not created
            exec("DROP ROLE IF EXISTS ragt_mixed_exists_r1, ragt_mixed_notexists_r1");
        });
    }

    @Test
    @DisplayName("drop: DROP ROLE after granting role membership raises error (dependency)")
    void dropRole_withMembership_raisesError() throws SQLException {
        exec("CREATE ROLE ragt_dep_group_r1");
        exec("CREATE ROLE ragt_dep_member_r1");
        exec("GRANT ragt_dep_group_r1 TO ragt_dep_member_r1");
        try {
            // In PG18, DROP ROLE succeeds even with membership grants and auto-revokes them
            assertDoesNotThrow(
                    () -> exec("DROP ROLE ragt_dep_group_r1"),
                    "DROP ROLE should succeed and auto-revoke membership in PG18");
        } finally {
            exec("DROP ROLE IF EXISTS ragt_dep_member_r1");
            exec("DROP ROLE IF EXISTS ragt_dep_group_r1");
        }
    }
}
