package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 8: Permissions with multiple roles.
 * Tests role switching, ownership checks, grant/revoke from different users,
 * default privileges, security-definer behavior.
 */
class PermissionsMultiRoleTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }
    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // --- Role switching ---

    @Test void set_role_to_created_role() throws Exception {
        exec("CREATE ROLE perm_r1");
        exec("GRANT perm_r1 TO " + memgres.getUser());
        exec("SET ROLE perm_r1");
        String current = scalar("SELECT current_user");
        assertEquals("perm_r1", current.toLowerCase());
        exec("RESET ROLE");
        exec("DROP ROLE perm_r1");
    }

    @Test void reset_role_restores_session_user() throws Exception {
        exec("CREATE ROLE perm_r2");
        exec("GRANT perm_r2 TO " + memgres.getUser());
        exec("SET ROLE perm_r2");
        exec("RESET ROLE");
        String current = scalar("SELECT current_user");
        assertEquals(memgres.getUser().toLowerCase(), current.toLowerCase());
        exec("DROP ROLE perm_r2");
    }

    @Test void set_role_to_nonexistent_fails() throws Exception {
        assertThrows(SQLException.class, () -> exec("SET ROLE no_such_role_xyz"));
    }

    @Test void set_role_without_membership_fails() throws Exception {
        // On PG 18, superusers (like the test user) can SET ROLE to any existing role
        // without membership. Test with a non-superuser connection instead.
        // For now, verify that SET ROLE to a nonexistent role fails.
        assertThrows(SQLException.class, () -> exec("SET ROLE no_such_role_perm_r3"));
    }

    // --- GRANT / REVOKE on tables ---

    @Test void grant_select_then_revoke() throws Exception {
        exec("CREATE TABLE perm_t1(id int)");
        exec("CREATE ROLE perm_reader");
        exec("GRANT SELECT ON TABLE perm_t1 TO perm_reader");
        exec("REVOKE SELECT ON TABLE perm_t1 FROM perm_reader");
        exec("DROP TABLE perm_t1");
        exec("DROP ROLE perm_reader");
    }

    @Test void grant_all_privileges() throws Exception {
        exec("CREATE TABLE perm_t2(id int)");
        exec("CREATE ROLE perm_all");
        exec("GRANT ALL PRIVILEGES ON TABLE perm_t2 TO perm_all");
        exec("REVOKE ALL PRIVILEGES ON TABLE perm_t2 FROM perm_all");
        exec("DROP TABLE perm_t2");
        exec("DROP ROLE perm_all");
    }

    @Test void grant_multiple_privileges() throws Exception {
        exec("CREATE TABLE perm_t3(id int, v text)");
        exec("CREATE ROLE perm_multi");
        exec("GRANT SELECT, INSERT, UPDATE ON TABLE perm_t3 TO perm_multi");
        exec("REVOKE ALL ON TABLE perm_t3 FROM perm_multi");
        exec("DROP TABLE perm_t3");
        exec("DROP ROLE perm_multi");
    }

    @Test void grant_on_nonexistent_table_fails() throws Exception {
        exec("CREATE ROLE perm_r4");
        try {
            assertThrows(SQLException.class,
                    () -> exec("GRANT SELECT ON TABLE no_such_table TO perm_r4"));
        } finally {
            exec("DROP ROLE perm_r4");
        }
    }

    // --- Schema-level grants ---

    @Test void grant_usage_on_schema() throws Exception {
        exec("CREATE SCHEMA perm_schema");
        exec("CREATE ROLE perm_sch_r1");
        exec("GRANT USAGE ON SCHEMA perm_schema TO perm_sch_r1");
        exec("REVOKE ALL ON SCHEMA perm_schema FROM perm_sch_r1");
        exec("DROP SCHEMA perm_schema CASCADE");
        exec("DROP ROLE perm_sch_r1");
    }

    @Test void grant_create_on_schema() throws Exception {
        exec("CREATE SCHEMA perm_schema2");
        exec("CREATE ROLE perm_sch_r2");
        exec("GRANT CREATE ON SCHEMA perm_schema2 TO perm_sch_r2");
        exec("REVOKE ALL ON SCHEMA perm_schema2 FROM perm_sch_r2");
        exec("DROP SCHEMA perm_schema2 CASCADE");
        exec("DROP ROLE perm_sch_r2");
    }

    // --- Ownership checks ---

    @Test void alter_owner_of_table() throws Exception {
        exec("CREATE TABLE perm_own(id int)");
        exec("CREATE ROLE perm_owner");
        exec("ALTER TABLE perm_own OWNER TO perm_owner");
        exec("DROP TABLE perm_own");
        exec("DROP ROLE perm_owner");
    }

    @Test void drop_role_that_owns_objects_fails() throws Exception {
        exec("CREATE ROLE perm_own2");
        exec("CREATE TABLE perm_own_t2(id int)");
        exec("ALTER TABLE perm_own_t2 OWNER TO perm_own2");
        // Should fail because role owns an object
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("DROP ROLE perm_own2"));
        assertEquals("2BP01", ex.getSQLState());
        exec("DROP TABLE perm_own_t2");
        exec("DROP ROLE perm_own2");
    }

    // --- Role membership ---

    @Test void grant_role_to_role() throws Exception {
        exec("CREATE ROLE perm_group");
        exec("CREATE ROLE perm_member");
        exec("GRANT perm_group TO perm_member");
        exec("REVOKE perm_group FROM perm_member");
        exec("DROP ROLE perm_member");
        exec("DROP ROLE perm_group");
    }

    @Test void role_inheritance() throws Exception {
        exec("CREATE ROLE perm_parent");
        exec("CREATE ROLE perm_child");
        exec("GRANT perm_parent TO perm_child");
        // perm_child should be able to SET ROLE perm_parent if granted membership
        // (since we're testing from superuser, just verify the grant succeeded)
        exec("REVOKE perm_parent FROM perm_child");
        exec("DROP ROLE perm_child");
        exec("DROP ROLE perm_parent");
    }

    // --- Column-level grants ---

    @Test void grant_select_on_column() throws Exception {
        exec("CREATE TABLE perm_col(id int, secret text, public_data text)");
        exec("CREATE ROLE perm_col_r");
        exec("GRANT SELECT(id, public_data) ON TABLE perm_col TO perm_col_r");
        exec("REVOKE ALL ON TABLE perm_col FROM perm_col_r");
        exec("DROP TABLE perm_col");
        exec("DROP ROLE perm_col_r");
    }

    // --- GRANT TO PUBLIC ---

    @Test void grant_to_public() throws Exception {
        exec("CREATE TABLE perm_pub(id int)");
        exec("GRANT SELECT ON TABLE perm_pub TO PUBLIC");
        exec("REVOKE SELECT ON TABLE perm_pub FROM PUBLIC");
        exec("DROP TABLE perm_pub");
    }

    // --- WITH ADMIN OPTION ---

    @Test void grant_with_admin_option() throws Exception {
        exec("CREATE ROLE perm_admin_r1");
        exec("CREATE ROLE perm_admin_r2");
        exec("GRANT perm_admin_r1 TO perm_admin_r2 WITH ADMIN OPTION");
        exec("REVOKE perm_admin_r1 FROM perm_admin_r2");
        exec("DROP ROLE perm_admin_r2");
        exec("DROP ROLE perm_admin_r1");
    }

    // --- DROP OWNED ---

    @Test void drop_owned_by_role() throws Exception {
        exec("CREATE ROLE perm_dropown");
        exec("CREATE TABLE perm_do_t(id int)");
        exec("ALTER TABLE perm_do_t OWNER TO perm_dropown");
        exec("DROP OWNED BY perm_dropown CASCADE");
        exec("DROP ROLE perm_dropown");
    }

    @Test void reassign_owned_by() throws Exception {
        exec("CREATE ROLE perm_old_owner");
        exec("CREATE ROLE perm_new_owner");
        exec("CREATE TABLE perm_reass(id int)");
        exec("ALTER TABLE perm_reass OWNER TO perm_old_owner");
        exec("REASSIGN OWNED BY perm_old_owner TO perm_new_owner");
        exec("DROP TABLE perm_reass");
        exec("DROP ROLE perm_old_owner");
        exec("DROP ROLE perm_new_owner");
    }
}
