package com.memgres.privilege;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for privilege enforcement: C6, M9, M10, M11, M12.
 */
class PrivilegeEnforcementTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void stop() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private List<String> query(String sql) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append(",");
                    sb.append(rs.getString(i));
                }
                rows.add(sb.toString());
            }
        }
        return rows;
    }

    // === C6: Privilege enforcement ===

    @Test
    void c6_selectDeniedWithoutGrant() throws Exception {
        exec("CREATE ROLE c6r LOGIN");
        exec("CREATE TABLE c6t(id int)");
        exec("INSERT INTO c6t VALUES (1)");
        try {
            exec("SET ROLE c6r");
            SQLException ex = assertThrows(SQLException.class, () -> query("SELECT * FROM c6t"));
            assertEquals("42501", ex.getSQLState());
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE IF EXISTS c6t");
            exec("DROP ROLE IF EXISTS c6r");
        }
    }

    @Test
    void c6_insertDeniedWithoutGrant() throws Exception {
        exec("CREATE ROLE c6ri LOGIN");
        exec("CREATE TABLE c6ti(id int)");
        try {
            exec("SET ROLE c6ri");
            SQLException ex = assertThrows(SQLException.class, () -> exec("INSERT INTO c6ti VALUES (1)"));
            assertEquals("42501", ex.getSQLState());
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE IF EXISTS c6ti");
            exec("DROP ROLE IF EXISTS c6ri");
        }
    }

    @Test
    void c6_updateDeniedWithoutGrant() throws Exception {
        exec("CREATE ROLE c6ru LOGIN");
        exec("CREATE TABLE c6tu(id int)");
        exec("INSERT INTO c6tu VALUES (1)");
        try {
            exec("SET ROLE c6ru");
            SQLException ex = assertThrows(SQLException.class, () -> exec("UPDATE c6tu SET id = 2"));
            assertEquals("42501", ex.getSQLState());
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE IF EXISTS c6tu");
            exec("DROP ROLE IF EXISTS c6ru");
        }
    }

    @Test
    void c6_deleteDeniedWithoutGrant() throws Exception {
        exec("CREATE ROLE c6rd LOGIN");
        exec("CREATE TABLE c6td(id int)");
        exec("INSERT INTO c6td VALUES (1)");
        try {
            exec("SET ROLE c6rd");
            SQLException ex = assertThrows(SQLException.class, () -> exec("DELETE FROM c6td"));
            assertEquals("42501", ex.getSQLState());
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE IF EXISTS c6td");
            exec("DROP ROLE IF EXISTS c6rd");
        }
    }

    @Test
    void c6_truncateDeniedWithoutGrant() throws Exception {
        exec("CREATE ROLE c6rt LOGIN");
        exec("CREATE TABLE c6tt(id int)");
        try {
            exec("SET ROLE c6rt");
            SQLException ex = assertThrows(SQLException.class, () -> exec("TRUNCATE c6tt"));
            assertEquals("42501", ex.getSQLState());
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE IF EXISTS c6tt");
            exec("DROP ROLE IF EXISTS c6rt");
        }
    }

    @Test
    void c6_grantedSelectAllowed() throws Exception {
        exec("CREATE ROLE c6rg LOGIN");
        exec("CREATE TABLE c6tg(id int)");
        exec("INSERT INTO c6tg VALUES (1)");
        exec("GRANT SELECT ON c6tg TO c6rg");
        try {
            exec("SET ROLE c6rg");
            List<String> result = query("SELECT * FROM c6tg");
            assertEquals(List.of("1"), result);
            // INSERT should still be denied
            SQLException ex = assertThrows(SQLException.class, () -> exec("INSERT INTO c6tg VALUES (2)"));
            assertEquals("42501", ex.getSQLState());
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE IF EXISTS c6tg");
            exec("DROP ROLE IF EXISTS c6rg");
        }
    }

    @Test
    void c6_revokedSelectDenied() throws Exception {
        exec("CREATE ROLE c6rv LOGIN");
        exec("CREATE TABLE c6tv(id int)");
        exec("GRANT SELECT ON c6tv TO c6rv");
        exec("REVOKE SELECT ON c6tv FROM c6rv");
        try {
            exec("SET ROLE c6rv");
            SQLException ex = assertThrows(SQLException.class, () -> query("SELECT * FROM c6tv"));
            assertEquals("42501", ex.getSQLState());
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE IF EXISTS c6tv");
            exec("DROP ROLE IF EXISTS c6rv");
        }
    }

    @Test
    void c6_ownerHasAllPrivileges() throws Exception {
        exec("CREATE ROLE c6ro LOGIN");
        try {
            exec("SET ROLE c6ro");
            exec("CREATE TABLE c6to(id int)");
            exec("INSERT INTO c6to VALUES (1)");
            List<String> result = query("SELECT * FROM c6to");
            assertEquals(List.of("1"), result);
            exec("UPDATE c6to SET id = 2");
            exec("DELETE FROM c6to");
            exec("TRUNCATE c6to");
            exec("DROP TABLE c6to");
        } finally {
            exec("RESET ROLE");
            try { exec("DROP TABLE IF EXISTS c6to"); } catch (Exception ignored) {}
            exec("DROP ROLE IF EXISTS c6ro");
        }
    }

    // === M9: Grant-option semantics ===

    @Test
    void m9_cannotGrantPrivilegeNotHeld() throws Exception {
        exec("CREATE ROLE m9grantor LOGIN");
        exec("CREATE ROLE m9grantee LOGIN");
        exec("CREATE TABLE m9t(id int)");
        exec("GRANT SELECT ON m9t TO m9grantor WITH GRANT OPTION");
        try {
            exec("SET ROLE m9grantor");
            // Can grant SELECT (held with grant option)
            exec("GRANT SELECT ON m9t TO m9grantee");
            // Cannot grant INSERT (not held)
            SQLException ex = assertThrows(SQLException.class,
                () -> exec("GRANT INSERT ON m9t TO m9grantee"));
            assertEquals("42501", ex.getSQLState());
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE IF EXISTS m9t");
            exec("DROP ROLE IF EXISTS m9grantee");
            exec("DROP ROLE IF EXISTS m9grantor");
        }
    }

    @Test
    void m9_hasTablePrivilegeWithGrantOption() throws Exception {
        exec("CREATE ROLE m9hg LOGIN");
        exec("CREATE TABLE m9ht(id int)");
        exec("GRANT SELECT ON m9ht TO m9hg WITH GRANT OPTION");
        try {
            assertEquals(List.of("t"), query("SELECT has_table_privilege('m9hg', 'm9ht', 'SELECT')"));
            assertEquals(List.of("t"), query("SELECT has_table_privilege('m9hg', 'm9ht', 'SELECT WITH GRANT OPTION')"));
            assertEquals(List.of("f"), query("SELECT has_table_privilege('m9hg', 'm9ht', 'INSERT WITH GRANT OPTION')"));
        } finally {
            exec("DROP TABLE IF EXISTS m9ht");
            exec("DROP ROLE IF EXISTS m9hg");
        }
    }

    @Test
    void m9_revokeGrantOptionOnly() throws Exception {
        exec("CREATE ROLE m9rv LOGIN");
        exec("CREATE TABLE m9rt(id int)");
        exec("GRANT SELECT ON m9rt TO m9rv WITH GRANT OPTION");
        exec("REVOKE GRANT OPTION FOR SELECT ON m9rt FROM m9rv");
        try {
            // Should still have SELECT privilege
            assertEquals(List.of("t"), query("SELECT has_table_privilege('m9rv', 'm9rt', 'SELECT')"));
            // But no longer WITH GRANT OPTION
            assertEquals(List.of("f"), query("SELECT has_table_privilege('m9rv', 'm9rt', 'SELECT WITH GRANT OPTION')"));
        } finally {
            exec("DROP TABLE IF EXISTS m9rt");
            exec("DROP ROLE IF EXISTS m9rv");
        }
    }

    // === M10: Schema-qualified GRANT/REVOKE ===

    @Test
    void m10_grantOnSchemaQualifiedTable() throws Exception {
        exec("CREATE SCHEMA m10s");
        exec("CREATE TABLE m10s.m10t(id int)");
        exec("CREATE ROLE m10r LOGIN");
        try {
            exec("GRANT SELECT ON m10s.m10t TO m10r");
            exec("SET ROLE m10r");
            List<String> result = query("SELECT * FROM m10s.m10t");
            assertEquals(List.of(), result);
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE IF EXISTS m10s.m10t");
            exec("DROP SCHEMA IF EXISTS m10s");
            exec("DROP ROLE IF EXISTS m10r");
        }
    }

    // === M11: ALTER DEFAULT PRIVILEGES ===

    @Test
    void m11_defaultPrivilegesAppliedToNewTables() throws Exception {
        exec("CREATE ROLE m11creator LOGIN");
        exec("CREATE ROLE m11reader LOGIN");
        exec("ALTER DEFAULT PRIVILEGES FOR ROLE m11creator GRANT SELECT ON TABLES TO m11reader");
        try {
            exec("SET ROLE m11creator");
            exec("CREATE TABLE m11t(id int)");
            exec("INSERT INTO m11t VALUES (1)");
            exec("SET ROLE m11reader");
            List<String> result = query("SELECT * FROM m11t");
            assertEquals(List.of("1"), result);
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE IF EXISTS m11t");
            exec("DROP ROLE IF EXISTS m11reader");
            exec("DROP ROLE IF EXISTS m11creator");
        }
    }

    // === M12: Role plumbing ===

    @Test
    void m12_createRoleInRoleRecordsMembership() throws Exception {
        exec("CREATE ROLE m12group");
        exec("CREATE ROLE m12member IN ROLE m12group");
        try {
            assertEquals(List.of("t"),
                query("SELECT pg_has_role('m12member', 'm12group', 'MEMBER')"));
        } finally {
            exec("DROP ROLE IF EXISTS m12member");
            exec("DROP ROLE IF EXISTS m12group");
        }
    }

    @Test
    void m12_inheritedPrivilegesThroughRoleMembership() throws Exception {
        exec("CREATE ROLE m12grp");
        exec("CREATE ROLE m12usr IN ROLE m12grp LOGIN");
        exec("CREATE TABLE m12t(id int)");
        exec("INSERT INTO m12t VALUES (1)");
        exec("GRANT SELECT ON m12t TO m12grp");
        try {
            exec("SET ROLE m12usr");
            List<String> result = query("SELECT * FROM m12t");
            assertEquals(List.of("1"), result);
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE IF EXISTS m12t");
            exec("DROP ROLE IF EXISTS m12usr");
            exec("DROP ROLE IF EXISTS m12grp");
        }
    }

    @Test
    void m12_superuserHasAllPrivileges() throws Exception {
        exec("CREATE ROLE m12su SUPERUSER LOGIN");
        exec("CREATE TABLE m12st(id int)");
        exec("INSERT INTO m12st VALUES (1)");
        try {
            exec("SET ROLE m12su");
            List<String> result = query("SELECT * FROM m12st");
            assertEquals(List.of("1"), result);
            exec("INSERT INTO m12st VALUES (2)");
            exec("UPDATE m12st SET id = 3 WHERE id = 2");
            exec("DELETE FROM m12st WHERE id = 3");
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE IF EXISTS m12st");
            exec("DROP ROLE IF EXISTS m12su");
        }
    }

    @Test
    void m12_publicSchemaUsageImplicit() throws Exception {
        // PUBLIC role should have implicit USAGE on public schema
        assertEquals(List.of("t"),
            query("SELECT has_schema_privilege('public', 'public', 'USAGE')"));
    }
}
