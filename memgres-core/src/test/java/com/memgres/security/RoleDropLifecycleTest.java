package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diffs #14, #15: SET ROLE / DROP ROLE succeed after role was already dropped.
 * Diff #28: DROP ROLE compat_role2 succeeds when it shouldn't.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RoleDropLifecycleTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // Diff #14: SET ROLE on already-dropped role should fail
    @Test @Order(1) void set_role_after_explicit_drop_fails() throws SQLException {
        exec("CREATE ROLE compat_role_test1");
        exec("GRANT compat_role_test1 TO memgres");
        exec("SET ROLE compat_role_test1");
        exec("RESET ROLE");
        exec("DROP ROLE compat_role_test1");
        // Role is now dropped, so SET ROLE should fail
        assertThrows(SQLException.class, () -> exec("SET ROLE compat_role_test1"),
            "SET ROLE on dropped role should fail");
    }

    // Diff #15: DROP ROLE on already-dropped role should fail
    @Test @Order(2) void drop_role_after_explicit_drop_fails() throws SQLException {
        exec("CREATE ROLE compat_role_test2");
        exec("DROP ROLE compat_role_test2");
        assertThrows(SQLException.class, () -> exec("DROP ROLE compat_role_test2"),
            "Second DROP ROLE should fail");
    }

    // Diff #28: DROP ROLE for a role created within a schema context should also be dropped
    @Test @Order(3) void drop_role_created_in_session() throws SQLException {
        exec("CREATE ROLE compat_role_test3");
        exec("DROP ROLE compat_role_test3");
        // Verify the role is truly gone and cannot be granted to
        assertThrows(SQLException.class, () -> exec("GRANT SELECT ON pg_class TO compat_role_test3"),
            "Role should be completely gone after DROP");
    }
}
