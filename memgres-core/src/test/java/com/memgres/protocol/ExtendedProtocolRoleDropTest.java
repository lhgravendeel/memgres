package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Issue #2: Role not dropped from session state.
 * SET ROLE / DROP ROLE succeed after role was already dropped.
 * Extended query protocol version.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ExtendedProtocolRoleDropTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    @Test @Order(1) void set_role_after_drop_fails() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE ROLE ext_role_test1");
            s.execute("GRANT ext_role_test1 TO " + conn.getMetaData().getUserName());
            s.execute("SET ROLE ext_role_test1");
            s.execute("RESET ROLE");
            s.execute("DROP ROLE ext_role_test1");
        }
        try (PreparedStatement ps = conn.prepareStatement("SET ROLE ext_role_test1")) {
            assertThrows(SQLException.class, ps::execute,
                "SET ROLE on dropped role should fail");
        }
    }

    @Test @Order(2) void drop_role_twice_fails() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE ROLE ext_role_test2");
            s.execute("DROP ROLE ext_role_test2");
        }
        try (PreparedStatement ps = conn.prepareStatement("DROP ROLE ext_role_test2")) {
            assertThrows(SQLException.class, ps::execute,
                "Second DROP ROLE should fail");
        }
    }

    @Test @Order(3) void dropped_role_cannot_be_granted_to() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE ROLE ext_role_test3");
            s.execute("DROP ROLE ext_role_test3");
        }
        try (PreparedStatement ps = conn.prepareStatement("GRANT SELECT ON pg_class TO ext_role_test3")) {
            assertThrows(SQLException.class, ps::execute,
                "Role should be completely gone after DROP");
        }
    }
}
