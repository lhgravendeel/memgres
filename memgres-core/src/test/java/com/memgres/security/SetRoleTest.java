package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SET ROLE and related session-level SET commands.
 */
class SetRoleTest {

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

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    @Test
    void testSetRole() throws SQLException {
        exec("SET ROLE memgres");
    }

    @Test
    void testSetRoleQuoted() throws SQLException {
        exec("GRANT memgres TO test");
        exec("SET ROLE 'memgres'");
    }

    @Test
    void testSetRoleNone() throws SQLException {
        exec("SET ROLE NONE");
    }

    @Test
    void testSetRoleDefault() throws SQLException {
        exec("SET ROLE DEFAULT");
    }

    @Test
    void testResetRole() throws SQLException {
        exec("RESET ROLE");
    }

    @Test
    void testSetLocalRole() throws SQLException {
        exec("SET LOCAL ROLE memgres");
    }

    @Test
    void testSetSessionRole() throws SQLException {
        exec("GRANT memgres TO test");
        exec("SET SESSION ROLE memgres");
    }

    @Test
    void testSetRoleThenQuery() throws SQLException {
        exec("GRANT memgres TO test");
        exec("SET ROLE memgres");
        // Connection must remain usable after SET ROLE
        exec("CREATE TABLE set_role_test (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO set_role_test (val) VALUES ('after_role')");
        assertEquals("after_role", query1("SELECT val FROM set_role_test"));
    }

    @Test
    void testSetRoleCurrentUser() throws SQLException {
        exec("SET ROLE CURRENT_USER");
    }
}
