package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diffs #22-23: SET ROLE / DROP ROLE succeed after the role was already dropped
 * Diff #24: GRANT on nonexistent table → SQLSTATE should be 42P01 not 42704
 * Diff #25: SET ROLE nonexistent → SQLSTATE should be 22023 not 42704
 * Diff #39: DROP ROLE succeeds when role was already dropped via CASCADE
 */
class RoleAndSessionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // Diffs #22-23: role lifecycle, SET ROLE / DROP ROLE after drop
    @Test void set_role_after_drop_fails() throws SQLException {
        exec("CREATE ROLE test_role_1");
        exec("DROP ROLE test_role_1");
        try {
            exec("SET ROLE test_role_1");
            fail("SET ROLE after DROP should fail");
        } catch (SQLException e) {
            // accept either 42704 or 22023
            assertTrue("42704".equals(e.getSQLState()) || "22023".equals(e.getSQLState()),
                "SET ROLE dropped role should fail, got " + e.getSQLState());
        }
    }

    @Test void drop_role_after_drop_fails() throws SQLException {
        exec("CREATE ROLE test_role_2");
        exec("DROP ROLE test_role_2");
        try {
            exec("DROP ROLE test_role_2");
            fail("Second DROP ROLE should fail");
        } catch (SQLException e) {
            // expected
        }
    }

    // Diff #24: GRANT on nonexistent table → 42P01
    @Test void grant_on_nonexistent_table_sqlstate() throws SQLException {
        exec("CREATE ROLE test_role_3");
        try {
            try {
                exec("GRANT SELECT ON no_such_table TO test_role_3");
                fail("Should fail");
            } catch (SQLException e) {
                assertEquals("42P01", e.getSQLState(),
                    "GRANT on nonexistent table should be 42P01, got " + e.getSQLState());
            }
        } finally {
            exec("DROP ROLE test_role_3");
        }
    }

    // Diff #25: SET ROLE nonexistent → 22023
    @Test void set_role_nonexistent_sqlstate() {
        try {
            exec("SET ROLE totally_nonexistent_role");
            fail("Should fail");
        } catch (SQLException e) {
            assertEquals("22023", e.getSQLState(),
                "SET ROLE nonexistent should be 22023, got " + e.getSQLState());
        }
    }

    // Diff #39: DROP ROLE that was created earlier in session still exists
    @Test void role_lifecycle_create_use_drop() throws SQLException {
        exec("CREATE ROLE lifecycle_role");
        exec("GRANT lifecycle_role TO " + conn.getMetaData().getUserName());
        exec("SET ROLE lifecycle_role");
        exec("RESET ROLE");
        exec("DROP ROLE lifecycle_role");
        // After DROP, the role should truly be gone
        try {
            exec("SET ROLE lifecycle_role");
            fail("Role should be gone after DROP");
        } catch (SQLException ignored) {}
        try {
            exec("DROP ROLE lifecycle_role");
            fail("Second DROP should fail");
        } catch (SQLException ignored) {}
    }

    // Additional: DROP ROLE IF EXISTS on nonexistent role should succeed
    @Test void drop_role_if_exists_nonexistent() throws SQLException {
        exec("DROP ROLE IF EXISTS definitely_not_a_role");
        // should not throw
    }
}
