package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 2 remaining Memgres-vs-PG differences from security-definer.sql.
 *
 * These tests assert PG 18 behavior. They are expected to FAIL on current
 * Memgres and pass once the underlying issues are fixed.
 */
class RemainingSecurityCompat15Test {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS secdef_compat CASCADE");
            s.execute("CREATE SCHEMA secdef_compat");
            s.execute("SET search_path = secdef_compat, public");

            // Create SECURITY DEFINER function that returns current_user
            s.execute("CREATE FUNCTION secdef_definer_user() RETURNS text "
                    + "LANGUAGE sql SECURITY DEFINER AS $$ SELECT current_user::text $$");

            // Create SECURITY DEFINER function returning both session_user and current_user
            s.execute("CREATE FUNCTION secdef_both_users() RETURNS TABLE(sess text, curr text) "
                    + "LANGUAGE sql SECURITY DEFINER AS $$ "
                    + "SELECT session_user::text, current_user::text $$");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS secdef_compat CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    // ========================================================================
    // Stmt 7: SECURITY DEFINER function should switch current_user to the
    // function owner. In single-user setup, owner = session user, so
    // secdef_definer_user() should equal current_user.
    // PG: match = true
    // Memgres: match = false
    // ========================================================================
    @Test
    void stmt7_securityDefinerShouldSwitchCurrentUser() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT secdef_definer_user() = current_user AS match")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("match"),
                    "SECURITY DEFINER function should return current_user matching "
                    + "the function owner (which is the session user in single-user setup)");
        }
    }

    // ========================================================================
    // Stmt 9: SECURITY DEFINER function should report session_user vs
    // current_user correctly. In single-user setup, session_user = current_user
    // inside a SECURITY DEFINER function (both are the owner).
    // PG: session_matches = true
    // Memgres: session_matches = false
    // ========================================================================
    @Test
    void stmt9_securityDefinerSessionVsCurrentUser() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT (SELECT sess = curr FROM secdef_both_users()) AS session_matches")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("session_matches"),
                    "In SECURITY DEFINER function, session_user and current_user should both "
                    + "reflect the function owner in a single-user setup");
        }
    }
}
