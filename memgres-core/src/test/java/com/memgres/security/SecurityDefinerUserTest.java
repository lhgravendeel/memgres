package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 2 PG vs Memgres differences from security-definer.sql.
 *
 * These tests assert exact PG 18 behavior. They are expected to FAIL on
 * current Memgres, documenting the real gaps.
 *
 * Uses default JDBC (extended query protocol) to match the comparison framework.
 */
class SecurityDefinerUserTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS secdef_compat CASCADE");
            s.execute("CREATE SCHEMA secdef_compat");
            s.execute("SET search_path = secdef_compat, public");

            s.execute("CREATE FUNCTION secdef_definer_user() RETURNS text "
                    + "LANGUAGE sql SECURITY DEFINER AS $$ SELECT current_user::text $$");

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
        if (memgres != null) memgres.close();
    }

    /**
     * Stmt 7: SECURITY DEFINER function should return current_user matching
     * the function owner.
     *
     * PG: secdef_definer_user() = current_user → true
     * Memgres: → false (in extended query protocol)
     */
    @Test
    void stmt7_securityDefinerCurrentUserShouldMatch() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT secdef_definer_user() = current_user AS match")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("match"),
                    "PG returns true: SECURITY DEFINER switches current_user to function owner. "
                    + "Memgres returns false.");
        }
    }

    /**
     * Stmt 9: Inside SECURITY DEFINER function, session_user should equal
     * current_user (in single-user setup).
     *
     * PG: session_matches = true
     * Memgres: session_matches = false (in extended query protocol)
     */
    @Test
    void stmt9_securityDefinerSessionMatchesCurrentUser() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT (SELECT sess = curr FROM secdef_both_users()) AS session_matches")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("session_matches"),
                    "PG returns true: session_user = current_user inside SECURITY DEFINER. "
                    + "Memgres returns false.");
        }
    }
}
