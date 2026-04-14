package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 4 failures from security-definer.sql where Memgres diverges from PG 18.
 *
 * Stmt 7:  secdef_definer_user() = current_user should be true (PG=[t], Memgres=[f])
 * Stmt 9:  session_user = current_user inside SECURITY DEFINER should be true (PG=[t], Memgres=[f])
 * Stmt 27: SECURITY DEFINER with SET search_path should return pg_catalog (PG=[pg_catalog], Memgres=[NULL])
 * Stmt 54: SRF in FROM clause should work (PG succeeds with cnt=2, Memgres errors)
 */
class SecurityDefinerTest {

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
            s.execute("DROP SCHEMA IF EXISTS secdef_test CASCADE");
            s.execute("CREATE SCHEMA secdef_test");
            s.execute("SET search_path = secdef_test, public");

            // Stmt 7: SECURITY DEFINER function returning current_user
            s.execute("CREATE FUNCTION secdef_definer_user() RETURNS text "
                    + "LANGUAGE sql SECURITY DEFINER AS $$ SELECT current_user::text $$");

            // Stmt 9: SECURITY DEFINER function returning session_user and current_user
            s.execute("CREATE FUNCTION secdef_both_users() RETURNS TABLE(sess text, curr text) "
                    + "LANGUAGE sql SECURITY DEFINER AS $$ "
                    + "SELECT session_user::text, current_user::text $$");

            // Stmt 27: SECURITY DEFINER with SET search_path = pg_catalog
            s.execute("CREATE FUNCTION secdef_with_set() RETURNS text "
                    + "LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$ "
                    + "DECLARE sp text; "
                    + "BEGIN SHOW search_path INTO sp; RETURN sp; END; $$");

            // Stmt 54: SECURITY DEFINER RETURNS TABLE used in FROM clause
            s.execute("CREATE FUNCTION secdef_user_contexts() RETURNS TABLE(ctx text, val text) "
                    + "LANGUAGE plpgsql SECURITY DEFINER AS $$ "
                    + "BEGIN "
                    + "ctx := 'current_user'; val := current_user; RETURN NEXT; "
                    + "ctx := 'session_user'; val := session_user; RETURN NEXT; "
                    + "RETURN; "
                    + "END; $$");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS secdef_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    /**
     * Stmt 7: SECURITY DEFINER current_user should equal the caller's current_user.
     *
     * In a single-user setup the function owner is the same as the session user,
     * so secdef_definer_user() = current_user should be true.
     *
     * PG 18: [t]
     * Memgres: [f]
     */
    @Test
    void securityDefinerUserMatchesCurrentUser() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT secdef_definer_user() = current_user AS match")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("match"),
                    "SECURITY DEFINER function's current_user should equal caller's current_user in single-user setup");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 9: Inside a SECURITY DEFINER function, session_user should equal current_user
     * when the function owner is the same as the session user.
     *
     * PG 18: [t]
     * Memgres: [f]
     */
    @Test
    void sessionUserMatchesCurrentUserInSecurityDefiner() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT (SELECT sess = curr FROM secdef_both_users()) AS session_matches")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("session_matches"),
                    "session_user should equal current_user inside SECURITY DEFINER in single-user setup");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 27: SECURITY DEFINER with SET search_path = pg_catalog should
     * return 'pg_catalog' when querying SHOW search_path inside the function.
     *
     * PG 18: [pg_catalog]
     * Memgres: [NULL]
     */
    @Test
    void securityDefinerWithSetReturnsConfiguredSearchPath() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT secdef_with_set()")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals("pg_catalog", rs.getString(1),
                    "SECURITY DEFINER function with SET search_path = pg_catalog should return 'pg_catalog'");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 54: Calling a set-returning SECURITY DEFINER function in FROM clause
     * should succeed and return 2 rows.
     *
     * PG 18: succeeds with cnt=2
     * Memgres: ERROR [42000] Set-returning function not supported in FROM: secdef_user_contexts
     */
    @Test
    void securityDefinerReturnsTableInFromClause() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) AS cnt FROM secdef_user_contexts()")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals(2, rs.getInt("cnt"),
                    "secdef_user_contexts() should return 2 rows (current_user and session_user)");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }
}
