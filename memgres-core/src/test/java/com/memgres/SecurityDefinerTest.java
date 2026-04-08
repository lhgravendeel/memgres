package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SECURITY DEFINER enforcement.
 * Verifies that current_user switches to function owner during execution
 * and reverts afterwards.
 */
class SecurityDefinerTest {

    private Memgres memgres;

    @BeforeEach
    void setUp() {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterEach
    void tearDown() {
        if (memgres != null) memgres.close();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    // ---- current_user inside SECURITY DEFINER ----

    @Test
    void securityDefinerSwitchesCurrentUser() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Function created by default user (owner = connecting user)
            stmt.execute("CREATE FUNCTION whoami() RETURNS text AS $$ "
                    + "BEGIN RETURN current_user; END; $$ LANGUAGE plpgsql SECURITY DEFINER");

            // Switch caller role to something different
            stmt.execute("CREATE ROLE other_user");
            stmt.execute("SET ROLE other_user");

            // Verify caller is now other_user
            try (ResultSet rs = stmt.executeQuery("SELECT current_user")) {
                assertTrue(rs.next());
                assertEquals("other_user", rs.getString(1));
            }

            // Call SECURITY DEFINER function — should return function owner, not caller
            try (ResultSet rs = stmt.executeQuery("SELECT whoami()")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                // Function owner is the user who created it (before SET ROLE)
                assertNotEquals("other_user", result);
            }
        }
    }

    // ---- current_user reverts after call ----

    @Test
    void currentUserRevertsAfterSecurityDefiner() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sd_whoami() RETURNS text AS $$ "
                    + "BEGIN RETURN current_user; END; $$ LANGUAGE plpgsql SECURITY DEFINER");
            stmt.execute("CREATE ROLE caller_role");
            stmt.execute("SET ROLE caller_role");

            // Call the function
            stmt.executeQuery("SELECT sd_whoami()").close();

            // After the call, current_user should be back to caller_role
            try (ResultSet rs = stmt.executeQuery("SELECT current_user")) {
                assertTrue(rs.next());
                assertEquals("caller_role", rs.getString(1));
            }
        }
    }

    // ---- session_user unchanged ----

    @Test
    void sessionUserUnchangedInSecurityDefiner() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sd_session_user() RETURNS text AS $$ "
                    + "BEGIN RETURN session_user; END; $$ LANGUAGE plpgsql SECURITY DEFINER");
            stmt.execute("CREATE ROLE other_user");
            stmt.execute("SET ROLE other_user");

            try (ResultSet rs = stmt.executeQuery("SELECT sd_session_user()")) {
                assertTrue(rs.next());
                // session_user should always be the original connecting user
                assertEquals("memgres", rs.getString(1));
            }
        }
    }

    // ---- SECURITY INVOKER keeps caller identity ----

    @Test
    void securityInvokerKeepsCallerIdentity() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION si_whoami() RETURNS text AS $$ "
                    + "BEGIN RETURN current_user; END; $$ LANGUAGE plpgsql SECURITY INVOKER");
            stmt.execute("CREATE ROLE invoker_user");
            stmt.execute("SET ROLE invoker_user");

            try (ResultSet rs = stmt.executeQuery("SELECT si_whoami()")) {
                assertTrue(rs.next());
                // SECURITY INVOKER: current_user stays as caller
                assertEquals("invoker_user", rs.getString(1));
            }
        }
    }

    // ---- Default (no security clause) = INVOKER ----

    @Test
    void defaultSecurityIsInvoker() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION default_whoami() RETURNS text AS $$ "
                    + "BEGIN RETURN current_user; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE ROLE default_caller");
            stmt.execute("SET ROLE default_caller");

            try (ResultSet rs = stmt.executeQuery("SELECT default_whoami()")) {
                assertTrue(rs.next());
                assertEquals("default_caller", rs.getString(1));
            }
        }
    }

    // ---- Nested: SECURITY DEFINER calling SECURITY INVOKER ----

    @Test
    void nestedDefinerCallingInvoker() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Inner function: SECURITY INVOKER — should see the DEFINER's owner as current_user
            stmt.execute("CREATE FUNCTION inner_invoker() RETURNS text AS $$ "
                    + "BEGIN RETURN current_user; END; $$ LANGUAGE plpgsql SECURITY INVOKER");
            // Outer function: SECURITY DEFINER — switches to owner
            stmt.execute("CREATE FUNCTION outer_definer() RETURNS text AS $$ "
                    + "BEGIN RETURN inner_invoker(); END; $$ LANGUAGE plpgsql SECURITY DEFINER");

            stmt.execute("CREATE ROLE nested_caller");
            stmt.execute("SET ROLE nested_caller");

            try (ResultSet rs = stmt.executeQuery("SELECT outer_definer()")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                // outer_definer switches current_user to owner, inner_invoker sees that
                assertNotEquals("nested_caller", result);
            }
        }
    }

    // ---- Nested: SECURITY INVOKER calling SECURITY DEFINER ----

    @Test
    void nestedInvokerCallingDefiner() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Inner function: SECURITY DEFINER — switches to its owner
            stmt.execute("CREATE FUNCTION inner_definer() RETURNS text AS $$ "
                    + "BEGIN RETURN current_user; END; $$ LANGUAGE plpgsql SECURITY DEFINER");
            // Outer function: SECURITY INVOKER — keeps caller
            stmt.execute("CREATE FUNCTION outer_invoker() RETURNS text AS $$ "
                    + "BEGIN RETURN inner_definer(); END; $$ LANGUAGE plpgsql SECURITY INVOKER");

            stmt.execute("CREATE ROLE nested_caller2");
            stmt.execute("SET ROLE nested_caller2");

            try (ResultSet rs = stmt.executeQuery("SELECT outer_invoker()")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                // outer_invoker keeps caller, but inner_definer switches to its owner
                assertNotEquals("nested_caller2", result);
            }

            // After call, should revert to caller
            try (ResultSet rs = stmt.executeQuery("SELECT current_user")) {
                assertTrue(rs.next());
                assertEquals("nested_caller2", rs.getString(1));
            }
        }
    }

    // ---- SECURITY DEFINER with SET clause ----

    @Test
    void securityDefinerWithSetClause() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sd_with_set() RETURNS text AS $$ "
                    + "BEGIN RETURN current_user; END; $$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog");
            stmt.execute("CREATE ROLE set_caller");
            stmt.execute("SET ROLE set_caller");

            try (ResultSet rs = stmt.executeQuery("SELECT sd_with_set()")) {
                assertTrue(rs.next());
                // current_user should be function owner, not set_caller
                assertNotEquals("set_caller", rs.getString(1));
            }

            // Verify current_user reverts
            try (ResultSet rs = stmt.executeQuery("SELECT current_user")) {
                assertTrue(rs.next());
                assertEquals("set_caller", rs.getString(1));
            }
        }
    }

    // ---- SECURITY DEFINER with SQL language function ----

    @Test
    void securityDefinerSqlLanguageFunction() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sql_whoami() RETURNS text AS $$ "
                    + "SELECT current_user; $$ LANGUAGE sql SECURITY DEFINER");
            stmt.execute("CREATE ROLE sql_caller");
            stmt.execute("SET ROLE sql_caller");

            try (ResultSet rs = stmt.executeQuery("SELECT sql_whoami()")) {
                assertTrue(rs.next());
                assertNotEquals("sql_caller", rs.getString(1));
            }
        }
    }

    // ---- pg_proc.prosecdef catalog flag ----

    @Test
    void prosecdefCatalogFlag() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION cat_definer() RETURNS integer AS $$ "
                    + "BEGIN RETURN 1; END; $$ LANGUAGE plpgsql SECURITY DEFINER");
            stmt.execute("CREATE FUNCTION cat_invoker() RETURNS integer AS $$ "
                    + "BEGIN RETURN 1; END; $$ LANGUAGE plpgsql SECURITY INVOKER");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT proname, prosecdef FROM pg_proc WHERE proname IN ('cat_definer', 'cat_invoker') ORDER BY proname")) {
                assertTrue(rs.next());
                assertEquals("cat_definer", rs.getString(1));
                assertTrue(rs.getBoolean(2));
                assertTrue(rs.next());
                assertEquals("cat_invoker", rs.getString(1));
                assertFalse(rs.getBoolean(2));
                assertFalse(rs.next());
            }
        }
    }

    // ---- Multiple SECURITY DEFINER calls in sequence ----

    @Test
    void multipleSecurityDefinerCallsInSequence() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sd1() RETURNS text AS $$ "
                    + "BEGIN RETURN current_user; END; $$ LANGUAGE plpgsql SECURITY DEFINER");
            stmt.execute("CREATE FUNCTION sd2() RETURNS text AS $$ "
                    + "BEGIN RETURN current_user; END; $$ LANGUAGE plpgsql SECURITY DEFINER");
            stmt.execute("CREATE ROLE multi_caller");
            stmt.execute("SET ROLE multi_caller");

            // Call first
            try (ResultSet rs = stmt.executeQuery("SELECT sd1()")) {
                assertTrue(rs.next());
                assertNotEquals("multi_caller", rs.getString(1));
            }
            // Verify revert
            try (ResultSet rs = stmt.executeQuery("SELECT current_user")) {
                assertTrue(rs.next());
                assertEquals("multi_caller", rs.getString(1));
            }
            // Call second
            try (ResultSet rs = stmt.executeQuery("SELECT sd2()")) {
                assertTrue(rs.next());
                assertNotEquals("multi_caller", rs.getString(1));
            }
            // Verify revert again
            try (ResultSet rs = stmt.executeQuery("SELECT current_user")) {
                assertTrue(rs.next());
                assertEquals("multi_caller", rs.getString(1));
            }
        }
    }

    // ---- SECURITY DEFINER function that errors still reverts role ----

    @Test
    void securityDefinerRevertsOnError() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sd_error() RETURNS integer AS $$ "
                    + "BEGIN RAISE EXCEPTION 'intentional error'; END; $$ LANGUAGE plpgsql SECURITY DEFINER");
            stmt.execute("CREATE ROLE error_caller");
            stmt.execute("SET ROLE error_caller");

            // Call should fail
            assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT sd_error()"));

            // current_user should still revert
            try (ResultSet rs = stmt.executeQuery("SELECT current_user")) {
                assertTrue(rs.next());
                assertEquals("error_caller", rs.getString(1));
            }
        }
    }

    // ---- Nested DEFINER calling DEFINER ----

    @Test
    void nestedDefinerCallingDefiner() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Create inner function owned by default user (memgres)
            stmt.execute("CREATE FUNCTION inner_def() RETURNS text AS $$ "
                    + "BEGIN RETURN current_user; END; $$ LANGUAGE plpgsql SECURITY DEFINER");
            // Create outer function also owned by default user
            stmt.execute("CREATE FUNCTION outer_def() RETURNS text AS $$ "
                    + "BEGIN RETURN inner_def(); END; $$ LANGUAGE plpgsql SECURITY DEFINER");

            stmt.execute("CREATE ROLE nested_def_caller");
            stmt.execute("SET ROLE nested_def_caller");

            // Both are SECURITY DEFINER owned by same user
            try (ResultSet rs = stmt.executeQuery("SELECT outer_def()")) {
                assertTrue(rs.next());
                assertNotEquals("nested_def_caller", rs.getString(1));
            }

            // Verify revert to caller
            try (ResultSet rs = stmt.executeQuery("SELECT current_user")) {
                assertTrue(rs.next());
                assertEquals("nested_def_caller", rs.getString(1));
            }
        }
    }

    // ---- SECURITY DEFINER function used in a view ----

    @Test
    void securityDefinerInView() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION view_whoami() RETURNS text AS $$ "
                    + "BEGIN RETURN current_user; END; $$ LANGUAGE plpgsql SECURITY DEFINER");
            stmt.execute("CREATE VIEW whoami_view AS SELECT view_whoami() AS u");

            stmt.execute("CREATE ROLE view_caller");
            stmt.execute("SET ROLE view_caller");

            try (ResultSet rs = stmt.executeQuery("SELECT u FROM whoami_view")) {
                assertTrue(rs.next());
                // SECURITY DEFINER should switch to owner even when called through a view
                assertNotEquals("view_caller", rs.getString(1));
            }
        }
    }

    // ---- SECURITY DEFINER aggregate SFUNC ----

    @Test
    void securityDefinerAsSfunc() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE sd_agg_data (val integer)");
            stmt.execute("INSERT INTO sd_agg_data VALUES (10), (20), (30)");

            // SFUNC is SECURITY DEFINER — should still work correctly as aggregate
            stmt.execute("CREATE FUNCTION sd_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql SECURITY DEFINER");
            stmt.execute("CREATE AGGREGATE sd_agg_sum(integer) ("
                    + "SFUNC = sd_sum_sf, STYPE = integer, INITCOND = '0')");

            stmt.execute("CREATE ROLE agg_caller");
            stmt.execute("SET ROLE agg_caller");

            try (ResultSet rs = stmt.executeQuery("SELECT sd_agg_sum(val) FROM sd_agg_data")) {
                assertTrue(rs.next());
                assertEquals(60, rs.getInt(1));
            }

            // Role should revert after aggregate execution
            try (ResultSet rs = stmt.executeQuery("SELECT current_user")) {
                assertTrue(rs.next());
                assertEquals("agg_caller", rs.getString(1));
            }
        }
    }

    // ---- OR REPLACE keeps owner ----

    @Test
    void orReplaceKeepsOwner() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Create function as default user
            stmt.execute("CREATE FUNCTION replace_me() RETURNS text AS $$ "
                    + "BEGIN RETURN current_user; END; $$ LANGUAGE plpgsql SECURITY DEFINER");

            // Replace it — owner should remain the original creator
            stmt.execute("CREATE OR REPLACE FUNCTION replace_me() RETURNS text AS $$ "
                    + "BEGIN RETURN 'replaced: ' || current_user; END; $$ LANGUAGE plpgsql SECURITY DEFINER");

            stmt.execute("CREATE ROLE replace_caller");
            stmt.execute("SET ROLE replace_caller");

            try (ResultSet rs = stmt.executeQuery("SELECT replace_me()")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                assertTrue(result.startsWith("replaced:"));
                assertFalse(result.contains("replace_caller"));
            }
        }
    }

    // ---- SECURITY DEFINER function that does DML ----

    @Test
    void securityDefinerFunctionWithDml() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE sd_log (who text, ts timestamp DEFAULT now())");
            stmt.execute("CREATE FUNCTION sd_log_action() RETURNS text AS $$ "
                    + "BEGIN INSERT INTO sd_log (who) VALUES (current_user); RETURN current_user; END; "
                    + "$$ LANGUAGE plpgsql SECURITY DEFINER");

            stmt.execute("CREATE ROLE dml_caller");
            stmt.execute("SET ROLE dml_caller");

            try (ResultSet rs = stmt.executeQuery("SELECT sd_log_action()")) {
                assertTrue(rs.next());
                // Function runs as owner, not caller
                assertNotEquals("dml_caller", rs.getString(1));
            }

            // The INSERT inside the function should have logged the owner's name
            stmt.execute("SET ROLE memgres");
            try (ResultSet rs = stmt.executeQuery("SELECT who FROM sd_log")) {
                assertTrue(rs.next());
                assertNotEquals("dml_caller", rs.getString(1));
            }
        }
    }
}
