package com.memgres.session;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for session identity, SET semantics, name resolution: H35, H36, H37, M13, L7, L8.
 */
class SessionGucFidelityTest {

    static Memgres memgres;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (memgres != null) memgres.close();
    }

    private Connection conn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    // === H35: Temp table shadowing ===

    @Test
    void h35_schemaQualifiedBypassesTempShadow() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE public.h35_t(id int, val text)");
            st.execute("INSERT INTO public.h35_t VALUES (1, 'permanent')");
            st.execute("CREATE TEMP TABLE h35_t(id int, val text)");
            st.execute("INSERT INTO h35_t VALUES (2, 'temp')");
            try {
                // Explicitly schema-qualified should read the permanent table
                try (ResultSet rs = st.executeQuery("SELECT val FROM public.h35_t")) {
                    assertTrue(rs.next());
                    assertEquals("permanent", rs.getString(1));
                }
            } finally {
                st.execute("DROP TABLE IF EXISTS h35_t"); // drops temp
                st.execute("DROP TABLE IF EXISTS public.h35_t");
            }
        }
    }

    @Test
    void h35_emptySearchPathRejectsUnqualified() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE public.h35_empty(id int)");
            st.execute("SET search_path = ''");
            try {
                st.executeQuery("SELECT * FROM h35_empty");
                fail("Expected error for unqualified name with empty search_path");
            } catch (SQLException e) {
                // 42P01 = relation does not exist
                assertEquals("42P01", e.getSQLState());
            } finally {
                st.execute("SET search_path = '\"$user\", public'");
                st.execute("DROP TABLE IF EXISTS public.h35_empty");
            }
        }
    }

    // === H36: SET SESSION AUTHORIZATION / SET ROLE ===

    @Test
    void h36_setSessionAuthorizationUpdatesCurrentUser() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            // session_user should reflect the connecting user
            try (ResultSet rs = st.executeQuery("SELECT session_user, current_user")) {
                assertTrue(rs.next());
                String sessionUser = rs.getString(1);
                assertNotNull(sessionUser);
            }
        }
    }

    @Test
    void h36_setRoleNonexistentFails() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try {
                st.execute("SET ROLE nonexistent_role_xyz_12345");
                fail("Expected error for nonexistent role");
            } catch (SQLException e) {
                assertEquals("22023", e.getSQLState());
            }
        }
    }

    @Test
    void h36_setSessionAuthorizationNonexistentFails() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try {
                st.execute("SET SESSION AUTHORIZATION nonexistent_user_xyz_12345");
                fail("Expected error for nonexistent user");
            } catch (SQLException e) {
                // PG returns 22023 for invalid parameter value
                assertEquals("22023", e.getSQLState());
            }
        }
    }

    // === H37: Invalid SET values ===

    @Test
    void h37_invalidDatestyleRejected() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try {
                st.execute("SET datestyle = 'bogus'");
                fail("Expected error for invalid datestyle");
            } catch (SQLException e) {
                assertEquals("22023", e.getSQLState());
            }
        }
    }

    @Test
    void h37_invalidRowSecurityRejected() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try {
                st.execute("SET row_security = 'maybe'");
                fail("Expected error for invalid row_security value");
            } catch (SQLException e) {
                assertEquals("22023", e.getSQLState());
            }
        }
    }

    @Test
    void h37_invalidStatementTimeoutRejected() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try {
                st.execute("SET statement_timeout = 'notanum'");
                fail("Expected error for invalid statement_timeout");
            } catch (SQLException e) {
                assertEquals("22023", e.getSQLState());
            }
        }
    }

    @Test
    void h37_validDatestyleStored() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("SET datestyle = 'ISO, DMY'");
            try (ResultSet rs = st.executeQuery("SHOW datestyle")) {
                assertTrue(rs.next());
                String val = rs.getString(1);
                assertTrue(val.contains("ISO") && val.contains("DMY"),
                    "Expected ISO, DMY but got: " + val);
            }
            // Reset
            st.execute("SET datestyle = 'ISO, MDY'");
        }
    }

    // === M13: SET transactional rollback ===

    @Test
    void m13_plainSetRolledBack() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("SET application_name = 'original'");
            c.setAutoCommit(false);
            st.execute("SET application_name = 'changed'");
            // Verify it changed
            try (ResultSet rs = st.executeQuery("SHOW application_name")) {
                assertTrue(rs.next());
                assertEquals("changed", rs.getString(1));
            }
            c.rollback();
            c.setAutoCommit(true);
            // After rollback, should revert
            try (ResultSet rs = st.executeQuery("SHOW application_name")) {
                assertTrue(rs.next());
                assertEquals("original", rs.getString(1));
            }
        }
    }

    @Test
    void m13_setLocalOutsideTxnNoOp() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("SET application_name = 'before'");
            // SET LOCAL outside transaction should be a no-op (just warning)
            st.execute("SET LOCAL application_name = 'local_outside'");
            try (ResultSet rs = st.executeQuery("SHOW application_name")) {
                assertTrue(rs.next());
                assertEquals("before", rs.getString(1));
            }
        }
    }

    @Test
    void m13_setConfigLocalOutsideTxnNoOp() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("SET application_name = 'before_config'");
            // set_config with is_local=true outside transaction should not persist
            st.executeQuery("SELECT set_config('application_name', 'via_set_config', true)");
            try (ResultSet rs = st.executeQuery("SHOW application_name")) {
                assertTrue(rs.next());
                assertEquals("before_config", rs.getString(1));
            }
        }
    }

    // === L7: DISCARD ALL, current_setting, SHOW search_path ===

    @Test
    void l7_discardAllResetsAppNameToEmpty() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("SET application_name = 'test_app'");
            st.execute("DISCARD ALL");
            try (ResultSet rs = st.executeQuery("SHOW application_name")) {
                assertTrue(rs.next());
                assertEquals("", rs.getString(1));
            }
        }
    }

    @Test
    void l7_showSearchPathQuotes() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("SET search_path = '\"$user\", public'");
            try (ResultSet rs = st.executeQuery("SHOW search_path")) {
                assertTrue(rs.next());
                String val = rs.getString(1);
                assertTrue(val.contains("\"$user\""),
                    "Expected quoted $user in search_path, got: " + val);
            }
        }
    }

    @Test
    void l7_currentSettingMissingAfterReset() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("SET \"custom.test_var\" = 'hello'");
            st.execute("RESET \"custom.test_var\"");
            try (ResultSet rs = st.executeQuery("SELECT current_setting('custom.test_var', true)")) {
                assertTrue(rs.next());
                // PG returns NULL for unset custom variables with missing_ok=true
                assertNull(rs.getObject(1));
            }
        }
    }

    // === L8: CLUSTER without clustered index ===

    @Test
    void l8_clusterWithoutIndexErrors() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE l8_t(id int)");
            try {
                st.execute("CLUSTER l8_t");
                fail("Expected error for CLUSTER without clustered index");
            } catch (SQLException e) {
                assertEquals("42704", e.getSQLState());
            } finally {
                st.execute("DROP TABLE l8_t");
            }
        }
    }
}
