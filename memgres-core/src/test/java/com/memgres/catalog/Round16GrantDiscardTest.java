package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category N: GRANT / REVOKE / DISCARD / SET.
 *
 * Covers:
 *  - GRANT ... GRANTED BY role parsing
 *  - GRANT ... ON LARGE OBJECT loid
 *  - GRANT USAGE ON FOREIGN DATA WRAPPER / FOREIGN SERVER
 *  - GRANT SET ON PARAMETER
 *  - ALTER DEFAULT PRIVILEGES FOR ROLE target persisted
 *  - REVOKE ... CASCADE cascades to dependent grants
 *  - REASSIGN OWNED BY / DROP OWNED BY
 *  - SET LOCAL outside transaction issues a warning
 *  - DISCARD SEQUENCES resets sequence caches
 */
class Round16GrantDiscardTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // =========================================================================
    // N1. GRANT ... GRANTED BY role
    // =========================================================================

    @Test
    void grant_granted_by_role_parses() throws SQLException {
        exec("DROP ROLE IF EXISTS r16_grantor");
        exec("DROP ROLE IF EXISTS r16_grantee");
        exec("CREATE ROLE r16_grantor");
        exec("CREATE ROLE r16_grantee");
        exec("DROP TABLE IF EXISTS r16_gt");
        exec("CREATE TABLE r16_gt (id int)");
        // GRANTED BY with a role other than the current user must be rejected (0A000)
        try {
            exec("GRANT SELECT ON r16_gt TO r16_grantee GRANTED BY r16_grantor");
            fail("Expected SQLSTATE 0A000: grantor must be current user");
        } catch (java.sql.SQLException e) {
            assertEquals("0A000", e.getSQLState(), "Wrong SQLSTATE: " + e.getMessage());
        }
    }

    // =========================================================================
    // N2. GRANT ... ON LARGE OBJECT loid
    // =========================================================================

    @Test
    void grant_on_large_object_parses() throws SQLException {
        exec("DROP ROLE IF EXISTS r16_lo_user");
        exec("CREATE ROLE r16_lo_user");
        // The loid does not have to resolve to an existing LO for the grant
        // to parse — but if Memgres validates existence, we create one.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT lo_create(12345)")) {
            assertTrue(rs.next());
        }
        exec("GRANT SELECT ON LARGE OBJECT 12345 TO r16_lo_user");
    }

    // =========================================================================
    // N3. GRANT USAGE ON FOREIGN DATA WRAPPER / FOREIGN SERVER
    // =========================================================================

    @Test
    void grant_usage_on_foreign_data_wrapper_parses() throws SQLException {
        exec("DROP ROLE IF EXISTS r16_fdw_u");
        exec("CREATE ROLE r16_fdw_u");
        // Must parse. In PG there's always a 'postgres_fdw'-like FDW in most setups;
        // use a generic one-word identifier that PG would create via extension.
        try {
            exec("CREATE FOREIGN DATA WRAPPER r16_fdw");
        } catch (SQLException e) {
            // If not supported, skip creation — still test GRANT parsing on a name
            // that may not exist (Memgres should then error with FDW-not-found, not parser error)
        }
        try {
            exec("GRANT USAGE ON FOREIGN DATA WRAPPER r16_fdw TO r16_fdw_u");
        } catch (SQLException e) {
            // Acceptable only if the error is about the FDW not existing, NOT a parse error.
            String sql = e.getSQLState();
            assertNotEquals("42601", sql,
                    "GRANT USAGE ON FOREIGN DATA WRAPPER must parse (not raise syntax error 42601); got " + sql);
        }
    }

    @Test
    void grant_usage_on_foreign_server_parses() throws SQLException {
        exec("DROP ROLE IF EXISTS r16_fs_u");
        exec("CREATE ROLE r16_fs_u");
        try {
            exec("GRANT USAGE ON FOREIGN SERVER nonexistent TO r16_fs_u");
        } catch (SQLException e) {
            assertNotEquals("42601", e.getSQLState(),
                    "GRANT ... ON FOREIGN SERVER must parse (not raise 42601); got " + e.getSQLState());
        }
    }

    // =========================================================================
    // N4. GRANT SET ON PARAMETER
    // =========================================================================

    @Test
    void grant_set_on_parameter_parses() throws SQLException {
        exec("DROP ROLE IF EXISTS r16_set_u");
        exec("CREATE ROLE r16_set_u");
        exec("GRANT SET ON PARAMETER work_mem TO r16_set_u");
        // Round-trip via has_parameter_privilege (also tested in M6)
    }

    // =========================================================================
    // N5. ALTER DEFAULT PRIVILEGES FOR ROLE target persisted
    // =========================================================================

    @Test
    void alter_default_privileges_for_role_persists_target() throws SQLException {
        exec("DROP ROLE IF EXISTS r16_adp_owner");
        exec("DROP ROLE IF EXISTS r16_adp_grantee");
        exec("CREATE ROLE r16_adp_owner");
        exec("CREATE ROLE r16_adp_grantee");
        exec("ALTER DEFAULT PRIVILEGES FOR ROLE r16_adp_owner " +
                "GRANT SELECT ON TABLES TO r16_adp_grantee");
        // pg_default_acl.defaclrole must hold the oid of r16_adp_owner
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT r.rolname FROM pg_default_acl da " +
                             "JOIN pg_roles r ON da.defaclrole = r.oid " +
                             "WHERE r.rolname='r16_adp_owner'")) {
            assertTrue(rs.next(),
                    "ALTER DEFAULT PRIVILEGES FOR ROLE r must store r's oid in pg_default_acl.defaclrole");
        }
    }

    // =========================================================================
    // N6. REVOKE ... CASCADE
    // =========================================================================

    @Test
    void revoke_cascade_propagates_to_downstream_grants() throws SQLException {
        exec("DROP ROLE IF EXISTS r16_rc_a");
        exec("DROP ROLE IF EXISTS r16_rc_b");
        exec("DROP TABLE IF EXISTS r16_rc_t");
        exec("CREATE ROLE r16_rc_a");
        exec("CREATE ROLE r16_rc_b");
        exec("CREATE TABLE r16_rc_t (id int)");
        exec("GRANT SELECT ON r16_rc_t TO r16_rc_a WITH GRANT OPTION");
        // r16_rc_a grants to r16_rc_b — simulate via SET ROLE
        try {
            exec("SET ROLE r16_rc_a");
            exec("GRANT SELECT ON r16_rc_t TO r16_rc_b");
        } finally {
            exec("RESET ROLE");
        }
        // Cascade from owner → r16_rc_b must lose its grant
        exec("REVOKE SELECT ON r16_rc_t FROM r16_rc_a CASCADE");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT has_table_privilege('r16_rc_b', 'r16_rc_t', 'SELECT')")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1),
                    "REVOKE ... CASCADE must drop the downstream grant to r16_rc_b");
        }
    }

    // =========================================================================
    // N7. DROP OWNED BY cascade
    // =========================================================================

    @Test
    void drop_owned_by_drops_owned_objects() throws SQLException {
        exec("DROP ROLE IF EXISTS r16_do_u");
        exec("CREATE ROLE r16_do_u");
        exec("GRANT CREATE ON SCHEMA public TO r16_do_u");
        try {
            exec("SET ROLE r16_do_u");
            exec("CREATE TABLE r16_do_t (id int)");
        } finally {
            exec("RESET ROLE");
        }
        exec("DROP OWNED BY r16_do_u CASCADE");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_class WHERE relname='r16_do_t'")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1),
                    "DROP OWNED BY must remove r16_do_u's table r16_do_t");
        }
        exec("REVOKE CREATE ON SCHEMA public FROM r16_do_u");
        exec("DROP ROLE r16_do_u");
    }

    // =========================================================================
    // N8. SET LOCAL outside transaction raises WARNING
    // =========================================================================

    @Test
    void set_local_outside_transaction_emits_warning() throws SQLException {
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("SET LOCAL work_mem = '8MB'");
            SQLWarning w = s.getWarnings();
            assertNotNull(w,
                    "SET LOCAL outside a transaction must raise a WARNING");
        }
    }

    // =========================================================================
    // N9. DISCARD SEQUENCES resets sequence caches
    // =========================================================================

    @Test
    void discard_sequences_parses_and_does_not_error() throws SQLException {
        exec("DISCARD SEQUENCES");
        // No error means it's at least handled. Deeper semantics (cache reset) tested at fix time.
    }
}
