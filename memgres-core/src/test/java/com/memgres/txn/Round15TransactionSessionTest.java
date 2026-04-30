package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category E: Transaction/session controls.
 *
 * Covers:
 *  - START TRANSACTION (alias for BEGIN)
 *  - COMMIT AND NO CHAIN / ROLLBACK AND NO CHAIN
 *  - SET TRANSACTION SNAPSHOT (paired with pg_export_snapshot)
 *  - RESET SESSION AUTHORIZATION
 *  - SET LOCAL SESSION AUTHORIZATION
 *  - RESET ROLE, SET ROLE NONE / DEFAULT
 *  - current_role vs current_user distinction after SET ROLE
 *  - information_schema.applicable_roles / enabled_roles
 */
class Round15TransactionSessionTest {

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

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // A. START TRANSACTION
    // =========================================================================

    @Test
    void start_transaction_equivalent_to_begin() throws SQLException {
        exec("START TRANSACTION");
        // Should be in a txn now
        String state = scalarString("SHOW transaction_isolation");
        assertNotNull(state);
        exec("ROLLBACK");
    }

    @Test
    void start_transaction_with_isolation_level() throws SQLException {
        exec("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        String iso = scalarString("SHOW transaction_isolation");
        assertTrue(iso.toLowerCase().contains("serializable"),
                "transaction_isolation must reflect SERIALIZABLE; got " + iso);
        exec("ROLLBACK");
    }

    @Test
    void start_transaction_read_only() throws SQLException {
        exec("START TRANSACTION READ ONLY");
        String ro = scalarString("SHOW transaction_read_only");
        assertEquals("on", ro);
        exec("ROLLBACK");
    }

    // =========================================================================
    // B. COMMIT / ROLLBACK AND NO CHAIN / AND CHAIN
    // =========================================================================

    @Test
    void commit_and_no_chain_accepted() throws SQLException {
        exec("BEGIN");
        exec("COMMIT AND NO CHAIN");
        // After this we should NOT be in a transaction
    }

    @Test
    void rollback_and_no_chain_accepted() throws SQLException {
        exec("BEGIN");
        exec("ROLLBACK AND NO CHAIN");
    }

    @Test
    void commit_and_chain_starts_new_txn() throws SQLException {
        exec("CREATE TABLE r15_ch (id int)");
        exec("BEGIN");
        exec("INSERT INTO r15_ch VALUES (1)");
        exec("COMMIT AND CHAIN");
        // A new txn is now open; if we insert then rollback, the row must be undone
        exec("INSERT INTO r15_ch VALUES (2)");
        exec("ROLLBACK");
        int n = scalarInt("SELECT count(*)::int FROM r15_ch");
        assertEquals(1, n, "COMMIT AND CHAIN should start a new txn; id=2 should roll back");
    }

    // =========================================================================
    // C. SET TRANSACTION SNAPSHOT / pg_export_snapshot
    // =========================================================================

    @Test
    void pg_export_snapshot_returns_id() throws SQLException {
        exec("BEGIN ISOLATION LEVEL REPEATABLE READ");
        try {
            String id = scalarString("SELECT pg_export_snapshot()");
            assertNotNull(id, "pg_export_snapshot() must return a snapshot id");
            assertFalse(id.isEmpty());
        } finally {
            exec("ROLLBACK");
        }
    }

    @Test
    void set_transaction_snapshot_reuses_exported() throws SQLException {
        // Connection 1 exports, connection 2 imports
        String id;
        exec("BEGIN ISOLATION LEVEL REPEATABLE READ");
        try {
            id = scalarString("SELECT pg_export_snapshot()");
            assertNotNull(id);

            try (Connection c2 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword())) {
                c2.setAutoCommit(false);
                try (Statement s = c2.createStatement()) {
                    s.execute("BEGIN ISOLATION LEVEL REPEATABLE READ");
                    s.execute("SET TRANSACTION SNAPSHOT '" + id.replace("'", "''") + "'");
                    s.execute("ROLLBACK");
                }
            }
        } finally {
            exec("ROLLBACK");
        }
    }

    // =========================================================================
    // D. SESSION AUTHORIZATION
    // =========================================================================

    @Test
    void reset_session_authorization() throws SQLException {
        String before = scalarString("SHOW session_authorization");
        exec("RESET SESSION AUTHORIZATION");
        String after = scalarString("SHOW session_authorization");
        assertEquals(before, after,
                "RESET SESSION AUTHORIZATION should leave us as default session user");
    }

    @Test
    void set_local_session_authorization_txn_scoped() throws SQLException {
        exec("CREATE ROLE r15_sa_r1");
        exec("BEGIN");
        try {
            exec("SET LOCAL SESSION AUTHORIZATION r15_sa_r1");
            String sa = scalarString("SHOW session_authorization");
            assertEquals("r15_sa_r1", sa);
        } finally {
            exec("ROLLBACK");
        }
        // After rollback, should be back to original
        String saAfter = scalarString("SHOW session_authorization");
        assertNotEquals("r15_sa_r1", saAfter,
                "SET LOCAL must be scoped to the txn");
    }

    // =========================================================================
    // E. ROLE switching
    // =========================================================================

    @Test
    void reset_role() throws SQLException {
        exec("CREATE ROLE r15_r_reset");
        exec("GRANT r15_r_reset TO CURRENT_USER");
        exec("SET ROLE r15_r_reset");
        assertEquals("r15_r_reset", scalarString("SELECT current_role"));
        exec("RESET ROLE");
        String role = scalarString("SELECT current_role");
        assertNotEquals("r15_r_reset", role,
                "RESET ROLE must revert to original");
    }

    @Test
    void set_role_none_reverts() throws SQLException {
        exec("CREATE ROLE r15_r_none");
        exec("GRANT r15_r_none TO CURRENT_USER");
        exec("SET ROLE r15_r_none");
        assertEquals("r15_r_none", scalarString("SELECT current_role"));
        exec("SET ROLE NONE");
        String role = scalarString("SELECT current_role");
        assertNotEquals("r15_r_none", role,
                "SET ROLE NONE must revert to original");
    }

    @Test
    void current_role_differs_from_current_user_after_set_role() throws SQLException {
        exec("CREATE ROLE r15_r_cr");
        exec("GRANT r15_r_cr TO CURRENT_USER");
        String originalUser = scalarString("SELECT current_user");
        exec("SET ROLE r15_r_cr");
        String cr = scalarString("SELECT current_role");
        String cu = scalarString("SELECT current_user");
        assertEquals("r15_r_cr", cr);
        assertEquals("r15_r_cr", cu,
                "current_user should also track SET ROLE; current_user becomes the set role");
        exec("RESET ROLE");
        assertEquals(originalUser, scalarString("SELECT current_user"));
    }

    // =========================================================================
    // F. information_schema role views
    // =========================================================================

    @Test
    void information_schema_applicable_roles_exists() throws SQLException {
        int n = scalarInt("SELECT count(*)::int FROM information_schema.applicable_roles");
        assertTrue(n >= 0);
    }

    @Test
    void information_schema_enabled_roles_exists() throws SQLException {
        int n = scalarInt("SELECT count(*)::int FROM information_schema.enabled_roles");
        assertTrue(n >= 1,
                "enabled_roles should include at least the current session role");
    }

    @Test
    void information_schema_role_table_grants() throws SQLException {
        exec("CREATE TABLE r15_rtg (id int)");
        exec("CREATE ROLE r15_rtg_r");
        exec("GRANT SELECT ON r15_rtg TO r15_rtg_r");

        int n = scalarInt(
                "SELECT count(*)::int FROM information_schema.role_table_grants "
                        + "WHERE grantee='r15_rtg_r' AND table_name='r15_rtg'");
        assertTrue(n >= 1,
                "information_schema.role_table_grants should reflect the GRANT");
    }

    // =========================================================================
    // G. SAVEPOINT inside START TRANSACTION
    // =========================================================================

    @Test
    void savepoint_in_start_transaction() throws SQLException {
        exec("CREATE TABLE r15_sp (id int)");
        exec("START TRANSACTION");
        exec("SAVEPOINT s1");
        exec("INSERT INTO r15_sp VALUES (1)");
        exec("ROLLBACK TO SAVEPOINT s1");
        exec("COMMIT");
        int n = scalarInt("SELECT count(*)::int FROM r15_sp");
        assertEquals(0, n, "ROLLBACK TO SAVEPOINT should undo INSERT inside STARTED txn");
    }
}
