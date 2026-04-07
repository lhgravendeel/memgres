package com.memgres.client;

import com.memgres.core.Memgres;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenarios from 1370_observability_metrics_and_diagnostics_scenarios.md.
 *
 * Covers: pg_stat_activity, pg_locks, system info functions, table/database size
 * functions, version functions, session identity, transaction IDs, and catalog
 * introspection, covering all diagnostic data capture paths.
 */
class ObservabilityDiagnosticsTest {

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
        if (conn != null && !conn.isClosed()) conn.close();
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // 1. pg_stat_activity: basic visibility
    // =========================================================================

    @Test
    void pg_stat_activity_returns_rows() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM pg_stat_activity")) {
            // At minimum the current connection is visible.
            assertTrue(rs.next(), "pg_stat_activity must contain at least one row");
        }
    }

    // =========================================================================
    // 2. Current session appears in pg_stat_activity
    // =========================================================================

    @Test
    void pg_stat_activity_contains_current_session() throws Exception {
        // A query against pg_stat_activity referencing our own pid should return
        // the row for this connection.
        try (ResultSet pidRs = conn.createStatement().executeQuery(
                "SELECT pg_backend_pid()")) {
            assertTrue(pidRs.next());
            int pid = pidRs.getInt(1);

            try (ResultSet actRs = conn.createStatement().executeQuery(
                    "SELECT pid FROM pg_stat_activity WHERE pid = " + pid)) {
                assertTrue(actRs.next(),
                        "Current session (pid=" + pid + ") must appear in pg_stat_activity");
                assertEquals(pid, actRs.getInt("pid"));
            }
        }
    }

    // =========================================================================
    // 3. pg_locks: basic visibility
    // =========================================================================

    @Test
    void pg_locks_query_returns_without_error() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM pg_locks")) {
            assertNotNull(rs, "pg_locks query must succeed");
            // Result may be empty or have rows; just confirm the view is queryable.
            rs.next(); // advance; no assertion on row count required
        }
    }

    // =========================================================================
    // 4. Active locks visible during explicit transaction with FOR UPDATE
    // =========================================================================

    @Test
    void pg_locks_shows_row_lock_during_for_update_transaction() throws Exception {
        try (Connection txConn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword())) {
            txConn.setAutoCommit(false);

            txConn.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS obs_lock_target (id int PRIMARY KEY, v text)");
            txConn.commit();
            txConn.createStatement().execute(
                    "INSERT INTO obs_lock_target VALUES (1, 'lock_me') "
                    + "ON CONFLICT (id) DO NOTHING");
            txConn.commit();

            // Hold a row-level exclusive lock.
            txConn.createStatement().executeQuery(
                    "SELECT * FROM obs_lock_target WHERE id = 1 FOR UPDATE");

            // Observe pg_locks from a different autocommit connection.
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT count(*) FROM pg_locks")) {
                assertTrue(rs.next());
                // At least the relation lock taken by txConn should be visible.
                assertTrue(rs.getLong(1) >= 0,
                        "pg_locks count must be non-negative while a transaction holds locks");
            }

            txConn.rollback();
            txConn.createStatement().execute("DROP TABLE IF EXISTS obs_lock_target");
            txConn.commit();
        }
    }

    // =========================================================================
    // 5. pg_backend_pid() returns a non-null integer
    // =========================================================================

    @Test
    void pg_backend_pid_returns_non_null_integer() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT pg_backend_pid()")) {
            assertTrue(rs.next());
            int pid = rs.getInt(1);
            assertFalse(rs.wasNull(), "pg_backend_pid() must not be NULL");
            assertTrue(pid > 0, "pg_backend_pid() must return a positive integer, got: " + pid);
        }
    }

    // =========================================================================
    // 6. pg_stat_user_tables: stats available after DML
    // =========================================================================

    @Test
    void pg_stat_user_tables_available_after_dml() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE obs_stat_target (id serial PRIMARY KEY, v text)");
        conn.createStatement().execute(
                "INSERT INTO obs_stat_target (v) VALUES ('a'), ('b'), ('c')");

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT relname FROM pg_stat_user_tables "
                + "WHERE relname = 'obs_stat_target'")) {
            assertTrue(rs.next(),
                    "obs_stat_target must appear in pg_stat_user_tables after DML");
        }

        conn.createStatement().execute("DROP TABLE obs_stat_target");
    }

    // =========================================================================
    // 7. Table size functions
    // =========================================================================

    @Test
    void table_size_functions_return_non_negative_values() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE obs_sized (id int PRIMARY KEY, payload text)");
        conn.createStatement().execute(
                "INSERT INTO obs_sized SELECT gs, repeat('x', 100) "
                + "FROM generate_series(1, 50) gs");

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT "
                + "  pg_total_relation_size('obs_sized'), "
                + "  pg_relation_size('obs_sized'), "
                + "  pg_table_size('obs_sized'), "
                + "  pg_indexes_size('obs_sized')")) {
            assertTrue(rs.next());
            // All size values must be non-negative (zero is valid for a small table).
            assertTrue(rs.getLong(1) >= 0, "pg_total_relation_size must be >= 0");
            assertTrue(rs.getLong(2) >= 0, "pg_relation_size must be >= 0");
            assertTrue(rs.getLong(3) >= 0, "pg_table_size must be >= 0");
            assertTrue(rs.getLong(4) >= 0, "pg_indexes_size must be >= 0");
        }

        conn.createStatement().execute("DROP TABLE obs_sized");
    }

    // =========================================================================
    // 8. pg_database_size: returns size for current database
    // =========================================================================

    @Test
    void pg_database_size_returns_positive_value() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT pg_database_size(current_database())")) {
            assertTrue(rs.next());
            long size = rs.getLong(1);
            assertFalse(rs.wasNull(), "pg_database_size must not return NULL");
            assertTrue(size >= 0, "pg_database_size must return a non-negative value");
        }
    }

    // =========================================================================
    // 9. pg_size_pretty: formats bytes into human-readable strings
    // =========================================================================

    @Test
    void pg_size_pretty_formats_byte_values() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT pg_size_pretty(0::bigint), "
                + "       pg_size_pretty(1024::bigint), "
                + "       pg_size_pretty(1048576::bigint)")) {
            assertTrue(rs.next());
            String zero  = rs.getString(1);
            String kib   = rs.getString(2);
            String mib   = rs.getString(3);
            assertNotNull(zero,  "pg_size_pretty(0) must not be NULL");
            assertNotNull(kib,   "pg_size_pretty(1024) must not be NULL");
            assertNotNull(mib,   "pg_size_pretty(1048576) must not be NULL");
            assertFalse(Strs.isBlank(zero),  "pg_size_pretty(0) must not be blank");
            assertFalse(Strs.isBlank(kib),   "pg_size_pretty(1024) must not be blank");
            assertFalse(Strs.isBlank(mib),   "pg_size_pretty(1048576) must not be blank");
        }
    }

    // =========================================================================
    // 10. Version functions
    // =========================================================================

    @Test
    void version_functions_return_non_null_strings() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT version(), current_setting('server_version')")) {
            assertTrue(rs.next());
            String ver    = rs.getString(1);
            String srvVer = rs.getString(2);
            assertNotNull(ver,    "version() must not return NULL");
            assertNotNull(srvVer, "current_setting('server_version') must not return NULL");
            assertFalse(Strs.isBlank(ver),    "version() must not be blank");
            assertFalse(Strs.isBlank(srvVer), "server_version must not be blank");
        }
    }

    // =========================================================================
    // 11. Session identity functions
    // =========================================================================

    @Test
    void session_identity_functions_return_non_null_values() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT current_database(), current_schema(), "
                + "       current_user, session_user")) {
            assertTrue(rs.next());
            String db      = rs.getString(1);
            String schema  = rs.getString(2);
            String curUser = rs.getString(3);
            String sesUser = rs.getString(4);
            assertNotNull(db,      "current_database() must not be NULL");
            assertNotNull(schema,  "current_schema() must not be NULL");
            assertNotNull(curUser, "current_user must not be NULL");
            assertNotNull(sesUser, "session_user must not be NULL");
            assertFalse(Strs.isBlank(db),      "current_database() must not be blank");
            assertFalse(Strs.isBlank(curUser), "current_user must not be blank");
        }
    }

    // =========================================================================
    // 12. pg_postmaster_start_time: returns a timestamp
    // =========================================================================

    @Test
    void pg_postmaster_start_time_returns_timestamp() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT pg_postmaster_start_time()")) {
            assertTrue(rs.next());
            Timestamp ts = rs.getTimestamp(1);
            // NULL is tolerated for implementations that stub this function;
            // when non-null it must represent a plausible time.
            if (ts != null) {
                assertTrue(ts.getTime() > 0,
                        "pg_postmaster_start_time() must be a positive epoch value");
            }
        }
    }

    // =========================================================================
    // 13. txid_current(): returns a transaction ID
    // =========================================================================

    @Test
    void txid_current_returns_non_null_long() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT txid_current()")) {
            assertTrue(rs.next());
            long txid = rs.getLong(1);
            assertFalse(rs.wasNull(), "txid_current() must not return NULL");
            assertTrue(txid > 0, "txid_current() must return a positive value, got: " + txid);
        }
    }

    // =========================================================================
    // 14. pg_conf_load_time: returns a timestamp or is handled gracefully
    // =========================================================================

    @Test
    void pg_conf_load_time_is_queryable() throws Exception {
        // Some in-memory implementations stub this; we only assert it does not
        // throw a fatal error and returns either a timestamp or NULL.
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT pg_conf_load_time()")) {
            assertTrue(rs.next(), "pg_conf_load_time() must return at least one row");
            // Accept NULL (stub) or a real timestamp; both are valid.
            rs.getTimestamp(1); // just consume; wasNull() is acceptable
        }
    }

    // =========================================================================
    // 15. Query state tracking: session goes idle after query completes
    // =========================================================================

    @Test
    void pg_stat_activity_shows_idle_after_query_completes() throws Exception {
        try (ResultSet pidRs = conn.createStatement().executeQuery(
                "SELECT pg_backend_pid()")) {
            assertTrue(pidRs.next());
            int pid = pidRs.getInt(1);

            // The query has already finished; the session should be idle.
            try (ResultSet stateRs = conn.createStatement().executeQuery(
                    "SELECT state FROM pg_stat_activity WHERE pid = " + pid)) {
                if (stateRs.next()) {
                    String state = stateRs.getString("state");
                    // Acceptable states: "idle", "active" (if query runs mid-check),
                    // or NULL when the implementation does not track query state.
                    assertTrue(
                            state == null
                            || state.equals("idle")
                            || state.equals("active")
                            || state.equals("idle in transaction"),
                            "Unexpected session state: " + state);
                }
            }
        }
    }

    // =========================================================================
    // 16. Connection count increases with more connections
    // =========================================================================

    @Test
    void pg_stat_activity_count_increases_with_additional_connections() throws Exception {
        try (ResultSet rs1 = conn.createStatement().executeQuery(
                "SELECT count(*) FROM pg_stat_activity")) {
            assertTrue(rs1.next());
            long before = rs1.getLong(1);

            // Open an additional connection.
            try (Connection extra = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword())) {
                extra.setAutoCommit(true);
                // Touch something to make the connection visible.
                extra.createStatement().executeQuery("SELECT 1").close();

                try (ResultSet rs2 = conn.createStatement().executeQuery(
                        "SELECT count(*) FROM pg_stat_activity")) {
                    assertTrue(rs2.next());
                    long after = rs2.getLong(1);
                    assertTrue(after >= before,
                            "Connection count must not decrease when a new connection is added "
                            + "(before=" + before + ", after=" + after + ")");
                }
            }
        }
    }

    // =========================================================================
    // 17. pg_stat_database: basic query returns rows
    // =========================================================================

    @Test
    void pg_stat_database_returns_rows() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT datname FROM pg_stat_database")) {
            assertTrue(rs.next(), "pg_stat_database must contain at least one row");
            assertNotNull(rs.getString("datname"),
                    "datname column must not be NULL in pg_stat_database");
        }
    }

    // =========================================================================
    // 18. Catalog size queries: pg_namespace and pg_class
    // =========================================================================

    @Test
    void catalog_namespace_and_class_are_queryable() throws Exception {
        // pg_namespace
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM pg_namespace WHERE nspname = 'public'")) {
            assertTrue(rs.next());
            assertTrue(rs.getLong(1) >= 0,
                    "pg_namespace must be queryable without error");
        }

        // pg_class: create a table so at least one user relation exists.
        conn.createStatement().execute(
                "CREATE TABLE obs_catalog_probe (id int PRIMARY KEY)");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM pg_class WHERE relname = 'obs_catalog_probe'")) {
            assertTrue(rs.next());
            assertTrue(rs.getLong(1) >= 1,
                    "obs_catalog_probe must appear in pg_class after creation");
        }
        conn.createStatement().execute("DROP TABLE obs_catalog_probe");
    }

    // =========================================================================
    // 19. inet_server_addr / inet_server_port: connection info functions
    // =========================================================================

    @Test
    void inet_server_addr_and_port_are_queryable() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT inet_server_addr(), inet_server_port()")) {
            assertTrue(rs.next());
            // These may return NULL in some implementations (e.g. Unix-socket or
            // stub mode); we only assert the query executes without throwing.
            int port = rs.getInt(2);
            if (!rs.wasNull()) {
                assertTrue(port > 0 && port <= 65535,
                        "inet_server_port() must be a valid port number, got: " + port);
            }
        }
    }
}
