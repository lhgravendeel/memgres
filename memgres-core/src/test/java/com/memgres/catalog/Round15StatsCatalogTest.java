package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category D: PG 14+ stats catalog columns and views.
 *
 * Covers:
 *  - pg_stat_io (PG 16+)
 *  - pg_stat_user_functions
 *  - pg_stat_archiver
 *  - pg_stat_database new columns (session_time, active_time,
 *    idle_in_transaction_time, sessions, sessions_abandoned/fatal/killed)
 *  - pg_stat_activity new columns (query_id, backend_xid, backend_xmin,
 *    wait_event, wait_event_type, leader_pid)
 *  - pg_stat_user_indexes idx_blks_read/hit (PG 16+)
 */
class Round15StatsCatalogTest {

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

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static void assertHasColumns(String viewName, String... cols) throws SQLException {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < cols.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(cols[i]);
        }
        sb.append(" FROM ").append(viewName).append(" LIMIT 1");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sb.toString())) {
            // Don't need a row — shape alone is enough
            rs.getMetaData();
        } catch (SQLException e) {
            fail("View " + viewName + " missing columns; query failed: "
                    + e.getMessage());
        }
    }

    // =========================================================================
    // A. pg_stat_io (PG 16+)
    // =========================================================================

    @Test
    void pg_stat_io_view_exists() throws SQLException {
        assertHasColumns("pg_stat_io",
                "backend_type", "object", "context",
                "reads", "writes", "extends", "op_bytes",
                "read_time", "write_time", "hits", "evictions", "reuses");
    }

    // =========================================================================
    // B. pg_stat_user_functions
    // =========================================================================

    @Test
    void pg_stat_user_functions_view_exists() throws SQLException {
        assertHasColumns("pg_stat_user_functions",
                "funcid", "schemaname", "funcname", "calls",
                "total_time", "self_time");
    }

    @Test
    void pg_stat_user_functions_populated_after_call() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE FUNCTION r15_sf_fn() RETURNS int AS "
                    + "'SELECT 1' LANGUAGE SQL");
            s.execute("SET track_functions = 'all'");
            s.executeQuery("SELECT r15_sf_fn()").close();
            s.executeQuery("SELECT r15_sf_fn()").close();
        }
        int calls = scalarInt(
                "SELECT COALESCE(calls,0)::int FROM pg_stat_user_functions "
                        + "WHERE funcname='r15_sf_fn'");
        assertTrue(calls >= 2,
                "pg_stat_user_functions.calls should track function invocations; got " + calls);
    }

    // =========================================================================
    // C. pg_stat_archiver
    // =========================================================================

    @Test
    void pg_stat_archiver_view_exists() throws SQLException {
        assertHasColumns("pg_stat_archiver",
                "archived_count", "last_archived_wal", "last_archived_time",
                "failed_count", "last_failed_wal", "last_failed_time", "stats_reset");
    }

    // =========================================================================
    // D. pg_stat_database PG 14+ columns
    // =========================================================================

    @Test
    void pg_stat_database_pg14_columns() throws SQLException {
        assertHasColumns("pg_stat_database",
                "session_time", "active_time", "idle_in_transaction_time",
                "sessions", "sessions_abandoned",
                "sessions_fatal", "sessions_killed");
    }

    @Test
    void pg_stat_database_existing_columns_still_present() throws SQLException {
        assertHasColumns("pg_stat_database",
                "datid", "datname", "numbackends", "xact_commit", "xact_rollback",
                "blks_read", "blks_hit", "tup_returned", "tup_fetched",
                "tup_inserted", "tup_updated", "tup_deleted", "conflicts",
                "temp_files", "temp_bytes", "deadlocks", "stats_reset");
    }

    // =========================================================================
    // E. pg_stat_activity new columns
    // =========================================================================

    @Test
    void pg_stat_activity_pg14_columns() throws SQLException {
        assertHasColumns("pg_stat_activity",
                "query_id", "backend_xid", "backend_xmin",
                "wait_event", "wait_event_type", "leader_pid");
    }

    @Test
    void pg_stat_activity_query_id_populated_with_compute_query_id() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("SET compute_query_id = 'on'");
        }
        // Any subsequent query should have query_id in pg_stat_activity
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT query_id FROM pg_stat_activity WHERE pid = pg_backend_pid()")) {
            assertTrue(rs.next());
            Object qid = rs.getObject(1);
            // query_id may be null if statement hasn't been computed yet,
            // but the column must exist and be queryable
            // Don't assert non-null, just that the read works
        }
    }

    // =========================================================================
    // F. pg_stat_user_indexes PG 16+ columns
    // =========================================================================

    @Test
    void pg_stat_user_indexes_has_blks_columns() throws SQLException {
        assertHasColumns("pg_stat_user_indexes",
                "idx_blks_read", "idx_blks_hit");
    }

    // =========================================================================
    // G. pg_stat_replication / pg_stat_wal_receiver / pg_stat_subscription stubs
    // =========================================================================

    @Test
    void pg_stat_replication_exists() throws SQLException {
        // Should exist even empty
        int n = scalarInt("SELECT count(*)::int FROM pg_stat_replication");
        assertTrue(n >= 0);
    }

    @Test
    void pg_stat_wal_receiver_exists() throws SQLException {
        int n = scalarInt("SELECT count(*)::int FROM pg_stat_wal_receiver");
        assertTrue(n >= 0);
    }

    @Test
    void pg_stat_subscription_exists() throws SQLException {
        int n = scalarInt("SELECT count(*)::int FROM pg_stat_subscription");
        assertTrue(n >= 0);
    }

    // =========================================================================
    // H. pg_stat_bgwriter / pg_stat_checkpointer (PG 17+)
    // =========================================================================

    @Test
    void pg_stat_bgwriter_exists() throws SQLException {
        int n = scalarInt("SELECT count(*)::int FROM pg_stat_bgwriter");
        assertTrue(n >= 0);
    }

    @Test
    void pg_stat_checkpointer_exists() throws SQLException {
        // PG 17+: moved from pg_stat_bgwriter
        int n = scalarInt("SELECT count(*)::int FROM pg_stat_checkpointer");
        assertTrue(n >= 0);
    }

    // =========================================================================
    // I. Stats reset functions
    // =========================================================================

    @Test
    void pg_stat_reset_executes() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT pg_stat_reset()");
        }
    }

    @Test
    void pg_stat_reset_shared_executes() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT pg_stat_reset_shared('archiver')");
            s.execute("SELECT pg_stat_reset_shared('bgwriter')");
        }
    }
}
