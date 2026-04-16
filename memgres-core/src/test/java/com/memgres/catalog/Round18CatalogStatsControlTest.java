package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap categories U + V: Stats / file / control catalogs + replication.
 *
 * Covers:
 *  - pg_stat_database PG 17+ columns (sessions, session_time, active_time,
 *    idle_in_transaction_time, parallel_workers_launched)
 *  - pg_stat_checkpointer split from pg_stat_bgwriter (PG 17)
 *  - pg_hba_file_rules / pg_file_settings / pg_ident_file_mappings
 *  - pg_control_checkpoint / pg_control_system / pg_control_init / pg_control_recovery
 *  - pg_database.datfrozenxid typed as xid, datcollversion populated
 *  - pg_replication_origin
 */
class Round18CatalogStatsControlTest {

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

    private static int int1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static boolean hasColumn(String schema, String table, String col) throws SQLException {
        return int1("SELECT count(*)::int FROM information_schema.columns " +
                "WHERE table_schema='" + schema + "' AND table_name='" + table +
                "' AND column_name='" + col + "'") >= 1;
    }

    private static void assertRelationQueryable(String relation, String hint) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM " + relation)) {
            assertTrue(rs.next(), hint);
        }
    }

    // =========================================================================
    // U1. pg_stat_database PG 17+ columns
    // =========================================================================

    @Test
    void pg_stat_database_sessions_column() throws SQLException {
        assertTrue(hasColumn("pg_catalog", "pg_stat_database", "sessions"),
                "pg_stat_database.sessions (PG 14+) must exist");
    }

    @Test
    void pg_stat_database_session_time_column() throws SQLException {
        assertTrue(hasColumn("pg_catalog", "pg_stat_database", "session_time"),
                "pg_stat_database.session_time (PG 14+) must exist");
    }

    @Test
    void pg_stat_database_active_time_column() throws SQLException {
        assertTrue(hasColumn("pg_catalog", "pg_stat_database", "active_time"),
                "pg_stat_database.active_time (PG 14+) must exist");
    }

    @Test
    void pg_stat_database_idle_in_transaction_time_column() throws SQLException {
        assertTrue(hasColumn("pg_catalog", "pg_stat_database", "idle_in_transaction_time"),
                "pg_stat_database.idle_in_transaction_time (PG 14+) must exist");
    }

    @Test
    void pg_stat_database_parallel_workers_launched_column() throws SQLException {
        assertTrue(hasColumn("pg_catalog", "pg_stat_database", "parallel_workers_launched"),
                "pg_stat_database.parallel_workers_launched (PG 18) must exist");
    }

    // =========================================================================
    // U2. pg_stat_checkpointer (PG 17 split)
    // =========================================================================

    @Test
    void pg_stat_checkpointer_view_exists() throws SQLException {
        assertRelationQueryable("pg_stat_checkpointer",
                "pg_stat_checkpointer view (PG 17+) must be queryable");
    }

    @Test
    void pg_stat_bgwriter_has_no_legacy_checkpoint_columns() throws SQLException {
        // PG 17 moved checkpoint-related columns to pg_stat_checkpointer.
        assertFalse(hasColumn("pg_catalog", "pg_stat_bgwriter", "checkpoint_write_time"),
                "checkpoint_write_time must have moved to pg_stat_checkpointer in PG 17");
        assertFalse(hasColumn("pg_catalog", "pg_stat_bgwriter", "buffers_checkpoint"),
                "buffers_checkpoint must have moved to pg_stat_checkpointer in PG 17");
    }

    // =========================================================================
    // U3. pg_hba_file_rules / pg_file_settings / pg_ident_file_mappings
    // =========================================================================

    @Test
    void pg_hba_file_rules_queryable() throws SQLException {
        assertRelationQueryable("pg_hba_file_rules",
                "pg_hba_file_rules must be queryable (even if empty)");
    }

    @Test
    void pg_file_settings_queryable() throws SQLException {
        assertRelationQueryable("pg_file_settings",
                "pg_file_settings must be queryable (even if empty)");
    }

    @Test
    void pg_ident_file_mappings_queryable() throws SQLException {
        assertRelationQueryable("pg_ident_file_mappings",
                "pg_ident_file_mappings must be queryable (even if empty)");
    }

    // =========================================================================
    // U4. pg_control_* SRFs
    // =========================================================================

    @Test
    void pg_control_checkpoint_registered() throws SQLException {
        assertTrue(int1("SELECT count(*)::int FROM pg_proc WHERE proname='pg_control_checkpoint'") >= 1,
                "pg_control_checkpoint must be registered");
    }

    @Test
    void pg_control_system_registered() throws SQLException {
        assertTrue(int1("SELECT count(*)::int FROM pg_proc WHERE proname='pg_control_system'") >= 1,
                "pg_control_system must be registered");
    }

    @Test
    void pg_control_init_registered() throws SQLException {
        assertTrue(int1("SELECT count(*)::int FROM pg_proc WHERE proname='pg_control_init'") >= 1,
                "pg_control_init must be registered");
    }

    @Test
    void pg_control_recovery_registered() throws SQLException {
        assertTrue(int1("SELECT count(*)::int FROM pg_proc WHERE proname='pg_control_recovery'") >= 1,
                "pg_control_recovery must be registered");
    }

    // =========================================================================
    // U5. pg_database.datfrozenxid typed as xid (not int)
    // =========================================================================

    @Test
    void pg_database_datfrozenxid_is_xid_type() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT udt_name FROM information_schema.columns " +
                             "WHERE table_schema='pg_catalog' AND table_name='pg_database' " +
                             "AND column_name='datfrozenxid'")) {
            assertTrue(rs.next(), "datfrozenxid column must exist");
            String t = rs.getString(1);
            assertEquals("xid", t,
                    "pg_database.datfrozenxid must be type 'xid'; got '" + t + "'");
        }
    }

    // =========================================================================
    // U6. pg_database.datcollversion / daticulocale exist
    // =========================================================================

    @Test
    void pg_database_datcollversion_column() throws SQLException {
        assertTrue(hasColumn("pg_catalog", "pg_database", "datcollversion"),
                "pg_database.datcollversion must exist (PG 15+)");
    }

    @Test
    void pg_database_daticulocale_column() throws SQLException {
        assertTrue(hasColumn("pg_catalog", "pg_database", "daticulocale"),
                "pg_database.daticulocale must exist (PG 15+)");
    }

    // =========================================================================
    // V. pg_replication_origin
    // =========================================================================

    @Test
    void pg_replication_origin_queryable() throws SQLException {
        assertRelationQueryable("pg_replication_origin",
                "pg_replication_origin must be queryable (even if empty)");
    }
}
