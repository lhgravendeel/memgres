package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: Replication infrastructure.
 *
 * PG exposes a rich set of replication functions and catalogs. Memgres has
 * stubs for a few but lacks most. Tests verify the presence + behavior of
 * slot management, publications, subscriptions, and WAL-side functions.
 *
 * Expected to fail today.
 */
class Round14ReplicationTest {

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

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getInt(1);
        }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. pg_replication_slots catalog
    // =========================================================================

    @Test
    void pg_replication_slots_queryable() throws SQLException {
        assertEquals(0, scalarInt("SELECT count(*)::int FROM pg_replication_slots"));
    }

    @Test
    void pg_replication_slots_has_expected_columns() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM information_schema.columns "
                             + "WHERE table_name = 'pg_replication_slots'")) {
            assertTrue(rs.next());
            // PG 18 has ~16 columns: slot_name, plugin, slot_type, datoid, database,
            // temporary, active, active_pid, xmin, catalog_xmin, restart_lsn,
            // confirmed_flush_lsn, wal_status, safe_wal_size, two_phase, conflicting
            assertTrue(rs.getInt(1) >= 10, "pg_replication_slots must expose PG columns");
        }
    }

    // =========================================================================
    // B. Logical replication slot functions
    // =========================================================================

    @Test
    void pg_create_logical_replication_slot_exists() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_create_logical_replication_slot'"));
    }

    @Test
    void pg_create_logical_replication_slot_creates_slot() throws SQLException {
        try {
            exec("SELECT * FROM pg_create_logical_replication_slot('r14_slot_a', 'pgoutput')");
            assertEquals(1, scalarInt(
                    "SELECT count(*)::int FROM pg_replication_slots WHERE slot_name = 'r14_slot_a'"));
        } finally {
            try { exec("SELECT pg_drop_replication_slot('r14_slot_a')"); } catch (SQLException ignored) {}
        }
    }

    @Test
    void pg_logical_slot_get_changes_exists() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_logical_slot_get_changes'"));
    }

    @Test
    void pg_logical_slot_peek_changes_exists() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_logical_slot_peek_changes'"));
    }

    @Test
    void pg_replication_slot_advance_exists() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_replication_slot_advance'"));
    }

    // =========================================================================
    // C. Physical replication slot
    // =========================================================================

    @Test
    void pg_create_physical_replication_slot_exists() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_create_physical_replication_slot'"));
    }

    @Test
    void pg_drop_replication_slot_exists() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_drop_replication_slot'"));
    }

    // =========================================================================
    // D. pg_stat_replication / wal_receiver
    // =========================================================================

    @Test
    void pg_stat_replication_queryable() throws SQLException {
        assertEquals(0, scalarInt("SELECT count(*)::int FROM pg_stat_replication"));
    }

    @Test
    void pg_stat_wal_receiver_queryable() throws SQLException {
        assertEquals(0, scalarInt("SELECT count(*)::int FROM pg_stat_wal_receiver"));
    }

    @Test
    void pg_stat_subscription_queryable() throws SQLException {
        assertEquals(0, scalarInt("SELECT count(*)::int FROM pg_stat_subscription"));
    }

    @Test
    void pg_stat_subscription_stats_queryable() throws SQLException {
        // PG 15+
        assertEquals(0, scalarInt("SELECT count(*)::int FROM pg_stat_subscription_stats"));
    }

    // =========================================================================
    // E. Replication origins
    // =========================================================================

    @Test
    void pg_replication_origin_queryable() throws SQLException {
        assertEquals(0, scalarInt("SELECT count(*)::int FROM pg_replication_origin"));
    }

    @Test
    void pg_replication_origin_status_queryable() throws SQLException {
        assertEquals(0, scalarInt("SELECT count(*)::int FROM pg_replication_origin_status"));
    }

    // =========================================================================
    // F. Publications
    // =========================================================================

    @Test
    void create_publication_for_all_tables() throws SQLException {
        exec("CREATE PUBLICATION r14_pub_all FOR ALL TABLES");
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_publication WHERE pubname = 'r14_pub_all'"));
        // Should have puballtables = true
        assertEquals("t",
                scalarString("SELECT puballtables::text FROM pg_publication WHERE pubname = 'r14_pub_all'"));
    }

    @Test
    void create_publication_for_specific_tables() throws SQLException {
        exec("CREATE TABLE r14_pub_t1 (id int)");
        exec("CREATE TABLE r14_pub_t2 (id int)");
        exec("CREATE PUBLICATION r14_pub_spec FOR TABLE r14_pub_t1, r14_pub_t2");
        assertEquals(2, scalarInt(
                "SELECT count(*)::int FROM pg_publication_rel "
                        + "WHERE prpubid = (SELECT oid FROM pg_publication WHERE pubname = 'r14_pub_spec')"));
    }

    @Test
    void pg_publication_tables_view_populated() throws SQLException {
        exec("CREATE TABLE r14_pt_t1 (id int)");
        exec("CREATE PUBLICATION r14_pt_pub FOR TABLE r14_pt_t1");
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_publication_tables WHERE pubname = 'r14_pt_pub'"));
    }

    @Test
    void alter_publication_add_table() throws SQLException {
        exec("CREATE TABLE r14_alt_pub_t (id int)");
        exec("CREATE PUBLICATION r14_alt_pub FOR TABLE r14_alt_pub_t");
        exec("CREATE TABLE r14_alt_pub_t2 (id int)");
        exec("ALTER PUBLICATION r14_alt_pub ADD TABLE r14_alt_pub_t2");
        assertEquals(2, scalarInt(
                "SELECT count(*)::int FROM pg_publication_rel "
                        + "WHERE prpubid = (SELECT oid FROM pg_publication WHERE pubname = 'r14_alt_pub')"));
    }

    @Test
    void create_publication_for_tables_in_schema() throws SQLException {
        // PG 15+
        exec("CREATE SCHEMA r14_pub_sch");
        try {
            exec("CREATE PUBLICATION r14_pub_bysch FOR TABLES IN SCHEMA r14_pub_sch");
            assertEquals(1, scalarInt(
                    "SELECT count(*)::int FROM pg_publication WHERE pubname = 'r14_pub_bysch'"));
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"), "FOR TABLES IN SCHEMA must parse, got: " + msg);
        }
    }

    // =========================================================================
    // G. Subscriptions
    // =========================================================================

    @Test
    void pg_subscription_queryable() throws SQLException {
        assertEquals(0, scalarInt("SELECT count(*)::int FROM pg_subscription"));
    }

    @Test
    void create_subscription_parses() throws SQLException {
        // CREATE SUBSCRIPTION will attempt an actual connect; we test parse+catalog.
        try {
            exec("CREATE SUBSCRIPTION r14_sub CONNECTION 'dbname=nosuch host=127.0.0.1' "
                    + "PUBLICATION r14_pub_none WITH (connect = false)");
            assertEquals(1, scalarInt(
                    "SELECT count(*)::int FROM pg_subscription WHERE subname = 'r14_sub'"));
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"), "CREATE SUBSCRIPTION must parse, got: " + msg);
        }
    }

    // =========================================================================
    // H. WAL-side utilities
    // =========================================================================

    @Test
    void pg_switch_wal_exists() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_switch_wal'"));
    }

    @Test
    void pg_walfile_name_exists() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_walfile_name'"));
    }

    @Test
    void pg_last_wal_receive_lsn_exists() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_last_wal_receive_lsn'"));
    }

    @Test
    void pg_last_wal_replay_lsn_exists() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_last_wal_replay_lsn'"));
    }

    @Test
    void pg_is_in_recovery_returns_boolean() throws SQLException {
        String v = scalarString("SELECT pg_is_in_recovery()::text");
        assertTrue(v.equals("t") || v.equals("f"), "pg_is_in_recovery must return bool, got: " + v);
    }

    @Test
    void pg_backup_start_exists() throws SQLException {
        // Renamed from pg_start_backup in PG 15+
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_backup_start'"));
    }

    @Test
    void pg_backup_stop_exists() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_backup_stop'"));
    }

    @Test
    void pg_promote_exists() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_promote'"));
    }

    @Test
    void pg_create_restore_point_exists() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_create_restore_point'"));
    }
}
