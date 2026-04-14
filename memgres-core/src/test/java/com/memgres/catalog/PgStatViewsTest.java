package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 10 failures from pg-stat-views.sql where Memgres diverges from PG 18.
 *
 * Stmt 4:  pg_stat_user_tables columns (relid, schemaname, relname, seq_scan, idx_scan,
 *          n_tup_ins, n_tup_upd, n_tup_del) should all be found -> Memgres returns NULL for all
 * Stmt 5:  pg_stat_all_tables should be queryable (count >= 0 = true) -> Memgres returns NULL
 * Stmt 6:  pg_stat_user_indexes columns (relid, indexrelid, idx_scan, idx_tup_read) should all
 *          be found -> Memgres returns NULL for all
 * Stmt 7:  pg_stat_all_indexes should be queryable -> Memgres returns NULL
 * Stmt 8:  pg_stat_database columns (datid, datname, numbackends, xact_commit, xact_rollback)
 *          should all be found -> Memgres returns NULL for all
 * Stmt 12: pg_statio_user_tables should be queryable -> Memgres returns NULL
 * Stmt 14: pg_stat_activity columns (datid, pid, usename, state, query, backend_start)
 *          should all be found -> Memgres returns NULL for all
 * Stmt 15: pg_stat_activity state for current session should be 'active' -> Memgres returns 'idle'
 * Stmt 16: pg_stat_replication should be queryable -> Memgres returns NULL
 * Stmt 18: pg_am columns (oid, amname, amhandler, amtype) should all be found
 *          -> Memgres returns NULL for all
 */
class PgStatViewsTest {

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
            s.execute("DROP SCHEMA IF EXISTS psv_test CASCADE");
            s.execute("CREATE SCHEMA psv_test");
            s.execute("SET search_path = psv_test, public");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS psv_test CASCADE");
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
     * Stmt 4: pg_stat_user_tables should have standard columns in information_schema.
     *
     * PG 18: all 8 columns found (true for each)
     * Memgres: returns NULL for all 8
     */
    @Test
    void pgStatUserTablesHasExpectedColumns() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'relid') AS has_relid, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'schemaname') AS has_schemaname, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'relname') AS has_relname, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'seq_scan') AS has_seq_scan, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'idx_scan') AS has_idx_scan, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'n_tup_ins') AS has_n_tup_ins, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'n_tup_upd') AS has_n_tup_upd, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_tables' AND column_name = 'n_tup_del') AS has_n_tup_del")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("has_relid"), "pg_stat_user_tables should have column 'relid'");
            assertTrue(rs.getBoolean("has_schemaname"), "pg_stat_user_tables should have column 'schemaname'");
            assertTrue(rs.getBoolean("has_relname"), "pg_stat_user_tables should have column 'relname'");
            assertTrue(rs.getBoolean("has_seq_scan"), "pg_stat_user_tables should have column 'seq_scan'");
            assertTrue(rs.getBoolean("has_idx_scan"), "pg_stat_user_tables should have column 'idx_scan'");
            assertTrue(rs.getBoolean("has_n_tup_ins"), "pg_stat_user_tables should have column 'n_tup_ins'");
            assertTrue(rs.getBoolean("has_n_tup_upd"), "pg_stat_user_tables should have column 'n_tup_upd'");
            assertTrue(rs.getBoolean("has_n_tup_del"), "pg_stat_user_tables should have column 'n_tup_del'");
        }
    }

    /**
     * Stmt 5: pg_stat_all_tables should be queryable.
     *
     * PG 18: count(*) >= 0 returns true
     * Memgres: returns NULL
     */
    @Test
    void pgStatAllTablesIsQueryable() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) >= 0 AS queryable FROM pg_stat_all_tables")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("queryable"),
                    "pg_stat_all_tables should be queryable (count >= 0 should be true)");
        }
    }

    /**
     * Stmt 6: pg_stat_user_indexes should have standard columns in information_schema.
     *
     * PG 18: all 4 columns found (true for each)
     * Memgres: returns NULL for all 4
     */
    @Test
    void pgStatUserIndexesHasExpectedColumns() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_indexes' AND column_name = 'relid') AS has_relid, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_indexes' AND column_name = 'indexrelid') AS has_indexrelid, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_indexes' AND column_name = 'idx_scan') AS has_idx_scan, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_user_indexes' AND column_name = 'idx_tup_read') AS has_idx_tup_read")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("has_relid"), "pg_stat_user_indexes should have column 'relid'");
            assertTrue(rs.getBoolean("has_indexrelid"), "pg_stat_user_indexes should have column 'indexrelid'");
            assertTrue(rs.getBoolean("has_idx_scan"), "pg_stat_user_indexes should have column 'idx_scan'");
            assertTrue(rs.getBoolean("has_idx_tup_read"), "pg_stat_user_indexes should have column 'idx_tup_read'");
        }
    }

    /**
     * Stmt 7: pg_stat_all_indexes should be queryable.
     *
     * PG 18: count(*) >= 0 returns true
     * Memgres: returns NULL
     */
    @Test
    void pgStatAllIndexesIsQueryable() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) >= 0 AS queryable FROM pg_stat_all_indexes")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("queryable"),
                    "pg_stat_all_indexes should be queryable (count >= 0 should be true)");
        }
    }

    /**
     * Stmt 8: pg_stat_database should have standard columns in information_schema.
     *
     * PG 18: all 5 columns found (true for each)
     * Memgres: returns NULL for all 5
     */
    @Test
    void pgStatDatabaseHasExpectedColumns() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_database' AND column_name = 'datid') AS has_datid, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_database' AND column_name = 'datname') AS has_datname, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_database' AND column_name = 'numbackends') AS has_numbackends, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_database' AND column_name = 'xact_commit') AS has_xact_commit, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_database' AND column_name = 'xact_rollback') AS has_xact_rollback")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("has_datid"), "pg_stat_database should have column 'datid'");
            assertTrue(rs.getBoolean("has_datname"), "pg_stat_database should have column 'datname'");
            assertTrue(rs.getBoolean("has_numbackends"), "pg_stat_database should have column 'numbackends'");
            assertTrue(rs.getBoolean("has_xact_commit"), "pg_stat_database should have column 'xact_commit'");
            assertTrue(rs.getBoolean("has_xact_rollback"), "pg_stat_database should have column 'xact_rollback'");
        }
    }

    /**
     * Stmt 12: pg_statio_user_tables should be queryable.
     *
     * PG 18: count(*) >= 0 returns true
     * Memgres: returns NULL
     */
    @Test
    void pgStatioUserTablesIsQueryable() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) >= 0 AS queryable FROM pg_statio_user_tables")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("queryable"),
                    "pg_statio_user_tables should be queryable (count >= 0 should be true)");
        }
    }

    /**
     * Stmt 14: pg_stat_activity should have standard columns in information_schema.
     *
     * PG 18: all 6 columns found (true for each)
     * Memgres: returns NULL for all 6
     */
    @Test
    void pgStatActivityHasExpectedColumns() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_activity' AND column_name = 'datid') AS has_datid, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_activity' AND column_name = 'pid') AS has_pid, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_activity' AND column_name = 'usename') AS has_usename, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_activity' AND column_name = 'state') AS has_state, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_activity' AND column_name = 'query') AS has_query, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_activity' AND column_name = 'backend_start') AS has_backend_start")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("has_datid"), "pg_stat_activity should have column 'datid'");
            assertTrue(rs.getBoolean("has_pid"), "pg_stat_activity should have column 'pid'");
            assertTrue(rs.getBoolean("has_usename"), "pg_stat_activity should have column 'usename'");
            assertTrue(rs.getBoolean("has_state"), "pg_stat_activity should have column 'state'");
            assertTrue(rs.getBoolean("has_query"), "pg_stat_activity should have column 'query'");
            assertTrue(rs.getBoolean("has_backend_start"), "pg_stat_activity should have column 'backend_start'");
        }
    }

    /**
     * Stmt 15: pg_stat_activity should report the current session state as 'active'.
     *
     * PG 18: returns 'active'
     * Memgres: returns 'idle'
     */
    @Test
    void pgStatActivityCurrentSessionStateIsActive() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT state FROM pg_stat_activity WHERE pid = pg_backend_pid()")) {
            assertTrue(rs.next(), "Expected one result row for the current backend");
            assertEquals("active", rs.getString("state"),
                    "Current session state should be 'active' while executing a query");
        }
    }

    /**
     * Stmt 16: pg_stat_replication should be queryable.
     *
     * PG 18: count(*) >= 0 returns true
     * Memgres: returns NULL
     */
    @Test
    void pgStatReplicationIsQueryable() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) >= 0 AS queryable FROM pg_stat_replication")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("queryable"),
                    "pg_stat_replication should be queryable (count >= 0 should be true)");
        }
    }

    /**
     * Stmt 18: pg_am should have standard columns in information_schema.
     *
     * PG 18: all 4 columns found (true for each)
     * Memgres: returns NULL for all 4
     */
    @Test
    void pgAmHasExpectedColumns() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_am' AND column_name = 'oid') AS has_oid, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_am' AND column_name = 'amname') AS has_amname, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_am' AND column_name = 'amhandler') AS has_amhandler, "
                     + "(SELECT count(*) > 0 FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_am' AND column_name = 'amtype') AS has_amtype")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("has_oid"), "pg_am should have column 'oid'");
            assertTrue(rs.getBoolean("has_amname"), "pg_am should have column 'amname'");
            assertTrue(rs.getBoolean("has_amhandler"), "pg_am should have column 'amhandler'");
            assertTrue(rs.getBoolean("has_amtype"), "pg_am should have column 'amtype'");
        }
    }
}
