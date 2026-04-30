package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category D: VACUUM / ANALYZE / CLUSTER / REINDEX observable surface.
 *
 * Memgres doesn't have a physical storage engine so no rows get reclaimed,
 * but a PG-compatible engine must still expose the observable side effects:
 *  - pg_stat_user_tables.last_vacuum / last_analyze timestamps update
 *  - pg_statistic populated after ANALYZE (approximate n_distinct, avg_width, etc.)
 *  - command tags "VACUUM", "ANALYZE", "CLUSTER", "REINDEX"
 *  - VACUUM/CLUSTER in a transaction block raises SQLSTATE 25001
 *  - VACUUM VERBOSE emits NOTICE messages (getWarnings())
 */
class Round15VacuumMaintenanceTest {

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
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static Timestamp scalarTs(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getTimestamp(1);
        }
    }

    // =========================================================================
    // A. VACUUM updates pg_stat_user_tables.last_vacuum
    // =========================================================================

    @Test
    void vacuum_updates_last_vacuum_timestamp() throws SQLException {
        exec("CREATE TABLE r15_v_last (id int)");
        exec("INSERT INTO r15_v_last VALUES (1),(2),(3)");
        exec("VACUUM r15_v_last");

        Timestamp lv = scalarTs(
                "SELECT last_vacuum FROM pg_stat_user_tables WHERE relname='r15_v_last'");
        assertNotNull(lv,
                "VACUUM should update pg_stat_user_tables.last_vacuum (was null)");
    }

    @Test
    void vacuum_analyze_updates_last_analyze() throws SQLException {
        exec("CREATE TABLE r15_va_last (id int, v text)");
        exec("INSERT INTO r15_va_last VALUES (1,'a'),(2,'b')");
        exec("VACUUM ANALYZE r15_va_last");

        Timestamp la = scalarTs(
                "SELECT last_analyze FROM pg_stat_user_tables WHERE relname='r15_va_last'");
        assertNotNull(la,
                "VACUUM ANALYZE should update pg_stat_user_tables.last_analyze");
    }

    @Test
    void analyze_updates_last_analyze() throws SQLException {
        exec("CREATE TABLE r15_a_last (id int)");
        exec("INSERT INTO r15_a_last VALUES (1)");
        exec("ANALYZE r15_a_last");

        Timestamp la = scalarTs(
                "SELECT last_analyze FROM pg_stat_user_tables WHERE relname='r15_a_last'");
        assertNotNull(la, "ANALYZE should update last_analyze");
    }

    // =========================================================================
    // B. ANALYZE populates pg_statistic / pg_stats
    // =========================================================================

    @Test
    void analyze_populates_pg_stats() throws SQLException {
        exec("CREATE TABLE r15_ps (id int, v text)");
        for (int i = 0; i < 50; i++) {
            exec("INSERT INTO r15_ps VALUES (" + i + ", 'val" + (i % 5) + "')");
        }
        exec("ANALYZE r15_ps");

        int n = scalarInt("SELECT count(*)::int FROM pg_stats WHERE tablename='r15_ps'");
        assertTrue(n >= 2, "ANALYZE must populate pg_stats rows for each column; got " + n);
    }

    @Test
    void analyze_computes_n_distinct() throws SQLException {
        exec("CREATE TABLE r15_nd (g int)");
        for (int i = 0; i < 100; i++) {
            exec("INSERT INTO r15_nd VALUES (" + (i % 4) + ")");  // 4 distinct values
        }
        exec("ANALYZE r15_nd");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT n_distinct FROM pg_stats WHERE tablename='r15_nd' AND attname='g'")) {
            assertTrue(rs.next(), "pg_stats row for r15_nd.g should exist after ANALYZE");
            float nd = rs.getFloat(1);
            // Can be exact 4 or a small positive value; just check non-zero
            assertTrue(nd > 0 || nd < 0,
                    "ANALYZE should populate non-zero n_distinct; got " + nd);
        }
    }

    @Test
    void analyze_specific_columns() throws SQLException {
        exec("CREATE TABLE r15_ac (a int, b int, c text)");
        exec("INSERT INTO r15_ac VALUES (1,2,'x')");
        // Column-list form
        exec("ANALYZE r15_ac (a, c)");

        int n = scalarInt(
                "SELECT count(*)::int FROM pg_stats "
                        + "WHERE tablename='r15_ac' AND attname IN ('a','c')");
        assertTrue(n >= 1, "ANALYZE (a,c) should populate stats for those columns only");
    }

    // =========================================================================
    // C. VACUUM/CLUSTER in transaction block must error with SQLSTATE 25001
    // =========================================================================

    @Test
    void vacuum_inside_txn_block_errors_25001() throws SQLException {
        exec("CREATE TABLE r15_v_txn (id int)");
        conn.setAutoCommit(false);
        try {
            exec("VACUUM r15_v_txn");
            fail("VACUUM inside transaction block must error with SQLSTATE 25001");
        } catch (SQLException e) {
            String ss = e.getSQLState();
            assertEquals("25001", ss,
                    "Expected SQLSTATE 25001 (active_sql_transaction); got " + ss
                            + " — " + e.getMessage());
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void cluster_inside_txn_block_succeeds() throws SQLException {
        // PG allows CLUSTER inside transaction blocks (unlike VACUUM)
        exec("CREATE TABLE r15_cl_txn (id int PRIMARY KEY)");
        conn.setAutoCommit(false);
        try {
            exec("CLUSTER r15_cl_txn USING r15_cl_txn_pkey");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // D. VACUUM options matrix
    // =========================================================================

    @Test
    void vacuum_options_matrix() throws SQLException {
        exec("CREATE TABLE r15_vopt (id int)");
        // All these should be accepted; none should error
        exec("VACUUM (FULL) r15_vopt");
        exec("VACUUM (FREEZE) r15_vopt");
        exec("VACUUM (VERBOSE) r15_vopt");
        exec("VACUUM (ANALYZE) r15_vopt");
        exec("VACUUM (SKIP_LOCKED) r15_vopt");
        exec("VACUUM (DISABLE_PAGE_SKIPPING) r15_vopt");
        exec("VACUUM (INDEX_CLEANUP TRUE) r15_vopt");
        exec("VACUUM (TRUNCATE FALSE) r15_vopt");
        // PG 16+: PROCESS_TOAST, PROCESS_MAIN
        exec("VACUUM (PROCESS_TOAST) r15_vopt");
        // Combo
        exec("VACUUM (ANALYZE, VERBOSE) r15_vopt");
    }

    @Test
    void vacuum_verbose_emits_notice() throws SQLException {
        exec("CREATE TABLE r15_vv (id int)");
        exec("INSERT INTO r15_vv VALUES (1),(2),(3)");
        try (Statement s = conn.createStatement()) {
            s.execute("VACUUM (VERBOSE) r15_vv");
            SQLWarning w = s.getWarnings();
            assertNotNull(w,
                    "VACUUM VERBOSE must emit at least one NOTICE/SQLWarning");
        }
    }

    // =========================================================================
    // E. REINDEX
    // =========================================================================

    @Test
    void reindex_table_accepted() throws SQLException {
        exec("CREATE TABLE r15_ri (id int PRIMARY KEY, v text)");
        exec("CREATE INDEX r15_ri_v_idx ON r15_ri (v)");
        exec("INSERT INTO r15_ri VALUES (1,'a'),(2,'b')");
        exec("REINDEX TABLE r15_ri");

        // Index still usable
        int n = scalarInt("SELECT count(*)::int FROM r15_ri WHERE v='a'");
        assertEquals(1, n, "Table should be queryable after REINDEX");
    }

    @Test
    void reindex_index_accepted() throws SQLException {
        exec("CREATE TABLE r15_rii (id int PRIMARY KEY)");
        exec("CREATE INDEX r15_rii_idx ON r15_rii (id)");
        exec("REINDEX INDEX r15_rii_idx");
    }

    @Test
    void reindex_concurrently_accepted() throws SQLException {
        exec("CREATE TABLE r15_ric (id int PRIMARY KEY)");
        exec("CREATE INDEX r15_ric_idx ON r15_ric (id)");
        exec("REINDEX INDEX CONCURRENTLY r15_ric_idx");
    }

    @Test
    void reindex_schema_accepted() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS r15_rischema");
        exec("CREATE TABLE r15_rischema.t (id int PRIMARY KEY)");
        exec("REINDEX SCHEMA r15_rischema");
    }

    // =========================================================================
    // F. CLUSTER
    // =========================================================================

    @Test
    void cluster_accepts_using_index() throws SQLException {
        exec("CREATE TABLE r15_cl (id int PRIMARY KEY, v text)");
        exec("INSERT INTO r15_cl VALUES (1,'a'),(2,'b'),(3,'c')");
        // Requires an index to cluster on
        exec("CLUSTER r15_cl USING r15_cl_pkey");

        int n = scalarInt("SELECT count(*)::int FROM r15_cl");
        assertEquals(3, n, "CLUSTER should preserve rows");
    }

    @Test
    void cluster_remembers_index_for_reclustering() throws SQLException {
        exec("CREATE TABLE r15_cl2 (id int PRIMARY KEY, v text)");
        exec("CLUSTER r15_cl2 USING r15_cl2_pkey");
        // pg_class.relkind='r' and indisclustered='t' on the index
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_index i "
                        + "JOIN pg_class c ON i.indexrelid = c.oid "
                        + "WHERE c.relname='r15_cl2_pkey' AND i.indisclustered");
        assertEquals(1, n, "pg_index.indisclustered should be true after CLUSTER USING");
    }

    // =========================================================================
    // G. VACUUM FULL command tag (should tag commands correctly)
    // =========================================================================

    @Test
    void vacuum_full_accepted() throws SQLException {
        exec("CREATE TABLE r15_vf (id int)");
        exec("VACUUM FULL r15_vf");
    }
}
