package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: pg_stat_statements extension surface.
 *
 * Tools like pgAdmin, DataDog, and any query-latency dashboard lean on this
 * extension. At minimum, the following must be SELECT-able even if empty:
 *   - pg_stat_statements view
 *   - pg_stat_statements_info
 *   - pg_stat_statements_reset()
 * And CREATE EXTENSION pg_stat_statements must succeed or be idempotent.
 */
class Round14PgStatStatementsTest {

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

    @Test
    void create_extension_pg_stat_statements_idempotent() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        // Running again should not error
        exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        assertTrue(scalarInt(
                "SELECT count(*)::int FROM pg_extension WHERE extname = 'pg_stat_statements'") >= 1);
    }

    @Test
    void pg_stat_statements_view_queryable() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::int FROM pg_stat_statements")) {
            assertTrue(rs.next());
            // Any non-negative count is acceptable
            assertTrue(rs.getInt(1) >= 0);
        }
    }

    @Test
    void pg_stat_statements_info_queryable() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::int FROM pg_stat_statements_info")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 0);
        }
    }

    @Test
    void pg_stat_statements_reset_callable() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        // reset() returns void or empty; should not error
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT pg_stat_statements_reset()");
        }
    }

    @Test
    void pg_stat_statements_required_columns_present() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        // Core columns that every observability tool inspects
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT queryid, query, calls, total_exec_time, rows "
                             + "FROM pg_stat_statements LIMIT 0")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(5, md.getColumnCount());
        }
    }

    @Test
    void pg_stat_statements_total_plan_time_column() throws SQLException {
        // PG 13+ split plan vs exec time
        exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT total_plan_time, total_exec_time FROM pg_stat_statements LIMIT 0")) {
            assertEquals(2, rs.getMetaData().getColumnCount());
        }
    }

    @Test
    void pg_stat_statements_toplevel_column() throws SQLException {
        // PG 14+ toplevel column
        exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT toplevel FROM pg_stat_statements LIMIT 0")) {
            assertEquals(1, rs.getMetaData().getColumnCount());
        }
    }

    @Test
    void pg_stat_statements_wal_counters_present() throws SQLException {
        // PG 13+ WAL metrics
        exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT wal_records, wal_fpi, wal_bytes FROM pg_stat_statements LIMIT 0")) {
            assertEquals(3, rs.getMetaData().getColumnCount());
        }
    }

    @Test
    void pg_stat_statements_jit_columns_present() throws SQLException {
        // PG 15+ JIT counters
        exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT jit_functions, jit_generation_time FROM pg_stat_statements LIMIT 0")) {
            assertEquals(2, rs.getMetaData().getColumnCount());
        }
    }

    @Test
    void pg_stat_statements_reset_with_args_pg17() throws SQLException {
        // PG 17+ pg_stat_statements_reset(userid, dbid, queryid, minmax_only)
        exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT pg_stat_statements_reset(0, 0, 0)");
        }
    }

    @Test
    void track_activity_query_size_setting() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SHOW track_activity_query_size")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void pg_stat_statements_max_setting() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SHOW pg_stat_statements.max")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void compute_query_id_setting() throws SQLException {
        // PG 14+
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SHOW compute_query_id")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }
}
