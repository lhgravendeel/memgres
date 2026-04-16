package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests pg_stat_* view population differences between Memgres and PostgreSQL 18.
 *
 * PG 18: Statistical views like pg_stat_user_tables, pg_stat_user_indexes, and
 * pg_stat_database return real accumulated statistics after DML operations.
 * ANALYZE populates pg_statistic for the planner. pg_stat_activity shows real
 * backend information.
 *
 * Memgres: All pg_stat_* views return empty result sets or minimal stubs.
 * ANALYZE is a no-op. pg_stat_activity shows basic session info but no real
 * backend statistics.
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres.
 */
class PgStatViewsCompatTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    @BeforeEach
    void createAndPopulate() throws SQLException {
        exec("DROP TABLE IF EXISTS stat_test CASCADE");
        exec("CREATE TABLE stat_test (id integer PRIMARY KEY, val text)");
        exec("INSERT INTO stat_test VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        exec("UPDATE stat_test SET val = 'aa' WHERE id = 1");
        exec("DELETE FROM stat_test WHERE id = 3");
    }

    // -------------------------------------------------------------------------
    // pg_stat_user_tables should show DML counts
    // -------------------------------------------------------------------------

    @Test
    void pgStatUserTables_shouldShowInsertCount() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT n_tup_ins FROM pg_stat_user_tables "
                             + "WHERE relname = 'stat_test'")) {
            assertTrue(rs.next(),
                    "pg_stat_user_tables should have a row for 'stat_test'");
            long inserts = rs.getLong("n_tup_ins");
            assertTrue(inserts >= 3,
                    "n_tup_ins should be >= 3 after inserting 3 rows, got " + inserts);
        }
    }

    @Test
    void pgStatUserTables_shouldShowUpdateCount() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT n_tup_upd FROM pg_stat_user_tables "
                             + "WHERE relname = 'stat_test'")) {
            assertTrue(rs.next(),
                    "pg_stat_user_tables should have a row for 'stat_test'");
            long updates = rs.getLong("n_tup_upd");
            assertTrue(updates >= 1,
                    "n_tup_upd should be >= 1 after updating 1 row, got " + updates);
        }
    }

    @Test
    void pgStatUserTables_shouldShowDeleteCount() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT n_tup_del FROM pg_stat_user_tables "
                             + "WHERE relname = 'stat_test'")) {
            assertTrue(rs.next());
            long deletes = rs.getLong("n_tup_del");
            assertTrue(deletes >= 1,
                    "n_tup_del should be >= 1 after deleting 1 row, got " + deletes);
        }
    }

    // -------------------------------------------------------------------------
    // pg_stat_user_indexes should show index scans after query
    // -------------------------------------------------------------------------

    @Test
    void pgStatUserIndexes_shouldExistForPrimaryKey() throws SQLException {
        // The PK index should appear in pg_stat_user_indexes
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT indexrelname, idx_scan FROM pg_stat_user_indexes "
                             + "WHERE relname = 'stat_test'")) {
            assertTrue(rs.next(),
                    "pg_stat_user_indexes should have a row for stat_test's PK index");
        }
    }

    // -------------------------------------------------------------------------
    // pg_stat_database should show transaction counts
    // -------------------------------------------------------------------------

    @Test
    void pgStatDatabase_shouldShowTransactionCounts() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT xact_commit FROM pg_stat_database "
                             + "WHERE datname = current_database()")) {
            assertTrue(rs.next(),
                    "pg_stat_database should have a row for the current database");
            long commits = rs.getLong("xact_commit");
            assertTrue(commits > 0,
                    "xact_commit should be > 0 after running queries, got " + commits);
        }
    }

    // -------------------------------------------------------------------------
    // ANALYZE should populate pg_statistic (at least for user tables)
    // -------------------------------------------------------------------------

    @Test
    void analyze_shouldPopulateStatistics() throws SQLException {
        exec("ANALYZE stat_test");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT starelid FROM pg_statistic "
                             + "WHERE starelid = (SELECT oid FROM pg_class WHERE relname = 'stat_test') "
                             + "LIMIT 1")) {
            assertTrue(rs.next(),
                    "ANALYZE should populate pg_statistic for the table. "
                            + "Memgres treats ANALYZE as a no-op.");
        }
    }

    // -------------------------------------------------------------------------
    // pg_stat_activity should show real query state
    // -------------------------------------------------------------------------

    @Test
    void pgStatActivity_shouldShowActiveQuery() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT pid, state, query FROM pg_stat_activity "
                             + "WHERE pid = pg_backend_pid()")) {
            assertTrue(rs.next(), "Should find own backend in pg_stat_activity");
            String state = rs.getString("state");
            // PG shows "active" for the current query
            assertEquals("active", state,
                    "Own backend state should be 'active' during query execution, got: " + state);
        }
    }

    // -------------------------------------------------------------------------
    // pg_stat_all_tables should include system tables
    // -------------------------------------------------------------------------

    @Test
    void pgStatAllTables_shouldIncludeUserTables() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) FROM pg_stat_all_tables "
                             + "WHERE relname = 'stat_test'")) {
            rs.next();
            assertTrue(rs.getInt(1) > 0,
                    "pg_stat_all_tables should include user table 'stat_test'");
        }
    }
}
