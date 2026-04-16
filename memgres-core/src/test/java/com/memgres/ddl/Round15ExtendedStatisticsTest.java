package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category D: CREATE STATISTICS (extended statistics).
 *
 * Covers PG 10+ / 14+ extended stats:
 *  - CREATE STATISTICS with dependencies / ndistinct / mcv
 *  - CREATE STATISTICS on expressions (PG 14+)
 *  - ALTER STATISTICS … SET STATISTICS (target)
 *  - DROP STATISTICS
 *  - pg_statistic_ext and pg_statistic_ext_data populated
 */
class Round15ExtendedStatisticsTest {

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

    // =========================================================================
    // A. CREATE STATISTICS dependencies / ndistinct / mcv
    // =========================================================================

    @Test
    void create_statistics_dependencies() throws SQLException {
        exec("CREATE TABLE r15_st_dep (a int, b int, c int)");
        exec("CREATE STATISTICS r15_stat_dep (dependencies) ON a, b FROM r15_st_dep");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_statistic_ext WHERE stxname='r15_stat_dep'");
        assertEquals(1, n, "pg_statistic_ext must contain CREATE STATISTICS row");
    }

    @Test
    void create_statistics_ndistinct() throws SQLException {
        exec("CREATE TABLE r15_st_nd (a int, b int)");
        exec("CREATE STATISTICS r15_stat_nd (ndistinct) ON a, b FROM r15_st_nd");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_statistic_ext WHERE stxname='r15_stat_nd'");
        assertEquals(1, n);
    }

    @Test
    void create_statistics_mcv() throws SQLException {
        exec("CREATE TABLE r15_st_mcv (a int, b int)");
        exec("CREATE STATISTICS r15_stat_mcv (mcv) ON a, b FROM r15_st_mcv");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_statistic_ext WHERE stxname='r15_stat_mcv'");
        assertEquals(1, n);
    }

    @Test
    void create_statistics_all_kinds() throws SQLException {
        exec("CREATE TABLE r15_st_all (a int, b int)");
        exec("CREATE STATISTICS r15_stat_all (ndistinct, dependencies, mcv) "
                + "ON a, b FROM r15_st_all");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_statistic_ext WHERE stxname='r15_stat_all'");
        assertEquals(1, n);
    }

    // =========================================================================
    // B. CREATE STATISTICS on expressions (PG 14+)
    // =========================================================================

    @Test
    void create_statistics_on_expression() throws SQLException {
        exec("CREATE TABLE r15_st_expr (a int, b int)");
        exec("CREATE STATISTICS r15_stat_expr ON (a + b) FROM r15_st_expr");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_statistic_ext WHERE stxname='r15_stat_expr'");
        assertEquals(1, n, "CREATE STATISTICS on expression must be recorded");
    }

    @Test
    void create_statistics_on_multi_expressions() throws SQLException {
        exec("CREATE TABLE r15_st_me (a int, b int, c int)");
        exec("CREATE STATISTICS r15_stat_me "
                + "ON (a + b), (b * c), c FROM r15_st_me");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_statistic_ext WHERE stxname='r15_stat_me'");
        assertEquals(1, n);
    }

    // =========================================================================
    // C. pg_statistic_ext_data populated by ANALYZE
    // =========================================================================

    @Test
    void statistic_ext_data_populated_after_analyze() throws SQLException {
        exec("CREATE TABLE r15_st_data (a int, b int)");
        for (int i = 0; i < 100; i++) {
            exec("INSERT INTO r15_st_data VALUES (" + (i % 10) + ", " + (i % 3) + ")");
        }
        exec("CREATE STATISTICS r15_stat_data (ndistinct) ON a, b FROM r15_st_data");
        exec("ANALYZE r15_st_data");

        int n = scalarInt(
                "SELECT count(*)::int FROM pg_statistic_ext_data e "
                        + "JOIN pg_statistic_ext s ON e.stxoid = s.oid "
                        + "WHERE s.stxname='r15_stat_data'");
        assertTrue(n >= 1,
                "pg_statistic_ext_data should have a row after ANALYZE populates stats");
    }

    // =========================================================================
    // D. ALTER STATISTICS … SET STATISTICS
    // =========================================================================

    @Test
    void alter_statistics_set_statistics_target() throws SQLException {
        exec("CREATE TABLE r15_st_alt (a int, b int)");
        exec("CREATE STATISTICS r15_stat_alt (ndistinct) ON a, b FROM r15_st_alt");
        exec("ALTER STATISTICS r15_stat_alt SET STATISTICS 1000");

        int target = scalarInt(
                "SELECT stxstattarget::int FROM pg_statistic_ext WHERE stxname='r15_stat_alt'");
        assertEquals(1000, target,
                "ALTER STATISTICS SET STATISTICS must update stxstattarget");
    }

    @Test
    void alter_statistics_rename() throws SQLException {
        exec("CREATE TABLE r15_st_ren (a int, b int)");
        exec("CREATE STATISTICS r15_stat_ren1 (ndistinct) ON a, b FROM r15_st_ren");
        exec("ALTER STATISTICS r15_stat_ren1 RENAME TO r15_stat_ren2");

        int n = scalarInt(
                "SELECT count(*)::int FROM pg_statistic_ext WHERE stxname='r15_stat_ren2'");
        assertEquals(1, n, "RENAME must update stxname");
    }

    // =========================================================================
    // E. DROP STATISTICS
    // =========================================================================

    @Test
    void drop_statistics_removes_from_catalog() throws SQLException {
        exec("CREATE TABLE r15_st_drop (a int, b int)");
        exec("CREATE STATISTICS r15_stat_drop (ndistinct) ON a, b FROM r15_st_drop");
        exec("DROP STATISTICS r15_stat_drop");

        int n = scalarInt(
                "SELECT count(*)::int FROM pg_statistic_ext WHERE stxname='r15_stat_drop'");
        assertEquals(0, n, "DROP STATISTICS must remove pg_statistic_ext row");
    }

    @Test
    void drop_statistics_if_exists() throws SQLException {
        // IF EXISTS must swallow missing-object error
        exec("DROP STATISTICS IF EXISTS r15_stat_nonexistent");
    }

    // =========================================================================
    // F. pg_stats_ext view
    // =========================================================================

    @Test
    void pg_stats_ext_view_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT schemaname, tablename, statistics_name, attnames, kinds, exprs"
                             + " FROM pg_stats_ext LIMIT 1")) {
            // Just shape — don't need rows
            rs.getMetaData();
        }
    }
}
